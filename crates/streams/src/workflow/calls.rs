//! The Stream calls of one run, with no I/O. The machines of the run own them, and the Worker's
//! [`StreamManager`](super::StreamManager) runs the calls that they start.

use super::{
    RunGate, StreamRuntimes, TaskState,
    manager::{Call, Op, Outcome, Request, StreamResolution, failure, next_call_id},
    marker::MarkerBuilder,
};
use crate::{
    StreamError,
    ids::{check_store_name, check_stream_id},
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, Producer, StreamRef,
        WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
};
use temporalio_common::protos::{
    coresdk::{
        external_data::{StreamMarkerData, stream_append_result},
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, StreamAppended, resolve_stream_append,
            workflow_activation_job,
        },
        workflow_commands::{AppendToStream, CloseStream},
    },
    temporal::api::{common::v1::Payload, failure::v1::Failure},
};
use tracing::warn;

/// Why a Stream call fails its Workflow Task.
#[derive(Clone, Debug, PartialEq)]
pub enum CallError {
    /// Workflow code and History disagree.
    Nondeterminism(String),
    /// A command or a marker that no valid run gives.
    Fatal(String),
    /// The Worker can't serve the call. Another attempt or Worker can, so only the task fails.
    Unserved(String),
}

/// How far a run is in its History.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// A call waits for its result in a marker of History.
    Replaying,
    /// Replay ended, so calls run.
    Live,
    /// The run is on a replay Worker, which makes no live call.
    ReplayOnly,
}

/// A call by its kind and seq. Results of one activation come in this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CallKey {
    Append(u32),
    Close(u32),
}

impl fmt::Display for CallKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallKey::Append(seq) => write!(f, "append {seq}"),
            CallKey::Close(seq) => write!(f, "close {seq}"),
        }
    }
}

struct Append {
    stream: StreamRef,
    state: AppendState,
}

#[allow(clippy::large_enum_variant)] // Each append ends in `Done`
enum AppendState {
    /// In its queue. A replayed append gets its epoch at go-live.
    Waiting {
        bodies: Vec<Payload>,
        sequence: i64,
        epoch: Option<WorkflowTaskEpoch>,
    },
    Running(u64),
    Done(resolve_stream_append::Status),
    /// History has the result, which waits for its activation.
    Replayed,
}

struct Close {
    stream: StreamRef,
    state: CloseState,
}

#[allow(clippy::large_enum_variant)] // Each close ends in `Done`
enum CloseState {
    /// No marker has the intent yet.
    Asked,
    /// The marker of the task has the intent. A replayed close gets its epoch at go-live.
    Sealed(Option<WorkflowTaskEpoch>),
    /// History has the intent, so the close is in its queue.
    Waiting(Option<WorkflowTaskEpoch>),
    Running(u64),
    Done(Option<Failure>),
    /// History has the result, which waits for its activation.
    Replayed,
}

/// The entries of a marker of History whose call Workflow code didn't issue yet, by seq.
#[derive(Default)]
struct Held {
    appends: HashMap<u32, (StreamRef, u64, resolve_stream_append::Status)>,
    intents: HashMap<u32, StreamRef>,
    closes: HashMap<u32, (u64, Option<Failure>)>,
}

/// The Stream calls of one run. A call leaves once its result is in a marker.
pub struct StreamCalls {
    namespace: String,
    run_id: String,
    producer_id: String,
    runtimes: StreamRuntimes,
    phase: Phase,
    gate: RunGate,
    appends: HashMap<u32, Append>,
    closes: HashMap<u32, Close>,
    /// The appends and closes of each Stream, in the order that they joined. The head runs.
    queues: HashMap<(String, String), VecDeque<CallKey>>,
    last_append: u32,
    last_close: u32,
    /// The live calls whose result waits for an activation.
    done: BTreeSet<CallKey>,
    /// The failures of calls that Core refused, for the next activation. No marker has them.
    refused: Vec<workflow_activation_job::Variant>,
    held: Held,
    /// History's results whose call Workflow code issued, by the activation that gets them.
    ready: BTreeMap<(u64, CallKey), workflow_activation_job::Variant>,
    marker: MarkerBuilder,
    requests: Vec<Request>,
    /// Set once the run stops its calls. It leaves the Worker, so a later command records nothing.
    stopped: bool,
}

impl StreamCalls {
    /// The calls of a new run of the namespace `namespace`.
    pub fn new(namespace: &str, run_id: &str, runtimes: StreamRuntimes, phase: Phase) -> Self {
        Self {
            namespace: namespace.to_string(),
            run_id: run_id.to_string(),
            producer_id: format!("wf:{}", quote(run_id)),
            runtimes,
            phase,
            gate: RunGate::new(),
            appends: HashMap::new(),
            closes: HashMap::new(),
            queues: HashMap::new(),
            last_append: 0,
            last_close: 0,
            done: BTreeSet::new(),
            refused: vec![],
            held: Held::default(),
            ready: BTreeMap::new(),
            marker: MarkerBuilder::default(),
            requests: vec![],
            stopped: false,
        }
    }

    /// Sets the task that the run has in hand.
    pub fn set_task(&mut self, state: TaskState) {
        self.gate.set(state);
        self.start_heads();
    }

    /// Takes an append. A call over a limit of its input gets a failure, and no Store gets it.
    pub fn append(&mut self, command: AppendToStream) -> Result<(), CallError> {
        if self.stopped {
            return Ok(());
        }
        let AppendToStream {
            seq,
            stream,
            bodies,
            sequence,
        } = command;
        next_seq("append", seq, &mut self.last_append)?;
        let stream =
            stream.ok_or_else(|| CallError::Fatal(format!("Stream append {seq} has no Stream")))?;
        if let Some(error) = over_limit(&stream) {
            let status = resolve_stream_append::Status::Failed(failure(&error));
            let job = ResolveStreamAppend {
                seq,
                status: Some(status),
            };
            self.refused.push(job.into());
            return Ok(());
        }
        self.check_runtime(&stream)?;
        let epoch = match self.phase {
            Phase::Live => self.gate.epoch(),
            Phase::Replaying | Phase::ReplayOnly => None,
        };
        let state = AppendState::Waiting {
            bodies,
            sequence: if sequence != 0 {
                sequence
            } else {
                i64::from(seq)
            },
            epoch,
        };
        let queue = self.queues.entry(key(&stream)).or_default();
        queue.push_back(CallKey::Append(seq));
        self.appends.insert(seq, Append { stream, state });
        self.match_append(seq)?;
        self.start_heads();
        Ok(())
    }

    /// Takes a close. The marker of the task records its intent, and it runs once History has it.
    pub fn close(&mut self, command: CloseStream) -> Result<(), CallError> {
        if self.stopped {
            return Ok(());
        }
        let CloseStream { seq, stream } = command;
        next_seq("close", seq, &mut self.last_close)?;
        let stream =
            stream.ok_or_else(|| CallError::Fatal(format!("Stream close {seq} has no Stream")))?;
        if let Some(error) = over_limit(&stream) {
            let job = ResolveStreamClose {
                seq,
                failed: Some(failure(&error)),
            };
            self.refused.push(job.into());
            return Ok(());
        }
        self.check_runtime(&stream)?;
        let state = CloseState::Asked;
        self.closes.insert(seq, Close { stream, state });
        self.match_intent(seq)
    }

    /// Takes the result of a call that runs. The result of a call that stopped finds no call.
    pub fn resolve(&mut self, resolution: StreamResolution) {
        let StreamResolution {
            call_id,
            seq,
            outcome,
        } = resolution;
        let (call, stream) = match outcome {
            Outcome::Append(status) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return;
                };
                if !matches!(append.state, AppendState::Running(id) if id == call_id) {
                    return;
                }
                append.state = AppendState::Done(status);
                (CallKey::Append(seq), key(&append.stream))
            }
            Outcome::Close(failure) => {
                let Some(close) = self.closes.get_mut(&seq) else {
                    return;
                };
                if !matches!(close.state, CloseState::Running(id) if id == call_id) {
                    return;
                }
                close.state = CloseState::Done(failure);
                (CallKey::Close(seq), key(&close.stream))
            }
        };
        self.leave_queue(&stream, call);
        self.done.insert(call);
        self.start_heads();
    }

    /// True if activation `activation` of the task gets a result.
    pub fn has_jobs(&self, activation: u64) -> bool {
        !self.refused.is_empty()
            || self
                .ready
                .keys()
                .next()
                .is_some_and(|(at, _)| *at <= activation)
            || self.next_result_fits(activation) == Some(true)
    }

    /// The results that activation `activation` of the task gets. The marker records each live
    /// one with the activation.
    pub fn take_jobs(&mut self, activation: u64) -> Vec<workflow_activation_job::Variant> {
        let mut jobs = std::mem::take(&mut self.refused);
        while let Some(entry) = self.ready.first_entry()
            && entry.key().0 <= activation
        {
            let ((_, call), job) = entry.remove_entry();
            self.leave(call);
            jobs.push(job);
        }
        if !self.gate.is_open() {
            return jobs;
        }
        while let Some(&call) = self.done.first() {
            let Some(job) = self.record(call, activation) else {
                break;
            };
            self.done.remove(&call);
            self.leave(call);
            jobs.push(job);
        }
        jobs
    }

    /// Holds the entries of a marker of History, each for its call. The decode of the marker
    /// checked each index of a Stream.
    pub fn peeked(&mut self, data: StreamMarkerData) -> Result<(), CallError> {
        let StreamMarkerData {
            streams,
            close_intents,
            appends,
            closes,
            reads: _,
        } = data;
        for intent in close_intents {
            let stream = streams[intent.stream as usize].clone();
            if self.held.intents.insert(intent.seq, stream).is_some() {
                return Err(second("intent", CallKey::Close(intent.seq)));
            }
            self.match_intent(intent.seq)?;
        }
        for append in appends {
            let status = match append.result {
                Some(stream_append_result::Result::Appended(offsets)) => {
                    resolve_stream_append::Status::Appended(StreamAppended {
                        first_offset: offsets.first_offset,
                        last_offset: offsets.last_offset,
                    })
                }
                Some(stream_append_result::Result::Failure(failure)) => {
                    resolve_stream_append::Status::Failed(failure)
                }
                None => return Err(CallError::Fatal(format!("{append:?} has no result"))),
            };
            let entry = (
                streams[append.stream as usize].clone(),
                append.activation,
                status,
            );
            if self.held.appends.insert(append.seq, entry).is_some() {
                return Err(second("result", CallKey::Append(append.seq)));
            }
            self.match_append(append.seq)?;
        }
        for close in closes {
            let entry = (close.activation, close.failure);
            if self.held.closes.insert(close.seq, entry).is_some() {
                return Err(second("result", CallKey::Close(close.seq)));
            }
            self.match_result(close.seq)?;
        }
        Ok(())
    }

    /// The event of a marker. Each entry of History's marker reached its call, and the closes of
    /// the marker's intents join their queues.
    pub fn marker_recorded(&mut self) -> Result<(), CallError> {
        let never = |what: &str, call: CallKey| {
            CallError::Nondeterminism(format!(
                "History has {what} of Stream {call} that Workflow code never issued"
            ))
        };
        if let Some(seq) = self.held.intents.keys().min() {
            return Err(never("an intent", CallKey::Close(*seq)));
        }
        if let Some(seq) = self.held.appends.keys().min() {
            return Err(never("a result", CallKey::Append(*seq)));
        }
        if let Some(seq) = self.held.closes.keys().min() {
            return Err(never("a result", CallKey::Close(*seq)));
        }
        if let Some((_, call)) = self.ready.keys().next() {
            return Err(CallError::Nondeterminism(format!(
                "History has a result of Stream {call} that Workflow code never got"
            )));
        }
        let mut sealed: Vec<u32> = (self.closes.iter())
            .filter(|(_, close)| matches!(close.state, CloseState::Sealed(_)))
            .map(|(seq, _)| *seq)
            .collect();
        sealed.sort_unstable();
        for seq in sealed {
            let close = self
                .closes
                .get_mut(&seq)
                .expect("The close was just listed");
            if let CloseState::Sealed(epoch) = close.state {
                close.state = CloseState::Waiting(epoch);
            }
            let queue = self.queues.entry(key(&close.stream)).or_default();
            queue.push_back(CallKey::Close(seq));
        }
        self.start_heads();
        Ok(())
    }

    /// Ends the replay. Each call that waits takes the epoch of the task in hand.
    pub fn go_live(&mut self) -> Result<(), CallError> {
        if self.phase != Phase::Replaying {
            return Ok(());
        }
        let appends = (self.appends.iter()).map(|(seq, append)| (CallKey::Append(*seq), append));
        let unserved = (appends.map(|(call, append)| (call, &append.stream)))
            .chain((self.closes.iter()).map(|(seq, close)| (CallKey::Close(*seq), &close.stream)))
            .filter(|(_, stream)| self.runtimes.get(&stream.store).is_none())
            .min_by_key(|(call, _)| *call);
        if let Some((_, stream)) = unserved {
            return Err(no_runtime(stream));
        }
        self.phase = Phase::Live;
        let epoch = self.gate.epoch();
        for append in self.appends.values_mut() {
            if let AppendState::Waiting { epoch: waiting, .. } = &mut append.state {
                *waiting = epoch;
            }
        }
        for close in self.closes.values_mut() {
            if let CloseState::Sealed(waiting) | CloseState::Waiting(waiting) = &mut close.state {
                *waiting = epoch;
            }
        }
        self.start_heads();
        Ok(())
    }

    /// Stops the appends at a command that ends the Workflow. One that waits is never sent. The
    /// closes go on.
    pub fn stop_for_terminal(&mut self) {
        let mut unsent = HashSet::new();
        self.requests.retain(|request| match request {
            Request::Run(call) if matches!(call.op, Op::Append(_)) => {
                unsent.insert(call.call_id);
                false
            }
            _ => true,
        });
        for append in self.appends.values() {
            if let AppendState::Running(call_id) = append.state
                && !unsent.contains(&call_id)
            {
                self.requests.push(Request::Stop(call_id));
            }
        }
        self.appends.clear();
        self.refused.clear();
        self.done.retain(|call| matches!(call, CallKey::Close(_)));
        self.queues.retain(|_, queue| {
            queue.retain(|call| matches!(call, CallKey::Close(_)));
            !queue.is_empty()
        });
        self.start_heads();
    }

    /// Puts each close result that came in the hold of a terminal command into the marker, with
    /// no job. A replay takes History's results at the activation of the first run.
    pub fn take_for_hold(&mut self, activation: u64) {
        let replayed: Vec<(u64, CallKey)> = (self.ready.keys())
            .filter(|(at, call)| *at <= activation && matches!(call, CallKey::Close(_)))
            .copied()
            .collect();
        for entry in replayed {
            self.ready.remove(&entry);
            self.leave(entry.1);
        }
        let done: Vec<CallKey> = self.done.iter().copied().collect();
        for call in done {
            let CallKey::Close(seq) = call else {
                continue;
            };
            let close = &self.closes[&seq];
            let CloseState::Done(failure) = &close.state else {
                continue;
            };
            if !self.marker.close_result(seq, activation, failure.as_ref()) {
                return;
            }
            if let Some(failure) = failure {
                warn!(
                    run_id = %self.run_id,
                    stream_id = %close.stream.id,
                    store = %close.stream.store,
                    failure = %failure.message,
                    "The Workflow ends, and no Workflow code gets the failure of its Stream close"
                );
            }
            self.done.remove(&call);
            self.leave(call);
        }
    }

    /// True while a close has no result in a marker.
    pub fn has_closes(&self) -> bool {
        !self.closes.is_empty()
    }

    /// True while a call has no result in a marker, so the run needs another task.
    pub fn needs_next_task(&self) -> bool {
        !self.appends.is_empty() || !self.closes.is_empty()
    }

    /// True if the marker of the task has an entry.
    pub fn has_marker(&self) -> bool {
        !self.marker.is_empty()
    }

    /// True once the marker of the task has no room for a result that waits, so the task must
    /// complete with a new one forced.
    pub fn must_complete(&self, activation: u64) -> bool {
        self.marker.is_full() || self.next_result_fits(activation) == Some(false)
    }

    /// The live calls that hold the task open, as a Local Activity does.
    pub fn outstanding(&self) -> usize {
        if self.phase != Phase::Live {
            return 0;
        }
        let appends = (self.appends.values())
            .filter(|append| {
                matches!(
                    append.state,
                    AppendState::Waiting { .. } | AppendState::Running(_)
                )
            })
            .count();
        let closes = (self.closes.values())
            .filter(|close| matches!(close.state, CloseState::Waiting(_) | CloseState::Running(_)))
            .count();
        appends + closes
    }

    /// The activation of History's first result whose call Workflow code issued.
    pub fn next_ready_activation(&self) -> Option<u64> {
        self.ready.keys().next().map(|(at, _)| *at)
    }

    /// The marker of the task as it completes, with the intent of each close that fits.
    pub fn take_marker(&mut self) -> Option<StreamMarkerData> {
        if self.phase != Phase::Live {
            return None;
        }
        let epoch = self.gate.epoch();
        let mut asked: Vec<u32> = (self.closes.iter())
            .filter(|(_, close)| matches!(close.state, CloseState::Asked))
            .map(|(seq, _)| *seq)
            .collect();
        asked.sort_unstable();
        for seq in asked {
            let close = self
                .closes
                .get_mut(&seq)
                .expect("The close was just listed");
            if !self.marker.close_intent(&close.stream, seq) {
                break;
            }
            close.state = CloseState::Sealed(epoch);
        }
        self.marker.take()
    }

    /// The calls that the Worker must start or stop.
    pub fn take_requests(&mut self) -> Vec<Request> {
        std::mem::take(&mut self.requests)
    }

    /// Drops every call and closes the gate, as a failed task does.
    pub fn stop(&mut self) {
        self.stopped = true;
        self.gate.set(TaskState::NoTask);
        self.appends.clear();
        self.closes.clear();
        self.queues.clear();
        self.done.clear();
        self.refused.clear();
        self.held = Held::default();
        self.ready.clear();
        self.marker = MarkerBuilder::default();
        self.requests.clear();
    }

    fn check_runtime(&self, stream: &StreamRef) -> Result<(), CallError> {
        if self.phase == Phase::Live && self.runtimes.get(&stream.store).is_none() {
            return Err(no_runtime(stream));
        }
        Ok(())
    }

    fn match_append(&mut self, seq: u32) -> Result<(), CallError> {
        let Some(append) = self.appends.get_mut(&seq) else {
            return Ok(());
        };
        let Some((stream, activation, status)) = self.held.appends.remove(&seq) else {
            return Ok(());
        };
        check_stream(CallKey::Append(seq), &append.stream, &stream)?;
        if !matches!(append.state, AppendState::Waiting { .. }) {
            return Err(second("result", CallKey::Append(seq)));
        }
        append.state = AppendState::Replayed;
        let job = ResolveStreamAppend {
            seq,
            status: Some(status),
        };
        self.ready
            .insert((activation, CallKey::Append(seq)), job.into());
        Ok(())
    }

    fn match_intent(&mut self, seq: u32) -> Result<(), CallError> {
        let Some(close) = self.closes.get_mut(&seq) else {
            return Ok(());
        };
        let Some(stream) = self.held.intents.remove(&seq) else {
            return Ok(());
        };
        check_stream(CallKey::Close(seq), &close.stream, &stream)?;
        if !matches!(close.state, CloseState::Asked) {
            return Err(second("intent", CallKey::Close(seq)));
        }
        close.state = CloseState::Sealed(None);
        Ok(())
    }

    fn match_result(&mut self, seq: u32) -> Result<(), CallError> {
        let Some(close) = self.closes.get_mut(&seq) else {
            return Ok(());
        };
        let Some((activation, failure)) = self.held.closes.remove(&seq) else {
            return Ok(());
        };
        if !matches!(close.state, CloseState::Waiting(_)) {
            return Err(CallError::Nondeterminism(format!(
                "History has a result of Stream close {seq} with no intent before it, or a \
                 second one"
            )));
        }
        close.state = CloseState::Replayed;
        let job = ResolveStreamClose {
            seq,
            failed: failure,
        };
        self.ready
            .insert((activation, CallKey::Close(seq)), job.into());
        Ok(())
    }

    /// Whether the marker has room for the first result that waits, if the task is open.
    fn next_result_fits(&self, activation: u64) -> Option<bool> {
        let call = *self.done.first()?;
        if !self.gate.is_open() {
            return None;
        }
        match call {
            CallKey::Append(seq) => {
                let append = &self.appends[&seq];
                let AppendState::Done(status) = &append.state else {
                    return None;
                };
                Some((self.marker).has_room_for_append(&append.stream, seq, activation, status))
            }
            CallKey::Close(seq) => {
                let CloseState::Done(failure) = &self.closes[&seq].state else {
                    return None;
                };
                Some((self.marker).has_room_for_close(seq, activation, failure.as_ref()))
            }
        }
    }

    /// Records a live result in the marker. `None` when the marker has no room for it.
    fn record(
        &mut self,
        call: CallKey,
        activation: u64,
    ) -> Option<workflow_activation_job::Variant> {
        match call {
            CallKey::Append(seq) => {
                let append = &self.appends[&seq];
                let AppendState::Done(status) = &append.state else {
                    return None;
                };
                if !self
                    .marker
                    .append_result(&append.stream, seq, activation, status)
                {
                    return None;
                }
                let job = ResolveStreamAppend {
                    seq,
                    status: Some(status.clone()),
                };
                Some(job.into())
            }
            CallKey::Close(seq) => {
                let CloseState::Done(failure) = &self.closes[&seq].state else {
                    return None;
                };
                if !self.marker.close_result(seq, activation, failure.as_ref()) {
                    return None;
                }
                let job = ResolveStreamClose {
                    seq,
                    failed: failure.clone(),
                };
                Some(job.into())
            }
        }
    }

    /// Starts the call at the head of each queue, once the run is live and its task is open.
    fn start_heads(&mut self) {
        if self.phase != Phase::Live || !self.gate.is_open() {
            return;
        }
        let heads: Vec<CallKey> = (self.queues.values())
            .filter_map(|queue| queue.front().copied())
            .collect();
        for call in heads {
            self.start(call);
        }
    }

    fn start(&mut self, call: CallKey) {
        let call_id = next_call_id();
        let (seq, op) = match call {
            CallKey::Append(seq) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return;
                };
                let AppendState::Waiting {
                    epoch: Some(epoch), ..
                } = append.state
                else {
                    return;
                };
                let running = AppendState::Running(call_id);
                let AppendState::Waiting {
                    bodies, sequence, ..
                } = std::mem::replace(&mut append.state, running)
                else {
                    return;
                };
                let request = AppendRecordsRequest {
                    namespace: self.namespace.clone(),
                    r#ref: Some(append.stream.clone()),
                    producer: Some(producer(&self.producer_id, &self.run_id, epoch)),
                    sequence,
                    records: (bodies.into_iter())
                        .map(|body| AppendRecord { body: Some(body) })
                        .collect(),
                };
                (seq, Op::Append(request))
            }
            CallKey::Close(seq) => {
                let Some(close) = self.closes.get_mut(&seq) else {
                    return;
                };
                let CloseState::Waiting(Some(epoch)) = close.state else {
                    return;
                };
                close.state = CloseState::Running(call_id);
                let request = CloseStreamRequest {
                    namespace: self.namespace.clone(),
                    r#ref: Some(close.stream.clone()),
                    producer: Some(producer(&self.producer_id, &self.run_id, epoch)),
                };
                (seq, Op::Close(request))
            }
        };
        self.requests.push(Request::Run(Call { call_id, seq, op }));
    }

    fn leave(&mut self, call: CallKey) {
        let stream = match call {
            CallKey::Append(seq) => self.appends.remove(&seq).map(|append| append.stream),
            CallKey::Close(seq) => self.closes.remove(&seq).map(|close| close.stream),
        };
        if let Some(stream) = stream {
            self.leave_queue(&key(&stream), call);
        }
    }

    fn leave_queue(&mut self, stream: &(String, String), call: CallKey) {
        let Some(queue) = self.queues.get_mut(stream) else {
            return;
        };
        if queue.front() == Some(&call) {
            queue.pop_front();
        } else {
            queue.retain(|queued| *queued != call);
        }
        if queue.is_empty() {
            self.queues.remove(stream);
        }
    }
}

fn next_seq(kind: &str, seq: u32, last: &mut u32) -> Result<(), CallError> {
    if seq == 0 {
        return Err(CallError::Fatal(format!(
            "Stream {kind} has seq 0, and a seq starts at 1"
        )));
    }
    if seq <= *last {
        return Err(CallError::Fatal(format!(
            "Stream {kind} {seq} came after {kind} {last}"
        )));
    }
    *last = seq;
    Ok(())
}

fn over_limit(stream: &StreamRef) -> Option<StreamError> {
    check_stream_id(&stream.id)
        .and_then(|()| check_store_name(&stream.store))
        .err()
}

fn no_runtime(stream: &StreamRef) -> CallError {
    CallError::Unserved(format!(
        "This Worker has no Stream runtime for the Store `{}`",
        stream.store
    ))
}

/// A call with no Store name matches any Store, since a lang with no Stream config sends none.
fn check_stream(call: CallKey, sent: &StreamRef, history: &StreamRef) -> Result<(), CallError> {
    if sent.id == history.id && (sent.store.is_empty() || sent.store == history.store) {
        return Ok(());
    }
    Err(CallError::Nondeterminism(format!(
        "Stream {call} is for Stream `{}` in Store `{}`, and History has it for Stream `{}` in \
         Store `{}`",
        sent.id, sent.store, history.id, history.store
    )))
}

fn second(what: &str, call: CallKey) -> CallError {
    CallError::Nondeterminism(format!("History has a second {what} of Stream {call}"))
}

fn producer(id: &str, run_id: &str, epoch: WorkflowTaskEpoch) -> Producer {
    Producer {
        id: id.to_string(),
        kind: Some(Kind::Workflow(WorkflowProducer {
            run_id: run_id.to_string(),
            epoch: Some(epoch),
        })),
    }
}

fn key(stream: &StreamRef) -> (String, String) {
    (stream.store.clone(), stream.id.clone())
}

/// `part` quoted as Python's `urllib.parse.quote(part, safe="")` quotes it, so each SDK builds
/// the same Producer id for a run.
fn quote(part: &str) -> String {
    let mut quoted = String::with_capacity(part.len());
    for byte in part.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'~') {
            quoted.push(byte as char);
        } else {
            quoted.push_str(&format!("%{byte:02X}"));
        }
    }
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        StreamRuntime,
        record::BatchLimits,
        test_store::Scripted,
        workflow::marker::{self, MAX_MARKER_ENTRY_BYTES},
    };
    use prost::Message;
    use std::sync::Arc;
    use workflow_activation_job::Variant;

    fn calls(phase: Phase, task: i64) -> StreamCalls {
        let runtime = StreamRuntime::new(Arc::new(Scripted::default()), BatchLimits::default());
        let runtimes = StreamRuntimes::new([("mem".to_string(), Arc::new(runtime))]);
        let mut calls = StreamCalls::new("ns", "r", runtimes, phase);
        calls.set_task(TaskState::Open(epoch(task)));
        calls
    }

    /// The run completes its task, and the next one opens.
    fn next_task(calls: &mut StreamCalls, task: i64) -> Option<StreamMarkerData> {
        let marker = calls.take_marker();
        calls.set_task(TaskState::NoTask);
        calls.set_task(TaskState::Open(epoch(task)));
        marker
    }

    fn epoch(task: i64) -> WorkflowTaskEpoch {
        WorkflowTaskEpoch {
            started_event_id: task,
            attempt: 1,
            started_time: None,
        }
    }

    fn stream(id: &str) -> StreamRef {
        StreamRef {
            id: id.to_string(),
            store: "mem".to_string(),
        }
    }

    fn append(seq: u32, id: &str) -> AppendToStream {
        AppendToStream {
            seq,
            stream: Some(stream(id)),
            bodies: vec![Payload::default()],
            sequence: 0,
        }
    }

    fn close(seq: u32, id: &str) -> CloseStream {
        CloseStream {
            seq,
            stream: Some(stream(id)),
        }
    }

    fn appended(seq: u32) -> resolve_stream_append::Status {
        resolve_stream_append::Status::Appended(StreamAppended {
            first_offset: vec![seq as u8],
            last_offset: vec![seq as u8],
        })
    }

    fn history(write: impl FnOnce(&mut marker::MarkerBuilder)) -> StreamMarkerData {
        let mut marker = marker::MarkerBuilder::default();
        write(&mut marker);
        marker.take().unwrap()
    }

    /// The calls that started since the last look, as `append 1 of s at 3`, with the started
    /// event id of the epoch.
    fn started(calls: &mut StreamCalls) -> Vec<String> {
        let mut started: Vec<String> = (calls.take_requests().into_iter())
            .map(|request| {
                let (call, stream, producer) = match request {
                    Request::Run(Call { seq, op, .. }) => match op {
                        Op::Append(r) => (CallKey::Append(seq), r.r#ref, r.producer),
                        Op::Close(r) => (CallKey::Close(seq), r.r#ref, r.producer),
                    },
                    Request::Stop(_) => return "stop".to_string(),
                };
                let Some(Kind::Workflow(workflow)) = producer.unwrap().kind else {
                    panic!("{call} has no Workflow Producer");
                };
                let at = workflow.epoch.unwrap().started_event_id;
                format!("{call} of {} at {at}", stream.unwrap().id)
            })
            .collect();
        started.sort();
        started
    }

    /// Gives a call that runs its result, a failure with `message` if there is one.
    fn finish(calls: &mut StreamCalls, call: CallKey, message: Option<&str>) {
        let failed = message.map(|message| failure(&StreamError::invalid(message)));
        let (call_id, seq, outcome) = match call {
            CallKey::Append(seq) => {
                let AppendState::Running(call_id) = calls.appends[&seq].state else {
                    panic!("{call} doesn't run");
                };
                let status = match failed {
                    Some(failure) => resolve_stream_append::Status::Failed(failure),
                    None => appended(seq),
                };
                (call_id, seq, Outcome::Append(status))
            }
            CallKey::Close(seq) => {
                let CloseState::Running(call_id) = calls.closes[&seq].state else {
                    panic!("{call} doesn't run");
                };
                (call_id, seq, Outcome::Close(failed))
            }
        };
        calls.resolve(StreamResolution {
            call_id,
            seq,
            outcome,
        });
    }

    /// The results of activation `activation`, as `append 1` or `close 2: <failure>`.
    fn jobs(calls: &mut StreamCalls, activation: u64) -> Vec<String> {
        (calls.take_jobs(activation).into_iter())
            .map(|job| match job {
                Variant::ResolveStreamAppend(ResolveStreamAppend {
                    seq,
                    status: Some(resolve_stream_append::Status::Failed(failure)),
                }) => format!("append {seq}: {}", failure.message),
                Variant::ResolveStreamAppend(append) => format!("append {}", append.seq),
                Variant::ResolveStreamClose(ResolveStreamClose {
                    seq,
                    failed: Some(failure),
                }) => format!("close {seq}: {}", failure.message),
                Variant::ResolveStreamClose(close) => format!("close {}", close.seq),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn calls_to_one_stream_run_one_at_a_time_in_queue_order() {
        let mut live = calls(Phase::Live, 3);
        live.append(append(1, "s")).unwrap();
        live.close(close(1, "s")).unwrap();
        live.append(append(2, "s")).unwrap();
        live.append(append(3, "t")).unwrap();
        assert_eq!(
            started(&mut live),
            ["append 1 of s at 3", "append 3 of t at 3"]
        );
        finish(&mut live, CallKey::Append(1), None);
        assert_eq!(started(&mut live), ["append 2 of s at 3"]);
        assert_eq!(jobs(&mut live, 1), ["append 1"]);
        let marker = next_task(&mut live, 7).unwrap();
        live.marker_recorded().unwrap();
        assert_eq!(started(&mut live), Vec::<String>::new());
        finish(&mut live, CallKey::Append(2), None);
        assert_eq!(started(&mut live), ["close 1 of s at 3"]);

        let mut replay = calls(Phase::Replaying, 9);
        replay.append(append(1, "s")).unwrap();
        replay.close(close(1, "s")).unwrap();
        replay.append(append(2, "s")).unwrap();
        replay.append(append(3, "t")).unwrap();
        replay.peeked(marker).unwrap();
        assert_eq!(jobs(&mut replay, 1), ["append 1"]);
        replay.marker_recorded().unwrap();
        replay.go_live().unwrap();
        assert_eq!(
            started(&mut replay),
            ["append 2 of s at 9", "append 3 of t at 9"]
        );
        finish(&mut replay, CallKey::Append(2), None);
        assert_eq!(started(&mut replay), ["close 1 of s at 9"]);
    }

    #[test]
    fn an_append_sends_lang_sequence_or_its_seq() {
        let mut calls = calls(Phase::Live, 3);
        calls.append(append(1, "s")).unwrap();
        let batch = AppendToStream {
            sequence: 77,
            ..append(2, "t")
        };
        calls.append(batch).unwrap();
        let request = |id: &str, sequence| AppendRecordsRequest {
            namespace: "ns".to_string(),
            r#ref: Some(stream(id)),
            producer: Some(producer("wf:r", "r", epoch(3))),
            sequence,
            records: vec![AppendRecord {
                body: Some(Payload::default()),
            }],
        };
        let mut sent: Vec<_> = (calls.take_requests().into_iter())
            .map(|request| match request {
                Request::Run(Call {
                    op: Op::Append(request),
                    ..
                }) => request,
                other => panic!("{other:?}"),
            })
            .collect();
        sent.sort_by_key(|request| request.sequence);
        assert_eq!(sent, [request("s", 1), request("t", 77)]);
    }

    #[test]
    fn a_seq_of_zero_or_one_that_came_before_is_fatal() {
        let mut calls = calls(Phase::Live, 3);
        calls.append(append(2, "s")).unwrap();
        calls.close(close(2, "s")).unwrap();
        let cases = [
            (
                calls.append(append(0, "s")),
                "append has seq 0, and a seq starts at 1",
            ),
            (calls.append(append(2, "s")), "append 2 came after append 2"),
            (calls.append(append(1, "s")), "append 1 came after append 2"),
            (
                calls.close(close(0, "s")),
                "close has seq 0, and a seq starts at 1",
            ),
            (calls.close(close(1, "s")), "close 1 came after close 2"),
        ];
        for (result, message) in cases {
            assert_eq!(result, Err(CallError::Fatal(format!("Stream {message}"))));
        }
    }

    #[test]
    fn a_live_call_keeps_the_epoch_of_the_task_in_hand() {
        let mut calls = calls(Phase::Live, 3);
        calls.append(append(1, "s")).unwrap();
        calls.append(append(2, "s")).unwrap();
        calls.close(close(1, "s")).unwrap();
        assert_eq!(started(&mut calls), ["append 1 of s at 3"]);
        // Closes on Streams with long ids fill the marker, so the last intent waits for task 7.
        let long = |seq: u32| format!("{seq:02}{}", "s".repeat(4000));
        for seq in 2..=70 {
            calls.close(close(seq, &long(seq))).unwrap();
        }
        let first = next_task(&mut calls, 7).unwrap();
        let sealed = first.close_intents.len() as u32;
        assert!(sealed < 70, "{sealed}");
        calls.marker_recorded().unwrap();
        finish(&mut calls, CallKey::Append(1), None);
        let mut expected = vec!["append 2 of s at 3".to_string()];
        expected.extend((2..=sealed).map(|seq| format!("close {seq} of {} at 3", long(seq))));
        expected.sort();
        assert_eq!(started(&mut calls), expected);
        finish(&mut calls, CallKey::Append(2), None);
        assert_eq!(started(&mut calls), ["close 1 of s at 3"]);
        next_task(&mut calls, 11).unwrap();
        calls.marker_recorded().unwrap();
        let rest = started(&mut calls);
        assert_eq!(rest.len(), 70 - sealed as usize);
        assert!(rest.iter().all(|call| call.ends_with(" at 7")), "{rest:?}");
    }

    #[test]
    fn a_call_that_a_replay_leaves_waiting_takes_the_go_live_epoch() {
        let mut replay = calls(Phase::Replaying, 9);
        replay.append(append(1, "s")).unwrap();
        replay.append(append(2, "t")).unwrap();
        replay.close(close(1, "u")).unwrap();
        replay
            .peeked(history(|marker| {
                assert!(marker.close_intent(&stream("u"), 1));
                assert!(marker.append_result(&stream("s"), 1, 0, &appended(1)));
            }))
            .unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
        replay.marker_recorded().unwrap();
        assert_eq!(started(&mut replay), Vec::<String>::new());
        replay.go_live().unwrap();
        assert_eq!(
            started(&mut replay),
            ["append 2 of t at 9", "close 1 of u at 9"]
        );
    }

    #[test]
    fn a_close_runs_only_once_history_has_its_intent() {
        let mut live = calls(Phase::Live, 3);
        live.close(close(1, "s")).unwrap();
        let marker = next_task(&mut live, 7).unwrap();
        assert_eq!(started(&mut live), Vec::<String>::new());
        live.marker_recorded().unwrap();
        assert_eq!(started(&mut live), ["close 1 of s at 3"]);

        // A replayed close with no intent in History waits for the marker of the live task.
        let mut replay = calls(Phase::Replaying, 9);
        replay.close(close(1, "s")).unwrap();
        replay.close(close(2, "t")).unwrap();
        replay.peeked(marker).unwrap();
        replay.marker_recorded().unwrap();
        replay.go_live().unwrap();
        assert_eq!(started(&mut replay), ["close 1 of s at 9"]);
        next_task(&mut replay, 13).unwrap();
        assert_eq!(started(&mut replay), Vec::<String>::new());
        replay.marker_recorded().unwrap();
        assert_eq!(started(&mut replay), ["close 2 of t at 9"]);
    }

    #[test]
    fn a_result_waits_with_its_call_until_an_activation_takes_it() {
        let mut live = calls(Phase::Live, 3);
        live.append(append(1, "s")).unwrap();
        live.append(append(2, "t")).unwrap();
        started(&mut live);
        finish(&mut live, CallKey::Append(2), None);
        finish(&mut live, CallKey::Append(1), None);
        live.set_task(TaskState::Completing(epoch(3)));
        assert!(!live.has_jobs(4));
        assert_eq!(jobs(&mut live, 4), Vec::<String>::new());
        let marker = next_task(&mut live, 7);
        assert_eq!(marker, None);
        assert!(live.has_jobs(0));
        assert_eq!(jobs(&mut live, 0), ["append 1", "append 2"]);
        let marker = next_task(&mut live, 11).unwrap();
        let entries: Vec<_> = (marker.appends.iter())
            .map(|append| (append.seq, append.activation))
            .collect();
        assert_eq!(entries, [(1, 0), (2, 0)]);

        let mut replay = calls(Phase::Replaying, 13);
        replay.append(append(1, "s")).unwrap();
        replay.append(append(2, "t")).unwrap();
        replay.peeked(marker).unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1", "append 2"]);
    }

    #[test]
    fn a_replayed_result_comes_in_its_activation() {
        let mut replay = calls(Phase::Replaying, 9);
        replay.append(append(1, "s")).unwrap();
        replay
            .peeked(history(|marker| {
                assert!(marker.append_result(&stream("s"), 1, 2, &appended(1)));
                assert!(marker.append_result(&stream("s"), 2, 3, &appended(2)));
            }))
            .unwrap();
        assert!(!replay.has_jobs(1));
        assert_eq!(replay.next_ready_activation(), Some(2));
        assert_eq!(jobs(&mut replay, 2), ["append 1"]);
        replay.append(append(2, "s")).unwrap();
        assert_eq!(jobs(&mut replay, 2), Vec::<String>::new());
        assert_eq!(jobs(&mut replay, 3), ["append 2"]);
        replay.append(append(3, "s")).unwrap();
        replay.marker_recorded().unwrap();
        replay.go_live().unwrap();
        assert_eq!(started(&mut replay), ["append 3 of s at 9"]);
    }

    #[test]
    fn changed_workflow_code_is_a_nondeterminism_error() {
        let result_of =
            |seq| history(|m| assert!(m.append_result(&stream("s"), seq, 0, &appended(seq))));
        let intent_of = |seq| history(|m| assert!(m.close_intent(&stream("s"), seq)));
        let close_result = || history(|m| assert!(m.close_result(1, 0, None)));
        type Case = Box<dyn Fn(&mut StreamCalls) -> Result<(), CallError>>;
        let cases: [(Case, &str); 8] = [
            (
                Box::new(move |c| {
                    c.peeked(result_of(1))?;
                    c.marker_recorded()
                }),
                "History has a result of Stream append 1 that Workflow code never issued",
            ),
            (
                Box::new(move |c| {
                    c.peeked(intent_of(1))?;
                    c.marker_recorded()
                }),
                "History has an intent of Stream close 1 that Workflow code never issued",
            ),
            (
                Box::new(move |c| {
                    c.append(append(1, "t"))?;
                    c.peeked(result_of(1))
                }),
                "Stream append 1 is for Stream `t` in Store `mem`, and History has it for Stream \
                 `s` in Store `mem`",
            ),
            (
                Box::new(move |c| {
                    c.close(close(1, "t"))?;
                    c.peeked(intent_of(1))
                }),
                "Stream close 1 is for Stream `t` in Store `mem`, and History has it for Stream \
                 `s` in Store `mem`",
            ),
            (
                Box::new(move |c| {
                    c.append(append(1, "s"))?;
                    c.peeked(result_of(1))?;
                    c.peeked(result_of(1))
                }),
                "History has a second result of Stream append 1",
            ),
            (
                Box::new(move |c| {
                    c.close(close(1, "s"))?;
                    c.peeked(intent_of(1))?;
                    c.peeked(intent_of(1))
                }),
                "History has a second intent of Stream close 1",
            ),
            (
                Box::new(move |c| {
                    c.close(close(1, "s"))?;
                    c.peeked(close_result())
                }),
                "History has a result of Stream close 1 with no intent before it, or a second one",
            ),
            (
                Box::new(move |c| {
                    c.append(append(1, "s"))?;
                    c.peeked(history(|m| {
                        assert!(m.append_result(&stream("s"), 1, 5, &appended(1)))
                    }))?;
                    c.marker_recorded()
                }),
                "History has a result of Stream append 1 that Workflow code never got",
            ),
        ];
        for (case, message) in cases {
            let error = CallError::Nondeterminism(message.to_string());
            assert_eq!(case(&mut calls(Phase::Replaying, 9)), Err(error));
        }
        let mut lang_with_no_config = calls(Phase::ReplayOnly, 9);
        let mut nameless = append(1, "s");
        nameless.stream.as_mut().unwrap().store.clear();
        lang_with_no_config.append(nameless).unwrap();
        lang_with_no_config.peeked(result_of(1)).unwrap();
        assert_eq!(jobs(&mut lang_with_no_config, 0), ["append 1"]);
    }

    #[test]
    fn a_call_over_its_input_limit_is_refused_with_no_request() {
        for phase in [Phase::Live, Phase::Replaying] {
            let mut calls = calls(phase, 3);
            calls.append(append(1, &"s".repeat(4097))).unwrap();
            calls.append(append(2, "")).unwrap();
            let mut long_store = close(1, "s");
            long_store.stream.as_mut().unwrap().store = "m".repeat(4097);
            calls.close(long_store).unwrap();
            assert_eq!(
                jobs(&mut calls, 0),
                [
                    "append 1: stream_invalid: The Stream id has 4097 bytes, more than 4096",
                    "append 2: stream_invalid: The Stream id is empty",
                    "close 1: stream_invalid: The Store name has 4097 bytes, more than 4096",
                ]
            );
            calls.go_live().unwrap();
            assert_eq!(calls.take_requests().len(), 0);
            assert!(!calls.needs_next_task());
            assert_eq!(calls.take_marker(), None);
        }
    }

    #[test]
    fn a_call_on_a_store_with_no_runtime_fails_the_task() {
        let mut elsewhere = append(1, "s");
        elsewhere.stream.as_mut().unwrap().store = "other".to_string();
        let unserved = CallError::Unserved(
            "This Worker has no Stream runtime for the Store `other`".to_string(),
        );
        let mut live = calls(Phase::Live, 3);
        assert_eq!(live.append(elsewhere.clone()), Err(unserved.clone()));
        let close = CloseStream {
            seq: 1,
            stream: elsewhere.stream.clone(),
        };
        assert_eq!(live.close(close), Err(unserved.clone()));
        assert!(!live.needs_next_task());
        let mut replay = calls(Phase::Replaying, 9);
        replay.append(elsewhere).unwrap();
        assert_eq!(replay.go_live(), Err(unserved));
    }

    #[test]
    fn a_terminal_command_waits_while_a_close_has_no_result() {
        let mut calls = calls(Phase::Live, 3);
        for (seq, id) in [(1, "s"), (2, "t")] {
            calls.close(close(seq, id)).unwrap();
        }
        next_task(&mut calls, 7);
        calls.marker_recorded().unwrap();
        calls.close(close(3, "u")).unwrap();
        started(&mut calls);
        finish(&mut calls, CallKey::Close(1), None);
        assert_eq!(jobs(&mut calls, 0), ["close 1"]);
        finish(&mut calls, CallKey::Close(2), None);
        // The result of close 2 goes into the marker with no job, and close 3 has no intent yet.
        calls.take_for_hold(1);
        assert!(calls.has_closes());
        assert_eq!(jobs(&mut calls, 1), Vec::<String>::new());
        let closes: Vec<_> = (next_task(&mut calls, 11).unwrap().closes.iter())
            .map(|close| (close.seq, close.activation))
            .collect();
        assert_eq!(closes, [(1, 0), (2, 1)]);
        calls.marker_recorded().unwrap();
        started(&mut calls);
        finish(&mut calls, CallKey::Close(3), None);
        calls.take_for_hold(0);
        assert!(!calls.has_closes());
    }

    #[test]
    fn a_terminal_command_stops_appends_and_keeps_closes() {
        let mut calls = calls(Phase::Live, 3);
        calls.append(append(1, "s")).unwrap();
        calls.close(close(1, "s")).unwrap();
        next_task(&mut calls, 7);
        calls.marker_recorded().unwrap();
        calls.append(append(2, "s")).unwrap();
        assert_eq!(started(&mut calls), ["append 1 of s at 3"]);
        calls.append(append(3, "t")).unwrap();
        calls.append(append(4, &"s".repeat(4097))).unwrap();
        calls.stop_for_terminal();
        assert_eq!(started(&mut calls), ["close 1 of s at 3", "stop"]);
        assert!(calls.has_closes() && calls.appends.is_empty());
        // No code gets the failure of a refused call after the command.
        assert!(!calls.has_jobs(1));
    }

    #[test]
    fn the_hold_takes_a_close_result_with_no_job() {
        let mut live = calls(Phase::Live, 3);
        live.close(close(1, "s")).unwrap();
        let intent = next_task(&mut live, 7).unwrap();
        live.marker_recorded().unwrap();
        started(&mut live);
        live.take_for_hold(2);
        assert!(live.has_closes());
        finish(&mut live, CallKey::Close(1), Some("The Store refused"));
        live.take_for_hold(2);
        assert!(!live.has_closes());
        assert_eq!(jobs(&mut live, 2), Vec::<String>::new());
        let result = next_task(&mut live, 11).unwrap();
        assert_eq!(result.closes[0].activation, 2);
        assert!(result.closes[0].failure.is_some());

        let mut replay = calls(Phase::Replaying, 13);
        replay.close(close(1, "s")).unwrap();
        replay.peeked(intent).unwrap();
        replay.marker_recorded().unwrap();
        replay.peeked(result).unwrap();
        replay.take_for_hold(1);
        assert!(replay.has_closes());
        replay.take_for_hold(2);
        assert!(!replay.has_closes());
        replay.marker_recorded().unwrap();
    }

    #[test]
    fn a_call_leaves_the_run_once_its_result_is_in_a_marker() {
        let mut live = calls(Phase::Live, 3);
        let mut replay = calls(Phase::Replaying, 9);
        for seq in 1..=100 {
            live.append(append(seq, "s")).unwrap();
            started(&mut live);
            finish(&mut live, CallKey::Append(seq), None);
            assert_eq!(jobs(&mut live, u64::from(seq)), [format!("append {seq}")]);
            replay.append(append(seq, "s")).unwrap();
        }
        replay.peeked(next_task(&mut live, 7).unwrap()).unwrap();
        assert_eq!(jobs(&mut replay, 100).len(), 100);
        replay.marker_recorded().unwrap();
        for calls in [&live, &replay] {
            assert!(calls.appends.is_empty() && calls.queues.is_empty());
            assert!(calls.done.is_empty() && calls.ready.is_empty());
        }
    }

    #[test]
    fn a_marker_at_its_entry_bound_completes_the_task() {
        let message = "x".repeat(4000);
        // Results come one at a time. The one that doesn't fit gives no job and completes the task.
        let mut one_at_a_time = calls(Phase::Live, 3);
        let mut seq = 0;
        loop {
            seq += 1;
            one_at_a_time.append(append(seq, "s")).unwrap();
            started(&mut one_at_a_time);
            finish(&mut one_at_a_time, CallKey::Append(seq), Some(&message));
            if !one_at_a_time.has_jobs(seq.into()) {
                break;
            }
            assert!(!one_at_a_time.must_complete(seq.into()));
            assert_eq!(jobs(&mut one_at_a_time, seq.into()).len(), 1);
        }
        assert!(seq > 1 && one_at_a_time.must_complete(seq.into()));
        assert_eq!(jobs(&mut one_at_a_time, seq.into()), Vec::<String>::new());

        let mut calls = calls(Phase::Live, 3);
        for seq in 1..=70 {
            calls.append(append(seq, "s")).unwrap();
            started(&mut calls);
            finish(&mut calls, CallKey::Append(seq), Some(&message));
        }
        calls.close(close(1, "s")).unwrap();
        let taken = jobs(&mut calls, 0).len() as u32;
        assert!(calls.must_complete(1) && !calls.has_jobs(1));
        let first = next_task(&mut calls, 7).unwrap();
        let size = first.encoded_len();
        assert!(size <= MAX_MARKER_ENTRY_BYTES && size + 8192 > MAX_MARKER_ENTRY_BYTES);
        assert!(first.close_intents.is_empty());
        let rest: Vec<String> = (taken + 1..=70)
            .map(|seq| format!("append {seq}: stream_invalid: {message}"))
            .collect();
        assert_eq!(jobs(&mut calls, 0), rest);
        let second = next_task(&mut calls, 11).unwrap();
        assert_eq!(second.close_intents.len(), 1);

        // The hold puts close results in the marker by the same bound.
        calls.marker_recorded().unwrap();
        for seq in 2..=70 {
            calls.close(close(seq, &format!("s{seq}"))).unwrap();
        }
        next_task(&mut calls, 13);
        calls.marker_recorded().unwrap();
        started(&mut calls);
        for seq in 1..=70 {
            finish(&mut calls, CallKey::Close(seq), Some(&message));
        }
        calls.take_for_hold(0);
        assert!(calls.must_complete(0) && calls.has_closes());
        let held = next_task(&mut calls, 17).unwrap();
        assert!(held.encoded_len() <= MAX_MARKER_ENTRY_BYTES);
        calls.take_for_hold(0);
        assert!(!calls.has_closes());
        let closes = held.closes.len() + next_task(&mut calls, 19).unwrap().closes.len();
        assert_eq!(closes, 70);
    }

    #[test]
    fn the_producer_id_quotes_the_run_id_as_python_does() {
        let quoted = [
            (
                "0193c2e4-7f3a-7c1d-9b2e-1a2b3c4d5e6f",
                "0193c2e4-7f3a-7c1d-9b2e-1a2b3c4d5e6f",
            ),
            ("a/b c?d=e&f#%", "a%2Fb%20c%3Fd%3De%26f%23%25"),
            ("ré~_.-+:|", "r%C3%A9~_.-%2B%3A%7C"),
            ("\u{0}\u{7f}!*()", "%00%7F%21%2A%28%29"),
        ];
        for (run_id, id) in quoted {
            assert_eq!(quote(run_id), id);
        }
    }
}
