//! The Stream calls of one run. Nothing here does I/O. The machines of the run drive the calls,
//! and the Worker's [`StreamManager`](super::StreamManager) runs the requests that they start.

use super::{
    RunGate, StreamRuntimes, TaskState,
    manager::{Call, Op, Outcome, Request, StreamResolution, failure, next_call_id},
    marker::{Done, MarkerBuilder, status_of},
};
use crate::{
    StreamError,
    ids::{check_ref, check_store_name},
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
        external_data::{StreamAppendResult, StreamCloseResult, StreamMarkerData},
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, resolve_stream_append, workflow_activation_job,
        },
        workflow_commands::{AppendToStream, CloseStream},
    },
    temporal::api::{common::v1::Payload, failure::v1::Failure},
};
use tracing::warn;

/// The reason a Stream call fails its Workflow Task.
#[derive(Clone, Debug, PartialEq)]
pub enum CallError {
    /// Workflow code and History disagree.
    Nondeterminism(String),
    /// A command or a marker that no valid run produces.
    Fatal(String),
    /// This Worker can't serve the call. Another attempt or another Worker can, so only the task
    /// fails.
    Unserved(String),
}

/// How far a run is through its History.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// The run replays History, so each call waits for its result in a marker.
    Replaying,
    /// The replay is over, so calls run.
    Live,
    /// The run is on a replay Worker, which makes no live calls.
    ReplayOnly,
    /// The run stopped its calls and leaves the Worker, so it records no new call.
    Stopped,
}

/// A call by its kind and seq. The results of one activation come in this order.
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
    /// The append is in its queue. A replayed one gets its epoch at go-live.
    Waiting {
        bodies: Vec<Payload>,
        sequence: i64,
        epoch: Option<WorkflowTaskEpoch>,
    },
    Running(u64),
    /// The result waits for an activation, in the form that the marker records it.
    Done(Done<StreamAppendResult>),
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
    /// The result waits for an activation, in the form that the marker records it.
    Done(Done<StreamCloseResult>),
    /// History has the result, which waits for its activation.
    Replayed,
}

/// The entries of History's markers whose calls Workflow code hasn't issued yet, by seq.
#[derive(Default)]
struct Held {
    appends: HashMap<u32, (StreamRef, u64, resolve_stream_append::Status)>,
    intents: HashMap<u32, StreamRef>,
    closes: HashMap<u32, (u64, Option<Failure>)>,
}

/// The Stream calls of one run. A call leaves the run once its result is in a marker.
pub struct StreamCalls {
    namespace: String,
    run_id: String,
    producer_id: String,
    runtimes: StreamRuntimes,
    phase: Phase,
    gate: RunGate,
    appends: HashMap<u32, Append>,
    closes: HashMap<u32, Close>,
    /// The appends and closes of each Stream, in the order that they joined. Only the head runs.
    queues: HashMap<(String, String), VecDeque<CallKey>>,
    last_append: u32,
    last_close: u32,
    /// The live calls whose result waits for an activation.
    done: BTreeSet<CallKey>,
    /// The failures of calls that Core refused, for the next activation. No marker records them.
    refused: Vec<workflow_activation_job::Variant>,
    held: Held,
    /// History's results whose calls Workflow code issued, by the activation that gets them.
    ready: BTreeMap<(u64, CallKey), workflow_activation_job::Variant>,
    marker: MarkerBuilder,
    requests: Vec<Request>,
}

impl StreamCalls {
    /// Creates the calls of a new run in `namespace`.
    pub fn new(namespace: &str, run_id: &str, runtimes: StreamRuntimes, phase: Phase) -> Self {
        debug_assert_ne!(phase, Phase::Stopped);
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
        }
    }

    /// Sets the task that the run has in hand. When the task opens, the head of each queue starts.
    pub fn set_task(&mut self, state: TaskState) {
        self.gate.set(state);
        self.start_heads();
    }

    /// Takes an append command. If an input is over its limit, Workflow code gets a failure, and no
    /// Store sees the call.
    pub fn append(&mut self, command: AppendToStream) -> Result<(), CallError> {
        if self.phase == Phase::Stopped {
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
            Phase::Replaying | Phase::ReplayOnly | Phase::Stopped => None,
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
        let queue = key(&stream);
        self.queues
            .entry(queue.clone())
            .or_default()
            .push_back(CallKey::Append(seq));
        self.appends.insert(seq, Append { stream, state });
        self.match_append(seq)?;
        self.start_head(&queue);
        Ok(())
    }

    /// Takes a close command. The marker of the task records its intent, and the close runs once
    /// History has that intent.
    pub fn close(&mut self, command: CloseStream) -> Result<(), CallError> {
        if self.phase == Phase::Stopped {
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

    /// Takes the result of a running call, and starts the next call of its Stream. The result of
    /// a call that was stopped finds no call.
    pub fn resolve(&mut self, resolution: StreamResolution) {
        let StreamResolution {
            call_id,
            seq,
            outcome,
        } = resolution;
        let (call, queue) = match outcome {
            Outcome::Append(status) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return;
                };
                if !matches!(append.state, AppendState::Running(id) if id == call_id) {
                    return;
                }
                append.state = AppendState::Done(Done::append(seq, status));
                (CallKey::Append(seq), key(&append.stream))
            }
            Outcome::Close(failure) => {
                let Some(close) = self.closes.get_mut(&seq) else {
                    return;
                };
                if !matches!(close.state, CloseState::Running(id) if id == call_id) {
                    return;
                }
                close.state = CloseState::Done(Done::close(seq, failure));
                (CallKey::Close(seq), key(&close.stream))
            }
        };
        self.done.insert(call);
        self.leave_queue(&queue, call);
    }

    /// Returns true if the activation at index `activation` of the task gets a result.
    pub fn has_jobs(&self, activation: u64) -> bool {
        !self.refused.is_empty()
            || self
                .ready
                .keys()
                .next()
                .is_some_and(|(at, _)| *at <= activation)
            || self.next_result_fits(activation) == Some(true)
    }

    /// Returns the results for the activation at index `activation` of the task. The marker
    /// records each live result with that index.
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

    /// Holds the entries of a marker from History, each for its call. The decode of the marker
    /// already checked each Stream index.
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
            let Some(result) = append.result else {
                return Err(CallError::Fatal(format!("{append:?} has no result")));
            };
            let entry = (
                streams[append.stream as usize].clone(),
                append.activation,
                status_of(result),
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

    /// Handles the event of a marker. Each entry of the marker must have reached its call by now,
    /// and the closes whose intents it has join their queues.
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
            let queue = key(&close.stream);
            self.queues
                .entry(queue.clone())
                .or_default()
                .push_back(CallKey::Close(seq));
            self.start_head(&queue);
        }
        Ok(())
    }

    /// Ends the replay. Each call that waits takes the epoch of the task in hand, and the head of
    /// each queue starts.
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

    /// Stops the appends at a command that ends the Workflow, so an append that still waits is
    /// never sent. The closes go on.
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
        let mut changed = vec![];
        self.queues.retain(|queue, calls| {
            let head = calls.front().copied();
            calls.retain(|call| matches!(call, CallKey::Close(_)));
            if calls.front().copied() != head {
                changed.push(queue.clone());
            }
            !calls.is_empty()
        });
        for queue in changed {
            self.start_head(&queue);
        }
    }

    /// Puts each close result that came while Core holds a terminal command into the marker, with
    /// no job. A replay takes History's results at the activations of the first run.
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
            let CloseState::Done(result) = &close.state else {
                continue;
            };
            if !self.marker.close_result(result, activation) {
                return;
            }
            if let Some(failure) = result.failure() {
                warn!(
                    run_id = %self.run_id,
                    stream_id = %close.stream.id,
                    store = %close.stream.store,
                    failure = %failure.message,
                    "A Stream close failed while Core holds the Workflow's last command, so no \
                     Workflow code gets the failure"
                );
            }
            self.done.remove(&call);
            self.leave(call);
        }
    }

    /// Returns true while a close has no result in a marker.
    pub fn has_closes(&self) -> bool {
        !self.closes.is_empty()
    }

    /// Returns true while a call has no result in a marker, so the run needs another task.
    pub fn needs_next_task(&self) -> bool {
        !self.appends.is_empty() || !self.closes.is_empty()
    }

    /// Returns true if the marker of the task has an entry.
    pub fn has_marker(&self) -> bool {
        !self.marker.is_empty()
    }

    /// Returns true once the marker of the task has no room for a result that waits, so the task
    /// must complete with a new one forced.
    pub fn must_complete(&self, activation: u64) -> bool {
        self.next_result_fits(activation) == Some(false)
    }

    /// Returns the number of live calls that hold the task open, as a Local Activity does.
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

    /// Returns the activation of the first result from History whose call Workflow code issued.
    pub fn next_ready_activation(&self) -> Option<u64> {
        self.ready.keys().next().map(|(at, _)| *at)
    }

    /// Returns the marker of the task as it completes, with the intent of each close that fits.
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

    /// Returns the calls that the Worker must start or stop.
    pub fn take_requests(&mut self) -> Vec<Request> {
        std::mem::take(&mut self.requests)
    }

    /// Drops every call and closes the gate, as a failed task does. The run records no call after
    /// this.
    pub fn stop(&mut self) {
        self.phase = Phase::Stopped;
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

    /// Returns whether the marker has room for the first result that waits, if the task is open.
    fn next_result_fits(&self, activation: u64) -> Option<bool> {
        let call = *self.done.first()?;
        if !self.gate.is_open() {
            return None;
        }
        match call {
            CallKey::Append(seq) => {
                let append = &self.appends[&seq];
                let AppendState::Done(result) = &append.state else {
                    return None;
                };
                Some((self.marker).has_room_for_append(&append.stream, result, activation))
            }
            CallKey::Close(seq) => {
                let CloseState::Done(result) = &self.closes[&seq].state else {
                    return None;
                };
                Some(self.marker.has_room_for_close(result, activation))
            }
        }
    }

    /// Records a live result in the marker, and returns its job. Returns `None` if the marker has
    /// no room for it.
    fn record(
        &mut self,
        call: CallKey,
        activation: u64,
    ) -> Option<workflow_activation_job::Variant> {
        match call {
            CallKey::Append(seq) => {
                let append = &self.appends[&seq];
                let AppendState::Done(result) = &append.state else {
                    return None;
                };
                if !self
                    .marker
                    .append_result(&append.stream, result, activation)
                {
                    return None;
                }
                Some(result.job().into())
            }
            CallKey::Close(seq) => {
                let CloseState::Done(result) = &self.closes[&seq].state else {
                    return None;
                };
                if !self.marker.close_result(result, activation) {
                    return None;
                }
                Some(result.job().into())
            }
        }
    }

    /// Starts the call at the head of each queue, once the run is live and its task is open. It
    /// runs only when the task opens and at go-live, since every other change starts its own head.
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

    /// Starts the call at the head of the queue of one Stream, once the run is live and its task
    /// is open.
    fn start_head(&mut self, queue: &(String, String)) {
        if self.phase != Phase::Live || !self.gate.is_open() {
            return;
        }
        if let Some(&call) = self.queues.get(queue).and_then(VecDeque::front) {
            self.start(call);
        }
    }

    /// Starts `call` if it waits with an epoch. A call that already runs, or that has no epoch yet,
    /// stays as it is.
    fn start(&mut self, call: CallKey) {
        let (seq, call_id, op) = match call {
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
                let call_id = next_call_id();
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
                (seq, call_id, Op::Append(request))
            }
            CallKey::Close(seq) => {
                let Some(close) = self.closes.get_mut(&seq) else {
                    return;
                };
                let CloseState::Waiting(Some(epoch)) = close.state else {
                    return;
                };
                let call_id = next_call_id();
                close.state = CloseState::Running(call_id);
                let request = CloseStreamRequest {
                    namespace: self.namespace.clone(),
                    r#ref: Some(close.stream.clone()),
                    producer: Some(producer(&self.producer_id, &self.run_id, epoch)),
                };
                (seq, call_id, Op::Close(request))
            }
        };
        self.requests.push(Request::Run(Call { call_id, seq, op }));
    }

    /// Removes `call` from the run. A live result already left its queue when it came.
    fn leave(&mut self, call: CallKey) {
        let stream = match call {
            CallKey::Append(seq) => (self.appends.remove(&seq))
                .filter(|append| !matches!(append.state, AppendState::Done(_)))
                .map(|append| append.stream),
            CallKey::Close(seq) => (self.closes.remove(&seq))
                .filter(|close| !matches!(close.state, CloseState::Done(_)))
                .map(|close| close.stream),
        };
        if let Some(stream) = stream {
            self.leave_queue(&key(&stream), call);
        }
    }

    /// Removes `call` from its queue. If it was the head, the next call of the queue starts.
    fn leave_queue(&mut self, queue: &(String, String), call: CallKey) {
        let Some(calls) = self.queues.get_mut(queue) else {
            return;
        };
        let was_head = calls.front() == Some(&call);
        if was_head {
            calls.pop_front();
        } else {
            calls.retain(|queued| *queued != call);
        }
        if calls.is_empty() {
            self.queues.remove(queue);
        } else if was_head {
            self.start_head(queue);
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
    check_ref(Some(stream))
        .and_then(|stream| check_store_name(&stream.store))
        .err()
}

fn no_runtime(stream: &StreamRef) -> CallError {
    CallError::Unserved(format!(
        "This Worker has no Stream runtime for the Store `{}`",
        stream.store
    ))
}

/// A call with no Store name matches any Store, since a lang SDK with no Stream config sends none.
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

/// Quotes `part` as Python's `urllib.parse.quote(part, safe="")` does, so every SDK builds the same
/// Producer id for a run.
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
        proto::{StreamErrorDetails, StreamErrorReason},
        record::BatchLimits,
        test_store::Scripted,
        workflow::marker::MAX_MARKER_ENTRY_BYTES,
    };
    use prost::Message;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::Arc;
    use temporalio_common::protos::{
        coresdk::{external_data::stream_append_result, workflow_activation::StreamAppended},
        temporal::api::{
            common::v1::Payloads,
            failure::v1::{ApplicationFailureInfo, failure::FailureInfo},
        },
    };
    use workflow_activation_job::Variant;

    /// The number of seeds that each generated test runs.
    const SEEDS: u64 = 400;

    fn runtimes(stores: &[&str]) -> StreamRuntimes {
        StreamRuntimes::new(stores.iter().map(|store| {
            let runtime = StreamRuntime::new(Arc::new(Scripted::default()), BatchLimits::default());
            (store.to_string(), Arc::new(runtime))
        }))
    }

    fn calls(phase: Phase, task: i64) -> StreamCalls {
        let mut calls = StreamCalls::new("ns", "r", runtimes(&["mem"]), phase);
        calls.set_task(TaskState::Open(epoch(task)));
        calls
    }

    /// Completes the task in hand, and opens the next one.
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

    /// Returns a marker that `write` fills.
    fn history(write: impl FnOnce(&mut MarkerBuilder)) -> StreamMarkerData {
        let mut marker = MarkerBuilder::default();
        write(&mut marker);
        marker.take().unwrap()
    }

    /// Returns the calls that started since the last look, as `append 1 of s at 3`, with the
    /// started event id of their epoch.
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

    /// Gives a running call its result, a failure with `message` if there is one.
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

    /// Returns the results of the activation at `activation`, as `append 1` or `close 2: <text>`.
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
        let with_sequence = AppendToStream {
            sequence: 77,
            ..append(2, "s")
        };
        live.append(with_sequence).unwrap();
        live.append(append(3, "t")).unwrap();
        // An append sends the sequence of lang, or its seq when lang gives none.
        let request = |id: &str, sequence| AppendRecordsRequest {
            namespace: "ns".to_string(),
            r#ref: Some(stream(id)),
            producer: Some(producer("wf:r", "r", epoch(3))),
            sequence,
            records: vec![AppendRecord {
                body: Some(Payload::default()),
            }],
        };
        let sent = |calls: &mut StreamCalls| -> Vec<AppendRecordsRequest> {
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
            sent
        };
        assert_eq!(sent(&mut live), [request("s", 1), request("t", 3)]);
        finish(&mut live, CallKey::Append(1), None);
        assert_eq!(sent(&mut live), [request("s", 77)]);
        assert_eq!(jobs(&mut live, 1), ["append 1"]);
        // The close joins the queue at the event of its marker, behind append 2.
        let marker = next_task(&mut live, 7).unwrap();
        assert_eq!(started(&mut live), Vec::<String>::new());
        live.marker_recorded().unwrap();
        assert_eq!(started(&mut live), Vec::<String>::new());
        finish(&mut live, CallKey::Append(2), None);
        assert_eq!(started(&mut live), ["close 1 of s at 3"]);

        // A replay queues the same way. A replayed close with no intent in History waits for the
        // marker of the live task.
        let mut replay = calls(Phase::Replaying, 9);
        replay.append(append(1, "s")).unwrap();
        replay.close(close(1, "s")).unwrap();
        replay.append(append(2, "s")).unwrap();
        replay.append(append(3, "t")).unwrap();
        replay.close(close(2, "u")).unwrap();
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
        next_task(&mut replay, 13).unwrap();
        assert_eq!(started(&mut replay), Vec::<String>::new());
        replay.marker_recorded().unwrap();
        assert_eq!(started(&mut replay), ["close 2 of u at 9"]);
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
                assert!(marker.append_result(&stream("s"), &Done::append(1, appended(1)), 0));
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
        live.append(append(3, "s")).unwrap();
        started(&mut live);
        finish(&mut live, CallKey::Append(3), None);
        assert_eq!(jobs(&mut live, 2), ["append 3"]);
        let marker = next_task(&mut live, 11).unwrap();
        let entries: Vec<_> = (marker.appends.iter())
            .map(|append| (append.seq, append.activation))
            .collect();
        assert_eq!(entries, [(1, 0), (2, 0), (3, 2)]);

        // A replay gives each result in the activation that the first run gave it, and no sooner.
        let mut replay = calls(Phase::Replaying, 13);
        replay.append(append(1, "s")).unwrap();
        replay.append(append(2, "t")).unwrap();
        replay.peeked(marker).unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1", "append 2"]);
        assert!(!replay.has_jobs(1));
        assert_eq!(replay.next_ready_activation(), None);
        replay.append(append(3, "s")).unwrap();
        assert_eq!(replay.next_ready_activation(), Some(2));
        assert_eq!(jobs(&mut replay, 1), Vec::<String>::new());
        assert_eq!(jobs(&mut replay, 2), ["append 3"]);
        replay.append(append(4, "s")).unwrap();
        replay.marker_recorded().unwrap();
        replay.go_live().unwrap();
        assert_eq!(started(&mut replay), ["append 4 of s at 13"]);
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

    /// A command of Workflow code in a generated script.
    #[derive(Clone, Debug)]
    enum Command {
        Append(AppendToStream),
        Close(CloseStream),
        /// A command that ends the Workflow.
        Terminal,
    }

    /// Where a run is with the last command of the Workflow.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum End {
        Running,
        /// Core holds the command while a close has no result in a marker.
        Held,
        /// The command goes out with the completion of the task in hand.
        Sent,
    }

    /// One activation, as the first run gave it.
    #[derive(Clone, Debug)]
    struct Activation {
        index: u64,
        jobs: Vec<Variant>,
        commands: Vec<Command>,
    }

    /// A Workflow Task that History has, with what the first run did in it.
    #[derive(Clone, Debug, Default)]
    struct Task {
        /// True if the events of the task give Workflow code a job, so its first activation comes
        /// even with no Stream result.
        has_events: bool,
        activations: Vec<Activation>,
        marker: Option<StreamMarkerData>,
        /// True if the last command of the Workflow went out with this task.
        ends_workflow: bool,
    }

    /// The jobs of each activation of one task, with the index of the activation.
    type TaskJobs = Vec<(u64, Vec<Variant>)>;

    impl Task {
        fn jobs(&self) -> TaskJobs {
            (self.activations.iter())
                .map(|activation| (activation.index, activation.jobs.clone()))
                .collect()
        }

        fn commands_at(&self, index: u64) -> Vec<Command> {
            (self.activations.iter())
                .find(|activation| activation.index == index)
                .map_or(vec![], |activation| activation.commands.clone())
        }
    }

    /// A run that a generated test drives as the machines of Core do.
    struct Run {
        calls: StreamCalls,
        /// The index of the next activation of the task in hand.
        activation: u64,
        end: End,
    }

    impl Run {
        /// Gives Workflow code an activation, and takes its commands.
        fn activate(&mut self, commands: &[Command]) -> Result<Vec<Variant>, CallError> {
            let jobs = self.calls.take_jobs(self.activation);
            self.activation += 1;
            for command in commands {
                match command {
                    Command::Append(append) => self.calls.append(append.clone())?,
                    Command::Close(close) => self.calls.close(close.clone())?,
                    Command::Terminal => {
                        self.calls.stop_for_terminal();
                        self.end = End::Held;
                        self.continue_hold();
                    }
                }
            }
            Ok(jobs)
        }

        /// Puts close results into the marker while Core holds the last command, and sends the
        /// command once no close is left.
        fn continue_hold(&mut self) {
            if self.end == End::Held {
                self.calls.take_for_hold(self.activation);
                if !self.calls.has_closes() {
                    self.end = End::Sent;
                }
            }
        }

        /// Returns the appends and closes that the run still tracks.
        fn tracked(&self) -> BTreeSet<CallKey> {
            (self.calls.appends.keys().map(|seq| CallKey::Append(*seq)))
                .chain(self.calls.closes.keys().map(|seq| CallKey::Close(*seq)))
                .collect()
        }
    }

    /// What a replay gives.
    struct Replayed {
        jobs: Vec<TaskJobs>,
        /// The task in which the last command of the Workflow went out.
        sent_in: Option<usize>,
        run: Run,
    }

    /// Replays `history` with the commands of the first run of `seed`, and goes live in the task
    /// `live`, as a new run of Core does.
    fn replay(seed: u64, history: &[Task], live: WorkflowTaskEpoch) -> Result<Replayed, CallError> {
        let calls = StreamCalls::new("ns", "r", runtimes(&["mem"]), Phase::Replaying);
        let mut run = Run {
            calls,
            activation: 0,
            end: End::Running,
        };
        run.calls.set_task(TaskState::Open(live));
        let mut replayed = vec![];
        let mut sent_in = None;
        for (index, task) in history.iter().enumerate() {
            if index > 0 && history[index - 1].marker.is_some() {
                run.calls.marker_recorded()?;
            }
            // Core peeks the marker that the task's completion recorded before it activates.
            if let Some(marker) = &task.marker {
                run.calls.peeked(marker.clone())?;
            }
            run.activation = 0;
            run.continue_hold();
            let mut activations = vec![];
            let mut first = task.has_events;
            while run.end == End::Running {
                if !first && !run.calls.has_jobs(run.activation) {
                    // Core sets its activation index to that of the next result from History when
                    // nothing else needs an activation.
                    match run.calls.next_ready_activation() {
                        Some(at) if at > run.activation => run.activation = at,
                        _ => break,
                    }
                    continue;
                }
                first = false;
                let at = run.activation;
                let jobs = run.activate(&task.commands_at(at))?;
                activations.push((at, jobs));
            }
            if run.end == End::Sent && sent_in.is_none() {
                sent_in = Some(index);
            }
            let requests = run.calls.take_requests();
            assert!(requests.is_empty(), "seed {seed}: {requests:?}");
            replayed.push(activations);
        }
        if history.last().is_some_and(|task| task.marker.is_some()) {
            run.calls.marker_recorded()?;
        }
        run.activation = 0;
        run.continue_hold();
        run.calls.go_live()?;
        Ok(Replayed {
            jobs: replayed,
            sent_in,
            run,
        })
    }

    /// The things that a generated script reached, over all seeds.
    #[derive(Default)]
    struct Seen {
        full_markers: usize,
        heartbeats: usize,
        failed_tasks: usize,
        holds: usize,
        refused: usize,
        bad_seqs: usize,
    }

    /// The first run of a generated script. It writes History, and checks a replay of it at each
    /// failed task and at the end.
    struct FirstRun<'a> {
        seed: u64,
        rng: StdRng,
        seen: &'a mut Seen,
        run: Run,
        history: Vec<Task>,
        task: Task,
        epoch: WorkflowTaskEpoch,
        /// The Store calls in flight, which the Store answers in any order.
        in_flight: Vec<Call>,
        /// Results that came while no task was in hand. Core applies them once the next task's
        /// events are applied.
        deferred: Vec<StreamResolution>,
        last_append: u32,
        last_close: u32,
        /// The calls whose result no marker has.
        unresolved: BTreeSet<CallKey>,
        /// The calls that Core refused for an input over its limit.
        refused: BTreeSet<CallKey>,
        /// The state as of the last task in History, which a failed task goes back to.
        committed: (u32, u32, BTreeSet<CallKey>, BTreeSet<CallKey>),
    }

    impl<'a> FirstRun<'a> {
        fn new(seed: u64, seen: &'a mut Seen) -> Self {
            let run = Run {
                calls: StreamCalls::new("ns", "r", runtimes(&["mem"]), Phase::Live),
                activation: 0,
                end: End::Running,
            };
            Self {
                seed,
                rng: StdRng::seed_from_u64(seed),
                seen,
                run,
                history: vec![],
                task: Task::default(),
                epoch: epoch(3),
                in_flight: vec![],
                deferred: vec![],
                last_append: 0,
                last_close: 0,
                unresolved: BTreeSet::new(),
                refused: BTreeSet::new(),
                committed: Default::default(),
            }
        }

        /// Runs the script for up to `tasks` tasks, or until the Workflow ends.
        fn play(&mut self, tasks: usize) {
            let mut started = false;
            for _ in 0..tasks {
                if !started {
                    self.start_task();
                }
                match self.run_task() {
                    Ok(()) => {
                        if self.history.last().unwrap().ends_workflow {
                            return;
                        }
                        self.between_tasks();
                        self.epoch = WorkflowTaskEpoch {
                            started_event_id: self.epoch.started_event_id + 4,
                            attempt: 1,
                            started_time: None,
                        };
                        started = false;
                    }
                    Err(()) => {
                        self.retry();
                        started = true;
                    }
                }
            }
        }

        fn start_task(&mut self) {
            self.run.calls.set_task(TaskState::Open(self.epoch));
            if self
                .history
                .last()
                .is_some_and(|task| task.marker.is_some())
            {
                self.run.calls.marker_recorded().unwrap();
            }
            self.run.activation = 0;
            self.run.continue_hold();
            for resolution in std::mem::take(&mut self.deferred) {
                self.run.calls.resolve(resolution);
                self.run.continue_hold();
            }
            self.dispatch();
        }

        /// Runs the task in hand until it completes, and returns an error if it fails.
        fn run_task(&mut self) -> Result<(), ()> {
            self.task.has_events = self.history.is_empty() || self.rng.random_bool(0.6);
            if self.run.end == End::Running
                && (self.task.has_events || self.run.calls.has_jobs(self.run.activation))
            {
                self.activate()?;
            }
            loop {
                if self.run.end == End::Sent {
                    self.complete();
                    return Ok(());
                }
                if self.run.end == End::Running && self.run.calls.has_jobs(self.run.activation) {
                    self.activate()?;
                    continue;
                }
                if self.run.calls.must_complete(self.run.activation) {
                    self.seen.full_markers += 1;
                    self.complete();
                    return Ok(());
                }
                if self.run.calls.outstanding() == 0 {
                    self.complete();
                    return Ok(());
                }
                let seed = self.seed;
                assert!(
                    !self.in_flight.is_empty(),
                    "seed {seed}: a call waits, and none runs"
                );
                match self.rng.random_range(0..40) {
                    // A few answers can come before Core gives the next activation.
                    0..=35 => {
                        for _ in 0..self.rng.random_range(1..=8) {
                            if self.in_flight.is_empty() {
                                break;
                            }
                            let resolution = self.answer();
                            self.run.calls.resolve(resolution);
                            self.run.continue_hold();
                            self.dispatch();
                        }
                    }
                    36..=38 => {
                        // The heartbeat completes the task. Results that come before the
                        // completion wait for the next task.
                        self.seen.heartbeats += 1;
                        self.run.calls.set_task(TaskState::Completing(self.epoch));
                        for _ in 0..self.rng.random_range(0..=2) {
                            if !self.in_flight.is_empty() {
                                let resolution = self.answer();
                                self.run.calls.resolve(resolution);
                                self.run.continue_hold();
                                self.dispatch();
                            }
                        }
                        self.complete();
                        return Ok(());
                    }
                    _ => return Err(()),
                }
            }
        }

        /// Gives Workflow code an activation with commands that the script makes up.
        fn activate(&mut self) -> Result<(), ()> {
            let index = self.run.activation;
            let (commands, bad_seq) = self.commands();
            let result = self.run.activate(&commands);
            if let Some(expected) = bad_seq {
                self.seen.bad_seqs += 1;
                assert_eq!(result, Err(expected), "seed {}", self.seed);
                return Err(());
            }
            let jobs = result.unwrap_or_else(|error| panic!("seed {}: {error:?}", self.seed));
            for command in &commands {
                match command {
                    Command::Append(append) if !self.is_refused(CallKey::Append(append.seq)) => {
                        self.unresolved.insert(CallKey::Append(append.seq));
                    }
                    Command::Close(close) if !self.is_refused(CallKey::Close(close.seq)) => {
                        self.unresolved.insert(CallKey::Close(close.seq));
                    }
                    Command::Terminal => {
                        self.seen.holds += usize::from(self.run.end == End::Held);
                        self.unresolved
                            .retain(|call| matches!(call, CallKey::Close(_)));
                    }
                    _ => {}
                }
            }
            self.task.activations.push(Activation {
                index,
                jobs,
                commands,
            });
            self.dispatch();
            Ok(())
        }

        fn is_refused(&self, call: CallKey) -> bool {
            self.refused.contains(&call)
        }

        /// Returns the commands of an activation, and the error that a command with a seq that
        /// can't follow the last one gets.
        fn commands(&mut self) -> (Vec<Command>, Option<CallError>) {
            let mut commands = vec![];
            for _ in 0..self.rng.random_range(0..=3) {
                let id = format!("s{}", self.rng.random_range(0..3));
                match self.rng.random_range(0..100) {
                    0..=44 => {
                        self.last_append += 1;
                        commands.push(Command::Append(append(self.last_append, &id)));
                    }
                    45..=69 => {
                        self.last_close += 1;
                        commands.push(Command::Close(close(self.last_close, &id)));
                    }
                    70..=79 => {
                        // An input over its limit gets a failure, and no Store sees the call.
                        self.seen.refused += 1;
                        let long = "x".repeat(4097);
                        match self.rng.random_range(0..3) {
                            0 => {
                                self.last_append += 1;
                                self.refused.insert(CallKey::Append(self.last_append));
                                commands.push(Command::Append(append(self.last_append, &long)));
                            }
                            1 => {
                                self.last_append += 1;
                                self.refused.insert(CallKey::Append(self.last_append));
                                commands.push(Command::Append(append(self.last_append, "")));
                            }
                            _ => {
                                self.last_close += 1;
                                self.refused.insert(CallKey::Close(self.last_close));
                                let mut long_store = close(self.last_close, &id);
                                long_store.stream.as_mut().unwrap().store = long;
                                commands.push(Command::Close(long_store));
                            }
                        }
                    }
                    80..=82 => {
                        let (command, error) = if self.rng.random_bool(0.5) {
                            let seq = self.last_append;
                            let error = match seq {
                                0 => "Stream append has seq 0, and a seq starts at 1".to_string(),
                                _ => format!("Stream append {seq} came after append {seq}"),
                            };
                            (Command::Append(append(seq, &id)), error)
                        } else {
                            let seq = self.last_close;
                            let error = match seq {
                                0 => "Stream close has seq 0, and a seq starts at 1".to_string(),
                                _ => format!("Stream close {seq} came after close {seq}"),
                            };
                            (Command::Close(close(seq, &id)), error)
                        };
                        commands.push(command);
                        return (commands, Some(CallError::Fatal(error)));
                    }
                    83..=86 => {
                        commands.push(Command::Terminal);
                        return (commands, None);
                    }
                    87..=89 => {
                        // A burst of appends, each on a Stream of its own, can fill a marker.
                        for _ in 0..self.rng.random_range(40..=90) {
                            self.last_append += 1;
                            let long = "x".repeat(self.rng.random_range(0..3000));
                            let id = format!("b{}{long}", self.last_append);
                            commands.push(Command::Append(append(self.last_append, &id)));
                        }
                    }
                    _ => {}
                }
            }
            (commands, None)
        }

        /// Takes a random call in flight, and returns the Store's answer to it.
        fn answer(&mut self) -> StreamResolution {
            let call = (self.in_flight).swap_remove(self.rng.random_range(0..self.in_flight.len()));
            let failed = |message: String| failure(&StreamError::invalid(message));
            let outcome = match &call.op {
                Op::Append(request) => {
                    // A large failure fills the marker in a few dozen results.
                    let burst = request
                        .r#ref
                        .as_ref()
                        .is_some_and(|s| s.id.starts_with('b'));
                    let large = if burst { 8 } else { 1 };
                    Outcome::Append(match self.rng.random_range(0..10) {
                        at if at < large => {
                            resolve_stream_append::Status::Failed(failed("x".repeat(4000)))
                        }
                        at if at < large + 2 => {
                            resolve_stream_append::Status::Failed(failed("refused".into()))
                        }
                        _ => appended(call.seq),
                    })
                }
                Op::Close(_) => {
                    Outcome::Close(self.rng.random_bool(0.3).then(|| failed("refused".into())))
                }
            };
            StreamResolution {
                call_id: call.call_id,
                seq: call.seq,
                outcome,
            }
        }

        fn dispatch(&mut self) {
            for request in self.run.calls.take_requests() {
                match request {
                    Request::Run(call) => {
                        let key = match call.op {
                            Op::Append(_) => CallKey::Append(call.seq),
                            Op::Close(_) => CallKey::Close(call.seq),
                        };
                        let seed = self.seed;
                        assert!(!self.is_refused(key), "seed {seed}: a refused {key} ran");
                        self.in_flight.push(call);
                    }
                    Request::Stop(call_id) => self.in_flight.retain(|call| call.call_id != call_id),
                }
            }
        }

        /// Completes the task in hand, and checks that the run tracks exactly the calls whose
        /// result no marker has.
        fn complete(&mut self) {
            let marker = self.run.calls.take_marker();
            self.run.calls.set_task(TaskState::NoTask);
            for entry in marker.iter() {
                for append in &entry.appends {
                    self.unresolved.remove(&CallKey::Append(append.seq));
                }
                for close in &entry.closes {
                    self.unresolved.remove(&CallKey::Close(close.seq));
                }
            }
            let mut task = std::mem::take(&mut self.task);
            task.marker = marker;
            task.ends_workflow = self.run.end == End::Sent;
            self.history.push(task);
            self.committed = (
                self.last_append,
                self.last_close,
                self.unresolved.clone(),
                self.refused.clone(),
            );
            let seed = self.seed;
            assert_eq!(self.run.tracked(), self.unresolved, "seed {seed}");
            assert!(self.run.calls.ready.is_empty(), "seed {seed}");
            let queued = self.run.calls.queues.values().flatten();
            let waiting = self.run.calls.done.iter().chain(queued);
            assert!(
                waiting
                    .into_iter()
                    .all(|call| self.unresolved.contains(call)),
                "seed {seed}"
            );
        }

        /// Lets the Store answer while no task is in hand. Core applies these results once the
        /// next task's events are applied.
        fn between_tasks(&mut self) {
            for _ in 0..self.rng.random_range(0..=2) {
                if !self.in_flight.is_empty() {
                    let resolution = self.answer();
                    self.deferred.push(resolution);
                }
            }
        }

        /// Fails the task in hand. A new run replays History and goes live in the next attempt.
        fn retry(&mut self) {
            self.seen.failed_tasks += 1;
            self.run.calls.stop();
            self.in_flight.clear();
            self.deferred.clear();
            (
                self.last_append,
                self.last_close,
                self.unresolved,
                self.refused,
            ) = self.committed.clone();
            self.task = Task::default();
            self.epoch.attempt += 1;
            self.run = self.check_replay(self.epoch);
            self.dispatch();
        }

        /// Replays History, checks it against the first run, and returns the replayed run.
        fn check_replay(&self, live: WorkflowTaskEpoch) -> Run {
            let seed = self.seed;
            let Replayed {
                jobs: replayed,
                sent_in,
                mut run,
            } = replay(seed, &self.history, live)
                .unwrap_or_else(|error| panic!("seed {seed}: {error:?}"));
            let first: Vec<_> = self.history.iter().map(Task::jobs).collect();
            assert_eq!(replayed, first, "seed {seed}");
            let ended = self.history.iter().position(|task| task.ends_workflow);
            assert_eq!(sent_in, ended, "seed {seed}");
            // The replay resumes each call whose result no marker has, and only those.
            assert_eq!(run.tracked(), self.unresolved, "seed {seed}");
            let recorded: BTreeSet<CallKey> = (self.history.iter())
                .filter_map(|task| task.marker.as_ref())
                .flat_map(|marker| {
                    (marker
                        .appends
                        .iter()
                        .map(|append| CallKey::Append(append.seq)))
                    .chain(marker.closes.iter().map(|close| CallKey::Close(close.seq)))
                })
                .collect();
            for request in run.calls.take_requests() {
                let Request::Run(Call { seq, op, .. }) = &request else {
                    panic!("seed {seed}: {request:?}");
                };
                let (key, producer) = match op {
                    Op::Append(append) => (CallKey::Append(*seq), &append.producer),
                    Op::Close(close) => (CallKey::Close(*seq), &close.producer),
                };
                assert!(!recorded.contains(&key), "seed {seed}: {key} ran again");
                assert_eq!(*producer, Some(producer_of(live)), "seed {seed}");
                run.calls.requests.push(request);
            }
            run
        }
    }

    fn producer_of(epoch: WorkflowTaskEpoch) -> Producer {
        producer("wf:r", "r", epoch)
    }

    #[test]
    fn a_replay_of_history_gives_workflow_code_the_same_jobs_in_the_same_activations() {
        let mut seen = Seen::default();
        for seed in 0..SEEDS {
            let mut first = FirstRun::new(seed, &mut seen);
            first.play(12);
            let live = WorkflowTaskEpoch {
                started_event_id: first.epoch.started_event_id + 4,
                ..first.epoch
            };
            first.check_replay(live);
        }
        // The scripts reach each case that the test is for.
        let Seen {
            full_markers,
            heartbeats,
            failed_tasks,
            holds,
            refused,
            bad_seqs,
        } = seen;
        let counts = [
            full_markers,
            heartbeats,
            failed_tasks,
            holds,
            refused,
            bad_seqs,
        ];
        assert!(counts.iter().all(|count| *count > 0), "{counts:?}");
    }

    #[test]
    fn the_calls_of_one_stream_go_to_the_store_one_at_a_time_in_call_order() {
        let mut together = 0;
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut calls = calls(Phase::Live, 3);
            // The calls of each Stream that have no result yet, in the order that they joined its
            // queue. An append joins at its command, and a close once History has its intent.
            let mut queues: BTreeMap<String, VecDeque<CallKey>> = BTreeMap::new();
            let mut in_flight: Vec<(String, CallKey, u64)> = vec![];
            let (mut appends, mut closes, mut task, mut ended) = (0, 0, 3, false);
            let take = |calls: &mut StreamCalls, in_flight: &mut Vec<(String, CallKey, u64)>| {
                for request in calls.take_requests() {
                    match request {
                        Request::Run(Call { call_id, seq, op }) => {
                            let (call, stream) = match op {
                                Op::Append(request) => (CallKey::Append(seq), request.r#ref),
                                Op::Close(request) => (CallKey::Close(seq), request.r#ref),
                            };
                            in_flight.push((stream.unwrap().id, call, call_id));
                        }
                        Request::Stop(call_id) => in_flight.retain(|(.., id)| *id != call_id),
                    }
                }
            };
            for _ in 0..60 {
                let id = format!("s{}", rng.random_range(0..4));
                match rng.random_range(0..20) {
                    _ if ended => {}
                    0..=11 => {
                        appends += 1;
                        calls.append(append(appends, &id)).unwrap();
                        queues
                            .entry(id)
                            .or_default()
                            .push_back(CallKey::Append(appends));
                    }
                    12..=15 => {
                        closes += 1;
                        calls.close(close(closes, &id)).unwrap();
                    }
                    16 => {
                        // A terminal command stops the appends, and the closes go on.
                        calls.stop_for_terminal();
                        for queue in queues.values_mut() {
                            queue.retain(|call| matches!(call, CallKey::Close(_)));
                        }
                        ended = true;
                    }
                    _ => {}
                }
                if rng.random_bool(0.15) {
                    task += 4;
                    if let Some(marker) = next_task(&mut calls, task) {
                        calls.marker_recorded().unwrap();
                        for intent in &marker.close_intents {
                            let id = marker.streams[intent.stream as usize].id.clone();
                            queues
                                .entry(id)
                                .or_default()
                                .push_back(CallKey::Close(intent.seq));
                        }
                    }
                }
                take(&mut calls, &mut in_flight);
                // The Store answers some of the calls in flight, in any order.
                for _ in 0..rng.random_range(0..=2) {
                    if in_flight.is_empty() {
                        break;
                    }
                    let (id, call, call_id) =
                        in_flight.swap_remove(rng.random_range(0..in_flight.len()));
                    let oldest = queues.get_mut(&id).and_then(VecDeque::pop_front);
                    assert_eq!(oldest, Some(call), "seed {seed}: Stream {id}");
                    let failed = rng
                        .random_bool(0.3)
                        .then(|| failure(&StreamError::invalid("no")));
                    let outcome = match (call, failed) {
                        (CallKey::Append(seq), None) => Outcome::Append(appended(seq)),
                        (CallKey::Append(_), Some(failure)) => {
                            Outcome::Append(resolve_stream_append::Status::Failed(failure))
                        }
                        (CallKey::Close(_), failure) => Outcome::Close(failure),
                    };
                    let seq = match call {
                        CallKey::Append(seq) | CallKey::Close(seq) => seq,
                    };
                    calls.resolve(StreamResolution {
                        call_id,
                        seq,
                        outcome,
                    });
                    take(&mut calls, &mut in_flight);
                }
                // Each Stream with calls that wait has its oldest one in flight, and only that
                // one, whatever the other Streams do.
                for id in (0..4).map(|index| format!("s{index}")) {
                    let running: Vec<CallKey> = (in_flight.iter())
                        .filter(|(stream, ..)| *stream == id)
                        .map(|(_, call, _)| *call)
                        .collect();
                    let oldest = queues.get(&id).and_then(|queue| queue.front().copied());
                    assert_eq!(running, Vec::from_iter(oldest), "seed {seed}: Stream {id}");
                }
                let streams: HashSet<&String> = in_flight.iter().map(|(id, ..)| id).collect();
                together += usize::from(streams.len() > 1);
            }
        }
        assert!(together > 0);
    }

    /// Returns a failure message of at least `bytes` bytes. Its characters have 1 to 4 bytes, so
    /// a cut can fall inside one.
    fn message(rng: &mut StdRng, bytes: usize) -> String {
        let chars = ['x', 'é', '€', '𝄞'];
        let mut message: String = (0..rng.random_range(0..8))
            .map(|_| chars[rng.random_range(0..4)])
            .collect();
        let fill = chars[rng.random_range(0..4)];
        let count = bytes
            .saturating_sub(message.len())
            .div_ceil(fill.len_utf8());
        message.push_str(&fill.to_string().repeat(count));
        message
    }

    /// Returns a random answer of the Store to `call`. A failure message is near the cut at 4096
    /// bytes, or far below it, and goes into `texts`. Rarely an append gets a Failure that no Store
    /// answer gives, so that its one entry is larger than the bound of a marker.
    fn answer(
        rng: &mut StdRng,
        call: Call,
        texts: &mut HashMap<CallKey, String>,
    ) -> StreamResolution {
        let key = match call.op {
            Op::Append(_) => CallKey::Append(call.seq),
            Op::Close(_) => CallKey::Close(call.seq),
        };
        let failed = match rng.random_range(0..10) {
            0..=2 => None,
            3 if matches!(key, CallKey::Append(_)) && rng.random_bool(0.05) => Some(Failure {
                message: "x".repeat(MAX_MARKER_ENTRY_BYTES + 1),
                ..Default::default()
            }),
            at => {
                let bytes = match at {
                    3..=7 => rng.random_range(4070..4100),
                    8 => rng.random_range(0..4200),
                    _ => rng.random_range(0..50),
                };
                let text = message(rng, bytes);
                texts.insert(key, text.clone());
                Some(failure(&StreamError::invalid(text)))
            }
        };
        let outcome = match (key, failed) {
            (CallKey::Append(seq), None) => Outcome::Append(appended(seq)),
            (CallKey::Append(_), Some(failure)) => {
                Outcome::Append(resolve_stream_append::Status::Failed(failure))
            }
            (CallKey::Close(_), failure) => Outcome::Close(failure),
        };
        StreamResolution {
            call_id: call.call_id,
            seq: call.seq,
            outcome,
        }
    }

    fn key_of(job: &Variant) -> CallKey {
        match job {
            Variant::ResolveStreamAppend(append) => CallKey::Append(append.seq),
            Variant::ResolveStreamClose(close) => CallKey::Close(close.seq),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn each_result_and_intent_is_in_exactly_one_marker_in_order() {
        let (mut full, mut oversized, mut in_hold, mut out_of_order) = (0, 0, 0, 0);
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut calls = calls(Phase::Live, 3);
            let budget = rng.random_range(1..=150);
            // Half the runs end with a terminal command once the appends are done, so the close
            // results go into the markers while Core holds the command.
            let hold = rng.random_bool(0.5);
            let (mut appends, mut closes, mut held) = (0, 0, false);
            let mut in_flight: Vec<Call> = vec![];
            let mut texts = HashMap::new();
            // The results that Core took in each task, in order, with their activation.
            let mut taken: Vec<Vec<(u64, CallKey)>> = vec![vec![]];
            let mut markers = vec![];
            let (mut activation, mut task) = (0, 3);
            // The highest seq of each kind that Core took so far.
            let mut highest = (0, 0);
            let start = |calls: &mut StreamCalls, in_flight: &mut Vec<Call>| {
                in_flight.extend(calls.take_requests().into_iter().map(|request| {
                    let Request::Run(call) = request else {
                        panic!("seed {seed}: {request:?}");
                    };
                    call
                }));
            };
            for round in 0.. {
                assert!(round < 100_000, "seed {seed}: the run never ends");
                // Most calls share a few Streams, so they wait in a queue. The others have a
                // Stream of their own, whose long id fills a marker sooner.
                for _ in 0..rng.random_range(0..=4) {
                    if held || appends + closes == budget {
                        break;
                    }
                    let id = match rng.random_bool(0.6) {
                        true => format!("s{}", rng.random_range(0..4)),
                        false => {
                            let long = "a".repeat(rng.random_range(0..3000));
                            format!("own{}{long}", appends + closes)
                        }
                    };
                    if rng.random_bool(0.7) {
                        appends += 1;
                        calls.append(append(appends, &id)).unwrap();
                    } else {
                        closes += 1;
                        calls.close(close(closes, &id)).unwrap();
                    }
                }
                if hold && !held && appends + closes == budget && calls.appends.is_empty() {
                    calls.stop_for_terminal();
                    held = true;
                }
                start(&mut calls, &mut in_flight);
                // The Store answers some of the calls in flight, in any order.
                for _ in 0..rng.random_range(0..=in_flight.len().min(6)) {
                    let call = in_flight.swap_remove(rng.random_range(0..in_flight.len()));
                    calls.resolve(answer(&mut rng, call, &mut texts));
                    start(&mut calls, &mut in_flight);
                }
                // Core takes the results that wait in their order, up to the first that doesn't
                // fit, and the marker records them with the activation.
                let waiting: Vec<CallKey> = calls.done.iter().copied().collect();
                let took = if held {
                    calls.take_for_hold(activation);
                    let left = waiting
                        .iter()
                        .filter(|call| calls.done.contains(call))
                        .count();
                    waiting.len() - left
                } else if calls.has_jobs(activation) && rng.random_bool(0.8) {
                    let jobs: Vec<CallKey> =
                        calls.take_jobs(activation).iter().map(key_of).collect();
                    assert_eq!(jobs, waiting[..jobs.len()], "seed {seed}");
                    jobs.len()
                } else {
                    0
                };
                assert!(
                    waiting[took..].iter().all(|call| calls.done.contains(call)),
                    "seed {seed}"
                );
                // A take stops only at a result that doesn't fit, so a result that still waits
                // means that the marker is full and the task must complete.
                if held || took > 0 {
                    let waits = !calls.done.is_empty();
                    assert_eq!(calls.must_complete(activation), waits, "seed {seed}");
                    full += usize::from(waits);
                }
                in_hold += usize::from(held && took > 0);
                for call in &waiting[..took] {
                    let (seq, high) = match call {
                        CallKey::Append(seq) => (*seq, &mut highest.0),
                        CallKey::Close(seq) => (*seq, &mut highest.1),
                    };
                    out_of_order += usize::from(seq < *high);
                    *high = seq.max(*high);
                }
                let entries = taken.last_mut().unwrap();
                entries.extend(waiting[..took].iter().map(|call| (activation, *call)));
                if !held && took > 0 {
                    activation += 1;
                    continue;
                }
                // A full marker completes the task, and so does a heartbeat at any time.
                let finished = appends + closes == budget && !calls.needs_next_task();
                if finished || calls.must_complete(activation) || rng.random_bool(0.03) {
                    let marker = calls.take_marker();
                    task += 4;
                    calls.set_task(TaskState::NoTask);
                    calls.set_task(TaskState::Open(epoch(task)));
                    if marker.is_some() {
                        calls.marker_recorded().unwrap();
                    }
                    markers.push(marker.unwrap_or_default());
                    taken.push(vec![]);
                    activation = 0;
                    if finished {
                        break;
                    }
                }
            }
            // Each marker records the results of its task in the order that Core took them, with
            // their activations. The appends come first, then the closes.
            for (index, marker) in markers.iter().enumerate() {
                let appended = marker.appends.iter();
                let recorded: Vec<(u64, CallKey)> = (appended)
                    .map(|append| (append.activation, CallKey::Append(append.seq)))
                    .chain(
                        marker
                            .closes
                            .iter()
                            .map(|c| (c.activation, CallKey::Close(c.seq))),
                    )
                    .collect();
                let mut expected = taken[index].clone();
                expected.sort_by_key(|(_, call)| matches!(call, CallKey::Close(_)));
                assert_eq!(recorded, expected, "seed {seed}: task {index}");
            }
            let once = |seqs: Vec<u32>, count: u32| {
                let mut sorted = seqs.clone();
                sorted.sort_unstable();
                assert_eq!(
                    sorted,
                    (1..=count).collect::<Vec<_>>(),
                    "seed {seed}: {seqs:?}"
                );
            };
            let all = |seqs: &dyn Fn(&StreamMarkerData) -> Vec<u32>| -> Vec<u32> {
                markers.iter().flat_map(seqs).collect()
            };
            once(all(&|m| m.appends.iter().map(|a| a.seq).collect()), appends);
            once(all(&|m| m.closes.iter().map(|c| c.seq).collect()), closes);
            let intents = all(&|m| m.close_intents.iter().map(|i| i.seq).collect());
            assert_eq!(intents, (1..=closes).collect::<Vec<_>>(), "seed {seed}");
            for marker in &markers {
                let entries =
                    marker.appends.len() + marker.close_intents.len() + marker.closes.len();
                let size = marker.encoded_len();
                assert!(
                    size <= MAX_MARKER_ENTRY_BYTES || entries == 1,
                    "seed {seed}: {size}"
                );
                oversized += usize::from(size > MAX_MARKER_ENTRY_BYTES);
                let failures = (marker.appends.iter())
                    .filter_map(|append| match &append.result {
                        Some(stream_append_result::Result::Failure(failure)) => {
                            Some((CallKey::Append(append.seq), failure))
                        }
                        _ => None,
                    })
                    .chain((marker.closes.iter()).filter_map(|close| {
                        Some((CallKey::Close(close.seq), close.failure.as_ref()?))
                    }));
                for (key, failure) in failures {
                    if let Some(text) = texts.get(&key) {
                        check_failure(failure, text, &format!("seed {seed}: {key}"));
                    }
                }
            }
        }
        // The scripts reach each case that the test is for.
        let counts = [full, oversized, in_hold, out_of_order];
        assert!(
            counts.iter().all(|count| *count > 0),
            "full {full}, oversized {oversized}, in hold {in_hold}, out of order {out_of_order}"
        );
    }

    /// Checks that `failure` is what Workflow code gets for an invalid request with `text`. A
    /// message over 4096 bytes is cut on a character, with `...` at the end.
    fn check_failure(failure: &Failure, text: &str, at: &str) {
        let whole = StreamError::invalid(text);
        let full = whole.to_string();
        let message = &failure.message;
        if full.len() <= 4096 {
            assert_eq!(*message, full, "{at}");
        } else {
            let kept = message
                .strip_suffix("...")
                .unwrap_or_else(|| panic!("{at}"));
            assert!(full.starts_with(kept) && message.len() <= 4096, "{at}");
            let next = full[kept.len()..].chars().next().unwrap();
            assert!(message.len() + next.len_utf8() > 4096, "{at}");
        }
        let details = StreamErrorDetails::decode(whole.details().as_slice()).unwrap();
        assert_eq!(details.reason, StreamErrorReason::Invalid as i32, "{at}");
        let payload = Payload {
            metadata: HashMap::from([
                ("encoding".to_string(), b"binary/protobuf".to_vec()),
                (
                    "messageType".to_string(),
                    b"temporal.api.stream.v1.StreamErrorDetails".to_vec(),
                ),
            ]),
            data: whole.details(),
            ..Default::default()
        };
        let info = ApplicationFailureInfo {
            r#type: "stream_invalid".to_string(),
            non_retryable: true,
            details: Some(Payloads {
                payloads: vec![payload],
            }),
            ..Default::default()
        };
        let expected = Failure {
            message: message.clone(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(info)),
            ..Default::default()
        };
        assert_eq!(*failure, expected, "{at}");
    }

    /// A change of History or of Workflow code that a replay must not take for the first run.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Change {
        /// The command of a call is on another Stream.
        Stream,
        /// Workflow code never issues a call that History has an entry of.
        Missing,
        /// History has an entry twice.
        Twice,
        /// History has an entry for a seq that no command has.
        Seq,
        /// History has an entry as the other kind of call.
        Kind,
        /// History has an entry at another activation.
        Activation,
    }

    /// Makes a random change to `history`, if it has an entry to change.
    fn mutate(rng: &mut StdRng, history: &mut [Task]) -> Option<Change> {
        let entries: Vec<(usize, CallKey)> = (history.iter().enumerate())
            .filter_map(|(index, task)| Some((index, task.marker.as_ref()?)))
            .flat_map(|(index, marker)| {
                (marker
                    .appends
                    .iter()
                    .map(move |append| (index, CallKey::Append(append.seq))))
                .chain(
                    marker
                        .closes
                        .iter()
                        .map(move |close| (index, CallKey::Close(close.seq))),
                )
            })
            .collect();
        if entries.is_empty() {
            return None;
        }
        let (task, call) = entries[rng.random_range(0..entries.len())];
        let change = [
            Change::Stream,
            Change::Missing,
            Change::Twice,
            Change::Seq,
            Change::Kind,
            Change::Activation,
        ][rng.random_range(0..6)];
        if let Change::Stream | Change::Missing = change {
            let commands = (history.iter_mut())
                .flat_map(|task| task.activations.iter_mut())
                .map(|activation| &mut activation.commands)
                .find(|commands| commands.iter().any(|command| is_command_of(command, call)))?;
            let at = commands
                .iter()
                .position(|command| is_command_of(command, call))?;
            match (change, &mut commands[at]) {
                (Change::Missing, _) => {
                    commands.remove(at);
                }
                (_, Command::Append(append)) => append.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Close(close)) => close.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Terminal) => return None,
            }
            return Some(change);
        }
        let marker = history[task].marker.as_mut().unwrap();
        match call {
            CallKey::Append(seq) => {
                let at = marker.appends.iter().position(|append| append.seq == seq)?;
                match change {
                    Change::Twice => marker.appends.push(marker.appends[at].clone()),
                    Change::Seq => marker.appends[at].seq = 999,
                    Change::Activation => marker.appends[at].activation += 1,
                    _ => {
                        let entry = marker.appends.remove(at);
                        marker.closes.push(StreamCloseResult {
                            seq,
                            activation: entry.activation,
                            failure: None,
                        });
                    }
                }
            }
            CallKey::Close(seq) => {
                let at = marker.closes.iter().position(|close| close.seq == seq)?;
                match change {
                    Change::Twice => marker.closes.push(marker.closes[at].clone()),
                    Change::Seq => marker.closes[at].seq = 999,
                    Change::Activation => marker.closes[at].activation += 1,
                    _ => {
                        let entry = marker.closes.remove(at);
                        let index = marker.streams.len() as u32;
                        marker.streams.push(stream("s0"));
                        marker.appends.push(StreamAppendResult {
                            seq,
                            stream: index,
                            activation: entry.activation,
                            result: Some(
                                stream_append_result::Result::Appended(Default::default()),
                            ),
                        });
                    }
                }
            }
        }
        Some(change)
    }

    fn is_command_of(command: &Command, call: CallKey) -> bool {
        match (command, call) {
            (Command::Append(append), CallKey::Append(seq)) => append.seq == seq,
            (Command::Close(close), CallKey::Close(seq)) => close.seq == seq,
            _ => false,
        }
    }

    #[test]
    fn changed_workflow_code_is_a_nondeterminism_error() {
        let mut seen = Seen::default();
        let mut mutated = 0;
        for seed in 0..SEEDS {
            let mut first = FirstRun::new(seed, &mut seen);
            first.play(6);
            let mut rng = StdRng::seed_from_u64(seed);
            let mut history = first.history.clone();
            let Some(change) = mutate(&mut rng, &mut history) else {
                continue;
            };
            mutated += 1;
            let first_jobs: Vec<_> = first.history.iter().map(Task::jobs).collect();
            match replay(seed, &history, epoch(1000)) {
                Err(CallError::Nondeterminism(_)) => {}
                // An entry at another activation, or of the other kind, can match a call. Then
                // Workflow code gets other jobs, which is as wrong as an error.
                Ok(replayed) if matches!(change, Change::Activation | Change::Kind) => {
                    assert_ne!(replayed.jobs, first_jobs, "seed {seed}: {change:?}");
                }
                other => panic!("seed {seed}: {change:?}: {:?}", other.map(|r| r.jobs)),
            }
        }
        assert!(mutated > SEEDS / 2, "{mutated}");

        // A lang SDK with no Stream config sends no Store name, which matches any Store.
        let mut lang_with_no_config = calls(Phase::ReplayOnly, 9);
        let mut nameless = append(1, "s");
        nameless.stream.as_mut().unwrap().store.clear();
        lang_with_no_config.append(nameless).unwrap();
        let result = Done::append(1, appended(1));
        let marker = history(|m| assert!(m.append_result(&stream("s"), &result, 0)));
        lang_with_no_config.peeked(marker).unwrap();
        assert_eq!(jobs(&mut lang_with_no_config, 0), ["append 1"]);
    }

    #[test]
    fn a_call_on_a_store_with_no_runtime_fails_the_task() {
        let stores = ["a", "b", "c"];
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            let served: Vec<&str> = stores
                .into_iter()
                .filter(|_| rng.random_bool(0.5))
                .collect();
            let phase = [Phase::Live, Phase::Replaying, Phase::ReplayOnly][rng.random_range(0..3)];
            let mut calls = StreamCalls::new("ns", "r", runtimes(&served), phase);
            calls.set_task(TaskState::Open(epoch(3)));
            let (mut appends, mut closes) = (0, 0);
            let mut unserved = BTreeMap::new();
            for index in 0..rng.random_range(1..=6) {
                let store = stores[rng.random_range(0..3)];
                let stream = StreamRef {
                    id: format!("s{index}"),
                    store: store.to_string(),
                };
                let (call, result) = if rng.random_bool(0.5) {
                    appends += 1;
                    let command = AppendToStream {
                        seq: appends,
                        stream: Some(stream),
                        bodies: vec![Payload::default()],
                        sequence: 0,
                    };
                    (CallKey::Append(appends), calls.append(command))
                } else {
                    closes += 1;
                    let command = CloseStream {
                        seq: closes,
                        stream: Some(stream),
                    };
                    (CallKey::Close(closes), calls.close(command))
                };
                let error = CallError::Unserved(format!(
                    "This Worker has no Stream runtime for the Store `{store}`"
                ));
                let is_served = served.contains(&store);
                if phase == Phase::Live && !is_served {
                    // The command fails the task, so the run records nothing of the call.
                    assert_eq!(result, Err(error), "seed {seed}");
                    let tracked = match call {
                        CallKey::Append(seq) => calls.appends.contains_key(&seq),
                        CallKey::Close(seq) => calls.closes.contains_key(&seq),
                    };
                    assert!(!tracked, "seed {seed}");
                    break;
                }
                assert_eq!(result, Ok(()), "seed {seed}");
                if !is_served {
                    unserved.insert(call, error);
                }
            }
            let marker = calls.take_marker().unwrap_or_default();
            let mut sent = calls.take_requests();
            let at_go_live = calls.go_live();
            sent.extend(calls.take_requests());
            // No call on a Store with no runtime records an intent or reaches a Store.
            let streams: Vec<&str> = (marker.close_intents.iter())
                .map(|intent| marker.streams[intent.stream as usize].store.as_str())
                .chain(sent.iter().map(|request| match request {
                    Request::Run(Call {
                        op: Op::Append(append),
                        ..
                    }) => append.r#ref.as_ref().unwrap().store.as_str(),
                    Request::Run(Call {
                        op: Op::Close(close),
                        ..
                    }) => close.r#ref.as_ref().unwrap().store.as_str(),
                    Request::Stop(_) => panic!("seed {seed}"),
                }))
                .collect();
            assert!(
                streams.iter().all(|store| served.contains(store)),
                "seed {seed}"
            );
            // A replay fails the task when it goes live with an unserved call, which a replay
            // Worker never does.
            let expected = match (phase, unserved.into_values().next()) {
                (Phase::Replaying, Some(error)) => Err(error),
                _ => Ok(()),
            };
            assert_eq!(at_go_live, expected, "seed {seed}");
        }
    }
}
