//! The Stream calls of one run. Nothing here does I/O. The machines of the run drive the calls,
//! and the Worker's [`StreamManager`](super::StreamManager) runs the requests that they start.

use super::{
    MarkerReads, RunGate, StreamRuntimes, TaskRoom, TaskState,
    manager::{Call, Op, Outcome, Request, StreamResolution, failure, next_call_id},
    marker::{Done, MarkerBuilder, ReadOf, status_of},
    reader::{DEFAULT_IDLE_TIMEOUT, Page, PageTaken, ReadCall},
    replay::{ReplayError, needs_records, offset_text},
};
use crate::{
    Reason, StreamError,
    ids::{check_offset, check_ref, check_store_name},
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, Producer, ReadRecordsRequest,
        StreamRecord, StreamRef, WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    },
};
use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt, mem,
    sync::Arc,
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::{
        external_data::{
            StreamAppendResult, StreamCloseResult, StreamMarkerData, StreamReadEnded,
            StreamReadEntry,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, ResolveStreamRead, StreamReadEnd,
            resolve_stream_append, workflow_activation_job,
        },
        workflow_commands::{AppendToStream, CloseStream, CloseStreamRead, StartStreamRead},
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
    /// The Store no longer has the Records that History counts, as the first run read them, so
    /// the run can't replay.
    Gone(String),
}

impl From<ReplayError> for CallError {
    fn from(error: ReplayError) -> Self {
        match error {
            ReplayError::Gone(message) => CallError::Gone(message),
            ReplayError::Unavailable(message) => CallError::Unserved(message),
        }
    }
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
    Read(u32),
}

impl fmt::Display for CallKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallKey::Append(seq) => write!(f, "append {seq}"),
            CallKey::Close(seq) => write!(f, "close {seq}"),
            CallKey::Read(seq) => write!(f, "read {seq}"),
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

struct Read {
    stream: StreamRef,
    records_in_history: bool,
    idle_timeout: Duration,
    /// The offset of the last Record that Workflow code got, or `from_offset`. A reader that runs
    /// again asks after it.
    after: Vec<u8>,
    state: ReadState,
}

enum ReadState {
    /// The read takes its pages from History, and runs once the replay is over.
    Replaying,
    /// The reader runs, and the page that it gave waits here for an activation.
    Running {
        call_id: u64,
        taken: Arc<PageTaken>,
        page: Option<Box<Waiting>>,
    },
    /// A graceful shutdown stopped the reader, so another Worker goes on with the read.
    Parked,
}

/// A page that waits for an activation.
struct Waiting {
    /// The order in which the pages of the run came, so the oldest goes first.
    arrival: u64,
    /// The bytes of its Records.
    bytes: usize,
    page: Page,
}

/// The entries of History's markers whose calls Workflow code hasn't issued yet, by seq.
#[derive(Default)]
struct Held {
    appends: HashMap<u32, (StreamRef, u64, resolve_stream_append::Status)>,
    intents: HashMap<u32, StreamRef>,
    closes: HashMap<u32, (u64, Option<Failure>)>,
    reads: HashMap<u32, HeldRead>,
}

/// A read entry of History, with the Records that the replay has for it.
struct HeldRead {
    stream: StreamRef,
    entry: StreamReadEntry,
    records: Result<Vec<StreamRecord>, ReplayError>,
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
    reads: HashMap<u32, Read>,
    /// The appends and closes of each Stream, in the order that they joined. Only the head runs.
    queues: HashMap<(String, String), VecDeque<CallKey>>,
    last_append: u32,
    last_close: u32,
    last_read: u32,
    /// The live calls whose result waits for an activation.
    done: BTreeSet<CallKey>,
    /// The reads whose page waits for an activation, by the order in which the pages came.
    waiting: BTreeMap<u64, u32>,
    next_arrival: u64,
    /// The failures of calls that Core refused, for the next activation. No marker records them.
    refused: Vec<workflow_activation_job::Variant>,
    held: Held,
    /// History's results whose calls Workflow code issued, by the activation that gets them.
    ready: BTreeMap<(u64, CallKey), workflow_activation_job::Variant>,
    marker: MarkerBuilder,
    room: Arc<TaskRoom>,
    requests: Vec<Request>,
}

impl StreamCalls {
    /// Creates the calls of a new run in `namespace`.
    pub fn new(namespace: &str, run_id: &str, runtimes: StreamRuntimes, phase: Phase) -> Self {
        debug_assert_ne!(phase, Phase::Stopped);
        let marker = MarkerBuilder::default();
        let room = Arc::new(TaskRoom::default());
        room.set(marker.records_room());
        Self {
            namespace: namespace.to_string(),
            run_id: run_id.to_string(),
            producer_id: format!("wf:{}", quote(run_id)),
            runtimes,
            phase,
            gate: RunGate::new(),
            appends: HashMap::new(),
            closes: HashMap::new(),
            reads: HashMap::new(),
            queues: HashMap::new(),
            last_append: 0,
            last_close: 0,
            last_read: 0,
            done: BTreeSet::new(),
            waiting: BTreeMap::new(),
            next_arrival: 0,
            refused: vec![],
            held: Held::default(),
            ready: BTreeMap::new(),
            marker,
            room,
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

    /// Takes a read command. If an input is over its limit or malformed, Workflow code gets an end
    /// with a failure, and no Store sees the read.
    pub fn start_read(&mut self, command: StartStreamRead) -> Result<(), CallError> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        let StartStreamRead {
            seq,
            stream,
            from_offset,
            idle_timeout,
            records_in_history,
        } = command;
        next_seq("read", seq, &mut self.last_read)?;
        let stream =
            stream.ok_or_else(|| CallError::Fatal(format!("Stream read {seq} has no Stream")))?;
        let checked = match over_limit(&stream) {
            Some(error) => Err(error),
            None => check_offset(&from_offset, "The offset", Reason::Invalid)
                .and_then(|()| idle_timeout.map_or(Ok(DEFAULT_IDLE_TIMEOUT), idle_duration)),
        };
        let idle_timeout = match checked {
            Ok(idle_timeout) => idle_timeout,
            Err(error) => {
                let end = StreamReadEnd {
                    done: false,
                    failed: Some(failure(&error)),
                };
                let job = ResolveStreamRead {
                    seq,
                    records: vec![],
                    end: Some(end),
                };
                self.refused.push(job.into());
                return Ok(());
            }
        };
        self.check_runtime(&stream)?;
        let read = Read {
            stream,
            records_in_history,
            idle_timeout,
            after: from_offset,
            state: ReadState::Replaying,
        };
        self.reads.insert(seq, read);
        self.match_read(seq)?;
        if self.phase == Phase::Live {
            self.run_read(seq);
        }
        Ok(())
    }

    /// Takes the close of a read. Its reader stops, the page that waits is dropped, and Workflow
    /// code gets no job for it.
    pub fn close_read(&mut self, command: CloseStreamRead) -> Result<(), CallError> {
        let seq = command.seq;
        if let Some(read) = self.reads.remove(&seq) {
            stop_reader(read.state, &mut self.requests, &mut self.waiting);
        }
        // The first run got no page of the read after its close.
        if self
            .ready
            .keys()
            .any(|(_, call)| *call == CallKey::Read(seq))
        {
            return Err(CallError::Nondeterminism(format!(
                "History has a page of Stream read {seq} after Workflow code closed it"
            )));
        }
        Ok(())
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
            Outcome::Page(page) => {
                let Some(read) = self.reads.get_mut(&seq) else {
                    return;
                };
                let ReadState::Running {
                    call_id: id,
                    page: held,
                    ..
                } = &mut read.state
                else {
                    return;
                };
                if *id != call_id {
                    return;
                }
                debug_assert!(
                    held.is_none(),
                    "A reader gives a page once the run took its last one"
                );
                let arrival = self.next_arrival;
                self.next_arrival += 1;
                let bytes = page.records.iter().map(Message::encoded_len).sum();
                *held = Some(Box::new(Waiting {
                    arrival,
                    bytes,
                    page,
                }));
                self.waiting.insert(arrival, seq);
                return;
            }
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
            || self.next_page_fits(activation) == Some(true)
    }

    /// Returns the results for the activation at index `activation` of the task. The marker
    /// records each live result with that index.
    pub fn take_jobs(&mut self, activation: u64) -> Vec<workflow_activation_job::Variant> {
        let mut jobs = std::mem::take(&mut self.refused);
        while let Some(entry) = self.ready.first_entry()
            && entry.key().0 <= activation
        {
            let ((_, call), job) = entry.remove_entry();
            // A replayed read leaves once Workflow code gets its end.
            if !matches!(&job, workflow_activation_job::Variant::ResolveStreamRead(read)
                if read.end.is_none())
            {
                self.leave(call);
            }
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
        // The oldest page goes first, so no read keeps another out of a task. The jobs go by seq,
        // as a replay gives them.
        let mut pages = vec![];
        while let Some((&arrival, &seq)) = self.waiting.first_key_value() {
            let Some(job) = self.take_page(seq, activation) else {
                break;
            };
            self.waiting.remove(&arrival);
            pages.push((seq, job));
        }
        pages.sort_by_key(|(seq, _)| *seq);
        jobs.extend(pages.into_iter().map(|(_, job)| job));
        jobs
    }

    /// Holds the entries of a marker from History, each for its call, with the Records that the
    /// replay read for its read entries. The decode of the marker already checked each Stream
    /// index.
    pub fn peeked(
        &mut self,
        data: StreamMarkerData,
        mut replayed: MarkerReads,
    ) -> Result<(), CallError> {
        let StreamMarkerData {
            streams,
            close_intents,
            appends,
            closes,
            reads,
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
        let mut seen = HashSet::new();
        for (index, mut entry) in reads.into_iter().enumerate() {
            let seq = entry.seq;
            if !seen.insert(seq) {
                return Err(second("result", CallKey::Read(seq)));
            }
            let records = if needs_records(&entry) {
                (replayed.get_mut(index).and_then(Option::take)).unwrap_or_else(|| {
                    Err(ReplayError::Unavailable(format!(
                        "Core read no Records of Stream read {seq} for its replay"
                    )))
                })
            } else {
                Ok(mem::take(&mut entry.records))
            };
            let stream = streams[entry.stream as usize].clone();
            let held = HeldRead {
                stream,
                entry,
                records,
            };
            self.held.reads.insert(seq, held);
            self.match_read(seq)?;
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
        if let Some(seq) = self.held.reads.keys().min() {
            return Err(never("a result", CallKey::Read(*seq)));
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
            .chain((self.reads.iter()).map(|(seq, read)| (CallKey::Read(*seq), &read.stream)))
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
        let reads: Vec<u32> = self.reads.keys().copied().collect();
        for seq in reads {
            self.run_read(seq);
        }
        self.start_heads();
        Ok(())
    }

    /// Stops the appends and the reads at a command that ends the Workflow, so an append that
    /// still waits is never sent, and the page of a read that waits is dropped. The closes go on.
    pub fn stop_for_terminal(&mut self) -> Result<(), CallError> {
        for read in mem::take(&mut self.reads).into_values() {
            stop_reader(read.state, &mut self.requests, &mut self.waiting);
        }
        // The first run got no page of a read after the command.
        if let Some((_, call)) =
            (self.ready.keys()).find(|(_, call)| matches!(call, CallKey::Read(_)))
        {
            return Err(CallError::Nondeterminism(format!(
                "History has a page of Stream {call} after the Workflow's last command"
            )));
        }
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
        Ok(())
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

    /// Returns true while a call has no result in a marker, or a read has no end in one, so the
    /// run needs another task.
    pub fn needs_next_task(&self) -> bool {
        !self.appends.is_empty() || !self.closes.is_empty() || !self.reads.is_empty()
    }

    /// Returns true if the marker of the task has an entry.
    pub fn has_marker(&self) -> bool {
        !self.marker.is_empty()
    }

    /// Returns true once the marker of the task has no room for a result or a page that waits, or
    /// the task can give no more Records while a read runs. The task must then complete with a
    /// new one forced.
    pub fn must_complete(&self, activation: u64) -> bool {
        let (task, marker) = self.marker.records_room();
        let in_history = (self.reads.values()).any(|read| read.records_in_history);
        let room_spent = self.gate.is_open()
            && !self.reads.is_empty()
            && (task == 0 || (marker == 0 && in_history));
        self.next_result_fits(activation) == Some(false)
            || self.next_page_fits(activation) == Some(false)
            || room_spent
    }

    /// Returns the number of live calls that hold the task open, as a Local Activity does.
    pub fn outstanding(&self) -> usize {
        if self.phase != Phase::Live {
            return 0;
        }
        self.outstanding_writes() + self.running_reads()
    }

    /// Returns true if only reads hold the task open. A command, a message or a close intent for
    /// the server must not wait for a read.
    pub fn holds_only_reads(&self) -> bool {
        self.phase == Phase::Live && self.outstanding_writes() == 0 && self.running_reads() > 0
    }

    /// Returns true while a close waits for a marker to take its intent.
    pub fn has_intents(&self) -> bool {
        (self.closes.values()).any(|close| matches!(close.state, CloseState::Asked))
    }

    /// Stops each read with no end, at a graceful shutdown of the Worker. The page that waits is
    /// dropped, and another Worker goes on with the read from History.
    pub fn park_reads(&mut self) {
        for read in self.reads.values_mut() {
            let state = mem::replace(&mut read.state, ReadState::Parked);
            stop_reader(state, &mut self.requests, &mut self.waiting);
        }
    }

    /// Returns true while a read that a shutdown stopped waits for another Worker.
    pub fn has_parked_reads(&self) -> bool {
        (self.reads.values()).any(|read| matches!(read.state, ReadState::Parked))
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
        let marker = self.marker.take();
        self.room.set(self.marker.records_room());
        marker
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
        self.reads.clear();
        self.queues.clear();
        self.done.clear();
        self.waiting.clear();
        self.refused.clear();
        self.held = Held::default();
        self.ready.clear();
        self.marker = MarkerBuilder::default();
        self.requests.clear();
    }

    fn outstanding_writes(&self) -> usize {
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

    fn running_reads(&self) -> usize {
        (self.reads.values())
            .filter(|read| matches!(read.state, ReadState::Running { .. }))
            .count()
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

    /// Gives a replayed read the pages of its entry in History. The entry must be of the read that
    /// Workflow code issued, from where Workflow code is, and only then does a failed replay of
    /// its Records count.
    fn match_read(&mut self, seq: u32) -> Result<(), CallError> {
        let Some(read) = self.reads.get_mut(&seq) else {
            return Ok(());
        };
        let Some(held) = self.held.reads.remove(&seq) else {
            return Ok(());
        };
        check_stream(CallKey::Read(seq), &read.stream, &held.stream)?;
        if held.entry.after_offset != read.after {
            return Err(CallError::Nondeterminism(format!(
                "Stream read {seq} gives the Records after {}, and History has them after {}",
                offset_text(&read.after),
                offset_text(&held.entry.after_offset)
            )));
        }
        let mut records = held.records?.into_iter();
        let StreamReadEntry {
            page_records,
            page_activations,
            end,
            ..
        } = held.entry;
        for (count, activation) in page_records.into_iter().zip(page_activations) {
            let records: Vec<StreamRecord> = records.by_ref().take(count as usize).collect();
            if let Some(last) = records.last() {
                read.after.clone_from(&last.offset);
            }
            let job = ResolveStreamRead {
                seq,
                records,
                end: None,
            };
            self.ready
                .insert((activation, CallKey::Read(seq)), job.into());
        }
        if let Some(StreamReadEnded {
            activation,
            done,
            failure,
        }) = end
        {
            let end = StreamReadEnd {
                done,
                failed: failure,
            };
            // The end comes in the job of the last page when they came in one activation.
            let job = (self.ready)
                .entry((activation, CallKey::Read(seq)))
                .or_insert_with(|| {
                    ResolveStreamRead {
                        seq,
                        ..Default::default()
                    }
                    .into()
                });
            if let workflow_activation_job::Variant::ResolveStreamRead(job) = job {
                job.end = Some(end);
            }
        }
        Ok(())
    }

    /// Starts the reader of read `seq`, after the last Record that Workflow code got.
    fn run_read(&mut self, seq: u32) {
        let read = self.reads.get_mut(&seq).expect("The read was just listed");
        let call_id = next_call_id();
        let taken = Arc::new(PageTaken::default());
        let call = ReadCall {
            request: ReadRecordsRequest {
                namespace: self.namespace.clone(),
                r#ref: Some(read.stream.clone()),
                from_offset: read.after.clone(),
                ..Default::default()
            },
            idle_timeout: read.idle_timeout,
            records_in_history: read.records_in_history,
            gate: self.gate.subscribe(),
            room: self.room.clone(),
            taken: taken.clone(),
        };
        read.state = ReadState::Running {
            call_id,
            taken,
            page: None,
        };
        let op = Op::Read(call);
        self.requests.push(Request::Run(Call { call_id, seq, op }));
    }

    /// Returns whether the marker and the task have room for the oldest page that waits, if the
    /// task is open.
    fn next_page_fits(&self, activation: u64) -> Option<bool> {
        let (_, seq) = self.waiting.first_key_value()?;
        if !self.gate.is_open() {
            return None;
        }
        let read = &self.reads[seq];
        let ReadState::Running {
            page: Some(waiting),
            ..
        } = &read.state
        else {
            return None;
        };
        let of = ReadOf {
            seq: *seq,
            stream: &read.stream,
            after: &read.after,
            records_in_history: read.records_in_history,
        };
        Some((self.marker).has_room_for_page(&of, &waiting.page, waiting.bytes, activation))
    }

    /// Records the page of read `seq` in the marker, tells its reader that the page was taken, and
    /// returns its job. Returns `None` if the marker or the task has no room for it.
    fn take_page(&mut self, seq: u32, activation: u64) -> Option<workflow_activation_job::Variant> {
        let Read {
            stream,
            records_in_history,
            after,
            state,
            ..
        } = self.reads.get_mut(&seq)?;
        let ReadState::Running { taken, page, .. } = state else {
            return None;
        };
        let waiting = page.as_ref()?;
        let of = ReadOf {
            seq,
            stream,
            after,
            records_in_history: *records_in_history,
        };
        if !(self.marker).read_page(&of, &waiting.page, waiting.bytes, activation) {
            return None;
        }
        let Page { records, end } = page.take()?.page;
        // The reader asks again as soon as it learns that the page was taken, with the room that
        // this page left.
        self.room.set(self.marker.records_room());
        taken.notify();
        if let Some(last) = records.last() {
            after.clone_from(&last.offset);
        }
        if end.is_some() {
            self.reads.remove(&seq);
        }
        let job = ResolveStreamRead { seq, records, end };
        Some(job.into())
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
            // A page waits with its read, never in `done`.
            CallKey::Read(_) => None,
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
            CallKey::Read(_) => None,
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
            // A read is in no queue.
            CallKey::Read(_) => return,
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
            CallKey::Read(seq) => {
                self.reads.remove(&seq);
                None
            }
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

/// Returns the idle timeout of a read, or a failure for a duration that is negative or that has
/// its nanoseconds out of range.
fn idle_duration(idle: prost_types::Duration) -> Result<Duration, StreamError> {
    let prost_types::Duration { seconds, nanos } = idle;
    match (u64::try_from(seconds), u32::try_from(nanos)) {
        (Ok(whole), Ok(part)) if part < 1_000_000_000 => Ok(Duration::new(whole, part)),
        _ => Err(StreamError::invalid(format!(
            "The idle_timeout of the read has {seconds} seconds and {nanos} nanoseconds, which is \
             negative or has its nanoseconds out of range"
        ))),
    }
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

/// Stops the reader of a read in `state`, and drops the page that waits. A reader that the Worker
/// didn't start yet never starts.
fn stop_reader(state: ReadState, requests: &mut Vec<Request>, waiting: &mut BTreeMap<u64, u32>) {
    if let ReadState::Running { call_id, page, .. } = state {
        let queued = (requests.iter())
            .position(|request| matches!(request, Request::Run(call) if call.call_id == call_id));
        match queued {
            Some(at) => {
                requests.remove(at);
            }
            None => requests.push(Request::Stop(call_id)),
        }
        if let Some(page) = page {
            waiting.remove(&page.arrival);
        }
    }
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
        workflow::{
            digest::Digest,
            marker::{MAX_MARKER_ENTRY_BYTES, MAX_MARKER_RECORD_BYTES, MAX_TASK_RECORD_BYTES},
        },
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
                        Op::Read(_) => return format!("read {seq}"),
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
            CallKey::Read(_) => panic!("{call} has pages, not a result"),
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
        replay.peeked(marker, vec![]).unwrap();
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
        let marker = history(|marker| {
            assert!(marker.close_intent(&stream("u"), 1));
            assert!(marker.append_result(&stream("s"), &Done::append(1, appended(1)), 0));
        });
        replay.peeked(marker, vec![]).unwrap();
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
        replay.peeked(marker, vec![]).unwrap();
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
        calls.stop_for_terminal().unwrap();
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
        replay.peeked(intent, vec![]).unwrap();
        replay.marker_recorded().unwrap();
        replay.peeked(result, vec![]).unwrap();
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
        StartRead(StartStreamRead),
        CloseRead(CloseStreamRead),
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
                    Command::StartRead(read) => self.calls.start_read(read.clone())?,
                    Command::CloseRead(read) => self.calls.close_read(*read)?,
                    Command::Terminal => {
                        self.calls.stop_for_terminal()?;
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

        /// Returns the calls that the run still tracks.
        fn tracked(&self) -> BTreeSet<CallKey> {
            (self.calls.appends.keys().map(|seq| CallKey::Append(*seq)))
                .chain(self.calls.closes.keys().map(|seq| CallKey::Close(*seq)))
                .chain(self.calls.reads.keys().map(|seq| CallKey::Read(*seq)))
                .collect()
        }
    }

    /// A Stream that the reads of a generated script read, as its Store has it.
    struct ModelStream {
        records: Vec<StreamRecord>,
        closed: bool,
    }

    type Streams = BTreeMap<String, ModelStream>;

    /// Returns the Records of `stream` after `offset`.
    fn after<'a>(stream: &'a ModelStream, offset: &[u8]) -> &'a [StreamRecord] {
        let from = (stream.records.iter()).position(|record| record.offset.as_slice() > offset);
        &stream.records[from.unwrap_or(stream.records.len())..]
    }

    /// The reads of `history` that keep their Records in History.
    fn in_history(history: &[Task]) -> BTreeSet<u32> {
        (history.iter())
            .flat_map(|task| &task.activations)
            .flat_map(|activation| &activation.commands)
            .filter_map(|command| match command {
                Command::StartRead(read) if read.records_in_history => Some(read.seq),
                _ => None,
            })
            .collect()
    }

    /// Returns what the Store gives a replay for the read entries of the marker of task `task`.
    /// `gone` lists the entries whose Records the Store lost, with the message of each.
    fn store_reads(
        task: usize,
        marker: &StreamMarkerData,
        streams: &Streams,
        in_history: &BTreeSet<u32>,
        gone: &dyn Fn(usize, usize) -> Option<String>,
    ) -> MarkerReads {
        (marker.reads.iter().enumerate())
            .map(|(index, entry)| {
                let count: usize = entry.page_records.iter().map(|count| *count as usize).sum();
                if in_history.contains(&entry.seq) || count == 0 {
                    return None;
                }
                if let Some(message) = gone(task, index) {
                    return Some(Err(ReplayError::Gone(message)));
                }
                let stream = &streams[&marker.streams[entry.stream as usize].id];
                let records = after(stream, &entry.after_offset).iter().take(count);
                Some(Ok(records.cloned().collect()))
            })
            .collect()
    }

    /// What a replay gives.
    struct Replayed {
        jobs: Vec<TaskJobs>,
        /// The task in which the last command of the Workflow went out.
        sent_in: Option<usize>,
        run: Run,
    }

    /// Replays `history` with the commands of the first run of `seed`, and goes live in the task
    /// `live`, as a new run of Core does. The Store has the Records of `streams`, but for the read
    /// entries that `gone` lists.
    fn replay(
        seed: u64,
        history: &[Task],
        live: WorkflowTaskEpoch,
        streams: &Streams,
        gone: &dyn Fn(usize, usize) -> Option<String>,
    ) -> Result<Replayed, CallError> {
        let in_history = in_history(history);
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
                let reads = store_reads(index, marker, streams, &in_history, gone);
                run.calls.peeked(marker.clone(), reads)?;
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
        pages: usize,
        read_ends: usize,
        replay_errors: usize,
        /// Tasks that completed with a page that didn't fit, which the next task gave.
        full_rooms: usize,
    }

    /// The reader of a read in a generated script. It gives the run pages of its Stream, one at a
    /// time, as the reader of Core does.
    struct ModelReader {
        call_id: u64,
        seq: u32,
        stream: String,
        /// The index of the next Record of the Stream that the reader gives.
        next: usize,
        call: ReadCall,
        /// The maximum bytes of the page that the reader asked the Store for, while it waits for
        /// the answer.
        asked: Option<usize>,
    }

    /// The state as of the last task in History, which a failed task goes back to.
    #[derive(Clone, Default)]
    struct Committed {
        seqs: (u32, u32, u32),
        unresolved: BTreeSet<CallKey>,
        refused: BTreeSet<CallKey>,
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
        readers: Vec<ModelReader>,
        streams: Streams,
        /// Results that came while no task was in hand. Core applies them once the next task's
        /// events are applied.
        deferred: Vec<StreamResolution>,
        last_append: u32,
        last_close: u32,
        last_read: u32,
        /// The calls whose result no marker has, and the reads whose end no marker has.
        unresolved: BTreeSet<CallKey>,
        /// The calls that Core refused for an input over its limit.
        refused: BTreeSet<CallKey>,
        committed: Committed,
    }

    impl<'a> FirstRun<'a> {
        fn new(seed: u64, seen: &'a mut Seen) -> Self {
            let run = Run {
                calls: StreamCalls::new("ns", "r", runtimes(&["mem"]), Phase::Live),
                activation: 0,
                end: End::Running,
            };
            let mut rng = StdRng::seed_from_u64(seed);
            // In a few seeds, large Records fill the room of a marker, so a page waits for the
            // next task.
            let big = rng.random_bool(0.08);
            let streams = (0..2)
                .map(|index| {
                    let records = (1..=rng.random_range(0u8..30))
                        .map(|at| {
                            let mut data = vec![index, at];
                            if big {
                                data.resize(rng.random_range(100_000..700_000), 0);
                            }
                            StreamRecord {
                                offset: vec![at * 3],
                                body: Some(Payload {
                                    data,
                                    ..Default::default()
                                }),
                                producer: None,
                            }
                        })
                        .collect();
                    let closed = rng.random_bool(0.5);
                    (format!("r{index}"), ModelStream { records, closed })
                })
                .collect();
            Self {
                seed,
                rng,
                seen,
                run,
                history: vec![],
                task: Task::default(),
                epoch: epoch(3),
                in_flight: vec![],
                readers: vec![],
                streams,
                deferred: vec![],
                last_append: 0,
                last_close: 0,
                last_read: 0,
                unresolved: BTreeSet::new(),
                refused: BTreeSet::new(),
                committed: Committed::default(),
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
                    self.seen.full_rooms += usize::from(!self.run.calls.waiting.is_empty());
                    self.complete();
                    return Ok(());
                }
                if self.run.calls.outstanding() == 0 {
                    self.complete();
                    return Ok(());
                }
                let seed = self.seed;
                assert!(
                    !self.in_flight.is_empty() || !self.readers.is_empty(),
                    "seed {seed}: a call waits, and none runs"
                );
                match self.rng.random_range(0..40) {
                    // A few answers can come before Core gives the next activation.
                    0..=35 => {
                        for _ in 0..self.rng.random_range(1..=8) {
                            if let Some(resolution) = self.step() {
                                self.run.calls.resolve(resolution);
                                self.run.continue_hold();
                                self.dispatch();
                            }
                        }
                    }
                    36..=38 => {
                        // The heartbeat completes the task. Results that come before the
                        // completion wait for the next task.
                        self.seen.heartbeats += 1;
                        self.run.calls.set_task(TaskState::Completing(self.epoch));
                        for _ in 0..self.rng.random_range(0..=2) {
                            if let Some(resolution) = self.step() {
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

        /// Lets the Store answer a random call in flight, or lets a random reader ask the Store or
        /// get its answer. Returns the result that comes, if one does.
        fn step(&mut self) -> Option<StreamResolution> {
            let actors = self.in_flight.len() + self.readers.len();
            if actors == 0 {
                return None;
            }
            match self.rng.random_range(0..actors) {
                at if at < self.in_flight.len() => Some(self.answer()),
                at => self.read(at - self.in_flight.len()),
            }
        }

        /// Lets reader `index` ask the Store, or gives it the Store's answer to what it asked. The
        /// answer has Records of any number up to what the reader asked for, an end at any point,
        /// or nothing yet while no Record came.
        fn read(&mut self, index: usize) -> Option<StreamResolution> {
            let reader = &mut self.readers[index];
            let Some(max_bytes) = reader.asked else {
                // A reader asks only once the run took its last page, with the task open and room.
                let state = self
                    .run
                    .calls
                    .reads
                    .get(&reader.seq)
                    .map(|read| &read.state);
                let free = matches!(state,
                    Some(ReadState::Running { call_id, page: None, .. })
                        if *call_id == reader.call_id);
                let open = matches!(*reader.call.gate.borrow(), TaskState::Open(_));
                let room = reader.call.room_left();
                if free && open && room > 0 {
                    let ahead = self.run.calls.runtimes.settings.read_ahead_bytes;
                    reader.asked = Some(room.min(ahead));
                }
                return None;
            };
            let stream = &self.streams[&reader.stream];
            let left = &stream.records[reader.next..];
            let rng = &mut self.rng;
            let end = |done| StreamReadEnd { done, failed: None };
            let page = if rng.random_bool(0.02) {
                let error = StreamError::invalid(format!("Read {} failed", reader.seq));
                let failed = Some(failure(&error));
                Page {
                    records: vec![],
                    end: Some(StreamReadEnd {
                        done: false,
                        failed,
                    }),
                }
            } else if !left.is_empty() {
                // A page is at most what the reader asked for, or one larger Record.
                let mut bytes = 0;
                let count = (left[..rng.random_range(1..=left.len().min(8))].iter())
                    .take_while(|record| {
                        bytes += record.encoded_len();
                        bytes <= max_bytes
                    })
                    .count()
                    .max(1);
                let done = stream.closed && count == left.len();
                Page {
                    records: left[..count].to_vec(),
                    end: done.then(|| end(true)),
                }
            } else if stream.closed {
                Page {
                    records: vec![],
                    end: Some(end(true)),
                }
            } else if rng.random_bool(0.3) {
                // No Record came for the idle time.
                Page {
                    records: vec![],
                    end: Some(end(false)),
                }
            } else {
                return None;
            };
            reader.next += page.records.len();
            reader.asked = None;
            self.seen.pages += 1;
            let (call_id, seq) = (reader.call_id, reader.seq);
            if page.end.is_some() {
                self.seen.read_ends += 1;
                self.readers.swap_remove(index);
            }
            Some(StreamResolution {
                call_id,
                seq,
                outcome: Outcome::Page(page),
            })
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
                    Command::StartRead(read) if !self.is_refused(CallKey::Read(read.seq)) => {
                        self.unresolved.insert(CallKey::Read(read.seq));
                    }
                    Command::CloseRead(read) => {
                        self.unresolved.remove(&CallKey::Read(read.seq));
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
                    0..=37 => {
                        self.last_append += 1;
                        commands.push(Command::Append(append(self.last_append, &id)));
                    }
                    38..=44 => {
                        // A read from the start of its Stream, or from one of its Records.
                        self.last_read += 1;
                        let id = format!("r{}", self.rng.random_range(0..2));
                        let records = &self.streams[&id].records;
                        let from_offset = match self.rng.random_range(0..=records.len()) {
                            0 => vec![],
                            at => records[at - 1].offset.clone(),
                        };
                        commands.push(Command::StartRead(StartStreamRead {
                            seq: self.last_read,
                            stream: Some(stream(&id)),
                            from_offset,
                            idle_timeout: None,
                            records_in_history: self.rng.random_bool(0.5),
                        }));
                    }
                    45..=66 => {
                        self.last_close += 1;
                        commands.push(Command::Close(close(self.last_close, &id)));
                    }
                    67..=69 => {
                        let open: Vec<u32> = (self.unresolved.iter())
                            .filter_map(|call| match call {
                                CallKey::Read(seq) => Some(*seq),
                                _ => None,
                            })
                            .collect();
                        if !open.is_empty() {
                            let seq = open[self.rng.random_range(0..open.len())];
                            commands.push(Command::CloseRead(CloseStreamRead { seq }));
                        }
                    }
                    70..=79 => {
                        // An input over its limit gets a failure, and no Store sees the call.
                        self.seen.refused += 1;
                        let long = "x".repeat(4097);
                        match self.rng.random_range(0..5) {
                            3 => {
                                self.last_read += 1;
                                self.refused.insert(CallKey::Read(self.last_read));
                                commands.push(Command::StartRead(StartStreamRead {
                                    seq: self.last_read,
                                    stream: Some(stream("r0")),
                                    from_offset: vec![0; 1025],
                                    ..Default::default()
                                }));
                            }
                            4 => {
                                // A negative idle timeout, or one with its nanoseconds out of
                                // range.
                                let (seconds, nanos) = match self.rng.random_bool(0.5) {
                                    true => (self.rng.random_range(i64::MIN..0), 0),
                                    false => match self.rng.random_bool(0.5) {
                                        true => {
                                            (0, self.rng.random_range(1_000_000_000..=i32::MAX))
                                        }
                                        false => (0, self.rng.random_range(i32::MIN..0)),
                                    },
                                };
                                self.last_read += 1;
                                self.refused.insert(CallKey::Read(self.last_read));
                                commands.push(Command::StartRead(StartStreamRead {
                                    seq: self.last_read,
                                    stream: Some(stream("r0")),
                                    idle_timeout: Some(prost_types::Duration { seconds, nanos }),
                                    ..Default::default()
                                }));
                            }
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
                        let error = |kind: &str, seq: u32| match seq {
                            0 => format!("Stream {kind} has seq 0, and a seq starts at 1"),
                            _ => format!("Stream {kind} {seq} came after {kind} {seq}"),
                        };
                        let (command, error) = match self.rng.random_range(0..3) {
                            0 => {
                                let seq = self.last_append;
                                (Command::Append(append(seq, &id)), error("append", seq))
                            }
                            1 => {
                                let seq = self.last_close;
                                (Command::Close(close(seq, &id)), error("close", seq))
                            }
                            _ => {
                                let seq = self.last_read;
                                let read = StartStreamRead {
                                    seq,
                                    stream: Some(stream("r0")),
                                    ..Default::default()
                                };
                                (Command::StartRead(read), error("read", seq))
                            }
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
                Op::Read(_) => panic!("A read is never in flight"),
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
                            Op::Read(_) => CallKey::Read(call.seq),
                        };
                        let seed = self.seed;
                        assert!(!self.is_refused(key), "seed {seed}: a refused {key} ran");
                        let Call { call_id, seq, op } = call;
                        let Op::Read(read) = op else {
                            self.in_flight.push(Call { call_id, seq, op });
                            continue;
                        };
                        let stream = read.request.r#ref.as_ref().unwrap().id.clone();
                        let records = &self.streams[&stream];
                        let next =
                            records.records.len() - after(records, &read.request.from_offset).len();
                        self.readers.push(ModelReader {
                            call_id,
                            seq,
                            stream,
                            next,
                            call: read,
                            asked: None,
                        });
                    }
                    Request::Stop(call_id) => {
                        self.in_flight.retain(|call| call.call_id != call_id);
                        self.readers.retain(|reader| reader.call_id != call_id);
                    }
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
                for read in entry.reads.iter().filter(|read| read.end.is_some()) {
                    self.unresolved.remove(&CallKey::Read(read.seq));
                }
            }
            let mut task = std::mem::take(&mut self.task);
            task.marker = marker;
            task.ends_workflow = self.run.end == End::Sent;
            self.history.push(task);
            self.committed = Committed {
                seqs: (self.last_append, self.last_close, self.last_read),
                unresolved: self.unresolved.clone(),
                refused: self.refused.clone(),
            };
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
                if let Some(resolution) = self.step() {
                    self.deferred.push(resolution);
                }
            }
        }

        /// Fails the task in hand. A new run replays History and goes live in the next attempt.
        fn retry(&mut self) {
            self.seen.failed_tasks += 1;
            self.run.calls.stop();
            self.in_flight.clear();
            self.readers.clear();
            self.deferred.clear();
            let committed = self.committed.clone();
            (self.last_append, self.last_close, self.last_read) = committed.seqs;
            (self.unresolved, self.refused) = (committed.unresolved, committed.refused);
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
            } = replay(seed, &self.history, live, &self.streams, &|_, _| None)
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
            let reads = self.reads();
            for request in run.calls.take_requests() {
                let Request::Run(Call { seq, op, .. }) = &request else {
                    panic!("seed {seed}: {request:?}");
                };
                let (key, producer) = match op {
                    Op::Append(append) => (CallKey::Append(*seq), &append.producer),
                    Op::Close(close) => (CallKey::Close(*seq), &close.producer),
                    Op::Read(read) => {
                        // A read goes on after the last Record that Workflow code got.
                        let got = &reads[seq];
                        assert!(got.end.is_none(), "seed {seed}: read {seq} ran again");
                        let last = got.records.last().map(|record| &record.offset);
                        let after = last.unwrap_or(&got.from_offset);
                        assert_eq!(read.request.from_offset, *after, "seed {seed}: read {seq}");
                        run.calls.requests.push(request);
                        continue;
                    }
                };
                assert!(!recorded.contains(&key), "seed {seed}: {key} ran again");
                assert_eq!(*producer, Some(producer_of(live)), "seed {seed}");
                run.calls.requests.push(request);
            }
            run
        }

        /// Returns what Workflow code got of each read of History, and checks that it got the
        /// Records of the read's Stream in order, each once, with the end only after the last.
        fn reads(&self) -> BTreeMap<u32, ReadSeen> {
            let seed = self.seed;
            let mut reads = BTreeMap::new();
            let activations = (self.history.iter()).flat_map(|task| &task.activations);
            for activation in activations {
                for job in &activation.jobs {
                    let Variant::ResolveStreamRead(job) = job else {
                        continue;
                    };
                    let read: &mut ReadSeen = reads.entry(job.seq).or_default();
                    assert!(
                        read.end.is_none(),
                        "seed {seed}: read {} gave a job after its end",
                        job.seq
                    );
                    read.records.extend(job.records.iter().cloned());
                    read.end.clone_from(&job.end);
                }
                for command in &activation.commands {
                    if let Command::StartRead(start) = command
                        && !self.is_refused(CallKey::Read(start.seq))
                    {
                        let read = reads.entry(start.seq).or_default();
                        read.stream = start.stream.as_ref().unwrap().id.clone();
                        read.from_offset.clone_from(&start.from_offset);
                    }
                }
            }
            for (seq, read) in &reads {
                if read.stream.is_empty() {
                    // Core refused the read, and gave only an end with the failure.
                    let failed = (read.end.as_ref()).and_then(|end| end.failed.as_ref());
                    let info = failed.and_then(|failure| failure.failure_info.as_ref());
                    assert!(
                        read.records.is_empty()
                            && matches!(info, Some(FailureInfo::ApplicationFailureInfo(info))
                                if info.r#type == "stream_invalid"),
                        "seed {seed}: read {seq}: {:?}",
                        read.end
                    );
                    continue;
                }
                let stream = &self.streams[&read.stream];
                let after = after(stream, &read.from_offset);
                assert!(after.starts_with(&read.records), "seed {seed}: read {seq}");
                if read.end.as_ref().is_some_and(|end| end.done) {
                    assert!(
                        stream.closed && after.len() == read.records.len(),
                        "seed {seed}"
                    );
                }
            }
            reads
        }

        /// Replays History with a random set of read entries whose Records the Store lost, and
        /// checks that the replay fails with the error of the first that Workflow code reaches.
        /// It reaches an entry at the start of the entry's task if it started the read before,
        /// or else at the command that starts the read.
        fn check_lost_records(&mut self, live: WorkflowTaskEpoch) {
            let seed = self.seed;
            let mut started = BTreeMap::new();
            for (task, of) in self.history.iter().enumerate() {
                for (at, activation) in of.activations.iter().enumerate() {
                    for (position, command) in activation.commands.iter().enumerate() {
                        if let Command::StartRead(read) = command {
                            started.insert(read.seq, (task, at, position));
                        }
                    }
                }
            }
            let in_history = in_history(&self.history);
            let mut reached = vec![];
            for (task, of) in self.history.iter().enumerate() {
                let Some(marker) = &of.marker else {
                    continue;
                };
                for (index, entry) in marker.reads.iter().enumerate() {
                    if in_history.contains(&entry.seq) || entry.page_records.is_empty() {
                        continue;
                    }
                    let reach = match started[&entry.seq] {
                        (at, ..) if at < task => (task, 0, index, 0),
                        (_, activation, position) => (task, 1, activation, position),
                    };
                    reached.push((reach, (task, index)));
                }
            }
            let lost: BTreeSet<(usize, usize)> = (reached.iter())
                .filter(|_| self.rng.random_bool(0.4))
                .map(|(_, entry)| *entry)
                .collect();
            let Some((_, (task, index))) = (reached.iter())
                .filter(|(_, entry)| lost.contains(entry))
                .min()
            else {
                return;
            };
            let message = |task: usize, index: usize| format!("Entry {index} of task {task}");
            let gone = |task, index| lost.contains(&(task, index)).then(|| message(task, index));
            let error = replay(seed, &self.history, live, &self.streams, &gone).err();
            assert_eq!(
                error,
                Some(CallError::Gone(message(*task, *index))),
                "seed {seed}"
            );
            self.seen.replay_errors += 1;
        }
    }

    /// What Workflow code got of one read in History.
    #[derive(Default)]
    struct ReadSeen {
        stream: String,
        from_offset: Vec<u8>,
        records: Vec<StreamRecord>,
        end: Option<StreamReadEnd>,
    }

    fn producer_of(epoch: WorkflowTaskEpoch) -> Producer {
        producer("wf:r", "r", epoch)
    }

    // Reads give Workflow code the Records of their Streams in order, each once, also over failed
    // tasks, heartbeats, full rooms and the last command. A replay of a read with no Records in
    // History reads them from the Store, and one with `records_in_history` reads nothing.
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
            first.check_lost_records(live);
        }
        // The scripts reach each case that the test is for.
        let Seen {
            full_markers,
            heartbeats,
            failed_tasks,
            holds,
            refused,
            bad_seqs,
            pages,
            read_ends,
            replay_errors,
            full_rooms,
        } = seen;
        let counts = [
            full_markers,
            heartbeats,
            failed_tasks,
            holds,
            refused,
            bad_seqs,
            pages,
            read_ends,
            replay_errors,
            full_rooms,
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
                                Op::Read(_) => panic!("The test starts no read"),
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
                        calls.stop_for_terminal().unwrap();
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
                        (CallKey::Read(_), _) => panic!("The test starts no read"),
                    };
                    let seq = match call {
                        CallKey::Append(seq) | CallKey::Close(seq) | CallKey::Read(seq) => seq,
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
            Op::Read(_) => panic!("A read is never in flight"),
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
            (CallKey::Read(_), _) => panic!("A read is never in flight"),
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
            Variant::ResolveStreamRead(read) => CallKey::Read(read.seq),
            other => panic!("{other:?}"),
        }
    }

    /// The reader of a read in the test of the marker bounds. It makes up its Records as it goes.
    struct PageMaker {
        call_id: u64,
        call: ReadCall,
        /// The offset of the next Record that it makes.
        next: u32,
        /// The pages that it gives before the one that ends the read.
        pages: usize,
    }

    /// Returns a page of up to `max_bytes` of new Records, or one larger Record, as a Store gives
    /// it. Rarely a Record is larger than a marker holds.
    fn make_page(rng: &mut StdRng, maker: &mut PageMaker, max_bytes: usize, big: bool) -> Page {
        let mut records = vec![];
        let mut bytes = 0;
        for _ in 0..rng.random_range(1..=8) {
            let size = match big {
                true => rng.random_range(100_000..1_500_000),
                false => rng.random_range(0..300),
            };
            let record = StreamRecord {
                offset: maker.next.to_be_bytes().to_vec(),
                body: Some(Payload {
                    data: vec![0; size],
                    ..Default::default()
                }),
                producer: None,
            };
            if !records.is_empty() && bytes + record.encoded_len() > max_bytes {
                break;
            }
            bytes += record.encoded_len();
            maker.next += 1;
            records.push(record);
        }
        let end = (maker.pages == 0).then_some(StreamReadEnd {
            done: true,
            failed: None,
        });
        maker.pages = maker.pages.saturating_sub(1);
        Page { records, end }
    }

    /// Checks the read entries of the marker of a task against the pages that Core took in the
    /// task, by activation and seq, and checks the two bounds of the Records of a task.
    fn check_pages(seed: u64, marker: &StreamMarkerData, pages: &[(u64, u32, Vec<StreamRecord>)]) {
        let seqs: BTreeSet<u32> = pages.iter().map(|(_, seq, _)| *seq).collect();
        let entries: BTreeSet<u32> = marker.reads.iter().map(|read| read.seq).collect();
        assert!(
            seqs.is_subset(&entries),
            "seed {seed}: {seqs:?} {entries:?}"
        );
        for entry in &marker.reads {
            let of: Vec<_> = (pages.iter())
                .filter(|(_, seq, _)| *seq == entry.seq)
                .collect();
            let counts: Vec<u32> = (of.iter())
                .map(|(_, _, records)| records.len() as u32)
                .collect();
            let at: Vec<u64> = of.iter().map(|(activation, ..)| *activation).collect();
            assert_eq!(
                (&entry.page_records, &entry.page_activations),
                (&counts, &at),
                "{seed}"
            );
            let records: Vec<StreamRecord> = (of.iter())
                .flat_map(|(_, _, records)| records.iter().cloned())
                .collect();
            if entry.digest.is_empty() {
                assert_eq!(entry.records, records, "seed {seed}");
            } else {
                let mut digest = Digest::new();
                digest.add(&records);
                assert_eq!(entry.digest, digest.finish(), "seed {seed}");
            }
        }
        let bytes = |records: &mut dyn Iterator<Item = &StreamRecord>| {
            records.map(Message::encoded_len).sum::<usize>()
        };
        let given = bytes(&mut pages.iter().flat_map(|(_, _, records)| records));
        assert!(given <= MAX_TASK_RECORD_BYTES, "seed {seed}: {given}");
        let held = bytes(&mut marker.reads.iter().flat_map(|read| &read.records));
        let first_alone = marker
            .reads
            .iter()
            .map(|read| read.records.len())
            .sum::<usize>()
            == 1;
        assert!(
            held <= MAX_MARKER_RECORD_BYTES || first_alone,
            "seed {seed}: {held}"
        );
    }

    // Reads give pages of any size up to what their reader asked for, so a page can wait for the
    // next task. Each page goes whole into one marker, the oldest first, and no task gives more
    // Records than its bounds.
    #[test]
    fn each_result_and_intent_is_in_exactly_one_marker_in_order() {
        let (mut full, mut oversized, mut in_hold, mut out_of_order) = (0, 0, 0, 0);
        let (mut pages_wait, mut larger_first) = (0, 0);
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
            // A few seeds read Records of up to 1.5 MiB, so that a task reaches its bounds.
            let big = rng.random_bool(0.15);
            for seq in 1..=rng.random_range(0..=2) {
                let read = StartStreamRead {
                    seq,
                    stream: Some(stream(&format!("p{seq}"))),
                    records_in_history: rng.random_bool(0.5),
                    ..Default::default()
                };
                calls.start_read(read).unwrap();
            }
            let mut makers: BTreeMap<u32, PageMaker> = BTreeMap::new();
            // The pages that wait for Core in the order that they came, and the pages that Core
            // took in the task in hand.
            let mut arrived: VecDeque<u32> = VecDeque::new();
            let mut pages = vec![];
            let start = |calls: &mut StreamCalls,
                         in_flight: &mut Vec<Call>,
                         makers: &mut BTreeMap<u32, PageMaker>,
                         rng: &mut StdRng| {
                for request in calls.take_requests() {
                    match request {
                        Request::Run(Call {
                            call_id,
                            seq,
                            op: Op::Read(call),
                        }) => {
                            let maker = PageMaker {
                                call_id,
                                call,
                                next: 1,
                                pages: rng.random_range(0..=if big { 8 } else { 40 }),
                            };
                            makers.insert(seq, maker);
                        }
                        Request::Run(call) => in_flight.push(call),
                        // The last command stops the reads.
                        Request::Stop(call_id) => {
                            makers.retain(|_, maker| maker.call_id != call_id)
                        }
                    }
                }
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
                    calls.stop_for_terminal().unwrap();
                    arrived.clear();
                    held = true;
                }
                start(&mut calls, &mut in_flight, &mut makers, &mut rng);
                // The Store answers some of the calls in flight, in any order.
                for _ in 0..rng.random_range(0..=in_flight.len().min(6)) {
                    let call = in_flight.swap_remove(rng.random_range(0..in_flight.len()));
                    calls.resolve(answer(&mut rng, call, &mut texts));
                    start(&mut calls, &mut in_flight, &mut makers, &mut rng);
                }
                // A reader gives a page once the run took the one before, with the task open and
                // room in it.
                for (seq, maker) in &mut makers {
                    let state = calls.reads.get(seq).map(|read| &read.state);
                    let free = matches!(state,
                        Some(ReadState::Running { call_id, page: None, .. })
                            if *call_id == maker.call_id);
                    let open = matches!(*maker.call.gate.borrow(), TaskState::Open(_));
                    let room = maker.call.room_left();
                    if !free || !open || room == 0 || rng.random_bool(0.3) {
                        continue;
                    }
                    let ahead = calls.runtimes.settings.read_ahead_bytes;
                    let page = make_page(&mut rng, maker, room.min(ahead), big);
                    arrived.push_back(*seq);
                    calls.resolve(StreamResolution {
                        call_id: maker.call_id,
                        seq: *seq,
                        outcome: Outcome::Page(page),
                    });
                }
                // Core takes the results that wait in their order, up to the first that doesn't
                // fit, and the marker records them with the activation.
                let waiting: Vec<CallKey> = calls.done.iter().copied().collect();
                let (took, took_pages) = if held {
                    calls.take_for_hold(activation);
                    let left = waiting
                        .iter()
                        .filter(|call| calls.done.contains(call))
                        .count();
                    (waiting.len() - left, false)
                } else if calls.has_jobs(activation) && rng.random_bool(0.8) {
                    let (reads, results): (Vec<Variant>, Vec<Variant>) =
                        (calls.take_jobs(activation).into_iter())
                            .partition(|job| matches!(job, Variant::ResolveStreamRead(_)));
                    let results: Vec<CallKey> = results.iter().map(key_of).collect();
                    assert_eq!(results, waiting[..results.len()], "seed {seed}");
                    // The pages that Core took are the oldest that waited.
                    let seqs: BTreeSet<CallKey> = reads.iter().map(key_of).collect();
                    let oldest: BTreeSet<CallKey> =
                        (arrived.drain(..reads.len())).map(CallKey::Read).collect();
                    assert_eq!(seqs, oldest, "seed {seed}");
                    for job in &reads {
                        if let Variant::ResolveStreamRead(read) = job {
                            pages.push((activation, read.seq, read.records.clone()));
                        }
                    }
                    (results.len(), !reads.is_empty())
                } else {
                    (0, false)
                };
                assert!(
                    waiting[took..].iter().all(|call| calls.done.contains(call)),
                    "seed {seed}"
                );
                // A take stops only at a result or a page that doesn't fit, so one that still
                // waits means that the marker or the task is full and the task must complete.
                if held || took > 0 || took_pages {
                    // A reader with no room can't ask, so the task must not wait for it.
                    let starved = (makers.iter()).any(|(seq, maker)| {
                        calls.reads.contains_key(seq) && maker.call.room_left() == 0
                    });
                    let waits = !calls.done.is_empty() || !calls.waiting.is_empty() || starved;
                    assert_eq!(calls.must_complete(activation), waits, "seed {seed}");
                    full += usize::from(!calls.done.is_empty());
                    pages_wait += usize::from(!calls.waiting.is_empty());
                }
                in_hold += usize::from(held && took > 0);
                for call in &waiting[..took] {
                    let (seq, high) = match call {
                        CallKey::Append(seq) => (*seq, &mut highest.0),
                        CallKey::Close(seq) => (*seq, &mut highest.1),
                        CallKey::Read(_) => panic!("A page is never in `done`"),
                    };
                    out_of_order += usize::from(seq < *high);
                    *high = seq.max(*high);
                }
                let entries = taken.last_mut().unwrap();
                entries.extend(waiting[..took].iter().map(|call| (activation, *call)));
                if !held && (took > 0 || took_pages) {
                    activation += 1;
                    continue;
                }
                // A full marker completes the task, and so does a heartbeat at any time.
                let finished = appends + closes == budget && !calls.needs_next_task();
                if finished || calls.must_complete(activation) || rng.random_bool(0.03) {
                    let marker = calls.take_marker();
                    check_pages(seed, marker.as_ref().unwrap_or(&Default::default()), &pages);
                    let read = |marker: &StreamMarkerData| -> Vec<usize> {
                        marker.reads.iter().map(|read| read.records.len()).collect()
                    };
                    larger_first += usize::from(marker.as_ref().is_some_and(|marker| {
                        read(marker) == [1]
                            && marker.reads[0].records[0].encoded_len() > MAX_MARKER_RECORD_BYTES
                    }));
                    pages.clear();
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
                let entries = marker.appends.len()
                    + marker.close_intents.len()
                    + marker.closes.len()
                    + marker.reads.len();
                // The bound of the entries leaves out the Records of reads.
                let mut bare = marker.clone();
                bare.reads.iter_mut().for_each(|read| read.records.clear());
                let size = bare.encoded_len();
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
        let counts = [
            full,
            oversized,
            in_hold,
            out_of_order,
            pages_wait,
            larger_first,
        ];
        assert!(counts.iter().all(|count| *count > 0), "{counts:?}");
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
        /// The command of a read starts at another offset.
        Offset,
        /// History has a page of a read after the read's end.
        AfterEnd,
    }

    /// Makes a random change to `history`, if it has an entry to change. Returns it with the task
    /// of the changed entry.
    fn mutate(rng: &mut StdRng, history: &mut [Task]) -> Option<(Change, usize)> {
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
                .chain((marker.reads.iter()).map(move |read| (index, CallKey::Read(read.seq))))
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
            Change::Offset,
            Change::AfterEnd,
        ][rng.random_range(0..8)];
        if let Change::Stream | Change::Missing | Change::Offset = change {
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
                (Change::Offset, Command::StartRead(read)) => read.from_offset.push(0),
                (Change::Offset, _) => return None,
                (_, Command::Append(append)) => append.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Close(close)) => close.stream.as_mut().unwrap().id.push('!'),
                (_, Command::StartRead(read)) => read.stream.as_mut().unwrap().id.push('!'),
                (_, Command::CloseRead(_) | Command::Terminal) => return None,
            }
            return Some((change, task));
        }
        if change == Change::AfterEnd {
            // A later marker gets a page of the read that ended.
            let CallKey::Read(seq) = call else {
                return None;
            };
            let marker = history[task].marker.as_ref().unwrap();
            let entry = marker.reads.iter().find(|read| read.seq == seq)?;
            // An entry with no page and no end doesn't decode.
            entry
                .end
                .as_ref()
                .filter(|_| !entry.page_records.is_empty())?;
            let page = StreamReadEntry {
                end: None,
                ..entry.clone()
            };
            let stream = marker.streams[entry.stream as usize].clone();
            let (later, task) = (history.iter_mut().enumerate().skip(task + 1))
                .find_map(|(index, task)| Some((task.marker.as_mut()?, index)))?;
            later.reads.push(StreamReadEntry {
                stream: later.streams.len() as u32,
                ..page
            });
            later.streams.push(stream);
            return Some((change, task));
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
            CallKey::Read(seq) => {
                let at = marker.reads.iter().position(|read| read.seq == seq)?;
                match change {
                    Change::Twice => marker.reads.push(marker.reads[at].clone()),
                    Change::Seq => marker.reads[at].seq = 999,
                    Change::Activation => {
                        let read = &mut marker.reads[at];
                        read.page_activations.iter_mut().for_each(|at| *at += 1);
                        if let Some(end) = read.end.as_mut() {
                            end.activation += 1;
                        }
                    }
                    _ => {
                        let entry = marker.reads.remove(at);
                        let activation = entry.page_activations.first().copied();
                        let ended = entry.end.as_ref().map(|end| end.activation);
                        marker.appends.push(StreamAppendResult {
                            seq,
                            stream: entry.stream,
                            activation: activation.or(ended).unwrap_or_default(),
                            result: Some(
                                stream_append_result::Result::Appended(Default::default()),
                            ),
                        });
                    }
                }
            }
        }
        Some((change, task))
    }

    fn is_command_of(command: &Command, call: CallKey) -> bool {
        match (command, call) {
            (Command::Append(append), CallKey::Append(seq)) => append.seq == seq,
            (Command::Close(close), CallKey::Close(seq)) => close.seq == seq,
            (Command::StartRead(read), CallKey::Read(seq)) => read.seq == seq,
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
            let Some((change, task)) = mutate(&mut rng, &mut history) else {
                continue;
            };
            mutated += 1;
            let first_jobs: Vec<_> = first.history.iter().map(Task::jobs).collect();
            // Workflow code reaches the change before the read entries of later tasks, whose
            // Records the Store lost, so the change decides the error.
            let lost = !matches!(change, Change::Activation | Change::Kind);
            let gone = |at, _| (lost && at > task).then(|| "lost".to_string());
            match replay(seed, &history, epoch(1000), &first.streams, &gone) {
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
        lang_with_no_config.peeked(marker, vec![]).unwrap();
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
            let (mut appends, mut closes, mut reads) = (0, 0, 0);
            let mut unserved = BTreeMap::new();
            for index in 0..rng.random_range(1..=6) {
                let store = stores[rng.random_range(0..3)];
                let stream = StreamRef {
                    id: format!("s{index}"),
                    store: store.to_string(),
                };
                let (call, result) = match rng.random_range(0..3) {
                    0 => {
                        appends += 1;
                        let command = AppendToStream {
                            seq: appends,
                            stream: Some(stream),
                            bodies: vec![Payload::default()],
                            sequence: 0,
                        };
                        (CallKey::Append(appends), calls.append(command))
                    }
                    1 => {
                        closes += 1;
                        let command = CloseStream {
                            seq: closes,
                            stream: Some(stream),
                        };
                        (CallKey::Close(closes), calls.close(command))
                    }
                    _ => {
                        reads += 1;
                        let command = StartStreamRead {
                            seq: reads,
                            stream: Some(stream),
                            ..Default::default()
                        };
                        (CallKey::Read(reads), calls.start_read(command))
                    }
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
                        CallKey::Read(seq) => calls.reads.contains_key(&seq),
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
                    Request::Run(Call {
                        op: Op::Read(read), ..
                    }) => read.request.r#ref.as_ref().unwrap().store.as_str(),
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
