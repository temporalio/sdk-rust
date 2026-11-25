//! The Stream calls of one run. Nothing here does I/O. The machines of the run drive the calls,
//! and the Worker's [`StreamManager`](super::StreamManager) runs the requests that they start.

use super::{
    DEFAULT_PAGE_BYTES, DEFAULT_PAGE_RECORDS, MarkerReads, StreamRuntimes, StreamSettings,
    TaskState,
    manager::{Call, LastTry, Op, Outcome, ReadCall, Request, StreamResolution},
    marker::{PeekedMarker, append_job},
    read::{After, ReadDone, read_job},
    replay::{ReplayError, needs_records, offset_text},
};
use crate::worker::workflow::WFMachinesError;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt, mem,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::{
        external_data::{
            StreamAppendMarkerData, StreamAppendResult, StreamFailure, StreamReadMarkerData,
            StreamReadResult, stream_read_result,
        },
        workflow_activation::workflow_activation_job,
        workflow_commands::{AppendToStream, ReadStream},
        workflow_completion,
    },
    temporal::api::{common::v1::Payload, failure::v1::failure::FailureInfo},
};
use temporalio_streams::{
    AnyError, AnyReason, ReadError,
    attempt::attempt_key,
    ids::{validate_offset, validate_stream_id},
    proto::{
        AppendRecord, Producer, StreamErrorReason, StreamRecord, WorkflowProducer,
        WorkflowTaskAttempt,
        producer::Kind,
        sdk::{AppendRecordsRequest, ReadRecordsRequest, StreamRef},
    },
};

/// A marker records the Store name of each call, so a longer name fails the Workflow Task.
const MAX_STORE_NAME_BYTES: usize = 4096;

/// One counter for the whole process, so a late result of an evicted run never matches a call of
/// the run that replaces it.
static CALL_IDS: AtomicU64 = AtomicU64::new(0);

impl From<ReplayError> for WFMachinesError {
    fn from(error: ReplayError) -> Self {
        match error {
            ReplayError::Gone(message) => WFMachinesError::StreamReplay(message),
            ReplayError::Unavailable(message) | ReplayError::NoRuntime(message) => {
                WFMachinesError::Stream(message)
            }
        }
    }
}

/// The type of the failure of a [`WFMachinesError::StreamReplay`], which can fail the Workflow.
/// It's a fact of the format of that failure, which History keeps.
const STREAM_REPLAY_ERROR_TYPE: &str = "StreamReplayError";

/// Gives the failure of a [`WFMachinesError::StreamReplay`] its type.
pub(crate) fn set_replay_failure_type(failure: &mut workflow_completion::Failure) {
    if let Some(FailureInfo::ApplicationFailureInfo(info)) =
        (failure.failure.as_mut()).and_then(|failure| failure.failure_info.as_mut())
    {
        info.r#type = STREAM_REPLAY_ERROR_TYPE.to_string();
    }
}

/// Returns true for the failure of a [`WFMachinesError::StreamReplay`].
pub(crate) fn is_stream_replay_failure(failure: &workflow_completion::Failure) -> bool {
    let info = (failure.failure.as_ref()).and_then(|failure| failure.failure_info.as_ref());
    matches!(info, Some(FailureInfo::ApplicationFailureInfo(info))
        if info.r#type == STREAM_REPLAY_ERROR_TYPE)
}

/// How far a run is through its History.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The run replays History, so each call waits for its result in a marker.
    Replaying,
    /// The replay is over, so calls run.
    Live,
    /// The run is on a replay Worker, which makes no live calls.
    ReplayOnly,
    /// The run stopped its calls, at a failed task or at the command that ends the Workflow, so it
    /// records no new call.
    Stopped,
}

/// The kind and seq of a call. Core gives the results of one activation in this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CallKey {
    Append(u32),
    Read(u32),
}

impl fmt::Display for CallKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallKey::Append(seq) => write!(f, "append {seq}"),
            CallKey::Read(seq) => write!(f, "read {seq}"),
        }
    }
}

struct Append {
    stream: StreamRef,
    state: AppendState,
}

enum AppendState {
    /// The append is in its queue. A replayed one gets its attempt at go-live.
    Waiting {
        bodies: Vec<Payload>,
        attempt: Option<WorkflowTaskAttempt>,
    },
    Running(u64),
    /// The result waits for an activation, in the form that the marker records it.
    Done(StreamAppendResult),
    /// History has the result, which waits for its activation.
    Replayed,
}

struct Read {
    stream: StreamRef,
    /// The request of each start of the read, with the page size of the read.
    request: ReadRecordsRequest,
    /// How long the read waits for a Record, as its command asks.
    wait: Duration,
    /// How long the call that runs asks the Store to wait, which the read limit can cut.
    sent_wait: Duration,
    /// True once the read had a check in the task in hand. A read that its check parked doesn't
    /// start again in that task.
    checked: bool,
    state: ReadState,
}

enum ReadState {
    /// The read hasn't started, or Core stopped it at the completion of a task. A live one starts
    /// once its task is open, and a replayed one once the replay is over.
    Waiting,
    /// The read runs, and has the failure of its last try while it waits to try again. A check
    /// (true) is one Store call with no wait.
    Running(u64, LastTry, bool),
    /// The result waits for an activation.
    Done(ReadDone),
    /// History has the result, which waits for its activation.
    Replayed,
}

/// A read entry of History, with the Records that the replay read for its page.
struct HeldRead {
    entry: StreamReadResult,
    records: Result<Vec<StreamRecord>, ReplayError>,
}

/// Marker entries from History whose calls Workflow code hasn't issued yet, by seq.
#[derive(Default)]
struct Held {
    appends: HashMap<u32, StreamAppendResult>,
    reads: HashMap<u32, HeldRead>,
}

impl Held {
    /// Returns the activation and the call of the first result, in the order of the first run.
    fn first_result(&self) -> Option<(u64, CallKey)> {
        let appends =
            (self.appends.iter()).map(|(seq, a)| (a.activation_index, CallKey::Append(*seq)));
        let reads =
            (self.reads.iter()).map(|(seq, r)| (r.entry.activation_index, CallKey::Read(*seq)));
        appends.chain(reads).min()
    }
}

/// The Stream calls of one run. A call is removed from the run once its result is in a marker.
pub(crate) struct StreamCalls {
    /// The Producer of the run. A write adds the attempt of its Workflow Task.
    producer: WorkflowProducer,
    runtimes: StreamRuntimes,
    phase: Phase,
    task: TaskState,
    appends: HashMap<u32, Append>,
    reads: HashMap<u32, Read>,
    /// The appends of each Stream, in the order that they were queued. Only the head runs.
    queues: HashMap<(String, String), VecDeque<CallKey>>,
    last_append: u32,
    last_read: u32,
    /// True once the task in hand passed its read limit, so its reads hold it no longer.
    read_limit_passed: bool,
    /// The bytes of the Records that the task in hand gave Workflow code.
    task_record_bytes: usize,
    /// The number of read results that the task in hand gave Workflow code.
    task_read_results: usize,
    /// True if the task in hand completes with a command. A page with Records from a check then
    /// waits for the next task, so that the command goes at once.
    hold_checks: bool,
    /// The reads whose page waits for the next task.
    held_pages: BTreeSet<u32>,
    /// For each Stream, the `last_offset` of the last page that Workflow code got if the Store had
    /// Records after it. A read from there knows that the Stream has more.
    more_after: HashMap<(String, String), Vec<u8>>,
    /// The live calls whose result waits for an activation.
    done: BTreeSet<CallKey>,
    held: Held,
    /// Results from History whose calls Workflow code has issued, by the activation that gets them.
    ready: BTreeMap<(u64, CallKey), workflow_activation_job::Variant>,
    /// The last result of `ready` that a release gave to the next activation.
    released: Option<(u64, CallKey)>,
    /// The live results of the activation in hand, which its markers record.
    marker: StreamAppendMarkerData,
    read_marker: StreamReadMarkerData,
    /// True once the task in hand recorded a Stream marker, until the task completes.
    marked: bool,
    requests: Vec<Request>,
}

impl StreamCalls {
    /// Creates the calls of a new run, which writes as the Workflow Producer with these ids.
    pub(crate) fn new(
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
        runtimes: StreamRuntimes,
        phase: Phase,
    ) -> Self {
        debug_assert_ne!(phase, Phase::Stopped);
        Self {
            producer: WorkflowProducer {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                attempt: None,
            },
            runtimes,
            phase,
            task: TaskState::NoTask,
            appends: HashMap::new(),
            reads: HashMap::new(),
            queues: HashMap::new(),
            last_append: 0,
            last_read: 0,
            read_limit_passed: false,
            task_record_bytes: 0,
            task_read_results: 0,
            hold_checks: false,
            held_pages: BTreeSet::new(),
            more_after: HashMap::new(),
            done: BTreeSet::new(),
            held: Held::default(),
            ready: BTreeMap::new(),
            released: None,
            marker: StreamAppendMarkerData::default(),
            read_marker: StreamReadMarkerData::default(),
            marked: false,
            requests: vec![],
        }
    }

    /// When the task opens, the head of each queue and each waiting read start. A page that waits
    /// for this task comes first.
    pub(crate) fn set_task(&mut self, state: TaskState) {
        if state == TaskState::NoTask {
            self.marked = false;
            self.task_record_bytes = 0;
            self.task_read_results = 0;
        }
        if !matches!(state, TaskState::Completing(_)) {
            self.read_limit_passed = false;
            self.hold_checks = false;
        }
        if state.is_open() {
            self.held_pages.clear();
            for read in self.reads.values_mut() {
                read.checked = false;
            }
        }
        self.task = state;
        self.start_heads();
    }

    pub(crate) fn append(&mut self, command: AppendToStream) -> Result<(), WFMachinesError> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        let AppendToStream {
            seq,
            stream,
            bodies,
        } = command;
        next_seq("append", seq, &mut self.last_append)?;
        let stream = valid_stream(CallKey::Append(seq), stream)?;
        self.check_runtime(&stream)?;
        let attempt = match self.phase {
            Phase::Live => self.task.attempt(),
            Phase::Replaying | Phase::ReplayOnly | Phase::Stopped => None,
        };
        let state = AppendState::Waiting { bodies, attempt };
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

    /// Handles a read command. A live read starts at once, and its Store waits at most the time
    /// that its task has left to its read limit.
    pub(crate) fn read(&mut self, command: ReadStream) -> Result<(), WFMachinesError> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        let ReadStream {
            seq,
            stream,
            from_offset,
            wait,
            max_records,
            max_bytes,
        } = command;
        next_seq("read", seq, &mut self.last_read)?;
        let call = CallKey::Read(seq);
        let stream = valid_stream(call, stream)?;
        let wait = validate_offset(&from_offset, "The offset", AnyReason::InvalidArgument)
            .and_then(|()| check_page_size(max_records, max_bytes))
            .and_then(|()| wait.map_or(Ok(Duration::ZERO), wait_duration))
            .map_err(|error| {
                WFMachinesError::Fatal(format!("Stream {call} is invalid: {}", error.message))
            })?;
        self.check_runtime(&stream)?;
        // A page must fit a task with no Records yet.
        let room = self.runtimes.settings.max_task_record_bytes();
        let max_bytes = match max_bytes {
            0 => DEFAULT_PAGE_BYTES,
            max => max,
        };
        let request = ReadRecordsRequest {
            r#ref: Some(stream.clone()),
            from_offset,
            max_records: match max_records {
                0 => DEFAULT_PAGE_RECORDS,
                max => max,
            },
            max_bytes: max_bytes.min(i64::try_from(room).unwrap_or(i64::MAX)),
            wait: None,
        };
        let read = Read {
            stream,
            request,
            wait,
            sent_wait: wait,
            checked: false,
            state: ReadState::Waiting,
        };
        self.reads.insert(seq, read);
        self.match_read(seq)?;
        self.run_read(seq);
        Ok(())
    }

    /// Handles the result of a running call, and starts the next call of its Stream. It ignores the
    /// result of a call that was stopped. A check of the task in hand that meets an outage fails
    /// the task, as an append that meets one does at the heartbeat. A check of an earlier task that
    /// met an outage, or a check with no Record of a read that waits, parks the read, which starts
    /// again when a task opens.
    pub(crate) fn resolve(&mut self, resolution: StreamResolution) -> Result<(), WFMachinesError> {
        let StreamResolution {
            call_id,
            seq,
            outcome,
        } = resolution;
        let (call, queue) = match outcome {
            // A read is in no queue.
            Outcome::Read(mut result) => {
                let Some(read) = self.reads.get_mut(&seq) else {
                    return Ok(());
                };
                let ReadState::Running(id, _, check) = read.state else {
                    return Ok(());
                };
                if id != call_id {
                    return Ok(());
                }
                let outage = result.failure().filter(|failure| is_outage(failure));
                if check && let Some(failure) = outage {
                    if read.checked && self.task.is_open() {
                        return Err(read_outage(&read.stream, failure));
                    }
                    read.state = ReadState::Waiting;
                    self.run_read(seq);
                    return Ok(());
                }
                if result.is_empty_page() && read.sent_wait < read.wait {
                    result.set_wait_cut();
                }
                if check && result.is_empty_page() && !read.wait.is_zero() {
                    read.state = ReadState::Waiting;
                    self.run_read(seq);
                    return Ok(());
                }
                if check && self.hold_checks && !result.records.is_empty() {
                    self.held_pages.insert(seq);
                }
                read.state = ReadState::Done(result);
                self.done.insert(CallKey::Read(seq));
                return Ok(());
            }
            Outcome::Append(result) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return Ok(());
                };
                if !matches!(append.state, AppendState::Running(id) if id == call_id) {
                    return Ok(());
                }
                append.state = AppendState::Done(StreamAppendResult {
                    seq,
                    stream_id: append.stream.id.clone(),
                    store: append.stream.store.clone(),
                    activation_index: 0,
                    result: Some(result),
                });
                (CallKey::Append(seq), key(&append.stream))
            }
        };
        self.done.insert(call);
        self.leave_queue(&queue, call);
        Ok(())
    }

    /// Returns true if activation `activation` of the task has a Stream result to give. A result
    /// from History is held back while the index of the next Local Activity marker, `marker`, is at
    /// or below the result's own index, so that the Local Activity result goes first.
    pub(crate) fn has_jobs(&self, activation: u64, marker: Option<u64>) -> bool {
        // A run that stopped its calls gives no result, even one that History has.
        self.phase != Phase::Stopped
            && ((self.ready.keys().next()).is_some_and(|key| self.is_due(key, activation, marker))
                || (self.task.is_open() && self.done.first().is_some_and(|c| self.fits(*c))))
    }

    /// Returns the results for the activation at index `activation` of the task, as
    /// [`Self::has_jobs`] decides them. The markers of the activation record each live result with
    /// that index, and [`Self::take_marker`] and [`Self::take_read_marker`] return them.
    pub(crate) fn take_jobs(
        &mut self,
        activation: u64,
        marker: Option<u64>,
    ) -> Vec<workflow_activation_job::Variant> {
        let mut jobs = vec![];
        while let Some(key) = self.ready.keys().next().copied()
            && self.is_due(&key, activation, marker)
        {
            let job = self.ready.remove(&key).expect("The key was just read");
            self.leave(key.1);
            jobs.push(job);
        }
        self.released = None;
        if !self.task.is_open() {
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

    /// Holds the entries of a marker from History, each for its call. `records` has the Records
    /// that the replay read for the read entries of a read marker, by the index of each entry.
    pub(crate) fn peeked(
        &mut self,
        data: PeekedMarker,
        mut records: MarkerReads,
    ) -> Result<(), WFMachinesError> {
        match data {
            PeekedMarker::Append(data) => {
                for append in data.appends {
                    let seq = append.seq;
                    if self.held.appends.insert(seq, append).is_some() {
                        return Err(second("result", CallKey::Append(seq)));
                    }
                    self.match_append(seq)?;
                }
            }
            PeekedMarker::Read(data) => {
                for (index, entry) in data.reads.into_iter().enumerate() {
                    let seq = entry.seq;
                    let records = if needs_records(&entry) {
                        (records.get_mut(index).and_then(Option::take)).unwrap_or_else(|| {
                            Err(ReplayError::Unavailable(format!(
                                "Core read no Records of Stream read {seq} for its replay"
                            )))
                        })
                    } else {
                        Ok(vec![])
                    };
                    let held = HeldRead { entry, records };
                    if self.held.reads.insert(seq, held).is_some() {
                        return Err(second("result", CallKey::Read(seq)));
                    }
                    self.match_read(seq)?;
                }
            }
        }
        Ok(())
    }

    /// Handles the event of a marker. Each entry of the markers of its task must already have its
    /// call, and Workflow code must have its result.
    pub(crate) fn marker_recorded(&mut self) -> Result<(), WFMachinesError> {
        let appends = self.held.appends.keys().map(|seq| CallKey::Append(*seq));
        let reads = self.held.reads.keys().map(|seq| CallKey::Read(*seq));
        if let Some(call) = appends.chain(reads).min() {
            return Err(WFMachinesError::Nondeterminism(format!(
                "History has a result of Stream {call} that Workflow code never issued"
            )));
        }
        if let Some((_, call)) = self.ready.keys().next() {
            return Err(WFMachinesError::Nondeterminism(format!(
                "History has a result of Stream {call} that Workflow code never got"
            )));
        }
        Ok(())
    }

    /// Ends the replay. Each waiting call gets the attempt of the current task, and the head of
    /// each queue and each waiting read start.
    pub(crate) fn go_live(&mut self) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Replaying {
            return Ok(());
        }
        let appends = (self.appends.iter()).map(|(seq, append)| (CallKey::Append(*seq), append));
        let unserved = (appends.map(|(call, append)| (call, &append.stream)))
            .chain((self.reads.iter()).map(|(seq, read)| (CallKey::Read(*seq), &read.stream)))
            .filter(|(_, stream)| self.runtimes.get(&stream.store).is_none())
            .min_by_key(|(call, _)| *call);
        if let Some((_, stream)) = unserved {
            return Err(no_runtime(stream));
        }
        self.phase = Phase::Live;
        let attempt = self.task.attempt();
        for append in self.appends.values_mut() {
            if let AppendState::Waiting {
                attempt: waiting, ..
            } = &mut append.state
            {
                *waiting = attempt;
            }
        }
        self.start_heads();
        Ok(())
    }

    /// Drops each call at a command that ends the Workflow, as Core drops a Local Activity there.
    /// The markers keep the results that Workflow code already got in the task.
    pub(crate) fn stop_for_terminal(&mut self) {
        let mut unsent = HashSet::new();
        self.requests.retain(|request| match request {
            Request::Run(call) => {
                unsent.insert(call.call_id);
                false
            }
            Request::Stop(_) => true,
        });
        let appends = (self.appends.values()).filter_map(|append| match append.state {
            AppendState::Running(call_id) => Some(call_id),
            _ => None,
        });
        let reads = (self.reads.values()).filter_map(|read| match read.state {
            ReadState::Running(call_id, ..) => Some(call_id),
            _ => None,
        });
        let running = appends.chain(reads);
        let running = running.filter(|call_id| !unsent.contains(call_id));
        self.requests.extend(running.map(Request::Stop));
        self.phase = Phase::Stopped;
        self.appends.clear();
        self.reads.clear();
        self.queues.clear();
        self.done.clear();
    }

    /// Returns true while the run needs another task at once: an append has no result in a
    /// marker, Core knows that the Stream of an open read has more Records for it, or the read
    /// result budget of the task holds back a read.
    pub(crate) fn needs_next_task(&self) -> bool {
        !self.appends.is_empty() || (self.reads.values()).any(|read| self.knows_more(read))
    }

    /// Returns true if Core knows for certain that `read` has a result for the next task. Core then
    /// holds a page of it with Records, the read starts where a page ended that the Store had
    /// Records after, or the read result budget of the task holds the read back.
    fn knows_more(&self, read: &Read) -> bool {
        match &read.state {
            ReadState::Done(done) => !done.records.is_empty(),
            ReadState::Waiting if self.budget_holds(read) => true,
            ReadState::Waiting | ReadState::Running(..) => {
                (self.more_after.get(&key(&read.stream)))
                    .is_some_and(|after| *after == read.request.from_offset)
            }
            ReadState::Replayed => false,
        }
    }

    /// Returns true if `read` waits only because the task in hand gave all the read results that
    /// it can. A read that its check parked in the task got no Record, so it waits for another
    /// reason. The next task counts from zero, so a held read forces at most one task per budget.
    fn budget_holds(&self, read: &Read) -> bool {
        matches!(read.state, ReadState::Waiting)
            && !read.checked
            && self.task_read_results >= self.runtimes.settings.max_task_read_results
    }

    /// Stops each live read that Core knows no result for in the next task, before the task
    /// completes. It starts again when the next task opens, so a read on an idle Stream makes no
    /// new task.
    pub(crate) fn stop_idle_reads(&mut self) {
        if self.phase != Phase::Live {
            return;
        }
        let idle: Vec<u32> = (self.reads.iter())
            .filter(|(_, read)| !self.knows_more(read))
            .filter(|(_, read)| !matches!(read.state, ReadState::Replayed))
            .map(|(seq, _)| *seq)
            .collect();
        for seq in idle {
            let read = self.reads.get_mut(&seq).expect("The read was just listed");
            if let ReadState::Running(call_id, ..) = read.state {
                stop_call(&mut self.requests, call_id);
            }
            read.state = ReadState::Waiting;
            self.done.remove(&CallKey::Read(seq));
        }
    }

    /// Notes that the task in hand completes with a command for the server if `hold`, so that the
    /// Records of its checks wait for the next task and the command goes at once.
    pub(crate) fn hold_check_pages(&mut self, hold: bool) {
        self.hold_checks |= hold;
    }

    /// Readies the live reads for a completion of the task in hand. A read that had no answer in
    /// this task gets a check first, so that the completion stops no read that the Store has
    /// Records for, and forces no task for a Store in an outage. A read that waits to try again
    /// after a failed try fails the task.
    pub(crate) fn check_open_reads(&mut self) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Live || !self.task.is_open() {
            return Ok(());
        }
        self.check_read_outage()?;
        let mut stopped: Vec<u32> = (self.reads.iter())
            .filter(|(_, read)| matches!(read.state, ReadState::Running(_, _, false)))
            .filter(|(_, read)| !read.checked)
            .map(|(seq, _)| *seq)
            .collect();
        stopped.sort_unstable();
        for seq in stopped {
            let read = self.reads.get_mut(&seq).expect("The read was just listed");
            if let ReadState::Running(call_id, ..) = read.state {
                stop_call(&mut self.requests, call_id);
            }
            read.state = ReadState::Waiting;
            self.start_read(seq, true);
        }
        Ok(())
    }

    /// Fails the task if a live read waits to try again after a try that failed. A failure of the
    /// Store is no answer, so Workflow code never gets it as a page or as a failure.
    pub(crate) fn check_read_outage(&self) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Live {
            return Ok(());
        }
        let mut reads: Vec<(&u32, &Read)> = self.reads.iter().collect();
        reads.sort_unstable_by_key(|(seq, _)| **seq);
        for (_, read) in reads {
            if let ReadState::Running(_, last_try, _) = &read.state
                && let Some(failure) = last_try.lock().as_ref()
            {
                return Err(read_outage(&read.stream, failure));
            }
        }
        Ok(())
    }

    /// Returns true if the task in hand recorded a Stream marker that History doesn't have yet.
    pub(crate) fn has_marker(&self) -> bool {
        self.marked
    }

    /// Returns the number of live calls that hold the task open, as a Local Activity does. A read
    /// holds it only before its read limit and while the task has no work for the server
    /// (`has_work_for_server`), so a command waits for no more than one check. A check always holds
    /// it. A page that the task holds back for its Record budget holds it no longer.
    pub(crate) fn outstanding(&self, has_work_for_server: bool) -> usize {
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
        let reads_hold = !has_work_for_server && !self.read_limit_passed;
        let reads = (self.reads.values())
            .filter(|read| match read.state {
                ReadState::Running(_, _, check) => check || reads_hold,
                _ => false,
            })
            .count();
        appends + reads
    }

    /// Returns true once the replay is over and the run makes live calls.
    pub(crate) fn is_live(&self) -> bool {
        self.phase == Phase::Live
    }

    pub(crate) fn settings(&self) -> &StreamSettings {
        &self.runtimes.settings
    }

    /// Returns true while a live read of the task in hand has no answer.
    pub(crate) fn read_runs(&self) -> bool {
        self.phase == Phase::Live
            && self.task.is_open()
            && (self.reads.values()).any(|read| matches!(read.state, ReadState::Running(..)))
    }

    /// Ends the task's reads that wait and have no answer, at its read limit. A read that waits to
    /// try again after a failed try fails the task. From the limit on only a check holds the task,
    /// and each later read of the task is a check.
    pub(crate) fn read_limit(&mut self) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Live || !self.task.is_open() {
            return Ok(());
        }
        self.check_read_outage()?;
        self.read_limit_passed = true;
        let mut reads: Vec<u32> = self.reads.keys().copied().collect();
        reads.sort_unstable();
        for seq in reads {
            self.end_read(seq);
        }
        Ok(())
    }

    /// Returns the activation of the first result from History whose call Workflow code issued.
    pub(crate) fn next_ready_activation(&self) -> Option<u64> {
        self.ready.keys().next().map(|(at, _)| *at)
    }

    /// Returns the activation of the first result from History that Workflow code doesn't have.
    pub(crate) fn first_result_activation(&self) -> Option<u64> {
        let held = self.held.first_result().map(|(at, _)| at);
        self.next_ready_activation().into_iter().chain(held).min()
    }

    /// Releases the results from History to the next activation in the order of the first run,
    /// regardless of their index. It stops at the first result whose call Workflow code hasn't
    /// issued. A replay with chunking on does the same for Local Activity results when it has no
    /// other job to give.
    pub(crate) fn release_ready(&mut self) {
        let held = self.held.first_result();
        let comes = |key: &&(u64, CallKey)| held.is_none_or(|held| **key < held);
        self.released = self.ready.keys().take_while(comes).last().copied();
    }

    fn is_due(&self, key: &(u64, CallKey), activation: u64, marker: Option<u64>) -> bool {
        (key.0 <= activation || self.released.is_some_and(|released| *key <= released))
            && marker.is_none_or(|marker| key.0 < marker)
    }

    /// Returns the append marker of the live results that the last [`Self::take_jobs`] gave, if it
    /// gave any.
    pub(crate) fn take_marker(&mut self) -> Option<StreamAppendMarkerData> {
        (!self.marker.appends.is_empty()).then(|| mem::take(&mut self.marker))
    }

    /// Returns the read marker of the live results that the last [`Self::take_jobs`] gave, if it
    /// gave any.
    pub(crate) fn take_read_marker(&mut self) -> Option<StreamReadMarkerData> {
        (!self.read_marker.reads.is_empty()).then(|| mem::take(&mut self.read_marker))
    }

    /// Returns a marker with no entry if the task completes with a new task `forced`, while a call
    /// has no result in a marker and the task recorded none. A replay with chunking on would
    /// otherwise take the task as a heartbeat and join it to the next one, whose activation
    /// indexes would then not match. A task with another command event (`has_command_event`)
    /// or with chunking off isn't joined, so it needs none. It's of the append kind if an append
    /// is open.
    pub(crate) fn take_completion_marker(
        &mut self,
        forced: bool,
        has_command_event: bool,
        chunking_disabled: bool,
    ) -> Option<PeekedMarker> {
        if self.phase != Phase::Live
            || self.marked
            || !forced
            || has_command_event
            || chunking_disabled
        {
            return None;
        }
        let marker = if !self.appends.is_empty() {
            PeekedMarker::Append(StreamAppendMarkerData::default())
        } else if !self.reads.is_empty() {
            PeekedMarker::Read(StreamReadMarkerData::default())
        } else {
            return None;
        };
        self.marked = true;
        Some(marker)
    }

    /// Makes each read that starts ask the Store to wait at most `max_wait`, and notes the reads
    /// that then don't wait.
    pub(crate) fn limit_read_waits(&mut self, max_wait: Duration) {
        for request in &mut self.requests {
            let Request::Run(Call {
                seq,
                op: Op::Read(call),
                ..
            }) = request
            else {
                continue;
            };
            call.wait = call.wait.min(max_wait);
            if let Some(read) = self.reads.get_mut(seq) {
                read.sent_wait = call.wait;
            }
        }
    }

    /// Returns the calls that the Worker must start or stop.
    pub(crate) fn take_requests(&mut self) -> Vec<Request> {
        mem::take(&mut self.requests)
    }

    /// Drops every call, the markers of the activation and the entries of History, and leaves the
    /// run with no task, as a failed task does. The run records no call after this.
    pub(crate) fn stop(&mut self) {
        self.stop_for_terminal();
        self.task = TaskState::NoTask;
        self.held = Held::default();
        self.ready.clear();
        self.released = None;
        self.marker = StreamAppendMarkerData::default();
        self.read_marker = StreamReadMarkerData::default();
        self.marked = false;
        self.read_limit_passed = false;
        self.task_record_bytes = 0;
        self.task_read_results = 0;
        self.hold_checks = false;
        self.held_pages.clear();
        self.more_after.clear();
        self.requests.clear();
    }

    /// Checks that a live call can run on the Store of `stream`.
    fn check_runtime(&self, stream: &StreamRef) -> Result<(), WFMachinesError> {
        if self.phase == Phase::Live && self.runtimes.get(&stream.store).is_none() {
            return Err(no_runtime(stream));
        }
        Ok(())
    }

    fn match_append(&mut self, seq: u32) -> Result<(), WFMachinesError> {
        let Some(append) = self.appends.get_mut(&seq) else {
            return Ok(());
        };
        let Some(entry) = self.held.appends.remove(&seq) else {
            return Ok(());
        };
        let call = CallKey::Append(seq);
        check_stream(call, &append.stream, &entry.stream_id, &entry.store)?;
        if !matches!(append.state, AppendState::Waiting { .. }) {
            return Err(second("result", call));
        }
        append.state = AppendState::Replayed;
        let at = (entry.activation_index, call);
        self.ready.insert(at, append_job(&entry).into());
        Ok(())
    }

    /// Gives a replayed read the result that History has for it. An entry of another read is a
    /// nondeterminism error, and so is an entry whose Records the replay doesn't have.
    fn match_read(&mut self, seq: u32) -> Result<(), WFMachinesError> {
        let Some(read) = self.reads.get_mut(&seq) else {
            return Ok(());
        };
        let Some(held) = self.held.reads.remove(&seq) else {
            return Ok(());
        };
        let (call, entry) = (CallKey::Read(seq), &held.entry);
        check_stream(call, &read.stream, &entry.stream_id, &entry.store)?;
        let from_offset = &read.request.from_offset;
        if entry.from_offset != *from_offset {
            return Err(WFMachinesError::Nondeterminism(format!(
                "Stream read {seq} gives the Records after {}, and History has them after {}",
                offset_text(from_offset),
                offset_text(&entry.from_offset)
            )));
        }
        if !matches!(read.state, ReadState::Waiting) {
            return Err(second("result", call));
        }
        let records = held.records?;
        read.state = ReadState::Replayed;
        let at = (entry.activation_index, call);
        self.ready.insert(at, read_job(entry, records).into());
        Ok(())
    }

    /// Starts read `seq` if it waits, the run is live, its task is open and has room for another
    /// read result. After the read limit the read is a check.
    fn run_read(&mut self, seq: u32) {
        if self.phase != Phase::Live || !self.task.is_open() {
            return;
        }
        let Some(read) = self.reads.get(&seq) else {
            return;
        };
        if !matches!(read.state, ReadState::Waiting) || read.checked || self.budget_holds(read) {
            return;
        }
        self.start_read(seq, self.read_limit_passed);
    }

    /// Starts read `seq`, which waits. A check is one Store call with no wait.
    fn start_read(&mut self, seq: u32, check: bool) {
        let read = self.reads.get_mut(&seq).expect("The caller found the read");
        let call_id = CALL_IDS.fetch_add(1, Ordering::Relaxed);
        let last_try = LastTry::default();
        read.state = ReadState::Running(call_id, last_try.clone(), check);
        read.sent_wait = if check { Duration::ZERO } else { read.wait };
        read.checked |= check;
        let call = Call {
            call_id,
            seq,
            store: read.stream.store.clone(),
            op: Op::Read(ReadCall {
                request: read.request.clone(),
                wait: read.sent_wait,
                check,
                last_try,
            }),
        };
        self.requests.push(Request::Run(call));
    }

    /// Ends read `seq` if it waits for a Record and has no answer, with a page with no Record. A
    /// read that doesn't wait goes on, since only the Store can say that the Stream has no Record
    /// after its offset.
    fn end_read(&mut self, seq: u32) {
        let Some(read) = self.reads.get_mut(&seq) else {
            return;
        };
        let ReadState::Running(call_id, _, false) = read.state else {
            return;
        };
        if read.sent_wait.is_zero() {
            return;
        }
        stop_call(&mut self.requests, call_id);
        let from_offset = read.request.from_offset.clone();
        read.state = ReadState::Done(ReadDone::cut(&read.stream, seq, from_offset));
        self.done.insert(CallKey::Read(seq));
    }

    /// Returns true if the task has room for the live result of `call`. A task with no Records yet
    /// takes a page of any size, since a read asks for at most what such a task holds, and a
    /// larger page has a single Record.
    fn fits(&self, call: CallKey) -> bool {
        let CallKey::Read(seq) = call else {
            return true;
        };
        if self.held_pages.contains(&seq) {
            return false;
        }
        let Some(ReadState::Done(done)) = self.reads.get(&seq).map(|read| &read.state) else {
            return true;
        };
        let (max, used) = (
            self.settings().max_task_record_bytes(),
            self.task_record_bytes,
        );
        done.bytes == 0 || used == 0 || (used <= max && done.bytes <= max - used)
    }

    /// Records a live result in the marker of its kind for the activation at `activation`, and
    /// returns its job. Returns `None` if the task has no room for it.
    fn record(
        &mut self,
        call: CallKey,
        activation: u64,
    ) -> Option<workflow_activation_job::Variant> {
        let job = match call {
            CallKey::Append(seq) => {
                let AppendState::Done(result) = &self.appends[&seq].state else {
                    return None;
                };
                let entry = StreamAppendResult {
                    activation_index: activation,
                    ..result.clone()
                };
                let job = append_job(&entry).into();
                self.marker.appends.push(entry);
                job
            }
            CallKey::Read(seq) => {
                if !self.fits(call) {
                    return None;
                }
                let read = self.reads.get_mut(&seq)?;
                let ReadState::Done(done) = mem::replace(&mut read.state, ReadState::Replayed)
                else {
                    return None;
                };
                let entry = StreamReadResult {
                    activation_index: activation,
                    ..done.entry
                };
                let page = match &entry.result {
                    Some(stream_read_result::Result::Page(page)) => Some(&page.last_offset),
                    _ => None,
                };
                let stream = key(&read.stream);
                match (done.after, page) {
                    (After::More, Some(last_offset)) => {
                        self.more_after.insert(stream, last_offset.clone());
                    }
                    (After::AsBefore, _) => {}
                    _ => {
                        self.more_after.remove(&stream);
                    }
                }
                self.task_record_bytes += done.bytes;
                self.task_read_results += 1;
                let job = read_job(&entry, done.records).into();
                self.read_marker.reads.push(entry);
                job
            }
        };
        self.marked = true;
        Some(job)
    }

    /// Starts the call at the head of each queue, and each read that waits, once the run is live
    /// and its task is open. It runs only when the task opens and at go-live, since every other
    /// change starts its own calls.
    fn start_heads(&mut self) {
        if self.phase != Phase::Live || !self.task.is_open() {
            return;
        }
        let heads: Vec<CallKey> = (self.queues.values())
            .filter_map(|queue| queue.front().copied())
            .collect();
        for call in heads {
            self.start(call);
        }
        let mut reads: Vec<u32> = self.reads.keys().copied().collect();
        reads.sort_unstable();
        for seq in reads {
            self.run_read(seq);
        }
    }

    fn start_head(&mut self, queue: &(String, String)) {
        if self.phase != Phase::Live || !self.task.is_open() {
            return;
        }
        if let Some(&call) = self.queues.get(queue).and_then(VecDeque::front) {
            self.start(call);
        }
    }

    /// Starts `call` if it waits with an attempt. A call that already runs, or that has no attempt
    /// yet, stays as it is.
    fn start(&mut self, call: CallKey) {
        let (seq, call_id, stream, op) = match call {
            CallKey::Append(seq) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return;
                };
                let AppendState::Waiting {
                    attempt: Some(attempt),
                    ..
                } = append.state
                else {
                    return;
                };
                let call_id = CALL_IDS.fetch_add(1, Ordering::Relaxed);
                let running = AppendState::Running(call_id);
                let AppendState::Waiting { bodies, .. } = mem::replace(&mut append.state, running)
                else {
                    return;
                };
                let producer = producer(&self.producer, attempt);
                let request_id = request_id(&self.producer.run_id, &producer, seq);
                let request = AppendRecordsRequest {
                    r#ref: Some(append.stream.clone()),
                    producer: Some(producer),
                    records: (bodies.into_iter())
                        .map(|body| AppendRecord { body: Some(body) })
                        .collect(),
                };
                (
                    seq,
                    call_id,
                    &append.stream,
                    Op::Append(request, request_id),
                )
            }
            // A read is in no queue.
            CallKey::Read(_) => return,
        };
        let call = Call {
            call_id,
            seq,
            store: stream.store.clone(),
            op,
        };
        self.requests.push(Request::Run(call));
    }

    /// Removes `call` from the run. A live call left its queue when its result arrived.
    fn leave(&mut self, call: CallKey) {
        let stream = match call {
            CallKey::Append(seq) => (self.appends.remove(&seq))
                .filter(|append| !matches!(append.state, AppendState::Done(_)))
                .map(|append| append.stream),
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

/// Checks that `seq` is above the last one of its kind. A repeated seq would give two appends one
/// request id, and a Store would treat the second as a resend of the first.
fn next_seq(kind: &str, seq: u32, last: &mut u32) -> Result<(), WFMachinesError> {
    if seq == 0 {
        return Err(WFMachinesError::Fatal(format!(
            "Stream {kind} has seq 0, and a seq starts at 1"
        )));
    }
    if seq <= *last {
        return Err(WFMachinesError::Fatal(format!(
            "Stream {kind} {seq} came after {kind} {last}"
        )));
    }
    *last = seq;
    Ok(())
}

fn check_store_name(store: &str) -> Result<(), AnyError> {
    if store.len() > MAX_STORE_NAME_BYTES {
        return Err(AnyError::invalid_argument(format!(
            "The Store name has {} bytes, more than {MAX_STORE_NAME_BYTES}",
            store.len()
        )));
    }
    Ok(())
}

/// A command with no Stream, or with an id or a Store name out of range, fails its Workflow Task.
fn valid_stream(call: CallKey, stream: Option<StreamRef>) -> Result<StreamRef, WFMachinesError> {
    let stream =
        stream.ok_or_else(|| WFMachinesError::Fatal(format!("Stream {call} has no Stream")))?;
    validate_stream_id(&stream.id)
        .and_then(|()| check_store_name(&stream.store))
        .map_err(|error| {
            WFMachinesError::Fatal(format!("Stream {call} is invalid: {}", error.message))
        })?;
    Ok(stream)
}

/// Checks that neither maximum of the page of a read is negative.
fn check_page_size(max_records: i32, max_bytes: i64) -> Result<(), AnyError> {
    let maximums = [
        ("max_records", i64::from(max_records)),
        ("max_bytes", max_bytes),
    ];
    match maximums.into_iter().find(|(_, max)| *max < 0) {
        Some((field, max)) => Err(AnyError::invalid_argument(format!(
            "The `{field}` of the read is {max}, which is negative"
        ))),
        None => Ok(()),
    }
}

/// Returns the wait of a read, or a failure for a duration that is negative or that has its
/// nanoseconds out of range.
fn wait_duration(wait: prost_types::Duration) -> Result<Duration, AnyError> {
    let prost_types::Duration { seconds, nanos } = wait;
    match (u64::try_from(seconds), u32::try_from(nanos)) {
        (Ok(whole), Ok(part)) if part < 1_000_000_000 => Ok(Duration::new(whole, part)),
        _ => Err(AnyError::invalid_argument(format!(
            "The wait of the read has {seconds} seconds and {nanos} nanoseconds, which is \
             negative or has its nanoseconds out of range"
        ))),
    }
}

/// Stops the call `call_id`. A call that the Worker didn't start yet never starts.
fn stop_call(requests: &mut Vec<Request>, call_id: u64) {
    let queued = (requests.iter())
        .position(|request| matches!(request, Request::Run(call) if call.call_id == call_id));
    match queued {
        Some(at) => {
            requests.remove(at);
        }
        None => requests.push(Request::Stop(call_id)),
    }
}

/// Returns true for the failure of a Store in an outage.
fn is_outage(failure: &StreamFailure) -> bool {
    failure.reason == StreamErrorReason::StoreUnavailable.as_str_name()
}

/// Returns the error that fails the task of a read in an outage, in the form of an append's.
fn read_outage(stream: &StreamRef, failure: &StreamFailure) -> WFMachinesError {
    let error = ReadError::store_unavailable(failure.message.clone());
    WFMachinesError::Stream(format!(
        "StreamStoreUnavailable: read of Stream \"{}\" on Store \"{}\": {error}",
        stream.id, stream.store
    ))
}

fn no_runtime(stream: &StreamRef) -> WFMachinesError {
    WFMachinesError::Stream(format!(
        "This Worker has no Stream runtime for the Store `{}`",
        stream.store
    ))
}

/// Checks that a call is for the Stream that History has, in the Store that History has. A call
/// with no Store name matches any Store.
fn check_stream(
    call: CallKey,
    sent: &StreamRef,
    id: &str,
    store: &str,
) -> Result<(), WFMachinesError> {
    if sent.id == id && (sent.store.is_empty() || sent.store == store) {
        return Ok(());
    }
    Err(WFMachinesError::Nondeterminism(format!(
        "Stream {call} is for Stream `{}` in Store `{}`, and History has it for Stream `{id}` in \
         Store `{store}`",
        sent.id, sent.store
    )))
}

fn second(what: &str, call: CallKey) -> WFMachinesError {
    WFMachinesError::Nondeterminism(format!("History has a second {what} of Stream {call}"))
}

fn producer(run: &WorkflowProducer, attempt: WorkflowTaskAttempt) -> Producer {
    Producer {
        kind: Some(Kind::Workflow(WorkflowProducer {
            attempt: Some(attempt),
            ..run.clone()
        })),
    }
}

/// Returns `<run id>/<attempt key in hex>/<seq>`. A new try of an append has the same id, so the
/// Store writes its batch once. A later attempt of the task has another id.
fn request_id(run_id: &str, producer: &Producer, seq: u32) -> String {
    let key: String = (attempt_key(producer.kind.as_ref()).iter())
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{run_id}/{key}/{seq}")
}

fn key(stream: &StreamRef) -> (String, String) {
    (stream.store.clone(), stream.id.clone())
}

#[cfg(test)]
mod tests {
    use super::super::{StreamManager, manager::recorded, read::record_bytes};
    use super::*;
    use crate::{
        telemetry::metrics::MetricsContext,
        test_help::stream_store::{MemoryStore, Request as StoreRequest},
    };
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::Arc;
    use temporalio_common::protos::coresdk::{
        external_data::{StreamAppendOffsets, StreamFailure, stream_append_result},
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamRead, resolve_stream_append, resolve_stream_read,
        },
    };
    use temporalio_streams::{
        AnyError, ReadPage, StreamRuntime,
        proto::{ReadRecordsResponse, StreamRecord},
        record::StoreLimits,
    };
    use tokio::runtime::Handle;
    use workflow_activation_job::Variant;

    const SEEDS: u64 = 400;

    fn runtimes(stores: &[&str]) -> StreamRuntimes {
        StreamRuntimes::new(stores.iter().map(|name| {
            let store = Arc::new(MemoryStore::default());
            let runtime = StreamRuntime::new(store, StoreLimits::default());
            (name.to_string(), Arc::new(runtime))
        }))
    }

    fn calls(phase: Phase, task: i64) -> StreamCalls {
        let mut calls = StreamCalls::new("n", "w", "r", runtimes(&["mem"]), phase);
        calls.set_task(TaskState::Open(task_attempt(task)));
        calls
    }

    /// Completes the current task, and opens the next one.
    fn next_task(calls: &mut StreamCalls, task: i64) {
        calls.set_task(TaskState::NoTask);
        calls.set_task(TaskState::Open(task_attempt(task)));
    }

    fn task_attempt(task: i64) -> WorkflowTaskAttempt {
        WorkflowTaskAttempt {
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
        }
    }

    fn read(seq: u32, id: &str, from_offset: &[u8]) -> ReadStream {
        ReadStream {
            seq,
            stream: Some(stream(id)),
            from_offset: from_offset.to_vec(),
            ..Default::default()
        }
    }

    fn appended(seq: u32) -> stream_append_result::Result {
        stream_append_result::Result::Appended(StreamAppendOffsets {
            first_offset: vec![seq as u8],
            last_offset: vec![seq as u8],
        })
    }

    /// Returns `count` Records after `from_offset`, of `size` bytes of data each.
    fn records(from_offset: &[u8], count: u8, size: usize) -> Vec<StreamRecord> {
        let start = from_offset.first().copied().unwrap_or(0);
        (1..=count)
            .map(|at| StreamRecord {
                offset: vec![start + at],
                body: Some(Payload {
                    data: vec![0; size],
                    ..Default::default()
                }),
                producer: None,
            })
            .collect()
    }

    /// Returns the answer of the Store to read `seq` of `id` from `from_offset`, a page of
    /// `records` with Records after it if `more`.
    fn page(
        seq: u32,
        id: &str,
        from_offset: &[u8],
        records: Vec<StreamRecord>,
        more: bool,
    ) -> ReadDone {
        let last_offset = (records.last()).map_or(from_offset.to_vec(), |r| r.offset.clone());
        let response = ReadRecordsResponse {
            records,
            last_offset,
        };
        let answer = Ok(ReadPage { response, more });
        ReadDone::new(&stream(id), seq, from_offset.to_vec(), answer)
    }

    /// Returns the marker of an activation at `activation` that has the result of append `seq` to
    /// the Stream `id`.
    fn marker(activation: u64, seq: u32, id: &str) -> PeekedMarker {
        let entry = StreamAppendResult {
            seq,
            stream_id: id.to_string(),
            store: "mem".to_string(),
            activation_index: activation,
            result: Some(appended(seq)),
        };
        PeekedMarker::Append(StreamAppendMarkerData {
            appends: vec![entry],
        })
    }

    /// Returns the seq and the activation index of each entry of `marker`.
    fn entries(marker: Option<StreamAppendMarkerData>) -> Vec<(u32, u64)> {
        (marker.into_iter().flat_map(|marker| marker.appends))
            .map(|append| (append.seq, append.activation_index))
            .collect()
    }

    fn refused(message: impl Into<String>) -> StreamFailure {
        recorded(&AnyError::invalid_argument(message))
    }

    /// Returns the calls that started since the last look, as `append 1 of s at 3` with the
    /// started event id of their attempt, or as `read 1 of s`.
    fn started(calls: &mut StreamCalls) -> Vec<String> {
        let mut started: Vec<String> = (calls.take_requests().into_iter())
            .map(|request| {
                let (call, stream, producer) = match request {
                    Request::Run(Call {
                        seq,
                        op: Op::Append(r, _),
                        ..
                    }) => (CallKey::Append(seq), r.r#ref, r.producer),
                    Request::Run(Call {
                        seq,
                        op: Op::Read(r),
                        ..
                    }) => return format!("read {seq} of {}", r.request.r#ref.unwrap().id),
                    Request::Stop(_) => return "stop".to_string(),
                };
                let Some(Kind::Workflow(workflow)) = producer.unwrap().kind else {
                    panic!("{call} has no Workflow Producer");
                };
                let at = workflow.attempt.unwrap().started_event_id;
                format!("{call} of {} at {at}", stream.unwrap().id)
            })
            .collect();
        started.sort();
        started
    }

    /// Gives a running append its result, a failure with `message` if there is one.
    fn finish(calls: &mut StreamCalls, seq: u32, message: Option<&str>) {
        let failed = message.map(refused);
        let AppendState::Running(call_id) = calls.appends[&seq].state else {
            panic!("append {seq} doesn't run");
        };
        let result = match failed {
            Some(failure) => stream_append_result::Result::Failure(failure),
            None => appended(seq),
        };
        calls
            .resolve(StreamResolution {
                call_id,
                seq,
                outcome: Outcome::Append(result),
            })
            .unwrap();
    }

    /// Gives a running read the answer `done`.
    fn answer_read(calls: &mut StreamCalls, seq: u32, done: ReadDone) {
        let ReadState::Running(call_id, ..) = calls.reads[&seq].state else {
            panic!("read {seq} doesn't run");
        };
        calls
            .resolve(StreamResolution {
                call_id,
                seq,
                outcome: Outcome::Read(done),
            })
            .unwrap();
    }

    /// Returns the results of the activation at `activation`, as `append 1`, `append 2: <text>` or
    /// `read 3 of 2`, with the number of its Records.
    fn jobs(calls: &mut StreamCalls, activation: u64) -> Vec<String> {
        (calls.take_jobs(activation, None).into_iter())
            .map(|job| match job {
                Variant::ResolveStreamAppend(ResolveStreamAppend {
                    seq,
                    status: Some(resolve_stream_append::Status::Failed(failure)),
                }) => format!("append {seq}: {}", failure.message),
                Variant::ResolveStreamAppend(append) => format!("append {}", append.seq),
                Variant::ResolveStreamRead(ResolveStreamRead {
                    seq,
                    status: Some(resolve_stream_read::Status::Page(page)),
                }) => format!("read {seq} of {}", page.records.len()),
                Variant::ResolveStreamRead(read) => format!("read {}: failed", read.seq),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    /// Returns the key of a call that runs, and the id of its Stream.
    fn target(call: &Call) -> (CallKey, &str) {
        match &call.op {
            Op::Append(request, _) => (
                CallKey::Append(call.seq),
                &request.r#ref.as_ref().unwrap().id,
            ),
            Op::Read(read) => (
                CallKey::Read(call.seq),
                &read.request.r#ref.as_ref().unwrap().id,
            ),
        }
    }

    /// Moves the calls that the run starts into `in_flight`, and drops the ones that it stops.
    fn dispatch(calls: &mut StreamCalls, in_flight: &mut Vec<Call>) {
        for request in calls.take_requests() {
            match request {
                Request::Run(call) => in_flight.push(call),
                Request::Stop(call_id) => in_flight.retain(|call| call.call_id != call_id),
            }
        }
    }

    /// Returns the Store's answer to an append `call`, which is `failure` if there is one.
    fn answer(call: &Call, failure: Option<StreamFailure>) -> StreamResolution {
        let outcome = match failure {
            None => Outcome::Append(appended(call.seq)),
            Some(failure) => Outcome::Append(stream_append_result::Result::Failure(failure)),
        };
        StreamResolution {
            call_id: call.call_id,
            seq: call.seq,
            outcome,
        }
    }

    #[test]
    fn a_live_call_keeps_the_attempt_of_the_task_in_hand() {
        let mut calls = calls(Phase::Live, 3);
        calls.append(append(1, "s")).unwrap();
        calls.append(append(2, "s")).unwrap();
        assert_eq!(started(&mut calls), ["append 1 of s at 3"]);
        // Append 2 waits in the queue of its Stream past the end of its task.
        next_task(&mut calls, 7);
        calls.append(append(3, "s")).unwrap();
        finish(&mut calls, 1, None);
        assert_eq!(started(&mut calls), ["append 2 of s at 3"]);
        finish(&mut calls, 2, None);
        assert_eq!(started(&mut calls), ["append 3 of s at 7"]);
    }

    #[test]
    fn a_result_waits_with_its_call_until_an_activation_takes_it() {
        let mut live = calls(Phase::Live, 3);
        live.append(append(1, "s")).unwrap();
        live.append(append(2, "t")).unwrap();
        started(&mut live);
        finish(&mut live, 2, None);
        finish(&mut live, 1, None);
        live.set_task(TaskState::Completing(task_attempt(3)));
        assert!(!live.has_jobs(4, None));
        assert_eq!(jobs(&mut live, 4), Vec::<String>::new());
        assert_eq!(live.take_marker(), None);
        next_task(&mut live, 7);
        assert!(live.has_jobs(0, None));
        assert_eq!(jobs(&mut live, 0), ["append 1", "append 2"]);
        assert_eq!(entries(live.take_marker()), [(1, 0), (2, 0)]);
        live.append(append(3, "s")).unwrap();
        started(&mut live);
        finish(&mut live, 3, None);
        assert_eq!(jobs(&mut live, 2), ["append 3"]);
        assert_eq!(entries(live.take_marker()), [(3, 2)]);
    }

    #[test]
    fn a_release_ends_with_its_activation() {
        let mut replay = calls(Phase::Replaying, 9);
        replay.append(append(1, "1")).unwrap();
        replay.peeked(marker(2, 1, "1"), vec![]).unwrap();
        replay.release_ready();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
        // A result of the next task waits for its own activation.
        replay.append(append(2, "2")).unwrap();
        replay.peeked(marker(1, 2, "2"), vec![]).unwrap();
        assert!(!replay.has_jobs(0, None));
    }

    // Core asks for a new task for an open read only when it knows for certain that the Stream
    // has Records for it. Otherwise the read stops at the completion and starts in the next task.
    #[test]
    fn a_read_needs_the_next_task_only_when_its_stream_has_more_for_it() {
        let mut live = calls(Phase::Live, 3);
        live.read(read(1, "s", b"")).unwrap();
        assert_eq!(started(&mut live), ["read 1 of s"]);
        // A full page with nothing after it says nothing of the Stream.
        answer_read(&mut live, 1, page(1, "s", b"", records(b"", 2, 1), false));
        assert_eq!(jobs(&mut live, 0), ["read 1 of 2"]);
        live.read(read(2, "s", &[2])).unwrap();
        assert_eq!(started(&mut live), ["read 2 of s"]);
        assert!(!live.needs_next_task());
        // The completion stops the read, and the next task starts it again.
        live.stop_idle_reads();
        assert_eq!(started(&mut live), ["stop"]);
        assert_eq!(live.take_completion_marker(false, false, false), None);
        next_task(&mut live, 7);
        assert_eq!(started(&mut live), ["read 2 of s"]);
        // A page that the runtime cut while the Store had more makes a read from its end need the
        // next task, and a read from elsewhere not.
        answer_read(&mut live, 2, page(2, "s", &[2], records(&[2], 1, 1), true));
        assert_eq!(jobs(&mut live, 0), ["read 2 of 1"]);
        live.read(read(3, "s", &[9])).unwrap();
        assert!(!live.needs_next_task());
        let mut waiting = read(4, "s", &[3]);
        waiting.wait = Some(prost_types::Duration {
            seconds: 1,
            nanos: 0,
        });
        live.read(waiting).unwrap();
        assert_eq!(started(&mut live), ["read 3 of s", "read 4 of s"]);
        assert!(live.needs_next_task());
        live.stop_idle_reads();
        assert_eq!(started(&mut live), ["stop"]);
        // A forced completion with no marker in its task leaves a marker of the read kind.
        live.set_task(TaskState::NoTask);
        assert_eq!(
            live.take_completion_marker(true, false, false),
            Some(PeekedMarker::Read(Default::default()))
        );
        live.set_task(TaskState::Open(task_attempt(11)));
        started(&mut live);
        // An empty page that Core made at the read limit keeps what the Stream had.
        live.read_limit().unwrap();
        live.reads.remove(&3);
        let empty = jobs(&mut live, 0);
        assert_eq!(empty, ["read 4 of 0"]);
        live.read(read(5, "s", &[3])).unwrap();
        assert!(live.needs_next_task());
        // A failed read forgets it.
        started(&mut live);
        let ReadState::Running(call_id, ..) = live.reads[&5].state else {
            panic!("read 5 doesn't run");
        };
        let failed = ReadDone::new(&stream("s"), 5, vec![3], Err(refused("down")));
        live.resolve(StreamResolution {
            call_id,
            seq: 5,
            outcome: Outcome::Read(failed),
        })
        .unwrap();
        assert_eq!(jobs(&mut live, 1), ["read 5: failed"]);
        live.read(read(6, "s", &[3])).unwrap();
        assert!(!live.needs_next_task());
    }

    // A page that doesn't fit the Record budget of the task holds the task no longer, makes Core
    // ask for the next task, and comes in its first activation.
    #[test]
    fn a_page_over_the_record_budget_comes_first_in_the_next_task() {
        let mut live = calls(Phase::Live, 3);
        live.runtimes.settings.max_task_record_bytes = 100;
        live.read(read(1, "s", b"")).unwrap();
        live.read(read(2, "t", b"")).unwrap();
        started(&mut live);
        let first = records(b"", 1, 60);
        let bytes = record_bytes(&first);
        answer_read(&mut live, 1, page(1, "s", b"", first, false));
        answer_read(&mut live, 2, page(2, "t", b"", records(b"", 1, 60), false));
        assert_eq!(jobs(&mut live, 0), ["read 1 of 1"]);
        assert_eq!(live.task_record_bytes, bytes);
        assert!(!live.has_jobs(1, None));
        assert_eq!(live.outstanding(false), 0);
        assert!(live.needs_next_task());
        live.stop_idle_reads();
        assert_eq!(started(&mut live), Vec::<String>::new());
        next_task(&mut live, 7);
        assert_eq!(jobs(&mut live, 0), ["read 2 of 1"]);
    }

    // A read that the read result budget of the task holds back gets no check, makes Core ask for
    // the next task, and starts when that task opens. The command that ends the Workflow drops it,
    // so that task then needs no next task.
    #[test]
    fn a_read_that_the_result_budget_holds_back_needs_the_next_task() {
        let mut live = calls(Phase::Live, 3);
        live.runtimes.settings.max_task_read_results = 1;
        live.read(read(1, "s", b"")).unwrap();
        assert_eq!(started(&mut live), ["read 1 of s"]);
        answer_read(&mut live, 1, page(1, "s", b"", vec![], false));
        assert_eq!(jobs(&mut live, 0), ["read 1 of 0"]);
        assert!(!live.needs_next_task());
        live.read(read(2, "s", b"")).unwrap();
        assert_eq!(started(&mut live), Vec::<String>::new());
        assert_eq!(live.outstanding(false), 0);
        live.check_open_reads().unwrap();
        assert_eq!(started(&mut live), Vec::<String>::new());
        assert!(live.needs_next_task());
        next_task(&mut live, 7);
        assert_eq!(started(&mut live), ["read 2 of s"]);
        assert!(!live.needs_next_task());
        answer_read(&mut live, 2, page(2, "s", b"", vec![], false));
        assert_eq!(jobs(&mut live, 0), ["read 2 of 0"]);
        live.read(read(3, "s", b"")).unwrap();
        assert!(live.needs_next_task());
        live.stop_for_terminal();
        assert!(!live.needs_next_task());
    }

    /// A command of Workflow code in a generated script.
    #[derive(Clone, Debug)]
    enum Command {
        Append(AppendToStream),
        Read(ReadStream),
        /// A command that ends the Workflow.
        Terminal,
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
        /// True if the events of the task give Workflow code a job, so the task has a first
        /// activation even with no Stream result.
        has_events: bool,
        activations: Vec<Activation>,
        /// The marker of each activation that got a live result, in order.
        markers: Vec<PeekedMarker>,
        /// True if this task sent the last command of the Workflow.
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

    /// The calls of the entries of `marker`.
    fn calls_of(marker: &PeekedMarker) -> Vec<CallKey> {
        match marker {
            PeekedMarker::Append(data) => (data.appends.iter())
                .map(|append| CallKey::Append(append.seq))
                .collect(),
            PeekedMarker::Read(data) => (data.reads.iter())
                .map(|read| CallKey::Read(read.seq))
                .collect(),
        }
    }

    /// The Records that the first run gave each read, by its seq. A replay reads them again.
    type Given = HashMap<u32, Vec<StreamRecord>>;

    /// Returns what a replay reads for the entries of `marker`.
    fn replay_reads(marker: &PeekedMarker, given: &Given) -> MarkerReads {
        let PeekedMarker::Read(data) = marker else {
            return vec![];
        };
        (data.reads.iter())
            .map(|read| {
                let records = given.get(&read.seq).cloned().unwrap_or_default();
                needs_records(read).then_some(Ok(records))
            })
            .collect()
    }

    /// A run that a generated test drives as the machines of Core do.
    struct Run {
        calls: StreamCalls,
        /// The index of the next activation of the current task.
        activation: u64,
        /// True once Workflow code sent its last command.
        ended: bool,
    }

    impl Run {
        /// Gives Workflow code an activation, and takes its commands. Returns the jobs and the
        /// markers of the activation.
        fn activate(
            &mut self,
            commands: &[Command],
        ) -> Result<(Vec<Variant>, Vec<PeekedMarker>), WFMachinesError> {
            let jobs = self.calls.take_jobs(self.activation, None);
            let appends = self.calls.take_marker().map(PeekedMarker::Append);
            let reads = self.calls.take_read_marker().map(PeekedMarker::Read);
            self.activation += 1;
            for command in commands {
                match command {
                    Command::Append(append) => self.calls.append(append.clone())?,
                    Command::Read(read) => self.calls.read(read.clone())?,
                    Command::Terminal => {
                        self.calls.stop_for_terminal();
                        self.ended = true;
                    }
                }
            }
            Ok((jobs, appends.into_iter().chain(reads).collect()))
        }

        /// Returns the calls that the run still tracks.
        fn tracked(&self) -> BTreeSet<CallKey> {
            let appends = self.calls.appends.keys().map(|seq| CallKey::Append(*seq));
            let reads = self.calls.reads.keys().map(|seq| CallKey::Read(*seq));
            appends.chain(reads).collect()
        }
    }

    /// What a replay gives.
    struct Replayed {
        jobs: Vec<TaskJobs>,
        /// The task that sent the last command of the Workflow.
        sent_in: Option<usize>,
        run: Run,
    }

    /// Replays `history` with the commands of the first run of `seed`, and goes live in the task
    /// `live`, as a new run of Core does.
    fn replay(
        seed: u64,
        history: &[Task],
        given: &Given,
        live: WorkflowTaskAttempt,
    ) -> Result<Replayed, WFMachinesError> {
        let calls = StreamCalls::new("n", "w", "r", runtimes(&["mem"]), Phase::Replaying);
        let mut run = Run {
            calls,
            activation: 0,
            ended: false,
        };
        run.calls.set_task(TaskState::Open(live));
        let mut replayed = vec![];
        let mut sent_in = None;
        for (index, task) in history.iter().enumerate() {
            if index > 0 && !history[index - 1].markers.is_empty() {
                run.calls.marker_recorded()?;
            }
            // Core peeks the markers that the task's completion recorded before it activates.
            for marker in &task.markers {
                run.calls
                    .peeked(marker.clone(), replay_reads(marker, given))?;
            }
            run.activation = 0;
            let mut activations = vec![];
            let mut first = task.has_events;
            while !run.ended {
                if !first && !run.calls.has_jobs(run.activation, None) {
                    // For a History with chunking off, Core sets its activation index to that of
                    // the next result from History when nothing else needs an activation.
                    match run.calls.next_ready_activation() {
                        Some(at) if at > run.activation => run.activation = at,
                        _ => break,
                    }
                    continue;
                }
                first = false;
                let at = run.activation;
                let (jobs, markers) = run.activate(&task.commands_at(at))?;
                assert_eq!(markers, vec![], "seed {seed}: a replay records a result");
                activations.push((at, jobs));
            }
            if run.ended && sent_in.is_none() {
                sent_in = Some(index);
            }
            let requests = run.calls.take_requests();
            assert!(requests.is_empty(), "seed {seed}: {requests:?}");
            replayed.push(activations);
        }
        if history.last().is_some_and(|task| !task.markers.is_empty()) {
            run.calls.marker_recorded()?;
        }
        run.activation = 0;
        run.calls.go_live()?;
        Ok(Replayed {
            jobs: replayed,
            sent_in,
            run,
        })
    }

    /// The cases that the generated scripts reached, over all seeds.
    #[derive(Default)]
    struct Seen {
        heartbeats: usize,
        failed_tasks: usize,
        /// The last commands that Workflow code sent while a call had no result in a marker.
        dropped: usize,
        /// The commands with a Stream id or a Store name out of range.
        invalid: usize,
        bad_seqs: usize,
        read_limits: usize,
        /// Completions that stopped a read, and ones that asked for a new task for one.
        stopped_reads: usize,
        forced_by_reads: usize,
        held_pages: usize,
        /// Checks before a completion, the reads that a check parked, and tasks that a read in an
        /// outage failed.
        checks: usize,
        parked: usize,
        outages: usize,
        /// Tasks that held back a read since they gave all the read results that they can.
        full_budgets: usize,
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
        task_attempt: WorkflowTaskAttempt,
        /// The Store calls in flight, which the Store answers in any order.
        in_flight: Vec<Call>,
        /// Results that arrived while the run had no task. Core applies them after the events of
        /// the next task.
        deferred: Vec<StreamResolution>,
        last_append: u32,
        last_read: u32,
        /// The offset of the last Record that the Store gave on each Stream.
        offsets: HashMap<String, u8>,
        given: Given,
        /// The calls whose result no marker has.
        unresolved: BTreeSet<CallKey>,
        /// The state as of the last task in History, which a failed task goes back to.
        committed: (u32, u32, BTreeSet<CallKey>),
        /// True if the task in hand has a command for the server.
        has_work: bool,
        /// The reads that the read result budget of the last task held back.
        held_back: Vec<u32>,
    }

    impl<'a> FirstRun<'a> {
        fn new(seed: u64, seen: &'a mut Seen) -> Self {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut calls = StreamCalls::new("n", "w", "r", runtimes(&["mem"]), Phase::Live);
            if rng.random_bool(0.3) {
                calls.runtimes.settings.max_task_record_bytes = rng.random_range(1..400);
            }
            if rng.random_bool(0.3) {
                calls.runtimes.settings.max_task_read_results = rng.random_range(1..4);
            }
            let run = Run {
                calls,
                activation: 0,
                ended: false,
            };
            Self {
                seed,
                rng,
                seen,
                run,
                history: vec![],
                task: Task::default(),
                task_attempt: task_attempt(3),
                in_flight: vec![],
                deferred: vec![],
                last_append: 0,
                last_read: 0,
                offsets: HashMap::new(),
                given: Given::new(),
                unresolved: BTreeSet::new(),
                committed: Default::default(),
                has_work: false,
                held_back: vec![],
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
                        self.task_attempt = WorkflowTaskAttempt {
                            started_event_id: self.task_attempt.started_event_id + 4,
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
            self.run.calls.set_task(TaskState::Open(self.task_attempt));
            // A read that the budget held back starts as soon as the task that it forced opens.
            for seq in std::mem::take(&mut self.held_back) {
                let state = self.run.calls.reads.get(&seq).map(|read| &read.state);
                let runs = matches!(state, Some(ReadState::Running(..)));
                assert!(runs, "seed {}: read {seq} doesn't run", self.seed);
            }
            if (self.history.last()).is_some_and(|task| !task.markers.is_empty()) {
                self.run.calls.marker_recorded().unwrap();
            }
            self.run.activation = 0;
            for resolution in std::mem::take(&mut self.deferred) {
                let seed = self.seed;
                (self.run.calls.resolve(resolution))
                    .unwrap_or_else(|e| panic!("seed {seed}: {e:?}"));
            }
            self.dispatch();
        }

        /// Runs the current task until it completes, and returns an error if it fails.
        fn run_task(&mut self) -> Result<(), ()> {
            self.task.has_events = self.history.is_empty() || self.rng.random_bool(0.6);
            self.has_work = self.rng.random_bool(0.4);
            if self.task.has_events || self.run.calls.has_jobs(self.run.activation, None) {
                self.activate()?;
            }
            let mut checked = false;
            loop {
                if self.run.ended {
                    self.complete(false);
                    return Ok(());
                }
                if self.run.calls.has_jobs(self.run.activation, None) {
                    self.activate()?;
                    continue;
                }
                if self.run.calls.outstanding(self.has_work) == 0 {
                    // Core checks the reads that the completion would stop once per completion.
                    if !checked {
                        checked = true;
                        self.check_open_reads()?;
                        if self.run.calls.outstanding(self.has_work) > 0 {
                            continue;
                        }
                    }
                    self.complete(false);
                    return Ok(());
                }
                let seed = self.seed;
                assert!(
                    !self.in_flight.is_empty(),
                    "seed {seed}: a call waits, and none runs"
                );
                match self.rng.random_range(0..48) {
                    // Core gives an activation for the first answer that arrives.
                    0..=35 => {
                        let resolution = self.answer();
                        self.resolve(resolution)?;
                        checked = false;
                    }
                    36..=38 => {
                        // The heartbeat completes the task, and fails it for a read in an outage.
                        // Results that arrive before the completion wait for the next task.
                        self.seen.heartbeats += 1;
                        self.check_outage(|calls| calls.check_read_outage())?;
                        self.run
                            .calls
                            .set_task(TaskState::Completing(self.task_attempt));
                        for _ in 0..self.rng.random_range(0..=2) {
                            if !self.in_flight.is_empty() {
                                let resolution = self.answer();
                                self.resolve(resolution)?;
                            }
                        }
                        self.complete(true);
                        return Ok(());
                    }
                    39..=42 => {
                        self.seen.read_limits += 1;
                        self.check_outage(StreamCalls::read_limit)?;
                        self.dispatch();
                    }
                    43..=46 => self.fail_a_try(),
                    _ => return Err(()),
                }
            }
        }

        /// Readies the reads for a completion, as Core does when nothing else holds the task.
        fn check_open_reads(&mut self) -> Result<(), ()> {
            self.run.calls.hold_check_pages(self.has_work);
            // Only a read that runs gets a check, so a read that the budget holds back gets none.
            let waiting: BTreeSet<u32> = (self.run.calls.reads.iter())
                .filter(|(_, read)| matches!(read.state, ReadState::Waiting))
                .map(|(seq, _)| *seq)
                .collect();
            self.check_outage(StreamCalls::check_open_reads)?;
            let checks: Vec<u32> = (self.run.calls.requests.iter())
                .filter_map(|r| match r {
                    Request::Run(Call {
                        seq,
                        op: Op::Read(read),
                        ..
                    }) if read.check => Some(*seq),
                    _ => None,
                })
                .collect();
            let seed = self.seed;
            assert!(
                checks.iter().all(|seq| !waiting.contains(seq)),
                "seed {seed}"
            );
            self.seen.checks += checks.len();
            self.dispatch();
            Ok(())
        }

        /// Runs a step that fails the task for a read in an outage, and checks that it does so
        /// only for such a read.
        fn check_outage(
            &mut self,
            step: impl FnOnce(&mut StreamCalls) -> Result<(), WFMachinesError>,
        ) -> Result<(), ()> {
            let in_outage = (self.run.calls.reads.values()).any(|read| match &read.state {
                ReadState::Running(_, last_try, _) => last_try.lock().is_some(),
                _ => false,
            });
            let live = self.run.calls.phase == Phase::Live && self.run.calls.task.is_open();
            let result = step(&mut self.run.calls);
            let seed = self.seed;
            match result {
                Err(WFMachinesError::Stream(message)) => {
                    assert!(in_outage && live, "seed {seed}: {message}");
                    assert!(message.starts_with("StreamStoreUnavailable: read of Stream"));
                    self.seen.outages += 1;
                    Err(())
                }
                Err(error) => panic!("seed {seed}: {error:?}"),
                Ok(()) => {
                    assert!(!in_outage || !live, "seed {seed}");
                    Ok(())
                }
            }
        }

        /// Fails the last try of a read that runs, as the Store in an outage does, while the read
        /// waits to try again.
        fn fail_a_try(&mut self) {
            let running: Vec<&Read> = (self.run.calls.reads.values())
                .filter(|read| matches!(read.state, ReadState::Running(_, _, false)))
                .collect();
            if running.is_empty() {
                return;
            }
            let read = running[self.rng.random_range(0..running.len())];
            if let ReadState::Running(_, last_try, _) = &read.state {
                *last_try.lock() = Some(StreamFailure {
                    reason: StreamErrorReason::StoreUnavailable
                        .as_str_name()
                        .to_string(),
                    message: "Down".to_string(),
                });
            }
        }

        /// Handles an answer as Core does. A check that meets an outage fails the task.
        fn resolve(&mut self, resolution: StreamResolution) -> Result<(), ()> {
            let parks = (self.run.calls.reads.get(&resolution.seq)).is_some_and(|read| {
                matches!(read.state, ReadState::Running(id, _, true) if id == resolution.call_id)
            });
            match self.run.calls.resolve(resolution) {
                Ok(()) => {}
                Err(WFMachinesError::Stream(message)) => {
                    assert!(message.starts_with("StreamStoreUnavailable: read of Stream"));
                    self.seen.outages += 1;
                    return Err(());
                }
                Err(error) => panic!("seed {}: {error:?}", self.seed),
            }
            let seq_parked = |calls: &StreamCalls| {
                (calls.reads.values())
                    .filter(|read| matches!(read.state, ReadState::Waiting))
                    .count()
            };
            self.seen.parked += usize::from(parks && seq_parked(&self.run.calls) > 0);
            self.dispatch();
            Ok(())
        }

        /// Gives Workflow code an activation with commands that the script makes up.
        fn activate(&mut self) -> Result<(), ()> {
            let index = self.run.activation;
            let (commands, fatal) = self.commands();
            // The answers that arrived while Workflow code had the activation. Core handles them
            // after the commands of the completion.
            let came = [0, 0, 1, 2, 3, 8][self.rng.random_range(0..6)].min(self.in_flight.len());
            let late: Vec<StreamResolution> = (0..came).map(|_| self.answer()).collect();
            let result = self.run.activate(&commands);
            if let Some(expected) = fatal {
                assert_eq!(result, Err(expected), "seed {}", self.seed);
                return Err(());
            }
            let (jobs, markers) =
                result.unwrap_or_else(|error| panic!("seed {}: {error:?}", self.seed));
            self.task.markers.extend(markers);
            for job in &jobs {
                if let Variant::ResolveStreamRead(ResolveStreamRead {
                    seq,
                    status: Some(resolve_stream_read::Status::Page(page)),
                }) = job
                {
                    self.given.insert(*seq, page.records.clone());
                }
            }
            for resolution in late {
                self.resolve(resolution)?;
            }
            for command in &commands {
                match command {
                    Command::Append(append) => {
                        self.unresolved.insert(CallKey::Append(append.seq));
                    }
                    Command::Read(read) => {
                        self.unresolved.insert(CallKey::Read(read.seq));
                    }
                    Command::Terminal => {
                        self.seen.dropped += usize::from(!self.unresolved.is_empty());
                        self.unresolved.clear();
                    }
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

        /// Returns the commands of an activation. If the last one isn't valid, it also returns the
        /// error that fails the Workflow Task.
        fn commands(&mut self) -> (Vec<Command>, Option<WFMachinesError>) {
            let mut commands = vec![];
            for _ in 0..self.rng.random_range(0..=3) {
                let id = format!("s{}", self.rng.random_range(0..3));
                match self.rng.random_range(0..100) {
                    0..=44 => {
                        self.last_append += 1;
                        commands.push(Command::Append(append(self.last_append, &id)));
                    }
                    45..=69 => {
                        self.last_read += 1;
                        let from_offset = match self.rng.random_bool(0.7) {
                            true => self.offsets.get(&id).map_or(vec![], |at| vec![*at]),
                            false => vec![self.rng.random_range(0..200)],
                        };
                        let mut command = read(self.last_read, &id, &from_offset);
                        command.max_records = self.rng.random_range(0..4);
                        // A check with no Record parks a read with a wait, so the completion
                        // stops it and forces no task.
                        command.wait = self.rng.random_bool(0.3).then_some(prost_types::Duration {
                            seconds: 1,
                            nanos: 0,
                        });
                        commands.push(Command::Read(command));
                    }
                    70..=72 => {
                        self.seen.invalid += 1;
                        let long = "x".repeat(4097);
                        let (command, error) = match self.rng.random_range(0..3) {
                            0 => {
                                self.last_append += 1;
                                let seq = self.last_append;
                                let error = format!(
                                    "Stream append {seq} is invalid: The Stream id has 4097 \
                                     bytes, more than 4096"
                                );
                                (Command::Append(append(seq, &long)), error)
                            }
                            1 => {
                                self.last_read += 1;
                                let seq = self.last_read;
                                let error =
                                    format!("Stream read {seq} is invalid: The Stream id is empty");
                                (Command::Read(read(seq, "", b"")), error)
                            }
                            _ => {
                                self.last_append += 1;
                                let seq = self.last_append;
                                let mut long_store = append(seq, &id);
                                long_store.stream.as_mut().unwrap().store = long;
                                let error = format!(
                                    "Stream append {seq} is invalid: The Store name has 4097 \
                                     bytes, more than 4096"
                                );
                                (Command::Append(long_store), error)
                            }
                        };
                        commands.push(command);
                        return (commands, Some(WFMachinesError::Fatal(error)));
                    }
                    80..=82 => {
                        self.seen.bad_seqs += 1;
                        let seq = self.last_append;
                        let error = match seq {
                            0 => "Stream append has seq 0, and a seq starts at 1".to_string(),
                            _ => format!("Stream append {seq} came after append {seq}"),
                        };
                        commands.push(Command::Append(append(seq, &id)));
                        return (commands, Some(WFMachinesError::Fatal(error)));
                    }
                    83..=86 => {
                        commands.push(Command::Terminal);
                        return (commands, None);
                    }
                    _ => {}
                }
            }
            (commands, None)
        }

        /// Takes a random call in flight, and returns the Store's answer to it.
        fn answer(&mut self) -> StreamResolution {
            let call = (self.in_flight).swap_remove(self.rng.random_range(0..self.in_flight.len()));
            let mut failure = self.rng.random_bool(0.2).then(|| refused("refused"));
            let Op::Read(read) = &call.op else {
                return answer(&call, failure);
            };
            // A check has one try, so it returns the outage of the Store.
            if read.check && self.rng.random_bool(0.1) {
                failure = Some(StreamFailure {
                    reason: StreamErrorReason::StoreUnavailable
                        .as_str_name()
                        .to_string(),
                    message: "Down".to_string(),
                });
            }
            let request = &read.request;
            let id = request.r#ref.as_ref().unwrap().id.clone();
            let done = match failure {
                Some(failure) => {
                    let from_offset = request.from_offset.clone();
                    ReadDone::new(&stream(&id), call.seq, from_offset, Err(failure))
                }
                None => {
                    let count = self.rng.random_range(0..=request.max_records.min(3) as u8);
                    let size = self.rng.random_range(0..100);
                    let page_records = records(&request.from_offset, count, size);
                    if let Some(last) = page_records.last() {
                        self.offsets.insert(id.clone(), last.offset[0]);
                    }
                    let more = count > 0 && self.rng.random_bool(0.4);
                    page(call.seq, &id, &request.from_offset, page_records, more)
                }
            };
            StreamResolution {
                call_id: call.call_id,
                seq: call.seq,
                outcome: Outcome::Read(done),
            }
        }

        fn dispatch(&mut self) {
            dispatch(&mut self.run.calls, &mut self.in_flight);
        }

        /// Completes the task in hand as Core does, with a new task if a heartbeat forces it or a
        /// call needs it. Checks that the run tracks exactly the calls whose result no marker has.
        fn complete(&mut self, heartbeat: bool) {
            let needs_next_task = self.run.calls.needs_next_task();
            let forced = heartbeat || needs_next_task;
            // A read that the budget holds back forces the next task, and gets no check first.
            let calls = &self.run.calls;
            let budget = calls.runtimes.settings.max_task_read_results;
            let held_back: Vec<u32> = (calls.reads.iter())
                .filter(|(_, read)| matches!(read.state, ReadState::Waiting) && !read.checked)
                .filter(|_| calls.task_read_results >= budget)
                .map(|(seq, _)| *seq)
                .collect();
            assert!(held_back.is_empty() || forced, "seed {}", self.seed);
            self.seen.full_budgets += usize::from(!held_back.is_empty());
            self.held_back = held_back;
            let reads_open = !self.run.calls.reads.is_empty();
            self.seen.forced_by_reads +=
                usize::from(reads_open && needs_next_task && self.run.calls.appends.is_empty());
            self.seen.held_pages += usize::from((self.run.calls.reads.values()).any(
                |read| matches!(&read.state, ReadState::Done(done) if !done.records.is_empty()),
            ));
            if !forced {
                self.seen.stopped_reads += usize::from(reads_open);
                self.run.calls.stop_idle_reads();
                self.dispatch();
            }
            let open = self.run.calls.take_completion_marker(forced, false, false);
            self.task.markers.extend(open);
            // Workflow code never gets the outage of a Store from a live read.
            let outage = |marker: &PeekedMarker| match marker {
                PeekedMarker::Read(data) => (data.reads.iter()).any(|read| {
                    matches!(&read.result,
                        Some(stream_read_result::Result::Failure(f)) if is_outage(f))
                }),
                PeekedMarker::Append(_) => false,
            };
            assert!(!self.task.markers.iter().any(outage), "seed {}", self.seed);
            self.run.calls.set_task(TaskState::NoTask);
            for marker in &self.task.markers {
                for call in calls_of(marker) {
                    self.unresolved.remove(&call);
                }
            }
            let mut task = std::mem::take(&mut self.task);
            task.ends_workflow = self.run.ended;
            self.history.push(task);
            self.committed = (self.last_append, self.last_read, self.unresolved.clone());
            let seed = self.seed;
            assert_eq!(self.run.tracked(), self.unresolved, "seed {seed}");
            let in_flight = &self.in_flight;
            assert!(
                !self.run.ended || in_flight.is_empty(),
                "seed {seed}: {in_flight:?}"
            );
            // A completion that asks for no new task leaves no read running.
            let running = |call: &Call| matches!(call.op, Op::Read(_));
            assert!(forced || !in_flight.iter().any(running), "seed {seed}");
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

        /// Lets the Store answer while the run has no task. Core applies these results after the
        /// events of the next task.
        fn between_tasks(&mut self) {
            for _ in 0..self.rng.random_range(0..=2) {
                if !self.in_flight.is_empty() {
                    let resolution = self.answer();
                    self.deferred.push(resolution);
                }
            }
        }

        /// Fails the current task. A new run replays History and goes live in the next attempt.
        fn retry(&mut self) {
            self.seen.failed_tasks += 1;
            self.run.calls.stop();
            self.in_flight.clear();
            self.deferred.clear();
            (self.last_append, self.last_read, self.unresolved) = self.committed.clone();
            self.task = Task::default();
            self.held_back.clear();
            self.task_attempt.attempt += 1;
            let budget = self.run.calls.runtimes.settings.max_task_record_bytes;
            self.run = self.check_replay(self.task_attempt);
            self.run.calls.runtimes.settings.max_task_record_bytes = budget;
            self.dispatch();
        }

        /// Replays History, checks it against the first run, and returns the replayed run.
        fn check_replay(&self, live: WorkflowTaskAttempt) -> Run {
            let seed = self.seed;
            let Replayed {
                jobs: replayed,
                sent_in,
                mut run,
            } = replay(seed, &self.history, &self.given, live)
                .unwrap_or_else(|error| panic!("seed {seed}: {error:?}"));
            let first: Vec<_> = self.history.iter().map(Task::jobs).collect();
            assert_eq!(replayed, first, "seed {seed}");
            let ended = self.history.iter().position(|task| task.ends_workflow);
            assert_eq!(sent_in, ended, "seed {seed}");
            // The replay resumes each call whose result no marker has, and only those.
            assert_eq!(run.tracked(), self.unresolved, "seed {seed}");
            let recorded: BTreeSet<CallKey> = (self.history.iter())
                .flat_map(|task| task.markers.iter())
                .flat_map(calls_of)
                .collect();
            for request in run.calls.take_requests() {
                let Request::Run(call) = &request else {
                    panic!("seed {seed}: {request:?}");
                };
                let (key, _) = target(call);
                assert!(!recorded.contains(&key), "seed {seed}: {key} ran again");
                if let Op::Append(append, _) = &call.op {
                    let expected = producer(&run.calls.producer, live);
                    assert_eq!(append.producer, Some(expected), "seed {seed}");
                }
                run.calls.requests.push(request);
            }
            run
        }
    }

    #[test]
    fn a_replay_of_history_gives_workflow_code_the_same_jobs_in_the_same_activations() {
        let mut seen = Seen::default();
        for seed in 0..SEEDS {
            let mut first = FirstRun::new(seed, &mut seen);
            first.play(12);
            let live = WorkflowTaskAttempt {
                started_event_id: first.task_attempt.started_event_id + 4,
                ..first.task_attempt
            };
            first.check_replay(live);
        }
        // The scripts reach each case that the test is for.
        let Seen {
            heartbeats,
            failed_tasks,
            dropped,
            invalid,
            bad_seqs,
            read_limits,
            stopped_reads,
            forced_by_reads,
            held_pages,
            checks,
            parked,
            outages,
            full_budgets,
        } = seen;
        let counts = [
            heartbeats,
            failed_tasks,
            dropped,
            invalid,
            bad_seqs,
            read_limits,
            stopped_reads,
            forced_by_reads,
            held_pages,
            checks,
            parked,
            outages,
            full_budgets,
        ];
        assert!(counts.iter().all(|count| *count > 0), "{counts:?}");
    }

    #[test]
    fn the_calls_of_one_stream_go_to_the_store_one_at_a_time_in_call_order() {
        let mut together = 0;
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut calls = calls(Phase::Live, 3);
            // The calls of each Stream that have no result yet, in the order of their commands.
            let mut queues: BTreeMap<String, VecDeque<CallKey>> = BTreeMap::new();
            let mut in_flight: Vec<Call> = vec![];
            let (mut appends, mut task, mut ended) = (0, 3, false);
            for _ in 0..60 {
                let id = format!("s{}", rng.random_range(0..4));
                match rng.random_range(0..20) {
                    _ if ended => {}
                    0..=15 => {
                        appends += 1;
                        calls.append(append(appends, &id)).unwrap();
                        queues
                            .entry(id)
                            .or_default()
                            .push_back(CallKey::Append(appends));
                    }
                    16 => {
                        // The last command of the Workflow stops each call.
                        calls.stop_for_terminal();
                        queues.clear();
                        ended = true;
                    }
                    _ => {}
                }
                if rng.random_bool(0.15) {
                    task += 4;
                    next_task(&mut calls, task);
                }
                dispatch(&mut calls, &mut in_flight);
                // The Store answers some of the calls in flight, in any order.
                for _ in 0..rng.random_range(0..=2) {
                    if in_flight.is_empty() {
                        break;
                    }
                    let call = in_flight.swap_remove(rng.random_range(0..in_flight.len()));
                    let (key, id) = target(&call);
                    let oldest = queues.get_mut(id).and_then(VecDeque::pop_front);
                    assert_eq!(oldest, Some(key), "seed {seed}: Stream {id}");
                    let failure = rng.random_bool(0.3).then(|| refused("no"));
                    calls.resolve(answer(&call, failure)).unwrap();
                    dispatch(&mut calls, &mut in_flight);
                }
                // Each Stream with calls that wait has its oldest one in flight, and only that
                // one, whatever the other Streams do.
                for id in (0..4).map(|index| format!("s{index}")) {
                    let of_stream = in_flight.iter().map(target).filter(|(_, of)| *of == id);
                    let running: Vec<CallKey> = of_stream.map(|(key, _)| key).collect();
                    let oldest = queues.get(&id).and_then(|queue| queue.front().copied());
                    assert_eq!(running, Vec::from_iter(oldest), "seed {seed}: Stream {id}");
                }
                let streams: HashSet<&str> = in_flight.iter().map(|call| target(call).1).collect();
                together += usize::from(streams.len() > 1);
            }
        }
        assert!(together > 0);
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
        /// History has an entry at another activation.
        Activation,
        /// A read starts at another offset than History has.
        FromOffset,
    }

    /// Makes a random change to `history`, if it has an entry to change.
    fn mutate(rng: &mut StdRng, history: &mut [Task]) -> Option<Change> {
        let entries: Vec<(usize, usize, CallKey)> = (history.iter().enumerate())
            .flat_map(|(index, task)| task.markers.iter().enumerate().map(move |m| (index, m)))
            .flat_map(|(index, (at, marker))| {
                calls_of(marker)
                    .into_iter()
                    .map(move |call| (index, at, call))
            })
            .collect();
        if entries.is_empty() {
            return None;
        }
        let (task, at_marker, call) = entries[rng.random_range(0..entries.len())];
        let change = [
            Change::Stream,
            Change::Missing,
            Change::Twice,
            Change::Seq,
            Change::Activation,
            Change::FromOffset,
        ][rng.random_range(0..6)];
        if let Change::Stream | Change::Missing | Change::FromOffset = change {
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
                (Change::FromOffset, Command::Read(read)) => read.from_offset.push(0),
                (Change::FromOffset, _) => return None,
                (_, Command::Append(append)) => append.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Read(read)) => read.stream.as_mut().unwrap().id.push('!'),
                (_, Command::Terminal) => return None,
            }
            return Some(change);
        }
        match (&mut history[task].markers[at_marker], call) {
            (PeekedMarker::Append(data), CallKey::Append(seq)) => {
                let appends = &mut data.appends;
                let at = appends.iter().position(|append| append.seq == seq)?;
                match change {
                    Change::Twice => appends.push(appends[at].clone()),
                    Change::Seq => appends[at].seq = 999,
                    _ => appends[at].activation_index += 1,
                }
            }
            (PeekedMarker::Read(data), CallKey::Read(seq)) => {
                let reads = &mut data.reads;
                let at = reads.iter().position(|read| read.seq == seq)?;
                match change {
                    Change::Twice => reads.push(reads[at].clone()),
                    Change::Seq => reads[at].seq = 999,
                    _ => reads[at].activation_index += 1,
                }
            }
            _ => unreachable!("A marker has the calls of its kind"),
        }
        Some(change)
    }

    fn is_command_of(command: &Command, call: CallKey) -> bool {
        match (command, call) {
            (Command::Append(append), CallKey::Append(seq)) => append.seq == seq,
            (Command::Read(read), CallKey::Read(seq)) => read.seq == seq,
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
            match replay(seed, &history, &first.given, task_attempt(1000)) {
                Err(WFMachinesError::Nondeterminism(_)) => {}
                // An entry at another activation can match a call. Then Workflow code gets other
                // jobs, which is as wrong as an error.
                Ok(replayed) if change == Change::Activation => {
                    assert_ne!(replayed.jobs, first_jobs, "seed {seed}: {change:?}");
                }
                other => panic!("seed {seed}: {change:?}: {:?}", other.map(|r| r.jobs)),
            }
        }
        assert!(mutated > SEEDS / 2, "{mutated}");

        // A call with no Store name matches any Store of History.
        let mut replay = calls(Phase::ReplayOnly, 9);
        let mut nameless = append(1, "s");
        nameless.stream.as_mut().unwrap().store.clear();
        replay.append(nameless).unwrap();
        replay.peeked(marker(0, 1, "s"), vec![]).unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_with_no_runtime_fails_the_task() {
        let stores = ["a", "b", "c"];
        let mut with_no_runtime = 0;
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            let served: Vec<(&str, Arc<MemoryStore>)> = (stores.into_iter())
                .filter(|_| rng.random_bool(0.5))
                .map(|store| (store, Arc::new(MemoryStore::default())))
                .collect();
            let phase = [Phase::Live, Phase::Replaying, Phase::ReplayOnly][rng.random_range(0..3)];
            if served.is_empty() && phase != Phase::ReplayOnly {
                with_no_runtime += 1;
            }
            let runtimes = StreamRuntimes::new(served.iter().map(|(name, store)| {
                let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
                (name.to_string(), Arc::new(runtime))
            }));
            let mut calls = StreamCalls::new("n", "w", "r", runtimes.clone(), phase);
            calls.set_task(TaskState::Open(task_attempt(3)));
            let mut appends = 0;
            let (mut unserved, mut store_of) = (BTreeMap::new(), HashMap::new());
            for index in 0..rng.random_range(1..=6) {
                let store = stores[rng.random_range(0..3)];
                let stream = StreamRef {
                    id: format!("s{index}"),
                    store: store.to_string(),
                };
                store_of.insert(stream.id.clone(), store);
                appends += 1;
                let command = AppendToStream {
                    seq: appends,
                    stream: Some(stream),
                    bodies: vec![Payload::default()],
                };
                let result = calls.append(command);
                let error = WFMachinesError::Stream(format!(
                    "This Worker has no Stream runtime for the Store `{store}`"
                ));
                let is_served = served.iter().any(|(name, _)| *name == store);
                if phase == Phase::Live && !is_served {
                    // The command fails the task, so the run records nothing of the call.
                    assert_eq!(result, Err(error), "seed {seed}");
                    assert!(!calls.appends.contains_key(&appends), "seed {seed}");
                    break;
                }
                assert_eq!(result, Ok(()), "seed {seed}");
                if !is_served {
                    unserved.insert(appends, error);
                }
            }
            calls.marker_recorded().unwrap();
            let mut sent = calls.take_requests();
            let at_go_live = calls.go_live();
            sent.extend(calls.take_requests());
            // No call on a Store with no runtime reaches a Store.
            let streams: Vec<&str> = (sent.iter())
                .map(|request| match request {
                    Request::Run(call) => call.store.as_str(),
                    Request::Stop(_) => panic!("seed {seed}"),
                })
                .collect();
            assert!(
                (streams.iter()).all(|store| served.iter().any(|(name, _)| name == store)),
                "seed {seed}"
            );
            // A replay fails the task when it goes live with an unserved call, which a replay
            // Worker never does.
            let expected = match (phase, unserved.into_values().next()) {
                (Phase::Replaying, Some(error)) => Err(error),
                _ => Ok(()),
            };
            assert_eq!(at_go_live, expected, "seed {seed}");
            // Each Store gets only the calls whose ref has its name.
            let manager = StreamManager::new(runtimes, Handle::current(), |_, _| {});
            manager.dispatch("r", sent, |_| MetricsContext::no_op());
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            for (name, store) in &served {
                for request in store.requests() {
                    let StoreRequest::Append(append, _) = request else {
                        panic!("seed {seed}: {request:?}");
                    };
                    let id = append.stream_id;
                    assert_eq!(store_of[&id], *name, "seed {seed}: Stream {id}");
                }
            }
        }
        assert!(with_no_runtime > 0, "No seed had a Worker with no runtime");
    }
}
