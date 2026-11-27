//! The Stream calls of one run. Nothing here does I/O. The machines of the run drive the calls,
//! and the Worker's [`StreamManager`](super::StreamManager) runs the requests that they start.

use super::{
    MAX_STORE_NAME_BYTES, StreamRuntimes, TaskState,
    manager::{Call, Op, Outcome, Request, StreamResolution},
    marker::append_job,
};
use crate::worker::workflow::WFMachinesError;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};
use temporalio_common::protos::{
    coresdk::{
        external_data::{StreamAppendMarkerData, StreamAppendResult},
        workflow_activation::workflow_activation_job,
        workflow_commands::AppendToStream,
    },
    temporal::api::common::v1::Payload,
};
use temporalio_streams::{
    AnyError,
    attempt::attempt_key,
    ids::validate_stream_id,
    proto::{
        AppendRecord, Producer, WorkflowProducer, WorkflowTaskAttempt,
        producer::Kind,
        sdk::{AppendRecordsRequest, StreamRef},
    },
};

/// One counter for the whole process, so a late result of an evicted run never matches a call of
/// the run that replaces it.
static CALL_IDS: AtomicU64 = AtomicU64::new(0);

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
}

impl fmt::Display for CallKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallKey::Append(seq) => write!(f, "append {seq}"),
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

/// Marker entries from History whose calls Workflow code hasn't issued yet, by seq.
#[derive(Default)]
struct Held {
    appends: HashMap<u32, StreamAppendResult>,
}

impl Held {
    /// Returns the activation and the call of the first result, in the order of the first run.
    fn first_result(&self) -> Option<(u64, CallKey)> {
        (self.appends.iter())
            .map(|(seq, a)| (a.activation_index, CallKey::Append(*seq)))
            .min()
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
    /// The appends of each Stream, in the order that they were queued. Only the head runs.
    queues: HashMap<(String, String), VecDeque<CallKey>>,
    last_append: u32,
    /// The live calls whose result waits for an activation.
    done: BTreeSet<CallKey>,
    held: Held,
    /// Results from History whose calls Workflow code has issued, by the activation that gets them.
    ready: BTreeMap<(u64, CallKey), StreamAppendResult>,
    /// The last result of `ready` that a release gave to the next activation.
    released: Option<(u64, CallKey)>,
    /// The live results of the current activation, which its marker records.
    marker: StreamAppendMarkerData,
    /// True once the current task gave a live result, until the task completes.
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
            queues: HashMap::new(),
            last_append: 0,
            done: BTreeSet::new(),
            held: Held::default(),
            ready: BTreeMap::new(),
            released: None,
            marker: StreamAppendMarkerData::default(),
            marked: false,
            requests: vec![],
        }
    }

    /// When the task opens, the head of each queue starts.
    pub(crate) fn set_task(&mut self, state: TaskState) {
        if state == TaskState::NoTask {
            self.marked = false;
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
        let mut stream = valid_stream(CallKey::Append(seq), stream)?;
        self.resolve_store(&mut stream)?;
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

    /// Handles the result of a running call, and starts the next call of its Stream. It ignores the
    /// result of a call that was stopped.
    pub(crate) fn resolve(&mut self, resolution: StreamResolution) {
        let StreamResolution {
            call_id,
            seq,
            outcome,
        } = resolution;
        let (call, queue) = match outcome {
            Outcome::Append(result) => {
                let Some(append) = self.appends.get_mut(&seq) else {
                    return;
                };
                if !matches!(append.state, AppendState::Running(id) if id == call_id) {
                    return;
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
    }

    /// Returns true if activation `activation` of the task has a Stream result to give. A result
    /// from History is held back while the index of the next Local Activity marker, `marker`, is at
    /// or below the result's own index. So a Local Activity result of an earlier activation goes
    /// first, and one with the same index goes in the same activation.
    pub(crate) fn has_jobs(&self, activation: u64, marker: Option<u64>) -> bool {
        // A run that stopped its calls gives no result, even one that History has.
        self.phase != Phase::Stopped
            && ((self.ready.keys().next()).is_some_and(|key| self.is_due(key, activation, marker))
                || (self.task.is_open() && !self.done.is_empty()))
    }

    /// Returns the results for the activation at index `activation` of the task, as
    /// [`Self::has_jobs`] decides them, each with the index that its marker has. The marker of the
    /// activation records each live result with that index, and [`Self::take_marker`] returns it.
    pub(crate) fn take_jobs(
        &mut self,
        activation: u64,
        marker: Option<u64>,
    ) -> Vec<(u64, workflow_activation_job::Variant)> {
        let mut jobs = vec![];
        while let Some(key) = self.ready.keys().next().copied()
            && self.is_due(&key, activation, marker)
        {
            let entry = self.ready.remove(&key).expect("The key was just read");
            self.leave(key.1);
            jobs.push((key.0, append_job(&entry).into()));
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
            jobs.push((activation, job));
        }
        jobs
    }

    /// Holds the entries of a marker from History, each for its call.
    pub(crate) fn peeked(&mut self, data: StreamAppendMarkerData) -> Result<(), WFMachinesError> {
        for append in data.appends {
            let seq = append.seq;
            if self.held.appends.insert(seq, append).is_some() {
                return Err(second("result", CallKey::Append(seq)));
            }
            self.match_append(seq)?;
        }
        Ok(())
    }

    /// Handles the event of a marker. Each entry of the markers of its task must already have its
    /// call, and Workflow code must have its result.
    pub(crate) fn marker_recorded(&mut self) -> Result<(), WFMachinesError> {
        if let Some(seq) = self.held.appends.keys().min() {
            return Err(WFMachinesError::Nondeterminism(format!(
                "History has a result of Stream {} that Workflow code never issued",
                CallKey::Append(*seq)
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
    /// each queue starts.
    pub(crate) fn go_live(&mut self) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Replaying {
            return Ok(());
        }
        let unserved = (self.appends.iter())
            .filter(|(_, append)| self.runtimes.get(&append.stream.store).is_none())
            .min_by_key(|(seq, _)| **seq);
        if let Some((_, append)) = unserved {
            return Err(no_runtime(&append.stream));
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
        let running = (self.appends.values())
            .filter_map(|append| match append.state {
                AppendState::Running(call_id) => Some(call_id),
                _ => None,
            })
            .filter(|call_id| !unsent.contains(call_id));
        self.requests.extend(running.map(Request::Stop));
        self.phase = Phase::Stopped;
        self.appends.clear();
        self.queues.clear();
        self.done.clear();
    }

    /// Returns true while a call has no result in a marker, so the run needs another task.
    pub(crate) fn needs_next_task(&self) -> bool {
        !self.appends.is_empty()
    }

    /// Returns true if the current task gave a live result, so it has a marker that History doesn't
    /// have yet.
    pub(crate) fn has_marker(&self) -> bool {
        self.marked
    }

    /// Returns the number of live calls that hold the task open, as a Local Activity does.
    pub(crate) fn outstanding(&self) -> usize {
        if self.phase != Phase::Live {
            return 0;
        }
        (self.appends.values())
            .filter(|append| {
                matches!(
                    append.state,
                    AppendState::Waiting { .. } | AppendState::Running(_)
                )
            })
            .count()
    }

    /// Returns the first result from History whose call Workflow code issued.
    pub(crate) fn next_ready(&self) -> Option<&StreamAppendResult> {
        self.ready.values().next()
    }

    /// Returns the activation of the first result from History whose call Workflow code issued.
    pub(crate) fn next_ready_activation(&self) -> Option<u64> {
        self.ready.keys().next().map(|(at, _)| *at)
    }

    /// Returns the activation of the first result from History whose call Workflow code hasn't
    /// issued.
    pub(crate) fn first_held_activation(&self) -> Option<u64> {
        self.held.first_result().map(|(at, _)| at)
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

    /// Returns the marker of the live results that the last [`Self::take_jobs`] gave, if it gave
    /// any.
    pub(crate) fn take_marker(&mut self) -> Option<StreamAppendMarkerData> {
        (!self.marker.appends.is_empty()).then(|| std::mem::take(&mut self.marker))
    }

    /// Returns a marker with no entry if the task completes while a call has no result in a marker
    /// and the task recorded none. A replay with chunking on would otherwise take the task as a
    /// heartbeat and join it to the next one, whose activation indexes would then not match.
    pub(crate) fn take_completion_marker(&mut self) -> Option<StreamAppendMarkerData> {
        if self.phase != Phase::Live || self.marked || !self.needs_next_task() {
            return None;
        }
        self.marked = true;
        Some(StreamAppendMarkerData::default())
    }

    /// Returns the calls that the Worker must start or stop.
    pub(crate) fn take_requests(&mut self) -> Vec<Request> {
        std::mem::take(&mut self.requests)
    }

    /// Drops every call, the marker of the activation and the entries of History, and leaves the
    /// run with no task, as a failed task does. The run records no call after this.
    pub(crate) fn stop(&mut self) {
        self.stop_for_terminal();
        self.task = TaskState::NoTask;
        self.held = Held::default();
        self.ready.clear();
        self.released = None;
        self.marker = StreamAppendMarkerData::default();
        self.marked = false;
        self.requests.clear();
    }

    /// Gives a ref with no Store name the Store of the Worker's runtime under the empty name, else
    /// of its only runtime. A replay does the same, so it compares History with the Store that a
    /// live call uses.
    fn resolve_store(&self, stream: &mut StreamRef) -> Result<(), WFMachinesError> {
        if !stream.store.is_empty() {
            return Ok(());
        }
        let Some(store) = self.runtimes.default_store() else {
            return Err(WFMachinesError::Stream(format!(
                "A Stream call with no Store name needs a Stream runtime under the empty name or \
                 exactly one Stream runtime, and this Worker has {}",
                self.runtimes.count()
            )));
        };
        stream.store = store.to_string();
        Ok(())
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
        self.ready.insert(at, entry);
        Ok(())
    }

    /// Records a live result in the marker of the activation at `activation`, and returns its
    /// job.
    fn record(
        &mut self,
        call: CallKey,
        activation: u64,
    ) -> Option<workflow_activation_job::Variant> {
        let CallKey::Append(seq) = call;
        let AppendState::Done(result) = &self.appends[&seq].state else {
            return None;
        };
        let entry = StreamAppendResult {
            activation_index: activation,
            ..result.clone()
        };
        let job = append_job(&entry).into();
        self.marker.appends.push(entry);
        self.marked = true;
        Some(job)
    }

    /// Starts the call at the head of each queue, once the run is live and its task is open. It
    /// runs only when the task opens and at go-live, because any other change starts the head of
    /// its own queue.
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
                let AppendState::Waiting { bodies, .. } =
                    std::mem::replace(&mut append.state, running)
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

fn no_runtime(stream: &StreamRef) -> WFMachinesError {
    WFMachinesError::Stream(format!(
        "This Worker has no Stream runtime for the Store `{}`",
        stream.store
    ))
}

/// Checks that a call is for the Stream that History has, in the Store that History has.
fn check_stream(
    call: CallKey,
    sent: &StreamRef,
    id: &str,
    store: &str,
) -> Result<(), WFMachinesError> {
    if sent.id == id && sent.store == store {
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
    use super::super::{StreamManager, manager::recorded};
    use super::*;
    use crate::test_help::stream_store::{MemoryStore, Request as StoreRequest};
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::Arc;
    use temporalio_common::protos::coresdk::{
        external_data::{StreamAppendOffsets, StreamFailure, stream_append_result},
        workflow_activation::{ResolveStreamAppend, resolve_stream_append},
    };
    use temporalio_streams::{AnyError, StreamRuntime, record::StoreLimits};
    use tokio::{
        runtime::Handle,
        sync::mpsc,
        time::{Duration, timeout},
    };
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

    fn appended(seq: u32) -> stream_append_result::Result {
        stream_append_result::Result::Appended(StreamAppendOffsets {
            first_offset: vec![seq as u8],
            last_offset: vec![seq as u8],
        })
    }

    /// Returns the marker of an activation at `activation` that has the result of append `seq` to
    /// the Stream `id`.
    fn marker(activation: u64, seq: u32, id: &str) -> StreamAppendMarkerData {
        let entry = StreamAppendResult {
            seq,
            stream_id: id.to_string(),
            store: "mem".to_string(),
            activation_index: activation,
            result: Some(appended(seq)),
        };
        StreamAppendMarkerData {
            appends: vec![entry],
        }
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

    /// Returns the calls that started since the last look, as `append 1 of s at 3`, with the
    /// started event id of their attempt.
    fn started(calls: &mut StreamCalls) -> Vec<String> {
        let mut started: Vec<String> = (calls.take_requests().into_iter())
            .map(|request| {
                let (call, stream, producer) = match request {
                    Request::Run(Call {
                        seq,
                        op: Op::Append(r, _),
                        ..
                    }) => (CallKey::Append(seq), r.r#ref, r.producer),
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

    /// Gives a running call its result, a failure with `message` if there is one.
    fn finish(calls: &mut StreamCalls, call: CallKey, message: Option<&str>) {
        let failed = message.map(refused);
        let CallKey::Append(seq) = call;
        let AppendState::Running(call_id) = calls.appends[&seq].state else {
            panic!("{call} doesn't run");
        };
        let result = match failed {
            Some(failure) => stream_append_result::Result::Failure(failure),
            None => appended(seq),
        };
        calls.resolve(StreamResolution {
            call_id,
            seq,
            outcome: Outcome::Append(result),
        });
    }

    /// Returns the results of the activation at `activation`, as `append 1` or `append 2: <text>`.
    fn jobs(calls: &mut StreamCalls, activation: u64) -> Vec<String> {
        (calls.take_jobs(activation, None).into_iter())
            .map(|(_, job)| match job {
                Variant::ResolveStreamAppend(ResolveStreamAppend {
                    seq,
                    status: Some(resolve_stream_append::Status::Failed(failure)),
                }) => format!("append {seq}: {}", failure.message),
                Variant::ResolveStreamAppend(append) => format!("append {}", append.seq),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    /// Returns the key of a call that runs, and the id of its Stream.
    fn target(call: &Call) -> (CallKey, &str) {
        let Op::Append(request, _) = &call.op;
        (
            CallKey::Append(call.seq),
            &request.r#ref.as_ref().unwrap().id,
        )
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

    /// Returns the Store's answer to `call`, which is `failure` if there is one.
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
        finish(&mut calls, CallKey::Append(1), None);
        assert_eq!(started(&mut calls), ["append 2 of s at 3"]);
        finish(&mut calls, CallKey::Append(2), None);
        assert_eq!(started(&mut calls), ["append 3 of s at 7"]);
    }

    #[test]
    fn a_result_waits_with_its_call_until_an_activation_takes_it() {
        let mut live = calls(Phase::Live, 3);
        live.append(append(1, "s")).unwrap();
        live.append(append(2, "t")).unwrap();
        started(&mut live);
        finish(&mut live, CallKey::Append(2), None);
        finish(&mut live, CallKey::Append(1), None);
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
        finish(&mut live, CallKey::Append(3), None);
        assert_eq!(jobs(&mut live, 2), ["append 3"]);
        assert_eq!(entries(live.take_marker()), [(3, 2)]);
    }

    #[test]
    fn a_release_ends_with_its_activation() {
        let mut replay = calls(Phase::Replaying, 9);
        replay.append(append(1, "1")).unwrap();
        replay.peeked(marker(2, 1, "1")).unwrap();
        replay.release_ready();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
        // A result of the next task waits for its own activation.
        replay.append(append(2, "2")).unwrap();
        replay.peeked(marker(1, 2, "2")).unwrap();
        assert!(!replay.has_jobs(0, None));
    }

    /// A command of Workflow code in a generated script.
    #[derive(Clone, Debug)]
    enum Command {
        Append(AppendToStream),
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
        markers: Vec<StreamAppendMarkerData>,
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
        /// marker of the activation.
        fn activate(
            &mut self,
            commands: &[Command],
        ) -> Result<(Vec<Variant>, Option<StreamAppendMarkerData>), WFMachinesError> {
            let jobs = (self.calls.take_jobs(self.activation, None).into_iter())
                .map(|(_, job)| job)
                .collect();
            let marker = self.calls.take_marker();
            self.activation += 1;
            for command in commands {
                match command {
                    Command::Append(append) => self.calls.append(append.clone())?,
                    Command::Terminal => {
                        self.calls.stop_for_terminal();
                        self.ended = true;
                    }
                }
            }
            Ok((jobs, marker))
        }

        /// Returns the appends that the run still tracks.
        fn tracked(&self) -> BTreeSet<CallKey> {
            (self.calls.appends.keys().map(|seq| CallKey::Append(*seq))).collect()
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
                run.calls.peeked(marker.clone())?;
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
                let (jobs, marker) = run.activate(&task.commands_at(at))?;
                assert_eq!(marker, None, "seed {seed}: a replay records a result");
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
        /// The calls whose result no marker has.
        unresolved: BTreeSet<CallKey>,
        /// The state as of the last task in History, which a failed task goes back to.
        committed: (u32, BTreeSet<CallKey>),
    }

    impl<'a> FirstRun<'a> {
        fn new(seed: u64, seen: &'a mut Seen) -> Self {
            let run = Run {
                calls: StreamCalls::new("n", "w", "r", runtimes(&["mem"]), Phase::Live),
                activation: 0,
                ended: false,
            };
            Self {
                seed,
                rng: StdRng::seed_from_u64(seed),
                seen,
                run,
                history: vec![],
                task: Task::default(),
                task_attempt: task_attempt(3),
                in_flight: vec![],
                deferred: vec![],
                last_append: 0,
                unresolved: BTreeSet::new(),
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
            if (self.history.last()).is_some_and(|task| !task.markers.is_empty()) {
                self.run.calls.marker_recorded().unwrap();
            }
            self.run.activation = 0;
            for resolution in std::mem::take(&mut self.deferred) {
                self.run.calls.resolve(resolution);
            }
            self.dispatch();
        }

        /// Runs the current task until it completes, and returns an error if it fails.
        fn run_task(&mut self) -> Result<(), ()> {
            self.task.has_events = self.history.is_empty() || self.rng.random_bool(0.6);
            if self.task.has_events || self.run.calls.has_jobs(self.run.activation, None) {
                self.activate()?;
            }
            loop {
                if self.run.ended {
                    self.complete();
                    return Ok(());
                }
                if self.run.calls.has_jobs(self.run.activation, None) {
                    self.activate()?;
                    continue;
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
                    // Core gives an activation for the first answer that arrives.
                    0..=35 => {
                        let resolution = self.answer();
                        self.run.calls.resolve(resolution);
                        self.dispatch();
                    }
                    36..=38 => {
                        // The heartbeat completes the task. Results that arrive before the
                        // completion wait for the next task.
                        self.seen.heartbeats += 1;
                        self.run
                            .calls
                            .set_task(TaskState::Completing(self.task_attempt));
                        for _ in 0..self.rng.random_range(0..=2) {
                            if !self.in_flight.is_empty() {
                                let resolution = self.answer();
                                self.run.calls.resolve(resolution);
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
            let (jobs, marker) =
                result.unwrap_or_else(|error| panic!("seed {}: {error:?}", self.seed));
            self.task.markers.extend(marker);
            for resolution in late {
                self.run.calls.resolve(resolution);
            }
            for command in &commands {
                match command {
                    Command::Append(append) => {
                        self.unresolved.insert(CallKey::Append(append.seq));
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
                    0..=69 => {
                        self.last_append += 1;
                        commands.push(Command::Append(append(self.last_append, &id)));
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
                                self.last_append += 1;
                                let seq = self.last_append;
                                let error = format!(
                                    "Stream append {seq} is invalid: The Stream id is empty"
                                );
                                (Command::Append(append(seq, "")), error)
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
            let failure = self.rng.random_bool(0.2).then(|| refused("refused"));
            answer(&call, failure)
        }

        fn dispatch(&mut self) {
            dispatch(&mut self.run.calls, &mut self.in_flight);
        }

        /// Completes the current task, and checks that the run tracks exactly the calls whose
        /// result no marker has.
        fn complete(&mut self) {
            let open = self.run.calls.take_completion_marker();
            self.task.markers.extend(open);
            self.run.calls.set_task(TaskState::NoTask);
            for marker in &self.task.markers {
                for append in &marker.appends {
                    self.unresolved.remove(&CallKey::Append(append.seq));
                }
            }
            let mut task = std::mem::take(&mut self.task);
            task.ends_workflow = self.run.ended;
            self.history.push(task);
            self.committed = (self.last_append, self.unresolved.clone());
            let seed = self.seed;
            assert_eq!(self.run.tracked(), self.unresolved, "seed {seed}");
            let in_flight = &self.in_flight;
            assert!(
                !self.run.ended || in_flight.is_empty(),
                "seed {seed}: {in_flight:?}"
            );
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
            (self.last_append, self.unresolved) = self.committed.clone();
            self.task = Task::default();
            self.task_attempt.attempt += 1;
            self.run = self.check_replay(self.task_attempt);
            self.dispatch();
        }

        /// Replays History, checks it against the first run, and returns the replayed run.
        fn check_replay(&self, live: WorkflowTaskAttempt) -> Run {
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
                .flat_map(|task| task.markers.iter())
                .flat_map(|marker| marker.appends.iter())
                .map(|append| CallKey::Append(append.seq))
                .collect();
            for request in run.calls.take_requests() {
                let Request::Run(Call {
                    seq,
                    op: Op::Append(append, _),
                    ..
                }) = &request
                else {
                    panic!("seed {seed}: {request:?}");
                };
                let (key, sent) = (CallKey::Append(*seq), &append.producer);
                assert!(!recorded.contains(&key), "seed {seed}: {key} ran again");
                let expected = producer(&run.calls.producer, live);
                assert_eq!(*sent, Some(expected), "seed {seed}");
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
        } = seen;
        let counts = [heartbeats, failed_tasks, dropped, invalid, bad_seqs];
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
                    calls.resolve(answer(&call, failure));
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
    }

    /// Makes a random change to `history`, if it has an entry to change.
    fn mutate(rng: &mut StdRng, history: &mut [Task]) -> Option<Change> {
        let entries: Vec<(usize, usize, CallKey)> = (history.iter().enumerate())
            .flat_map(|(index, task)| task.markers.iter().enumerate().map(move |m| (index, m)))
            .flat_map(|(index, (at, marker))| {
                (marker.appends.iter()).map(move |append| (index, at, CallKey::Append(append.seq)))
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
        ][rng.random_range(0..5)];
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
                (_, Command::Terminal) => return None,
            }
            return Some(change);
        }
        let appends = &mut history[task].markers[at_marker].appends;
        let CallKey::Append(seq) = call;
        let at = appends.iter().position(|append| append.seq == seq)?;
        match change {
            Change::Twice => appends.push(appends[at].clone()),
            Change::Seq => appends[at].seq = 999,
            Change::Activation => appends[at].activation_index += 1,
            Change::Stream | Change::Missing => unreachable!("Handled above"),
        }
        Some(change)
    }

    fn is_command_of(command: &Command, call: CallKey) -> bool {
        match (command, call) {
            (Command::Append(append), CallKey::Append(seq)) => append.seq == seq,
            (Command::Terminal, _) => false,
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
            match replay(seed, &history, task_attempt(1000)) {
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
    }

    #[test]
    fn a_call_with_no_store_name_uses_the_only_runtime() {
        let nameless = |seq| {
            let mut command = append(seq, "s");
            command.stream.as_mut().unwrap().store.clear();
            command
        };
        for phase in [Phase::Live, Phase::Replaying, Phase::ReplayOnly] {
            for stores in [&[][..], &["mem", "other"][..]] {
                let mut calls = StreamCalls::new("n", "w", "r", runtimes(stores), phase);
                calls.set_task(TaskState::Open(task_attempt(3)));
                let error = format!(
                    "A Stream call with no Store name needs a Stream runtime under the empty \
                     name or exactly one Stream runtime, and this Worker has {}",
                    stores.len()
                );
                let result = calls.append(nameless(1));
                assert_eq!(result, Err(WFMachinesError::Stream(error)), "{phase:?}");
                assert!(calls.appends.is_empty(), "{phase:?}");
            }
        }
        let mut live = calls(Phase::Live, 3);
        live.append(nameless(1)).unwrap();
        let stores: Vec<String> = (live.take_requests().into_iter())
            .map(|request| match request {
                Request::Run(call) => call.store,
                Request::Stop(_) => panic!("Nothing stops"),
            })
            .collect();
        assert_eq!(stores, ["mem"]);
        finish(&mut live, CallKey::Append(1), None);
        assert_eq!(jobs(&mut live, 0), ["append 1"]);
        assert_eq!(live.take_marker().unwrap().appends[0].store, "mem");

        // A replayed call goes live on the only Store, in the queue of the named calls.
        let mut replaying = calls(Phase::Replaying, 3);
        replaying.append(nameless(1)).unwrap();
        replaying.append(append(2, "s")).unwrap();
        replaying.go_live().unwrap();
        let running: Vec<(u32, String)> = (replaying.take_requests().into_iter())
            .map(|request| match request {
                Request::Run(call) => (call.seq, call.store),
                Request::Stop(_) => panic!("Nothing stops"),
            })
            .collect();
        assert_eq!(running, [(1, "mem".to_string())]);

        let mut replay = calls(Phase::ReplayOnly, 9);
        replay.append(nameless(1)).unwrap();
        replay.peeked(marker(0, 1, "s")).unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
        let mut other = StreamCalls::new("n", "w", "r", runtimes(&["other"]), Phase::ReplayOnly);
        other.append(nameless(1)).unwrap();
        let error = "Stream append 1 is for Stream `s` in Store `other`, and History has it for \
                     Stream `s` in Store `mem`";
        assert_eq!(
            other.peeked(marker(0, 1, "s")),
            Err(WFMachinesError::Nondeterminism(error.to_string()))
        );
    }

    #[test]
    fn a_call_with_no_store_name_uses_the_runtime_under_the_empty_name() {
        let nameless = |seq| {
            let mut command = append(seq, "s");
            command.stream.as_mut().unwrap().store.clear();
            command
        };
        let with_default = |phase| {
            let mut calls = StreamCalls::new("n", "w", "r", runtimes(&["", "mem"]), phase);
            calls.set_task(TaskState::Open(task_attempt(3)));
            calls
        };
        let running = |calls: &mut StreamCalls| -> Vec<(u32, String)> {
            (calls.take_requests().into_iter())
                .map(|request| match request {
                    Request::Run(call) => (call.seq, call.store),
                    Request::Stop(_) => panic!("Nothing stops"),
                })
                .collect()
        };
        let mut live = with_default(Phase::Live);
        live.append(nameless(1)).unwrap();
        live.append(append(2, "s")).unwrap();
        assert_eq!(
            running(&mut live),
            [(1, String::new()), (2, "mem".to_string())]
        );
        finish(&mut live, CallKey::Append(1), None);
        assert_eq!(jobs(&mut live, 0), ["append 1"]);
        assert_eq!(live.take_marker().unwrap().appends[0].store, "");

        let mut replaying = with_default(Phase::Replaying);
        replaying.append(nameless(1)).unwrap();
        replaying.go_live().unwrap();
        assert_eq!(running(&mut replaying), [(1, String::new())]);

        let mut replay = with_default(Phase::ReplayOnly);
        replay.append(nameless(1)).unwrap();
        let mut entry = marker(0, 1, "s");
        entry.appends[0].store.clear();
        replay.peeked(entry).unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
        let mut recorded_in_mem = with_default(Phase::ReplayOnly);
        recorded_in_mem.append(nameless(1)).unwrap();
        let error = "Stream append 1 is for Stream `s` in Store ``, and History has it for Stream \
                     `s` in Store `mem`";
        assert_eq!(
            recorded_in_mem.peeked(marker(0, 1, "s")),
            Err(WFMachinesError::Nondeterminism(error.to_string()))
        );
    }

    #[test]
    fn a_named_append_needs_its_runtime_when_it_goes_live() {
        let without_runtimes = |phase| {
            let mut calls = StreamCalls::new("n", "w", "r", runtimes(&[]), phase);
            calls.set_task(TaskState::Open(task_attempt(3)));
            calls
        };
        let missing = "This Worker has no Stream runtime for the Store `mem`";
        let mut live = without_runtimes(Phase::Live);
        assert_eq!(
            live.append(append(1, "s")),
            Err(WFMachinesError::Stream(missing.into()))
        );
        assert!(live.appends.is_empty());
        assert!(live.take_requests().is_empty());

        let mut replaying = without_runtimes(Phase::Replaying);
        replaying.append(append(1, "s")).unwrap();
        let mut second = append(2, "t");
        second.stream.as_mut().unwrap().store = "other".into();
        replaying.append(second).unwrap();
        assert_eq!(
            replaying.go_live(),
            Err(WFMachinesError::Stream(missing.into()))
        );
        assert!(replaying.take_requests().is_empty());

        let mut replay = without_runtimes(Phase::ReplayOnly);
        replay.append(append(1, "s")).unwrap();
        replay.peeked(marker(0, 1, "s")).unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
        replay.marker_recorded().unwrap();
        replay.append(append(2, "t")).unwrap();
        replay.go_live().unwrap();
        assert!(replay.take_requests().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_named_append_reaches_its_store() {
        let stores = [
            ("mem", Arc::new(MemoryStore::default())),
            ("other", Arc::new(MemoryStore::default())),
        ];
        let runtimes = StreamRuntimes::new(stores.iter().map(|(name, store)| {
            let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
            (name.to_string(), Arc::new(runtime))
        }));
        let mut calls = StreamCalls::new("n", "w", "r", runtimes.clone(), Phase::Live);
        calls.set_task(TaskState::Open(task_attempt(3)));
        calls.append(append(1, "first")).unwrap();
        let mut second = append(2, "second");
        second.stream.as_mut().unwrap().store = "other".into();
        calls.append(second).unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let manager = StreamManager::new(runtimes, Handle::current(), move |_, _| {
            tx.send(()).unwrap();
        });
        manager.dispatch("r", calls.take_requests());
        for _ in 0..2 {
            timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        for ((_, store), stream) in stores.iter().zip(["first", "second"]) {
            let requests = store.requests();
            let [StoreRequest::Append(request, _)] = &requests[..] else {
                panic!("expected one append for {stream}, got {requests:?}");
            };
            assert_eq!(request.stream_id, stream);
        }
    }
}
