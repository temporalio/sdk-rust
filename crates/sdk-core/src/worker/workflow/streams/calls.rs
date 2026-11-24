//! The Stream calls of one run. Nothing here does I/O. The machines of the run drive the calls,
//! and the Worker's [`StreamManager`](super::StreamManager) runs the requests that they start.

use super::{
    StreamRuntimes, TaskState,
    ledger::{LedgerBuilder, Waiting, activation_of, append_job, close_job, seq_of},
    manager::{Call, Op, Outcome, Request, StreamResolution},
};
use crate::worker::workflow::WFMachinesError;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::workflow_activation_job,
        workflow_commands::{AppendToStream, CloseStream},
    },
    temporal::api::{
        common::v1::Payload,
        sdk::v1::{
            WorkflowTaskStreamCalls,
            workflow_task_stream_calls::{AppendResult, CloseIntent, CloseResult},
        },
    },
};
use temporalio_streams::{
    AnyError,
    attempt::attempt_key,
    ids::validate_stream_id,
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, Producer, StreamRef,
        WorkflowProducer, WorkflowTaskAttempt, producer::Kind,
    },
};

/// The maximum length of a Store name in a Workflow call, in bytes. A ledger records the Store of
/// each call, so Core fails the Workflow Task of a call with a longer name.
const MAX_STORE_NAME_BYTES: usize = 4096;

/// The counter of the ids of running calls, for every run of the process. A late result of a call
/// of an evicted run then never matches a call of the run that replaces it.
static CALL_IDS: AtomicU64 = AtomicU64::new(0);

/// How far a run is through its History.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The run replays History, so each call waits for its result in a ledger.
    Replaying,
    /// The replay is over, so calls run.
    Live,
    /// The run is on a replay Worker, which makes no live calls.
    ReplayOnly,
    /// The run stopped its calls, at a failed task or at the command that ends the Workflow, so it
    /// records no new call.
    Stopped,
}

/// A call by its kind and seq. Core gives the results of one activation in this order.
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

enum AppendState {
    /// The append is in its queue. A replayed one gets its attempt at go-live.
    Waiting {
        bodies: Vec<Payload>,
        attempt: Option<WorkflowTaskAttempt>,
    },
    Running(u64),
    /// The result waits for an activation, in the form that the ledger records it.
    Done(Waiting<AppendResult>),
    /// History has the result, which waits for its activation.
    Replayed,
}

struct Close {
    stream: StreamRef,
    state: CloseState,
}

enum CloseState {
    /// No ledger has the intent yet.
    Asked,
    /// The ledger of the task has the intent. A replayed close gets its attempt at go-live.
    Sealed(Option<WorkflowTaskAttempt>),
    /// History has the intent, so the close is in its queue.
    Waiting(Option<WorkflowTaskAttempt>),
    Running(u64),
    /// The result waits for an activation, in the form that the ledger records it.
    Done(Waiting<CloseResult>),
    /// History has the result, which waits for its activation.
    Replayed,
}

/// The entries of History's ledgers whose calls Workflow code hasn't issued yet, by seq.
#[derive(Default)]
struct Held {
    appends: HashMap<u32, AppendResult>,
    intents: HashMap<u32, CloseIntent>,
    closes: HashMap<u32, CloseResult>,
}

impl Held {
    /// Returns the activation and the call of the first result, in the order of the first run.
    fn first_result(&self) -> Option<(u64, CallKey)> {
        let appends = (self.appends.iter())
            .map(|(seq, a)| (activation_of(a.activation_index), CallKey::Append(*seq)));
        let closes = (self.closes.iter())
            .map(|(seq, c)| (activation_of(c.activation_index), CallKey::Close(*seq)));
        appends.chain(closes).min()
    }
}

/// The Stream calls of one run. A call is removed from the run once its result is in a ledger.
pub(crate) struct StreamCalls {
    /// The Producer of the run. A write adds the attempt of its Workflow Task.
    producer: WorkflowProducer,
    runtimes: StreamRuntimes,
    phase: Phase,
    /// True if the server keeps the `sdk_metadata` of a completion, which has the ledger of the
    /// task.
    keeps_ledgers: bool,
    /// The state of the run's current Workflow Task.
    task: TaskState,
    appends: HashMap<u32, Append>,
    closes: HashMap<u32, Close>,
    /// The appends and closes of each Stream, in the order that they were queued. Only the head
    /// runs.
    queues: HashMap<(String, String), VecDeque<CallKey>>,
    last_append: u32,
    last_close: u32,
    /// The live calls whose result waits for an activation.
    done: BTreeSet<CallKey>,
    held: Held,
    /// History's results whose calls Workflow code issued, by the activation that gets them.
    ready: BTreeMap<(u64, CallKey), workflow_activation_job::Variant>,
    /// The last result of `ready` that a release gave to the next activation.
    released: Option<(u64, CallKey)>,
    ledger: LedgerBuilder,
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
        keeps_ledgers: bool,
    ) -> Self {
        debug_assert_ne!(phase, Phase::Stopped);
        let ledger = LedgerBuilder::new(runtimes.settings);
        Self {
            producer: WorkflowProducer {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                attempt: None,
            },
            runtimes,
            phase,
            keeps_ledgers,
            task: TaskState::NoTask,
            appends: HashMap::new(),
            closes: HashMap::new(),
            queues: HashMap::new(),
            last_append: 0,
            last_close: 0,
            done: BTreeSet::new(),
            held: Held::default(),
            ready: BTreeMap::new(),
            released: None,
            ledger,
            requests: vec![],
        }
    }

    /// Sets the state of the run's current task. When the task opens, the head of each queue
    /// starts.
    pub(crate) fn set_task(&mut self, state: TaskState) {
        self.task = state;
        self.start_heads();
    }

    /// Handles an append command.
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

    /// Handles a close command. The ledger of the task records its intent, and the close runs once
    /// History has that intent.
    pub(crate) fn close(&mut self, command: CloseStream) -> Result<(), WFMachinesError> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        let CloseStream { seq, stream } = command;
        next_seq("close", seq, &mut self.last_close)?;
        let stream = valid_stream(CallKey::Close(seq), stream)?;
        self.check_runtime(&stream)?;
        let state = CloseState::Asked;
        self.closes.insert(seq, Close { stream, state });
        self.match_intent(seq)
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
                append.state = AppendState::Done(Waiting::new(AppendResult {
                    seq: seq as i32,
                    r#ref: Some(append.stream.clone()),
                    result: Some(result),
                    activation_index: 0,
                }));
                (CallKey::Append(seq), key(&append.stream))
            }
            Outcome::Close(failure) => {
                let Some(close) = self.closes.get_mut(&seq) else {
                    return;
                };
                if !matches!(close.state, CloseState::Running(id) if id == call_id) {
                    return;
                }
                close.state = CloseState::Done(Waiting::new(CloseResult {
                    seq: seq as i32,
                    failure,
                    activation_index: 0,
                }));
                (CallKey::Close(seq), key(&close.stream))
            }
        };
        self.done.insert(call);
        self.leave_queue(&queue, call);
    }

    /// Returns true if the activation at index `activation` of the task gets a result. A result
    /// from History waits for the Local Activity result of the next marker, at index `marker`, if
    /// the first run gave that one before it or with it.
    pub(crate) fn has_jobs(&self, activation: u64, marker: Option<u64>) -> bool {
        // A run that stopped its calls gives no result, even one that History has.
        self.phase != Phase::Stopped
            && ((self.ready.keys().next()).is_some_and(|key| self.is_due(key, activation, marker))
                || self.next_result_fits(activation) == Some(true))
    }

    /// Returns the results for the activation at index `activation` of the task, as
    /// [`Self::has_jobs`] decides them. The ledger records each live result with that index.
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

    /// Holds the entries of a ledger from History, each for its call.
    pub(crate) fn peeked(&mut self, data: WorkflowTaskStreamCalls) -> Result<(), WFMachinesError> {
        let WorkflowTaskStreamCalls {
            close_intents,
            append_results,
            close_results,
            ..
        } = data;
        for intent in close_intents {
            let seq = seq_of(intent.seq)?;
            if self.held.intents.insert(seq, intent).is_some() {
                return Err(second("intent", CallKey::Close(seq)));
            }
            self.match_intent(seq)?;
        }
        for append in append_results {
            let seq = seq_of(append.seq)?;
            if self.held.appends.insert(seq, append).is_some() {
                return Err(second("result", CallKey::Append(seq)));
            }
            self.match_append(seq)?;
        }
        for close in close_results {
            let seq = seq_of(close.seq)?;
            if self.held.closes.insert(seq, close).is_some() {
                return Err(second("result", CallKey::Close(seq)));
            }
            self.match_result(seq)?;
        }
        Ok(())
    }

    /// Handles the completion in History that has a ledger. Each entry of the ledger must already
    /// have its call, and each close whose intent the ledger has joins its queue.
    pub(crate) fn ledger_recorded(&mut self) -> Result<(), WFMachinesError> {
        let never = |what: &str, call: CallKey| {
            WFMachinesError::Nondeterminism(format!(
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
            return Err(WFMachinesError::Nondeterminism(format!(
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
            if let CloseState::Sealed(attempt) = close.state {
                close.state = CloseState::Waiting(attempt);
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

    /// Ends the replay. Each waiting call gets the attempt of the current task, and the head of
    /// each queue starts.
    pub(crate) fn go_live(&mut self) -> Result<(), WFMachinesError> {
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
        if !self.keeps_ledgers && self.needs_next_task() {
            return Err(no_ledger());
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
        for close in self.closes.values_mut() {
            if let CloseState::Sealed(waiting) | CloseState::Waiting(waiting) = &mut close.state {
                *waiting = attempt;
            }
        }
        self.start_heads();
        Ok(())
    }

    /// Drops each call at a command that ends the Workflow, as Core drops a Local Activity there.
    /// The ledger keeps the results that Workflow code already got in the task.
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
        let closes = (self.closes.values()).filter_map(|close| match close.state {
            CloseState::Running(call_id) => Some(call_id),
            _ => None,
        });
        let running = appends
            .chain(closes)
            .filter(|call_id| !unsent.contains(call_id));
        self.requests.extend(running.map(Request::Stop));
        self.phase = Phase::Stopped;
        self.appends.clear();
        self.closes.clear();
        self.queues.clear();
        self.done.clear();
    }

    /// Returns true while a call has no result in a ledger, so the run needs another task.
    pub(crate) fn needs_next_task(&self) -> bool {
        !self.appends.is_empty() || !self.closes.is_empty()
    }

    /// Returns true if the ledger of the task has an entry.
    pub(crate) fn has_ledger(&self) -> bool {
        !self.ledger.is_empty()
    }

    /// Returns true once the ledger of the task has no room for a result that waits, so the task
    /// must complete with a new one forced.
    pub(crate) fn must_complete(&self, activation: u64) -> bool {
        self.next_result_fits(activation) == Some(false)
    }

    /// Returns the number of live calls that hold the task open, as a Local Activity does.
    pub(crate) fn outstanding(&self) -> usize {
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
    pub(crate) fn next_ready_activation(&self) -> Option<u64> {
        self.ready.keys().next().map(|(at, _)| *at)
    }

    /// Returns the activation of the first result from History that Workflow code doesn't have.
    pub(crate) fn first_result_activation(&self) -> Option<u64> {
        let held = self.held.first_result().map(|(at, _)| at);
        self.next_ready_activation().into_iter().chain(held).min()
    }

    /// Releases the results from History for the next activation, whatever their index, in the
    /// order of the first run, up to the first one whose call Workflow code hasn't issued. A replay
    /// with chunking on and no job to give does the same for Local Activity results.
    pub(crate) fn release_ready(&mut self) {
        let held = self.held.first_result();
        let comes = |key: &&(u64, CallKey)| held.is_none_or(|held| **key < held);
        self.released = self.ready.keys().take_while(comes).last().copied();
    }

    fn is_due(&self, key: &(u64, CallKey), activation: u64, marker: Option<u64>) -> bool {
        (key.0 <= activation || self.released.is_some_and(|released| *key <= released))
            && marker.is_none_or(|marker| key.0 < marker)
    }

    /// Returns the ledger of the task as it completes, with the intent of each close that fits. A
    /// task that leaves a call with no result has a ledger with no entry, so that its completion
    /// ends a task sequence as a command does.
    pub(crate) fn take_ledger(&mut self) -> Option<WorkflowTaskStreamCalls> {
        if matches!(self.phase, Phase::Replaying | Phase::ReplayOnly) {
            return None;
        }
        let attempt = self.task.attempt();
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
            if !self.ledger.close_intent(&close.stream, seq) {
                break;
            }
            close.state = CloseState::Sealed(attempt);
        }
        let ledger = self.ledger.take();
        if ledger.is_none() && self.needs_next_task() {
            return Some(WorkflowTaskStreamCalls::default());
        }
        ledger
    }

    /// Returns the calls that the Worker must start or stop.
    pub(crate) fn take_requests(&mut self) -> Vec<Request> {
        std::mem::take(&mut self.requests)
    }

    /// Drops every call, the ledger of the task and the entries of History, and leaves the run
    /// with no task, as a failed task does. The run records no call after this.
    pub(crate) fn stop(&mut self) {
        self.stop_for_terminal();
        self.task = TaskState::NoTask;
        self.held = Held::default();
        self.ready.clear();
        self.released = None;
        self.ledger = LedgerBuilder::new(self.runtimes.settings);
        self.requests.clear();
    }

    /// Checks that a live call can run on the Store of `stream`, and that History can keep its
    /// result. A call whose result History can't keep must never run.
    fn check_runtime(&self, stream: &StreamRef) -> Result<(), WFMachinesError> {
        if self.phase != Phase::Live {
            return Ok(());
        }
        if !self.keeps_ledgers {
            return Err(no_ledger());
        }
        if self.runtimes.get(&stream.store).is_none() {
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
        check_stream(call, &append.stream, entry.r#ref.as_ref())?;
        if !matches!(append.state, AppendState::Waiting { .. }) {
            return Err(second("result", call));
        }
        append.state = AppendState::Replayed;
        let at = (activation_of(entry.activation_index), call);
        self.ready.insert(at, append_job(seq, &entry).into());
        Ok(())
    }

    fn match_intent(&mut self, seq: u32) -> Result<(), WFMachinesError> {
        let Some(close) = self.closes.get_mut(&seq) else {
            return Ok(());
        };
        let Some(intent) = self.held.intents.remove(&seq) else {
            return Ok(());
        };
        let call = CallKey::Close(seq);
        check_stream(call, &close.stream, intent.r#ref.as_ref())?;
        if !matches!(close.state, CloseState::Asked) {
            return Err(second("intent", call));
        }
        close.state = CloseState::Sealed(None);
        Ok(())
    }

    fn match_result(&mut self, seq: u32) -> Result<(), WFMachinesError> {
        let Some(close) = self.closes.get_mut(&seq) else {
            return Ok(());
        };
        let Some(entry) = self.held.closes.remove(&seq) else {
            return Ok(());
        };
        if !matches!(close.state, CloseState::Waiting(_)) {
            return Err(WFMachinesError::Nondeterminism(format!(
                "History has a result of Stream close {seq} with no intent before it, or a \
                 second one"
            )));
        }
        close.state = CloseState::Replayed;
        let at = (activation_of(entry.activation_index), CallKey::Close(seq));
        self.ready.insert(at, close_job(seq, &entry).into());
        Ok(())
    }

    /// Returns whether the ledger has room for the first result that waits, if the task is open.
    fn next_result_fits(&self, activation: u64) -> Option<bool> {
        let call = *self.done.first()?;
        if !self.task.is_open() {
            return None;
        }
        match call {
            CallKey::Append(seq) => {
                let AppendState::Done(result) = &self.appends[&seq].state else {
                    return None;
                };
                Some(self.ledger.has_room_for(result, activation))
            }
            CallKey::Close(seq) => {
                let CloseState::Done(result) = &self.closes[&seq].state else {
                    return None;
                };
                Some(self.ledger.has_room_for(result, activation))
            }
        }
    }

    /// Records a live result in the ledger, and returns its job. Returns `None` if the ledger has
    /// no room for it.
    fn record(
        &mut self,
        call: CallKey,
        activation: u64,
    ) -> Option<workflow_activation_job::Variant> {
        match call {
            CallKey::Append(seq) => {
                let AppendState::Done(result) = &self.appends[&seq].state else {
                    return None;
                };
                if !self.ledger.append_result(result, activation) {
                    return None;
                }
                Some(append_job(seq, &result.entry).into())
            }
            CallKey::Close(seq) => {
                let CloseState::Done(result) = &self.closes[&seq].state else {
                    return None;
                };
                if !self.ledger.close_result(result, activation) {
                    return None;
                }
                Some(close_job(seq, &result.entry).into())
            }
        }
    }

    /// Starts the call at the head of each queue, once the run is live and its task is open. It
    /// runs only when the task opens and at go-live, since every other change starts its own head.
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

    /// Starts the call at the head of the queue of one Stream, once the run is live and its task
    /// is open.
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
            CallKey::Close(seq) => {
                let Some(close) = self.closes.get_mut(&seq) else {
                    return;
                };
                let CloseState::Waiting(Some(attempt)) = close.state else {
                    return;
                };
                let call_id = CALL_IDS.fetch_add(1, Ordering::Relaxed);
                close.state = CloseState::Running(call_id);
                let request = CloseStreamRequest {
                    r#ref: Some(close.stream.clone()),
                    producer: Some(producer(&self.producer, attempt)),
                };
                (seq, call_id, &close.stream, Op::Close(request))
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
    if seq > i32::MAX as u32 {
        return Err(WFMachinesError::Fatal(format!(
            "Stream {kind} has seq {seq}, and History keeps a seq as an `int32`"
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

/// Returns the Stream of the command of `call`, or an error if the command has none, or one with
/// an id or a Store name out of range. Such a command is not valid, so it fails its Workflow Task.
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

fn no_ledger() -> WFMachinesError {
    WFMachinesError::Stream(
        "The server doesn't take the `sdk_metadata` of a Workflow Task completion, so History \
         can't keep the results of Stream calls"
            .to_string(),
    )
}

fn no_runtime(stream: &StreamRef) -> WFMachinesError {
    WFMachinesError::Stream(format!(
        "This Worker has no Stream runtime for the Store `{}`",
        stream.store
    ))
}

/// Checks that a call is for the Stream that History has, in the Store that History has. A call
/// with no Store name matches any Store. An entry with no ref matches no call, since each call has
/// a Stream id.
fn check_stream(
    call: CallKey,
    sent: &StreamRef,
    recorded: Option<&StreamRef>,
) -> Result<(), WFMachinesError> {
    let (id, store) = recorded.map_or(("", ""), |stream| (&stream.id[..], &stream.store[..]));
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

/// Returns the request id of append `seq` of the run `run_id` in the attempt of `producer`. It's
/// the same exactly when the run, the attempt and the seq are, and it joins these parts with `/`.
///
/// - the run id
/// - the [`attempt_key`] of the attempt, in hex (56 characters)
/// - the seq
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
    use super::super::{StreamManager, StreamSettings, manager::recorded};
    use super::*;
    use crate::test_help::stream_store::{MemoryStore, Request as StoreRequest};
    use prost::Message;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::Arc;
    use temporalio_common::protos::{
        coresdk::workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, resolve_stream_append,
        },
        temporal::api::sdk::v1::workflow_task_stream_calls::{
            AppendOffsets, CallFailure, append_result,
        },
    };
    use temporalio_streams::{AnyError, StreamRuntime, record::StoreLimits};
    use tokio::runtime::Handle;
    use workflow_activation_job::Variant;

    /// The number of seeds that each generated test runs.
    const SEEDS: u64 = 400;

    fn runtimes(stores: &[&str]) -> StreamRuntimes {
        StreamRuntimes::new(stores.iter().map(|name| {
            let store = Arc::new(MemoryStore::default());
            let runtime = StreamRuntime::new(store, StoreLimits::default());
            (name.to_string(), Arc::new(runtime))
        }))
    }

    fn calls(phase: Phase, task: i64) -> StreamCalls {
        let mut calls = StreamCalls::new("n", "w", "r", runtimes(&["mem"]), phase, true);
        calls.set_task(TaskState::Open(task_attempt(task)));
        calls
    }

    /// Completes the current task, and opens the next one.
    fn next_task(calls: &mut StreamCalls, task: i64) -> Option<WorkflowTaskStreamCalls> {
        let ledger = calls.take_ledger();
        calls.set_task(TaskState::NoTask);
        calls.set_task(TaskState::Open(task_attempt(task)));
        ledger
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

    fn close(seq: u32, id: &str) -> CloseStream {
        CloseStream {
            seq,
            stream: Some(stream(id)),
        }
    }

    fn appended(seq: u32) -> append_result::Result {
        append_result::Result::Appended(AppendOffsets {
            first_offset: vec![seq as u8],
            last_offset: vec![seq as u8],
        })
    }

    /// Returns the result of append `seq` to the Stream `id`, as a run holds it before a ledger
    /// records it.
    fn append_done(seq: u32, id: &str) -> Waiting<AppendResult> {
        Waiting::new(AppendResult {
            seq: seq as i32,
            r#ref: Some(stream(id)),
            result: Some(appended(seq)),
            activation_index: 0,
        })
    }

    fn refused(message: impl Into<String>) -> CallFailure {
        recorded(&AnyError::invalid_argument(message))
    }

    /// Returns a ledger that `write` fills.
    fn history(write: impl FnOnce(&mut LedgerBuilder)) -> WorkflowTaskStreamCalls {
        let mut ledger = LedgerBuilder::new(StreamSettings::default());
        write(&mut ledger);
        ledger.take().unwrap()
    }

    /// Returns the calls that started since the last look, as `append 1 of s at 3`, with the
    /// started event id of their attempt.
    fn started(calls: &mut StreamCalls) -> Vec<String> {
        let mut started: Vec<String> = (calls.take_requests().into_iter())
            .map(|request| {
                let (call, stream, producer) = match request {
                    Request::Run(Call { seq, op, .. }) => match op {
                        Op::Append(r, _) => (CallKey::Append(seq), r.r#ref, r.producer),
                        Op::Close(r) => (CallKey::Close(seq), r.r#ref, r.producer),
                    },
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
        let (call_id, seq, outcome) = match call {
            CallKey::Append(seq) => {
                let AppendState::Running(call_id) = calls.appends[&seq].state else {
                    panic!("{call} doesn't run");
                };
                let result = match failed {
                    Some(failure) => append_result::Result::Failure(failure),
                    None => appended(seq),
                };
                (call_id, seq, Outcome::Append(result))
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
        (calls.take_jobs(activation, None).into_iter())
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

    /// Returns the key of a call that runs, and the id of its Stream.
    fn target(call: &Call) -> (CallKey, &str) {
        match &call.op {
            Op::Append(request, _) => (
                CallKey::Append(call.seq),
                &request.r#ref.as_ref().unwrap().id,
            ),
            Op::Close(request) => (
                CallKey::Close(call.seq),
                &request.r#ref.as_ref().unwrap().id,
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

    /// Returns the Store's answer to `call`, which is `failure` if there is one.
    fn answer(call: &Call, failure: Option<CallFailure>) -> StreamResolution {
        let outcome = match (&call.op, failure) {
            (Op::Append(..), None) => Outcome::Append(appended(call.seq)),
            (Op::Append(..), Some(failure)) => {
                Outcome::Append(append_result::Result::Failure(failure))
            }
            (Op::Close(_), failure) => Outcome::Close(failure),
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
        calls.close(close(1, "s")).unwrap();
        assert_eq!(started(&mut calls), ["append 1 of s at 3"]);
        // Closes on Streams with long ids fill the ledger, so the last intent waits for task 7.
        let long = |seq: u32| format!("{seq:02}{}", "s".repeat(4000));
        for seq in 2..=70 {
            calls.close(close(seq, &long(seq))).unwrap();
        }
        let first = next_task(&mut calls, 7).unwrap();
        let sealed = first.close_intents.len() as u32;
        assert!(sealed < 70, "{sealed}");
        calls.ledger_recorded().unwrap();
        finish(&mut calls, CallKey::Append(1), None);
        let mut expected = vec!["append 2 of s at 3".to_string()];
        expected.extend((2..=sealed).map(|seq| format!("close {seq} of {} at 3", long(seq))));
        expected.sort();
        assert_eq!(started(&mut calls), expected);
        finish(&mut calls, CallKey::Append(2), None);
        assert_eq!(started(&mut calls), ["close 1 of s at 3"]);
        next_task(&mut calls, 11).unwrap();
        calls.ledger_recorded().unwrap();
        let rest = started(&mut calls);
        assert_eq!(rest.len(), 70 - sealed as usize);
        assert!(rest.iter().all(|call| call.ends_with(" at 7")), "{rest:?}");
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
        // The ledger has no entry, and its completion ends a task sequence.
        let ledger = next_task(&mut live, 7);
        assert_eq!(ledger, Some(WorkflowTaskStreamCalls::default()));
        assert!(live.has_jobs(0, None));
        assert_eq!(jobs(&mut live, 0), ["append 1", "append 2"]);
        live.append(append(3, "s")).unwrap();
        started(&mut live);
        finish(&mut live, CallKey::Append(3), None);
        assert_eq!(jobs(&mut live, 2), ["append 3"]);
        let ledger = next_task(&mut live, 11).unwrap();
        let entries: Vec<_> = (ledger.append_results.iter())
            .map(|append| (append.seq, append.activation_index))
            .collect();
        assert_eq!(entries, [(1, 0), (2, 0), (3, 2)]);
    }

    #[test]
    fn a_release_ends_with_its_activation() {
        let ledger = history(|ledger| assert!(ledger.append_result(&append_done(1, "1"), 2)));
        let mut replay = calls(Phase::Replaying, 9);
        replay.append(append(1, "1")).unwrap();
        replay.peeked(ledger).unwrap();
        replay.release_ready();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
        // A result of the next task waits for its own activation.
        replay.append(append(2, "2")).unwrap();
        let next = history(|ledger| assert!(ledger.append_result(&append_done(2, "2"), 1)));
        replay.peeked(next).unwrap();
        assert!(!replay.has_jobs(0, None));
    }

    #[test]
    fn two_appends_have_one_request_id_exactly_when_their_run_attempt_and_seq_are_equal() {
        let runs = [
            "0193c2e4-7f3a-7c1d-9b2e-1a2b3c4d5e6f",
            "0193c2e4-7f3a-7c1d-9b2e-1a2b3c4d5e70",
        ];
        let mut equal = 0;
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            // Few values for each field, so that some appends are equal.
            let appends: Vec<(&str, WorkflowTaskAttempt, u32)> = (0..8)
                .map(|_| {
                    let started_time = rng.random_bool(0.8).then(|| prost_types::Timestamp {
                        seconds: 1_763_600_000 + rng.random_range(0..2),
                        nanos: [0, 999_999_999][rng.random_range(0..2)],
                    });
                    let attempt = WorkflowTaskAttempt {
                        started_event_id: [3, 31, 313][rng.random_range(0..3)],
                        attempt: [1, 13, 131][rng.random_range(0..3)],
                        started_time,
                    };
                    let seq = [1, 11, 111][rng.random_range(0..3)];
                    (runs[rng.random_range(0..2)], attempt, seq)
                })
                .collect();
            let id = |(run, attempt, seq): &(&str, WorkflowTaskAttempt, u32)| {
                let of_run = WorkflowProducer {
                    run_id: run.to_string(),
                    ..Default::default()
                };
                request_id(run, &producer(&of_run, *attempt), *seq)
            };
            for a in &appends {
                for b in &appends {
                    assert_eq!(id(a) == id(b), a == b, "seed {seed}: {a:?} and {b:?}");
                    equal += usize::from(a == b && !std::ptr::eq(a, b));
                }
            }
        }
        assert!(equal > 0, "No two appends were equal");
    }

    /// A command of Workflow code in a generated script.
    #[derive(Clone, Debug)]
    enum Command {
        Append(AppendToStream),
        Close(CloseStream),
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
        ledger: Option<WorkflowTaskStreamCalls>,
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
        /// Gives Workflow code an activation, and takes its commands.
        fn activate(&mut self, commands: &[Command]) -> Result<Vec<Variant>, WFMachinesError> {
            let jobs = self.calls.take_jobs(self.activation, None);
            self.activation += 1;
            for command in commands {
                match command {
                    Command::Append(append) => self.calls.append(append.clone())?,
                    Command::Close(close) => self.calls.close(close.clone())?,
                    Command::Terminal => {
                        self.calls.stop_for_terminal();
                        self.ended = true;
                    }
                }
            }
            Ok(jobs)
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
        let calls = StreamCalls::new("n", "w", "r", runtimes(&["mem"]), Phase::Replaying, true);
        let mut run = Run {
            calls,
            activation: 0,
            ended: false,
        };
        run.calls.set_task(TaskState::Open(live));
        let mut replayed = vec![];
        let mut sent_in = None;
        for (index, task) in history.iter().enumerate() {
            if index > 0 && history[index - 1].ledger.is_some() {
                run.calls.ledger_recorded()?;
            }
            // Core peeks the ledger that the task's completion recorded before it activates.
            if let Some(ledger) = &task.ledger {
                run.calls.peeked(ledger.clone())?;
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
                let jobs = run.activate(&task.commands_at(at))?;
                activations.push((at, jobs));
            }
            if run.ended && sent_in.is_none() {
                sent_in = Some(index);
            }
            let requests = run.calls.take_requests();
            assert!(requests.is_empty(), "seed {seed}: {requests:?}");
            replayed.push(activations);
        }
        if history.last().is_some_and(|task| task.ledger.is_some()) {
            run.calls.ledger_recorded()?;
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
        full_ledgers: usize,
        heartbeats: usize,
        failed_tasks: usize,
        /// The last commands that Workflow code sent while a call had no result in a ledger.
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
        last_close: u32,
        /// The calls whose result no ledger has.
        unresolved: BTreeSet<CallKey>,
        /// The state as of the last task in History, which a failed task goes back to.
        committed: (u32, u32, BTreeSet<CallKey>),
    }

    impl<'a> FirstRun<'a> {
        fn new(seed: u64, seen: &'a mut Seen) -> Self {
            let run = Run {
                calls: StreamCalls::new("n", "w", "r", runtimes(&["mem"]), Phase::Live, true),
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
                last_close: 0,
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
            if self
                .history
                .last()
                .is_some_and(|task| task.ledger.is_some())
            {
                self.run.calls.ledger_recorded().unwrap();
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
                if self.run.calls.must_complete(self.run.activation) {
                    self.seen.full_ledgers += 1;
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
            let jobs = result.unwrap_or_else(|error| panic!("seed {}: {error:?}", self.seed));
            for resolution in late {
                self.run.calls.resolve(resolution);
            }
            for command in &commands {
                match command {
                    Command::Append(append) => {
                        self.unresolved.insert(CallKey::Append(append.seq));
                    }
                    Command::Close(close) => {
                        self.unresolved.insert(CallKey::Close(close.seq));
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
                        self.last_close += 1;
                        commands.push(Command::Close(close(self.last_close, &id)));
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
                                self.last_close += 1;
                                let seq = self.last_close;
                                let mut long_store = close(seq, &id);
                                long_store.stream.as_mut().unwrap().store = long;
                                let error = format!(
                                    "Stream close {seq} is invalid: The Store name has 4097 \
                                     bytes, more than 4096"
                                );
                                (Command::Close(long_store), error)
                            }
                        };
                        commands.push(command);
                        return (commands, Some(WFMachinesError::Fatal(error)));
                    }
                    80..=82 => {
                        self.seen.bad_seqs += 1;
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
                        return (commands, Some(WFMachinesError::Fatal(error)));
                    }
                    83..=86 => {
                        commands.push(Command::Terminal);
                        return (commands, None);
                    }
                    87..=89 => {
                        // A burst of appends, each on a Stream of its own, can fill a ledger.
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
            let failure = match &call.op {
                Op::Append(request, _) => {
                    // A large failure fills the ledger in a few dozen results.
                    let burst = request.r#ref.as_ref().unwrap().id.starts_with('b');
                    let large = if burst { 8 } else { 1 };
                    match self.rng.random_range(0..10) {
                        at if at < large => Some(refused("x".repeat(4000))),
                        at if at < large + 2 => Some(refused("refused")),
                        _ => None,
                    }
                }
                Op::Close(_) => self.rng.random_bool(0.3).then(|| refused("refused")),
            };
            answer(&call, failure)
        }

        fn dispatch(&mut self) {
            dispatch(&mut self.run.calls, &mut self.in_flight);
        }

        /// Completes the task in hand, and checks that the run tracks exactly the calls whose
        /// result no ledger has.
        fn complete(&mut self) {
            let ledger = self.run.calls.take_ledger();
            self.run.calls.set_task(TaskState::NoTask);
            for entry in ledger.iter() {
                for append in &entry.append_results {
                    self.unresolved.remove(&CallKey::Append(append.seq as u32));
                }
                for close in &entry.close_results {
                    self.unresolved.remove(&CallKey::Close(close.seq as u32));
                }
            }
            let mut task = std::mem::take(&mut self.task);
            task.ledger = ledger;
            task.ends_workflow = self.run.ended;
            self.history.push(task);
            self.committed = (self.last_append, self.last_close, self.unresolved.clone());
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

        /// Fails the task in hand. A new run replays History and goes live in the next attempt.
        fn retry(&mut self) {
            self.seen.failed_tasks += 1;
            self.run.calls.stop();
            self.in_flight.clear();
            self.deferred.clear();
            (self.last_append, self.last_close, self.unresolved) = self.committed.clone();
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
            // The replay resumes each call whose result no ledger has, and only those.
            assert_eq!(run.tracked(), self.unresolved, "seed {seed}");
            let recorded: BTreeSet<CallKey> = (self.history.iter())
                .filter_map(|task| task.ledger.as_ref())
                .flat_map(|ledger| {
                    let appends = ledger.append_results.iter();
                    let closes = ledger.close_results.iter();
                    (appends.map(|append| CallKey::Append(append.seq as u32)))
                        .chain(closes.map(|close| CallKey::Close(close.seq as u32)))
                })
                .collect();
            for request in run.calls.take_requests() {
                let Request::Run(Call { seq, op, .. }) = &request else {
                    panic!("seed {seed}: {request:?}");
                };
                let (key, sent) = match op {
                    Op::Append(append, _) => (CallKey::Append(*seq), &append.producer),
                    Op::Close(close) => (CallKey::Close(*seq), &close.producer),
                };
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
            full_ledgers,
            heartbeats,
            failed_tasks,
            dropped,
            invalid,
            bad_seqs,
        } = seen;
        let counts = [
            full_ledgers,
            heartbeats,
            failed_tasks,
            dropped,
            invalid,
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
            let mut in_flight: Vec<Call> = vec![];
            let (mut appends, mut closes, mut task, mut ended) = (0, 0, 3, false);
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
                        // The last command of the Workflow stops each call.
                        calls.stop_for_terminal();
                        queues.clear();
                        ended = true;
                    }
                    _ => {}
                }
                if rng.random_bool(0.15) {
                    task += 4;
                    if let Some(ledger) = next_task(&mut calls, task) {
                        calls.ledger_recorded().unwrap();
                        for intent in &ledger.close_intents {
                            queues
                                .entry(intent.r#ref.as_ref().unwrap().id.clone())
                                .or_default()
                                .push_back(CallKey::Close(intent.seq as u32));
                        }
                    }
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

    fn key_of(job: &Variant) -> CallKey {
        match job {
            Variant::ResolveStreamAppend(append) => CallKey::Append(append.seq),
            Variant::ResolveStreamClose(close) => CallKey::Close(close.seq),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn each_result_and_intent_is_in_exactly_one_ledger_in_order() {
        let (mut full, mut oversized, mut out_of_order) = (0, 0, 0);
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            // A small bound fills a ledger after a few entries, and the default after many.
            let mut runtimes = runtimes(&["mem"]);
            if rng.random_bool(0.5) {
                runtimes.settings.bounds.max_ledger_bytes = rng.random_range(1..=16 * 1024);
            }
            let max_entry = runtimes.settings.bounds.max_ledger_bytes;
            let mut calls = StreamCalls::new("n", "w", "r", runtimes, Phase::Live, true);
            calls.set_task(TaskState::Open(task_attempt(3)));
            let budget = rng.random_range(1..=150);
            let (mut appends, mut closes) = (0, 0);
            let mut in_flight: Vec<Call> = vec![];
            // The results that Core took in each task, in order, with their activation.
            let mut taken: Vec<Vec<(u64, CallKey)>> = vec![vec![]];
            let mut ledgers = vec![];
            let (mut activation, mut task) = (0, 3);
            // The highest seq of each kind that Core took so far.
            let mut highest = (0, 0);
            for round in 0.. {
                assert!(round < 100_000, "seed {seed}: the run never ends");
                // Most calls share a few Streams, so they wait in a queue. The others have a
                // Stream of their own, whose long id fills a ledger sooner.
                for _ in 0..rng.random_range(0..=4) {
                    if appends + closes == budget {
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
                dispatch(&mut calls, &mut in_flight);
                // The Store answers some of the calls in flight, in any order. Most of them fail
                // with a long message, which fills a ledger sooner.
                for _ in 0..rng.random_range(0..=in_flight.len().min(6)) {
                    let call = in_flight.swap_remove(rng.random_range(0..in_flight.len()));
                    let bytes = rng.random_range(0..4200);
                    let failure = rng.random_bool(0.7).then(|| refused("x".repeat(bytes)));
                    calls.resolve(answer(&call, failure));
                    dispatch(&mut calls, &mut in_flight);
                }
                // Core takes the results that wait in their order, up to the first that doesn't
                // fit, and the ledger records them with the activation.
                let waiting: Vec<CallKey> = calls.done.iter().copied().collect();
                let took = if calls.has_jobs(activation, None) && rng.random_bool(0.8) {
                    let jobs: Vec<CallKey> = calls
                        .take_jobs(activation, None)
                        .iter()
                        .map(key_of)
                        .collect();
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
                // means that the ledger is full and the task must complete.
                if took > 0 {
                    let waits = !calls.done.is_empty();
                    assert_eq!(calls.must_complete(activation), waits, "seed {seed}");
                    full += usize::from(waits);
                }
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
                if took > 0 {
                    activation += 1;
                    continue;
                }
                // A full ledger completes the task, and so does a heartbeat at any time.
                let finished = appends + closes == budget && !calls.needs_next_task();
                if finished || calls.must_complete(activation) || rng.random_bool(0.03) {
                    task += 4;
                    let ledger = next_task(&mut calls, task);
                    if ledger.is_some() {
                        calls.ledger_recorded().unwrap();
                    }
                    ledgers.push(ledger.unwrap_or_default());
                    taken.push(vec![]);
                    activation = 0;
                    if finished {
                        break;
                    }
                }
            }
            // Each ledger records the results of its task in the order that Core took them, with
            // their activations. The appends come first, then the closes.
            for (index, ledger) in ledgers.iter().enumerate() {
                let appends = ledger.append_results.iter();
                let closes = ledger.close_results.iter();
                let recorded: Vec<(u64, CallKey)> = (appends)
                    .map(|a| {
                        (
                            activation_of(a.activation_index),
                            CallKey::Append(a.seq as u32),
                        )
                    })
                    .chain(closes.map(|c| {
                        (
                            activation_of(c.activation_index),
                            CallKey::Close(c.seq as u32),
                        )
                    }))
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
            let all = |seqs: &dyn Fn(&WorkflowTaskStreamCalls) -> Vec<u32>| -> Vec<u32> {
                ledgers.iter().flat_map(seqs).collect()
            };
            once(
                all(&|m| m.append_results.iter().map(|a| a.seq as u32).collect()),
                appends,
            );
            once(
                all(&|m| m.close_results.iter().map(|c| c.seq as u32).collect()),
                closes,
            );
            let intents = all(&|m| m.close_intents.iter().map(|i| i.seq as u32).collect());
            assert_eq!(intents, (1..=closes).collect::<Vec<_>>(), "seed {seed}");
            for ledger in &ledgers {
                let entries = ledger.append_results.len()
                    + ledger.close_intents.len()
                    + ledger.close_results.len();
                // The bound of the entries is on the encoded ledger.
                let size = ledger.encoded_len();
                assert!(size <= max_entry || entries == 1, "seed {seed}: {size}");
                oversized += usize::from(size > max_entry);
            }
        }
        // The scripts reach each case that the test is for.
        let counts = [full, oversized, out_of_order];
        assert!(
            counts.iter().all(|count| *count > 0),
            "full {full}, oversized {oversized}, out of order {out_of_order}"
        );
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
            .filter_map(|(index, task)| Some((index, task.ledger.as_ref()?)))
            .flat_map(|(index, ledger)| {
                let appends = ledger.append_results.iter();
                let closes = ledger.close_results.iter();
                (appends.map(move |append| (index, CallKey::Append(append.seq as u32))))
                    .chain(closes.map(move |close| (index, CallKey::Close(close.seq as u32))))
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
        let ledger = history[task].ledger.as_mut().unwrap();
        match call {
            CallKey::Append(seq) => {
                let at = ledger
                    .append_results
                    .iter()
                    .position(|append| append.seq == seq as i32)?;
                match change {
                    Change::Twice => ledger
                        .append_results
                        .push(ledger.append_results[at].clone()),
                    Change::Seq => ledger.append_results[at].seq = 999,
                    Change::Activation => ledger.append_results[at].activation_index += 1,
                    _ => {
                        let entry = ledger.append_results.remove(at);
                        ledger.close_results.push(CloseResult {
                            seq: seq as i32,
                            failure: None,
                            activation_index: entry.activation_index,
                        });
                    }
                }
            }
            CallKey::Close(seq) => {
                let at = ledger
                    .close_results
                    .iter()
                    .position(|close| close.seq == seq as i32)?;
                match change {
                    Change::Twice => ledger.close_results.push(ledger.close_results[at].clone()),
                    Change::Seq => ledger.close_results[at].seq = 999,
                    Change::Activation => ledger.close_results[at].activation_index += 1,
                    _ => {
                        let entry = ledger.close_results.remove(at);
                        ledger.append_results.push(AppendResult {
                            seq: seq as i32,
                            r#ref: Some(stream("s0")),
                            result: Some(append_result::Result::Appended(Default::default())),
                            activation_index: entry.activation_index,
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
            match replay(seed, &history, task_attempt(1000)) {
                Err(WFMachinesError::Nondeterminism(_)) => {}
                // An entry at another activation, or of the other kind, can match a call. Then
                // Workflow code gets other jobs, which is as wrong as an error.
                Ok(replayed) if matches!(change, Change::Activation | Change::Kind) => {
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
        let ledger = history(|m| assert!(m.append_result(&append_done(1, "s"), 0)));
        replay.peeked(ledger).unwrap();
        assert_eq!(jobs(&mut replay, 0), ["append 1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_with_no_runtime_or_no_ledger_fails_the_task() {
        let stores = ["a", "b", "c"];
        let (mut with_no_runtime, mut with_no_ledger) = (0, 0);
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
            // A server that doesn't take the `sdk_metadata` of a completion keeps no ledger.
            let keeps_ledgers = rng.random_bool(0.7);
            with_no_ledger += usize::from(!keeps_ledgers && phase != Phase::ReplayOnly);
            let mut calls = StreamCalls::new("n", "w", "r", runtimes.clone(), phase, keeps_ledgers);
            calls.set_task(TaskState::Open(task_attempt(3)));
            let (mut appends, mut closes) = (0, 0);
            let (mut unserved, mut store_of) = (BTreeMap::new(), HashMap::new());
            for index in 0..rng.random_range(1..=6) {
                let store = stores[rng.random_range(0..3)];
                let stream = StreamRef {
                    id: format!("s{index}"),
                    store: store.to_string(),
                };
                store_of.insert(stream.id.clone(), store);
                let (call, result) = if rng.random_bool(0.5) {
                    appends += 1;
                    let command = AppendToStream {
                        seq: appends,
                        stream: Some(stream),
                        bodies: vec![Payload::default()],
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
                let error = WFMachinesError::Stream(format!(
                    "This Worker has no Stream runtime for the Store `{store}`"
                ));
                let is_served = served.iter().any(|(name, _)| *name == store);
                if phase == Phase::Live && (!is_served || !keeps_ledgers) {
                    // The command fails the task, so the run records nothing of the call.
                    let error = if keeps_ledgers { error } else { no_ledger() };
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
            let ledger = calls.take_ledger().unwrap_or_default();
            calls.ledger_recorded().unwrap();
            let mut sent = calls.take_requests();
            let at_go_live = calls.go_live();
            sent.extend(calls.take_requests());
            // No call on a Store with no runtime records an intent or reaches a Store.
            let streams: Vec<&str> = (ledger.close_intents.iter())
                .map(|intent| intent.r#ref.as_ref().unwrap().store.as_str())
                .chain(sent.iter().map(|request| match request {
                    Request::Run(call) => call.store.as_str(),
                    Request::Stop(_) => panic!("seed {seed}"),
                }))
                .collect();
            assert!(
                (streams.iter()).all(|store| served.iter().any(|(name, _)| name == store)),
                "seed {seed}"
            );
            // A replay fails the task when it goes live with an unserved call, or with any call
            // whose result History can't keep, which a replay Worker never does.
            let expected = match (phase, unserved.into_values().next()) {
                (Phase::Replaying, Some(error)) => Err(error),
                (Phase::Replaying, None) if !keeps_ledgers && appends + closes > 0 => {
                    Err(no_ledger())
                }
                _ => Ok(()),
            };
            assert_eq!(at_go_live, expected, "seed {seed}");
            // No call runs if History can't keep its result.
            assert!(keeps_ledgers || sent.is_empty(), "seed {seed}");
            // Each Store gets only the calls whose ref has its name.
            let manager = StreamManager::new(runtimes, Handle::current(), |_, _| {});
            manager.dispatch("r", sent);
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            for (name, store) in &served {
                for request in store.requests() {
                    let id = match request {
                        StoreRequest::Append(append, _) => append.r#ref.unwrap().id,
                        StoreRequest::Close(close) => close.r#ref.unwrap().id,
                    };
                    assert_eq!(store_of[&id], *name, "seed {seed}: Stream {id}");
                }
            }
        }
        assert!(with_no_runtime > 0, "No seed had a Worker with no runtime");
        assert!(
            with_no_ledger > 0,
            "No seed had a server that keeps no ledger"
        );
    }
}
