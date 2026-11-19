//! The Stream calls of a run, and the marker that records what Workflow code got in one Workflow
//! Task. A call has no command or event of its own, so it needs no state machine.

use super::{
    CallError,
    manager::{
        AppendSpec, CloseSpec, MAX_MARKER_ENTRY_BYTES, StreamCallResult, StreamRequest,
        StreamResolution, input_over_limit, next_call_id,
    },
};
use prost::Message;
use std::collections::{HashMap, HashSet, VecDeque};
use temporalio_common::protos::{
    coresdk::{
        external_data::{StreamAppendEntry, StreamCloseEntry, StreamMarkerData},
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, StreamAppended, resolve_stream_append,
            workflow_activation_job,
        },
        workflow_commands::{AppendToStream, CloseStream},
    },
    temporal::api::{
        failure::v1::Failure,
        stream::v1::{Producer, StreamRef, WorkflowTaskEpoch, producer::Kind},
    },
};

type Result<T, E = CallError> = std::result::Result<T, E>;

macro_rules! nondeterminism {
    ($($arg:tt)*) => {{
        CallError::Nondeterminism(format!($($arg)*))
    }};
}

macro_rules! fatal {
    ($($arg:tt)*) => {{
        CallError::Fatal(format!($($arg)*))
    }};
}

/// What a result does to the activations of the run.
pub enum StreamJob {
    /// The result of an append.
    Append(ResolveStreamAppend),
    /// The result of a close.
    Close(ResolveStreamClose),
}

/// A call that waits for the one before it on its Stream. The first one is the one that runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Queued {
    Append(u32),
    Close(u32),
}

/// Where the intent of a close is. The Worker closes the Stream only after History has the task
/// that recorded the intent, so that a failed task closes nothing.
#[derive(Clone, Copy)]
enum Intent {
    /// Workflow code asked for the close in this task.
    Pending,
    /// The completion of this task, with this epoch, carries the intent.
    Sealed(WorkflowTaskEpoch),
    /// History has the intent, from the completion that the server took or from a replay.
    Recorded(Option<WorkflowTaskEpoch>),
    /// Workflow code asked for the close, and this task's marker had no room for its intent. A
    /// later task records it.
    Deferred,
}

struct Close {
    call_id: u64,
    stream: StreamRef,
    producer: Producer,
    intent: Intent,
    /// True once the close waits in its Stream's queue or runs.
    queued: bool,
    /// True once the Worker runs the close.
    dispatched: bool,
    resolved: bool,
    /// True for a close that Core refused for an input over its limit. The marker has nothing of it.
    refused: bool,
}

/// The tag and the length of a field of bytes or of a message of less than 2 MiB.
const FIELD_BYTES: usize = 4;
/// A field of a varint of 32 bits, with its tag.
const VARINT_BYTES: usize = 6;
/// A field of a varint of 64 bits, with its tag.
const VARINT64_BYTES: usize = 11;
/// An append entry with no failure, apart from its offsets.
const APPEND_ENTRY_BYTES: usize = FIELD_BYTES + 3 * VARINT_BYTES + 2 * FIELD_BYTES;
/// The entry of a close's result with no failure.
const CLOSE_RESULT_BYTES: usize = FIELD_BYTES + 4 * VARINT_BYTES;
/// The entry of a close's intent, with the epoch of its task. A negative 32-bit number takes the
/// ten bytes of a 64-bit one.
const CLOSE_INTENT_BYTES: usize =
    FIELD_BYTES + 2 * VARINT_BYTES + 2 * FIELD_BYTES + 4 * VARINT64_BYTES;

fn failure_bytes(failure: Option<&Failure>) -> usize {
    failure.map_or(0, |failure| FIELD_BYTES + failure.encoded_len())
}

/// What this task's marker holds, as an estimate that is never below its encoded size. A result
/// counts when it becomes a job, before Workflow code gets it, so that a result that doesn't fit
/// can wait for the next task.
#[derive(Default)]
struct EntryBudget {
    bytes: usize,
    /// The Streams of the marker's table.
    streams: HashSet<(String, String)>,
}

impl EntryBudget {
    /// Adds `bytes` of an entry of `stream`, and its Stream if the table lacks it. Returns false,
    /// and adds nothing, when the marker would pass [MAX_MARKER_ENTRY_BYTES]. An empty marker
    /// takes any entry, since no single one comes near the bound.
    fn add(&mut self, bytes: usize, stream: &StreamRef) -> bool {
        let key = (stream.store.clone(), stream.id.clone());
        let table = if self.streams.contains(&key) {
            0
        } else {
            FIELD_BYTES + stream.encoded_len()
        };
        if self.bytes > 0 && self.bytes + bytes + table > MAX_MARKER_ENTRY_BYTES {
            return false;
        }
        self.bytes += bytes + table;
        self.streams.insert(key);
        true
    }
}

/// The estimate of the marker entry of the result of an append or a close.
fn result_bytes(job: &StreamJob) -> usize {
    match job {
        StreamJob::Append(result) => {
            let (offsets, failure) = match &result.status {
                Some(resolve_stream_append::Status::Appended(appended)) => (
                    appended.first_offset.len() + appended.last_offset.len(),
                    None,
                ),
                Some(resolve_stream_append::Status::Failed(failure)) => (0, Some(failure)),
                None => (0, None),
            };
            APPEND_ENTRY_BYTES + offsets + failure_bytes(failure)
        }
        StreamJob::Close(result) => CLOSE_RESULT_BYTES + failure_bytes(result.failed.as_ref()),
    }
}

struct Append {
    call_id: u64,
    stream: StreamRef,
    /// Taken when the append is dispatched.
    spec: Option<AppendSpec>,
    resolved: bool,
    /// True for an append that Core refused for an input over its limit. The marker has nothing
    /// of it.
    refused: bool,
}

/// A result that history has for a call, which waits for its activation in a replay.
struct Held {
    seq: u32,
    activation: u32,
    stream: StreamRef,
    result: HeldResult,
}

enum HeldResult {
    Append(resolve_stream_append::Status),
    /// The intent of a close, with the epoch of its task, which gives no job.
    CloseIntent(Option<WorkflowTaskEpoch>),
    CloseResult(Option<Failure>),
}

/// The Stream calls of one run.
#[derive(Default)]
pub struct StreamCalls {
    appends: HashMap<u32, Append>,
    closes: HashMap<u32, Close>,
    /// The appends and closes of each Stream that wait for the one before them, in call order.
    /// The first one is the one that runs.
    append_queues: HashMap<(String, String), VecDeque<Queued>>,
    requests: Vec<StreamRequest>,
    /// The append and close entries of this task's marker, each with its Stream, which the marker
    /// gives as an index into its table of Streams.
    marker_appends: Vec<(StreamRef, StreamAppendEntry)>,
    marker_closes: Vec<(StreamRef, StreamCloseEntry)>,
    /// The results of the markers of history that wait for Workflow code, in marker order.
    held: Vec<Held>,
    /// False while the run replays. A call then waits for its results from history, and the
    /// Worker runs it only once replay ends.
    live: bool,
    /// False on a replay Worker, where a run never goes live and a call that history leaves open
    /// stays open, as a Local Activity does there.
    calls_stores: bool,
    /// What this task's marker holds.
    entry_budget: EntryBudget,
    /// The results of appends and closes past this task's entry bound, in the order they came.
    /// The next task takes them.
    carried_results: Vec<StreamJob>,
}

impl StreamCalls {
    /// The calls of a new run, which has started none.
    pub fn new(calls_stores: bool) -> Self {
        Self {
            calls_stores,
            ..Default::default()
        }
    }

    /// True once replay ended, so that the Worker runs new calls.
    pub fn is_live(&self) -> bool {
        self.live
    }

    /// Runs the calls that replay left open, from where Workflow code is. Returns the Stores that
    /// they use, so that the caller can check that the Worker has their runtimes.
    pub fn go_live(&mut self) -> Vec<String> {
        if self.live || !self.calls_stores {
            return vec![];
        }
        self.live = true;
        // The intents that replay left without a record go in the marker of this live task.
        for close in self.closes.values_mut() {
            if matches!(close.intent, Intent::Pending) {
                close.intent = Intent::Deferred;
            }
        }
        self.admit_deferred_intents();
        let mut stores = vec![];
        // A close that history recorded with no result waits in its Stream's queue since its intent
        // replayed. It runs again with the epoch of that intent.
        let mut keys: Vec<_> = self.append_queues.keys().cloned().collect();
        keys.sort();
        for key in keys {
            if let Some(store) = self.dispatch_head(&key) {
                stores.push(store);
            }
        }
        stores
    }

    /// Records a close, which the Worker runs only after History has this task. Returns the job of
    /// its refusal, for a close with an input over its limit.
    pub fn close(&mut self, attrs: CloseStream) -> Result<Option<StreamJob>> {
        let seq = attrs.seq;
        let stream = attrs
            .stream
            .ok_or_else(|| fatal!("Stream close {seq} has no Stream"))?;
        let producer = attrs
            .producer
            .ok_or_else(|| fatal!("Stream close {seq} has no Producer"))?;
        if !matches!(producer.kind, Some(Kind::Workflow(_))) {
            return Err(fatal!(
                "Stream close {seq} has a Producer that is not of kind Workflow"
            ));
        }
        if self.closes.contains_key(&seq) {
            return Err(fatal!("Stream close {seq} was issued twice"));
        }
        let refusal = input_over_limit(&stream);
        let refused = refusal.is_some();
        // A replay takes the intents of History, so only a live task counts them.
        let deferred = !refused
            && self.live
            && (self.has_deferred_intents() || !self.entry_budget.add(CLOSE_INTENT_BYTES, &stream));
        self.closes.insert(
            seq,
            Close {
                call_id: next_call_id(),
                stream,
                producer,
                // A refused close has no intent for a task to record, and never runs.
                intent: if refused {
                    Intent::Recorded(None)
                } else if deferred {
                    Intent::Deferred
                } else {
                    Intent::Pending
                },
                queued: false,
                dispatched: refused,
                resolved: refused,
                refused,
            },
        );
        Ok(refusal.map(|failure| {
            StreamJob::Close(ResolveStreamClose {
                seq,
                failed: Some(failure),
            })
        }))
    }

    /// True if Workflow code asked for a close that no completion carries. The task completes
    /// at once then, so that the Stream closes as soon as History has the intent.
    pub fn has_pending_intents(&self) -> bool {
        self.closes
            .values()
            .any(|close| matches!(close.intent, Intent::Pending))
    }

    /// The server took the completion that carries the sealed intents, so History has them. Each
    /// close waits in its Stream's queue, in front of every append of a later task. Returns the
    /// closes that run at once. A replay Worker runs none.
    pub fn intents_taken(&mut self) -> Vec<StreamRequest> {
        let mut sealed: Vec<_> = self
            .closes
            .iter()
            .filter(|(_, close)| matches!(close.intent, Intent::Sealed(_)))
            .map(|(seq, _)| *seq)
            .collect();
        sealed.sort();
        for seq in sealed {
            if let Some(close) = self.closes.get_mut(&seq)
                && let Intent::Sealed(epoch) = close.intent
            {
                close.intent = Intent::Recorded(Some(epoch));
            }
            if self.calls_stores {
                self.queue_close(seq);
            }
        }
        std::mem::take(&mut self.requests)
    }

    fn has_deferred_intents(&self) -> bool {
        self.closes
            .values()
            .any(|close| matches!(close.intent, Intent::Deferred))
    }

    /// Gives the deferred intents room in this task's marker, in seq order, while they fit.
    fn admit_deferred_intents(&mut self) {
        let mut deferred: Vec<_> = self
            .closes
            .iter()
            .filter(|(_, close)| matches!(close.intent, Intent::Deferred))
            .map(|(seq, _)| *seq)
            .collect();
        deferred.sort();
        for seq in deferred {
            let Some(close) = self.closes.get_mut(&seq) else {
                continue;
            };
            if !self.entry_budget.add(CLOSE_INTENT_BYTES, &close.stream) {
                return;
            }
            close.intent = Intent::Pending;
        }
    }

    /// Drops the intents that History doesn't have, when their task fails.
    pub fn drop_intents(&mut self) {
        self.closes
            .retain(|_, close| matches!(close.intent, Intent::Recorded(_)));
    }

    fn queue_close(&mut self, seq: u32) {
        let Some(close) = self.closes.get_mut(&seq) else {
            return;
        };
        close.queued = true;
        let key = (close.stream.store.clone(), close.stream.id.clone());
        self.append_queues
            .entry(key.clone())
            .or_default()
            .push_back(Queued::Close(seq));
        // A replaying run sends nothing. It runs the head of each queue once it goes live.
        if self.live {
            self.dispatch_head(&key);
        }
    }

    /// Runs the call at the front of a Stream's queue, unless it runs already. Returns its Store.
    fn dispatch_head(&mut self, key: &(String, String)) -> Option<String> {
        match self.append_queues.get(key)?.front().copied()? {
            Queued::Append(seq) => {
                let spec = self.appends.get_mut(&seq)?.spec.take()?;
                let store = spec.stream.store.clone();
                self.requests.push(StreamRequest::Append(spec));
                Some(store)
            }
            Queued::Close(seq) => {
                let close = self.closes.get_mut(&seq)?;
                let Intent::Recorded(epoch) = close.intent else {
                    return None;
                };
                if close.resolved || close.dispatched {
                    return None;
                }
                close.dispatched = true;
                let spec = CloseSpec {
                    call_id: close.call_id,
                    seq,
                    stream: close.stream.clone(),
                    producer: close.producer.clone(),
                    epoch,
                };
                self.requests.push(StreamRequest::Close(spec));
                Some(close.stream.store.clone())
            }
        }
    }

    /// Starts an append. Returns the job of its refusal, for an append with an input over its
    /// limit.
    pub fn append(&mut self, attrs: AppendToStream) -> Result<Option<StreamJob>> {
        let seq = attrs.seq;
        let stream = attrs
            .stream
            .ok_or_else(|| fatal!("Stream append {seq} has no Stream"))?;
        let producer = attrs
            .producer
            .ok_or_else(|| fatal!("Stream append {seq} has no Producer"))?;
        if !matches!(producer.kind, Some(Kind::Workflow(_))) {
            return Err(fatal!(
                "Stream append {seq} has a Producer that is not of kind Workflow"
            ));
        }
        if self.appends.contains_key(&seq) {
            return Err(fatal!("Stream append {seq} was issued twice"));
        }
        let call_id = next_call_id();
        if let Some(failure) = input_over_limit(&stream) {
            self.appends.insert(
                seq,
                Append {
                    call_id,
                    stream,
                    spec: None,
                    resolved: true,
                    refused: true,
                },
            );
            return Ok(Some(StreamJob::Append(ResolveStreamAppend {
                seq,
                status: Some(resolve_stream_append::Status::Failed(failure)),
            })));
        }
        let spec = AppendSpec {
            call_id,
            seq,
            stream: stream.clone(),
            producer,
            // The caller's own sequence goes to the Store as given. With none, the append's seq,
            // which grows in a run, lets the Store refuse a resend in one epoch.
            sequence: if attrs.sequence != 0 {
                attrs.sequence
            } else {
                i64::from(seq)
            },
            bodies: attrs.bodies,
        };
        let stream_key = (stream.store.clone(), stream.id.clone());
        let queue = self.append_queues.entry(stream_key.clone()).or_default();
        queue.push_back(Queued::Append(seq));
        let runs_now = queue.len() == 1;
        self.appends.insert(
            seq,
            Append {
                call_id,
                stream,
                spec: Some(spec),
                resolved: false,
                refused: false,
            },
        );
        if runs_now && self.live {
            self.dispatch_head(&(stream_key.0, stream_key.1));
        }
        Ok(None)
    }

    /// Takes a resolved call off its Stream's queue, and runs the next one once replay ended.
    fn unqueue(&mut self, key: &(String, String), done: Queued) {
        if let Some(queue) = self.append_queues.get_mut(key) {
            queue.retain(|queued| *queued != done);
        }
        if self.live {
            self.dispatch_head(key);
        }
    }

    /// Applies a result. Returns the job it makes, or `None` when its call is gone, stopped or
    /// from an older dispatch, or when its result waits for a later task.
    pub fn resolve(&mut self, resolution: StreamResolution) -> Option<StreamJob> {
        let StreamResolution {
            call_id,
            seq,
            result,
        } = resolution;
        match result {
            StreamCallResult::Append(status) => {
                let append = self.appends.get_mut(&seq)?;
                if append.call_id != call_id || append.resolved {
                    return None;
                }
                append.resolved = true;
                let stream = append.stream.clone();
                let key = (stream.store.clone(), stream.id.clone());
                self.unqueue(&key, Queued::Append(seq));
                let job = ResolveStreamAppend {
                    seq,
                    status: Some(status),
                };
                self.fit_result(StreamJob::Append(job), &stream)
            }
            StreamCallResult::Close(failed) => {
                let close = self.closes.get_mut(&seq)?;
                if close.call_id != call_id || close.resolved {
                    return None;
                }
                close.resolved = true;
                let stream = close.stream.clone();
                let key = (stream.store.clone(), stream.id.clone());
                self.unqueue(&key, Queued::Close(seq));
                self.fit_result(
                    StreamJob::Close(ResolveStreamClose { seq, failed }),
                    &stream,
                )
            }
        }
    }

    /// Holds the results of a marker of history, each for its activation in the Workflow Task.
    pub fn insert_peeked(&mut self, data: StreamMarkerData) -> Result<()> {
        let streams = data.streams;
        let stream_of = |kind: &str, seq: u32, index: u32| {
            streams.get(index as usize).cloned().ok_or_else(|| {
                fatal!(
                    "The Stream marker entry of {kind} {seq} has Stream {index}, which the marker \
                     doesn't have"
                )
            })
        };
        for entry in data.appends {
            let status = match entry.failure {
                Some(failure) => resolve_stream_append::Status::Failed(failure),
                None => resolve_stream_append::Status::Appended(StreamAppended {
                    first_offset: entry.first_offset,
                    last_offset: entry.last_offset,
                }),
            };
            self.held.push(Held {
                seq: entry.seq,
                activation: entry.activation,
                stream: stream_of("append", entry.seq, entry.stream_index)?,
                result: HeldResult::Append(status),
            });
        }
        for entry in data.closes {
            // An intent gives no job, so any activation of its task takes it.
            let (activation, result) = if entry.resolved {
                (entry.activation, HeldResult::CloseResult(entry.failure))
            } else {
                (0, HeldResult::CloseIntent(entry.intent_epoch))
            };
            self.held.push(Held {
                seq: entry.seq,
                activation,
                stream: stream_of("close", entry.seq, entry.stream_index)?,
                result,
            });
        }
        Ok(())
    }

    /// Takes the held results that Workflow code gets in activation `current`, or that it gets
    /// late, of calls that it started. With `include_held`, it takes those of later activations
    /// too, for a replay that made fewer activations than its first run.
    pub fn take_held(&mut self, current: u32, include_held: bool) -> Result<Vec<StreamJob>> {
        let mut jobs = vec![];
        let mut at = 0;
        while at < self.held.len() {
            let held = &self.held[at];
            let started = match held.result {
                HeldResult::Append(_) => self.appends.contains_key(&held.seq),
                HeldResult::CloseIntent(_) | HeldResult::CloseResult(_) => {
                    self.closes.contains_key(&held.seq)
                }
            };
            if !started || (held.activation > current && !include_held) {
                at += 1;
                continue;
            }
            let held = self.held.remove(at);
            jobs.extend(self.apply_held(held)?);
        }
        Ok(jobs)
    }

    fn apply_held(&mut self, held: Held) -> Result<Option<StreamJob>> {
        let Held {
            seq,
            stream,
            result,
            ..
        } = held;
        // A call with no Store name takes the one of history. Lang sends no name when it has no
        // Stream config to pick one from, as a replay Worker may run without one.
        let adopt = |call: &mut StreamRef| {
            if call.store.is_empty() {
                call.store.clone_from(&stream.store);
            }
        };
        let differs = |call: &StreamRef| call.store != stream.store || call.id != stream.id;
        match result {
            HeldResult::Append(status) => {
                let append = self
                    .appends
                    .get_mut(&seq)
                    .expect("take_held checked the append");
                // The queue of the append is under the name that the call had.
                let key = (append.stream.store.clone(), append.stream.id.clone());
                adopt(&mut append.stream);
                if differs(&append.stream) {
                    return Err(nondeterminism!(
                        "Stream append {seq} writes Stream `{}` in Store `{}`, but history has an \
                         append to Stream `{}` in Store `{}`",
                        append.stream.id,
                        append.stream.store,
                        stream.id,
                        stream.store
                    ));
                }
                if append.resolved {
                    return Err(nondeterminism!(
                        "History has a second result of Stream append {seq}"
                    ));
                }
                append.resolved = true;
                append.spec = None;
                if let Some(queue) = self.append_queues.get_mut(&key) {
                    queue.retain(|queued| *queued != Queued::Append(seq));
                }
                Ok(Some(StreamJob::Append(ResolveStreamAppend {
                    seq,
                    status: Some(status),
                })))
            }
            HeldResult::CloseIntent(epoch) => {
                let close = self.held_close(seq, &stream)?;
                if !matches!(close.intent, Intent::Pending) {
                    return Err(nondeterminism!(
                        "History has a second intent of Stream close {seq}"
                    ));
                }
                close.intent = Intent::Recorded(epoch);
                // The close takes its place in its Stream's queue here, as on the Worker that
                // recorded it, so that it runs in front of every call of a later task. A result
                // later in History takes it out again.
                if self.calls_stores {
                    self.queue_close(seq);
                }
                Ok(None)
            }
            HeldResult::CloseResult(failed) => {
                let close = self.held_close(seq, &stream)?;
                if close.resolved || matches!(close.intent, Intent::Pending) {
                    return Err(nondeterminism!(
                        "History has a result of Stream close {seq} with no intent before it, \
                         or a second one"
                    ));
                }
                close.resolved = true;
                let key = (close.stream.store.clone(), close.stream.id.clone());
                if let Some(queue) = self.append_queues.get_mut(&key) {
                    queue.retain(|queued| *queued != Queued::Close(seq));
                }
                Ok(Some(StreamJob::Close(ResolveStreamClose { seq, failed })))
            }
        }
    }

    /// The close of a held entry. A close with no Store name takes the one of history, and one of
    /// another Stream is a non-determinism error.
    fn held_close(&mut self, seq: u32, stream: &StreamRef) -> Result<&mut Close> {
        let close = self
            .closes
            .get_mut(&seq)
            .expect("take_held checked the close");
        if close.stream.store.is_empty() {
            close.stream.store.clone_from(&stream.store);
        }
        if close.stream.store != stream.store || close.stream.id != stream.id {
            return Err(nondeterminism!(
                "Stream close {seq} closes Stream `{}` in Store `{}`, but history has a close of \
                 Stream `{}` in Store `{}`",
                close.stream.id,
                close.stream.store,
                stream.id,
                stream.store
            ));
        }
        Ok(close)
    }

    /// Fails when history has results of a marker that Workflow code never got. A replayed
    /// marker event comes after every activation of its Workflow Task.
    pub fn check_marker_consumed(&self) -> Result<()> {
        match self.held.first() {
            Some(held) => Err(nondeterminism!(
                "History has a Stream result of {} {} that Workflow code never got",
                match held.result {
                    HeldResult::Append(_) => "append",
                    HeldResult::CloseIntent(_) | HeldResult::CloseResult(_) => "close",
                },
                held.seq
            )),
            None => Ok(()),
        }
    }

    /// True if history has results that wait for their activation.
    pub fn has_held(&self) -> bool {
        !self.held.is_empty()
    }

    /// Clears what Workflow code got in the last Workflow Task, when the next one begins. A
    /// replayed task has no marker to write, and a live one wrote its marker already.
    pub fn wft_applied(&mut self) {
        self.marker_appends.clear();
        self.marker_closes.clear();
    }

    /// The results that appends and closes gave past the marker bound of the last task, as jobs of
    /// this task. They count against this task's bound too.
    pub fn release_carried(&mut self) -> Vec<StreamJob> {
        let mut jobs = vec![];
        // The results go in the order they came, so the first that doesn't fit keeps the rest.
        for job in std::mem::take(&mut self.carried_results) {
            match self.result_stream(&job) {
                Some(stream) => {
                    if let Some(job) = self.fit_result(job, &stream) {
                        jobs.push(job);
                    }
                }
                None => self.carried_results.push(job),
            }
        }
        jobs
    }

    /// The Stream of the call of an append or a close result.
    fn result_stream(&self, job: &StreamJob) -> Option<StreamRef> {
        match job {
            StreamJob::Append(result) => self.appends.get(&result.seq).map(|a| a.stream.clone()),
            StreamJob::Close(result) => self.closes.get(&result.seq).map(|c| c.stream.clone()),
        }
    }

    /// Gives the result of an append or a close as a job, or keeps it for the next task when the
    /// entry bound has no room for it, or when an earlier result waits already.
    fn fit_result(&mut self, job: StreamJob, stream: &StreamRef) -> Option<StreamJob> {
        if !self.carried_results.is_empty() || !self.entry_budget.add(result_bytes(&job), stream) {
            self.carried_results.push(job);
            return None;
        }
        Some(job)
    }

    /// True if results or intents wait for a later task, which the task must then force.
    pub fn has_carried(&self) -> bool {
        !self.carried_results.is_empty() || self.has_deferred_intents()
    }

    /// True once the entries of this task's marker reached their bound, so that the task completes
    /// and a new one takes the rest.
    pub fn marker_full(&self) -> bool {
        self.has_carried()
    }

    /// Stops every call. A terminal command does it, as does a failed Workflow Task, whose
    /// results must not reach Workflow code. It keeps the results that history holds, so that a
    /// replayed marker event still finds one that Workflow code never got.
    pub fn stop_all(&mut self) {
        for append in self.appends.values_mut().filter(|a| !a.resolved) {
            append.resolved = true;
            append.spec = None;
            self.requests.push(StreamRequest::Cancel(append.call_id));
        }
        self.carried_results.clear();
        // The task that ends the Workflow is the last one, so it records every intent, also past
        // the entry bound.
        for close in self.closes.values_mut() {
            if matches!(close.intent, Intent::Deferred) {
                close.intent = Intent::Pending;
            }
        }
        // A close stays. History has its intent, or the completion with the terminal command
        // carries it, and the Stream must close after it.
        let mut keys = vec![];
        for (key, queue) in self.append_queues.iter_mut() {
            queue.retain(|queued| matches!(queued, Queued::Close(_)));
            keys.push(key.clone());
        }
        self.append_queues.retain(|_, queue| !queue.is_empty());
        if self.live {
            for key in keys {
                self.dispatch_head(&key);
            }
        }
    }

    /// Drops the results that history holds. A failed Workflow Task evicts the run, and a forced
    /// failure of the Workflow must not meet them.
    pub fn drop_held(&mut self) {
        self.held.clear();
    }

    /// Takes the calls that the Worker must start or stop.
    pub fn drain_requests(&mut self) -> Vec<StreamRequest> {
        std::mem::take(&mut self.requests)
    }

    /// True if Workflow code ever started a Stream call in this run.
    pub fn has_calls(&self) -> bool {
        !self.appends.is_empty() || !self.closes.is_empty()
    }

    /// True if there are calls that the Worker must start or stop.
    pub fn has_requests(&self) -> bool {
        !self.requests.is_empty()
    }

    /// The writes that have no result: appends, and closes that wait in a queue or run, once
    /// replay ended. A close holds a Workflow Task after the one of its intent, as an append does.
    pub fn open_writes(&self) -> usize {
        if !self.live {
            return 0;
        }
        self.appends.values().filter(|a| !a.resolved).count()
            + self
                .closes
                .values()
                .filter(|close| close.queued && !close.resolved)
                .count()
    }

    /// Notes the Stream jobs of an activation, which Workflow code gets at `activation` in the
    /// Workflow Task. The marker of the task records them.
    pub fn note_delivered<'a>(
        &mut self,
        variants: impl IntoIterator<Item = &'a workflow_activation_job::Variant>,
        activation: u32,
    ) {
        for variant in variants {
            match variant {
                workflow_activation_job::Variant::ResolveStreamAppend(result) => {
                    let Some(append) = self.appends.get(&result.seq).filter(|a| !a.refused) else {
                        continue;
                    };
                    let mut entry = StreamAppendEntry {
                        seq: result.seq,
                        activation,
                        ..Default::default()
                    };
                    match &result.status {
                        Some(resolve_stream_append::Status::Appended(appended)) => {
                            entry.first_offset.clone_from(&appended.first_offset);
                            entry.last_offset.clone_from(&appended.last_offset);
                        }
                        Some(resolve_stream_append::Status::Failed(failure)) => {
                            entry.failure = Some(failure.clone());
                        }
                        None => {}
                    }
                    self.marker_appends.push((append.stream.clone(), entry));
                }
                workflow_activation_job::Variant::ResolveStreamClose(result) => {
                    let Some(close) = self.closes.get(&result.seq).filter(|c| !c.refused) else {
                        continue;
                    };
                    self.marker_closes.push((
                        close.stream.clone(),
                        StreamCloseEntry {
                            seq: result.seq,
                            resolved: true,
                            failure: result.failed.clone(),
                            activation,
                            ..Default::default()
                        },
                    ));
                }
                _ => {}
            }
        }
    }

    /// True if Workflow code got a Stream result in this Workflow Task, or asked for a close.
    pub fn has_marker_entries(&self) -> bool {
        !self.marker_appends.is_empty()
            || !self.marker_closes.is_empty()
            || self.has_pending_intents()
    }

    /// The marker of this Workflow Task, if Workflow code got a Stream result in it or asked for a
    /// close. `epoch` is the task, which a close of the task closes with. It clears the entries,
    /// since the next task records its own.
    pub fn take_marker(&mut self, epoch: Option<WorkflowTaskEpoch>) -> Option<StreamMarkerData> {
        self.entry_budget = EntryBudget::default();
        let marker = self.seal_marker(epoch);
        // The intents that didn't fit come first in the next task's marker.
        self.admit_deferred_intents();
        marker
    }

    fn seal_marker(&mut self, epoch: Option<WorkflowTaskEpoch>) -> Option<StreamMarkerData> {
        if !self.has_marker_entries() {
            return None;
        }
        let mut closes = std::mem::take(&mut self.marker_closes);
        if let Some(epoch) = epoch {
            let mut pending: Vec<_> = self
                .closes
                .iter_mut()
                .filter(|(_, close)| matches!(close.intent, Intent::Pending))
                .collect();
            pending.sort_by_key(|(seq, _)| **seq);
            for (seq, close) in pending {
                close.intent = Intent::Sealed(epoch);
                closes.push((
                    close.stream.clone(),
                    StreamCloseEntry {
                        seq: *seq,
                        intent_epoch: Some(epoch),
                        ..Default::default()
                    },
                ));
            }
        }
        let mut streams = vec![];
        let appends = std::mem::take(&mut self.marker_appends)
            .into_iter()
            .map(|(stream, entry)| StreamAppendEntry {
                stream_index: index_of(&mut streams, &stream),
                ..entry
            })
            .collect();
        let closes = closes
            .into_iter()
            .map(|(stream, entry)| StreamCloseEntry {
                stream_index: index_of(&mut streams, &stream),
                ..entry
            })
            .collect();
        Some(StreamMarkerData {
            appends,
            closes,
            streams,
        })
    }
}

/// The index of `stream` in the marker's table of Streams, which it joins if it isn't there. A
/// task calls few Streams, so a scan costs less than a map.
fn index_of(streams: &mut Vec<StreamRef>, stream: &StreamRef) -> u32 {
    let at = streams
        .iter()
        .position(|known| known == stream)
        .unwrap_or_else(|| {
            streams.push(stream.clone());
            streams.len() - 1
        });
    u32::try_from(at).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Reason, StreamError,
        ids::{MAX_OFFSET_BYTES, MAX_STREAM_ID_BYTES},
        workflow::manager::{MAX_FAILURE_MESSAGE_BYTES, MAX_STORE_NAME_BYTES, failure},
    };
    use prost_types::Timestamp;

    /// The size of `entry` as a field of the marker.
    fn field(entry: &impl Message) -> usize {
        FIELD_BYTES + entry.encoded_len()
    }

    /// The largest entry of each kind, with every input at its limit, is at most its estimate and
    /// far under the entry bound, so no single entry can pass the bound alone.
    #[test]
    fn the_largest_entries_are_far_under_the_entry_bound() {
        let far_under = MAX_MARKER_ENTRY_BYTES / 16;
        let offset = vec![0xff; MAX_OFFSET_BYTES];
        let wordy = StreamError::new(Reason::StorageUnavailable, "\u{20ac}".repeat(100_000));
        let failure = failure(&wordy);
        assert!(failure.message.len() <= MAX_FAILURE_MESSAGE_BYTES);

        let stream = StreamRef {
            id: "x".repeat(MAX_STREAM_ID_BYTES),
            store: "m".repeat(MAX_STORE_NAME_BYTES),
        };
        let mut budget = EntryBudget::default();
        assert!(budget.add(0, &stream));
        assert!(field(&stream) <= budget.bytes && budget.bytes < far_under);

        let appended = StreamAppendEntry {
            seq: u32::MAX,
            stream_index: u32::MAX,
            first_offset: offset.clone(),
            last_offset: offset.clone(),
            failure: None,
            activation: u32::MAX,
        };
        let appended_job = StreamJob::Append(ResolveStreamAppend {
            seq: u32::MAX,
            status: Some(resolve_stream_append::Status::Appended(StreamAppended {
                first_offset: offset.clone(),
                last_offset: offset.clone(),
            })),
        });
        assert!(field(&appended) <= result_bytes(&appended_job));
        let failed = StreamAppendEntry {
            first_offset: vec![],
            last_offset: vec![],
            failure: Some(failure.clone()),
            ..appended
        };
        let failed_job = StreamJob::Append(ResolveStreamAppend {
            seq: u32::MAX,
            status: Some(resolve_stream_append::Status::Failed(failure.clone())),
        });
        assert!(field(&failed) <= result_bytes(&failed_job));
        assert!(result_bytes(&failed_job) < far_under);

        let close = StreamCloseEntry {
            seq: u32::MAX,
            stream_index: u32::MAX,
            intent_epoch: None,
            resolved: true,
            failure: Some(failure.clone()),
            activation: u32::MAX,
        };
        let close_job = StreamJob::Close(ResolveStreamClose {
            seq: u32::MAX,
            failed: Some(failure),
        });
        assert!(field(&close) <= result_bytes(&close_job));
        assert!(result_bytes(&close_job) < far_under);
        let intent = StreamCloseEntry {
            seq: u32::MAX,
            stream_index: u32::MAX,
            intent_epoch: Some(WorkflowTaskEpoch {
                started_event_id: i64::MIN,
                attempt: i32::MIN,
                started_time: Some(Timestamp {
                    seconds: i64::MIN,
                    nanos: i32::MIN,
                }),
            }),
            ..Default::default()
        };
        assert!(field(&intent) <= CLOSE_INTENT_BYTES);
    }
}
