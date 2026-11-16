//! The Stream calls of a run, and the marker that records what Workflow code got in one Workflow
//! Task. A call has no command or event of its own, so it needs no state machine.

use crate::{
    telemetry::metrics::MetricsContext,
    worker::workflow::{
        OutgoingJob, WFMachinesError, fatal, nondeterminism,
        streams::{
            AppendSpec, CloseSpec, ReadFlow, ReadSpec, StreamCallResult, StreamRequest,
            StreamResolution, next_call_id, store_metrics,
        },
    },
};
use prost::Message;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_common::protos::{
    coresdk::{
        external_data::{
            StreamAppendEntry, StreamCloseEntry, StreamMarkerData, StreamReadEnded, StreamReadEntry,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, ResolveStreamRead, StreamAppended,
            StreamReadEnd, resolve_stream_append, workflow_activation_job,
        },
        workflow_commands::{AppendToStream, CloseStream, StartStreamRead},
    },
    temporal::api::{
        failure::v1::Failure,
        stream::v1::{Producer, StreamRecord, StreamRef, WorkflowTaskEpoch, producer::Kind},
    },
};
use temporalio_streams::digest::records_digest;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// What a result does to the activations of the run.
pub(super) enum StreamJob {
    /// Records of a read, and its end if it ended. They join the read's pending job, if it has one.
    Read(ResolveStreamRead),
    Append(ResolveStreamAppend),
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
}

struct Read {
    call_id: u64,
    seq: u32,
    stream: StreamRef,
    from_offset: Vec<u8>,
    records_in_history: bool,
    deadline: Option<SystemTime>,
    /// `None` for a read that set no idle timeout, which has no idle bound.
    idle_timeout: Option<Duration>,
    max_records: u32,
    max_bytes: u64,
    flow: Arc<ReadFlow>,
    /// The offset of the last Record that Workflow code got, or `from_offset`.
    last_offset: Vec<u8>,
    /// The Records, and their bytes, that Workflow code got.
    delivered: (u32, u64),
    /// False once the read ended or was closed.
    open: bool,
    /// True once the Worker runs the read. A replayed read takes its results from history.
    dispatched: bool,
}

impl Read {
    /// The read to run from where Workflow code is, with what is left of its caps.
    fn spec(&self) -> ReadSpec {
        let (records, bytes) = self.delivered;
        ReadSpec {
            call_id: self.call_id,
            seq: self.seq,
            stream: self.stream.clone(),
            from_offset: self.last_offset.clone(),
            deadline: self.deadline,
            idle_timeout: self.idle_timeout,
            records_left: (self.max_records > 0).then(|| self.max_records.saturating_sub(records)),
            bytes_left: (self.max_bytes > 0).then(|| self.max_bytes.saturating_sub(bytes)),
            took_a_record: records > 0,
            flow: self.flow.clone(),
        }
    }
}

struct Append {
    call_id: u64,
    stream: StreamRef,
    /// Taken when the append is dispatched.
    spec: Option<AppendSpec>,
    resolved: bool,
}

/// A result that history has for a call, which waits for its activation in a replay.
struct Held {
    seq: u32,
    activation: u32,
    stream: StreamRef,
    result: HeldResult,
}

enum HeldResult {
    Read {
        from_offset: Vec<u8>,
        page: ResolveStreamRead,
    },
    Append(resolve_stream_append::Status),
    /// The intent of a close, which gives no job.
    CloseIntent,
    CloseResult(Option<Failure>),
}

/// What one read gave Workflow code in the current Workflow Task.
struct ReadEntryBuilder {
    seq: u32,
    after_offset: Vec<u8>,
    records: Vec<StreamRecord>,
    page_counts: Vec<u32>,
    /// The activation of each page, by its index in the task.
    page_activations: Vec<u32>,
    end: Option<StreamReadEnded>,
}

#[derive(Default)]
pub(super) struct StreamCalls {
    reads: HashMap<u32, Read>,
    appends: HashMap<u32, Append>,
    closes: HashMap<u32, Close>,
    /// The appends and closes of each Stream that wait for the one before them, in call order.
    /// The first one is the one that runs.
    append_queues: HashMap<(String, String), VecDeque<Queued>>,
    requests: Vec<StreamRequest>,
    marker_reads: Vec<ReadEntryBuilder>,
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
}

impl StreamCalls {
    pub(super) fn new(calls_stores: bool) -> Self {
        Self {
            calls_stores,
            ..Default::default()
        }
    }

    pub(super) fn start_read(
        &mut self,
        attrs: StartStreamRead,
        workflow_time: Option<SystemTime>,
    ) -> Result<()> {
        let stream = attrs
            .stream
            .ok_or_else(|| fatal!("Stream read {} has no Stream", attrs.seq))?;
        if self.reads.contains_key(&attrs.seq) {
            return Err(fatal!("Stream read {} was started twice", attrs.seq));
        }
        let duration = |value: Option<prost_types::Duration>| {
            value
                .and_then(|value| Duration::try_from(value).ok())
                .filter(|value| !value.is_zero())
        };
        // The bound counts from the Workflow time of the start, which a replay has too, so no
        // restart can stretch it.
        let deadline = duration(attrs.timeout)
            .map(|timeout| workflow_time.unwrap_or_else(SystemTime::now) + timeout);
        let read = Read {
            call_id: next_call_id(),
            seq: attrs.seq,
            stream,
            last_offset: attrs.from_offset.clone(),
            from_offset: attrs.from_offset,
            records_in_history: attrs.records_in_history,
            deadline,
            // An unset idle timeout is no bound, and a zero one, or a negative one, doesn't wait.
            idle_timeout: attrs
                .idle_timeout
                .map(|idle| Duration::try_from(idle).unwrap_or_default()),
            max_records: attrs.max_records,
            max_bytes: attrs.max_bytes,
            flow: Arc::new(ReadFlow::default()),
            delivered: (0, 0),
            open: true,
            dispatched: self.live,
        };
        if self.live {
            self.requests.push(StreamRequest::Read(read.spec()));
        }
        self.reads.insert(attrs.seq, read);
        Ok(())
    }

    /// True once replay ended, so that the Worker runs new calls.
    pub(super) fn is_live(&self) -> bool {
        self.live
    }

    /// Runs the calls that replay left open, from where Workflow code is. Returns the Stores that
    /// they use, so that the caller can check that the Worker has their runtimes.
    pub(super) fn go_live(&mut self) -> Vec<String> {
        if self.live || !self.calls_stores {
            return vec![];
        }
        self.live = true;
        let mut stores = vec![];
        let mut reads: Vec<_> = self
            .reads
            .values_mut()
            .filter(|read| read.open && !read.dispatched)
            .collect();
        reads.sort_by_key(|read| read.seq);
        for read in reads {
            read.dispatched = true;
            read.call_id = next_call_id();
            stores.push(read.stream.store.clone());
            self.requests.push(StreamRequest::Read(read.spec()));
        }
        // A close that history recorded with no result is sent again, with the task in hand.
        let mut resent: Vec<_> = self
            .closes
            .iter()
            .filter(|(_, close)| {
                matches!(close.intent, Intent::Recorded(_)) && !close.queued && !close.resolved
            })
            .map(|(seq, _)| *seq)
            .collect();
        resent.sort();
        for seq in resent {
            if let Some(close) = self.closes.get_mut(&seq) {
                close.intent = Intent::Recorded(None);
            }
            self.queue_close(seq);
        }
        let mut keys: Vec<_> = self.append_queues.keys().cloned().collect();
        keys.sort();
        for key in keys {
            if let Some(store) = self.dispatch_head(&key) {
                stores.push(store);
            }
        }
        stores
    }

    /// Records a close, which the Worker runs only after History has this task.
    pub(super) fn close(&mut self, attrs: CloseStream) -> Result<()> {
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
        self.closes.insert(
            seq,
            Close {
                call_id: next_call_id(),
                stream,
                producer,
                intent: Intent::Pending,
                queued: false,
                dispatched: false,
                resolved: false,
            },
        );
        Ok(())
    }

    /// True if Workflow code asked for a close that no completion carries. The task completes
    /// at once then, so that the Stream closes as soon as History has the intent.
    pub(super) fn has_pending_intents(&self) -> bool {
        self.closes
            .values()
            .any(|close| matches!(close.intent, Intent::Pending))
    }

    /// The server took the completion that carries the sealed intents, so History has them. Each
    /// close waits in its Stream's queue, in front of every append of a later task. Returns the
    /// closes that run at once. A replay Worker runs none.
    pub(super) fn intents_taken(&mut self) -> Vec<StreamRequest> {
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

    /// Drops the intents that History doesn't have, when their task fails.
    pub(super) fn drop_intents(&mut self) {
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
        self.dispatch_head(&key);
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

    /// Stops a read. Returns true if it was still open, so that its pending Records are dropped.
    pub(super) fn close_read(&mut self, seq: u32) -> Result<bool> {
        let read = self
            .reads
            .get_mut(&seq)
            .ok_or_else(|| fatal!("Workflow code closed Stream read {seq}, which never started"))?;
        if !read.open {
            return Ok(false);
        }
        read.open = false;
        if read.dispatched {
            self.requests.push(StreamRequest::Cancel(read.call_id));
        }
        Ok(true)
    }

    pub(super) fn append(&mut self, attrs: AppendToStream) -> Result<()> {
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
            },
        );
        if runs_now && self.live {
            self.dispatch_head(&(stream_key.0, stream_key.1));
        }
        Ok(())
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
    /// from an older dispatch.
    pub(super) fn resolve(&mut self, resolution: StreamResolution) -> Option<StreamJob> {
        let StreamResolution {
            call_id,
            seq,
            result,
        } = resolution;
        match result {
            StreamCallResult::Page { records, end } => {
                let read = self.reads.get_mut(&seq)?;
                if read.call_id != call_id || !read.open {
                    return None;
                }
                if end.is_some() {
                    read.open = false;
                }
                Some(StreamJob::Read(ResolveStreamRead { seq, records, end }))
            }
            StreamCallResult::Append(status) => {
                let append = self.appends.get_mut(&seq)?;
                if append.call_id != call_id || append.resolved {
                    return None;
                }
                append.resolved = true;
                let key = (append.stream.store.clone(), append.stream.id.clone());
                self.unqueue(&key, Queued::Append(seq));
                Some(StreamJob::Append(ResolveStreamAppend {
                    seq,
                    status: Some(status),
                }))
            }
            StreamCallResult::Close(failed) => {
                let close = self.closes.get_mut(&seq)?;
                if close.call_id != call_id || close.resolved {
                    return None;
                }
                close.resolved = true;
                let key = (close.stream.store.clone(), close.stream.id.clone());
                self.unqueue(&key, Queued::Close(seq));
                Some(StreamJob::Close(ResolveStreamClose { seq, failed }))
            }
        }
    }

    /// Holds the results of a marker of history, each for its activation in the Workflow Task.
    pub(super) fn insert_peeked(&mut self, data: StreamMarkerData) -> Result<()> {
        let streams = data.streams;
        let stream_of = |kind: &str, seq: u32, index: u32| {
            streams.get(index as usize).cloned().ok_or_else(|| {
                fatal!(
                    "The Stream marker entry of {kind} {seq} has Stream {index}, which the marker \
                     doesn't have"
                )
            })
        };
        for entry in data.reads {
            let stream = stream_of("read", entry.seq, entry.stream_index)?;
            let seq = entry.seq;
            let bad = |what: &str| fatal!("The Stream marker entry of read {seq} is bad: {what}");
            // A run that was cached when its history came, and left the cache before it replayed
            // it, gets markers with no Records read. Giving no Records would be a wrong replay.
            if entry.count > 0 && entry.records.is_empty() {
                return Err(WFMachinesError::Stream(format!(
                    "StreamReplayError: Stream `{}` in Store `{}`: the Records of the marker of \
                     read {seq} were not read",
                    stream.id, stream.store
                )));
            }
            if entry
                .page_counts
                .iter()
                .map(|count| *count as usize)
                .sum::<usize>()
                != entry.records.len()
                || entry.page_counts.len() != entry.page_activations.len()
            {
                return Err(bad("its pages don't match its Records"));
            }
            let mut records = entry.records.into_iter();
            let mut activation = 0u32;
            let mut pages = vec![];
            for (at, (count, gap)) in entry
                .page_counts
                .iter()
                .zip(entry.page_activations.iter())
                .enumerate()
            {
                activation = if at == 0 { *gap } else { activation + *gap };
                let page: Vec<_> = records.by_ref().take(*count as usize).collect();
                pages.push((activation, page));
            }
            let end = entry.end.map(|end| {
                (
                    end.activation,
                    StreamReadEnd {
                        last_offset: end.last_offset,
                        stop: end.stop,
                        done: end.done,
                        failure: end.failure,
                    },
                )
            });
            let mut end = end;
            let last = pages.len().checked_sub(1);
            for (at, (activation, records)) in pages.into_iter().enumerate() {
                // An end in the activation of the last page came in the same job.
                let with_end = if Some(at) == last
                    && end.as_ref().is_some_and(|(ended, _)| *ended == activation)
                {
                    end.take().map(|(_, end)| end)
                } else {
                    None
                };
                self.held.push(Held {
                    seq,
                    activation,
                    stream: stream.clone(),
                    result: HeldResult::Read {
                        from_offset: entry.from_offset.clone(),
                        page: ResolveStreamRead {
                            seq,
                            records,
                            end: with_end,
                        },
                    },
                });
            }
            if let Some((activation, end)) = end {
                self.held.push(Held {
                    seq,
                    activation,
                    stream: stream.clone(),
                    result: HeldResult::Read {
                        from_offset: entry.from_offset.clone(),
                        page: ResolveStreamRead {
                            seq,
                            records: vec![],
                            end: Some(end),
                        },
                    },
                });
            }
        }
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
                (0, HeldResult::CloseIntent)
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
    pub(super) fn take_held(&mut self, current: u32, include_held: bool) -> Result<Vec<StreamJob>> {
        let mut jobs = vec![];
        let mut at = 0;
        while at < self.held.len() {
            let held = &self.held[at];
            let started = match held.result {
                HeldResult::Read { .. } => self.reads.contains_key(&held.seq),
                HeldResult::Append(_) => self.appends.contains_key(&held.seq),
                HeldResult::CloseIntent | HeldResult::CloseResult(_) => {
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
            HeldResult::Read { from_offset, page } => {
                let read = self
                    .reads
                    .get_mut(&seq)
                    .expect("take_held checked the read");
                adopt(&mut read.stream);
                if differs(&read.stream) || read.from_offset != from_offset {
                    return Err(nondeterminism!(
                        "Stream read {seq} reads Stream `{}` in Store `{}`, but history has a \
                         read of Stream `{}` in Store `{}`, or from another offset",
                        read.stream.id,
                        read.stream.store,
                        stream.id,
                        stream.store
                    ));
                }
                if !read.open {
                    return Err(nondeterminism!(
                        "History has a result of Stream read {seq} after the read ended"
                    ));
                }
                if page.end.is_some() {
                    read.open = false;
                }
                Ok(Some(StreamJob::Read(page)))
            }
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
            HeldResult::CloseIntent => {
                let close = self.held_close(seq, &stream)?;
                if !matches!(close.intent, Intent::Pending) {
                    return Err(nondeterminism!(
                        "History has a second intent of Stream close {seq}"
                    ));
                }
                close.intent = Intent::Recorded(None);
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
    pub(super) fn check_marker_consumed(&self) -> Result<()> {
        match self.held.first() {
            Some(held) => Err(nondeterminism!(
                "History has a Stream result of {} {} that Workflow code never got",
                match held.result {
                    HeldResult::Read { .. } => "read",
                    HeldResult::Append(_) => "append",
                    HeldResult::CloseIntent | HeldResult::CloseResult(_) => "close",
                },
                held.seq
            )),
            None => Ok(()),
        }
    }

    /// True if history has results that wait for their activation.
    pub(super) fn has_held(&self) -> bool {
        !self.held.is_empty()
    }

    /// Clears what Workflow code got in the last Workflow Task, when the next one begins. A
    /// replayed task has no marker to write, and a live one wrote its marker already.
    pub(super) fn wft_applied(&mut self) {
        self.marker_reads.clear();
        self.marker_appends.clear();
        self.marker_closes.clear();
    }

    /// Stops every call. A terminal command does it, as does a failed Workflow Task, whose
    /// results must not reach Workflow code. It keeps the results that history holds, so that a
    /// replayed marker event still finds one that Workflow code never got.
    pub(super) fn stop_all(&mut self) {
        for read in self.reads.values_mut().filter(|read| read.open) {
            read.open = false;
            self.requests.push(StreamRequest::Cancel(read.call_id));
        }
        for append in self.appends.values_mut().filter(|a| !a.resolved) {
            append.resolved = true;
            append.spec = None;
            self.requests.push(StreamRequest::Cancel(append.call_id));
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
    pub(super) fn drop_held(&mut self) {
        self.held.clear();
    }

    pub(super) fn drain_requests(&mut self) -> Vec<StreamRequest> {
        std::mem::take(&mut self.requests)
    }

    pub(super) fn has_requests(&self) -> bool {
        !self.requests.is_empty()
    }

    /// The reads that the Worker runs. A replayed read that waits for history holds no task.
    pub(super) fn open_reads(&self) -> usize {
        self.reads
            .values()
            .filter(|read| read.open && read.dispatched)
            .count()
    }

    /// The appends that have no result, and the closes that wait in a queue or run, once replay
    /// ended. A close holds a Workflow Task after the one of its intent, as an append does.
    pub(super) fn open_appends(&self) -> usize {
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
    /// Workflow Task. The marker of the task records them. `metrics` are the run's.
    pub(super) fn note_delivered(
        &mut self,
        jobs: &[OutgoingJob],
        activation: u32,
        metrics: &MetricsContext,
    ) {
        for job in jobs {
            match &job.variant {
                workflow_activation_job::Variant::ResolveStreamRead(page) => {
                    self.note_page(page, activation, metrics);
                }
                workflow_activation_job::Variant::ResolveStreamAppend(result) => {
                    let Some(append) = self.appends.get(&result.seq) else {
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
                    let Some(close) = self.closes.get(&result.seq) else {
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

    fn note_page(&mut self, page: &ResolveStreamRead, activation: u32, metrics: &MetricsContext) {
        let Some(read) = self.reads.get_mut(&page.seq) else {
            return;
        };
        // Only a read that the Worker runs got its Records from the Store. A replayed one got
        // them from History, and a replay counts nothing.
        if read.dispatched
            && !page.records.is_empty()
            && let Some(metrics) = store_metrics(metrics, &read.stream.store)
        {
            metrics.stream_read_records(page.records.len() as u64);
        }
        let at = match self.marker_reads.iter().position(|e| e.seq == page.seq) {
            Some(at) => at,
            None => {
                self.marker_reads.push(ReadEntryBuilder {
                    seq: page.seq,
                    after_offset: read.last_offset.clone(),
                    records: vec![],
                    page_counts: vec![],
                    page_activations: vec![],
                    end: None,
                });
                self.marker_reads.len() - 1
            }
        };
        let entry = &mut self.marker_reads[at];
        if let Some(last) = page.records.last() {
            entry.page_counts.push(page.records.len() as u32);
            entry.page_activations.push(activation);
            entry.records.extend(page.records.iter().cloned());
            read.last_offset.clone_from(&last.offset);
            let bytes: usize = page.records.iter().map(Message::encoded_len).sum();
            read.delivered.0 += page.records.len() as u32;
            read.delivered.1 += bytes as u64;
            read.flow.delivered(bytes);
        }
        if let Some(StreamReadEnd {
            last_offset,
            stop,
            done,
            failure,
        }) = &page.end
        {
            entry.end = Some(StreamReadEnded {
                last_offset: last_offset.clone(),
                stop: *stop,
                done: *done,
                failure: failure.clone(),
                activation,
            });
        }
    }

    /// True if Workflow code got a Stream result in this Workflow Task, or asked for a close.
    pub(super) fn has_marker_entries(&self) -> bool {
        !self.marker_reads.is_empty()
            || !self.marker_appends.is_empty()
            || !self.marker_closes.is_empty()
            || self.has_pending_intents()
    }

    /// The marker of this Workflow Task, if Workflow code got a Stream result in it or asked for a
    /// close. `epoch` is the task, which a close of the task closes with. It clears the entries,
    /// since the next task records its own.
    pub(super) fn take_marker(
        &mut self,
        epoch: Option<WorkflowTaskEpoch>,
    ) -> Option<StreamMarkerData> {
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
        let reads = std::mem::take(&mut self.marker_reads)
            .into_iter()
            .filter_map(|entry| {
                let read = self.reads.get(&entry.seq)?;
                Some(read_entry(
                    read,
                    entry,
                    index_of(&mut streams, &read.stream),
                ))
            })
            .collect();
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
            reads,
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

fn read_entry(read: &Read, entry: ReadEntryBuilder, stream_index: u32) -> StreamReadEntry {
    let count = entry.records.len() as u32;
    let last_offset = entry
        .records
        .last()
        .map(|record| record.offset.clone())
        .unwrap_or_default();
    let (digest, records) = if read.records_in_history {
        (vec![], entry.records)
    } else if count > 0 {
        (records_digest(&entry.records).to_vec(), vec![])
    } else {
        (vec![], vec![])
    };
    let mut before = 0;
    let page_activations = entry
        .page_activations
        .into_iter()
        .enumerate()
        .map(|(at, activation)| {
            let coded = if at == 0 {
                activation
            } else {
                activation - before
            };
            before = activation;
            coded
        })
        .collect();
    StreamReadEntry {
        seq: entry.seq,
        stream_index,
        from_offset: read.from_offset.clone(),
        after_offset: entry.after_offset,
        last_offset,
        count,
        digest,
        records,
        page_counts: entry.page_counts,
        page_activations,
        end: entry.end,
    }
}
