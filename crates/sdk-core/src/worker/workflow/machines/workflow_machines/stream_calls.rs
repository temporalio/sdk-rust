//! The Stream calls of a run, and the marker that records what Workflow code got in one Workflow
//! Task. A call has no command or event of its own, so it needs no state machine.

use crate::{
    telemetry::metrics::MetricsContext,
    worker::workflow::{
        OutgoingJob, WFMachinesError, fatal, nondeterminism,
        streams::{
            AppendSpec, CloseSpec, DEFAULT_IDLE_TIMEOUT, MAX_MARKER_ENTRY_BYTES,
            MAX_MARKER_RECORD_BYTES, ReadFlow, ReadSpec, STREAM_PAGE_MAX_RECORDS, StreamCallResult,
            StreamRequest, StreamResolution, input_over_limit, next_call_id, store_metrics,
        },
    },
};
use prost::Message;
use std::{
    collections::{HashMap, HashSet, VecDeque},
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
use temporalio_streams::{
    digest::{DIGEST_LEN, records_digest},
    ids::MAX_OFFSET_BYTES,
};

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

struct Read {
    call_id: u64,
    seq: u32,
    stream: StreamRef,
    from_offset: Vec<u8>,
    records_in_history: bool,
    deadline: Option<SystemTime>,
    /// [DEFAULT_IDLE_TIMEOUT] for a read that set none.
    idle_timeout: Duration,
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
    /// Records, and the end, that the read gave past this task's marker bound. The next task
    /// takes them.
    carried: Option<ResolveStreamRead>,
    /// Records, and the end, past [STREAM_PAGE_MAX_RECORDS] in the read's next activation. The
    /// activation after it takes them.
    over_cap: Option<ResolveStreamRead>,
    /// True for a read that Core refused for an input over its limit. The marker has nothing of it.
    refused: bool,
}

/// Takes as much of a live `page` as the read's next activation has room for, when its pending
/// job holds `pending` Records, and keeps the rest in `read` for the activation after it. Records
/// come in order, so a page joins the ones that wait already.
fn fit_in_activation(
    read: &mut Read,
    page: ResolveStreamRead,
    pending: usize,
) -> Option<ResolveStreamRead> {
    if let Some(over_cap) = read.over_cap.as_mut() {
        over_cap.records.extend(page.records);
        if page.end.is_some() {
            over_cap.end = page.end;
        }
        return None;
    }
    let room = STREAM_PAGE_MAX_RECORDS.saturating_sub(pending);
    let ResolveStreamRead {
        seq,
        mut records,
        end,
    } = page;
    if records.len() <= room {
        return Some(ResolveStreamRead { seq, records, end });
    }
    let rest = records.split_off(room);
    read.over_cap = Some(ResolveStreamRead {
        seq,
        records: rest,
        end,
    });
    (!records.is_empty()).then_some(ResolveStreamRead {
        seq,
        records,
        end: None,
    })
}

/// Takes as much of `page` as the task's marker has room for, and keeps the rest in `read` for the
/// next task. `used` is what the task's marker holds already. The first Record of a task always
/// comes, also when it is larger than the bound alone.
fn fit_in_marker(
    read: &mut Read,
    page: ResolveStreamRead,
    used: &mut usize,
) -> Option<ResolveStreamRead> {
    let ResolveStreamRead { seq, records, end } = page;
    if let Some(carried) = read.carried.as_mut() {
        carried.records.extend(records);
        carried.end = end;
        return None;
    }
    let mut records = records.into_iter();
    let mut taken = vec![];
    while let Some(record) = records.next() {
        let size = record.encoded_len();
        if *used > 0 && *used + size > MAX_MARKER_RECORD_BYTES {
            let mut rest = vec![record];
            rest.extend(records);
            read.carried = Some(ResolveStreamRead {
                seq,
                records: rest,
                end,
            });
            return (!taken.is_empty()).then_some(ResolveStreamRead {
                seq,
                records: taken,
                end: None,
            });
        }
        *used += size;
        taken.push(record);
    }
    Some(ResolveStreamRead {
        seq,
        records: taken,
        end,
    })
}

/// The tag and the length of a field of bytes or of a message of less than 2 MiB.
const FIELD_BYTES: usize = 4;
/// A field of a varint of 32 bits, with its tag.
const VARINT_BYTES: usize = 6;
/// A field of a varint of 64 bits, with its tag.
const VARINT64_BYTES: usize = 11;
/// A read entry with no page and no end: its seq, Stream, count, digest, the headers of its
/// offsets and of its two lists of pages, and a `last_offset` at its limit. The two other offsets
/// count at their own size.
const READ_ENTRY_BYTES: usize = FIELD_BYTES
    + 3 * VARINT_BYTES
    + 3 * FIELD_BYTES
    + MAX_OFFSET_BYTES
    + FIELD_BYTES
    + DIGEST_LEN
    + 2 * FIELD_BYTES;
/// One page of a read: its count and its activation, in the two packed lists.
const PAGE_BYTES: usize = 2 * (VARINT_BYTES - 1);
/// The end of a read with no failure, apart from its `last_offset`.
const END_BYTES: usize = 2 * FIELD_BYTES + 3 * VARINT_BYTES;
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

/// What this task's marker holds apart from Records, as an estimate that is never below its
/// encoded size. A result counts when it becomes a job, before Workflow code gets it, so that a
/// result that doesn't fit can wait for the next task.
#[derive(Default)]
struct EntryBudget {
    bytes: usize,
    /// The Streams of the marker's table.
    streams: HashSet<(String, String)>,
    /// The reads that have an entry in the marker.
    reads: HashSet<u32>,
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

    /// Adds a page of `read`. `merges` is true when the page joins the read's pending job, which
    /// is one page of the marker already.
    fn add_page(&mut self, read: &Read, page: &ResolveStreamRead, merges: bool) -> bool {
        let new_entry = !self.reads.contains(&page.seq);
        let mut bytes = 0;
        if new_entry {
            // The entry's `after_offset` is where Workflow code is when it gets this page.
            bytes += READ_ENTRY_BYTES + read.from_offset.len() + read.last_offset.len();
        }
        if !merges && !page.records.is_empty() {
            bytes += PAGE_BYTES;
        }
        if let Some(end) = &page.end {
            bytes += END_BYTES + end.last_offset.len() + failure_bytes(end.failure.as_ref());
        }
        if !self.add(bytes, &read.stream) {
            return false;
        }
        self.reads.insert(page.seq);
        true
    }
}

/// Fits a live page of `read` in the Records bound and the entry bound of the task's marker, and in
/// the page cap of the next activation, when the read's pending job holds `pending` Records.
/// Returns the job, or `None` when the page waits for a later activation or task.
fn fit_page(
    read: &mut Read,
    page: ResolveStreamRead,
    pending: usize,
    record_bytes: &mut usize,
    budget: &mut EntryBudget,
) -> Option<ResolveStreamRead> {
    let page = if read.records_in_history {
        fit_in_marker(read, page, record_bytes)?
    } else {
        page
    };
    let page = fit_in_activation(read, page, pending)?;
    fit_entry(read, page, pending > 0, budget)
}

/// Fits a page that becomes a job in the entry bound of the task's marker, or keeps it for the
/// next task. `merges` is true when the page joins the read's pending job.
fn fit_entry(
    read: &mut Read,
    page: ResolveStreamRead,
    merges: bool,
    budget: &mut EntryBudget,
) -> Option<ResolveStreamRead> {
    if !budget.add_page(read, &page, merges) {
        carry_front(read, page);
        return None;
    }
    if page.end.is_some() {
        read.open = false;
    }
    Some(page)
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
        // A page counts by its read, with [EntryBudget::add_page].
        StreamJob::Read(_) => 0,
    }
}

/// Keeps `page` for the next task, in front of the Records that wait already for a later
/// activation or task. Only the last of them can have the end.
fn carry_front(read: &mut Read, page: ResolveStreamRead) {
    let ResolveStreamRead {
        seq,
        mut records,
        mut end,
    } = page;
    for later in [read.over_cap.take(), read.carried.take()]
        .into_iter()
        .flatten()
    {
        records.extend(later.records);
        end = later.end.or(end);
    }
    read.carried = Some(ResolveStreamRead { seq, records, end });
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
    Read {
        from_offset: Vec<u8>,
        page: ResolveStreamRead,
    },
    Append(resolve_stream_append::Status),
    /// The intent of a close, with the epoch of its task, which gives no job.
    CloseIntent(Option<WorkflowTaskEpoch>),
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
    /// The bytes of Records that this task's marker holds for reads with their Records in
    /// History, counted when a page becomes a job.
    marker_record_bytes: usize,
    /// What this task's marker holds apart from Records.
    entry_budget: EntryBudget,
    /// The results of appends and closes past this task's entry bound, in the order they came.
    /// The next task takes them.
    carried_results: Vec<StreamJob>,
}

impl StreamCalls {
    pub(super) fn new(calls_stores: bool) -> Self {
        Self {
            calls_stores,
            ..Default::default()
        }
    }

    /// Starts a read. Returns the job of its refusal, for a read with an input over its limit.
    pub(super) fn start_read(
        &mut self,
        attrs: StartStreamRead,
        workflow_time: Option<SystemTime>,
    ) -> Result<Option<StreamJob>> {
        let stream = attrs
            .stream
            .ok_or_else(|| fatal!("Stream read {} has no Stream", attrs.seq))?;
        if self.reads.contains_key(&attrs.seq) {
            return Err(fatal!("Stream read {} was started twice", attrs.seq));
        }
        // A set timeout must bound the read. Lang leaves it unset for no bound.
        let timeout = attrs
            .timeout
            .map(|timeout| Duration::try_from(timeout).unwrap_or_default());
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(fatal!(
                "Stream read {} has a timeout of zero or less. Leave it unset for no bound",
                attrs.seq
            ));
        }
        // The bound counts from the Workflow time of the start, which a replay has too, so no
        // restart can stretch it.
        let deadline =
            timeout.map(|timeout| workflow_time.unwrap_or_else(SystemTime::now) + timeout);
        let refusal = input_over_limit(&stream, &attrs.from_offset);
        let refused = refusal.is_some();
        let read = Read {
            call_id: next_call_id(),
            seq: attrs.seq,
            stream,
            last_offset: attrs.from_offset.clone(),
            from_offset: attrs.from_offset,
            records_in_history: attrs.records_in_history,
            deadline,
            // A zero idle timeout, or a negative one, doesn't wait.
            idle_timeout: attrs.idle_timeout.map_or(DEFAULT_IDLE_TIMEOUT, |idle| {
                Duration::try_from(idle).unwrap_or_default()
            }),
            max_records: attrs.max_records,
            max_bytes: attrs.max_bytes,
            flow: Arc::new(ReadFlow::default()),
            delivered: (0, 0),
            open: !refused,
            dispatched: self.live && !refused,
            carried: None,
            over_cap: None,
            refused,
        };
        // A replay refuses the same input again, so the refusal needs no marker entry.
        let job = refusal.map(|failure| {
            StreamJob::Read(ResolveStreamRead {
                seq: attrs.seq,
                records: vec![],
                end: Some(StreamReadEnd {
                    last_offset: read.from_offset.clone(),
                    failure: Some(failure),
                    ..Default::default()
                }),
            })
        });
        if read.dispatched {
            self.requests.push(StreamRequest::Read(read.spec()));
        }
        self.reads.insert(attrs.seq, read);
        Ok(job)
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
        // The intents that replay left without a record go in the marker of this live task.
        for close in self.closes.values_mut() {
            if matches!(close.intent, Intent::Pending) {
                close.intent = Intent::Deferred;
            }
        }
        self.admit_deferred_intents();
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
    pub(super) fn close(&mut self, attrs: CloseStream) -> Result<Option<StreamJob>> {
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
        let refusal = input_over_limit(&stream, &[]);
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
        read.carried = None;
        read.over_cap = None;
        if read.dispatched {
            self.requests.push(StreamRequest::Cancel(read.call_id));
        }
        Ok(true)
    }

    /// Starts an append. Returns the job of its refusal, for an append with an input over its
    /// limit.
    pub(super) fn append(&mut self, attrs: AppendToStream) -> Result<Option<StreamJob>> {
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
        if let Some(failure) = input_over_limit(&stream, &[]) {
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
    /// from an older dispatch, or when its Records wait for a later activation. `pending` gives
    /// the Records in the pending job of a read.
    pub(super) fn resolve(
        &mut self,
        resolution: StreamResolution,
        pending: impl FnOnce(u32) -> usize,
    ) -> Option<StreamJob> {
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
                let page = ResolveStreamRead { seq, records, end };
                if read.carried.is_some() {
                    carry_front(read, page);
                    return None;
                }
                let page = fit_page(
                    read,
                    page,
                    pending(seq),
                    &mut self.marker_record_bytes,
                    &mut self.entry_budget,
                )?;
                Some(StreamJob::Read(page))
            }
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
    pub(super) fn take_held(&mut self, current: u32, include_held: bool) -> Result<Vec<StreamJob>> {
        let mut jobs = vec![];
        let mut at = 0;
        while at < self.held.len() {
            let held = &self.held[at];
            let started = match held.result {
                HeldResult::Read { .. } => self.reads.contains_key(&held.seq),
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
    pub(super) fn check_marker_consumed(&self) -> Result<()> {
        match self.held.first() {
            Some(held) => Err(nondeterminism!(
                "History has a Stream result of {} {} that Workflow code never got",
                match held.result {
                    HeldResult::Read { .. } => "read",
                    HeldResult::Append(_) => "append",
                    HeldResult::CloseIntent(_) | HeldResult::CloseResult(_) => "close",
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

    /// The pages that reads gave past the marker bound of the last task, as jobs of this task.
    /// They count against this task's bound too, and against the page cap. `pending` gives the
    /// Records in the pending job of a read.
    pub(super) fn release_carried(&mut self, pending: impl Fn(u32) -> usize) -> Vec<StreamJob> {
        let mut seqs: Vec<_> = self
            .reads
            .iter()
            .filter(|(_, read)| read.carried.is_some())
            .map(|(seq, _)| *seq)
            .collect();
        seqs.sort();
        let mut jobs = vec![];
        for seq in seqs {
            let Some(read) = self.reads.get_mut(&seq) else {
                continue;
            };
            let Some(page) = read.carried.take() else {
                continue;
            };
            if let Some(page) = fit_page(
                read,
                page,
                pending(seq),
                &mut self.marker_record_bytes,
                &mut self.entry_budget,
            ) {
                jobs.push(StreamJob::Read(page));
            }
        }
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
            StreamJob::Read(_) => None,
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
    pub(super) fn has_carried(&self) -> bool {
        !self.carried_results.is_empty()
            || self.reads.values().any(|read| read.carried.is_some())
            || self.has_deferred_intents()
    }

    /// The Records that reads gave past the page cap, as jobs of the next activation. Called once
    /// an activation took the pending jobs, so that the next one comes with no wait, also when the
    /// Store has nothing new.
    pub(super) fn release_over_cap(&mut self) -> Vec<StreamJob> {
        let mut seqs: Vec<_> = self
            .reads
            .iter()
            .filter(|(_, read)| read.over_cap.is_some())
            .map(|(seq, _)| *seq)
            .collect();
        seqs.sort();
        let mut jobs = vec![];
        for seq in seqs {
            let Some(read) = self.reads.get_mut(&seq) else {
                continue;
            };
            let Some(page) = read.over_cap.take() else {
                continue;
            };
            if let Some(page) = fit_in_activation(read, page, 0)
                && let Some(page) = fit_entry(read, page, false, &mut self.entry_budget)
            {
                jobs.push(StreamJob::Read(page));
            }
        }
        jobs
    }

    /// True once the Records or the entries of this task's marker reached their bound, so that the
    /// task completes and a new one takes the rest.
    pub(super) fn marker_full(&self) -> bool {
        self.marker_record_bytes >= MAX_MARKER_RECORD_BYTES || self.has_carried()
    }

    /// Stops every call. A terminal command does it, as does a failed Workflow Task, whose
    /// results must not reach Workflow code. It keeps the results that history holds, so that a
    /// replayed marker event still finds one that Workflow code never got.
    pub(super) fn stop_all(&mut self) {
        for read in self.reads.values_mut().filter(|read| read.open) {
            read.open = false;
            read.carried = None;
            read.over_cap = None;
            self.requests.push(StreamRequest::Cancel(read.call_id));
        }
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
    pub(super) fn drop_held(&mut self) {
        self.held.clear();
    }

    pub(super) fn drain_requests(&mut self) -> Vec<StreamRequest> {
        std::mem::take(&mut self.requests)
    }

    /// True if Workflow code ever started a Stream call in this run.
    pub(super) fn has_calls(&self) -> bool {
        !self.reads.is_empty() || !self.appends.is_empty() || !self.closes.is_empty()
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

    /// The writes that have no result: appends, and closes that wait in a queue or run, once
    /// replay ended. A close holds a Workflow Task after the one of its intent, as an append does.
    pub(super) fn open_writes(&self) -> usize {
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

    fn note_page(&mut self, page: &ResolveStreamRead, activation: u32, metrics: &MetricsContext) {
        let Some(read) = self.reads.get_mut(&page.seq).filter(|read| !read.refused) else {
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
        self.marker_record_bytes = 0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::workflow::streams::{
        MAX_FAILURE_MESSAGE_BYTES, MAX_STORE_NAME_BYTES, failure,
    };
    use prost_types::Timestamp;
    use temporalio_common::protos::coresdk::common::StreamReadStop;
    use temporalio_streams::{Reason, StreamError, ids::MAX_STREAM_ID_BYTES};

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

        let read = StreamReadEntry {
            seq: u32::MAX,
            stream_index: u32::MAX,
            from_offset: offset.clone(),
            after_offset: offset.clone(),
            last_offset: offset.clone(),
            count: u32::MAX,
            digest: vec![0xff; DIGEST_LEN],
            records: vec![],
            page_counts: vec![u32::MAX],
            page_activations: vec![u32::MAX],
            end: Some(StreamReadEnded {
                last_offset: offset.clone(),
                stop: StreamReadStop::Timeout as i32,
                done: true,
                failure: Some(failure.clone()),
                activation: u32::MAX,
            }),
        };
        let read_estimate = READ_ENTRY_BYTES
            + 2 * MAX_OFFSET_BYTES
            + PAGE_BYTES
            + END_BYTES
            + MAX_OFFSET_BYTES
            + failure_bytes(Some(&failure));
        assert!(
            field(&read) <= read_estimate,
            "{} > {read_estimate}",
            field(&read)
        );
        assert!(read_estimate < far_under);

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
