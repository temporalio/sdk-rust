//! Direct calls to Streams. Core reads and appends a Stream on the Worker's Stream runtimes, in
//! tasks on the Worker's runtime, and gives each result to the run as a [LocalResolution].

use futures_util::{
    StreamExt, TryStreamExt,
    future::{AbortHandle, AbortRegistration, Abortable},
};
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        common::StreamReadStop,
        external_data::{StreamMarkerData, StreamReadEntry},
        workflow_activation::{StreamAppended, StreamReadEnd, resolve_stream_append},
        workflow_completion,
    },
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
        history::v1::{HistoryEvent, history_event},
    },
};
use temporalio_streams::{
    MAX_WAIT, Reason, StreamError, StreamRuntime,
    digest::records_digest,
    ids::{MAX_OFFSET_BYTES, MAX_STREAM_ID_BYTES},
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, Producer, ReadRecordsRequest,
        StreamRecord, StreamRef, WorkflowTaskEpoch, producer::Kind,
    },
};
use tokio::{runtime::Handle, sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{Span, warn};

/// The errors that the Stream calls of a run give to the machines.
#[derive(Debug)]
pub(crate) enum CallError {
    /// Workflow code and History disagree.
    Nondeterminism(String),
    /// A command or a marker entry that no valid run gives.
    Fatal(String),
    /// A call that the Worker can't serve. The text is the whole message.
    Stream(String),
}

/// The metrics that the Stream calls count, which Core implements with its own.
pub(crate) trait StreamMetrics: Send + Sync + 'static {
    /// False for a meter that counts nothing, so that a call site builds no attributes for it.
    fn has_meter(&self) -> bool;
    /// The metrics with the attribute of the Store `store` added.
    fn with_store(&self, store: &str) -> Arc<dyn StreamMetrics>;
    /// The metrics with the attributes of the Workflow type and of the Store added.
    fn with_workflow_and_store(&self, workflow_type: &str, store: &str) -> Arc<dyn StreamMetrics>;
    /// A read started.
    fn stream_read_started(&self);
    /// A read ended with a failure.
    fn stream_read_failed(&self);
    /// Workflow code got `records` Records of a read.
    fn stream_read_records(&self, records: u64);
    /// An append started.
    fn stream_append_started(&self);
    /// An append ended with a failure.
    fn stream_append_failed(&self);
    /// The time an append took.
    fn stream_append_latency(&self, dur: Duration);
    /// A close started.
    fn stream_close_started(&self);
    /// A close ended with a failure.
    fn stream_close_failed(&self);
    /// The time a close took.
    fn stream_close_latency(&self, dur: Duration);
    /// A graceful shutdown gave up a close that the Store didn't answer.
    fn stream_close_abandoned(&self);
    /// A replay started to read the Records of a marker entry.
    fn stream_replay_read_started(&self);
    /// A replay could not read or check the Records of a marker entry.
    fn stream_replay_read_failed(&self);
    /// The time a replay's read of a marker entry took.
    fn stream_replay_read_latency(&self, dur: Duration);
}

/// What the Stream manager gives to the Worker's runs.
#[derive(Debug)]
pub(crate) enum RunInput {
    /// The Workflow Task of the run with this id reached its heartbeat time.
    HeartbeatTimeout(String),
    /// Wakes the pending polls for activations, with no new result.
    Bump,
    /// A result of a call of the run with this id.
    Resolution(String, StreamResolution),
}

/// Where the Stream manager sends what it gives to the Worker's runs.
pub(crate) trait RunSink: Send + Sync + 'static {
    /// Sends `input`, which the receiver handles under `span`.
    fn send(&self, input: RunInput, span: Span);
    /// Resolves once nothing receives the inputs, which is when the Worker is gone.
    fn closed(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// The Stream runtimes of a Worker, by the name of the Store that each serves, and the Worker's
/// options of a read. A Workflow call on a Stream uses the runtime of its `StreamRef.store`.
#[derive(Clone, Debug, Default)]
pub struct StreamRuntimes {
    runtimes: HashMap<String, Arc<StreamRuntime>>,
    /// The bytes of one read's Records that may wait for Workflow code, or `None` for
    /// [DEFAULT_READ_AHEAD_BYTES].
    read_ahead_bytes: Option<usize>,
    /// The Records of one read in one activation, or `None` for [STREAM_PAGE_MAX_RECORDS].
    read_page_records: Option<usize>,
    /// The least time between two batches of one read from the Store, or `None` for no gap.
    read_min_gap: Option<Duration>,
}

impl StreamRuntimes {
    /// Builds the set from pairs of a Store name and its runtime.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self {
            runtimes: runtimes.into_iter().collect(),
            read_ahead_bytes: None,
            read_page_records: None,
            read_min_gap: None,
        }
    }

    /// The runtimes with `bytes` as the bound of one read's Records that may wait for Workflow
    /// code. A read stops asking the Store while that many bytes wait, and goes on when an
    /// activation takes Records. Its first page always comes. The Records wait in the Worker's
    /// memory, whichever Store they come from, so the bound is the Worker's and not a Store's.
    /// Unset is 4 MiB. Zero is refused when the Worker's config is built.
    pub fn with_read_ahead_bytes(self, bytes: usize) -> Self {
        Self {
            read_ahead_bytes: Some(bytes),
            ..self
        }
    }

    /// The bound that [StreamRuntimes::with_read_ahead_bytes] set, if it set one.
    pub fn read_ahead_bytes(&self) -> Option<usize> {
        self.read_ahead_bytes
    }

    /// The runtimes with `records` as the most Records of one read in one activation. Core keeps
    /// the Records over it and gives them in the next activation, with no wait. Unset is 256. A
    /// value under 1 or over 1000 is refused when the Worker's config is built. A replay gives
    /// each page as History has it, whatever this value is.
    pub fn with_read_page_records(self, records: usize) -> Self {
        Self {
            read_page_records: Some(records),
            ..self
        }
    }

    /// The cap that [StreamRuntimes::with_read_page_records] set, if it set one.
    pub fn read_page_records(&self) -> Option<usize> {
        self.read_page_records
    }

    /// The runtimes with `gap` as the least time between two batches of one read. A read gives its
    /// first batch immediately. After it gave a batch to its run, it asks the Store again only once
    /// `gap` has passed, so the Records that came in between go in one batch, still at most
    /// `read_page_records` in one activation. On a Stream that gets single Records, this caps how
    /// often Workflow code wakes, and a Record or the end of a read waits at most one gap. Unset
    /// and zero are no gap. A gap over 1 s is refused when the Worker's config is built. A replay
    /// gives each page as History has it, whatever this value is.
    pub fn with_read_min_gap(self, gap: Duration) -> Self {
        Self {
            read_min_gap: Some(gap),
            ..self
        }
    }

    /// The gap that [StreamRuntimes::with_read_min_gap] set, if it set one.
    pub fn read_min_gap(&self) -> Option<Duration> {
        self.read_min_gap
    }

    /// The Records of one read in one activation, with the default when the Worker set none.
    pub(crate) fn page_cap(&self) -> usize {
        self.read_page_records.unwrap_or(STREAM_PAGE_MAX_RECORDS)
    }

    /// The Worker's options of a read.
    fn read_options(&self) -> ReadOptions {
        ReadOptions {
            read_ahead_bytes: self.read_ahead_bytes.unwrap_or(DEFAULT_READ_AHEAD_BYTES),
            min_gap: self.read_min_gap.unwrap_or_default(),
        }
    }

    /// The runtime of the Store `store`, if the Worker has one.
    pub fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.runtimes.get(store)
    }

    /// True if the Worker has no Stream runtime.
    pub fn is_empty(&self) -> bool {
        self.runtimes.is_empty()
    }
}

/// Says for a Workflow type whether a replay that can't read its Records fails the Workflow.
pub(crate) type FailsWorkflow = Box<dyn Fn(&str) -> bool + Send + Sync>;

/// What the Worker builds from its config for its Stream calls. A run and each history page share
/// it.
#[derive(derive_more::Debug)]
pub(crate) struct WorkerStreams {
    runtimes: StreamRuntimes,
    /// False for a replay Worker, which reads the Store only for the Records of a marker.
    calls_stores: bool,
    /// The Worker's metrics, which a replay's reads of the Store count in. `None` for a Worker
    /// with no meter, so that it builds no attributes for a read.
    #[debug(skip)]
    metrics: Option<Arc<dyn StreamMetrics>>,
    /// True for a Workflow type that the Worker fails, and not only its task, when a replay can't
    /// read its Records.
    #[debug(skip)]
    fails_workflow: FailsWorkflow,
    hooks: StreamTestHooks,
}

impl WorkerStreams {
    pub(crate) fn new(
        runtimes: StreamRuntimes,
        calls_stores: bool,
        metrics: Option<Arc<dyn StreamMetrics>>,
        fails_workflow: FailsWorkflow,
    ) -> Self {
        Self {
            runtimes,
            calls_stores,
            metrics,
            fails_workflow,
            hooks: StreamTestHooks::default(),
        }
    }

    /// The Stream state with `hooks` in place of the default ones.
    #[cfg(test)]
    pub(crate) fn with_test_hooks(self, hooks: StreamTestHooks) -> Self {
        Self { hooks, ..self }
    }

    /// The runtime of the Store `store`, if the Worker has one.
    pub(crate) fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.runtimes.get(store)
    }

    /// False for a replay Worker, which makes no live call.
    pub(crate) fn calls_stores(&self) -> bool {
        self.calls_stores
    }

    /// True if a replay of `workflow_type` that can't read its Records fails the Workflow.
    pub(crate) fn fails_workflow(&self, workflow_type: &str) -> bool {
        (self.fails_workflow)(workflow_type)
    }

    /// How long a replay's read of the Store tries again after its first `StorageUnavailable`.
    fn replay_retry_bound(&self) -> Duration {
        if let Some(bound) = self.hooks.replay_retry_bound {
            return bound;
        }
        STORE_REPLAY_RETRY_BOUND
    }

    fn close_bound(&self) -> Duration {
        if let Some(bound) = self.hooks.close_bound {
            return bound;
        }
        CLOSE_SHUTDOWN_BOUND
    }
}

/// The Worker's Stream state for the history of one run, which reads its markers with it.
#[derive(Clone, Debug)]
pub(crate) struct RunStreams {
    worker: Arc<WorkerStreams>,
    /// The run's Workflow type, which a replay's reads count under. Kept only with a meter.
    workflow_type: Option<String>,
}

impl RunStreams {
    pub(crate) fn new(worker: &Arc<WorkerStreams>, workflow_type: &str) -> Self {
        Self {
            worker: worker.clone(),
            workflow_type: worker.metrics.is_some().then(|| workflow_type.to_string()),
        }
    }

    pub(crate) fn worker(&self) -> &WorkerStreams {
        &self.worker
    }

    /// The metrics of a replay's read of `store`, or `None` with no meter.
    fn replay_metrics(&self, store: &str) -> Option<Arc<dyn StreamMetrics>> {
        let metrics = self.worker.metrics.as_ref()?;
        Some(match &self.workflow_type {
            Some(ty) => metrics.with_workflow_and_store(ty, store),
            None => metrics.with_store(store),
        })
    }
}

/// What lets a test see and shorten the wait of a graceful shutdown for a close.
#[derive(Clone, Debug, Default)]
pub(crate) struct StreamTestHooks {
    /// A shorter bound in place of [CLOSE_SHUTDOWN_BOUND].
    pub(crate) close_bound: Option<Duration>,
    /// Told each time a graceful shutdown waits for a close in flight.
    pub(crate) shutdown_waits: Option<Arc<Notify>>,
    /// A shorter bound in place of [STORE_REPLAY_RETRY_BOUND] for a replay's reads. They run on the
    /// Worker's own thread, whose clock a test can't pause.
    pub(crate) replay_retry_bound: Option<Duration>,
}

/// The bytes of one read's Records that may wait for Workflow code when the Worker sets no
/// `read_ahead_bytes`. Past the bound the read stops asking the Store, so that a run with no
/// Workflow Task in hand doesn't gather Records in the Worker's memory without a limit.
pub(crate) const DEFAULT_READ_AHEAD_BYTES: usize = 4 * 1024 * 1024;
/// The Records of one read in one activation when the Worker sets no `read_page_records`. With no
/// bound, a read of a long backlog would give Workflow code all of it in one page, which lang must
/// handle in one activation. A count adds no wait, unlike a time gap, and means the same on any
/// machine. Core keeps the Records over it and gives them in the next activation, which it issues
/// with no wait. Replay gives the pages of History as they are, so the bound doesn't apply there.
pub(crate) const STREAM_PAGE_MAX_RECORDS: usize = 256;
/// The highest `read_page_records` that a Worker may set. A larger page is one activation that
/// lang must handle in one go, which is what the cap bounds.
pub(crate) const MAX_READ_PAGE_RECORDS: usize = 1000;
/// The longest `read_min_gap` that a Worker may set. Each Record and each end of a read may wait
/// one gap, so the bound caps the wait that the gap adds.
pub(crate) const MAX_READ_MIN_GAP: Duration = Duration::from_secs(1);
/// The wait before the second try of a Store call that failed with `StorageUnavailable`. A short
/// Store outage should not reach Workflow code, as the retry of a Local Activity keeps it away.
pub(crate) const STORE_RETRY_FIRST_BACKOFF: Duration = Duration::from_millis(100);
/// The longest wait between two tries of such a call. The wait doubles up to it.
pub(crate) const STORE_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(2);
/// How long after its first failure a call stops trying and gives Workflow code the error. A
/// longer outage is one that Workflow code should see and handle.
pub(crate) const STORE_RETRY_BOUND: Duration = Duration::from_secs(30);
/// The same bound for a replay's read of the Store. It is shorter than the default Workflow Task
/// timeout, so the task fails with its cause and not with a timeout.
pub(crate) const STORE_REPLAY_RETRY_BOUND: Duration = Duration::from_secs(5);
/// The bytes of Records that one Workflow Task may put into its marker, for reads with their
/// Records in History. A larger marker could pass the server's payload limit, and the task would
/// then fail for good. The first Record of a task always comes, also when it is larger.
pub(crate) const MAX_MARKER_RECORD_BYTES: usize = 1024 * 1024;
/// The bytes of one Workflow Task's Stream marker that aren't Records: the entries of reads,
/// appends and closes, and the table of Streams. A long task with many small results would
/// otherwise make a marker with no limit. Past the bound, the task completes with a new one
/// forced, as at a heartbeat, and the results that didn't fit go to the new task.
///
/// Core counts an estimate that is never below the encoded size. No single entry comes near the
/// bound, since each input is bounded. The largest is a read entry with one page, 8402 bytes:
/// four offsets of [MAX_OFFSET_BYTES] and an end with a failure of 4228 bytes, whose message has
/// at most [MAX_FAILURE_MESSAGE_BYTES]. Each further page adds at most 10 bytes, and the pages
/// past the bound go to the next task. A Stream of the table has at most 8202 bytes, an id and a
/// Store name of 4096 bytes each. An append entry has at most 4249 bytes, and a close entry 4251.
/// The test `the_largest_entries_are_far_under_the_entry_bound` checks each against its estimate.
pub(crate) const MAX_MARKER_ENTRY_BYTES: usize = 256 * 1024;
/// The bytes of the message of a Stream failure that Workflow code gets and the marker records. A
/// Store can give an error text of any size, and one failure must not fill a marker.
pub(crate) const MAX_FAILURE_MESSAGE_BYTES: usize = 4096;
/// The idle timeout of a read that sets none. A quiet read holds its Workflow Task open, and each
/// heartbeat adds three events to History, so a read with no bound would grow History for good.
/// The server also ends a chain of heartbeat tasks after 30 minutes. One constant in Core gives
/// every SDK the same default.
pub(crate) const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// The Worker's options of a read.
#[derive(Clone, Copy, Debug)]
struct ReadOptions {
    read_ahead_bytes: usize,
    /// Zero for no gap.
    min_gap: Duration,
}
/// How long a graceful shutdown waits for a close in flight, from the start of the shutdown or
/// of the close if it is later. The Worker has no other bound for the work of its runs at a
/// shutdown, and a Store that never answers must not hold the shutdown for ever.
pub(crate) const CLOSE_SHUTDOWN_BOUND: Duration = Duration::from_secs(10);

/// The tries of one Store call that fails with `StorageUnavailable`.
#[derive(Clone, Copy, Debug)]
struct StoreRetry {
    first_failure: Option<Instant>,
    backoff: Duration,
    bound: Duration,
}

impl StoreRetry {
    fn new() -> Self {
        Self::with_bound(STORE_RETRY_BOUND)
    }

    fn with_bound(bound: Duration) -> Self {
        Self {
            first_failure: None,
            backoff: STORE_RETRY_FIRST_BACKOFF,
            bound,
        }
    }

    /// The wait before the next try of a call that failed with `error`, or `None` when the call
    /// resolves with it. Only `StorageUnavailable` may pass, since every other reason is the
    /// Store's answer and a new try would get it again.
    fn next_wait(&mut self, error: &StreamError) -> Option<Duration> {
        if error.reason != Reason::StorageUnavailable {
            return None;
        }
        let now = Instant::now();
        let first = *self.first_failure.get_or_insert(now);
        let left = (first + self.bound).saturating_duration_since(now);
        if left.is_zero() {
            return None;
        }
        let wait = self.backoff.min(left);
        self.backoff = (self.backoff * 2).min(STORE_RETRY_MAX_BACKOFF);
        Some(wait)
    }
}

/// Runs `call` again while it fails with `StorageUnavailable`, within [STORE_RETRY_BOUND] of its
/// first failure. A cancelled `give_up` ends the wait and gives the last error.
async fn retry_unavailable<T, F>(
    mut call: impl FnMut() -> F,
    give_up: Option<&CancellationToken>,
) -> Result<T, StreamError>
where
    F: Future<Output = Result<T, StreamError>>,
{
    let mut retry = StoreRetry::new();
    loop {
        let error = match call().await {
            Ok(answer) => return Ok(answer),
            Err(error) => error,
        };
        let Some(wait) = retry.next_wait(&error) else {
            return Err(error);
        };
        match give_up {
            Some(give_up) => tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = give_up.cancelled() => return Err(error),
            },
            None => tokio::time::sleep(wait).await,
        }
    }
}

static NEXT_CALL_ID: AtomicU64 = AtomicU64::new(1);

/// A new id for one dispatch of a call. It is unique in the process, so that a result of an older
/// instance of a run never matches a call of a newer one.
pub(crate) fn next_call_id() -> u64 {
    NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed)
}

/// The metrics of a run's call to `store`, or `None` for a Worker with no meter, so that such a
/// Worker builds no attributes for a call.
pub(crate) fn store_metrics(
    run: &dyn StreamMetrics,
    store: &str,
) -> Option<Arc<dyn StreamMetrics>> {
    run.has_meter().then(|| run.with_store(store))
}

/// The Records that a read gave to the run and that no activation took yet.
#[derive(Debug, Default)]
pub(crate) struct ReadFlow {
    undelivered: AtomicUsize,
    taken: Notify,
    /// When an activation last took every Record that the read had sent.
    all_taken_at: parking_lot::Mutex<Option<Instant>>,
}

impl ReadFlow {
    /// The bytes of the read's Records that no activation took yet.
    fn undelivered(&self) -> usize {
        self.undelivered.load(Ordering::Acquire)
    }

    fn sent(&self, bytes: usize) {
        self.undelivered.fetch_add(bytes, Ordering::AcqRel);
    }

    /// Notes that an activation took `bytes` of the read's Records.
    pub(crate) fn delivered(&self, bytes: usize) {
        let mut held = self.undelivered.load(Ordering::Acquire);
        while let Err(now) = self.undelivered.compare_exchange_weak(
            held,
            held.saturating_sub(bytes),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            held = now;
        }
        if held <= bytes {
            *self.all_taken_at.lock() = Some(Instant::now());
        }
        self.taken.notify_one();
    }

    /// When the idle time of the read started to count, or `None` while Workflow code hasn't
    /// taken all of its Records. The idle time counts only while the read waits for the Store, so
    /// a page that waits for a Workflow Task or a paused read doesn't use it up.
    fn idle_since(&self, last_record_at: Instant) -> Option<Instant> {
        if self.undelivered.load(Ordering::Acquire) > 0 {
            return None;
        }
        Some(
            self.all_taken_at
                .lock()
                .map_or(last_record_at, |taken| taken.max(last_record_at)),
        )
    }

    /// Waits until an activation takes Records of the read.
    async fn wait_for_take(&self) {
        self.taken.notified().await;
    }

    async fn wait_for_room(&self, read_ahead_bytes: usize) {
        while self.undelivered.load(Ordering::Acquire) >= read_ahead_bytes {
            self.taken.notified().await;
        }
    }
}

/// A call that the machines ask the Worker to make or to stop.
#[derive(Debug)]
pub(crate) enum StreamRequest {
    Read(ReadSpec),
    Append(AppendSpec),
    Close(CloseSpec),
    Cancel(u64),
}

/// A read to run, with what is left of its caps.
#[derive(Debug)]
pub(crate) struct ReadSpec {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) stream: StreamRef,
    pub(crate) from_offset: Vec<u8>,
    /// The Workflow time at which the read started, plus its `timeout`.
    pub(crate) deadline: Option<SystemTime>,
    /// How long the read waits for the Store with no Record. Zero gives what is in the Stream and
    /// doesn't wait.
    pub(crate) idle_timeout: Duration,
    /// The Records left under `max_records`, or `None` for no cap.
    pub(crate) records_left: Option<u32>,
    /// The bytes left under `max_bytes`, or `None` for no cap.
    pub(crate) bytes_left: Option<u64>,
    /// True once Workflow code took a Record of the read, so the first-Record rule is spent.
    pub(crate) took_a_record: bool,
    pub(crate) flow: Arc<ReadFlow>,
}

/// An append to run.
#[derive(Debug)]
pub(crate) struct AppendSpec {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) stream: StreamRef,
    /// The Producer that lang sent, with no epoch.
    pub(crate) producer: Producer,
    /// The `sequence` of the request.
    pub(crate) sequence: i64,
    pub(crate) bodies: Vec<Payload>,
}

/// A close to run, after the server took the Workflow Task that recorded it.
#[derive(Debug)]
pub(crate) struct CloseSpec {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) stream: StreamRef,
    /// The Producer that lang sent, with no epoch.
    pub(crate) producer: Producer,
    /// The task that recorded the close, also when a replay sends it again. `None` only for a
    /// marker with no `intent_epoch`, which then takes the task in hand.
    pub(crate) epoch: Option<WorkflowTaskEpoch>,
}

/// The result of one step of a call.
#[derive(Debug)]
pub(crate) struct StreamResolution {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) result: StreamCallResult,
}

#[derive(Debug)]
pub(crate) enum StreamCallResult {
    /// The Records that a read gave since its last page, and its end if it ended.
    Page {
        records: Vec<StreamRecord>,
        end: Option<StreamReadEnd>,
    },
    Append(resolve_stream_append::Status),
    /// `None` when the Stream is closed.
    Close(Option<Failure>),
}

/// Runs the Stream calls of a Worker's runs. Its tasks run on the Worker's runtime, not on the
/// workflow processing thread, so that Store work never holds up the runs.
pub(crate) struct StreamManager {
    streams: Arc<WorkerStreams>,
    handle: Handle,
    local_tx: Arc<dyn RunSink>,
    /// The run and the abort handle of each running call, by its call id, and whether the call
    /// outlives its run.
    tasks: Arc<Mutex<HashMap<u64, Task>>>,
    /// The closes that the Store didn't answer yet. A graceful shutdown waits for them, since
    /// History has their intent and Workflow code may never await them.
    closes_in_flight: Arc<AtomicUsize>,
    /// The Worker's shutdown, from whose start a close in flight has [CLOSE_SHUTDOWN_BOUND] and
    /// no read starts.
    shutdown: CancellationToken,
}

struct Task {
    run_id: String,
    handle: AbortHandle,
    /// True for a close, which History recorded, so that the end of its run doesn't stop it.
    outlives_run: bool,
}

impl Drop for StreamManager {
    /// Stops every call, so that no Store work outlives the Worker.
    fn drop(&mut self) {
        for (_, task) in self.tasks.lock().drain() {
            task.handle.abort();
        }
    }
}

impl StreamManager {
    pub(crate) fn new(
        streams: Arc<WorkerStreams>,
        handle: Handle,
        local_tx: Arc<dyn RunSink>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            streams,
            handle,
            local_tx,
            tasks: Default::default(),
            closes_in_flight: Default::default(),
            shutdown,
        }
    }

    /// True while a close waits for the Store, so that a graceful shutdown isn't done.
    pub(crate) fn has_closes_in_flight(&self) -> bool {
        let in_flight = self.closes_in_flight.load(Ordering::Acquire) > 0;
        if in_flight && let Some(waits) = &self.streams.hooks.shutdown_waits {
            waits.notify_one();
        }
        in_flight
    }

    /// Starts or stops the calls of a run. `epoch` is the run's current Workflow Task, which an
    /// append writes with, or `None` when the run has no task in hand. `metrics` are the run's.
    pub(crate) fn dispatch(
        &self,
        run_id: &str,
        epoch: Option<WorkflowTaskEpoch>,
        requests: Vec<StreamRequest>,
        metrics: &dyn StreamMetrics,
    ) {
        for request in requests {
            match request {
                StreamRequest::Cancel(call_id) => {
                    if let Some(task) = self.tasks.lock().remove(&call_id) {
                        task.handle.abort();
                    }
                }
                // A graceful shutdown ends each task that only reads hold, and the next Worker
                // resumes the reads from History. A read that started now would only hold the
                // shutdown, also in a task that came back here through a poll still open.
                StreamRequest::Read(_) if self.shutdown.is_cancelled() => {}
                StreamRequest::Read(spec) => {
                    let (call_id, seq) = (spec.call_id, spec.seq);
                    match self.runtime(&spec.stream) {
                        Some(runtime) => {
                            let sink = self.sink(run_id, call_id, seq);
                            let metrics = store_metrics(metrics, &spec.stream.store);
                            if let Some(metrics) = &metrics {
                                metrics.stream_read_started();
                            }
                            let options = self.streams.runtimes.read_options();
                            let call = read(runtime, spec, sink, metrics, options);
                            self.spawn(run_id, call_id, false, call);
                        }
                        None => self.no_runtime(run_id, call_id, seq, Call::Read),
                    }
                }
                StreamRequest::Append(spec) => {
                    let (call_id, seq) = (spec.call_id, spec.seq);
                    match (self.runtime(&spec.stream), epoch) {
                        (Some(runtime), Some(epoch)) => {
                            let sink = self.sink(run_id, call_id, seq);
                            let timer = CallTimer::start(metrics, &spec.stream.store);
                            if let Some(timer) = &timer {
                                timer.metrics.stream_append_started();
                            }
                            let shutdown = self.shutdown.clone();
                            let call = append(runtime, spec, epoch, sink, timer, shutdown);
                            self.spawn(run_id, call_id, false, call);
                        }
                        _ => self.no_runtime(run_id, call_id, seq, Call::Append),
                    }
                }
                StreamRequest::Close(spec) => {
                    let (call_id, seq) = (spec.call_id, spec.seq);
                    match (self.runtime(&spec.stream), spec.epoch.or(epoch)) {
                        (Some(runtime), Some(epoch)) => {
                            let sink = self.sink(run_id, call_id, seq);
                            let timer = CallTimer::start(metrics, &spec.stream.store);
                            if let Some(timer) = &timer {
                                timer.metrics.stream_close_started();
                            }
                            let in_flight = CloseInFlight::new(&self.closes_in_flight);
                            let stream = spec.stream.clone();
                            let abandoned = timer.as_ref().map(|timer| timer.metrics.clone());
                            let call = close(runtime, spec, epoch, sink, timer, in_flight);
                            let call = self.bounded_by_shutdown(stream, call, abandoned);
                            // History recorded the close, so the end of the run must not stop it.
                            self.spawn(run_id, call_id, true, call);
                        }
                        _ => self.no_runtime(run_id, call_id, seq, Call::Close),
                    }
                }
            }
        }
    }

    /// Stops every call of the run but its closes. Results already on their way find no call and
    /// are dropped.
    pub(crate) fn cancel_run(&self, run_id: &str) {
        self.tasks.lock().retain(|_, task| {
            if task.run_id == run_id && !task.outlives_run {
                task.handle.abort();
                false
            } else {
                true
            }
        });
    }

    /// Sends a heartbeat timeout for the run at `deadline`, for a Worker with no Local Activity
    /// manager to keep the timer.
    pub(crate) fn start_heartbeat_timeout(
        &self,
        run_id: String,
        deadline: std::time::Instant,
        abort_reg: AbortRegistration,
    ) {
        let local_tx = self.local_tx.clone();
        let span = Span::current();
        // A duration, so that the timer runs on the runtime's own clock, as the read's timers do.
        let wait = deadline.saturating_duration_since(std::time::Instant::now());
        let sleep = {
            let _runtime = self.handle.enter();
            tokio::time::sleep(wait)
        };
        self.handle.spawn(Abortable::new(
            async move {
                sleep.await;
                local_tx.send(RunInput::HeartbeatTimeout(run_id), span);
            },
            abort_reg,
        ));
    }

    /// The close, which a graceful shutdown gives up after its bound. The bound counts from the
    /// start of the shutdown, or from the start of the close if that is later.
    fn bounded_by_shutdown(
        &self,
        stream: StreamRef,
        call: impl Future<Output = ()> + Send + 'static,
        metrics: Option<Arc<dyn StreamMetrics>>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let shutdown = self.shutdown.clone();
        let bound = self.streams.close_bound();
        let local_tx = self.local_tx.clone();
        async move {
            let mut call = Box::pin(call);
            let passed = async {
                shutdown.cancelled().await;
                tokio::time::sleep(bound).await;
            };
            tokio::select! {
                _ = &mut call => {}
                _ = passed => {
                    // Dropping the call ends its count as in flight before the shutdown looks.
                    drop(call);
                    warn!(
                        stream_id = %stream.id,
                        store = %stream.store,
                        "A graceful shutdown gave up a Stream close that the Store didn't answer \
                         in {bound:?}"
                    );
                    if let Some(metrics) = metrics {
                        metrics.stream_close_abandoned();
                    }
                    local_tx.send(RunInput::Bump, Span::current());
                }
            }
        }
    }

    fn runtime(&self, stream: &StreamRef) -> Option<Arc<StreamRuntime>> {
        self.streams.get(&stream.store).cloned()
    }

    /// The machines check the Store of each call, so a call with no runtime only comes when the
    /// runtimes and the machines disagree. It still resolves, so that the run doesn't wait forever,
    /// with a failure of the type that a Store gives for a call it can't serve.
    fn no_runtime(&self, run_id: &str, call_id: u64, seq: u32, call: Call) {
        warn!(
            run_id,
            seq, "A Stream call reached the Worker with no runtime or no epoch"
        );
        let failure = failure(&StreamError::new(
            Reason::Unsupported,
            "This Worker has no Stream runtime for the Store of the call",
        ));
        let result = match call {
            Call::Read => StreamCallResult::Page {
                records: vec![],
                end: Some(StreamReadEnd {
                    failure: Some(failure),
                    ..Default::default()
                }),
            },
            Call::Append => {
                StreamCallResult::Append(resolve_stream_append::Status::Failed(failure))
            }
            Call::Close => StreamCallResult::Close(Some(failure)),
        };
        (self.sink(run_id, call_id, seq))(result);
    }

    fn sink(&self, run_id: &str, call_id: u64, seq: u32) -> impl Fn(StreamCallResult) + use<> {
        let local_tx = self.local_tx.clone();
        let run_id = run_id.to_string();
        move |result| {
            local_tx.send(
                RunInput::Resolution(
                    run_id.clone(),
                    StreamResolution {
                        call_id,
                        seq,
                        result,
                    },
                ),
                Span::current(),
            );
        }
    }

    fn spawn(
        &self,
        run_id: &str,
        call_id: u64,
        outlives_run: bool,
        call: impl Future<Output = ()> + Send + 'static,
    ) {
        let (handle, registration) = AbortHandle::new_pair();
        self.tasks.lock().insert(
            call_id,
            Task {
                run_id: run_id.to_string(),
                handle,
                outlives_run,
            },
        );
        let tasks = self.tasks.clone();
        let local_tx = self.local_tx.clone();
        self.handle.spawn(async move {
            // A call whose Worker is gone stops at once, also one that pauses for its run.
            let call = async move {
                tokio::select! {
                    _ = call => {}
                    _ = local_tx.closed() => {}
                }
            };
            // An aborted call was already removed by whoever aborted it.
            if Abortable::new(call, registration).await.is_ok() {
                tasks.lock().remove(&call_id);
            }
        });
    }
}

/// Reads until the first stop. It sends each response with Records as one batch immediately, and
/// looks at the Store again right after, or once the Worker's `read_min_gap` after that batch has
/// passed. Batches that come while an activation of the run is out join, up to the Worker's
/// `read_page_records` for one activation.
async fn read(
    runtime: Arc<StreamRuntime>,
    spec: ReadSpec,
    sink: impl Fn(StreamCallResult),
    metrics: Option<Arc<dyn StreamMetrics>>,
    options: ReadOptions,
) {
    let deadline = spec.deadline.map(|deadline| {
        Instant::now()
            + deadline
                .duration_since(SystemTime::now())
                .unwrap_or_default()
    });
    let mut last_offset = spec.from_offset.clone();
    let mut records_left = spec.records_left;
    let mut bytes_left = spec.bytes_left;
    let mut took_a_record = spec.took_a_record;
    let mut last_record_at = Instant::now();
    let end = |last_offset: Vec<u8>, stop: StreamReadStop| StreamReadEnd {
        last_offset,
        stop: stop as i32,
        done: stop == StreamReadStop::Done,
        failure: None,
    };
    // A resumed read whose caps were spent before its Workflow Task failed ends at once.
    if records_left == Some(0) {
        sink(StreamCallResult::Page {
            records: vec![],
            end: Some(end(last_offset, StreamReadStop::MaxRecords)),
        });
        return;
    }
    // A read that gave no Record yet looks at the Store once before its timeout ends it. Its
    // deadline counts from the Workflow time of its task, so a slow start would otherwise end a
    // short read with no Record, and a loop of such reads could stand still.
    let mut looked = spec.took_a_record;
    // The tries of the current Store outage. A read that fails asks again from `last_offset`, so
    // it gives no Record twice and skips none.
    let mut retry = StoreRetry::new();
    // When the gap after the last batch ends. The read asks the Store again only then, so the
    // Records that come in the gap go in one batch.
    let mut next_batch_at: Option<Instant> = None;
    loop {
        if let Some(at) = next_batch_at.take() {
            // A deadline in the gap ends the read at the deadline, and not at the gap's end.
            tokio::time::sleep_until(deadline.map_or(at, |deadline| deadline.min(at))).await;
        }
        spec.flow.wait_for_room(options.read_ahead_bytes).await;
        let now = Instant::now();
        if looked && deadline.is_some_and(|deadline| deadline <= now) {
            sink(StreamCallResult::Page {
                records: vec![],
                end: Some(end(last_offset, StreamReadStop::Timeout)),
            });
            return;
        }
        let idle_deadline = |last_record_at: Instant| {
            spec.flow
                .idle_since(last_record_at)
                .map(|since| since + spec.idle_timeout)
        };
        // While the idle time doesn't count, the read looks again after one idle timeout.
        let idle_check = idle_deadline(last_record_at).unwrap_or(now + spec.idle_timeout);
        let wait_until = deadline
            .into_iter()
            .fold(idle_check.min(now + MAX_WAIT), Instant::min);
        let request = ReadRecordsRequest {
            namespace: String::new(),
            r#ref: Some(spec.stream.clone()),
            from_offset: last_offset.clone(),
            max_records: records_left.map_or(0, |left| left.min(i32::MAX as u32) as i32),
            max_bytes: page_bytes_cap(bytes_left, &spec.flow, options.read_ahead_bytes),
            wait: Some(proto_duration(wait_until.saturating_duration_since(now))),
        };
        let answer = runtime.read_records(request).await.and_then(|response| {
            let offsets = response
                .records
                .iter()
                .map(|record| record.offset.as_slice());
            store_offsets(offsets).map(|()| response)
        });
        let response = match answer {
            Ok(response) => {
                retry = StoreRetry::new();
                response
            }
            Err(error) => {
                if let Some(wait) = retry.next_wait(&error) {
                    tokio::time::sleep(wait).await;
                    continue;
                }
                if let Some(metrics) = &metrics {
                    metrics.stream_read_failed();
                }
                sink(StreamCallResult::Page {
                    records: vec![],
                    end: Some(StreamReadEnd {
                        last_offset,
                        failure: Some(failure(&error)),
                        ..Default::default()
                    }),
                });
                return;
            }
        };
        looked = true;
        let given = response.records.len();
        let mut page = Vec::with_capacity(given);
        let mut stop = None;
        let mut page_bytes = 0;
        for record in response.records {
            let size = record.encoded_len();
            if let Some(left) = bytes_left.as_mut() {
                if size as u64 > *left && took_a_record {
                    stop = Some(StreamReadStop::MaxBytes);
                    break;
                }
                *left = left.saturating_sub(size as u64);
            }
            last_offset.clone_from(&record.offset);
            page_bytes += size;
            page.push(record);
            took_a_record = true;
            if let Some(left) = records_left.as_mut() {
                *left -= 1;
                if *left == 0 {
                    stop = Some(StreamReadStop::MaxRecords);
                    break;
                }
            }
        }
        // A read that took the last Record of a closed Stream is done, also when that Record
        // filled a cap, so that Workflow code knows that nothing more comes.
        if response.done && page.len() == given {
            stop = Some(StreamReadStop::Done);
        }
        if stop.is_none() && bytes_left == Some(0) {
            stop = Some(StreamReadStop::MaxBytes);
        }
        if !page.is_empty() {
            last_record_at = Instant::now();
            spec.flow.sent(page_bytes);
        }
        match stop {
            Some(stop) => {
                sink(StreamCallResult::Page {
                    records: page,
                    end: Some(end(last_offset, stop)),
                });
                return;
            }
            None if !page.is_empty() => {
                sink(StreamCallResult::Page {
                    records: page,
                    end: None,
                });
                if !options.min_gap.is_zero() {
                    next_batch_at = Some(Instant::now() + options.min_gap);
                }
            }
            // The idle end comes after a look at the Store, so that a read with a zero idle
            // timeout gives the Records that are there.
            None if idle_deadline(last_record_at).is_some_and(|idle| idle <= Instant::now()) => {
                sink(StreamCallResult::Page {
                    records: vec![],
                    end: Some(end(last_offset, StreamReadStop::Idle)),
                });
                return;
            }
            // A read that doesn't wait for the Store found nothing, while its idle time doesn't
            // count. It looks again once Workflow code takes its Records, and not in a loop.
            None if spec.idle_timeout.is_zero() => match deadline {
                Some(deadline) => {
                    let _ = tokio::time::timeout_at(deadline, spec.flow.wait_for_take()).await;
                }
                None => spec.flow.wait_for_take().await,
            },
            None => {}
        }
    }
}

/// The byte cap of the next page: the room left under `read_ahead_bytes`, and under the read's
/// `max_bytes` if it set one. A cap of one byte still gives one Record, so the read moves on, and
/// a cap of zero bytes left ends the read with that Record.
fn page_bytes_cap(bytes_left: Option<u64>, flow: &ReadFlow, read_ahead_bytes: usize) -> i64 {
    let room = read_ahead_bytes.saturating_sub(flow.undelivered()) as u64;
    bytes_left
        .map_or(room, |left| left.min(room))
        .clamp(1, i64::MAX as u64) as i64
}

/// The metrics of an append or a close, and when Core started it.
struct CallTimer {
    metrics: Arc<dyn StreamMetrics>,
    started: Instant,
}

impl CallTimer {
    /// `None` for a Worker with no meter, which then reads no clock for a call.
    fn start(run: &dyn StreamMetrics, store: &str) -> Option<Self> {
        store_metrics(run, store).map(|metrics| Self {
            metrics,
            started: Instant::now(),
        })
    }
}

/// The kind of a call, for the failure of one that can't run.
enum Call {
    Read,
    Append,
    Close,
}

/// Counts a close as in flight for as long as it lives.
struct CloseInFlight(Arc<AtomicUsize>);

impl CloseInFlight {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self(count.clone())
    }
}

impl Drop for CloseInFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Closes the Stream as the run's Producer, with the Workflow Task that recorded the close as the
/// epoch. A Stream that is closed already is a success.
async fn close(
    runtime: Arc<StreamRuntime>,
    spec: CloseSpec,
    epoch: WorkflowTaskEpoch,
    sink: impl Fn(StreamCallResult),
    timer: Option<CallTimer>,
    in_flight: CloseInFlight,
) {
    let mut producer = spec.producer;
    if let Some(Kind::Workflow(workflow)) = producer.kind.as_mut() {
        workflow.epoch = Some(epoch);
    }
    let request = CloseStreamRequest {
        namespace: String::new(),
        r#ref: Some(spec.stream),
        producer: Some(producer),
    };
    // A second close of a closed Stream succeeds, so a try whose answer was lost is safe to send
    // again. A graceful shutdown bounds the tries, as it bounds the first one.
    let runtime = &*runtime;
    let request = &request;
    let result = retry_unavailable(move || runtime.close_stream(request.clone()), None)
        .await
        .err();
    // The Store answered, so the result that wakes the run also finds no close in flight.
    drop(in_flight);
    if let Some(timer) = timer {
        timer.metrics.stream_close_latency(timer.started.elapsed());
        if result.is_some() {
            timer.metrics.stream_close_failed();
        }
    }
    sink(StreamCallResult::Close(result.map(|error| failure(&error))));
}

/// Appends the bodies as the run's Producer, with the Workflow Task as the epoch.
async fn append(
    runtime: Arc<StreamRuntime>,
    spec: AppendSpec,
    epoch: WorkflowTaskEpoch,
    sink: impl Fn(StreamCallResult),
    timer: Option<CallTimer>,
    shutdown: CancellationToken,
) {
    let mut producer = spec.producer;
    if let Some(Kind::Workflow(workflow)) = producer.kind.as_mut() {
        workflow.epoch = Some(epoch);
    }
    let request = AppendRecordsRequest {
        namespace: String::new(),
        r#ref: Some(spec.stream),
        producer: Some(producer),
        sequence: spec.sequence,
        records: spec
            .bodies
            .into_iter()
            .map(|body| AppendRecord { body: Some(body) })
            .collect(),
    };
    let answer = if request.sequence == 0 {
        // The Store numbers a batch of sequence zero itself and keeps no dedupe for it, so a
        // second try after a lost answer would write its Records again.
        runtime.append_records(request).await
    } else {
        // The Store takes a resend of the same Producer, epoch, sequence and Records once, and
        // gives the offsets of the first write. A shutdown stops the tries, since the task waits
        // for this result.
        let runtime = &*runtime;
        let request = &request;
        retry_unavailable(
            move || runtime.append_records(request.clone()),
            Some(&shutdown),
        )
        .await
    };
    let answer = answer.and_then(|response| {
        store_offsets([
            response.first_offset.as_slice(),
            response.last_offset.as_slice(),
        ])
        .map(|()| response)
    });
    let status = match answer {
        Ok(response) => resolve_stream_append::Status::Appended(StreamAppended {
            first_offset: response.first_offset,
            last_offset: response.last_offset,
        }),
        Err(error) => resolve_stream_append::Status::Failed(failure(&error)),
    };
    if let Some(timer) = timer {
        timer.metrics.stream_append_latency(timer.started.elapsed());
        if matches!(status, resolve_stream_append::Status::Failed(_)) {
            timer.metrics.stream_append_failed();
        }
    }
    sink(StreamCallResult::Append(status));
}

/// Fails a Store answer with an offset over [MAX_OFFSET_BYTES]. Core records the offsets of a call
/// in History, so it takes none of any size, also from a Store.
fn store_offsets<'a>(offsets: impl IntoIterator<Item = &'a [u8]>) -> Result<(), StreamError> {
    match offsets
        .into_iter()
        .find(|offset| offset.len() > MAX_OFFSET_BYTES)
    {
        Some(offset) => Err(StreamError::storage_corrupt(format!(
            "The Store gave an offset of {} bytes, more than the limit of {MAX_OFFSET_BYTES}",
            offset.len()
        ))),
        None => Ok(()),
    }
}

/// The longest Store name of a Stream call, in bytes. The marker records the Store of each call.
pub(crate) const MAX_STORE_NAME_BYTES: usize = 4096;

/// The failure of a Stream call of Workflow code whose Stream or `from_offset` is over its limit,
/// if it is. Core refuses such a call before any Store call and records nothing of it in the
/// marker, so that no input of Workflow code makes a marker entry of any size. The message gives
/// the limit and not the value.
pub(crate) fn input_over_limit(stream: &StreamRef, from_offset: &[u8]) -> Option<Failure> {
    let over = |what: &str, bytes: usize, limit: usize| {
        (bytes > limit).then(|| {
            failure(&StreamError::invalid(format!(
                "{what} has {bytes} bytes, more than the limit of {limit}"
            )))
        })
    };
    over("The Stream id", stream.id.len(), MAX_STREAM_ID_BYTES)
        .or_else(|| over("The Store name", stream.store.len(), MAX_STORE_NAME_BYTES))
        .or_else(|| {
            over(
                "The offset `from_offset`",
                from_offset.len(),
                MAX_OFFSET_BYTES,
            )
        })
}

fn proto_duration(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: duration.as_secs().min(i64::MAX as u64) as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

/// The failure that Workflow code gets for a Store error. Its type is the reason's token, and its
/// details hold the `StreamErrorDetails` that lang reads the reason from.
pub(crate) fn failure(error: &StreamError) -> Failure {
    Failure {
        message: cut_message(error.to_string()),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: error.reason.token().to_string(),
                non_retryable: error.reason != Reason::StorageUnavailable,
                details: Some(Payloads {
                    payloads: vec![Payload {
                        metadata: HashMap::from([
                            ("encoding".to_string(), b"binary/protobuf".to_vec()),
                            (
                                "messageType".to_string(),
                                b"temporal.api.stream.v1.StreamErrorDetails".to_vec(),
                            ),
                        ]),
                        data: error.details(),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

/// `text` cut at [MAX_FAILURE_MESSAGE_BYTES] on a character boundary, with `...` at the end of a
/// cut one. Core cuts it where it builds the failure, so that lang and the marker get the same
/// text, and a replay gives the text of the marker.
fn cut_message(text: String) -> String {
    if text.len() <= MAX_FAILURE_MESSAGE_BYTES {
        return text;
    }
    let mut end = MAX_FAILURE_MESSAGE_BYTES - "...".len();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}

/// The marker detail that holds the encoded [StreamMarkerData].
pub(crate) const STREAM_MARKER_DATA_KEY: &str = "data";
/// The metadata key of a `tonic::Status` from a failed hydrate, so that the failed Workflow Task
/// carries its message as is.
pub(crate) const STREAM_REPLAY_ERROR_KEY: &str = "temporal-stream-replay-error";
/// The Store reads that a hydrate runs at a time.
const HYDRATE_CONCURRENCY: usize = 8;

/// The encoded marker data of a `core_stream` marker event, if the event is one.
pub(crate) fn stream_marker_payload(event: &mut HistoryEvent) -> Option<&mut Payload> {
    match event.attributes.as_mut()? {
        history_event::Attributes::MarkerRecordedEventAttributes(marker)
            if marker.marker_name == STREAM_MARKER_NAME =>
        {
            marker
                .details
                .get_mut(STREAM_MARKER_DATA_KEY)
                .and_then(|payloads| payloads.payloads.first_mut())
        }
        _ => None,
    }
}

/// The encoded marker data of a `core_stream` marker event, if the event is one.
pub(crate) fn stream_marker_payload_of(event: &HistoryEvent) -> Option<&Payload> {
    match event.attributes.as_ref()? {
        history_event::Attributes::MarkerRecordedEventAttributes(marker)
            if marker.marker_name == STREAM_MARKER_NAME =>
        {
            marker
                .details
                .get(STREAM_MARKER_DATA_KEY)
                .and_then(|payloads| payloads.payloads.first())
        }
        _ => None,
    }
}

/// True if a `core_stream` marker among the events counts Records that it doesn't hold.
pub(crate) fn needs_stream_records(events: &[HistoryEvent]) -> bool {
    events.iter().any(|event| {
        stream_marker_payload_of(event)
            .and_then(|payload| StreamMarkerData::decode(payload.data.as_slice()).ok())
            .is_some_and(|data| {
                data.reads
                    .iter()
                    .any(|entry| entry.count > 0 && entry.records.is_empty())
            })
    })
}

/// The type of the failure of a replay that can't read or check the Records of a marker. A replay
/// whose Store stays unavailable has no type, so that it never fails the Workflow.
pub(crate) const STREAM_REPLAY_ERROR_TYPE: &str = "StreamReplayError";

/// True if a failure of a Workflow Task is one of a replay that can't read its Records.
pub(crate) fn is_stream_replay_failure(failure: &workflow_completion::Failure) -> bool {
    failure
        .failure
        .as_ref()
        .and_then(|failure| failure.maybe_application_failure())
        .is_some_and(|info| info.r#type == STREAM_REPLAY_ERROR_TYPE)
}

/// True if the status comes from a failed hydrate.
pub(crate) fn is_stream_replay_error(status: &tonic::Status) -> bool {
    status.metadata().contains_key(STREAM_REPLAY_ERROR_KEY)
}

/// True if the status comes from a hydrate whose Store stayed unavailable. The Records may still be
/// there, so such a replay fails only its task, and a later attempt may pass.
pub(crate) fn is_stream_replay_unavailable(status: &tonic::Status) -> bool {
    is_stream_replay_error(status) && status.code() == tonic::Code::Unavailable
}

/// Reads the Records of each `core_stream` marker that has their count and digest but not the
/// Records themselves, and puts them in the marker. The machines are sync, so a replay reads the
/// Store here, where history enters in async code, and gives them a marker that has its Records.
pub(crate) async fn hydrate_stream_markers(
    events: &mut [HistoryEvent],
    streams: Option<&RunStreams>,
) -> Result<(), tonic::Status> {
    let mut markers = vec![];
    for (at, event) in events.iter_mut().enumerate() {
        let Some(payload) = stream_marker_payload(event) else {
            continue;
        };
        let data = StreamMarkerData::decode(payload.data.as_slice()).map_err(|error| {
            replay_error(
                tonic::Code::DataLoss,
                format!("StreamReplayError: a Stream marker is bad: {error}"),
            )
        })?;
        if data
            .reads
            .iter()
            .any(|entry| entry.count > 0 && entry.records.is_empty())
        {
            if let Some(entry) = data
                .reads
                .iter()
                .find(|entry| data.streams.get(entry.stream_index as usize).is_none())
            {
                return Err(replay_error(
                    tonic::Code::DataLoss,
                    format!(
                        "StreamReplayError: a Stream marker is bad: read {} has Stream {}, which the \
                     marker doesn't have",
                        entry.seq, entry.stream_index
                    ),
                ));
            }
            markers.push((at, data));
        }
    }
    if markers.is_empty() {
        return Ok(());
    }
    let reads: Vec<_> = markers
        .iter()
        .enumerate()
        .flat_map(|(marker, (_, data))| {
            data.reads
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.count > 0 && entry.records.is_empty())
                .map(move |(entry, read)| {
                    let stream = data.streams[read.stream_index as usize].clone();
                    (marker, entry, stream, read.clone())
                })
        })
        .collect();
    // The first failure ends the hydrate. Each read may retry an unavailable Store for its
    // bound, and the reads behind it would each wait that long again for the same outage.
    let results: Vec<_> = futures_util::stream::iter(reads)
        .map(|(marker, entry, stream, read)| async move {
            let records = read_range(streams, &stream, &read).await?;
            Ok::<_, tonic::Status>((marker, entry, records))
        })
        .buffer_unordered(HYDRATE_CONCURRENCY)
        .try_collect()
        .await?;
    for (marker, entry, records) in results {
        markers[marker].1.reads[entry].records = records;
    }
    for (at, data) in markers {
        if let Some(payload) = stream_marker_payload(&mut events[at]) {
            payload.data = data.encode_to_vec();
        }
    }
    Ok(())
}

/// Reads the Records of one entry and checks them, and counts the read in the metrics.
async fn read_range(
    streams: Option<&RunStreams>,
    stream: &StreamRef,
    entry: &StreamReadEntry,
) -> Result<Vec<StreamRecord>, tonic::Status> {
    let runtime = streams.and_then(|streams| streams.worker.get(&stream.store));
    let retry = StoreRetry::with_bound(streams.map_or(STORE_REPLAY_RETRY_BOUND, |streams| {
        streams.worker.replay_retry_bound()
    }));
    let Some(metrics) = streams.and_then(|streams| streams.replay_metrics(&stream.store)) else {
        return read_and_check(runtime, stream, entry, retry).await;
    };
    metrics.stream_replay_read_started();
    let started = Instant::now();
    let records = read_and_check(runtime, stream, entry, retry).await;
    metrics.stream_replay_read_latency(started.elapsed());
    if records.is_err() {
        metrics.stream_replay_read_failed();
    }
    records
}

/// Reads the Records of one entry, with no wait, and checks them against the entry.
async fn read_and_check(
    runtime: Option<&Arc<StreamRuntime>>,
    stream: &StreamRef,
    entry: &StreamReadEntry,
    fresh_retry: StoreRetry,
) -> Result<Vec<StreamRecord>, tonic::Status> {
    let range = || {
        format!(
            "Stream `{}` in Store `{}`, the Records after {} through {}",
            stream.id,
            stream.store,
            offset_text(&entry.after_offset),
            offset_text(&entry.last_offset),
        )
    };
    let fail = |cause: String| {
        replay_error(
            tonic::Code::DataLoss,
            format!("StreamReplayError: {}: {cause}", range()),
        )
    };
    let runtime = runtime.ok_or_else(|| {
        fail(format!(
            "this Worker has no Stream runtime for the Store `{}`",
            stream.store
        ))
    })?;
    let count = entry.count as usize;
    let mut records: Vec<StreamRecord> = Vec::with_capacity(count);
    let mut from_offset = entry.after_offset.clone();
    let mut retry = fresh_retry;
    while records.len() < count {
        let left = count - records.len();
        let answer = runtime
            .read_records(ReadRecordsRequest {
                namespace: String::new(),
                r#ref: Some(stream.clone()),
                from_offset: from_offset.clone(),
                max_records: i32::try_from(left).unwrap_or(i32::MAX),
                wait: None,
                max_bytes: 0,
            })
            .await;
        let response = match answer {
            Ok(response) => {
                retry = fresh_retry;
                response
            }
            Err(error) => {
                if let Some(wait) = retry.next_wait(&error) {
                    tokio::time::sleep(wait).await;
                    continue;
                }
                // A Store that didn't answer says nothing of the Records, so this failure must not
                // count as Records that are gone.
                if error.reason == Reason::StorageUnavailable {
                    return Err(replay_error(
                        tonic::Code::Unavailable,
                        format!("StreamReplayUnavailable: {}: {error}", range()),
                    ));
                }
                return Err(fail(error.to_string()));
            }
        };
        if response.records.is_empty() {
            return Err(fail(format!(
                "the Store has {} of the {count} Records",
                records.len()
            )));
        }
        from_offset = response.last_offset;
        records.extend(response.records.into_iter().take(left));
    }
    let last = records.last().map(|record| record.offset.clone());
    if last.as_ref() != Some(&entry.last_offset) {
        return Err(fail(format!(
            "the last Record is at {}",
            offset_text(&last.unwrap_or_default())
        )));
    }
    if records_digest(&records).as_slice() != entry.digest.as_slice() {
        return Err(fail("the digest of the Records differs".to_string()));
    }
    Ok(records)
}

fn offset_text(offset: &[u8]) -> String {
    if offset.is_empty() {
        return "the start".to_string();
    }
    let hex: String = offset.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("offset {hex}")
}

/// A status that carries `message` as is, with `code` to tell a Store that stayed unavailable
/// from Records that a replay can't use.
fn replay_error(code: tonic::Code, message: String) -> tonic::Status {
    let mut status = tonic::Status::new(code, message);
    status.metadata_mut().insert(
        STREAM_REPLAY_ERROR_KEY,
        "true".parse().expect("valid metadata"),
    );
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Store answer with an offset over the limit is a Store error of the call. An offset at
    /// the limit passes.
    #[test]
    fn a_store_offset_over_its_limit_fails_the_answer() {
        let at_limit = vec![1; MAX_OFFSET_BYTES];
        let over = vec![1; MAX_OFFSET_BYTES + 1];
        assert!(store_offsets([at_limit.as_slice(), &[]]).is_ok());
        let error = store_offsets([at_limit.as_slice(), over.as_slice()]).unwrap_err();
        assert_eq!(error.reason, Reason::StorageCorrupt);
        assert_eq!(
            error.message,
            "The Store gave an offset of 1025 bytes, more than the limit of 1024"
        );
    }

    /// Each input of a call may reach its limit and not pass it.
    #[test]
    fn an_input_at_its_limit_passes() {
        let at_limit = StreamRef {
            id: "x".repeat(MAX_STREAM_ID_BYTES),
            store: "m".repeat(MAX_STORE_NAME_BYTES),
        };
        assert!(input_over_limit(&at_limit, &[1; MAX_OFFSET_BYTES]).is_none());
        let over = |what: &str, stream: &StreamRef, from_offset: &[u8]| {
            let failure = input_over_limit(stream, from_offset).expect("no refusal");
            assert!(failure.message.contains(what), "{}", failure.message);
        };
        let mut long_id = at_limit.clone();
        long_id.id.push('x');
        over("Stream id", &long_id, &[]);
        let mut long_store = at_limit.clone();
        long_store.store.push('m');
        over("Store name", &long_store, &[]);
        over("from_offset", &at_limit, &[1; MAX_OFFSET_BYTES + 1]);
    }
}
