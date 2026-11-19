//! Direct calls to Streams. Core appends to and closes a Stream on the Worker's Stream runtimes, in
//! tasks on the Worker's runtime, and gives each result to the run through a [RunSink].

use super::{RunInput, RunSink, StreamMetrics};
use crate::{
    Reason, StreamError, StreamRuntime,
    ids::{MAX_OFFSET_BYTES, MAX_STREAM_ID_BYTES},
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, Producer, StreamRef,
        WorkflowTaskEpoch, producer::Kind,
    },
};
use futures_util::future::{AbortHandle, AbortRegistration, Abortable};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::workflow_activation::{StreamAppended, resolve_stream_append},
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
        history::v1::{HistoryEvent, history_event},
    },
};
use tokio::{runtime::Handle, sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{Span, warn};

/// The Stream runtimes of a Worker, by the name of the Store that each serves. A Workflow call on a
/// Stream uses the runtime of its `StreamRef.store`.
#[derive(Clone, Debug, Default)]
pub struct StreamRuntimes {
    runtimes: HashMap<String, Arc<StreamRuntime>>,
}

impl StreamRuntimes {
    /// Builds the set from pairs of a Store name and its runtime.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self {
            runtimes: runtimes.into_iter().collect(),
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

/// What the Worker builds from its config for its Stream calls. Its runs share it.
#[derive(Debug)]
pub struct WorkerStreams {
    runtimes: StreamRuntimes,
    hooks: StreamTestHooks,
}

impl WorkerStreams {
    /// The Stream state of a Worker, from the values that Core takes from its config.
    pub fn new(runtimes: StreamRuntimes) -> Self {
        Self {
            runtimes,
            hooks: StreamTestHooks::default(),
        }
    }

    /// The Stream state with `hooks` in place of the default ones.
    #[doc(hidden)]
    pub fn with_test_hooks(self, hooks: StreamTestHooks) -> Self {
        Self { hooks, ..self }
    }

    /// The runtime of the Store `store`, if the Worker has one.
    pub(crate) fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.runtimes.get(store)
    }

    fn close_bound(&self) -> Duration {
        if let Some(bound) = self.hooks.close_bound {
            return bound;
        }
        CLOSE_SHUTDOWN_BOUND
    }
}

/// What lets a test see and shorten the wait of a graceful shutdown for a close.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct StreamTestHooks {
    /// A shorter bound in place of [CLOSE_SHUTDOWN_BOUND].
    pub close_bound: Option<Duration>,
    /// Told each time a graceful shutdown waits for a close in flight.
    pub shutdown_waits: Option<Arc<Notify>>,
}

/// The wait before the second try of a Store call that failed with `StorageUnavailable`. A short
/// Store outage should not reach Workflow code, as the retry of a Local Activity keeps it away.
pub(crate) const STORE_RETRY_FIRST_BACKOFF: Duration = Duration::from_millis(100);
/// The longest wait between two tries of such a call. The wait doubles up to it.
pub(crate) const STORE_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(2);
/// The bytes of one Workflow Task's Stream marker: the entries of appends and closes, and the table
/// of Streams. A long task with many small results would otherwise make a marker with no limit.
/// Past the bound, the task completes with a new one forced, as at a heartbeat, and the results
/// that didn't fit go to the new task.
///
/// Core counts an estimate that is never below the encoded size. No single entry comes near the
/// bound, since each input is bounded. A Stream of the table has at most 8202 bytes, an id and a
/// Store name of 4096 bytes each. An append entry has at most 4249 bytes, and a close entry 4251.
/// The test `the_largest_entries_are_far_under_the_entry_bound` checks each against its estimate.
pub const MAX_MARKER_ENTRY_BYTES: usize = 256 * 1024;
/// The bytes of the message of a Stream failure that Workflow code gets and the marker records. A
/// Store can give an error text of any size, and one failure must not fill a marker.
pub const MAX_FAILURE_MESSAGE_BYTES: usize = 4096;
/// How long a graceful shutdown waits for a close in flight, from the start of the shutdown or
/// of the close if it is later. The Worker has no other bound for the work of its runs at a
/// shutdown, and a Store that never answers must not hold the shutdown for ever.
pub(crate) const CLOSE_SHUTDOWN_BOUND: Duration = Duration::from_secs(10);

/// The tries of one Store call that fails with `StorageUnavailable`.
#[derive(Clone, Copy, Debug)]
struct StoreRetry {
    backoff: Duration,
}

impl StoreRetry {
    /// The tries of a write, which go on until the call is stopped.
    fn unbounded() -> Self {
        Self {
            backoff: STORE_RETRY_FIRST_BACKOFF,
        }
    }

    /// The wait before the next try of a call that failed with `error`, or `None` when the call
    /// resolves with it. Only `StorageUnavailable` may pass, since every other reason is the
    /// Store's answer and a new try would get it again.
    fn next_wait(&mut self, error: &StreamError) -> Option<Duration> {
        if error.reason != Reason::StorageUnavailable {
            return None;
        }
        let wait = self.backoff;
        self.backoff = (self.backoff * 2).min(STORE_RETRY_MAX_BACKOFF);
        Some(wait)
    }
}

/// Runs the write `call` again while it fails with `StorageUnavailable`, with no bound in time. It
/// ends only when the call answers, or when its task is stopped. A graceful shutdown doesn't stop
/// it. After each failure, and until the call resolves, `note` says that the write is in an
/// outage, so that a Workflow Task that waits for it can fail at its heartbeat.
async fn retry_unavailable<T, F>(
    mut call: impl FnMut() -> F,
    note: &OutageNote,
) -> Result<T, StreamError>
where
    F: Future<Output = Result<T, StreamError>>,
{
    let mut retry = StoreRetry::unbounded();
    loop {
        let error = match call().await {
            Ok(answer) => {
                note.clear();
                return Ok(answer);
            }
            Err(error) => error,
        };
        let Some(wait) = retry.next_wait(&error) else {
            note.clear();
            return Err(error);
        };
        note.set(&error);
        tokio::time::sleep(wait).await;
    }
}

/// A write that is in a Store outage. It has failed with `StorageUnavailable` and waits for its
/// next try, which comes with no bound in time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOutage {
    /// The id of the Stream that the write is for.
    pub stream_id: String,
    /// The name of the Store that the write is for.
    pub store: String,
    /// The whole text of the failure that a Workflow Task gets for this outage, in the form
    /// `StreamStoreUnavailable: <call> of Stream "<id>" on Store "<store>": <error>`.
    pub message: String,
}

/// The outage of one write, and the run that waits for it.
#[derive(Debug)]
struct OutageEntry {
    run_id: String,
    outage: WriteOutage,
}

/// The writes in an outage, by call id.
type Outages = Arc<Mutex<HashMap<u64, OutageEntry>>>;

/// The place of one write in the manager's outages. It lives as long as the call's future, so that
/// a call that is stopped or dropped leaves no outage behind.
struct OutageNote {
    outages: Outages,
    call_id: u64,
    run_id: String,
    /// `append` or `close`.
    call: &'static str,
    stream: StreamRef,
}

impl OutageNote {
    /// Notes that the write waits for its next try after `error`, which replaces an older note.
    fn set(&self, error: &StreamError) {
        let message = format!(
            "StreamStoreUnavailable: {} of Stream \"{}\" on Store \"{}\": {}",
            self.call,
            self.stream.id,
            self.stream.store,
            cut_message(error.to_string()),
        );
        self.outages.lock().insert(
            self.call_id,
            OutageEntry {
                run_id: self.run_id.clone(),
                outage: WriteOutage {
                    stream_id: self.stream.id.clone(),
                    store: self.stream.store.clone(),
                    message,
                },
            },
        );
    }

    fn clear(&self) {
        self.outages.lock().remove(&self.call_id);
    }
}

impl Drop for OutageNote {
    fn drop(&mut self) {
        self.clear();
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

/// A call that the machines ask the Worker to make or to stop.
#[derive(Debug)]
pub enum StreamRequest {
    /// Starts an append.
    Append(AppendSpec),
    /// Starts a close.
    Close(CloseSpec),
    /// Stops the call with this id.
    Cancel(u64),
}

/// An append to run.
#[derive(Debug)]
pub struct AppendSpec {
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
pub struct CloseSpec {
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
pub struct StreamResolution {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) result: StreamCallResult,
}

#[derive(Debug)]
pub(crate) enum StreamCallResult {
    Append(resolve_stream_append::Status),
    /// `None` when the Stream is closed.
    Close(Option<Failure>),
}

/// Runs the Stream calls of a Worker's runs. Its tasks run on the Worker's runtime, not on the
/// workflow processing thread, so that Store work never holds up the runs.
pub struct StreamManager {
    streams: Arc<WorkerStreams>,
    handle: Handle,
    local_tx: Arc<dyn RunSink>,
    /// The run and the abort handle of each running call, by its call id, and whether the call
    /// outlives its run.
    tasks: Arc<Mutex<HashMap<u64, Task>>>,
    /// The closes that the Store didn't answer yet. A graceful shutdown waits for them, since
    /// History has their intent and Workflow code may never await them.
    closes_in_flight: Arc<AtomicUsize>,
    /// The writes that wait for their next try after `StorageUnavailable`, by call id. A run asks
    /// for them at the heartbeat of its Workflow Task.
    outages: Outages,
    /// The Worker's shutdown, from whose start a close in flight has [CLOSE_SHUTDOWN_BOUND].
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
    /// A manager that runs calls on `handle` and sends their results to `local_tx`.
    pub fn new(
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
            outages: Default::default(),
            shutdown,
        }
    }

    /// True while a close waits for the Store, so that a graceful shutdown isn't done.
    pub fn has_closes_in_flight(&self) -> bool {
        let in_flight = self.closes_in_flight.load(Ordering::Acquire) > 0;
        if in_flight && let Some(waits) = &self.streams.hooks.shutdown_waits {
            waits.notify_one();
        }
        in_flight
    }

    /// Starts or stops the calls of a run. `epoch` is the run's current Workflow Task, which an
    /// append writes with, or `None` when the run has no task in hand. `metrics` are the run's.
    pub fn dispatch(
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
                    self.outages.lock().remove(&call_id);
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
                            let note = self.outage_note(run_id, call_id, "append", &spec.stream);
                            let call = append(runtime, spec, epoch, sink, timer, note);
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
                            let note = self.outage_note(run_id, call_id, "close", &stream);
                            let abandoned = timer.as_ref().map(|timer| timer.metrics.clone());
                            let call = close(runtime, spec, epoch, sink, timer, in_flight, note);
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
    pub fn cancel_run(&self, run_id: &str) {
        self.cancel(run_id, false);
    }

    /// Stops every call of the run, its closes too. A run uses it when it fails its Workflow Task
    /// for a write in a Store outage. History has the intent of each close, and a close outlives
    /// its run. The next try of the task sends each recorded close again, so a close that stayed
    /// would leave one more behind with each failed task.
    pub fn cancel_run_with_closes(&self, run_id: &str) {
        self.cancel(run_id, true);
    }

    fn cancel(&self, run_id: &str, with_closes: bool) {
        let mut stopped = vec![];
        self.tasks.lock().retain(|call_id, task| {
            if task.run_id == run_id && (with_closes || !task.outlives_run) {
                task.handle.abort();
                stopped.push(*call_id);
                false
            } else {
                true
            }
        });
        // An aborted call drops its own note when the runtime next polls it. A run asks for its
        // outages right after it stops its calls, so the notes must go with the calls.
        let mut outages = self.outages.lock();
        for call_id in stopped {
            outages.remove(&call_id);
        }
    }

    /// The outage of one write of the run, the one with the lowest call id, or `None` when no
    /// write of the run waits for its next try after `StorageUnavailable`. A write that the Store
    /// has in hand and didn't answer yet is not in an outage.
    pub fn write_outage(&self, run_id: &str) -> Option<WriteOutage> {
        self.outages
            .lock()
            .iter()
            .filter(|(_, entry)| entry.run_id == run_id)
            .min_by_key(|(call_id, _)| **call_id)
            .map(|(_, entry)| entry.outage.clone())
    }

    fn outage_note(
        &self,
        run_id: &str,
        call_id: u64,
        call: &'static str,
        stream: &StreamRef,
    ) -> OutageNote {
        OutageNote {
            outages: self.outages.clone(),
            call_id,
            run_id: run_id.to_string(),
            call,
            stream: stream.clone(),
        }
    }

    /// Sends a heartbeat timeout for the run at `deadline`, for a Worker with no Local Activity
    /// manager to keep the timer.
    pub fn start_heartbeat_timeout(
        &self,
        run_id: String,
        deadline: std::time::Instant,
        abort_reg: AbortRegistration,
    ) {
        let local_tx = self.local_tx.clone();
        let span = Span::current();
        // A duration, so that the timer runs on the runtime's own clock, as other call timers do.
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
    note: OutageNote,
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
    // again. The tries have no bound in time. A graceful shutdown gives them up, as it gives up
    // the first one.
    let runtime = &*runtime;
    let request = &request;
    let result = retry_unavailable(move || runtime.close_stream(request.clone()), &note)
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
    note: OutageNote,
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
        // gives the offsets of the first write. The tries have no bound in time, and a graceful
        // shutdown doesn't stop them. The task that waits for this result fails at its heartbeat
        // while they go on, so Workflow code never gets the outage as an error of the append.
        let runtime = &*runtime;
        let request = &request;
        retry_unavailable(move || runtime.append_records(request.clone()), &note).await
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
pub const MAX_STORE_NAME_BYTES: usize = 4096;

/// The failure of a Stream call of Workflow code whose Stream is over its limit, if it is. Core
/// refuses such a call before any Store call and records nothing of it in the marker, so that no
/// input of Workflow code makes a marker entry of any size. The message gives the limit and not
/// the value.
pub fn input_over_limit(stream: &StreamRef) -> Option<Failure> {
    let over = |what: &str, bytes: usize, limit: usize| {
        (bytes > limit).then(|| {
            failure(&StreamError::invalid(format!(
                "{what} has {bytes} bytes, more than the limit of {limit}"
            )))
        })
    };
    over("The Stream id", stream.id.len(), MAX_STREAM_ID_BYTES)
        .or_else(|| over("The Store name", stream.store.len(), MAX_STORE_NAME_BYTES))
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

/// The marker detail that holds the encoded `StreamMarkerData`.
pub const STREAM_MARKER_DATA_KEY: &str = "data";

/// The encoded marker data of a `core_stream` marker event, if the event is one.
pub fn stream_marker_payload_of(event: &HistoryEvent) -> Option<&Payload> {
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
        assert!(input_over_limit(&at_limit).is_none());
        let over = |what: &str, stream: &StreamRef| {
            let failure = input_over_limit(stream).expect("no refusal");
            assert!(failure.message.contains(what), "{}", failure.message);
        };
        let mut long_id = at_limit.clone();
        long_id.id.push('x');
        over("Stream id", &long_id);
        let mut long_store = at_limit.clone();
        long_store.store.push('m');
        over("Store name", &long_store);
    }

    /// A write goes on with no bound in time. The wait of a write stays at the longest backoff.
    #[tokio::test(start_paused = true)]
    async fn a_retry_with_no_bound_goes_on() {
        let down = StreamError::storage_unavailable("down");
        let mut write = StoreRetry::unbounded();
        assert_eq!(write.next_wait(&down), Some(STORE_RETRY_FIRST_BACKOFF));
        for _ in 0..100 {
            let wait = write.next_wait(&down).expect("a write has no bound");
            assert!(wait <= STORE_RETRY_MAX_BACKOFF, "{wait:?}");
            tokio::time::advance(wait).await;
        }
        assert_eq!(write.next_wait(&down), Some(STORE_RETRY_MAX_BACKOFF));
        let other = StreamError::invalid("bad");
        assert_eq!(write.next_wait(&other), None);
    }

    struct NoSink;

    impl RunSink for NoSink {
        fn send(&self, _: RunInput, _: Span) {}

        fn closed(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(std::future::pending())
        }
    }

    fn manager() -> StreamManager {
        let streams = WorkerStreams::new(StreamRuntimes::default());
        StreamManager::new(
            Arc::new(streams),
            Handle::current(),
            Arc::new(NoSink),
            CancellationToken::new(),
        )
    }

    fn stream_of(id: &str) -> StreamRef {
        StreamRef {
            id: id.to_string(),
            store: "mem".to_string(),
        }
    }

    /// A note has no outage until the write fails, says the text of its last error, and goes away
    /// when it is cleared or dropped. A run sees the write with the lowest call id.
    #[tokio::test]
    async fn a_note_shows_its_outage_until_it_is_cleared_or_dropped() {
        let manager = manager();
        let late = manager.outage_note("run", 9, "close", &stream_of("late"));
        let early = manager.outage_note("run", 7, "append", &stream_of("early"));
        assert!(manager.write_outage("run").is_none());
        late.set(&StreamError::storage_unavailable("down"));
        early.set(&StreamError::storage_unavailable("still down"));
        let outage = manager.write_outage("run").expect("an outage");
        assert_eq!(
            outage,
            WriteOutage {
                stream_id: "early".to_string(),
                store: "mem".to_string(),
                message: "StreamStoreUnavailable: append of Stream \"early\" on Store \"mem\": \
                          stream_storage_unavailable: still down"
                    .to_string(),
            }
        );
        assert!(manager.write_outage("another run").is_none());
        early.clear();
        assert_eq!(manager.write_outage("run").unwrap().stream_id, "late");
        drop(late);
        assert!(manager.write_outage("run").is_none());
    }

    /// `cancel_run` stops a write and its note, and keeps a close. `cancel_run_with_closes` stops
    /// the close too, and a run that asks for its outages right after finds none.
    #[tokio::test]
    async fn a_cancel_removes_the_notes_of_the_calls_that_it_stops() {
        let manager = manager();
        let down = StreamError::storage_unavailable("down");
        for (call_id, call, outlives_run) in [(1, "append", false), (2, "close", true)] {
            let note = manager.outage_note("run", call_id, call, &stream_of("s"));
            note.set(&down);
            manager.spawn("run", call_id, outlives_run, async move {
                let _note = note;
                std::future::pending::<()>().await;
            });
        }
        manager.cancel_run("run");
        assert!(
            manager
                .write_outage("run")
                .unwrap()
                .message
                .contains("close of")
        );
        manager.cancel_run_with_closes("run");
        assert!(manager.write_outage("run").is_none());
        assert!(manager.tasks.lock().is_empty());
    }
}
