//! Runs the Stream calls of a Worker's runs, one task for each call.

use super::{StreamRuntimes, StreamSettings, read::ReadDone};
use crate::telemetry::metrics::{FailureReason, MetricsContext, failure_reason};
use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};
use parking_lot::Mutex;
use std::{collections::HashMap, fmt, future::Future, sync::Arc, time::Duration};
use temporalio_common::protos::{
    coresdk::external_data::{StreamAppendOffsets, StreamFailure, stream_append_result},
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    },
};
use temporalio_streams::{
    AnyReason, AppendError, ReadError, ReadPage, StreamError, StreamRuntime,
    proto::{
        StreamErrorReason,
        sdk::{AppendRecordsRequest, ReadRecordsRequest},
    },
};
use tokio::{runtime::Handle, task::AbortHandle, time::Instant};

/// The maximum length of a failure message that Workflow code gets and a marker records, in bytes.
/// A Store can return error text of any size, so the message needs a bound.
const MAX_FAILURE_MESSAGE_BYTES: usize = 4096;

/// The maximum random change of the wait before the next try, as a fraction of that wait. A retry
/// of a client call has the same value.
const RETRY_RANDOMIZATION: f64 = 0.2;

/// The shortest time between two asks of a read whose Store answers with no Record before the
/// wait is up.
pub(super) const MIN_ASK_AGAIN: Duration = Duration::from_millis(100);

/// A call of a run to start or to stop.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Most requests start a call
pub(crate) enum Request {
    Run(Call),
    /// Stops the call with this id, which then gives no result.
    Stop(u64),
}

/// A call to start, with the request that each of its tries sends.
#[derive(Debug)]
pub(crate) struct Call {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    /// The Store name of the call's ref, which picks the runtime of the call.
    pub(crate) store: String,
    pub(crate) op: Op,
}

#[derive(Debug)]
pub(crate) enum Op {
    /// An append, with the request id that each of its tries passes.
    Append(AppendRecordsRequest, String),
    Read(ReadCall),
}

/// A read to run.
#[derive(Debug)]
pub(crate) struct ReadCall {
    /// The request of each try, with the `from_offset` and the page size of the read.
    pub(crate) request: ReadRecordsRequest,
    /// How long the read waits for a Record.
    pub(crate) wait: Duration,
    /// True for a check, which is one try with no wait.
    pub(crate) check: bool,
    pub(crate) last_try: LastTry,
}

/// The failure of the last try of a read, while the read waits to try again. The run fails its task
/// if a read has one at the read limit, at the completion or at the heartbeat.
pub(crate) type LastTry = Arc<Mutex<Option<StreamFailure>>>;

/// The result of a call, for its run.
#[derive(Debug)]
pub(crate) struct StreamResolution {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) outcome: Outcome,
}

#[derive(Debug)]
pub(crate) enum Outcome {
    Append(stream_append_result::Result),
    Read(ReadDone),
}

type Sink = Arc<dyn Fn(String, StreamResolution) + Send + Sync>;

/// The running calls, keyed by run id and call id.
type Tasks = Arc<Mutex<HashMap<String, HashMap<u64, Task>>>>;

struct Task {
    abort: AbortHandle,
    /// The failure of the last try, while the call waits to try again.
    outage: Option<String>,
}

/// The runner of a Worker's Stream calls. It runs them on the Worker's runtime, so Store work
/// never blocks the thread of the runs, and dropping it stops every call.
pub(crate) struct StreamManager {
    runtimes: StreamRuntimes,
    handle: Handle,
    sink: Sink,
    tasks: Tasks,
}

impl StreamManager {
    /// Creates a manager that runs calls on `handle`, and gives each result to `sink` with its run
    /// id.
    pub(crate) fn new(
        runtimes: StreamRuntimes,
        handle: Handle,
        sink: impl Fn(String, StreamResolution) + Send + Sync + 'static,
    ) -> Self {
        Self {
            runtimes,
            handle,
            sink: Arc::new(sink),
            tasks: Default::default(),
        }
    }

    /// Starts and stops calls of the run `run_id`. `metrics` gives the metrics of a Store.
    pub(crate) fn dispatch(
        &self,
        run_id: &str,
        requests: Vec<Request>,
        mut metrics: impl FnMut(&str) -> MetricsContext,
    ) {
        // The lock is held across each spawn, so a call's entry exists even if the call ends at
        // once.
        let mut tasks = self.tasks.lock();
        for request in requests {
            match request {
                Request::Run(call) => {
                    let call_id = call.call_id;
                    let metrics = metrics(&call.store);
                    let abort = self.spawn(run_id, call, metrics);
                    let run = tasks.entry(run_id.to_string()).or_default();
                    run.insert(
                        call_id,
                        Task {
                            abort,
                            outage: None,
                        },
                    );
                }
                Request::Stop(call_id) => {
                    if let Some(run) = tasks.get_mut(run_id)
                        && let Some(task) = run.remove(&call_id)
                    {
                        task.abort.abort();
                        if run.is_empty() {
                            tasks.remove(run_id);
                        }
                    }
                }
            }
        }
    }

    /// Stops every call of the run. The run ignores a result that a call already sent.
    pub(crate) fn cancel_run(&self, run_id: &str) {
        if let Some(run) = self.tasks.lock().remove(run_id) {
            for task in run.into_values() {
                task.abort.abort();
            }
        }
    }

    /// Returns the failure of the run's call that waits to try again after an outage. If several
    /// do, it's the one with the lowest call id.
    pub(crate) fn outage(&self, run_id: &str) -> Option<String> {
        let tasks = self.tasks.lock();
        let run = tasks.get(run_id)?;
        run.iter()
            .filter_map(|(call_id, task)| Some((*call_id, task.outage.as_ref()?)))
            .min_by_key(|(call_id, _)| *call_id)
            .map(|(_, outage)| outage.clone())
    }

    fn spawn(&self, run_id: &str, call: Call, metrics: MetricsContext) -> AbortHandle {
        let Call {
            call_id,
            seq,
            store,
            op,
        } = call;
        let (what, stream) = match &op {
            Op::Append(request, _) => ("append", &request.r#ref),
            Op::Read(read) => ("read", &read.request.r#ref),
        };
        let stream_id = stream.as_ref().map_or("", |stream| &stream.id);
        let runtime = (self.runtimes.get(&store).cloned())
            .expect("The run starts no call on a Store with no runtime");
        let settings = self.runtimes.settings;
        let note = Note {
            tasks: self.tasks.clone(),
            run_id: run_id.to_string(),
            call_id,
            what: format!("{what} of Stream \"{stream_id}\" on Store \"{store}\""),
        };
        let sink = self.sink.clone();
        let task = self.handle.spawn(async move {
            let outcome = run(&runtime, seq, op, settings, &note, &metrics).await;
            if note.end() {
                sink(
                    note.run_id,
                    StreamResolution {
                        call_id,
                        seq,
                        outcome,
                    },
                );
            }
        });
        task.abort_handle()
    }
}

impl Drop for StreamManager {
    fn drop(&mut self) {
        for run in self.tasks.lock().drain() {
            for task in run.1.into_values() {
                task.abort.abort();
            }
        }
    }
}

/// Runs the call `seq` on `runtime`, and returns its outcome. The metrics count each try when it
/// starts, as those of a Local Activity count each attempt, and a check is one try. They time a
/// try of an append once it ends, so a try that is stopped has only its count, and a read has no
/// latency.
async fn run(
    runtime: &StreamRuntime,
    seq: u32,
    op: Op,
    settings: StreamSettings,
    note: &Note,
    metrics: &MetricsContext,
) -> Outcome {
    match op {
        Op::Append(request, request_id) => {
            let send = || {
                metrics.stream_append_executed();
                runtime.append_records(request.clone(), request_id.clone())
            };
            let tried = |took: Duration, error: Option<&AppendError>| {
                metrics.stream_append_exec_latency(took);
                if let Some(error) = error {
                    failed(metrics, error).stream_append_execution_failed();
                }
            };
            let answer = tries(settings, note, send, tried).await;
            Outcome::Append(match answer {
                Ok(appended) => stream_append_result::Result::Appended(StreamAppendOffsets {
                    first_offset: appended.first_offset,
                    last_offset: appended.last_offset,
                }),
                Err(error) => stream_append_result::Result::Failure(recorded(&error)),
            })
        }
        Op::Read(read) => {
            let stream = (read.request.r#ref.clone()).expect("A read has the ref of its command");
            let from_offset = read.request.from_offset.clone();
            let page = read_page(runtime, read, settings, metrics).await;
            if let Ok(page) = &page {
                metrics.stream_read_records(page.response.records.len() as u64);
            }
            let page = page.map_err(|error| recorded(&error));
            Outcome::Read(ReadDone::new(&stream, seq, from_offset, page))
        }
    }
}

/// Reads the page of Records after the `from_offset` of `read`. A page is one answer of the Store,
/// which waits up to the read's wait for the first Record. An answer with no Record before the
/// wait is up makes the read ask again from its `last_offset`, at most once per
/// [`MIN_ASK_AGAIN`]. A try that fails with `StoreUnavailable`, or gets no answer by its deadline,
/// is the last try of the read, and the read asks again after a backoff. A check is one try with
/// no wait, which returns its outage. The run fails the task of a read whose last try failed, so a
/// read notes no outage in the manager.
async fn read_page(
    runtime: &StreamRuntime,
    read: ReadCall,
    settings: StreamSettings,
    metrics: &MetricsContext,
) -> Result<ReadPage, ReadError> {
    let ReadCall {
        mut request,
        wait,
        check,
        last_try,
    } = read;
    if check {
        request.wait = None;
        let deadline = settings.read_check_deadline;
        metrics.stream_read_executed();
        let read = runtime.read_records_with_more(request);
        let answer = match tokio::time::timeout(deadline, read).await {
            Ok(answer) => answer,
            Err(_) => Err(StreamError::store_unavailable(format!(
                "The Store gave no answer in {deadline:?}"
            ))),
        };
        if let Err(error) = &answer {
            failed(metrics, error).stream_read_execution_failed();
        }
        return answer;
    }
    // A time past the end of the clock never ends the wait.
    let wait_until = Instant::now().checked_add(wait);
    let mut waits = retry_waits(&settings);
    let longest = prost_types::Duration {
        seconds: i64::MAX,
        nanos: 0,
    };
    loop {
        let wait = wait_until.map_or(wait, |at| at.saturating_duration_since(Instant::now()));
        request.wait = (!wait.is_zero()).then(|| wait.try_into().unwrap_or(longest));
        // The Store can take the whole wait to answer, so the deadline of the try adds it.
        let deadline = settings.try_deadline.saturating_add(wait);
        let asked = Instant::now();
        metrics.stream_read_executed();
        let read = runtime.read_records_with_more(request.clone());
        let answer = match tokio::time::timeout(deadline, read).await {
            Ok(Err(error)) if !is_outage(&error) => {
                failed(metrics, &error).stream_read_execution_failed();
                *last_try.lock() = None;
                return Err(error);
            }
            Ok(answer) => answer,
            Err(_) => Err(StreamError::store_unavailable(format!(
                "The Store gave no answer in {deadline:?}"
            ))),
        };
        match answer {
            Ok(page) => {
                *last_try.lock() = None;
                let left = wait_until.map(|at| at.saturating_duration_since(Instant::now()));
                if !page.response.records.is_empty() || left.is_some_and(|left| left.is_zero()) {
                    return Ok(page);
                }
                let next = asked + MIN_ASK_AGAIN;
                match wait_until {
                    Some(at) if next >= at => {
                        tokio::time::sleep_until(at).await;
                        return Ok(page);
                    }
                    _ => tokio::time::sleep_until(next).await,
                }
                request.from_offset = page.response.last_offset;
                waits = retry_waits(&settings);
            }
            Err(error) => {
                failed(metrics, &error).stream_read_execution_failed();
                *last_try.lock() = Some(recorded(&error));
                tokio::time::sleep(next_wait(&mut waits)).await;
            }
        }
    }
}

/// Sends the request again while the Store is in an outage. The loop has no time limit of its own,
/// because the run fails its Workflow Task if, at the task's heartbeat, a call has had no answer
/// since a try that failed. A resend is safe, because a Store doesn't write a batch whose request
/// id it still remembers.
/// `tried` gets the duration of each try, and its error if it failed.
async fn tries<T, R, F>(
    settings: StreamSettings,
    note: &Note,
    mut send: impl FnMut() -> F,
    tried: impl Fn(Duration, Option<&StreamError<R>>),
) -> Result<T, StreamError<R>>
where
    R: Copy + From<AnyReason> + Into<StreamErrorReason>,
    F: Future<Output = Result<T, StreamError<R>>>,
{
    let mut waits = retry_waits(&settings);
    loop {
        let started = Instant::now();
        let error = match tokio::time::timeout(settings.try_deadline, send()).await {
            Ok(Err(error)) if is_outage(&error) => error,
            Ok(answer) => {
                tried(started.elapsed(), answer.as_ref().err());
                note.set(None);
                return answer;
            }
            Err(_) => StreamError::store_unavailable(format!(
                "The Store gave no answer in {:?}",
                settings.try_deadline
            )),
        };
        tried(started.elapsed(), Some(&error));
        note.set(Some(&error));
        tokio::time::sleep(next_wait(&mut waits)).await;
    }
}

/// Returns the waits between the tries of a call, which double from `retry_initial_interval` up to
/// `retry_max_interval`.
pub(super) fn retry_waits(settings: &StreamSettings) -> ExponentialBackoff {
    ExponentialBuilder::new()
        .with_min_delay(settings.retry_initial_interval)
        .with_factor(2.0)
        .with_max_delay(settings.retry_max_interval)
        .without_max_times()
        .build()
}

/// Returns the next wait of `waits`, made longer or shorter at random by up to
/// [`RETRY_RANDOMIZATION`] of its length, so that the calls of many runs don't all try again at
/// the same moment.
pub(super) fn next_wait(waits: &mut ExponentialBackoff) -> Duration {
    let wait = waits.next().unwrap_or_default();
    let jitter = 1.0 + RETRY_RANDOMIZATION * (2.0 * rand::random::<f64>() - 1.0);
    Duration::try_from_secs_f64(wait.as_secs_f64() * jitter).unwrap_or(wait)
}

/// The handle of one call to its entry in the manager.
struct Note {
    tasks: Tasks,
    run_id: String,
    call_id: u64,
    what: String,
}

impl Note {
    fn set(&self, error: Option<&dyn fmt::Display>) {
        let mut tasks = self.tasks.lock();
        let task = tasks
            .get_mut(&self.run_id)
            .and_then(|run| run.get_mut(&self.call_id));
        if let Some(task) = task {
            task.outage = error.map(|error| {
                format!(
                    "StreamStoreUnavailable: {}: {}",
                    self.what,
                    cut(error.to_string())
                )
            });
        }
    }

    /// Removes the task. Returns false for a call that was stopped, which gives no result.
    fn end(&self) -> bool {
        let mut tasks = self.tasks.lock();
        let Some(run) = tasks.get_mut(&self.run_id) else {
            return false;
        };
        let ended = run.remove(&self.call_id).is_some();
        if run.is_empty() {
            tasks.remove(&self.run_id);
        }
        ended
    }
}

/// Returns `metrics` with the reason of `error`, such as `stream_not_found`, as `failure_reason`.
fn failed<R: Copy + Into<StreamErrorReason>>(
    metrics: &MetricsContext,
    error: &StreamError<R>,
) -> MetricsContext {
    metrics.with_new_attrs([failure_reason(FailureReason::StreamError(error.token()))])
}

/// Returns true if `error` says that the Store is in an outage, so that another try of the call may
/// succeed.
pub(super) fn is_outage<R: Copy + Into<StreamErrorReason>>(error: &StreamError<R>) -> bool {
    error.reason.into() == StreamErrorReason::StoreUnavailable
}

/// Returns the reason and the message of `error`, as a marker records a failed call.
pub(super) fn recorded<R: Copy + Into<StreamErrorReason>>(error: &StreamError<R>) -> StreamFailure {
    StreamFailure {
        reason: error.reason.into().as_str_name().to_string(),
        message: cut(error.message.clone()),
    }
}

/// Returns the failure that Workflow code gets for a failed call that a marker records.
pub(super) fn failure_of(recorded: &StreamFailure) -> Failure {
    let reason = StreamErrorReason::from_str_name(&recorded.reason)
        .expect("The decode of a marker checks the reason of each failure");
    let message = recorded.message.clone();
    failure(&StreamError { reason, message })
}

/// Returns the failure that Workflow code gets for `error`. Its message is the error's message, its
/// type is the reason's token, and its details hold the encoded
/// [`StreamErrorDetails`](temporalio_streams::proto::StreamErrorDetails).
fn failure<R: Copy + Into<StreamErrorReason>>(error: &StreamError<R>) -> Failure {
    Failure {
        message: cut(error.message.clone()),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: error.token(),
                non_retryable: !is_outage(error),
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

/// Cuts `text` on a character boundary to at most [`MAX_FAILURE_MESSAGE_BYTES`], with `...` at the
/// end.
pub(super) fn cut(text: String) -> String {
    if text.len() <= MAX_FAILURE_MESSAGE_BYTES {
        return text;
    }
    let mut end = MAX_FAILURE_MESSAGE_BYTES - "...".len();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::super::{
        read::After,
        test_store::{Entry, Fault, Model, ModelStore, client},
    };
    use super::*;
    use crate::{
        telemetry::metrics::{MetricsCallBuffer, stream_store},
        test_help::stream_store::{MemoryStore, Op as StoreOp},
    };
    use prost::Message;
    use prost_types::Timestamp;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::any::Any;
    use temporalio_common::{
        protos::coresdk::external_data::stream_read_result,
        telemetry::{
            TelemetryOptions,
            metrics::{
                CoreMeter,
                core::{
                    BufferAttributes, BufferInstrumentRef, CustomMetricAttributes,
                    MetricCallBufferer, MetricEvent, MetricUpdateVal,
                },
            },
            telemetry_init,
        },
    };
    use temporalio_streams::{
        AnyError, ReadReason, StreamStore,
        proto::{
            AppendRecord, CreateStreamRequest, Producer, ReadRecordsRequest as StoreRead,
            StreamErrorDetails, StreamRecord, WorkflowProducer, WorkflowTaskAttempt,
            producer::Kind, sdk::StreamRef,
        },
        record::{StoreLimits, field_len},
    };
    use tokio::sync::mpsc;

    type Results = mpsc::UnboundedReceiver<(String, StreamResolution)>;

    /// Returns a manager of a Store that has the Stream `s`.
    async fn manager() -> (Arc<MemoryStore>, StreamManager, Results) {
        let store = Arc::new(MemoryStore::default());
        let create = CreateStreamRequest {
            stream_id: "s".to_string(),
            ..Default::default()
        };
        store.create_stream(create, "c".to_string()).await.unwrap();
        let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
        let runtimes = StreamRuntimes::new([("mem".to_string(), Arc::new(runtime))]);
        let (tx, rx) = mpsc::unbounded_channel();
        let sink = move |run_id, resolution| tx.send((run_id, resolution)).unwrap();
        let manager = StreamManager::new(runtimes, Handle::current(), sink);
        (store, manager, rx)
    }

    fn no_metrics(_: &str) -> MetricsContext {
        MetricsContext::no_op()
    }

    fn stream() -> StreamRef {
        StreamRef {
            id: "s".to_string(),
            store: "mem".to_string(),
        }
    }

    fn producer() -> Option<Producer> {
        Some(Producer {
            kind: Some(Kind::Workflow(WorkflowProducer {
                namespace: "n".to_string(),
                workflow_id: "w".to_string(),
                run_id: "r".to_string(),
                attempt: Some(WorkflowTaskAttempt {
                    started_event_id: 3,
                    attempt: 1,
                    started_time: Some(Timestamp::default()),
                }),
            })),
        })
    }

    fn append(call_id: u64) -> Request {
        Request::Run(Call {
            call_id,
            seq: 1,
            store: "mem".to_string(),
            op: Op::Append(
                AppendRecordsRequest {
                    r#ref: Some(stream()),
                    producer: producer(),
                    records: vec![AppendRecord {
                        body: Some(Payload::default()),
                    }],
                },
                "a".to_string(),
            ),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_try_with_no_answer_is_an_outage() {
        let (store, manager, mut results) = manager().await;
        store.hold(StoreOp::Append);
        manager.dispatch("r", vec![append(1)], no_metrics);
        tokio::time::sleep(Duration::from_millis(5010)).await;
        let outage = "StreamStoreUnavailable: append of Stream \"s\" on Store \"mem\": \
                      stream_store_unavailable: The Store gave no answer in 5s";
        assert_eq!(manager.outage("r").as_deref(), Some(outage));
        store.release();
        let (run_id, resolution) = results.recv().await.unwrap();
        assert_eq!(run_id, "r");
        assert!(matches!(
            resolution.outcome,
            Outcome::Append(stream_append_result::Result::Appended(_))
        ));
        assert_eq!(manager.outage("r"), None);
        let tries = store.requests();
        assert!(tries.len() >= 2, "{tries:?}");
        assert!(tries.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancel_stops_the_calls_of_a_run_and_their_outage() {
        let (store, manager, mut results) = manager().await;
        let down = || AnyError::store_unavailable("The Store is down");
        store.fail(StoreOp::Append, down(), usize::MAX);
        manager.dispatch("a", vec![append(1), append(2), append(3)], no_metrics);
        manager.dispatch("b", vec![append(4)], no_metrics);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(manager.outage("a").is_some());
        manager.dispatch("a", vec![Request::Stop(3)], no_metrics);
        manager.cancel_run("a");
        assert_eq!(manager.outage("a"), None);
        assert!(manager.outage("b").is_some());
        store.fail(StoreOp::Append, down(), 0);
        let (run_id, resolution) = results.recv().await.unwrap();
        assert_eq!((run_id.as_str(), resolution.call_id), ("b", 4));
        assert!(matches!(
            resolution.outcome,
            Outcome::Append(stream_append_result::Result::Appended(_))
        ));
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(results.try_recv().is_err());
        let after = store.requests().len();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(store.requests().len(), after);
    }

    /// The labels of an attribute set of the metrics buffer.
    #[derive(Debug)]
    struct Labels(HashMap<String, String>);

    impl CustomMetricAttributes for Labels {
        fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
            self
        }
    }

    /// The name of an instrument of the metrics buffer.
    #[derive(Clone, Debug)]
    struct Instrument(String);

    impl BufferInstrumentRef for Instrument {}

    fn labels(attributes: &BufferAttributes) -> Arc<Labels> {
        (attributes.get().clone().as_any().downcast::<Labels>()).unwrap()
    }

    /// Returns the sum of each Stream counter and the sample count of each Stream histogram in
    /// `buffer`, by the name of the metric and its `failure_reason`.
    fn totals(buffer: &MetricsCallBuffer<Instrument>) -> HashMap<(String, String), u64> {
        let mut totals = HashMap::new();
        for event in buffer.retrieve() {
            match event {
                MetricEvent::Create {
                    params,
                    populate_into,
                    ..
                } => populate_into
                    .set(Instrument(params.name.to_string()))
                    .unwrap(),
                MetricEvent::CreateAttributes {
                    populate_into,
                    append_from,
                    attributes,
                } => {
                    let mut set =
                        append_from.map_or_else(HashMap::new, |from| labels(&from).0.clone());
                    set.extend((attributes.into_iter()).map(|kv| (kv.key, kv.value.to_string())));
                    populate_into.set(Arc::new(Labels(set))).unwrap();
                }
                MetricEvent::Update {
                    instrument,
                    attributes,
                    update,
                } => {
                    let name = instrument.get().0.strip_prefix("temporal_");
                    let Some(name) = name.filter(|name| name.starts_with("stream_")) else {
                        continue;
                    };
                    let name = name.to_string();
                    let set = labels(&attributes);
                    assert_eq!(set.0["stream_store"], "mem", "{name}");
                    let reason = set.0.get("failure_reason").cloned().unwrap_or_default();
                    let value = match update {
                        MetricUpdateVal::Delta(value) => value,
                        MetricUpdateVal::Duration(_) => 1,
                        other => panic!("{name} has the update {other:?}"),
                    };
                    *totals.entry((name, reason)).or_default() += value;
                }
            }
        }
        totals
    }

    fn read(call_id: u64, stream: StreamRef, check: bool) -> Request {
        let request = ReadRecordsRequest {
            r#ref: Some(stream),
            max_records: 10,
            max_bytes: 1000,
            ..Default::default()
        };
        Request::Run(Call {
            call_id,
            seq: call_id as u32,
            store: "mem".to_string(),
            op: Op::Read(ReadCall {
                request,
                wait: Duration::ZERO,
                check,
                last_try: LastTry::default(),
            }),
        })
    }

    // Each try of a call counts for its Store when it starts, and a check is one try. Only a try
    // of an append has a latency, and a failed try has the token of its reason.
    #[tokio::test(start_paused = true)]
    async fn the_metrics_count_each_try_of_a_call() {
        let (store, manager, mut results) = manager().await;
        let buffer = Arc::new(MetricsCallBuffer::<Instrument>::new(1000));
        let meter: Arc<dyn CoreMeter> = buffer.clone();
        let telemetry = telemetry_init(TelemetryOptions::builder().metrics(meter).build()).unwrap();
        let metrics = MetricsContext::top_level("n".to_string(), "q".to_string(), &telemetry);
        let of_store = |store: &str| metrics.with_new_attrs([stream_store(store.to_string())]);
        let down = || AnyError::store_unavailable("The Store is down");
        let missing = StreamRef {
            id: "missing".to_string(),
            ..stream()
        };
        // The first try of the append and of the read meet an outage, and so does the first check.
        let calls = [
            (Some(StoreOp::Append), append(1)),
            (Some(StoreOp::Read), read(2, stream(), false)),
            (Some(StoreOp::Read), read(3, stream(), true)),
            (None, read(4, missing, true)),
        ];
        for (outage, call) in calls {
            if let Some(op) = outage {
                store.fail(op, down(), 1);
            }
            manager.dispatch("r", vec![call], of_store);
            results.recv().await.unwrap();
        }
        let expected = [
            ("stream_append_total", "", 2),
            (
                "stream_append_execution_failed",
                "stream_store_unavailable",
                1,
            ),
            ("stream_append_execution_latency", "", 2),
            ("stream_read_total", "", 4),
            (
                "stream_read_execution_failed",
                "stream_store_unavailable",
                2,
            ),
            ("stream_read_execution_failed", "stream_not_found", 1),
            ("stream_read_records", "", 1),
        ];
        let expected = (expected.into_iter())
            .map(|(name, reason, value)| ((name.to_string(), reason.to_string()), value))
            .collect();
        assert_eq!(totals(&buffer), expected);
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

    // The failure is built from what a marker records, so a replay gives the same one as the
    // first run. A long message is cut on a character boundary.
    #[test]
    fn a_failure_reads_back_as_the_failure_of_its_call() {
        let reasons: Vec<StreamErrorReason> = (0..)
            .map_while(|number| StreamErrorReason::try_from(number).ok())
            .collect();
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let reason = reasons[rng.random_range(0..reasons.len())];
            // Most messages are near the cut, and the others far below or above it.
            let bytes = match rng.random_range(0..10) {
                0..=5 => rng.random_range(4070..4100),
                6..=7 => rng.random_range(0..50),
                _ => rng.random_range(0..10_000),
            };
            let text = message(&mut rng, bytes);
            let error = StreamError {
                reason,
                message: text.clone(),
            };
            let read_back = failure_of(&recorded(&error));
            assert_eq!(read_back, failure(&error), "seed {seed}: {reason:?}");
            let kept = &read_back.message;
            if text.len() <= 4096 {
                assert_eq!(*kept, text, "seed {seed}");
            } else {
                let start = kept.strip_suffix("...");
                let start = start.unwrap_or_else(|| panic!("seed {seed}: no `...`"));
                assert!(text.starts_with(start) && kept.len() <= 4096, "seed {seed}");
                let next = text[start.len()..].chars().next().unwrap();
                assert!(kept.len() + next.len_utf8() > 4096, "seed {seed}");
            }
            // The type of the failure is the token of the reason, its details hold the reason,
            // and only an outage may be tried again.
            let details = StreamErrorDetails {
                reason: reason as i32,
            };
            let payload = Payload {
                metadata: HashMap::from([
                    ("encoding".to_string(), b"binary/protobuf".to_vec()),
                    (
                        "messageType".to_string(),
                        b"temporal.api.stream.v1.StreamErrorDetails".to_vec(),
                    ),
                ]),
                data: details.encode_to_vec(),
                ..Default::default()
            };
            let name = reason.as_str_name().strip_prefix("STREAM_ERROR_REASON_");
            let info = ApplicationFailureInfo {
                r#type: format!("stream_{}", name.unwrap().to_ascii_lowercase()),
                non_retryable: reason != StreamErrorReason::StoreUnavailable,
                details: Some(Payloads {
                    payloads: vec![payload],
                }),
                ..Default::default()
            };
            let expected = Failure {
                message: kept.clone(),
                failure_info: Some(FailureInfo::ApplicationFailureInfo(info)),
                ..Default::default()
            };
            assert_eq!(read_back, expected, "seed {seed}: {reason:?}");
        }
    }

    /// The read that a test runs. The Store checks each request against it.
    #[derive(Default)]
    struct Reading {
        from_offset: Vec<u8>,
        max_records: i32,
        /// When the wait of the read is over, unless it never is.
        wait_until: Option<Instant>,
        /// The tries of the read that the Store answered.
        answers: usize,
        violations: Vec<String>,
    }

    /// Returns a Stream of up to 40 entries, of which some aren't Records, that come over time.
    fn entries(rng: &mut StdRng) -> Vec<Entry> {
        let mut at = Duration::ZERO;
        (0..rng.random_range(0..40u16))
            .map(|index| {
                at += Duration::from_millis(rng.random_range(0..30));
                let offset = (index * 2 + 1).to_be_bytes().to_vec();
                let record = rng.random_bool(0.75).then(|| StreamRecord {
                    offset: offset.clone(),
                    body: Some(Payload {
                        data: vec![0; rng.random_range(0..100)],
                        ..Default::default()
                    }),
                    producer: Some(client()),
                });
                Entry { offset, at, record }
            })
            .collect()
    }

    // Workflow code reads a Stream with one read for each page, from the `last_offset` of the page
    // before. Each read gives one answer of the Store as its page, and a page with no Record comes
    // once the wait is over, so Workflow code doesn't read in a loop. An early answer with no
    // Record makes the read ask again from its `last_offset`, no sooner than `MIN_ASK_AGAIN` after
    // it.
    #[tokio::test(start_paused = true)]
    async fn a_read_gives_one_answer_of_the_store_at_once_or_none_once_its_wait_is_over() {
        let (mut ends, mut outages, mut early, mut loops, mut moved, mut more, mut again) =
            ([0; 2], 0, 0, 0, 0, 0, 0);
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let entries = entries(&mut rng);
            let from_offset = match rng.random_range(0..=entries.len()) {
                0 => vec![],
                at => entries[at - 1].offset.clone(),
            };
            let wait = match rng.random_range(0..4) {
                0 => Duration::ZERO,
                // Core bounds a wait by the read limit of its task, so no wait is endless.
                1 => Duration::from_secs(100),
                _ => Duration::from_millis(rng.random_range(1..300)),
            };
            let settings = StreamSettings {
                retry_initial_interval: Duration::from_millis(1),
                retry_max_interval: Duration::from_millis(5),
                try_deadline: Duration::from_millis(20),
                ..Default::default()
            };
            let fails = rng.random_bool(0.3);
            let reading = Arc::new(Mutex::new(Reading::default()));
            let checked = reading.clone();
            let fault = move |rng: &mut StdRng, request: &StoreRead| {
                let mut reading = checked.lock();
                // The Store waits at most what the read has left to wait.
                let left = (reading.wait_until).map_or(Duration::MAX, |until| {
                    until.saturating_duration_since(Instant::now())
                });
                let wait = (request.wait).map_or(Duration::ZERO, |wait| wait.try_into().unwrap());
                // The runtime asks for one Record more than the page holds. An ask again starts
                // where the answer before it ended, which is never before the read's start.
                let rules = [
                    request.max_records == reading.max_records + 1,
                    request.from_offset >= reading.from_offset,
                    wait <= left,
                ];
                if let Some(rule) = rules.iter().position(|kept| !kept) {
                    let violation = format!("seed {seed}: rule {rule} of a request");
                    reading.violations.push(violation);
                }
                let fault = match rng.random_range(0..100) {
                    0..=4 => Some(Fault::Fails(StreamError::store_unavailable("Down"))),
                    5..=7 => Some(Fault::Silent),
                    8 if fails => {
                        let gone = ReadError::operation(ReadReason::NotFound, "Gone");
                        Some(Fault::Fails(gone))
                    }
                    _ => None,
                };
                reading.answers += usize::from(fault.is_none());
                fault
            };
            let ignores_wait = rng.random_bool(0.3);
            let store = Arc::new(ModelStore::new(Model {
                ignores_wait,
                ..Model::new(seed, entries, fault)
            }));
            let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
            let mut runtimes = StreamRuntimes::new([("mem".to_string(), Arc::new(runtime))]);
            runtimes.settings = settings;
            let (tx, mut results) = mpsc::unbounded_channel();
            let sink = move |_, resolution: StreamResolution| tx.send(resolution).unwrap();
            let manager = StreamManager::new(runtimes, Handle::current(), sink);
            let (mut cursor, mut got) = (from_offset.clone(), vec![]);
            for call_id in 1.. {
                let max_records = rng.random_range(1..=10);
                let max_bytes = rng.random_range(1..2000);
                let started = Instant::now();
                {
                    let mut reading = reading.lock();
                    reading.from_offset.clone_from(&cursor);
                    reading.max_records = max_records;
                    reading.wait_until = started.checked_add(wait);
                    reading.answers = 0;
                }
                let request = ReadRecordsRequest {
                    r#ref: Some(stream()),
                    from_offset: cursor.clone(),
                    max_records,
                    max_bytes,
                    wait: None,
                };
                let last_try = LastTry::default();
                let read = ReadCall {
                    request,
                    wait,
                    check: false,
                    last_try: last_try.clone(),
                };
                let call = Call {
                    call_id,
                    seq: call_id as u32,
                    store: "mem".to_string(),
                    op: Op::Read(read),
                };
                manager.dispatch("r", vec![Request::Run(call)], no_metrics);
                let resolution = loop {
                    let look = Duration::from_millis(rng.random_range(1..40));
                    tokio::select! {
                        resolution = results.recv() => break resolution.unwrap(),
                        () = tokio::time::sleep(look) => {
                            // A read keeps the failure of its last try while it waits to try
                            // again, and notes no outage that could fail its task at the heartbeat.
                            assert_eq!(manager.outage("r"), None, "seed {seed}");
                            outages += usize::from(last_try.lock().is_some());
                        }
                    }
                };
                let Outcome::Read(done) = resolution.outcome else {
                    panic!("seed {seed}: a read gives the result of a read");
                };
                let answers = reading.lock().answers;
                let page = match &done.entry.result {
                    Some(stream_read_result::Result::Page(page)) => page.clone(),
                    // Only a Store that can fail the read ends it with a failure.
                    other => {
                        assert!(fails, "seed {seed}: {other:?}");
                        ends[0] += 1;
                        break;
                    }
                };
                // The page is the last answer of the Store. Only a read with a wait asks again, and
                // each ask again comes `MIN_ASK_AGAIN` or more after the ask before it.
                assert!(answers == 1 || !wait.is_zero(), "seed {seed}");
                let spaced = MIN_ASK_AGAIN * u32::try_from(answers - 1).unwrap();
                assert!(started.elapsed() >= spaced, "seed {seed}");
                again += usize::from(answers > 1);
                let records = done.records;
                let bytes: usize = (records.iter())
                    .map(|record| field_len(record.encoded_len()))
                    .sum();
                assert!(records.len() <= max_records as usize, "seed {seed}");
                assert!(
                    bytes <= max_bytes as usize || records.len() == 1,
                    "seed {seed}"
                );
                // The page ends where the answer of the Store ended, also with no Record. With
                // `more` the Stream has a Record after it.
                let answered = store.model.lock().last_offset.clone();
                if done.after == After::More {
                    assert!(
                        !store.records_after(&page.last_offset).is_empty(),
                        "seed {seed}"
                    );
                    more += 1;
                } else {
                    assert_eq!(page.last_offset, answered, "seed {seed}");
                }
                let moved_on = records.is_empty() && page.last_offset > cursor;
                moved += usize::from(moved_on);
                cursor.clone_from(&page.last_offset);
                let came = !records.is_empty();
                got.extend(records);
                let all = store.records_after(&from_offset);
                assert!(all.starts_with(&got), "seed {seed}");
                // A page with no Record comes once the wait is over, even from a Store that
                // answers before it.
                if !came {
                    assert!(started.elapsed() >= wait, "seed {seed}");
                    // A Store that waits answers at the end of the wait with no Record unless it
                    // passed entries that aren't Records.
                    if !ignores_wait && !moved_on {
                        let came = store.records_came_after(&from_offset);
                        assert_eq!(got, came, "seed {seed}");
                    }
                    ends[1] += 1;
                    break;
                }
                loops += usize::from(call_id > 1);
            }
            let violations = [
                reading.lock().violations.clone(),
                store.model.lock().violations.clone(),
            ];
            assert_eq!(violations.concat(), Vec::<String>::new(), "seed {seed}");
            early += store.model.lock().answered_early;
        }
        // The scripts reach each kind of end, an outage, a Store that answers before the wait, a
        // Stream that takes several reads, a page with no Record that ends past its start, a page
        // with more Records after it, and a read that asks again.
        let reached = [outages, early, loops, moved, more, again];
        assert!(
            ends.iter().chain(&reached).all(|count| *count > 0),
            "{ends:?} {reached:?}"
        );
    }
}
