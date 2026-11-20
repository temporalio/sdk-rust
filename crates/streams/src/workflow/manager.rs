//! Runs the Stream calls of a Worker's runs, one task for each call.

use super::{
    StreamRuntimes, StreamSettings,
    reader::{self, Page, ReadCall},
};
use crate::{
    AnyReason, StreamError, StreamRuntime,
    proto::{AppendRecordsRequest, CloseStreamRequest, StreamErrorReason},
};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::workflow_activation::{StreamAppended, resolve_stream_append},
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    },
};
use tokio::{runtime::Handle, task::AbortHandle};

/// The maximum length of a failure message that Workflow code gets and a marker records, in bytes.
/// A Store can return error text of any size, so the message needs a bound.
const MAX_FAILURE_MESSAGE_BYTES: usize = 4096;

static NEXT_CALL_ID: AtomicU64 = AtomicU64::new(1);

/// Returns a new call id. Ids are unique in the process, so a result from a stopped call never
/// matches a call that a new run makes.
pub(crate) fn next_call_id() -> u64 {
    NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed)
}

/// A call of a run to start or to stop.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Most requests start a call
pub enum Request {
    /// Starts a call.
    Run(Call),
    /// Stops the call with this id, which then gives no result.
    Stop(u64),
}

/// A call to start, with the request that each of its tries sends.
#[derive(Debug)]
pub struct Call {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) op: Op,
}

#[derive(Debug)]
pub(crate) enum Op {
    Append(AppendRecordsRequest),
    Close(CloseStreamRequest),
    Read(ReadCall),
}

/// The result of a call, for its run.
#[derive(Debug)]
pub struct StreamResolution {
    pub(crate) call_id: u64,
    pub(crate) seq: u32,
    pub(crate) outcome: Outcome,
}

#[derive(Debug)]
pub(crate) enum Outcome {
    Append(resolve_stream_append::Status),
    /// `None` if the Stream is closed.
    Close(Option<Failure>),
    /// The next page of a read, which a read gives before it ends.
    Page(Page),
}

type Sink = Arc<dyn Fn(String, StreamResolution) + Send + Sync>;

/// The running calls, keyed by run id and call id.
type Tasks = Arc<Mutex<HashMap<String, HashMap<u64, Task>>>>;

struct Task {
    abort: AbortHandle,
    /// The failure of the last try, while the call waits to try again.
    outage: Option<String>,
}

/// The runner of a Worker's Stream calls. It runs them on the Worker's runtime, so Store work never
/// holds up the thread of the runs, and dropping it stops every call.
pub struct StreamManager {
    runtimes: StreamRuntimes,
    handle: Handle,
    sink: Sink,
    tasks: Tasks,
}

impl StreamManager {
    /// Creates a manager that runs calls on `handle`, and gives each result to `sink` with its run
    /// id.
    pub fn new(
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

    /// Starts and stops calls of the run `run_id`.
    pub fn dispatch(&self, run_id: &str, requests: Vec<Request>) {
        // The lock is held across each spawn, so a call that ends immediately finds its task.
        let mut tasks = self.tasks.lock();
        for request in requests {
            match request {
                Request::Run(call) => {
                    let call_id = call.call_id;
                    let abort = self.spawn(run_id, call);
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

    /// Stops every call of the run. A result that it already sent finds no call.
    pub fn cancel_run(&self, run_id: &str) {
        if let Some(run) = self.tasks.lock().remove(run_id) {
            for task in run.into_values() {
                task.abort.abort();
            }
        }
    }

    /// Returns the failure of the run's call that waits to try again after an outage. If several
    /// do, it's the one with the lowest call id.
    pub fn outage(&self, run_id: &str) -> Option<String> {
        let tasks = self.tasks.lock();
        let run = tasks.get(run_id)?;
        run.iter()
            .filter_map(|(call_id, task)| Some((*call_id, task.outage.as_ref()?)))
            .min_by_key(|(call_id, _)| *call_id)
            .map(|(_, outage)| outage.clone())
    }

    fn spawn(&self, run_id: &str, call: Call) -> AbortHandle {
        let Call { call_id, seq, op } = call;
        let stream = match &op {
            Op::Append(request) => request.r#ref.clone(),
            Op::Close(request) => request.r#ref.clone(),
            Op::Read(read) => read.request.r#ref.clone(),
        }
        .unwrap_or_default();
        let runtime = (self.runtimes.get(&stream.store).cloned())
            .expect("The run starts no call on a Store with no runtime");
        let settings = self.runtimes.settings;
        let note = Note {
            tasks: self.tasks.clone(),
            run_id: run_id.to_string(),
            call_id,
            what: format!(
                "{} of Stream \"{}\" on Store \"{}\"",
                match op {
                    Op::Append(_) => "append",
                    Op::Close(_) => "close",
                    Op::Read(_) => "read",
                },
                stream.id,
                stream.store
            ),
        };
        let sink = self.sink.clone();
        let task = self.handle.spawn(async move {
            let give = |page| {
                let outcome = Outcome::Page(page);
                let resolution = StreamResolution {
                    call_id,
                    seq,
                    outcome,
                };
                sink(note.run_id.clone(), resolution);
            };
            let outcome = run(&runtime, op, settings, &note, give).await;
            if note.end()
                && let Some(outcome) = outcome
            {
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

/// Runs a call, and returns the outcome of an append or a close. A read gives each of its pages to
/// `give` while it runs, and has no outcome.
async fn run(
    runtime: &StreamRuntime,
    op: Op,
    settings: StreamSettings,
    note: &Note,
    give: impl Fn(Page),
) -> Option<Outcome> {
    match op {
        Op::Append(request) => {
            let answer = tries(settings, note, || runtime.append_records(request.clone())).await;
            Some(Outcome::Append(match answer {
                Ok(appended) => resolve_stream_append::Status::Appended(StreamAppended {
                    first_offset: appended.first_offset,
                    last_offset: appended.last_offset,
                }),
                Err(error) => resolve_stream_append::Status::Failed(failure(&error)),
            }))
        }
        Op::Close(request) => {
            let answer = tries(settings, note, || runtime.close_stream(request.clone())).await;
            Some(Outcome::Close(answer.err().map(|error| failure(&error))))
        }
        Op::Read(read) => {
            reader::read(runtime, read, settings, note, give).await;
            None
        }
    }
}

/// Sends the request again while the Store is in an outage, with no bound in time. A resend is
/// safe, since a Store writes the last batch of an epoch once, and a second close succeeds.
async fn tries<T, R, F>(
    settings: StreamSettings,
    note: &Note,
    mut send: impl FnMut() -> F,
) -> Result<T, StreamError<R>>
where
    R: Copy + From<AnyReason> + Into<StreamErrorReason>,
    F: Future<Output = Result<T, StreamError<R>>>,
{
    let mut backoff = Backoff::new(&settings);
    loop {
        let error = match tokio::time::timeout(settings.try_deadline, send()).await {
            Ok(Err(error)) if is_outage(&error) => error,
            Ok(answer) => {
                note.set(None);
                return answer;
            }
            Err(_) => StreamError::storage_unavailable(format!(
                "The Store gave no answer in {:?}",
                settings.try_deadline
            )),
        };
        note.set(Some(&error));
        tokio::time::sleep(backoff.next_wait(rand::random())).await;
    }
}

pub(super) struct Backoff {
    next: Duration,
    max: Duration,
}

impl Backoff {
    pub(super) fn new(settings: &StreamSettings) -> Self {
        Self {
            next: settings.retry_first_wait,
            max: settings.retry_max_wait,
        }
    }

    /// Returns the wait before the next try, which `jitter` from 0 to 1 picks from half to all of
    /// the backoff. The calls of many runs then don't all try again at the same moment.
    pub(super) fn next_wait(&mut self, jitter: f64) -> Duration {
        let wait = self.next.mul_f64(0.5 + jitter / 2.0);
        self.next = (self.next * 2).min(self.max);
        wait
    }
}

/// The handle of one call to its entry in the manager.
pub(super) struct Note {
    tasks: Tasks,
    run_id: String,
    call_id: u64,
    what: String,
}

impl Note {
    /// Notes the failure of the last try, or clears it with `None`.
    pub(super) fn set(&self, error: Option<&dyn fmt::Display>) {
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

/// Returns true if `error` says that the Store is in an outage, so that another try of the call may
/// succeed.
pub(super) fn is_outage<R: Copy + Into<StreamErrorReason>>(error: &StreamError<R>) -> bool {
    error.reason.into() == StreamErrorReason::StorageUnavailable
}

/// Returns the failure that Workflow code gets for `error`. Its type is the reason's token, and its
/// details hold the encoded [`StreamErrorDetails`](crate::proto::StreamErrorDetails).
pub(crate) fn failure<R: Copy + Into<StreamErrorReason>>(error: &StreamError<R>) -> Failure {
    Failure {
        message: cut(error.to_string()),
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
    use super::*;
    use crate::{
        proto::{AppendRecord, StreamRef},
        record::BatchLimits,
        test_store::{Scripted, client, task, workflow},
    };
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use tokio::sync::mpsc;

    type Results = mpsc::UnboundedReceiver<(String, StreamResolution)>;

    fn manager(store: &Arc<Scripted>) -> (StreamManager, Results) {
        let runtime = StreamRuntime::new(store.clone(), BatchLimits::default());
        let runtimes = StreamRuntimes::new([("mem".to_string(), Arc::new(runtime))]);
        let (tx, rx) = mpsc::unbounded_channel();
        let sink = move |run_id, resolution| tx.send((run_id, resolution)).unwrap();
        (StreamManager::new(runtimes, Handle::current(), sink), rx)
    }

    fn stream() -> Option<StreamRef> {
        Some(StreamRef {
            id: "s".to_string(),
            store: "mem".to_string(),
        })
    }

    fn append(call_id: u64) -> Request {
        Request::Run(Call {
            call_id,
            seq: 1,
            op: Op::Append(AppendRecordsRequest {
                r#ref: stream(),
                producer: Some(workflow(Some(task(3, 1, 0)))),
                records: vec![AppendRecord {
                    body: Some(Payload::default()),
                }],
                ..Default::default()
            }),
        })
    }

    fn close(call_id: u64) -> Request {
        Request::Run(Call {
            call_id,
            seq: 2,
            op: Op::Close(CloseStreamRequest {
                r#ref: stream(),
                producer: Some(client()),
                ..Default::default()
            }),
        })
    }

    #[test]
    fn a_try_waits_half_to_all_of_its_backoff_up_to_the_longest() {
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let retry_max_wait = Duration::from_millis(rng.random_range(1..=60_000));
            let first = rng.random_range(1..=retry_max_wait.as_millis() as u64);
            let settings = StreamSettings {
                retry_first_wait: Duration::from_millis(first),
                retry_max_wait,
                ..Default::default()
            };
            assert_eq!(settings.check(), Ok(()), "seed {seed}");
            let mut backoff = Backoff::new(&settings);
            let mut max = settings.retry_first_wait;
            for _ in 0..20 {
                let wait = backoff.next_wait(rng.random());
                assert!(
                    max / 2 <= wait && wait <= max,
                    "seed {seed}: {wait:?} for {max:?}"
                );
                max = (max * 2).min(retry_max_wait);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_try_with_no_answer_by_its_deadline_is_an_outage() {
        let store = Arc::new(Scripted::default());
        store.answers.lock().appended.first_offset = b"a".to_vec();
        store.answers.lock().appended.last_offset = b"a".to_vec();
        let (manager, mut results) = manager(&store);
        store.silent.store(true, Ordering::Release);
        manager.dispatch("r", vec![append(1)]);
        tokio::time::sleep(Duration::from_millis(5010)).await;
        assert_eq!(
            manager.outage("r").as_deref(),
            Some(
                "StreamStoreUnavailable: append of Stream \"s\" on Store \"mem\": \
                 stream_storage_unavailable: The Store gave no answer in 5s"
            )
        );
        store.silent.store(false, Ordering::Release);
        let (run_id, resolution) = results.recv().await.unwrap();
        assert_eq!(run_id, "r");
        assert!(matches!(
            resolution.outcome,
            Outcome::Append(resolve_stream_append::Status::Appended(StreamAppended {
                ref first_offset, ..
            })) if first_offset == b"a"
        ));
        assert_eq!(manager.outage("r"), None);
        let tries = store.requests();
        assert!(tries.len() >= 2, "{tries:?}");
        assert!(tries.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancel_stops_the_calls_of_a_run_and_their_outage() {
        let store = Arc::new(Scripted::default());
        let (manager, mut results) = manager(&store);
        *store.unavailable.lock() = usize::MAX;
        manager.dispatch("a", vec![append(1), close(2), append(3)]);
        manager.dispatch("b", vec![close(4)]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(manager.outage("a").is_some());
        manager.dispatch("a", vec![Request::Stop(3)]);
        manager.cancel_run("a");
        assert_eq!(manager.outage("a"), None);
        assert!(manager.outage("b").is_some());
        *store.unavailable.lock() = 0;
        let (run_id, resolution) = results.recv().await.unwrap();
        assert_eq!((run_id.as_str(), resolution.call_id), ("b", 4));
        assert!(matches!(resolution.outcome, Outcome::Close(None)));
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(results.try_recv().is_err());
        let after = store.requests().len();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(store.requests().len(), after);
    }
}
