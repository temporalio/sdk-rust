//! Direct calls to Streams. Core reads and appends a Stream on the Worker's Stream runtimes, in
//! tasks on the Worker's runtime, and gives each result to the run as a [LocalResolution].

use crate::{
    abstractions::dbg_panic,
    worker::workflow::{
        LocalResolution, LocalResolutionMsg,
        workflow_stream::{LocalInput, LocalInputs},
    },
};
use futures_util::future::{AbortHandle, AbortRegistration, Abortable};
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use temporalio_common::protos::{
    coresdk::{
        common::StreamReadStop,
        workflow_activation::{StreamAppended, StreamReadEnd, resolve_stream_append},
    },
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    },
};
use temporalio_streams::{
    MAX_WAIT, Reason, StreamError, StreamRuntime,
    proto::{
        AppendRecord, AppendRecordsRequest, Producer, ReadRecordsRequest, StreamRecord, StreamRef,
        WorkflowTaskEpoch, producer::Kind,
    },
};
use tokio::{
    runtime::Handle,
    sync::{Notify, mpsc::UnboundedSender},
    time::Instant,
};
use tracing::Span;

/// The Stream runtimes of a Worker, by the name of the Store that each serves. A Workflow call on
/// a Stream uses the runtime of its `StreamRef.store`.
#[derive(Clone, Debug, Default)]
pub struct StreamRuntimes(HashMap<String, Arc<StreamRuntime>>);

impl StreamRuntimes {
    /// Builds the set from pairs of a Store name and its runtime.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self(runtimes.into_iter().collect())
    }

    /// The runtime of the Store `store`, if the Worker has one.
    pub fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.0.get(store)
    }

    /// True if the Worker has no Stream runtime.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The idle timeout of a read that sets none.
pub(crate) const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(1);
/// The bytes of Records that a read may hold before Workflow code takes them. Past it, the read
/// waits, so a run with no Workflow Task in hand doesn't gather Records without a bound.
const MAX_UNDELIVERED_BYTES: usize = 4 * 1024 * 1024;

static NEXT_CALL_ID: AtomicU64 = AtomicU64::new(1);

/// A new id for one dispatch of a call. It is unique in the process, so that a result of an older
/// instance of a run never matches a call of a newer one.
pub(crate) fn next_call_id() -> u64 {
    NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed)
}

/// The Records that a read gave to the run and that no activation took yet.
#[derive(Debug, Default)]
pub(crate) struct ReadFlow {
    undelivered: AtomicUsize,
    taken: Notify,
}

impl ReadFlow {
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
        self.taken.notify_one();
    }

    async fn wait_for_room(&self) {
        while self.undelivered.load(Ordering::Acquire) >= MAX_UNDELIVERED_BYTES {
            self.taken.notified().await;
        }
    }
}

/// A call that the machines ask the Worker to make or to stop.
#[derive(Debug)]
pub(crate) enum StreamRequest {
    Read(ReadSpec),
    Append(AppendSpec),
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
    pub(crate) idle_timeout: Duration,
    /// Zero means no cap.
    pub(crate) max_records: u32,
    /// Zero means no cap.
    pub(crate) max_bytes: u64,
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
    pub(crate) bodies: Vec<Payload>,
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
}

/// Runs the Stream calls of a Worker's runs. Its tasks run on the Worker's runtime, not on the
/// workflow processing thread, so that Store work never holds up the runs.
pub(crate) struct StreamManager {
    runtimes: StreamRuntimes,
    handle: Handle,
    local_tx: UnboundedSender<LocalInput>,
    /// The run and the abort handle of each running call, by its call id.
    tasks: Arc<Mutex<HashMap<u64, (String, AbortHandle)>>>,
}

impl StreamManager {
    pub(crate) fn new(
        runtimes: StreamRuntimes,
        handle: Handle,
        local_tx: UnboundedSender<LocalInput>,
    ) -> Self {
        Self {
            runtimes,
            handle,
            local_tx,
            tasks: Default::default(),
        }
    }

    /// Starts or stops the calls of a run. `epoch` is the run's current Workflow Task, which an
    /// append writes with.
    pub(crate) fn dispatch(
        &self,
        run_id: &str,
        epoch: WorkflowTaskEpoch,
        requests: Vec<StreamRequest>,
    ) {
        for request in requests {
            match request {
                StreamRequest::Cancel(call_id) => {
                    if let Some((_, handle)) = self.tasks.lock().remove(&call_id) {
                        handle.abort();
                    }
                }
                StreamRequest::Read(spec) => {
                    let (call_id, seq) = (spec.call_id, spec.seq);
                    match self.runtime(&spec.stream) {
                        Some(runtime) => {
                            let sink = self.sink(run_id, call_id, seq);
                            self.spawn(run_id, call_id, read(runtime, spec, sink));
                        }
                        None => self.no_runtime(run_id, call_id, seq, true),
                    }
                }
                StreamRequest::Append(spec) => {
                    let (call_id, seq) = (spec.call_id, spec.seq);
                    match self.runtime(&spec.stream) {
                        Some(runtime) => {
                            let sink = self.sink(run_id, call_id, seq);
                            self.spawn(run_id, call_id, append(runtime, spec, epoch, sink));
                        }
                        None => self.no_runtime(run_id, call_id, seq, false),
                    }
                }
            }
        }
    }

    /// Stops every call of the run. Results already on their way find no call and are dropped.
    pub(crate) fn cancel_run(&self, run_id: &str) {
        self.tasks.lock().retain(|_, (run, handle)| {
            if run == run_id {
                handle.abort();
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
        self.handle.spawn(Abortable::new(
            async move {
                tokio::time::sleep_until(deadline.into()).await;
                let _ = local_tx.send(LocalInput {
                    input: LocalInputs::HeartbeatTimeout(run_id),
                    span,
                });
            },
            abort_reg,
        ));
    }

    fn runtime(&self, stream: &StreamRef) -> Option<Arc<StreamRuntime>> {
        self.runtimes.get(&stream.store).cloned()
    }

    /// The machines check the Store of each call, so a call with no runtime only comes when the
    /// runtimes and the machines disagree. It still resolves, so that the run doesn't wait forever.
    fn no_runtime(&self, run_id: &str, call_id: u64, seq: u32, is_read: bool) {
        dbg_panic!("A Stream call reached the Worker with no runtime for its Store");
        let failure = Failure {
            message: "This Worker has no Stream runtime for the Store of the call".to_string(),
            ..Default::default()
        };
        let result = if is_read {
            StreamCallResult::Page {
                records: vec![],
                end: Some(StreamReadEnd {
                    failure: Some(failure),
                    ..Default::default()
                }),
            }
        } else {
            StreamCallResult::Append(resolve_stream_append::Status::Failed(failure))
        };
        (self.sink(run_id, call_id, seq))(result);
    }

    fn sink(&self, run_id: &str, call_id: u64, seq: u32) -> impl Fn(StreamCallResult) + use<> {
        let local_tx = self.local_tx.clone();
        let run_id = run_id.to_string();
        move |result| {
            let _ = local_tx.send(LocalInput {
                input: LocalInputs::LocalResolution(LocalResolutionMsg {
                    run_id: run_id.clone(),
                    res: LocalResolution::Stream(StreamResolution {
                        call_id,
                        seq,
                        result,
                    }),
                }),
                span: Span::current(),
            });
        }
    }

    fn spawn(&self, run_id: &str, call_id: u64, call: impl Future<Output = ()> + Send + 'static) {
        let (handle, registration) = AbortHandle::new_pair();
        self.tasks
            .lock()
            .insert(call_id, (run_id.to_string(), handle));
        let tasks = self.tasks.clone();
        self.handle.spawn(async move {
            // An aborted call was already removed by whoever aborted it.
            if Abortable::new(call, registration).await.is_ok() {
                tasks.lock().remove(&call_id);
            }
        });
    }
}

/// Reads until the first stop, and sends each response with Records as one page at once.
async fn read(runtime: Arc<StreamRuntime>, spec: ReadSpec, sink: impl Fn(StreamCallResult)) {
    let deadline = spec.deadline.map(|deadline| {
        Instant::now()
            + deadline
                .duration_since(SystemTime::now())
                .unwrap_or_default()
    });
    let mut last_offset = spec.from_offset.clone();
    let mut records_left = (spec.max_records > 0).then_some(spec.max_records);
    let mut bytes_left = (spec.max_bytes > 0).then_some(spec.max_bytes);
    let mut took_a_record = spec.took_a_record;
    let mut idle_deadline = Instant::now() + spec.idle_timeout;
    let end = |last_offset: Vec<u8>, stop: StreamReadStop| StreamReadEnd {
        last_offset,
        stop: stop as i32,
        done: stop == StreamReadStop::Done,
        failure: None,
    };
    loop {
        spec.flow.wait_for_room().await;
        let now = Instant::now();
        if deadline.is_some_and(|deadline| deadline <= now) {
            sink(StreamCallResult::Page {
                records: vec![],
                end: Some(end(last_offset, StreamReadStop::Timeout)),
            });
            return;
        }
        if idle_deadline <= now {
            sink(StreamCallResult::Page {
                records: vec![],
                end: Some(end(last_offset, StreamReadStop::Idle)),
            });
            return;
        }
        let wait_until = deadline
            .unwrap_or(idle_deadline)
            .min(idle_deadline)
            .min(now + MAX_WAIT);
        let request = ReadRecordsRequest {
            namespace: String::new(),
            r#ref: Some(spec.stream.clone()),
            from_offset: last_offset.clone(),
            max_records: records_left.map_or(0, |left| left.min(i32::MAX as u32) as i32),
            // A cap of zero bytes left still asks for one Record, which then ends the read.
            max_bytes: bytes_left.map_or(0, |left| left.clamp(1, i64::MAX as u64) as i64),
            wait: Some(proto_duration(wait_until - now)),
        };
        let response = match runtime.read_records(request).await {
            Ok(response) => response,
            Err(error) => {
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
        if stop.is_none() && response.done && page.len() == given {
            stop = Some(StreamReadStop::Done);
        }
        if stop.is_none() && bytes_left == Some(0) {
            stop = Some(StreamReadStop::MaxBytes);
        }
        if !page.is_empty() {
            idle_deadline = Instant::now() + spec.idle_timeout;
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
            None if !page.is_empty() => sink(StreamCallResult::Page {
                records: page,
                end: None,
            }),
            None => {}
        }
    }
}

/// Appends the bodies as the run's Producer, with the Workflow Task as the epoch.
async fn append(
    runtime: Arc<StreamRuntime>,
    spec: AppendSpec,
    epoch: WorkflowTaskEpoch,
    sink: impl Fn(StreamCallResult),
) {
    let mut producer = spec.producer;
    if let Some(Kind::Workflow(workflow)) = producer.kind.as_mut() {
        workflow.epoch = Some(epoch);
    }
    let request = AppendRecordsRequest {
        namespace: String::new(),
        r#ref: Some(spec.stream),
        producer: Some(producer),
        // The seq grows in a run, so the Store can refuse a resend of one batch in one epoch.
        sequence: i64::from(spec.seq),
        records: spec
            .bodies
            .into_iter()
            .map(|body| AppendRecord { body: Some(body) })
            .collect(),
    };
    let status = match runtime.append_records(request).await {
        Ok(response) => resolve_stream_append::Status::Appended(StreamAppended {
            first_offset: response.first_offset,
            last_offset: response.last_offset,
        }),
        Err(error) => resolve_stream_append::Status::Failed(failure(&error)),
    };
    sink(StreamCallResult::Append(status));
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
        message: error.to_string(),
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
