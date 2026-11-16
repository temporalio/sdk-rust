//! Direct calls to Streams. Core reads and appends a Stream on the Worker's Stream runtimes, in
//! tasks on the Worker's runtime, and gives each result to the run as a [LocalResolution].

use crate::{
    abstractions::dbg_panic,
    worker::workflow::{
        LocalResolution, LocalResolutionMsg,
        workflow_stream::{LocalInput, LocalInputs},
    },
};
use futures_util::{
    StreamExt,
    future::{AbortHandle, AbortRegistration, Abortable},
};
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
    constants::STREAM_MARKER_NAME,
    coresdk::{
        common::StreamReadStop,
        external_data::{StreamMarkerData, StreamReadEntry},
        workflow_activation::{StreamAppended, StreamReadEnd, resolve_stream_append},
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
    proto::{
        AppendRecord, AppendRecordsRequest, CloseStreamRequest, Producer, ReadRecordsRequest,
        StreamRecord, StreamRef, WorkflowTaskEpoch, producer::Kind,
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
#[derive(Clone, Debug)]
pub struct StreamRuntimes {
    runtimes: HashMap<String, Arc<StreamRuntime>>,
    /// False for a replay Worker, which reads the Store only for the Records of a marker.
    calls_stores: bool,
}

impl Default for StreamRuntimes {
    fn default() -> Self {
        Self::new([])
    }
}

impl StreamRuntimes {
    /// Builds the set from pairs of a Store name and its runtime.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self {
            runtimes: runtimes.into_iter().collect(),
            calls_stores: true,
        }
    }

    /// The runtimes of a replay Worker. It reads the Records of a marker, and never makes a live
    /// call, so a call that history leaves open stays open there.
    pub(crate) fn for_replay(self) -> Self {
        Self {
            calls_stores: false,
            ..self
        }
    }

    /// False for a replay Worker, which makes no live call.
    pub(crate) fn calls_stores(&self) -> bool {
        self.calls_stores
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
    /// When an activation last took every Record that the read had sent.
    all_taken_at: parking_lot::Mutex<Option<Instant>>,
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
    /// How long the read waits for the Store with no Record, or `None` for no bound. Zero gives
    /// what is in the Stream and doesn't wait.
    pub(crate) idle_timeout: Option<Duration>,
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
    /// The task that recorded the close, or `None` for the task in hand, when a replay sends again
    /// a close that history recorded with no result.
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
    runtimes: StreamRuntimes,
    handle: Handle,
    local_tx: UnboundedSender<LocalInput>,
    /// The run and the abort handle of each running call, by its call id, and whether the call
    /// outlives its run.
    tasks: Arc<Mutex<HashMap<u64, Task>>>,
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
    /// append writes with, or `None` when the run has no task in hand.
    pub(crate) fn dispatch(
        &self,
        run_id: &str,
        epoch: Option<WorkflowTaskEpoch>,
        requests: Vec<StreamRequest>,
    ) {
        for request in requests {
            match request {
                StreamRequest::Cancel(call_id) => {
                    if let Some(task) = self.tasks.lock().remove(&call_id) {
                        task.handle.abort();
                    }
                }
                StreamRequest::Read(spec) => {
                    let (call_id, seq) = (spec.call_id, spec.seq);
                    match self.runtime(&spec.stream) {
                        Some(runtime) => {
                            let sink = self.sink(run_id, call_id, seq);
                            self.spawn(run_id, call_id, false, read(runtime, spec, sink));
                        }
                        None => self.no_runtime(run_id, call_id, seq, Call::Read),
                    }
                }
                StreamRequest::Append(spec) => {
                    let (call_id, seq) = (spec.call_id, spec.seq);
                    match (self.runtime(&spec.stream), epoch) {
                        (Some(runtime), Some(epoch)) => {
                            let sink = self.sink(run_id, call_id, seq);
                            let call = append(runtime, spec, epoch, sink);
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
                            let call = close(runtime, spec, epoch, sink);
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
    fn no_runtime(&self, run_id: &str, call_id: u64, seq: u32, call: Call) {
        dbg_panic!("A Stream call reached the Worker with no runtime or no epoch");
        let failure = Failure {
            message: "This Worker has no Stream runtime for the Store of the call".to_string(),
            ..Default::default()
        };
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

/// Reads until the first stop, and sends each response with Records as one page at once.
async fn read(runtime: Arc<StreamRuntime>, spec: ReadSpec, sink: impl Fn(StreamCallResult)) {
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
        let idle_deadline = |last_record_at: Instant| {
            spec.idle_timeout.and_then(|idle| {
                spec.flow
                    .idle_since(last_record_at)
                    .map(|since| since + idle)
            })
        };
        // While the idle time doesn't count, the read looks again after one idle timeout.
        let idle_check = spec
            .idle_timeout
            .map(|idle| idle_deadline(last_record_at).unwrap_or(now + idle));
        let wait_until = [deadline, idle_check]
            .into_iter()
            .flatten()
            .fold(now + MAX_WAIT, Instant::min);
        let request = ReadRecordsRequest {
            namespace: String::new(),
            r#ref: Some(spec.stream.clone()),
            from_offset: last_offset.clone(),
            max_records: records_left.map_or(0, |left| left.min(i32::MAX as u32) as i32),
            // A cap of zero bytes left still asks for one Record, which then ends the read.
            max_bytes: bytes_left.map_or(0, |left| left.clamp(1, i64::MAX as u64) as i64),
            wait: Some(proto_duration(wait_until.saturating_duration_since(now))),
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
            None if !page.is_empty() => sink(StreamCallResult::Page {
                records: page,
                end: None,
            }),
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
            None if spec.idle_timeout == Some(Duration::ZERO) => match deadline {
                Some(deadline) => {
                    let _ = tokio::time::timeout_at(deadline, spec.flow.wait_for_take()).await;
                }
                None => spec.flow.wait_for_take().await,
            },
            None => {}
        }
    }
}

/// The kind of a call, for the failure of one that can't run.
enum Call {
    Read,
    Append,
    Close,
}

/// Closes the Stream as the run's Producer, with the Workflow Task that recorded the close as the
/// epoch. A Stream that is closed already is a success.
async fn close(
    runtime: Arc<StreamRuntime>,
    spec: CloseSpec,
    epoch: WorkflowTaskEpoch,
    sink: impl Fn(StreamCallResult),
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
    let result = runtime.close_stream(request).await.err();
    sink(StreamCallResult::Close(result.map(|error| failure(&error))));
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
        sequence: spec.sequence,
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

/// True if the status comes from a failed hydrate.
pub(crate) fn is_stream_replay_error(status: &tonic::Status) -> bool {
    status.metadata().contains_key(STREAM_REPLAY_ERROR_KEY)
}

/// Reads the Records of each `core_stream` marker that has their count and digest but not the
/// Records themselves, and puts them in the marker. The machines are sync, so a replay reads the
/// Store here, where history enters in async code, and gives them a marker that has its Records.
pub(crate) async fn hydrate_stream_markers(
    events: &mut [HistoryEvent],
    runtimes: &StreamRuntimes,
) -> Result<(), tonic::Status> {
    let mut markers = vec![];
    for (at, event) in events.iter_mut().enumerate() {
        let Some(payload) = stream_marker_payload(event) else {
            continue;
        };
        let data = StreamMarkerData::decode(payload.data.as_slice()).map_err(|error| {
            replay_error(format!(
                "StreamReplayError: a Stream marker is bad: {error}"
            ))
        })?;
        if data
            .reads
            .iter()
            .any(|entry| entry.count > 0 && entry.records.is_empty())
        {
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
                .map(move |(entry, read)| (marker, entry, read.clone()))
        })
        .collect();
    let results: Vec<_> =
        futures_util::stream::iter(reads)
            .map(|(marker, entry, read)| async move {
                (marker, entry, read_range(runtimes, &read).await)
            })
            .buffer_unordered(HYDRATE_CONCURRENCY)
            .collect()
            .await;
    for (marker, entry, records) in results {
        markers[marker].1.reads[entry].records = records?;
    }
    for (at, data) in markers {
        if let Some(payload) = stream_marker_payload(&mut events[at]) {
            payload.data = data.encode_to_vec();
        }
    }
    Ok(())
}

/// Reads the Records of one entry, with no wait, and checks them against the entry.
async fn read_range(
    runtimes: &StreamRuntimes,
    entry: &StreamReadEntry,
) -> Result<Vec<StreamRecord>, tonic::Status> {
    let stream = entry.stream.clone().unwrap_or_default();
    let fail = |cause: String| {
        replay_error(format!(
            "StreamReplayError: Stream `{}` in Store `{}`, the Records after {} through {}: {cause}",
            stream.id,
            stream.store,
            offset_text(&entry.after_offset),
            offset_text(&entry.last_offset),
        ))
    };
    let runtime = runtimes.get(&stream.store).ok_or_else(|| {
        fail(format!(
            "this Worker has no Stream runtime for the Store `{}`",
            stream.store
        ))
    })?;
    let count = entry.count as usize;
    let mut records: Vec<StreamRecord> = Vec::with_capacity(count);
    let mut from_offset = entry.after_offset.clone();
    while records.len() < count {
        let left = count - records.len();
        let response = runtime
            .read_records(ReadRecordsRequest {
                namespace: String::new(),
                r#ref: Some(stream.clone()),
                from_offset,
                max_records: i32::try_from(left).unwrap_or(i32::MAX),
                wait: None,
                max_bytes: 0,
            })
            .await
            .map_err(|error| fail(error.to_string()))?;
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

fn replay_error(message: String) -> tonic::Status {
    let mut status = tonic::Status::new(tonic::Code::DataLoss, message);
    status.metadata_mut().insert(
        STREAM_REPLAY_ERROR_KEY,
        "true".parse().expect("valid metadata"),
    );
    status
}
