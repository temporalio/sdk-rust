//! WASM ABI for driving Temporal Core's activity worker from a host-provided gRPC transport.
//!
//! WASM imports are synchronous, while Core expects several concurrent asynchronous long polls.
//! The bridge therefore exposes a small host-driven event loop: Core queues gRPC requests, the Go
//! host executes them, and then the host returns each response before ticking Core again.

use futures_util::FutureExt;
use http::{HeaderMap, HeaderName, HeaderValue};
use prost::Message;
use std::{
    collections::{HashMap, VecDeque},
    mem::MaybeUninit,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use temporalio_client::{
    Connection, ConnectionOptions,
    callback_based::{
        CallbackBasedGrpcService, GrpcRequest as CallbackGrpcRequest, GrpcSuccessResponse,
    },
};
use temporalio_common::protos::coresdk::{
    ActivityHeartbeat, ActivityTaskCompletion,
    wasm_bridge::{
        GrpcRequest as BridgeGrpcRequest, GrpcResponse as BridgeGrpcResponse, MetadataEntry,
    },
    workflow_completion::WorkflowActivationCompletion,
};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, RuntimeOptions, Worker, WorkerConfig, WorkerTaskTypes,
    WorkerVersioningStrategy, init_worker,
};
use tokio::sync::oneshot;
#[cfg(target_arch = "wasm32")]
use tokio::task::LocalSet;
use tonic::{
    Code, Status,
    metadata::{BinaryMetadataValue, KeyAndValueRef, MetadataMap},
};

const BRIDGE_ERROR: i32 = -1;
const BRIDGE_PENDING: i32 = 1;
const BRIDGE_BUFFER_TOO_SMALL: i32 = 2;
const READY_TASK_DRAIN_TURNS: usize = 16;
const BRIDGE_ABI_U32_LIMIT: usize = u32::MAX as usize;
const MAX_OPERATION_REGISTRY_ENTRIES: usize = 1024;
const MAX_HOST_TRANSPORT_IN_FLIGHT_REQUESTS: usize = 1024;
const MAX_HOST_TRANSPORT_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_HOST_TRANSPORT_QUEUED_BYTES: usize = 16 * 1024 * 1024;

type OperationSlot = Arc<Mutex<OperationState>>;
type ConnectionSlot = Arc<Mutex<Option<Result<Connection, String>>>>;
type OperationRegistry = Arc<Mutex<HashMap<u64, OperationState>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
enum BridgeInitMode {
    ActivityOnly = 0,
    WorkflowOnly = 1,
    Combined = 2,
}

impl BridgeInitMode {
    fn worker_kind(self) -> &'static str {
        match self {
            Self::ActivityOnly => "activity",
            Self::WorkflowOnly => "workflow",
            Self::Combined => "combined",
        }
    }

    fn supports_activities(self) -> bool {
        matches!(self, Self::ActivityOnly | Self::Combined)
    }

    fn supports_workflows(self) -> bool {
        matches!(self, Self::WorkflowOnly | Self::Combined)
    }
}

impl TryFrom<u32> for BridgeInitMode {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::ActivityOnly as u32 => Ok(Self::ActivityOnly),
            x if x == Self::WorkflowOnly as u32 => Ok(Self::WorkflowOnly),
            x if x == Self::Combined as u32 => Ok(Self::Combined),
            _ => Err(format!("unsupported bridge init mode {value}")),
        }
    }
}

enum OperationState {
    Idle,
    Pending,
    Ready(Result<Vec<u8>, String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerInitOptions {
    max_concurrent_activity_executions: usize,
    max_concurrent_activity_task_pollers: usize,
    max_concurrent_workflow_task_executions: usize,
    max_concurrent_workflow_task_pollers: usize,
    max_cached_workflows: usize,
    max_eager_activity_reservations_per_workflow_task: usize,
}

const DEFAULT_MAX_EAGER_ACTIVITY_RESERVATIONS_PER_WORKFLOW_TASK: usize = 3;

struct BridgeState {
    mode: BridgeInitMode,
    runtime: tokio::runtime::Runtime,
    _core_runtime: CoreRuntime,
    worker: Option<Arc<Worker>>,
    worker_config: Option<WorkerConfig>,
    connection_result: ConnectionSlot,
    initialization_result: OperationSlot,
    workflow_poll_result: OperationSlot,
    workflow_completion_results: OperationRegistry,
    activity_poll_result: OperationSlot,
    activity_completion_results: OperationRegistry,
    shutdown_result: OperationSlot,
}

type PendingGrpcRequest = BridgeGrpcRequest;
type HostGrpcResponse = Result<GrpcSuccessResponse, Status>;

#[derive(Default)]
struct QueuedGrpcRequests {
    queue: VecDeque<PendingGrpcRequest>,
    queued_bytes: usize,
}

#[derive(Default)]
struct HostTransport {
    next_id: AtomicU64,
    requests: Mutex<QueuedGrpcRequests>,
    responders: Mutex<HashMap<u64, oneshot::Sender<HostGrpcResponse>>>,
}

impl HostTransport {
    async fn call(&self, request: CallbackGrpcRequest) -> HostGrpcResponse {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (request, request_bytes) = match prepare_pending_grpc_request(id, request) {
            Ok(values) => values,
            Err(error) => return Err(Status::resource_exhausted(error)),
        };
        let (response_tx, response_rx) = oneshot::channel();
        if let Err(error) = self.reserve_response_slot(id, response_tx) {
            return Err(Status::resource_exhausted(error));
        }
        if let Err(error) = self.enqueue_request(request, request_bytes) {
            let _ = self
                .responders
                .lock()
                .expect("host transport responders lock is not poisoned")
                .remove(&id);
            return Err(Status::resource_exhausted(error));
        }
        response_rx
            .await
            .unwrap_or_else(|_| Err(Status::cancelled("host transport response was dropped")))
    }

    fn reserve_response_slot(
        &self,
        id: u64,
        response_tx: oneshot::Sender<HostGrpcResponse>,
    ) -> Result<(), String> {
        let mut responders = self
            .responders
            .lock()
            .map_err(|_| "host transport responders lock is poisoned".to_owned())?;
        if responders.len() >= MAX_HOST_TRANSPORT_IN_FLIGHT_REQUESTS {
            return Err(format!(
                "host transport in-flight request limit of {MAX_HOST_TRANSPORT_IN_FLIGHT_REQUESTS} reached"
            ));
        }
        responders.insert(id, response_tx);
        Ok(())
    }

    fn enqueue_request(
        &self,
        request: PendingGrpcRequest,
        request_bytes: usize,
    ) -> Result<(), String> {
        if request_bytes > MAX_HOST_TRANSPORT_MESSAGE_BYTES {
            return Err(format!(
                "host transport request protobuf is {request_bytes} bytes, exceeds {MAX_HOST_TRANSPORT_MESSAGE_BYTES} byte limit"
            ));
        }
        let mut queued = self
            .requests
            .lock()
            .map_err(|_| "host transport request lock is poisoned".to_owned())?;
        let next_queued_bytes = queued
            .queued_bytes
            .checked_add(request_bytes)
            .ok_or_else(|| "host transport queued request bytes overflowed usize".to_owned())?;
        if next_queued_bytes > MAX_HOST_TRANSPORT_QUEUED_BYTES {
            return Err(format!(
                "host transport queued request bytes would grow to {next_queued_bytes}, exceeds {MAX_HOST_TRANSPORT_QUEUED_BYTES} byte limit"
            ));
        }
        queued.queued_bytes = next_queued_bytes;
        queued.queue.push_back(request);
        Ok(())
    }

    fn complete(&self, id: u64, response: HostGrpcResponse) -> Result<(), String> {
        let sender = self
            .responders
            .lock()
            .map_err(|_| "host transport responders lock is poisoned".to_owned())?
            .remove(&id)
            .ok_or_else(|| format!("unknown host transport request id {id}"))?;
        // Core may cancel a long poll before the host-side gRPC call returns during shutdown.
        let _ = sender.send(response);
        Ok(())
    }

    fn reset(&self) {
        let mut queued = self
            .requests
            .lock()
            .expect("host transport request lock is not poisoned");
        queued.queue.clear();
        queued.queued_bytes = 0;
        self.responders
            .lock()
            .expect("host transport responders lock is not poisoned")
            .clear();
    }
}

static STATE: OnceLock<Mutex<Option<BridgeState>>> = OnceLock::new();
static HOST_TRANSPORT: OnceLock<HostTransport> = OnceLock::new();
#[cfg(target_arch = "wasm32")]
thread_local! {
    static WORKFLOW_LOCAL_SET: LocalSet = LocalSet::new();
}

/// Allocate guest memory that the Go host can populate before calling an exported function.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_alloc(len: usize) -> *mut u8 {
    let mut bytes = Box::<[u8]>::new_uninit_slice(len);
    let ptr = bytes.as_mut_ptr().cast::<u8>();
    let _ = Box::leak(bytes);
    ptr
}

/// Release guest memory previously returned by [`temporal_alloc`].
///
/// # Safety
///
/// `ptr` and `len` must exactly match a live allocation returned by [`temporal_alloc`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn temporal_dealloc(ptr: *mut u8, len: usize) {
    if len == 0 {
        return;
    }
    if !ptr.is_null() {
        // SAFETY: The caller promises this is the pointer and length from temporal_alloc.
        let slice = std::ptr::slice_from_raw_parts_mut(ptr.cast::<MaybeUninit<u8>>(), len);
        drop(unsafe { Box::from_raw(slice) });
    }
}

/// Start initializing one Temporal Core worker with explicit workflow and activity options.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_init_with_worker_options(
    namespace_ptr: *const u8,
    namespace_len: usize,
    task_queue_ptr: *const u8,
    task_queue_len: usize,
    identity_ptr: *const u8,
    identity_len: usize,
    max_concurrent_activity_executions: usize,
    max_concurrent_activity_task_pollers: usize,
    max_concurrent_workflow_task_executions: usize,
    max_concurrent_workflow_task_pollers: usize,
    max_cached_workflows: usize,
    max_eager_activity_reservations_per_workflow_task: usize,
    mode: u32,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let mode = BridgeInitMode::try_from(mode)?;
        let namespace = read_string(namespace_ptr, namespace_len)?;
        let task_queue = read_string(task_queue_ptr, task_queue_len)?;
        let identity = read_string(identity_ptr, identity_len)?;
        let options = decode_worker_init_options(
            mode,
            max_concurrent_activity_executions,
            max_concurrent_activity_task_pollers,
            max_concurrent_workflow_task_executions,
            max_concurrent_workflow_task_pollers,
            max_cached_workflows,
            max_eager_activity_reservations_per_workflow_task,
        )?;
        let state_lock = STATE.get_or_init(|| Mutex::new(None));
        let mut state = state_lock
            .lock()
            .map_err(|_| "Core bridge state lock is poisoned".to_owned())?;
        if state.is_some() {
            return Err("Core worker is already initialized".to_owned());
        }
        HOST_TRANSPORT.get_or_init(HostTransport::default).reset();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| format!("failed to create Tokio runtime: {err}"))?;
        let runtime_options = RuntimeOptions::builder()
            .heartbeat_interval(None)
            .disable_environment_info(true)
            .build()
            .map_err(|err| format!("invalid Core runtime options: {err}"))?;
        let core_runtime = {
            let _guard = runtime.enter();
            CoreRuntime::new_assume_tokio(runtime_options)
                .map_err(|err| format!("failed to initialize Temporal Core: {err}"))?
        };

        let grpc_service = CallbackBasedGrpcService {
            callback: Arc::new(|request| {
                HOST_TRANSPORT
                    .get_or_init(HostTransport::default)
                    .call(request)
                    .boxed()
            }),
        };
        let connection_options = ConnectionOptions::new(
            temporalio_client::Url::parse("http://temporal-host.invalid:7233")
                .expect("static URL is valid"),
        )
        .identity(identity)
        .service_override(grpc_service)
        .dns_load_balancing(None)
        .keep_alive(None)
        .build();

        let worker_config = worker_config_for_mode(mode, namespace, task_queue, options)?;

        let connection_result = Arc::new(Mutex::new(None));
        let initialization_result = operation_slot();
        mark_pending(&initialization_result, "worker initialization")?;
        let connection_result_for_task = connection_result.clone();
        runtime.spawn(async move {
            let result = Connection::connect(connection_options)
                .await
                .map_err(|err| format!("failed to initialize callback gRPC client: {err}"));
            *connection_result_for_task
                .lock()
                .expect("connection result lock is not poisoned") = Some(result);
        });

        *state = Some(BridgeState {
            mode,
            runtime,
            _core_runtime: core_runtime,
            worker: None,
            worker_config: Some(worker_config),
            connection_result,
            initialization_result,
            workflow_poll_result: operation_slot(),
            workflow_completion_results: operation_registry(),
            activity_poll_result: operation_slot(),
            activity_completion_results: operation_registry(),
            shutdown_result: operation_slot(),
        });
        Ok(Vec::new())
    })
}

fn worker_config_for_mode(
    mode: BridgeInitMode,
    namespace: String,
    task_queue: String,
    options: WorkerInitOptions,
) -> Result<WorkerConfig, String> {
    match mode {
        BridgeInitMode::ActivityOnly => activity_worker_config(
            namespace,
            task_queue,
            options.max_concurrent_activity_executions,
            options.max_concurrent_activity_task_pollers,
        ),
        BridgeInitMode::WorkflowOnly => workflow_worker_config(namespace, task_queue, options),
        BridgeInitMode::Combined => combined_worker_config(namespace, task_queue, options),
    }
}

fn decode_worker_init_options(
    mode: BridgeInitMode,
    max_concurrent_activity_executions: usize,
    max_concurrent_activity_task_pollers: usize,
    max_concurrent_workflow_task_executions: usize,
    max_concurrent_workflow_task_pollers: usize,
    max_cached_workflows: usize,
    max_eager_activity_reservations_per_workflow_task: usize,
) -> Result<WorkerInitOptions, String> {
    Ok(WorkerInitOptions {
        max_concurrent_activity_executions,
        max_concurrent_activity_task_pollers,
        max_concurrent_workflow_task_executions,
        max_concurrent_workflow_task_pollers,
        max_cached_workflows,
        max_eager_activity_reservations_per_workflow_task: match mode {
            BridgeInitMode::Combined => {
                if max_eager_activity_reservations_per_workflow_task == 0 {
                    DEFAULT_MAX_EAGER_ACTIVITY_RESERVATIONS_PER_WORKFLOW_TASK
                } else {
                    max_eager_activity_reservations_per_workflow_task
                }
            }
            BridgeInitMode::ActivityOnly | BridgeInitMode::WorkflowOnly => {
                DEFAULT_MAX_EAGER_ACTIVITY_RESERVATIONS_PER_WORKFLOW_TASK
            }
        },
    })
}

fn activity_worker_config(
    namespace: String,
    task_queue: String,
    max_concurrent_activity_executions: usize,
    max_concurrent_activity_task_pollers: usize,
) -> Result<WorkerConfig, String> {
    WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "go-wasm-core-prototype".to_owned(),
        })
        .task_types(WorkerTaskTypes::activity_only())
        .maybe_activity_task_poller_behavior((max_concurrent_activity_task_pollers > 0).then_some(
            PollerBehavior::SimpleMaximum(max_concurrent_activity_task_pollers),
        ))
        .max_outstanding_activities(max_concurrent_activity_executions)
        .build()
        .map_err(|err| format!("invalid activity worker configuration: {err}"))
}

fn workflow_worker_config(
    namespace: String,
    task_queue: String,
    options: WorkerInitOptions,
) -> Result<WorkerConfig, String> {
    WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "go-wasm-core-prototype".to_owned(),
        })
        .task_types(WorkerTaskTypes::workflow_only())
        .max_cached_workflows(options.max_cached_workflows)
        .max_outstanding_workflow_tasks(options.max_concurrent_workflow_task_executions)
        .maybe_workflow_task_poller_behavior(
            (options.max_concurrent_workflow_task_pollers > 0).then_some(
                PollerBehavior::SimpleMaximum(options.max_concurrent_workflow_task_pollers),
            ),
        )
        .build()
        .map_err(|err| format!("invalid workflow worker configuration: {err}"))
}

fn combined_worker_config(
    namespace: String,
    task_queue: String,
    options: WorkerInitOptions,
) -> Result<WorkerConfig, String> {
    WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "go-wasm-core-prototype".to_owned(),
        })
        .task_types(WorkerTaskTypes::all())
        .max_cached_workflows(options.max_cached_workflows)
        .max_outstanding_workflow_tasks(options.max_concurrent_workflow_task_executions)
        .maybe_workflow_task_poller_behavior(
            (options.max_concurrent_workflow_task_pollers > 0).then_some(
                PollerBehavior::SimpleMaximum(options.max_concurrent_workflow_task_pollers),
            ),
        )
        .max_outstanding_activities(options.max_concurrent_activity_executions)
        .maybe_activity_task_poller_behavior(
            (options.max_concurrent_activity_task_pollers > 0).then_some(
                PollerBehavior::SimpleMaximum(options.max_concurrent_activity_task_pollers),
            ),
        )
        .max_eager_activity_reservations_per_workflow_task(
            options.max_eager_activity_reservations_per_workflow_task,
        )
        .build()
        .map_err(|err| format!("invalid combined worker configuration: {err}"))
}

/// Take the result of Core connection and worker validation, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_init(error_ptr: *mut u8, error_capacity: usize) -> i64 {
    let preparation = with_state(|state| {
        if state.worker.is_some() {
            return Ok(());
        }
        let Some(connection) = state
            .connection_result
            .lock()
            .map_err(|_| "connection result lock is poisoned".to_owned())?
            .take()
        else {
            return Ok(());
        };
        let connection = match connection {
            Ok(connection) => connection,
            Err(error) => {
                set_ready(&state.initialization_result, Err(error));
                return Ok(());
            }
        };
        let mode = state.mode;
        let worker_config = state
            .worker_config
            .take()
            .ok_or_else(|| "worker configuration is missing".to_owned())?;
        #[cfg(not(target_arch = "wasm32"))]
        let worker_result = init_worker(&state._core_runtime, worker_config, connection);
        #[cfg(target_arch = "wasm32")]
        let worker_result = WORKFLOW_LOCAL_SET.with(|local| {
            let _guard = local.enter();
            init_worker(&state._core_runtime, worker_config, connection)
        });
        let worker = match worker_result {
            Ok(worker) => Arc::new(worker),
            Err(error) => {
                set_ready(
                    &state.initialization_result,
                    Err(format!(
                        "failed to initialize {} worker: {error}",
                        mode.worker_kind()
                    )),
                );
                return Ok(());
            }
        };
        state.worker = Some(worker.clone());
        let result = state.initialization_result.clone();
        let validation = async move {
            let value =
                worker.validate().await.map(|_| Vec::new()).map_err(|err| {
                    format!("{} worker validation failed: {err}", mode.worker_kind())
                });
            set_ready(&result, value);
        };
        #[cfg(not(target_arch = "wasm32"))]
        drop(state.runtime.spawn(validation));
        #[cfg(target_arch = "wasm32")]
        WORKFLOW_LOCAL_SET.with(|local| drop(local.spawn_local(validation)));
        Ok(())
    });
    if let Err(error) = preparation {
        let result = write_result(error_ptr, error_capacity, || Err(error));
        reset_bridge_state();
        return result;
    }
    let result = take_operation_result(error_ptr, error_capacity, |state| {
        &state.initialization_result
    });
    if unpack_result_code(result) == BRIDGE_ERROR {
        reset_bridge_state();
    }
    result
}

/// Start an asynchronous Core activity poll.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_poll_activity(
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        with_state(|state| {
            ensure_supports_activities(state)?;
            mark_pending(&state.activity_poll_result, "activity poll")?;
            let worker = initialized_worker(state)?;
            let result = state.activity_poll_result.clone();
            state.runtime.spawn(async move {
                let value = worker
                    .poll_activity_task()
                    .await
                    .map(|task| task.encode_to_vec())
                    .map_err(|err| format!("activity poll failed: {err}"));
                set_ready(&result, value);
            });
            Ok(Vec::new())
        })
    })
}

/// Take a completed activity poll result, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_poll_activity(
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    take_operation_result(output_ptr, output_capacity, |state| {
        &state.activity_poll_result
    })
}

/// Start submitting a protobuf-encoded activity completion to Core using a caller ID.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_complete_activity_with_id(
    operation_id: u64,
    completion_ptr: *const u8,
    completion_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    start_complete_activity(
        operation_id,
        completion_ptr,
        completion_len,
        error_ptr,
        error_capacity,
    )
}

fn start_complete_activity(
    operation_id: u64,
    completion_ptr: *const u8,
    completion_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let completion = ActivityTaskCompletion::decode(read_bytes(completion_ptr, completion_len))
            .map_err(|err| format!("invalid ActivityTaskCompletion protobuf: {err}"))?;
        with_state(|state| {
            ensure_supports_activities(state)?;
            let worker = initialized_worker(state)?;
            mark_operation_pending(
                &state.activity_completion_results,
                operation_id,
                "activity completion",
            )?;
            let results = state.activity_completion_results.clone();
            state.runtime.spawn(async move {
                let value = worker
                    .complete_activity_task(completion)
                    .await
                    .map(|_| Vec::new())
                    .map_err(|err| format!("activity completion failed: {err}"));
                set_operation_ready(&results, operation_id, value);
            });
            Ok(Vec::new())
        })
    })
}

/// Take the result of a started activity completion by caller ID.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_complete_activity_with_id(
    operation_id: u64,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    take_activity_completion_result(operation_id, error_ptr, error_capacity)
}

fn take_activity_completion_result(
    operation_id: u64,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    take_operation_registry_result(
        operation_id,
        error_ptr,
        error_capacity,
        |state| &state.activity_completion_results,
        "activity completion",
    )
}

/// Start an asynchronous Core workflow activation poll.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_poll_workflow_activation(
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        with_state(|state| {
            ensure_supports_workflows(state)?;
            mark_pending(&state.workflow_poll_result, "workflow activation poll")?;
            let worker = initialized_worker(state)?;
            let result = state.workflow_poll_result.clone();
            let poll = async move {
                let value = worker
                    .poll_workflow_activation()
                    .await
                    .map(|activation| activation.encode_to_vec())
                    .map_err(|err| format!("workflow activation poll failed: {err}"));
                set_ready(&result, value);
            };
            #[cfg(not(target_arch = "wasm32"))]
            drop(state.runtime.spawn(poll));
            #[cfg(target_arch = "wasm32")]
            WORKFLOW_LOCAL_SET.with(|local| drop(local.spawn_local(poll)));
            Ok(Vec::new())
        })
    })
}

/// Take a completed workflow activation poll result, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_poll_workflow_activation(
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    take_operation_result(output_ptr, output_capacity, |state| {
        &state.workflow_poll_result
    })
}

/// Start submitting a protobuf-encoded workflow activation completion to Core using a caller ID.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_complete_workflow_activation_with_id(
    operation_id: u64,
    completion_ptr: *const u8,
    completion_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    start_complete_workflow_activation(
        operation_id,
        completion_ptr,
        completion_len,
        error_ptr,
        error_capacity,
    )
}

fn start_complete_workflow_activation(
    operation_id: u64,
    completion_ptr: *const u8,
    completion_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let completion =
            WorkflowActivationCompletion::decode(read_bytes(completion_ptr, completion_len))
                .map_err(|err| format!("invalid WorkflowActivationCompletion protobuf: {err}"))?;
        with_state(|state| {
            ensure_supports_workflows(state)?;
            let worker = initialized_worker(state)?;
            mark_operation_pending(
                &state.workflow_completion_results,
                operation_id,
                "workflow activation completion",
            )?;
            let results = state.workflow_completion_results.clone();
            let completion = async move {
                let value = worker
                    .complete_workflow_activation(completion)
                    .await
                    .map(|_| Vec::new())
                    .map_err(|err| format!("workflow activation completion failed: {err}"));
                set_operation_ready(&results, operation_id, value);
            };
            #[cfg(not(target_arch = "wasm32"))]
            drop(state.runtime.spawn(completion));
            #[cfg(target_arch = "wasm32")]
            WORKFLOW_LOCAL_SET.with(|local| drop(local.spawn_local(completion)));
            Ok(Vec::new())
        })
    })
}

/// Take the result of a started workflow activation completion by caller ID.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_complete_workflow_activation_with_id(
    operation_id: u64,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    take_workflow_completion_result(operation_id, error_ptr, error_capacity)
}

fn take_workflow_completion_result(
    operation_id: u64,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    take_operation_registry_result(
        operation_id,
        error_ptr,
        error_capacity,
        |state| &state.workflow_completion_results,
        "workflow activation completion",
    )
}

/// Record a protobuf-encoded activity heartbeat through Core.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_record_activity_heartbeat(
    heartbeat_ptr: *const u8,
    heartbeat_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let heartbeat = ActivityHeartbeat::decode(read_bytes(heartbeat_ptr, heartbeat_len))
            .map_err(|err| format!("invalid ActivityHeartbeat protobuf: {err}"))?;
        with_state(|state| {
            ensure_supports_activities(state)?;
            initialized_worker(state)?.record_activity_heartbeat(heartbeat);
            Ok(Vec::new())
        })
    })
}

/// Advance all currently ready Core tasks without imposing a fixed host-side delay.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_drain_ready_tasks(
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        with_state(|state| {
            #[cfg(not(target_arch = "wasm32"))]
            state.runtime.block_on(async {
                for _ in 0..READY_TASK_DRAIN_TURNS {
                    tokio::task::yield_now().await;
                }
            });
            #[cfg(target_arch = "wasm32")]
            WORKFLOW_LOCAL_SET.with(|local| {
                local.block_on(&state.runtime, async {
                    for _ in 0..READY_TASK_DRAIN_TURNS {
                        tokio::task::yield_now().await;
                    }
                });
            });
            Ok(Vec::new())
        })
    })
}

/// Take the next queued Core gRPC request as a prost-encoded `coresdk.wasm_bridge.GrpcRequest`.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_grpc_request(
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    take_next_grpc_request_output(
        HOST_TRANSPORT.get_or_init(HostTransport::default),
        output_ptr,
        output_capacity,
    )
}

/// Return a prost-encoded `coresdk.wasm_bridge.GrpcResponse` to the waiting Core transport future.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_complete_grpc_request(
    id: u64,
    response_ptr: *const u8,
    response_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let response = read_bytes(response_ptr, response_len);
        ensure_host_transport_response_size(response.len())?;
        let response = decode_grpc_response(response)?;
        let response = into_grpc_response(response)?;
        HOST_TRANSPORT
            .get_or_init(HostTransport::default)
            .complete(id, response)?;
        Ok(Vec::new())
    })
}

/// Start Temporal Core's asynchronous graceful worker shutdown.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_shutdown(error_ptr: *mut u8, error_capacity: usize) -> i64 {
    write_result(error_ptr, error_capacity, || {
        with_state(|state| {
            mark_pending(&state.shutdown_result, "worker shutdown")?;
            let worker = initialized_worker(state)?;
            let result = state.shutdown_result.clone();
            state.runtime.spawn(async move {
                worker.shutdown().await;
                set_ready(&result, Ok(Vec::new()));
            });
            Ok(Vec::new())
        })
    })
}

/// Take the result of graceful worker shutdown, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_shutdown(error_ptr: *mut u8, error_capacity: usize) -> i64 {
    let result = take_operation_result(error_ptr, error_capacity, |state| &state.shutdown_result);
    if unpack_result_code(result) == 0 {
        reset_bridge_state();
    }
    result
}

fn operation_slot() -> OperationSlot {
    Arc::new(Mutex::new(OperationState::Idle))
}

fn operation_registry() -> OperationRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

fn mark_pending(slot: &OperationSlot, name: &str) -> Result<(), String> {
    let mut state = slot
        .lock()
        .map_err(|_| format!("{name} result lock is poisoned"))?;
    if !matches!(*state, OperationState::Idle) {
        return Err(format!("{name} is already in progress"));
    }
    *state = OperationState::Pending;
    Ok(())
}

fn set_ready(slot: &OperationSlot, value: Result<Vec<u8>, String>) {
    *slot.lock().expect("operation result lock is not poisoned") = OperationState::Ready(value);
}

fn mark_operation_pending(
    registry: &OperationRegistry,
    operation_id: u64,
    name: &str,
) -> Result<(), String> {
    let mut operations = registry
        .lock()
        .map_err(|_| format!("{name} registry lock is poisoned"))?;
    if operations.len() >= MAX_OPERATION_REGISTRY_ENTRIES {
        return Err(format!(
            "{name} registry limit of {MAX_OPERATION_REGISTRY_ENTRIES} in-flight operations reached"
        ));
    }
    if operations.contains_key(&operation_id) {
        return Err(format!(
            "{name} operation ID {operation_id} is already in progress"
        ));
    }
    operations.insert(operation_id, OperationState::Pending);
    Ok(())
}

fn set_operation_ready(
    registry: &OperationRegistry,
    operation_id: u64,
    value: Result<Vec<u8>, String>,
) {
    let mut operations = registry
        .lock()
        .expect("operation registry lock is not poisoned");
    if let Some(operation) = operations.get_mut(&operation_id) {
        *operation = OperationState::Ready(value);
    }
}

#[cfg(test)]
fn take_registered_operation(
    registry: &OperationRegistry,
    operation_id: u64,
    name: &str,
) -> Result<Option<Vec<u8>>, String> {
    let mut operations = registry
        .lock()
        .map_err(|_| format!("{name} registry lock is poisoned"))?;
    let Some(operation) = operations.remove(&operation_id) else {
        return Err(format!(
            "{name} operation ID {operation_id} has not been started"
        ));
    };
    match operation {
        OperationState::Ready(result) => result.map(Some),
        OperationState::Pending => {
            operations.insert(operation_id, OperationState::Pending);
            Ok(None)
        }
        OperationState::Idle => Err(format!(
            "{name} operation ID {operation_id} is in an invalid state"
        )),
    }
}

fn reset_operation_registry(registry: &OperationRegistry) {
    registry
        .lock()
        .expect("operation registry lock is not poisoned")
        .clear();
}

fn take_operation_registry_result(
    operation_id: u64,
    error_ptr: *mut u8,
    error_capacity: usize,
    select: impl FnOnce(&BridgeState) -> &OperationRegistry,
    name: &str,
) -> i64 {
    match with_state(|state| Ok(select(state).clone())) {
        Ok(registry) => take_registered_operation_output(
            &registry,
            operation_id,
            error_ptr,
            error_capacity,
            name,
        ),
        Err(error) => write_result(error_ptr, error_capacity, || Err(error)),
    }
}

fn take_operation_result(
    output_ptr: *mut u8,
    output_capacity: usize,
    select: impl FnOnce(&BridgeState) -> &OperationSlot,
) -> i64 {
    match with_state(|state| Ok(select(state).clone())) {
        Ok(slot) => take_operation_slot_result(&slot, output_ptr, output_capacity),
        Err(error) => write_result(output_ptr, output_capacity, || Err(error)),
    }
}

fn with_state<T>(
    operation: impl FnOnce(&mut BridgeState) -> Result<T, String>,
) -> Result<T, String> {
    let state_lock = STATE.get_or_init(|| Mutex::new(None));
    let mut state = state_lock
        .lock()
        .map_err(|_| "Core bridge state lock is poisoned".to_owned())?;
    let state = state
        .as_mut()
        .ok_or_else(|| "Core worker is not initialized".to_owned())?;
    operation(state)
}

fn initialized_worker(state: &BridgeState) -> Result<Arc<Worker>, String> {
    state
        .worker
        .clone()
        .ok_or_else(|| "Core worker initialization is not complete".to_owned())
}

fn ensure_supports_activities(state: &BridgeState) -> Result<(), String> {
    if state.mode.supports_activities() {
        Ok(())
    } else {
        Err(format!(
            "{} bridge does not support activity operations",
            state.mode.worker_kind()
        ))
    }
}

fn ensure_supports_workflows(state: &BridgeState) -> Result<(), String> {
    if state.mode.supports_workflows() {
        Ok(())
    } else {
        Err(format!(
            "{} bridge does not support workflow operations",
            state.mode.worker_kind()
        ))
    }
}

fn reset_bridge_state() {
    if let Ok(mut state) = STATE.get_or_init(|| Mutex::new(None)).lock()
        && let Some(state) = state.take()
    {
        reset_operation_registry(&state.workflow_completion_results);
        reset_operation_registry(&state.activity_completion_results);
    }
    HOST_TRANSPORT.get_or_init(HostTransport::default).reset();
}

fn unpack_result_code(result: i64) -> i32 {
    (result as u64 >> 32) as i32
}

fn checked_result_len(len: usize, context: &str) -> Result<u32, String> {
    u32::try_from(len)
        .map_err(|_| format!("{context} is {len} bytes, exceeds the bridge ABI u32 length limit"))
}

fn ensure_host_transport_response_size(response_len: usize) -> Result<(), String> {
    if response_len > MAX_HOST_TRANSPORT_MESSAGE_BYTES {
        return Err(format!(
            "host transport response payload is {response_len} bytes, exceeds {MAX_HOST_TRANSPORT_MESSAGE_BYTES} byte limit"
        ));
    }
    Ok(())
}

fn encoded_grpc_request_len(request: &PendingGrpcRequest) -> Result<usize, String> {
    let encoded_len = request.encoded_len();
    if encoded_len > BRIDGE_ABI_U32_LIMIT {
        return Err(format!(
            "gRPC request protobuf is {encoded_len} bytes, exceeds the bridge ABI u32 length limit"
        ));
    }
    Ok(encoded_len)
}

fn prepare_pending_grpc_request(
    id: u64,
    request: CallbackGrpcRequest,
) -> Result<(PendingGrpcRequest, usize), String> {
    let pending = PendingGrpcRequest {
        id,
        service: request.service,
        rpc: request.rpc,
        metadata: header_map_into_metadata_entries(request.headers)?,
        proto: request.proto.to_vec(),
    };
    let request_bytes = encoded_grpc_request_len(&pending)?;
    if request_bytes > MAX_HOST_TRANSPORT_MESSAGE_BYTES {
        return Err(format!(
            "host transport request protobuf is {request_bytes} bytes, exceeds {MAX_HOST_TRANSPORT_MESSAGE_BYTES} byte limit"
        ));
    }
    Ok((pending, request_bytes))
}

fn header_map_into_metadata_entries(headers: HeaderMap) -> Result<Vec<MetadataEntry>, String> {
    let metadata = MetadataMap::from_headers(headers);
    let mut entries = Vec::with_capacity(metadata.len());
    for entry in metadata.iter() {
        match entry {
            KeyAndValueRef::Ascii(key, value) => entries.push(MetadataEntry {
                key: key.as_str().to_owned(),
                value: value.as_encoded_bytes().to_vec(),
            }),
            KeyAndValueRef::Binary(key, value) => entries.push(MetadataEntry {
                key: key.as_str().to_owned(),
                value: value
                    .to_bytes()
                    .map_err(|error| {
                        format!(
                            "invalid gRPC binary metadata value for {}: {error}",
                            key.as_str()
                        )
                    })?
                    .to_vec(),
            }),
        }
    }
    Ok(entries)
}

fn encode_grpc_request(request: &PendingGrpcRequest) -> Result<Vec<u8>, String> {
    let _ = encoded_grpc_request_len(request)?;
    Ok(request.encode_to_vec())
}

fn decode_grpc_response(encoded: &[u8]) -> Result<BridgeGrpcResponse, String> {
    BridgeGrpcResponse::decode(encoded)
        .map_err(|error| format!("invalid host gRPC response protobuf: {error}"))
}

fn append_metadata_entries(
    headers: &mut HeaderMap,
    entries: &[MetadataEntry],
    context: &str,
) -> Result<(), String> {
    for entry in entries {
        let name = HeaderName::from_bytes(entry.key.as_bytes())
            .map_err(|error| format!("invalid host gRPC {context} metadata name: {error}"))?;
        let value = if entry.key.ends_with("-bin") {
            HeaderValue::from_bytes(
                BinaryMetadataValue::from_bytes(&entry.value).as_encoded_bytes(),
            )
            .map_err(|error| {
                format!("invalid host gRPC {context} binary metadata value: {error}")
            })?
        } else {
            HeaderValue::from_bytes(&entry.value)
                .map_err(|error| format!("invalid host gRPC {context} metadata value: {error}"))?
        };
        headers.append(name, value);
    }
    Ok(())
}

fn into_grpc_response(response: BridgeGrpcResponse) -> Result<HostGrpcResponse, String> {
    let mut metadata = HeaderMap::with_capacity(response.headers.len() + response.trailers.len());
    append_metadata_entries(&mut metadata, &response.headers, "response header")?;
    // The callback transport exposes one metadata map, so keep the wire-level distinction until
    // this final adapter and merge here to keep custom trailer metadata observable.
    append_metadata_entries(&mut metadata, &response.trailers, "response trailer")?;

    let response = if response.status_code == Code::Ok as i32 {
        Ok(GrpcSuccessResponse {
            headers: metadata,
            proto: response.proto,
        })
    } else {
        Err(Status::with_details_and_metadata(
            Code::from_i32(response.status_code),
            response.status_message,
            response.status_details.into(),
            MetadataMap::from_headers(metadata),
        ))
    };
    Ok(response)
}

fn ready_result_len(result: &Result<Vec<u8>, String>) -> usize {
    match result {
        Ok(output) => output.len(),
        Err(error) => error.len(),
    }
}

fn buffer_too_small_result(
    required_len: usize,
    output_capacity: usize,
    context: &str,
) -> Result<Option<i64>, String> {
    if required_len <= output_capacity {
        return Ok(None);
    }
    Ok(Some(pack_result(
        BRIDGE_BUFFER_TOO_SMALL,
        checked_result_len(required_len, context)?,
    )))
}

fn take_operation_slot_result(
    slot: &OperationSlot,
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    let result = (|| -> Result<i64, String> {
        let mut operation = slot
            .lock()
            .map_err(|_| "operation result lock is poisoned".to_owned())?;
        match &*operation {
            OperationState::Pending => return Ok(pack_result(BRIDGE_PENDING, 0)),
            OperationState::Idle => return Err("operation has not been started".to_owned()),
            OperationState::Ready(result) => {
                if let Some(too_small) = buffer_too_small_result(
                    ready_result_len(result),
                    output_capacity,
                    "bridge output",
                )? {
                    return Ok(too_small);
                }
            }
        }
        let ready = match std::mem::replace(&mut *operation, OperationState::Idle) {
            OperationState::Ready(result) => result,
            OperationState::Pending => return Ok(pack_result(BRIDGE_PENDING, 0)),
            OperationState::Idle => return Err("operation has not been started".to_owned()),
        };
        drop(operation);
        Ok(write_result(output_ptr, output_capacity, || ready))
    })();
    match result {
        Ok(code) => code,
        Err(error) => write_result(output_ptr, output_capacity, || Err(error)),
    }
}

fn take_registered_operation_output(
    registry: &OperationRegistry,
    operation_id: u64,
    output_ptr: *mut u8,
    output_capacity: usize,
    name: &str,
) -> i64 {
    let result = (|| -> Result<i64, String> {
        let mut operations = registry
            .lock()
            .map_err(|_| format!("{name} registry lock is poisoned"))?;
        let Some(operation) = operations.get(&operation_id) else {
            return Err(format!(
                "{name} operation ID {operation_id} has not been started"
            ));
        };
        match operation {
            OperationState::Pending => return Ok(pack_result(BRIDGE_PENDING, 0)),
            OperationState::Idle => {
                return Err(format!(
                    "{name} operation ID {operation_id} is in an invalid state"
                ));
            }
            OperationState::Ready(result) => {
                if let Some(too_small) = buffer_too_small_result(
                    ready_result_len(result),
                    output_capacity,
                    "bridge output",
                )? {
                    return Ok(too_small);
                }
            }
        }
        let ready = match operations.remove(&operation_id) {
            Some(OperationState::Ready(result)) => result,
            Some(OperationState::Pending) => return Ok(pack_result(BRIDGE_PENDING, 0)),
            Some(OperationState::Idle) => {
                return Err(format!(
                    "{name} operation ID {operation_id} is in an invalid state"
                ));
            }
            None => {
                return Err(format!(
                    "{name} operation ID {operation_id} has not been started"
                ));
            }
        };
        drop(operations);
        Ok(write_result(output_ptr, output_capacity, || ready))
    })();
    match result {
        Ok(code) => code,
        Err(error) => write_result(output_ptr, output_capacity, || Err(error)),
    }
}

fn take_next_grpc_request_output(
    transport: &HostTransport,
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    let result = (|| -> Result<i64, String> {
        let mut queued = transport
            .requests
            .lock()
            .map_err(|_| "host transport request lock is poisoned".to_owned())?;
        let Some(request) = queued.queue.front() else {
            return Ok(pack_result(BRIDGE_PENDING, 0));
        };
        let encoded = encode_grpc_request(request)?;
        if let Some(too_small) =
            buffer_too_small_result(encoded.len(), output_capacity, "gRPC request protobuf")?
        {
            return Ok(too_small);
        }
        let request_len = encoded.len();
        let _ = queued.queue.pop_front();
        queued.queued_bytes = queued
            .queued_bytes
            .checked_sub(request_len)
            .ok_or_else(|| "host transport queued request bytes underflowed usize".to_owned())?;
        drop(queued);
        copy_to_guest(output_ptr, &encoded);
        Ok(pack_result(
            0,
            checked_result_len(request_len, "gRPC request protobuf")?,
        ))
    })();
    match result {
        Ok(code) => code,
        Err(error) => write_result(output_ptr, output_capacity, || Err(error)),
    }
}

fn write_result(
    output_ptr: *mut u8,
    output_capacity: usize,
    operation: impl FnOnce() -> Result<Vec<u8>, String>,
) -> i64 {
    match operation() {
        Ok(output) if output.len() <= output_capacity => {
            copy_to_guest(output_ptr, &output);
            match checked_result_len(output.len(), "bridge output") {
                Ok(len) => pack_result(0, len),
                Err(error) => write_error(output_ptr, output_capacity, error),
            }
        }
        Ok(output) => write_error(
            output_ptr,
            output_capacity,
            format!(
                "bridge output is {} bytes but the host provided {} bytes",
                output.len(),
                output_capacity
            ),
        ),
        Err(error) => write_error(output_ptr, output_capacity, error),
    }
}

fn write_error(output_ptr: *mut u8, output_capacity: usize, error: String) -> i64 {
    let bytes = error.as_bytes();
    let len = bytes.len().min(output_capacity).min(BRIDGE_ABI_U32_LIMIT);
    copy_to_guest(output_ptr, &bytes[..len]);
    pack_result(BRIDGE_ERROR, len as u32)
}

fn copy_to_guest(output_ptr: *mut u8, bytes: &[u8]) {
    if bytes.is_empty() || output_ptr.is_null() {
        return;
    }
    // SAFETY: Export callers provide writable guest memory of at least bytes.len().
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), output_ptr, bytes.len()) };
}

fn read_bytes<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        return &[];
    }
    // SAFETY: Export callers provide readable guest memory for the duration of the call.
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

fn read_string(ptr: *const u8, len: usize) -> Result<String, String> {
    String::from_utf8(read_bytes(ptr, len).to_vec())
        .map_err(|err| format!("bridge string parameter is not UTF-8: {err}"))
}

fn pack_result(code: i32, len: u32) -> i64 {
    (((code as u32 as u64) << 32) | len as u64) as i64
}

#[cfg(test)]
fn unpack_result(result: i64) -> (i32, usize) {
    let result = result as u64;
    ((result >> 32) as u32 as i32, result as u32 as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn worker_config_uses_core_activity_concurrency() {
        let config = activity_worker_config("namespace".to_owned(), "queue".to_owned(), 12, 3)
            .expect("worker config is valid");
        assert_eq!(config.max_outstanding_activities, Some(12));
        assert_eq!(
            config.activity_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(3))
        );
    }

    #[test]
    fn worker_config_leaves_default_poller_behavior_to_core() {
        let config = activity_worker_config("namespace".to_owned(), "queue".to_owned(), 12, 0)
            .expect("worker config is valid");
        assert_eq!(config.activity_task_poller_behavior, None);
    }

    #[test]
    fn workflow_worker_config_enables_cached_state_and_evictions() {
        let config = workflow_worker_config(
            "namespace".to_owned(),
            "queue".to_owned(),
            WorkerInitOptions {
                max_concurrent_activity_executions: 12,
                max_concurrent_activity_task_pollers: 3,
                max_concurrent_workflow_task_executions: 2,
                max_concurrent_workflow_task_pollers: 2,
                max_cached_workflows: 1,
                max_eager_activity_reservations_per_workflow_task: 3,
            },
        )
        .expect("worker config is valid");
        assert_eq!(config.task_types, WorkerTaskTypes::workflow_only());
        assert_eq!(config.max_cached_workflows, 1);
        assert_eq!(config.max_outstanding_workflow_tasks, Some(2));
        assert_eq!(
            config.workflow_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(2))
        );
    }

    #[test]
    fn workflow_worker_config_uses_explicit_workflow_concurrency() {
        let config = workflow_worker_config(
            "namespace".to_owned(),
            "queue".to_owned(),
            WorkerInitOptions {
                max_concurrent_activity_executions: 7,
                max_concurrent_activity_task_pollers: 0,
                max_concurrent_workflow_task_executions: 8,
                max_concurrent_workflow_task_pollers: 5,
                max_cached_workflows: 3,
                max_eager_activity_reservations_per_workflow_task: 3,
            },
        )
        .expect("worker config is valid");
        assert_eq!(config.max_cached_workflows, 3);
        assert_eq!(config.max_outstanding_workflow_tasks, Some(8));
        assert_eq!(
            config.workflow_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(5))
        );
    }

    #[test]
    fn workflow_worker_config_leaves_default_poller_behavior_to_core() {
        let config = workflow_worker_config(
            "namespace".to_owned(),
            "queue".to_owned(),
            WorkerInitOptions {
                max_concurrent_activity_executions: 7,
                max_concurrent_activity_task_pollers: 0,
                max_concurrent_workflow_task_executions: 8,
                max_concurrent_workflow_task_pollers: 0,
                max_cached_workflows: 1,
                max_eager_activity_reservations_per_workflow_task: 3,
            },
        )
        .expect("worker config is valid");
        assert_eq!(config.workflow_task_poller_behavior, None);
    }

    #[test]
    fn workflow_worker_config_rejects_core_cache_minimum_violations() {
        let error = workflow_worker_config(
            "namespace".to_owned(),
            "queue".to_owned(),
            WorkerInitOptions {
                max_concurrent_activity_executions: 7,
                max_concurrent_activity_task_pollers: 0,
                max_concurrent_workflow_task_executions: 1,
                max_concurrent_workflow_task_pollers: 1,
                max_cached_workflows: 1,
                max_eager_activity_reservations_per_workflow_task: 3,
            },
        )
        .err()
        .expect("config should fail validation");
        assert!(error.contains("max_outstanding_workflow_tasks"));
    }

    #[test]
    fn worker_config_for_mode_routes_all_bridge_modes() {
        let activity = worker_config_for_mode(
            BridgeInitMode::ActivityOnly,
            "namespace".to_owned(),
            "queue".to_owned(),
            WorkerInitOptions {
                max_concurrent_activity_executions: 4,
                max_concurrent_activity_task_pollers: 2,
                max_concurrent_workflow_task_executions: 8,
                max_concurrent_workflow_task_pollers: 6,
                max_cached_workflows: 5,
                max_eager_activity_reservations_per_workflow_task: 3,
            },
        )
        .expect("activity config is valid");
        let workflow = worker_config_for_mode(
            BridgeInitMode::WorkflowOnly,
            "namespace".to_owned(),
            "queue".to_owned(),
            WorkerInitOptions {
                max_concurrent_activity_executions: 4,
                max_concurrent_activity_task_pollers: 2,
                max_concurrent_workflow_task_executions: 8,
                max_concurrent_workflow_task_pollers: 6,
                max_cached_workflows: 5,
                max_eager_activity_reservations_per_workflow_task: 3,
            },
        )
        .expect("workflow config is valid");
        let combined = worker_config_for_mode(
            BridgeInitMode::Combined,
            "namespace".to_owned(),
            "queue".to_owned(),
            WorkerInitOptions {
                max_concurrent_activity_executions: 4,
                max_concurrent_activity_task_pollers: 2,
                max_concurrent_workflow_task_executions: 8,
                max_concurrent_workflow_task_pollers: 6,
                max_cached_workflows: 5,
                max_eager_activity_reservations_per_workflow_task: 7,
            },
        )
        .expect("combined config is valid");
        assert_eq!(activity.task_types, WorkerTaskTypes::activity_only());
        assert_eq!(workflow.task_types, WorkerTaskTypes::workflow_only());
        assert_eq!(combined.task_types, WorkerTaskTypes::all());
        assert_eq!(activity.max_outstanding_activities, Some(4));
        assert_eq!(workflow.max_cached_workflows, 5);
        assert_eq!(combined.max_outstanding_activities, Some(4));
        assert_eq!(
            combined.max_eager_activity_reservations_per_workflow_task,
            7
        );
    }

    #[test]
    fn init_modes_are_stable_for_hosts() {
        assert_eq!(
            BridgeInitMode::try_from(BridgeInitMode::ActivityOnly as u32).expect("mode is valid"),
            BridgeInitMode::ActivityOnly
        );
        assert_eq!(
            BridgeInitMode::try_from(BridgeInitMode::WorkflowOnly as u32).expect("mode is valid"),
            BridgeInitMode::WorkflowOnly
        );
        assert_eq!(
            BridgeInitMode::try_from(BridgeInitMode::Combined as u32).expect("mode is valid"),
            BridgeInitMode::Combined
        );
        assert!(BridgeInitMode::try_from(99).is_err());
    }

    #[test]
    fn decode_worker_init_options_uses_explicit_combined_eager_settings() {
        let options = decode_worker_init_options(BridgeInitMode::Combined, 9, 2, 8, 3, 17, 5)
            .expect("combined worker settings decode");
        assert_eq!(options.max_cached_workflows, 17);
        assert_eq!(options.max_eager_activity_reservations_per_workflow_task, 5);
    }

    #[test]
    fn bridge_uses_independent_workflow_and_activity_slots() {
        let workflow_poll = operation_slot();
        let workflow_completion = operation_registry();
        let activity_poll = operation_slot();
        let activity_completion = operation_registry();
        assert!(!Arc::ptr_eq(&workflow_poll, &activity_poll));
        assert!(!Arc::ptr_eq(&workflow_completion, &activity_completion));
        assert_eq!(
            workflow_completion
                .lock()
                .expect("registry lock is not poisoned")
                .len(),
            0
        );
    }

    #[test]
    fn workflow_completion_registry_supports_out_of_order_ready_results() {
        let registry = operation_registry();
        mark_operation_pending(&registry, 1, "workflow activation completion")
            .expect("first operation starts");
        mark_operation_pending(&registry, 2, "workflow activation completion")
            .expect("second operation starts");

        set_operation_ready(&registry, 2, Ok(vec![2]));
        set_operation_ready(&registry, 1, Ok(vec![1]));

        assert_eq!(
            take_registered_operation(&registry, 2, "workflow activation completion")
                .expect("second result is available"),
            Some(vec![2])
        );
        assert_eq!(
            take_registered_operation(&registry, 1, "workflow activation completion")
                .expect("first result is available"),
            Some(vec![1])
        );
    }

    #[test]
    fn workflow_completion_registry_rejects_duplicate_unknown_and_reused_ids() {
        let registry = operation_registry();
        mark_operation_pending(&registry, 9, "workflow activation completion")
            .expect("operation starts");

        let duplicate = mark_operation_pending(&registry, 9, "workflow activation completion")
            .expect_err("duplicate ID should fail");
        assert!(duplicate.contains("operation ID 9 is already in progress"));

        let unknown = take_registered_operation(&registry, 7, "workflow activation completion")
            .expect_err("unknown ID should fail");
        assert!(unknown.contains("operation ID 7 has not been started"));

        set_operation_ready(&registry, 9, Ok(Vec::new()));
        assert_eq!(
            take_registered_operation(&registry, 9, "workflow activation completion")
                .expect("result should be available"),
            Some(Vec::new())
        );

        let reused = take_registered_operation(&registry, 9, "workflow activation completion")
            .expect_err("reused ID should fail");
        assert!(reused.contains("operation ID 9 has not been started"));
    }

    #[test]
    fn workflow_completion_registry_keeps_pending_entries_and_allows_second_take_error() {
        let registry = operation_registry();
        mark_operation_pending(&registry, 11, "workflow activation completion")
            .expect("operation starts");

        assert_eq!(
            take_registered_operation(&registry, 11, "workflow activation completion")
                .expect("pending result should not error"),
            None
        );
        assert!(
            registry
                .lock()
                .expect("registry lock is not poisoned")
                .contains_key(&11)
        );

        set_operation_ready(&registry, 11, Ok(vec![1, 1]));
        assert_eq!(
            take_registered_operation(&registry, 11, "workflow activation completion")
                .expect("ready result should be returned"),
            Some(vec![1, 1])
        );

        let second_take =
            take_registered_operation(&registry, 11, "workflow activation completion")
                .expect_err("second take should fail");
        assert!(second_take.contains("operation ID 11 has not been started"));
    }

    #[test]
    fn operation_slot_retains_ready_result_after_undersized_buffer_retry() {
        let slot = operation_slot();
        set_ready(&slot, Ok(vec![1, 2, 3]));

        let mut short_buffer = [0_u8; 2];
        assert_eq!(
            unpack_result(take_operation_slot_result(
                &slot,
                short_buffer.as_mut_ptr(),
                short_buffer.len(),
            )),
            (BRIDGE_BUFFER_TOO_SMALL, 3)
        );
        assert!(matches!(
            *slot.lock().expect("slot lock is not poisoned"),
            OperationState::Ready(Ok(_))
        ));

        let mut retry_buffer = [0_u8; 3];
        assert_eq!(
            unpack_result(take_operation_slot_result(
                &slot,
                retry_buffer.as_mut_ptr(),
                retry_buffer.len(),
            )),
            (0, 3)
        );
        assert_eq!(retry_buffer, [1, 2, 3]);
        assert!(matches!(
            *slot.lock().expect("slot lock is not poisoned"),
            OperationState::Idle
        ));
    }

    #[test]
    fn workflow_completion_registry_cleanup_drops_pending_operations() {
        let registry = operation_registry();
        mark_operation_pending(&registry, 15, "workflow activation completion")
            .expect("operation starts");
        reset_operation_registry(&registry);
        assert!(
            registry
                .lock()
                .expect("registry lock is not poisoned")
                .is_empty()
        );
        set_operation_ready(&registry, 15, Ok(vec![9]));
        assert!(
            registry
                .lock()
                .expect("registry lock is not poisoned")
                .is_empty()
        );
    }

    #[test]
    fn activity_completion_registry_supports_multiple_in_flight_operations() {
        let registry = operation_registry();
        mark_operation_pending(&registry, 21, "activity completion")
            .expect("first operation starts");
        mark_operation_pending(&registry, 22, "activity completion")
            .expect("second operation starts");

        assert_eq!(
            take_registered_operation(&registry, 21, "activity completion")
                .expect("pending result should not error"),
            None
        );
        set_operation_ready(&registry, 22, Ok(vec![2, 2]));
        set_operation_ready(&registry, 21, Ok(vec![1, 1]));

        assert_eq!(
            take_registered_operation(&registry, 22, "activity completion")
                .expect("second result is available"),
            Some(vec![2, 2])
        );
        assert_eq!(
            take_registered_operation(&registry, 21, "activity completion")
                .expect("first result is available"),
            Some(vec![1, 1])
        );
    }

    #[test]
    fn activity_completion_registry_rejects_duplicate_ids_and_drops_reset_entries() {
        let registry = operation_registry();
        mark_operation_pending(&registry, 33, "activity completion").expect("operation starts");

        let duplicate = mark_operation_pending(&registry, 33, "activity completion")
            .expect_err("duplicate ID should fail");
        assert!(duplicate.contains("operation ID 33 is already in progress"));

        reset_operation_registry(&registry);
        assert!(
            take_registered_operation(&registry, 33, "activity completion")
                .expect_err("reset removes pending entries")
                .contains("operation ID 33 has not been started")
        );
    }

    #[test]
    fn completion_registry_take_retries_after_undersized_buffer() {
        let registry = operation_registry();
        mark_operation_pending(&registry, 0, "activity completion").expect("operation starts");
        set_operation_ready(&registry, 0, Ok(vec![4, 5, 6]));

        let mut short_buffer = [0_u8; 2];
        assert_eq!(
            unpack_result(take_registered_operation_output(
                &registry,
                0,
                short_buffer.as_mut_ptr(),
                short_buffer.len(),
                "activity completion",
            )),
            (BRIDGE_BUFFER_TOO_SMALL, 3)
        );
        assert!(
            registry
                .lock()
                .expect("registry lock is not poisoned")
                .contains_key(&0)
        );

        let mut retry_buffer = [0_u8; 3];
        assert_eq!(
            unpack_result(take_registered_operation_output(
                &registry,
                0,
                retry_buffer.as_mut_ptr(),
                retry_buffer.len(),
                "activity completion",
            )),
            (0, 3)
        );
        assert_eq!(retry_buffer, [4, 5, 6]);
        assert!(
            !registry
                .lock()
                .expect("registry lock is not poisoned")
                .contains_key(&0)
        );
    }

    #[test]
    fn operation_registry_rejects_in_flight_limit() {
        let registry = operation_registry();
        for operation_id in 0..MAX_OPERATION_REGISTRY_ENTRIES as u64 {
            mark_operation_pending(&registry, operation_id, "workflow activation completion")
                .expect("operation starts within limit");
        }

        let limit_error = mark_operation_pending(
            &registry,
            MAX_OPERATION_REGISTRY_ENTRIES as u64,
            "workflow activation completion",
        )
        .expect_err("limit should reject additional operations");
        assert!(limit_error.contains(&MAX_OPERATION_REGISTRY_ENTRIES.to_string()));
    }

    #[test]
    fn temporal_alloc_round_trips_host_written_bytes() {
        let len = 4;
        let ptr = temporal_alloc(len);
        assert!(!ptr.is_null());

        let written = [4_u8, 3, 2, 1];
        // SAFETY: ptr comes from temporal_alloc(len) and is valid for len bytes.
        unsafe { std::ptr::copy_nonoverlapping(written.as_ptr(), ptr, len) };
        // SAFETY: ptr comes from temporal_alloc(len) and remains live until temporal_dealloc.
        let actual = unsafe { std::slice::from_raw_parts(ptr, len) };
        assert_eq!(actual, written);

        // SAFETY: ptr and len come directly from temporal_alloc.
        unsafe { temporal_dealloc(ptr, len) };
    }

    #[test]
    fn temporal_alloc_zero_length_dealloc_is_safe() {
        let ptr = temporal_alloc(0);
        // SAFETY: ptr and len come directly from temporal_alloc.
        unsafe { temporal_dealloc(ptr, 0) };
    }

    #[test]
    fn result_word_preserves_signed_code_and_length() {
        assert_eq!(unpack_result(pack_result(-1, 42)), (-1, 42));
        assert_eq!(
            unpack_result(pack_result(Code::Unavailable as i32, 7)),
            (14, 7)
        );
    }

    #[test]
    fn grpc_request_encoding_uses_generated_proto_with_raw_binary_metadata() {
        let mut headers = HeaderMap::new();
        headers.append("initial", HeaderValue::from_static("one"));
        headers.append("initial", HeaderValue::from_static("again"));
        headers.append("initial-bin", HeaderValue::from_static("AQI"));
        let (request, encoded_len) = prepare_pending_grpc_request(
            9,
            CallbackGrpcRequest {
                service: "service".to_owned(),
                rpc: "rpc".to_owned(),
                headers,
                proto: vec![3, 4, 5].into(),
            },
        )
        .expect("request preparation should succeed");
        let encoded = encode_grpc_request(&request).expect("request encoding should succeed");
        let decoded =
            BridgeGrpcRequest::decode(encoded.as_slice()).expect("request protobuf should decode");

        assert_eq!(encoded.len(), encoded_len);
        assert_eq!(decoded.id, 9);
        assert_eq!(decoded.service, "service");
        assert_eq!(decoded.rpc, "rpc");
        assert_eq!(decoded.proto, [3, 4, 5]);
        assert_eq!(
            decoded
                .metadata
                .iter()
                .filter(|entry| entry.key == "initial")
                .map(|entry| entry.value.as_slice())
                .collect::<Vec<_>>(),
            vec![b"one".as_slice(), b"again".as_slice()]
        );
        assert_eq!(
            decoded
                .metadata
                .iter()
                .find(|entry| entry.key == "initial-bin")
                .expect("binary metadata entry is present")
                .value,
            [1, 2]
        );
    }

    #[test]
    fn grpc_response_decoding_preserves_header_trailer_and_binary_metadata_split() {
        let encoded = BridgeGrpcResponse {
            status_code: Code::ResourceExhausted as i32,
            status_message: "limited".to_owned(),
            headers: vec![
                MetadataEntry {
                    key: "initial".to_owned(),
                    value: b"one".to_vec(),
                },
                MetadataEntry {
                    key: "initial".to_owned(),
                    value: b"again".to_vec(),
                },
                MetadataEntry {
                    key: "initial-bin".to_owned(),
                    value: vec![1, 2],
                },
            ],
            trailers: vec![MetadataEntry {
                key: "trailing".to_owned(),
                value: b"two".to_vec(),
            }],
            status_details: vec![9, 8],
            proto: vec![1, 2, 3],
        }
        .encode_to_vec();
        let decoded = decode_grpc_response(&encoded).expect("response protobuf should decode");

        assert_eq!(decoded.status_code, Code::ResourceExhausted as i32);
        assert_eq!(decoded.status_message, "limited");
        assert_eq!(decoded.status_details, [9, 8]);
        assert_eq!(decoded.proto, [1, 2, 3]);
        assert_eq!(
            decoded
                .headers
                .iter()
                .filter(|entry| entry.key == "initial")
                .map(|entry| entry.value.as_slice())
                .collect::<Vec<_>>(),
            vec![b"one".as_slice(), b"again".as_slice()]
        );
        assert_eq!(decoded.headers[2].value, [1, 2]);
        assert_eq!(decoded.trailers[0].value, b"two");
    }

    #[test]
    fn grpc_error_response_preserves_metadata_and_structured_details() {
        let response = into_grpc_response(BridgeGrpcResponse {
            status_code: Code::ResourceExhausted as i32,
            status_message: "limited".to_owned(),
            headers: vec![
                MetadataEntry {
                    key: "initial".to_owned(),
                    value: b"one".to_vec(),
                },
                MetadataEntry {
                    key: "binary-bin".to_owned(),
                    value: vec![1, 2],
                },
            ],
            trailers: vec![MetadataEntry {
                key: "trailing".to_owned(),
                value: b"two".to_vec(),
            }],
            status_details: vec![9, 8],
            proto: Vec::new(),
        })
        .expect("response adaptation should succeed")
        .expect_err("non-OK status should be an error");

        assert_eq!(response.code(), Code::ResourceExhausted);
        assert_eq!(response.message(), "limited");
        assert_eq!(response.details(), [9, 8]);
        assert_eq!(response.metadata().get("initial").unwrap(), "one");
        assert_eq!(response.metadata().get("trailing").unwrap(), "two");
        assert_eq!(
            response
                .metadata()
                .get_bin("binary-bin")
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            [1, 2]
        );
    }

    #[test]
    fn grpc_success_response_preserves_metadata_and_proto() {
        let response = into_grpc_response(BridgeGrpcResponse {
            status_code: Code::Ok as i32,
            status_message: String::new(),
            headers: vec![MetadataEntry {
                key: "initial".to_owned(),
                value: b"one".to_vec(),
            }],
            trailers: vec![MetadataEntry {
                key: "trailing".to_owned(),
                value: b"two".to_vec(),
            }],
            status_details: Vec::new(),
            proto: vec![1, 2, 3],
        })
        .expect("response adaptation should succeed")
        .expect("OK status should be successful");

        assert_eq!(response.headers.get("initial").unwrap(), "one");
        assert_eq!(response.headers.get("trailing").unwrap(), "two");
        assert_eq!(response.proto, [1, 2, 3]);
    }

    #[test]
    fn grpc_response_decoding_rejects_invalid_protobuf() {
        let mut encoded = BridgeGrpcResponse {
            status_code: Code::Ok as i32,
            status_message: String::new(),
            headers: Vec::new(),
            trailers: Vec::new(),
            status_details: Vec::new(),
            proto: vec![1, 2, 3],
        }
        .encode_to_vec();
        encoded.pop();
        assert!(decode_grpc_response(&encoded).is_err());
    }

    #[test]
    fn grpc_request_take_retries_after_undersized_buffer_without_dequeueing() {
        let transport = HostTransport::default();
        let request = PendingGrpcRequest {
            id: 5,
            service: "svc".to_owned(),
            rpc: "rpc".to_owned(),
            metadata: vec![MetadataEntry {
                key: "x-host".to_owned(),
                value: b"ok".to_vec(),
            }],
            proto: vec![3, 4, 5],
        };
        let encoded_len = encoded_grpc_request_len(&request).expect("request size should fit");
        let expected = encode_grpc_request(&request).expect("request encoding should succeed");
        let request_id = request.id;
        {
            let mut queued = transport
                .requests
                .lock()
                .expect("request lock is not poisoned");
            queued.queued_bytes = encoded_len;
            queued.queue.push_back(request);
        }

        let mut short_buffer = [0_u8; 4];
        assert_eq!(
            unpack_result(take_next_grpc_request_output(
                &transport,
                short_buffer.as_mut_ptr(),
                short_buffer.len(),
            )),
            (BRIDGE_BUFFER_TOO_SMALL, expected.len())
        );
        {
            let queued = transport
                .requests
                .lock()
                .expect("request lock is not poisoned");
            assert_eq!(queued.queue.len(), 1);
            assert_eq!(
                queued.queue.front().expect("request remains queued").id,
                request_id
            );
            assert_eq!(queued.queued_bytes, encoded_len);
        }

        let mut retry_buffer = vec![0_u8; expected.len()];
        assert_eq!(
            unpack_result(take_next_grpc_request_output(
                &transport,
                retry_buffer.as_mut_ptr(),
                retry_buffer.len(),
            )),
            (0, expected.len())
        );
        assert_eq!(retry_buffer, expected);
        let queued = transport
            .requests
            .lock()
            .expect("request lock is not poisoned");
        assert!(queued.queue.is_empty());
        assert_eq!(queued.queued_bytes, 0);
    }

    #[test]
    fn host_transport_rejects_in_flight_limit() {
        let transport = HostTransport::default();
        for request_id in 0..MAX_HOST_TRANSPORT_IN_FLIGHT_REQUESTS as u64 {
            transport
                .responders
                .lock()
                .expect("responders lock is not poisoned")
                .insert(request_id, oneshot::channel().0);
        }

        let error = transport
            .reserve_response_slot(99, oneshot::channel().0)
            .expect_err("request should be rejected once in-flight limit is reached");
        assert!(error.contains(&MAX_HOST_TRANSPORT_IN_FLIGHT_REQUESTS.to_string()));
    }

    #[test]
    fn host_transport_rejects_oversized_queued_request_bytes() {
        let transport = HostTransport::default();
        let request = PendingGrpcRequest {
            id: 3,
            service: "svc".to_owned(),
            rpc: "rpc".to_owned(),
            metadata: Vec::new(),
            proto: vec![0_u8; MAX_HOST_TRANSPORT_MESSAGE_BYTES],
        };
        let request_bytes = encoded_grpc_request_len(&request).expect("encoded length should fit");
        let error = transport
            .enqueue_request(request, request_bytes)
            .expect_err("oversized request should be rejected");
        assert!(error.contains(&MAX_HOST_TRANSPORT_MESSAGE_BYTES.to_string()));
    }

    #[test]
    fn host_transport_rejects_total_queued_byte_limit() {
        let transport = HostTransport::default();
        let request = PendingGrpcRequest {
            id: 4,
            service: "svc".to_owned(),
            rpc: "rpc".to_owned(),
            metadata: Vec::new(),
            proto: vec![0_u8; 32],
        };
        let request_bytes = encoded_grpc_request_len(&request).expect("encoded length should fit");
        {
            let mut queued = transport
                .requests
                .lock()
                .expect("request lock is not poisoned");
            queued.queued_bytes = MAX_HOST_TRANSPORT_QUEUED_BYTES - request_bytes + 1;
        }

        let error = transport
            .enqueue_request(request, request_bytes)
            .expect_err("queued byte limit should reject additional requests");
        assert!(error.contains(&MAX_HOST_TRANSPORT_QUEUED_BYTES.to_string()));
    }

    #[test]
    fn late_grpc_response_is_ignored_after_core_cancels_request() {
        let transport = HostTransport::default();
        let (sender, receiver) = oneshot::channel();
        transport
            .responders
            .lock()
            .expect("responders lock is not poisoned")
            .insert(7, sender);
        drop(receiver);

        assert!(
            transport
                .complete(7, Err(Status::cancelled("late response")))
                .is_ok()
        );
    }
}
