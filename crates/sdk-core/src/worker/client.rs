//! Worker-specific client needs

pub(crate) mod mocks;
use crate::{
    protosext::legacy_query_failure,
    worker::{WorkerVersioningStrategy, worker_control_task_queue},
};
use backon::{BackoffBuilder, ExponentialBuilder};
use futures_util::{StreamExt, TryStreamExt, stream};
use parking_lot::Mutex;
use prost::Message;
use prost_types::Duration as PbDuration;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_client::{
    Connection, NamespacedClient, PayloadErrorLimits, RetryOptions, SharedReplaceableClient,
    grpc::{PayloadLimitsClient, WorkflowService},
    request_extensions::{IsWorkerTaskLongPoll, NoRetryOnMatching, RetryConfigForCall},
    worker::ClientWorkerSet,
};
use temporalio_common::protos::{
    TaskToken,
    coresdk::{
        activity_result::ActivityTaskFailedCause, workflow_commands::QueryResult,
        workflow_completion,
    },
    google::rpc::Status as RpcStatus,
    temporal::api::{
        command::v1::Command,
        common::v1::{
            MeteringMetadata, Payloads, WorkerVersionCapabilities, WorkerVersionStamp,
            WorkflowExecution,
        },
        deployment,
        enums::v1::{
            TaskQueueKind, TaskQueueType, VersioningBehavior, WorkerVersioningMode,
            WorkflowTaskFailedCause,
        },
        errordetails::v1::WorkflowTaskCompletionBufferLostFailure,
        failure::v1::Failure,
        nexus::{self, v1::NexusTaskFailure},
        protocol::v1::Message as ProtocolMessage,
        query::v1::WorkflowQueryResult,
        sdk::v1::WorkflowTaskCompletedMetadata,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue, TaskQueueMetadata},
        worker::v1::{WorkerHeartbeat, WorkerSlotsInfo},
        workflowservice::v1::{get_system_info_response::Capabilities, *},
    },
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::{IntoRequest, metadata::MetadataValue};
use uuid::Uuid;

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

pub(crate) fn payload_limit_violation_from(
    status: &tonic::Status,
) -> Option<&temporalio_common::payload_limits::PayloadLimitViolation> {
    std::error::Error::source(status).and_then(|source| source.downcast_ref())
}

/// Maximum encoded size of a single completion page, kept below the ~4 MiB gRPC frame limit. This
/// per-page cap is distinct from the server's namespace-wide limit on the recombined completion
/// size.
///
/// Pages are packed by summing command body sizes only; the 512 KiB of headroom below 4 MiB absorbs
/// everything that sum omits: the per-request overhead (task token, identity, namespace) and the
/// per-command wire framing (a field tag plus a length varint, up to 6 bytes each). At the server's
/// default per-workflow history-count limit (~51,200 events), worst-case framing is ~300 KiB, so
/// this headroom covers even a page of many tiny commands and lets us skip per-command accounting.
const MAX_WFT_COMPLETION_PAGE_SIZE: usize = 4 * 1024 * 1024 - 512 * 1024;
// Conservative heuristic, not a tuned value: caps the client-side burst (concurrent request bodies
// and streams); the cost is only extra serial rounds for completions over this many pages.
const MAX_CONCURRENT_WFT_COMPLETION_PAGES: usize = 3;
// Backoff between resends of lost pages. Values are a conservative heuristic, not tuned;
// `without_max_times` leaves the number of resends to the loop (bounded by a stale token or
// shutdown), not the backoff.
const WFT_COMPLETION_PAGE_RESEND_BACKOFF: ExponentialBuilder = ExponentialBuilder::new()
    .with_min_delay(Duration::from_millis(100))
    .with_factor(2.0)
    .with_max_delay(Duration::from_secs(5))
    .without_max_times();
/// Marker set on the error returned when a completion is failed proactively for exceeding the
/// namespace's recombined completion-size limit, so the workflow layer reports it as
/// `REQUEST_TOO_LARGE`.
pub(crate) static REQUEST_TOO_LARGE_KEY: &str = "request-too-large";

/// How a workflow task completion should be delivered, produced by [paginate_wft_completion].
enum WftCompletionPages {
    /// Send as a single request: it fits within a page, or it cannot be split.
    Single(RespondWorkflowTaskCompletedRequest),
    /// The server buffers only the commands of intermediate pages, so all messages and metadata
    /// ride on the final page.
    Paginated {
        intermediate_pages: Vec<RespondWorkflowTaskCompletedRequest>,
        final_page: RespondWorkflowTaskCompletedRequest,
    },
}

/// Split a completion that may exceed `max_page_bytes` into pages that each stay under it, by
/// distributing its commands across intermediate pages in order.
///
/// Falls back to [WftCompletionPages::Single] when the request already fits, has no commands to
/// distribute, or has a single command that alone exceeds a page (which the server then rejects).
fn paginate_wft_completion(
    mut request: RespondWorkflowTaskCompletedRequest,
    max_page_bytes: usize,
) -> WftCompletionPages {
    if request.encoded_len() <= max_page_bytes {
        return WftCompletionPages::Single(request);
    }

    let intermediate_template = RespondWorkflowTaskCompletedRequest {
        task_token: request.task_token.clone(),
        identity: request.identity.clone(),
        namespace: request.namespace.clone(),
        intermediate_page: true,
        ..Default::default()
    };

    // Pages are packed purely by command body size; MAX_WFT_COMPLETION_PAGE_SIZE reserves headroom
    // for the per-request and per-command overhead this ignores. Only commands can be split across
    // pages, so pagination cannot help when there are none, or when a single command alone exceeds
    // a page.
    if request.commands.is_empty()
        || request
            .commands
            .iter()
            .any(|c| c.encoded_len() > max_page_bytes)
    {
        return WftCompletionPages::Single(request);
    }

    let commands = std::mem::take(&mut request.commands);
    let mut intermediate = Vec::new();
    let mut current = Vec::new();
    let mut current_len = 0;
    for command in commands {
        let command_len = command.encoded_len();
        if !current.is_empty() && current_len + command_len > max_page_bytes {
            let mut page = intermediate_template.clone();
            page.commands = std::mem::take(&mut current);
            page.page_number = intermediate.len() as i32;
            intermediate.push(page);
            current_len = 0;
        }
        current_len += command_len;
        current.push(command);
    }
    if !current.is_empty() {
        let mut page = intermediate_template.clone();
        page.commands = current;
        page.page_number = intermediate.len() as i32;
        intermediate.push(page);
    }

    request.page_number = intermediate.len() as i32;
    request.intermediate_page = false;
    WftCompletionPages::Paginated {
        intermediate_pages: intermediate,
        final_page: request,
    }
}

/// Returns true if `status` carries a `WorkflowTaskCompletionBufferLostFailure` detail, the
/// server's signal that it dropped the buffered pages and they must be resent from page 0.
fn is_workflow_task_completion_buffer_lost(status: &tonic::Status) -> bool {
    RpcStatus::decode(status.details())
        .map(|rpc_status| {
            rpc_status.details.iter().any(|detail| {
                detail
                    .to_msg::<WorkflowTaskCompletionBufferLostFailure>()
                    .is_ok()
            })
        })
        .unwrap_or(false)
}

/// Wraps a completion page in a request that opts out of the client-layer retry for buffer loss,
/// which `complete_workflow_task` recovers itself by resending every page.
fn wft_completion_page_request(
    page: RespondWorkflowTaskCompletedRequest,
) -> tonic::Request<RespondWorkflowTaskCompletedRequest> {
    let mut request = page.into_request();
    request.extensions_mut().insert(NoRetryOnMatching {
        predicate: is_workflow_task_completion_buffer_lost,
    });
    request
}

/// The result of a legacy query sent via `respond_legacy_query`.
pub enum LegacyQueryResult {
    /// The query handler returned a result successfully.
    Succeeded(QueryResult),
    /// The query handler failed.
    Failed(workflow_completion::Failure),
}

/// Contains everything a worker needs to interact with the server
pub(crate) struct WorkerClientBag {
    /// Shared connection handle, used for management operations (capabilities, identity, client
    /// replacement, etc.).
    connection: SharedReplaceableClient<Connection>,
    /// Issues outbound gRPC calls, automatically attaching this worker's payload/memo error limits
    /// (set via `set_payload_error_limits`) so the gRPC layer can enforce them. Wraps a clone of
    /// `connection`, so a client replacement on `connection` is reflected here too.
    client: PayloadLimitsClient<SharedReplaceableClient<Connection>>,
    namespace: String,
    worker_versioning_strategy: WorkerVersioningStrategy,
    worker_instance_key: Uuid,
    worker_heartbeat_map: Arc<Mutex<HashMap<String, ClientHeartbeatData>>>,
}

impl WorkerClientBag {
    pub(crate) fn new(
        connection: SharedReplaceableClient<Connection>,
        namespace: String,
        worker_versioning_strategy: WorkerVersioningStrategy,
        worker_instance_key: Uuid,
    ) -> Self {
        Self {
            client: PayloadLimitsClient::new(connection.clone()),
            connection,
            namespace,
            worker_versioning_strategy,
            worker_instance_key,
            worker_heartbeat_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn identity(&self) -> String {
        self.connection.inner_cow().identity().to_owned()
    }

    fn default_capabilities(&self) -> Capabilities {
        self.capabilities().unwrap_or_default()
    }

    fn binary_checksum(&self) -> String {
        if self.default_capabilities().build_id_based_versioning {
            "".to_string()
        } else {
            self.worker_versioning_strategy.build_id().to_owned()
        }
    }

    fn deployment_options(&self) -> Option<deployment::v1::WorkerDeploymentOptions> {
        match &self.worker_versioning_strategy {
            WorkerVersioningStrategy::WorkerDeploymentBased(dopts) => {
                Some(deployment::v1::WorkerDeploymentOptions {
                    deployment_name: dopts.version.deployment_name.clone(),
                    build_id: dopts.version.build_id.clone(),
                    worker_versioning_mode: if dopts.use_worker_versioning {
                        WorkerVersioningMode::Versioned.into()
                    } else {
                        WorkerVersioningMode::Unversioned.into()
                    },
                })
            }
            _ => None,
        }
    }

    fn worker_version_capabilities(&self) -> Option<WorkerVersionCapabilities> {
        if self.default_capabilities().build_id_based_versioning {
            Some(WorkerVersionCapabilities {
                build_id: self.worker_versioning_strategy.build_id().to_owned(),
                use_versioning: self.worker_versioning_strategy.uses_build_id_based(),
                // This will never be used, as it is the v3 version that we never supported in
                // Core SDKs.
                deployment_series_name: "".to_string(),
            })
        } else {
            None
        }
    }

    fn worker_version_stamp(&self) -> Option<WorkerVersionStamp> {
        if self.default_capabilities().build_id_based_versioning {
            Some(WorkerVersionStamp {
                build_id: self.worker_versioning_strategy.build_id().to_owned(),
                use_versioning: self.worker_versioning_strategy.uses_build_id_based(),
            })
        } else {
            None
        }
    }

    fn worker_control_task_queue(&self) -> String {
        let workers = self.connection.inner_cow().workers();
        if workers.worker_control_task_queue_enabled(&self.namespace) {
            worker_control_task_queue(&self.namespace, &workers.worker_grouping_key().to_string())
        } else {
            String::new()
        }
    }
}

/// This trait contains everything workers need to interact with Temporal, and hence provides a
/// minimal mocking surface.
#[cfg_attr(any(feature = "test-utilities", test), mockall::automock)]
#[async_trait::async_trait]
pub trait WorkerClient: Sync + Send {
    /// Poll workflow tasks
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options: PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse>;
    /// Poll activity tasks
    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse>;
    /// Poll Nexus tasks
    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
        nexus_options: PollNexusOptions,
    ) -> Result<PollNexusTaskQueueResponse>;
    /// Complete a workflow task
    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
        shutdown_token: CancellationToken,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;
    /// Complete an activity task
    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse>;
    /// Complete a Nexus task
    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response: nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse>;
    /// Record an activity heartbeat
    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse>;
    /// Cancel an activity task
    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;
    /// Fail an activity task
    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        cause: ActivityTaskFailedCause,
        failure: Option<Failure>,
        last_heartbeat_details: Option<Payloads>,
    ) -> Result<RespondActivityTaskFailedResponse>;
    /// Fail a workflow task
    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse>;
    /// Fail a Nexus task
    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: NexusTaskFailure,
    ) -> Result<RespondNexusTaskFailedResponse>;
    /// Get the workflow execution history
    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse>;
    /// Respond to a legacy query
    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: LegacyQueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse>;
    /// Describe the namespace
    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse>;
    /// Shutdown the worker
    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
        task_queue: String,
        task_queue_types: Vec<TaskQueueType>,
        final_heartbeat: Option<WorkerHeartbeat>,
    ) -> Result<ShutdownWorkerResponse>;
    /// Record a worker heartbeat
    async fn record_worker_heartbeat(
        &self,
        namespace: String,
        worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> Result<RecordWorkerHeartbeatResponse>;

    /// Replace the underlying connection
    fn replace_connection(&self, new_client: Connection);
    /// Return a clone of the current underlying connection, if one is available.
    fn connection(&self) -> Option<Connection> {
        None
    }
    /// Return server capabilities
    fn capabilities(&self) -> Option<Capabilities>;
    /// Return workers using this client
    fn workers(&self) -> Arc<ClientWorkerSet>;
    /// Indicates if this is a mock client
    fn is_mock(&self) -> bool;
    /// Return name and version of the SDK
    fn sdk_name_and_version(&self) -> (String, String);
    /// Get worker identity
    fn identity(&self) -> String;
    /// Get worker grouping key
    fn worker_grouping_key(&self) -> Uuid;
    /// Get worker instance key (unique per worker instance)
    fn worker_instance_key(&self) -> Uuid;
    /// Sets the client-reliant fields for WorkerHeartbeat. This also updates client-level tracking
    /// of heartbeat fields, like last heartbeat timestamp.
    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat);
    /// Set the worker's payload/memo error limits
    fn set_payload_error_limits(&self, _limits: Option<PayloadErrorLimits>) {}
    /// Get the worker's payload/memo error limits
    fn payload_error_limits(&self) -> Option<PayloadErrorLimits> {
        None
    }
}

/// Configuration options shared by workflow, activity, and Nexus polling calls
#[derive(Debug, Clone)]
pub struct PollOptions {
    /// The name of the task queue to poll
    pub task_queue: String,
    /// Prevents retrying on specific gRPC statuses
    pub no_retry: Option<NoRetryOnMatching>,
    /// Overrides the default RPC timeout for the poll request
    pub timeout_override: Option<Duration>,
}
/// Additional options specific to workflow task polling
#[derive(Debug, Clone)]
pub struct PollWorkflowOptions {
    /// Optional sticky queue name for session‐based workflow polling
    pub sticky_queue_name: Option<String>,
}
/// Additional options specific to activity task polling
#[derive(Debug, Clone)]
pub struct PollActivityOptions {
    /// Optional rate limit (tasks per second) for activity polling
    pub max_tasks_per_sec: Option<f64>,
}
/// Additional options specific to Nexus task polling
#[derive(Debug, Clone, Default)]
pub struct PollNexusOptions {
    /// If true, poll using `TaskQueueKind::WorkerCommands` — the per-process control queue used
    /// by the shared-namespace worker to receive server-to-worker commands.
    pub worker_commands_queue: bool,
}

#[async_trait::async_trait]
impl WorkerClient for WorkerClientBag {
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options: PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        let task_queue = if let Some(sticky) = wf_options.sticky_queue_name {
            TaskQueue {
                name: sticky,
                kind: TaskQueueKind::Sticky.into(),
                normal_name: poll_options.task_queue,
            }
        } else {
            TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal.into(),
                normal_name: "".to_string(),
            }
        };
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollWorkflowTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(task_queue),
            identity: self.identity(),
            binary_checksum: self.binary_checksum(),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_instance_key: self.worker_instance_key.to_string(),
            worker_control_task_queue: self.worker_control_task_queue(),
            poller_group_id: Default::default(),
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .client
            .clone()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollActivityTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity(),
            task_queue_metadata: act_options.max_tasks_per_sec.map(|tps| TaskQueueMetadata {
                max_tasks_per_second: Some(tps),
            }),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_instance_key: self.worker_instance_key.to_string(),
            worker_control_task_queue: self.worker_control_task_queue(),
            poller_group_id: Default::default(),
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .client
            .clone()
            .poll_activity_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
        nexus_options: PollNexusOptions,
    ) -> Result<PollNexusTaskQueueResponse> {
        let (kind, worker_version_capabilities, deployment_options) =
            if nexus_options.worker_commands_queue {
                // Worker-command partitions do not support versioning, even when the client was
                // created for a versioned application worker.
                (TaskQueueKind::WorkerCommands, None, None)
            } else {
                (
                    TaskQueueKind::Normal,
                    self.worker_version_capabilities(),
                    self.deployment_options(),
                )
            };
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollNexusTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: kind as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity(),
            worker_version_capabilities,
            deployment_options,
            // TODO: Piggyback worker heartbeats here if this is the system nexus worker and reset
            //   heartbeating ticker when done
            worker_heartbeat: Vec::new(),
            worker_instance_key: self.worker_instance_key.to_string(),
            poller_group_id: Default::default(),
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .client
            .clone()
            .poll_nexus_task_queue(request)
            .await?
            .into_inner())
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
        shutdown_token: CancellationToken,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        let pagination_enabled = request.pagination_enabled;
        let wft_completion_size_limit = request.wft_completion_size_limit;
        #[allow(deprecated)] // want to list all fields explicitly
        let request = RespondWorkflowTaskCompletedRequest {
            task_token: request.task_token.into(),
            commands: request.commands,
            messages: request.messages,
            identity: self.identity(),
            sticky_attributes: request.sticky_attributes,
            return_new_workflow_task: request.return_new_workflow_task,
            force_create_new_workflow_task: request.force_create_new_workflow_task,
            worker_version_stamp: self.worker_version_stamp(),
            binary_checksum: self.binary_checksum(),
            query_results: request
                .query_responses
                .into_iter()
                .map(|qr| {
                    let (id, completed_type, query_result, error_message) = qr.into_components();
                    (
                        id,
                        WorkflowQueryResult {
                            result_type: completed_type as i32,
                            answer: query_result,
                            error_message,
                            // TODO: https://github.com/temporalio/sdk-core/issues/867
                            failure: None,
                        },
                    )
                })
                .collect(),
            namespace: self.namespace.clone(),
            sdk_metadata: Some(request.sdk_metadata),
            metering_metadata: Some(request.metering_metadata),
            capabilities: Some(respond_workflow_task_completed_request::Capabilities {
                discard_speculative_workflow_task_with_events: true,
            }),
            // Will never be set, deprecated.
            deployment: None,
            versioning_behavior: request.versioning_behavior.into(),
            deployment_options: self.deployment_options(),
            worker_instance_key: self.worker_instance_key.to_string(),
            worker_control_task_queue: self.worker_control_task_queue(),
            resource_id: Default::default(),
            page_number: 0,
            intermediate_page: false,
        };

        let pages = if pagination_enabled {
            paginate_wft_completion(request, MAX_WFT_COMPLETION_PAGE_SIZE)
        } else {
            WftCompletionPages::Single(request)
        };
        let (intermediate_pages, final_page) = match pages {
            WftCompletionPages::Single(request) => {
                return Ok(self
                    .client
                    .clone()
                    .respond_workflow_task_completed(request.into_request())
                    .await?
                    .into_inner());
            }
            WftCompletionPages::Paginated {
                intermediate_pages,
                final_page,
            } => (intermediate_pages, final_page),
        };

        // The server rejects the completion with REQUEST_TOO_LARGE and terminates the workflow once
        // the buffered command bytes exceed the namespace limit, so fail here instead of sending
        // doomed pages. Only buffered command bytes count toward that limit, not messages or
        // metadata, which aren't buffered.
        if let Some(limit) = wft_completion_size_limit {
            let buffered_command_bytes: usize = intermediate_pages
                .iter()
                .flat_map(|page| page.commands.iter())
                .map(|command| command.encoded_len())
                .sum();
            if buffered_command_bytes > limit {
                let mut status = tonic::Status::resource_exhausted(
                    "workflow task completion exceeds the namespace's recombined size limit",
                );
                status
                    .metadata_mut()
                    .insert(REQUEST_TOO_LARGE_KEY, MetadataValue::from(0));
                return Err(status);
            }
        }

        // Buffer loss is transient, so resend the whole set from page 0 with exponential backoff.
        // The server bounds the loop: once the task times out it starts a new attempt, and the next
        // resend fails the token check with a non-buffer-lost error. Worker shutdown ends the loop
        // sooner, so a stream of buffer losses can't hold shutdown's drain open until the task times
        // out (a completion that is not resending still drains normally). Recovery has to live here
        // because the client's retry layer would resend only the single failed page, which cannot
        // rebuild the buffer the server dropped; it is told to pass buffer loss straight through
        // (see `wft_completion_page_request`).
        let mut backoff = WFT_COMPLETION_PAGE_RESEND_BACKOFF.build();
        loop {
            let send_all = async {
                // Cancel in-flight pages on the first error rather than awaiting them: any failure
                // means we fail the task or resend from page 0, so the rest is wasted work.
                stream::iter(intermediate_pages.iter().cloned())
                    .map(|page| {
                        let mut client = self.client.clone();
                        async move {
                            client
                                .respond_workflow_task_completed(wft_completion_page_request(page))
                                .await
                        }
                    })
                    .buffer_unordered(MAX_CONCURRENT_WFT_COMPLETION_PAGES)
                    .try_collect::<Vec<_>>()
                    .await?;
                // The final page must be sent only after every intermediate page has been
                // buffered: it triggers the server-side merge, which requires pages 0..N-1 to all
                // be present and otherwise returns a buffer-lost error.
                self.client
                    .clone()
                    .respond_workflow_task_completed(wft_completion_page_request(
                        final_page.clone(),
                    ))
                    .await
            };
            match send_all.await {
                Ok(response) => return Ok(response.into_inner()),
                Err(e) if is_workflow_task_completion_buffer_lost(&e) => {
                    let delay = backoff.next().expect("resend backoff is unbounded");
                    tokio::select! {
                        _ = shutdown_token.cancelled() => return Err(e),
                        _ = sleep(delay) => {}
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(self
            .client
            .clone()
            .respond_activity_task_completed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCompletedRequest {
                    task_token: task_token.into_inner(),
                    result,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response: nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse> {
        Ok(self
            .client
            .clone()
            .respond_nexus_task_completed(
                RespondNexusTaskCompletedRequest {
                    namespace: self.namespace.clone(),
                    identity: self.identity(),
                    task_token: task_token.into_inner(),
                    response: Some(response),
                    poller_group_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(self
            .client
            .clone()
            .record_activity_task_heartbeat(
                RecordActivityTaskHeartbeatRequest {
                    task_token: task_token.into_inner(),
                    details,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .client
            .clone()
            .respond_activity_task_canceled(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCanceledRequest {
                    task_token: task_token.into_inner(),
                    details,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        // Unused until `RespondActivityTaskFailedRequest` gains a cause field
        //  (https://github.com/temporalio/api/pull/816). Taken as a parameter regardless so the
        //  cause is decided next to the failure it describes, as `fail_workflow_task` does.
        _cause: ActivityTaskFailedCause,
        failure: Option<Failure>,
        last_heartbeat_details: Option<Payloads>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        Ok(self
            .client
            .clone()
            .respond_activity_task_failed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskFailedRequest {
                    task_token: task_token.into_inner(),
                    failure,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    last_heartbeat_details,
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let request = RespondWorkflowTaskFailedRequest {
            task_token: task_token.into_inner(),
            cause: cause as i32,
            failure,
            identity: self.identity(),
            binary_checksum: self.binary_checksum(),
            namespace: self.namespace.clone(),
            messages: vec![],
            worker_version: self.worker_version_stamp(),
            // Will never be set, deprecated.
            deployment: None,
            deployment_options: self.deployment_options(),
            resource_id: Default::default(),
        };
        Ok(self
            .client
            .clone()
            .respond_workflow_task_failed(request.into_request())
            .await?
            .into_inner())
    }

    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: NexusTaskFailure,
    ) -> Result<RespondNexusTaskFailedResponse> {
        let (error, failure) = match error {
            NexusTaskFailure::Legacy(handler_err) => (Some(handler_err), None),
            NexusTaskFailure::Temporal(failure) => (None, Some(failure)),
        };

        Ok(self
            .client
            .clone()
            .respond_nexus_task_failed(
                #[allow(deprecated)]
                RespondNexusTaskFailedRequest {
                    namespace: self.namespace.clone(),
                    identity: self.identity(),
                    task_token: task_token.into_inner(),
                    failure,
                    error,
                    poller_group_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        Ok(self
            .client
            .clone()
            .get_workflow_execution_history(
                GetWorkflowExecutionHistoryRequest {
                    namespace: self.namespace.clone(),
                    execution: Some(WorkflowExecution {
                        workflow_id,
                        run_id: run_id.unwrap_or_default(),
                    }),
                    next_page_token: page_token,
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: LegacyQueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        let mut failure = None;
        let (query_result, cause) = match query_result {
            LegacyQueryResult::Succeeded(s) => (s, WorkflowTaskFailedCause::Unspecified),
            #[allow(deprecated)]
            LegacyQueryResult::Failed(f) => {
                let cause = f.force_cause();
                failure = f.failure.clone();
                (legacy_query_failure(f), cause)
            }
        };
        let (_, completed_type, query_result, error_message) = query_result.into_components();

        Ok(self
            .client
            .clone()
            .respond_query_task_completed(
                RespondQueryTaskCompletedRequest {
                    task_token: task_token.into(),
                    completed_type: completed_type as i32,
                    query_result,
                    error_message,
                    namespace: self.namespace.clone(),
                    failure,
                    cause: cause.into(),
                    poller_group_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse> {
        Ok(self
            .client
            .clone()
            .describe_namespace(
                DescribeNamespaceRequest {
                    namespace: self.namespace.clone(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
        task_queue: String,
        task_queue_types: Vec<TaskQueueType>,
        final_heartbeat: Option<WorkerHeartbeat>,
    ) -> Result<ShutdownWorkerResponse> {
        let mut final_heartbeat = final_heartbeat;
        if let Some(w) = final_heartbeat.as_mut() {
            self.set_heartbeat_client_fields(w);
        }
        let mut request = ShutdownWorkerRequest {
            namespace: self.namespace.clone(),
            identity: self.identity(),
            sticky_task_queue,
            reason: "graceful shutdown".to_string(),
            worker_heartbeat: final_heartbeat,
            worker_instance_key: self.worker_instance_key.to_string(),
            task_queue,
            task_queue_types: task_queue_types.into_iter().map(|t| t as i32).collect(),
        }
        .into_request();
        request
            .extensions_mut()
            .insert(RetryConfigForCall(RetryOptions::no_retries()));

        Ok(
            WorkflowService::shutdown_worker(&mut self.client.clone(), request)
                .await?
                .into_inner(),
        )
    }

    async fn record_worker_heartbeat(
        &self,
        namespace: String,
        worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> Result<RecordWorkerHeartbeatResponse> {
        let request = RecordWorkerHeartbeatRequest {
            namespace,
            identity: self.identity(),
            worker_heartbeat,
            resource_id: Default::default(),
        };
        Ok(self
            .client
            .clone()
            .record_worker_heartbeat(request.into_request())
            .await?
            .into_inner())
    }

    fn replace_connection(&self, new_connection: Connection) {
        self.connection.replace_client(new_connection);
    }

    fn connection(&self) -> Option<Connection> {
        Some(self.connection.inner_clone())
    }

    fn capabilities(&self) -> Option<Capabilities> {
        self.connection.inner_cow().capabilities().cloned()
    }

    fn workers(&self) -> Arc<ClientWorkerSet> {
        self.connection.inner_cow().workers()
    }

    fn is_mock(&self) -> bool {
        false
    }

    fn sdk_name_and_version(&self) -> (String, String) {
        let inner = self.connection.inner_cow();
        (
            inner.client_name().to_owned(),
            inner.client_version().to_owned(),
        )
    }

    fn identity(&self) -> String {
        self.identity()
    }

    fn worker_grouping_key(&self) -> Uuid {
        self.connection.inner_cow().worker_grouping_key()
    }

    fn worker_instance_key(&self) -> Uuid {
        self.worker_instance_key
    }

    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat) {
        if let Some(host_info) = heartbeat.host_info.as_mut() {
            host_info.worker_grouping_key = self.worker_grouping_key().to_string();
        }
        heartbeat.worker_identity = WorkerClient::identity(self);
        let sdk_name_and_ver = self.sdk_name_and_version();
        heartbeat.sdk_name = sdk_name_and_ver.0;
        heartbeat.sdk_version = sdk_name_and_ver.1;

        let now = SystemTime::now();
        heartbeat.heartbeat_time = Some(now.into());
        let mut heartbeat_map = self.worker_heartbeat_map.lock();
        let client_heartbeat_data = heartbeat_map
            .entry(heartbeat.worker_instance_key.clone())
            .or_default();
        let elapsed_since_last_heartbeat =
            client_heartbeat_data.last_heartbeat_time.map(|hb_time| {
                let dur = now.duration_since(hb_time).unwrap_or(Duration::ZERO);
                PbDuration {
                    seconds: dur.as_secs() as i64,
                    nanos: dur.subsec_nanos() as i32,
                }
            });
        heartbeat.elapsed_since_last_heartbeat = elapsed_since_last_heartbeat;
        client_heartbeat_data.last_heartbeat_time = Some(now);

        update_slots(
            &mut heartbeat.workflow_task_slots_info,
            &mut client_heartbeat_data.workflow_task_slots_info,
        );
        update_slots(
            &mut heartbeat.activity_task_slots_info,
            &mut client_heartbeat_data.activity_task_slots_info,
        );
        update_slots(
            &mut heartbeat.nexus_task_slots_info,
            &mut client_heartbeat_data.nexus_task_slots_info,
        );
        update_slots(
            &mut heartbeat.local_activity_slots_info,
            &mut client_heartbeat_data.local_activity_slots_info,
        );
    }

    fn set_payload_error_limits(&self, limits: Option<PayloadErrorLimits>) {
        self.client.set_error_limits(limits);
    }

    fn payload_error_limits(&self) -> Option<PayloadErrorLimits> {
        self.client.error_limits()
    }
}

impl NamespacedClient for WorkerClientBag {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn identity(&self) -> String {
        self.identity()
    }
}

/// A version of [RespondWorkflowTaskCompletedRequest] that will finish being filled out by the
/// server client
#[derive(Debug, Clone)]
pub struct WorkflowTaskCompletion {
    /// The task token that would've been received from polling for a workflow activation
    pub task_token: TaskToken,
    /// A list of new commands to send to the server, such as starting a timer.
    pub commands: Vec<Command>,
    /// A list of protocol messages to send to the server.
    pub messages: Vec<ProtocolMessage>,
    /// If set, indicate that next task should be queued on sticky queue with given attributes.
    pub sticky_attributes: Option<StickyExecutionAttributes>,
    /// Responses to queries in the `queries` field of the workflow task.
    pub query_responses: Vec<QueryResult>,
    /// Indicate that the task completion should return a new WFT if one is available
    pub return_new_workflow_task: bool,
    /// Force a new WFT to be created after this completion
    pub force_create_new_workflow_task: bool,
    /// SDK-specific metadata to send
    pub sdk_metadata: WorkflowTaskCompletedMetadata,
    /// Metering info
    pub metering_metadata: MeteringMetadata,
    /// Versioning behavior of the workflow, if any.
    pub versioning_behavior: VersioningBehavior,
    /// Whether the namespace permits paginating this completion across multiple page requests when
    /// it would otherwise exceed the server's gRPC request size limit.
    pub pagination_enabled: bool,
    /// The namespace's limit on the recombined size of a paginated completion, if the server
    /// advertises one. A paginated completion larger than this is rejected server-side with
    /// `REQUEST_TOO_LARGE`, so the worker fails it proactively instead of sending doomed pages.
    pub wft_completion_size_limit: Option<usize>,
}

#[derive(Clone, Default)]
struct SlotsInfo {
    total_processed_tasks: i32,
    total_failed_tasks: i32,
}

#[derive(Clone, Default)]
struct ClientHeartbeatData {
    last_heartbeat_time: Option<SystemTime>,

    workflow_task_slots_info: SlotsInfo,
    activity_task_slots_info: SlotsInfo,
    nexus_task_slots_info: SlotsInfo,
    local_activity_slots_info: SlotsInfo,
}

fn update_slots(slots_info: &mut Option<WorkerSlotsInfo>, client_heartbeat_data: &mut SlotsInfo) {
    if let Some(wft_slot_info) = slots_info.as_mut() {
        wft_slot_info.last_interval_processed_tasks =
            wft_slot_info.total_processed_tasks - client_heartbeat_data.total_processed_tasks;
        wft_slot_info.last_interval_failure_tasks =
            wft_slot_info.total_failed_tasks - client_heartbeat_data.total_failed_tasks;

        client_heartbeat_data.total_processed_tasks = wft_slot_info.total_processed_tasks;
        client_heartbeat_data.total_failed_tasks = wft_slot_info.total_failed_tasks;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::sync::{Arc, Mutex};
    use temporalio_client::{
        ConnectionOptions,
        callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse},
    };
    use temporalio_common::worker::{WorkerDeploymentOptions, WorkerDeploymentVersion};

    #[tokio::test]
    async fn activity_failure_request_includes_last_heartbeat_details() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_clone = requests.clone();
        let service_override = CallbackBasedGrpcService {
            callback: Arc::new(move |request| {
                let requests = requests_clone.clone();
                Box::pin(async move {
                    let proto = match request.rpc.as_str() {
                        "GetSystemInfo" => GetSystemInfoResponse::default().encode_to_vec(),
                        "RespondActivityTaskFailed" => {
                            requests.lock().unwrap().push(
                                RespondActivityTaskFailedRequest::decode(request.proto)
                                    .expect("failure request is valid"),
                            );
                            RespondActivityTaskFailedResponse::default().encode_to_vec()
                        }
                        rpc => panic!("unexpected RPC: {rpc}"),
                    };
                    Ok(GrpcSuccessResponse {
                        headers: Default::default(),
                        proto,
                    })
                })
            }),
        };
        let connection = Connection::connect(
            ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                .service_override(service_override)
                .dns_load_balancing(None)
                .build(),
        )
        .await
        .unwrap();
        let client = WorkerClientBag::new(
            SharedReplaceableClient::new(connection),
            "namespace".to_string(),
            WorkerVersioningStrategy::None {
                build_id: String::new(),
            },
            Uuid::new_v4(),
        );
        let last_heartbeat_details = Payloads {
            payloads: vec![
                temporalio_common::protos::temporal::api::common::v1::Payload {
                    data: vec![1, 2, 3],
                    ..Default::default()
                },
            ],
        };

        client
            .fail_activity_task(
                vec![1].into(),
                ActivityTaskFailedCause::ActivityWorkerUnhandledFailure,
                None,
                Some(last_heartbeat_details.clone()),
            )
            .await
            .unwrap();

        assert_eq!(
            requests.lock().unwrap()[0].last_heartbeat_details,
            Some(last_heartbeat_details)
        );
    }

    #[allow(deprecated)]
    #[tokio::test]
    async fn worker_command_nexus_polls_omit_versioning_metadata() {
        let strategies = [
            (
                "deployment",
                WorkerVersioningStrategy::WorkerDeploymentBased(
                    WorkerDeploymentOptions::new(
                        WorkerDeploymentVersion::builder()
                            .deployment_name("deployment".to_string())
                            .build_id("deployment-build".to_string())
                            .build(),
                    )
                    .use_worker_versioning(true)
                    .build(),
                ),
            ),
            (
                "legacy",
                WorkerVersioningStrategy::LegacyBuildIdBased {
                    build_id: "legacy-build".to_string(),
                },
            ),
        ];

        for (strategy_name, strategy) in strategies {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_clone = requests.clone();
            let service_override = CallbackBasedGrpcService {
                callback: Arc::new(move |request| {
                    let requests = requests_clone.clone();
                    Box::pin(async move {
                        let proto = match request.rpc.as_str() {
                            "GetSystemInfo" => GetSystemInfoResponse {
                                capabilities: Some(Capabilities {
                                    build_id_based_versioning: true,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                            "PollNexusTaskQueue" => {
                                requests.lock().unwrap().push(
                                    PollNexusTaskQueueRequest::decode(request.proto)
                                        .expect("poll request is valid"),
                                );
                                PollNexusTaskQueueResponse::default().encode_to_vec()
                            }
                            rpc => panic!("unexpected RPC: {rpc}"),
                        };
                        Ok(GrpcSuccessResponse {
                            headers: Default::default(),
                            proto,
                        })
                    })
                }),
            };
            let connection = Connection::connect(
                ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                    .service_override(service_override)
                    .dns_load_balancing(None)
                    .build(),
            )
            .await
            .unwrap();
            let client = WorkerClientBag::new(
                SharedReplaceableClient::new(connection),
                "namespace".to_string(),
                strategy,
                Uuid::new_v4(),
            );

            client
                .poll_nexus_task(
                    PollOptions {
                        task_queue: "application-queue".to_string(),
                        no_retry: None,
                        timeout_override: None,
                    },
                    PollNexusOptions {
                        worker_commands_queue: false,
                    },
                )
                .await
                .unwrap();
            client
                .poll_nexus_task(
                    PollOptions {
                        task_queue: "worker-command-queue".to_string(),
                        no_retry: None,
                        timeout_override: None,
                    },
                    PollNexusOptions {
                        worker_commands_queue: true,
                    },
                )
                .await
                .unwrap();

            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 2, "{strategy_name}");
            let normal_poll = &requests[0];
            assert_eq!(
                normal_poll.task_queue.as_ref().unwrap().kind,
                TaskQueueKind::Normal as i32,
                "{strategy_name}",
            );
            assert!(
                normal_poll
                    .worker_version_capabilities
                    .as_ref()
                    .is_some_and(|capabilities| capabilities.use_versioning)
                    || normal_poll
                        .deployment_options
                        .as_ref()
                        .is_some_and(|options| {
                            options.worker_versioning_mode == WorkerVersioningMode::Versioned as i32
                        }),
                "{strategy_name}",
            );

            let worker_command_poll = &requests[1];
            assert_eq!(
                worker_command_poll.task_queue.as_ref().unwrap().kind,
                TaskQueueKind::WorkerCommands as i32,
                "{strategy_name}",
            );
            assert!(
                worker_command_poll.worker_version_capabilities.is_none(),
                "{strategy_name}",
            );
            assert!(
                worker_command_poll.deployment_options.is_none(),
                "{strategy_name}",
            );
        }
    }

    mod pagination {
        use super::*;
        use temporalio_common::protos::temporal::api::{
            command::v1::{CompleteWorkflowExecutionCommandAttributes, command},
            common::v1::{Payload, Payloads},
            errordetails::v1::WorkflowExecutionAlreadyStartedFailure,
        };

        fn command_with_payload(data_size: usize) -> Command {
            Command {
                attributes: Some(
                    command::Attributes::CompleteWorkflowExecutionCommandAttributes(
                        CompleteWorkflowExecutionCommandAttributes {
                            result: Some(Payloads {
                                payloads: vec![Payload {
                                    metadata: Default::default(),
                                    data: vec![0u8; data_size],
                                    ..Default::default()
                                }],
                            }),
                        },
                    ),
                ),
                ..Default::default()
            }
        }

        fn request_with(commands: Vec<Command>) -> RespondWorkflowTaskCompletedRequest {
            RespondWorkflowTaskCompletedRequest {
                task_token: b"task-token".to_vec(),
                identity: "identity".to_string(),
                namespace: "namespace".to_string(),
                commands,
                ..Default::default()
            }
        }

        #[test]
        fn completion_within_limit_is_a_single_final_page() {
            let request = request_with(vec![command_with_payload(16)]);
            let WftCompletionPages::Single(page) = paginate_wft_completion(request, 4096) else {
                panic!("expected a single page");
            };
            assert_eq!(page.page_number, 0);
            assert!(!page.intermediate_page);
            assert_eq!(page.commands.len(), 1);
        }

        #[test]
        fn large_completion_splits_commands_across_pages() {
            let max = 1024;
            let command_count = 6;
            let commands: Vec<_> = (0..command_count)
                .map(|_| command_with_payload(400))
                .collect();
            let request = request_with(commands);
            assert!(request.encoded_len() > max);

            let WftCompletionPages::Paginated {
                intermediate_pages: intermediate,
                final_page,
            } = paginate_wft_completion(request, max)
            else {
                panic!("expected multiple pages");
            };

            assert!(!final_page.intermediate_page);
            assert!(final_page.commands.is_empty());
            assert_eq!(final_page.page_number as usize, intermediate.len());
            assert!(final_page.encoded_len() <= max);
            assert_eq!(final_page.task_token, b"task-token");

            let mut total_commands = 0;
            for (idx, page) in intermediate.iter().enumerate() {
                assert!(page.intermediate_page);
                assert_eq!(page.page_number as usize, idx);
                assert_eq!(page.task_token, b"task-token");
                assert!(
                    page.encoded_len() <= max,
                    "intermediate page {idx} over limit"
                );
                total_commands += page.commands.len();
            }
            // Every command is preserved exactly once across the intermediate pages.
            assert_eq!(total_commands, command_count);
        }

        #[test]
        fn single_command_larger_than_a_page_is_not_split() {
            let max = 1024;
            let request = request_with(vec![command_with_payload(4096)]);
            // Cannot be split, so it is left as one (oversized) request for the server to reject.
            let WftCompletionPages::Single(page) = paginate_wft_completion(request, max) else {
                panic!("expected a single page");
            };
            assert_eq!(page.commands.len(), 1);
            assert!(!page.intermediate_page);
        }

        // Pack the detail with `Any::from_msg` so its `type_url` is derived from the message name,
        // the way the server sets it, rather than a hand-written string.
        fn status_with_detail<M: prost::Name>(detail: &M) -> tonic::Status {
            let rpc_status = RpcStatus {
                code: tonic::Code::Aborted as i32,
                message: String::new(),
                details: vec![prost_types::Any::from_msg(detail).expect("detail encodes")],
            };
            tonic::Status::with_details(tonic::Code::Aborted, "", rpc_status.encode_to_vec().into())
        }

        #[test]
        fn detects_buffer_lost_failure_detail() {
            let status = status_with_detail(&WorkflowTaskCompletionBufferLostFailure {});
            assert!(is_workflow_task_completion_buffer_lost(&status));

            let unrelated = tonic::Status::new(tonic::Code::Internal, "boom");
            assert!(!is_workflow_task_completion_buffer_lost(&unrelated));
        }

        #[test]
        fn buffer_lost_detection_ignores_unrelated_detail() {
            // A different error detail carried on the same gRPC code must not be mistaken for a
            // buffer-lost failure.
            let status = status_with_detail(&WorkflowExecutionAlreadyStartedFailure {
                start_request_id: "req".to_string(),
                run_id: "run".to_string(),
                ..Default::default()
            });
            assert!(!is_workflow_task_completion_buffer_lost(&status));
        }

        #[tokio::test]
        async fn paginated_completion_sends_ordered_pages_sharing_a_token() {
            let captured = Arc::new(Mutex::new(Vec::new()));
            let captured_clone = captured.clone();
            let service_override = CallbackBasedGrpcService {
                callback: Arc::new(move |request| {
                    let captured = captured_clone.clone();
                    Box::pin(async move {
                        let proto = match request.rpc.as_str() {
                            "GetSystemInfo" => GetSystemInfoResponse {
                                capabilities: Some(Capabilities::default()),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                            "RespondWorkflowTaskCompleted" => {
                                captured.lock().unwrap().push(
                                    RespondWorkflowTaskCompletedRequest::decode(request.proto)
                                        .expect("completion request is valid"),
                                );
                                RespondWorkflowTaskCompletedResponse::default().encode_to_vec()
                            }
                            rpc => panic!("unexpected RPC: {rpc}"),
                        };
                        Ok(GrpcSuccessResponse {
                            headers: Default::default(),
                            proto,
                        })
                    })
                }),
            };
            let connection = Connection::connect(
                ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                    .service_override(service_override)
                    .dns_load_balancing(None)
                    .build(),
            )
            .await
            .unwrap();
            let client = WorkerClientBag::new(
                SharedReplaceableClient::new(connection),
                "namespace".to_string(),
                WorkerVersioningStrategy::LegacyBuildIdBased {
                    build_id: "test-build".to_string(),
                },
                Uuid::new_v4(),
            );

            // Roughly 4 MiB of commands forces splitting under the ~3 MiB page target.
            let commands: Vec<_> = (0..8).map(|_| command_with_payload(512 * 1024)).collect();
            let completion = WorkflowTaskCompletion {
                task_token: b"shared-token".to_vec().into(),
                commands,
                messages: vec![],
                sticky_attributes: None,
                query_responses: vec![],
                return_new_workflow_task: false,
                force_create_new_workflow_task: false,
                sdk_metadata: Default::default(),
                metering_metadata: Default::default(),
                versioning_behavior: VersioningBehavior::Unspecified,
                pagination_enabled: true,
                wft_completion_size_limit: None,
            };
            client
                .complete_workflow_task(completion, CancellationToken::new())
                .await
                .unwrap();

            let sent = captured.lock().unwrap();
            assert!(
                sent.len() >= 2,
                "expected multiple pages, got {}",
                sent.len()
            );
            // Every page shares the one task token.
            assert!(sent.iter().all(|r| r.task_token == b"shared-token"));
            // Exactly one final page, numbered after all the intermediate ones.
            let finals: Vec<_> = sent.iter().filter(|r| !r.intermediate_page).collect();
            assert_eq!(finals.len(), 1);
            assert_eq!(finals[0].page_number as usize, sent.len() - 1);
            assert!(finals[0].commands.is_empty());
            // Intermediate pages carry sequential page numbers 0..N-1.
            let mut intermediate_numbers: Vec<_> = sent
                .iter()
                .filter(|r| r.intermediate_page)
                .map(|r| r.page_number)
                .collect();
            intermediate_numbers.sort_unstable();
            assert_eq!(
                intermediate_numbers,
                (0..(sent.len() as i32 - 1)).collect::<Vec<_>>()
            );
        }

        #[tokio::test]
        async fn failed_page_cancels_other_inflight_pages() {
            // Page 0 fails immediately; every other intermediate page hangs forever. The call can
            // only return if the failed page short-circuits the send and the hung pages are
            // dropped (cancelled) rather than awaited.
            let never = Arc::new(tokio::sync::Notify::new());
            let never_cb = never.clone();
            let service_override = CallbackBasedGrpcService {
                callback: Arc::new(move |request| {
                    let never = never_cb.clone();
                    Box::pin(async move {
                        match request.rpc.as_str() {
                            "GetSystemInfo" => Ok(GrpcSuccessResponse {
                                headers: Default::default(),
                                proto: GetSystemInfoResponse {
                                    capabilities: Some(Capabilities::default()),
                                    ..Default::default()
                                }
                                .encode_to_vec(),
                            }),
                            "RespondWorkflowTaskCompleted" => {
                                let page =
                                    RespondWorkflowTaskCompletedRequest::decode(request.proto)
                                        .expect("completion request is valid");
                                if page.intermediate_page && page.page_number == 0 {
                                    // InvalidArgument is non-retryable, so it is forwarded at once.
                                    Err(tonic::Status::new(tonic::Code::InvalidArgument, "boom"))
                                } else {
                                    never.notified().await;
                                    unreachable!("a cancelled page must not resume");
                                }
                            }
                            rpc => panic!("unexpected RPC: {rpc}"),
                        }
                    })
                }),
            };
            let connection = Connection::connect(
                ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                    .service_override(service_override)
                    .dns_load_balancing(None)
                    .build(),
            )
            .await
            .unwrap();
            let client = WorkerClientBag::new(
                SharedReplaceableClient::new(connection),
                "namespace".to_string(),
                WorkerVersioningStrategy::LegacyBuildIdBased {
                    build_id: "test-build".to_string(),
                },
                Uuid::new_v4(),
            );

            // Enough commands to yield at least two intermediate pages (one fails, one hangs).
            let commands: Vec<_> = (0..8).map(|_| command_with_payload(512 * 1024)).collect();
            let completion = WorkflowTaskCompletion {
                task_token: b"shared-token".to_vec().into(),
                commands,
                messages: vec![],
                sticky_attributes: None,
                query_responses: vec![],
                return_new_workflow_task: false,
                force_create_new_workflow_task: false,
                sdk_metadata: Default::default(),
                metering_metadata: Default::default(),
                versioning_behavior: VersioningBehavior::Unspecified,
                pagination_enabled: true,
                wft_completion_size_limit: None,
            };

            // Without cancellation this would hang on the never-completing page; the timeout guards
            // against that regression instead of relying on a sleep.
            let outcome = tokio::time::timeout(
                Duration::from_secs(10),
                client.complete_workflow_task(completion, CancellationToken::new()),
            )
            .await
            .expect("completion resolved without waiting on the hung page");
            assert!(
                outcome.is_err(),
                "the failed page should surface as an error"
            );
        }

        #[tokio::test]
        async fn completion_over_namespace_limit_fails_proactively_without_sending() {
            let sent = Arc::new(Mutex::new(0usize));
            let sent_cb = sent.clone();
            let service_override = CallbackBasedGrpcService {
                callback: Arc::new(move |request| {
                    let sent = sent_cb.clone();
                    Box::pin(async move {
                        let proto = match request.rpc.as_str() {
                            "GetSystemInfo" => GetSystemInfoResponse {
                                capabilities: Some(Capabilities::default()),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                            "RespondWorkflowTaskCompleted" => {
                                *sent.lock().unwrap() += 1;
                                RespondWorkflowTaskCompletedResponse::default().encode_to_vec()
                            }
                            rpc => panic!("unexpected RPC: {rpc}"),
                        };
                        Ok(GrpcSuccessResponse {
                            headers: Default::default(),
                            proto,
                        })
                    })
                }),
            };
            let connection = Connection::connect(
                ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                    .service_override(service_override)
                    .dns_load_balancing(None)
                    .build(),
            )
            .await
            .unwrap();
            let client = WorkerClientBag::new(
                SharedReplaceableClient::new(connection),
                "namespace".to_string(),
                WorkerVersioningStrategy::LegacyBuildIdBased {
                    build_id: "test-build".to_string(),
                },
                Uuid::new_v4(),
            );

            // ~4 MiB total (so it would be paginated) but the namespace caps the recombined size
            // at 1 MiB, so the server would reject it, and the worker must fail it without sending.
            let commands: Vec<_> = (0..8).map(|_| command_with_payload(512 * 1024)).collect();
            let completion = WorkflowTaskCompletion {
                task_token: b"shared-token".to_vec().into(),
                commands,
                messages: vec![],
                sticky_attributes: None,
                query_responses: vec![],
                return_new_workflow_task: false,
                force_create_new_workflow_task: false,
                sdk_metadata: Default::default(),
                metering_metadata: Default::default(),
                versioning_behavior: VersioningBehavior::Unspecified,
                pagination_enabled: true,
                wft_completion_size_limit: Some(1024 * 1024),
            };
            let err = client
                .complete_workflow_task(completion, CancellationToken::new())
                .await
                .expect_err("completion over the namespace limit must fail");
            assert!(err.metadata().contains_key(REQUEST_TOO_LARGE_KEY));
            assert_eq!(*sent.lock().unwrap(), 0, "no pages should have been sent");
        }

        #[tokio::test]
        async fn buffer_loss_resends_all_pages_until_it_succeeds() {
            // The server reports buffer loss on the final page of the first two attempts, then
            // accepts the third. The whole set must be resent from page 0 each time, and the
            // completion must ultimately succeed. Because buffer loss is marked non-retryable at the
            // client layer, each attempt sends the final page exactly once, so a count of three
            // proves the resend loop, not the client's retry policy, did the retrying.
            let final_attempts = Arc::new(Mutex::new(0usize));
            let final_attempts_cb = final_attempts.clone();
            let service_override = CallbackBasedGrpcService {
                callback: Arc::new(move |request| {
                    let final_attempts = final_attempts_cb.clone();
                    Box::pin(async move {
                        match request.rpc.as_str() {
                            "GetSystemInfo" => Ok(GrpcSuccessResponse {
                                headers: Default::default(),
                                proto: GetSystemInfoResponse {
                                    capabilities: Some(Capabilities::default()),
                                    ..Default::default()
                                }
                                .encode_to_vec(),
                            }),
                            "RespondWorkflowTaskCompleted" => {
                                let page =
                                    RespondWorkflowTaskCompletedRequest::decode(request.proto)
                                        .expect("completion request is valid");
                                if !page.intermediate_page {
                                    let mut attempts = final_attempts.lock().unwrap();
                                    *attempts += 1;
                                    if *attempts <= 2 {
                                        return Err(status_with_detail(
                                            &WorkflowTaskCompletionBufferLostFailure {},
                                        ));
                                    }
                                }
                                Ok(GrpcSuccessResponse {
                                    headers: Default::default(),
                                    proto: RespondWorkflowTaskCompletedResponse::default()
                                        .encode_to_vec(),
                                })
                            }
                            rpc => panic!("unexpected RPC: {rpc}"),
                        }
                    })
                }),
            };
            let connection = Connection::connect(
                ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                    .service_override(service_override)
                    .dns_load_balancing(None)
                    .build(),
            )
            .await
            .unwrap();
            let client = WorkerClientBag::new(
                SharedReplaceableClient::new(connection),
                "namespace".to_string(),
                WorkerVersioningStrategy::LegacyBuildIdBased {
                    build_id: "test-build".to_string(),
                },
                Uuid::new_v4(),
            );

            let commands: Vec<_> = (0..8).map(|_| command_with_payload(512 * 1024)).collect();
            let completion = WorkflowTaskCompletion {
                task_token: b"shared-token".to_vec().into(),
                commands,
                messages: vec![],
                sticky_attributes: None,
                query_responses: vec![],
                return_new_workflow_task: false,
                force_create_new_workflow_task: false,
                sdk_metadata: Default::default(),
                metering_metadata: Default::default(),
                versioning_behavior: VersioningBehavior::Unspecified,
                pagination_enabled: true,
                wft_completion_size_limit: None,
            };
            client
                .complete_workflow_task(completion, CancellationToken::new())
                .await
                .expect("completion eventually succeeds after the buffer is re-established");
            assert_eq!(
                *final_attempts.lock().unwrap(),
                3,
                "the final page is sent once per resend, with no client-layer retry"
            );
        }

        #[tokio::test]
        async fn shutdown_stops_buffer_loss_resends() {
            // The server never re-establishes the buffer, so without shutdown handling the resend
            // loop would run until the task times out on the server, potentially minutes, holding
            // shutdown open. A cancelled shutdown token must end it promptly instead. The token is
            // cancelled before the call, so the first attempt still sends fully (in-flight work is
            // never abandoned) and the loop bails as soon as that attempt reports buffer loss.
            let final_attempts = Arc::new(Mutex::new(0usize));
            let final_attempts_cb = final_attempts.clone();
            let service_override = CallbackBasedGrpcService {
                callback: Arc::new(move |request| {
                    let final_attempts = final_attempts_cb.clone();
                    Box::pin(async move {
                        match request.rpc.as_str() {
                            "GetSystemInfo" => Ok(GrpcSuccessResponse {
                                headers: Default::default(),
                                proto: GetSystemInfoResponse {
                                    capabilities: Some(Capabilities::default()),
                                    ..Default::default()
                                }
                                .encode_to_vec(),
                            }),
                            "RespondWorkflowTaskCompleted" => {
                                let page =
                                    RespondWorkflowTaskCompletedRequest::decode(request.proto)
                                        .expect("completion request is valid");
                                if page.intermediate_page {
                                    Ok(GrpcSuccessResponse {
                                        headers: Default::default(),
                                        proto: RespondWorkflowTaskCompletedResponse::default()
                                            .encode_to_vec(),
                                    })
                                } else {
                                    *final_attempts.lock().unwrap() += 1;
                                    Err(status_with_detail(
                                        &WorkflowTaskCompletionBufferLostFailure {},
                                    ))
                                }
                            }
                            rpc => panic!("unexpected RPC: {rpc}"),
                        }
                    })
                }),
            };
            let connection = Connection::connect(
                ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                    .service_override(service_override)
                    .dns_load_balancing(None)
                    .build(),
            )
            .await
            .unwrap();
            let client = WorkerClientBag::new(
                SharedReplaceableClient::new(connection),
                "namespace".to_string(),
                WorkerVersioningStrategy::LegacyBuildIdBased {
                    build_id: "test-build".to_string(),
                },
                Uuid::new_v4(),
            );

            let shutdown_token = CancellationToken::new();
            shutdown_token.cancel();
            let commands: Vec<_> = (0..8).map(|_| command_with_payload(512 * 1024)).collect();
            let completion = WorkflowTaskCompletion {
                task_token: b"shared-token".to_vec().into(),
                commands,
                messages: vec![],
                sticky_attributes: None,
                query_responses: vec![],
                return_new_workflow_task: false,
                force_create_new_workflow_task: false,
                sdk_metadata: Default::default(),
                metering_metadata: Default::default(),
                versioning_behavior: VersioningBehavior::Unspecified,
                pagination_enabled: true,
                wft_completion_size_limit: None,
            };

            let err = tokio::time::timeout(
                Duration::from_secs(10),
                client.complete_workflow_task(completion, shutdown_token),
            )
            .await
            .expect("shutdown ends the resend loop instead of waiting for the server timeout")
            .expect_err("the last buffer-loss error is surfaced");
            assert!(is_workflow_task_completion_buffer_lost(&err));
            assert_eq!(
                *final_attempts.lock().unwrap(),
                1,
                "the first attempt still sends; shutdown prevents any resend"
            );
        }

        #[tokio::test]
        async fn cancelled_shutdown_does_not_interrupt_successful_completion() {
            // The shutdown token is only consulted while resending after buffer loss. A completion
            // that never hits buffer loss must still succeed even when the worker is shutting down,
            // so graceful drain can finish outstanding completions rather than abandon them.
            let final_pages = Arc::new(Mutex::new(0usize));
            let final_pages_cb = final_pages.clone();
            let service_override = CallbackBasedGrpcService {
                callback: Arc::new(move |request| {
                    let final_pages = final_pages_cb.clone();
                    Box::pin(async move {
                        let proto = match request.rpc.as_str() {
                            "GetSystemInfo" => GetSystemInfoResponse {
                                capabilities: Some(Capabilities::default()),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                            "RespondWorkflowTaskCompleted" => {
                                let page =
                                    RespondWorkflowTaskCompletedRequest::decode(request.proto)
                                        .expect("completion request is valid");
                                if !page.intermediate_page {
                                    *final_pages.lock().unwrap() += 1;
                                }
                                RespondWorkflowTaskCompletedResponse::default().encode_to_vec()
                            }
                            rpc => panic!("unexpected RPC: {rpc}"),
                        };
                        Ok(GrpcSuccessResponse {
                            headers: Default::default(),
                            proto,
                        })
                    })
                }),
            };
            let connection = Connection::connect(
                ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                    .service_override(service_override)
                    .dns_load_balancing(None)
                    .build(),
            )
            .await
            .unwrap();
            let client = WorkerClientBag::new(
                SharedReplaceableClient::new(connection),
                "namespace".to_string(),
                WorkerVersioningStrategy::LegacyBuildIdBased {
                    build_id: "test-build".to_string(),
                },
                Uuid::new_v4(),
            );

            let shutdown_token = CancellationToken::new();
            shutdown_token.cancel();
            let commands: Vec<_> = (0..8).map(|_| command_with_payload(512 * 1024)).collect();
            let completion = WorkflowTaskCompletion {
                task_token: b"shared-token".to_vec().into(),
                commands,
                messages: vec![],
                sticky_attributes: None,
                query_responses: vec![],
                return_new_workflow_task: false,
                force_create_new_workflow_task: false,
                sdk_metadata: Default::default(),
                metering_metadata: Default::default(),
                versioning_behavior: VersioningBehavior::Unspecified,
                pagination_enabled: true,
                wft_completion_size_limit: None,
            };

            client
                .complete_workflow_task(completion, shutdown_token)
                .await
                .expect("a completion without buffer loss succeeds despite shutdown");
            // The paginated path ran to its final page rather than being cut short.
            assert_eq!(*final_pages.lock().unwrap(), 1);
        }
    }
}
