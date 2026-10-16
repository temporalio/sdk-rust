//! Experimental Nexus operation handlers and workflow Nexus APIs.
//!
//! This module is experimental and may change without notice.
//!
//! ```no_run
//! use nexus_rpc::service;
//! use temporalio_sdk::{
//!     WorkerOptions,
//!     nexus::{
//!         TemporalNexusClient, TemporalOperationError, TemporalOperationResult,
//!         temporal_nexus_service_handler, temporal_operation,
//!     },
//! };
//!
//! #[service]
//! trait PaymentService {
//!     fn quote(input: String) -> String;
//! }
//! struct PaymentHandlers;
//!
//! #[temporal_nexus_service_handler(service = PaymentService)]
//! impl PaymentHandlers {
//!     #[temporal_operation]
//!     async fn quote(
//!         &self,
//!         client: TemporalNexusClient,
//!         input: String,
//!     ) -> Result<TemporalOperationResult<String>, TemporalOperationError> {
//!         let _request_id = context.request_id();
//!         Ok(TemporalOperationResult::Sync(input))
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let options = WorkerOptions::new("payments")
//!     .register_nexus_service(PaymentHandlers)?
//!     .build();
//! # Ok(())
//! # }
//! ```

pub use temporalio_macros::{temporal_nexus_service_handler, temporal_operation};
pub use temporalio_workflow::nexus::*;

use crate::{
    ApplicationFailure, CommonWorker, OutgoingActivityError, OutgoingError, WorkerRunError,
};
use futures_util::{FutureExt, future::BoxFuture};
pub use nexus_rpc::{NexusOperation, NexusService};
use prost::Message;
use std::{
    collections::HashMap, future::Future, marker::PhantomData, panic::AssertUnwindSafe, sync::Arc,
    time::SystemTime,
};
use temporalio_client::{
    Client, NamespacedClient, Url, WorkflowCancelOptions, WorkflowExecutionInfo,
    WorkflowStartOptions,
    errors::{WorkflowInteractionError, WorkflowStartError},
    tonic::Code,
};
use temporalio_common::{
    HasWorkflowDefinition,
    data_converters::{
        DataConverter, NexusSerializationContext, SerializationContextData, TemporalDeserializable,
        TemporalSerializable,
    },
    payload_visitor::encode_payloads,
    protos::{
        coresdk::nexus::{NexusTaskCompletion, nexus_task, nexus_task_completion},
        temporal::api::{
            common::v1::{Callback, callback},
            enums::v1::NexusHandlerErrorRetryBehavior,
            failure::v1::{Failure, NexusHandlerFailureInfo, failure::FailureInfo},
            nexus::v1::{self as proto, request, response, start_operation_response},
        },
    },
};
use temporalio_sdk_core::PollError;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Result of starting a Nexus operation.
#[derive(Debug)]
pub enum TemporalOperationResult<T> {
    /// The operation completed in the handler.
    Sync(T),
    /// A workflow will complete the operation through its completion callback.
    Async(TemporalOperationToken),
}

/// Opaque identity of a workflow-backed operation.
#[derive(Debug)]
pub struct TemporalOperationToken {
    token: String,
    link: proto::Link,
}

#[derive(Clone, PartialEq, Message)]
struct WorkflowOperationToken {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    workflow_id: String,
}

impl WorkflowOperationToken {
    fn encode_token(&self) -> String {
        let mut token = String::from("rust-nexus-v1:");
        for byte in self.encode_to_vec() {
            token.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            token.push(char::from(b"0123456789abcdef"[(byte & 15) as usize]));
        }
        token
    }

    fn decode_token(token: &str) -> Result<Self, TemporalOperationError> {
        let invalid =
            || TemporalOperationError::handler("BAD_REQUEST", "Invalid operation token", false);
        let encoded = token.strip_prefix("rust-nexus-v1:").ok_or_else(invalid)?;
        if encoded.len() > 32_768 || !encoded.len().is_multiple_of(2) {
            return Err(invalid());
        }
        let bytes = encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let hi = char::from(pair[0]).to_digit(16).ok_or_else(invalid)?;
                let lo = char::from(pair[1]).to_digit(16).ok_or_else(invalid)?;
                Ok(((hi << 4) | lo) as u8)
            })
            .collect::<Result<Vec<_>, TemporalOperationError>>()?;
        let decoded = Self::decode(bytes.as_slice()).map_err(|_| invalid())?;
        if decoded.namespace.is_empty() || decoded.workflow_id.is_empty() {
            return Err(invalid());
        }
        Ok(decoded)
    }
}

/// Request-scoped client for starting workflow-backed Nexus operations.
#[derive(Clone, Debug)]
pub struct TemporalNexusClient {
    context: TemporalOperationStartContext,
    request: proto::StartOperationRequest,
}

impl TemporalNexusClient {
    /// Context of the request currently being served.
    pub fn context(&self) -> &TemporalOperationStartContext {
        &self.context
    }

    /// Configured Temporal client, including the worker's converters and codecs.
    pub fn client(&self) -> &Client {
        self.context.client()
    }

    /// Start a workflow that completes this Nexus operation when it completes.
    ///
    /// The supplied task queue is passed through unchanged. Use the request ID for a stable
    /// workflow ID so retried Nexus start requests do not start multiple workflows. The Nexus
    /// request ID is also used as the workflow start RPC's idempotency key; other options,
    /// including workflow ID reuse and conflict policies, are preserved.
    ///
    /// The workflow ID identifies the operation, including cancellation. Do not reuse it for
    /// independent operations, since a delayed cancellation could target a later execution.
    pub async fn start_workflow<W>(
        &self,
        workflow: W,
        input: W::Input,
        mut options: WorkflowStartOptions,
    ) -> Result<TemporalOperationResult<W::Output>, TemporalOperationError>
    where
        W: HasWorkflowDefinition,
        W::Input: Send,
    {
        let token = WorkflowOperationToken {
            namespace: self.context.client.namespace(),
            workflow_id: options.workflow_id.clone(),
        }
        .encode_token();
        let links = self
            .request
            .links
            .iter()
            .filter(|link| link.r#type == "temporal.api.common.v1.Link.WorkflowEvent")
            .map(proto::workflow_event_link_from_nexus)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| TemporalOperationError::handler("BAD_REQUEST", error, false))?;
        options.links.extend(links.clone());
        if !self.request.callback.is_empty() {
            let mut header = self.request.callback_header.clone();
            // Completion can reach the caller before AsyncSuccess supplies the operation identity.
            header.insert("nexus-operation-token".to_owned(), token.clone());
            header.insert("nexus-operation-id".to_owned(), token.clone());
            options.completion_callbacks.push(Callback {
                variant: Some(callback::Variant::Nexus(callback::Nexus {
                    url: self.request.callback.clone(),
                    header,
                })),
                links,
            });
        }
        options.request_id = Some(self.context.request_id.clone());
        let handle = self
            .context
            .client
            .start_workflow(workflow, input, options)
            .await
            .map_err(|error| {
                if matches!(&error, WorkflowStartError::AlreadyStarted { .. }) {
                    return TemporalOperationError::handler("CONFLICT", error, false);
                }
                let invalid = matches!(
                    &error,
                    WorkflowStartError::PayloadConversion(_)
                ) || matches!(&error, WorkflowStartError::Rpc(status) if status.code() == Code::InvalidArgument);
                TemporalOperationError::handler(
                    if invalid { "BAD_REQUEST" } else { "INTERNAL" },
                    error,
                    !invalid,
                )
            })?;
        let info = handle.info();
        let run_id = info.run_id.as_deref().ok_or_else(|| {
            TemporalOperationError::handler("INTERNAL", "Workflow start returned no run ID", true)
        })?;
        let mut url = Url::parse("temporal:///").expect("valid Temporal link base");
        url.path_segments_mut()
            .expect("Temporal link supports path segments")
            .extend([
                "namespaces",
                &info.namespace,
                "workflows",
                &info.workflow_id,
                run_id,
                "history",
            ]);
        url.query_pairs_mut()
            .append_pair("eventID", "1")
            .append_pair("eventType", "WorkflowExecutionStarted")
            .append_pair("referenceType", "EventReference");
        Ok(TemporalOperationResult::Async(TemporalOperationToken {
            token,
            link: proto::Link {
                url: url.into(),
                r#type: "temporal.api.common.v1.Link.WorkflowEvent".to_owned(),
            },
        }))
    }
}

/// Invalid Nexus service or operation registration.
#[derive(Debug, thiserror::Error)]
pub enum NexusRegistrationError {
    /// A service has no registered operations.
    #[error("Nexus service {0:?} has no registered operations")]
    EmptyService(String),
    /// A service with this wire name is already registered.
    #[error("Nexus service {0:?} is already registered")]
    DuplicateService(String),
    /// An operation with this wire name is already registered in the service.
    #[error("Nexus operation {service:?}/{operation:?} is already registered")]
    DuplicateOperation {
        /// Service wire name.
        service: String,
        /// Operation wire name.
        operation: String,
    },
}

/// A handler implementation that can be registered on a worker.
pub trait TemporalNexusServiceHandler: Send + Sync + 'static {
    /// Typed Nexus service served by this implementation.
    type Service: NexusService;
    /// Convert the implementation into its typed operation registry.
    fn into_service_handler(
        self: Arc<Self>,
    ) -> Result<NexusServiceHandler<Self::Service>, NexusRegistrationError>;
}

/// Typed registry associating Nexus operation markers with async handlers.
///
/// Operations from other services cannot be registered:
///
/// ```compile_fail
/// use nexus_rpc::service;
/// use temporalio_sdk::nexus::{NexusServiceHandler, TemporalOperationResult};
/// #[service]
/// trait First { fn call(input: String) -> String; }
/// #[service]
/// trait Second { fn call(input: String) -> String; }
/// # fn main() {
/// let handler = NexusServiceHandler::new(First)
///     .operation(Second::call, |_, _, input: String| async move {
///         Ok(TemporalOperationResult::Sync(input))
///     });
/// # }
/// ```
///
/// Handler return types must match the service declaration:
///
/// ```compile_fail
/// use nexus_rpc::service;
/// use temporalio_sdk::nexus::{
///     temporal_nexus_service_handler, temporal_operation, TemporalNexusClient,
///     TemporalOperationError, TemporalOperationResult,
/// };
/// #[service]
/// trait PaymentService { fn quote(input: String) -> String; }
/// struct PaymentHandlers;
/// #[temporal_nexus_service_handler(service = PaymentService)]
/// impl PaymentHandlers {
///     #[temporal_operation]
///     async fn quote(&self, client: TemporalNexusClient, input: String)
///         -> Result<TemporalOperationResult<u32>, TemporalOperationError>
///     {
///         Ok(TemporalOperationResult::Sync(42))
///     }
/// }
/// # fn main() {}
/// ```
pub struct NexusServiceHandler<S: NexusService> {
    operations: HashMap<String, OperationHandler>,
    service: PhantomData<fn() -> S>,
}

impl<S: NexusService> NexusServiceHandler<S> {
    /// Start a registry for the supplied service marker.
    pub fn new(_: S) -> Self {
        Self {
            operations: HashMap::new(),
            service: PhantomData,
        }
    }

    /// Register a closure with the input and output types declared by this operation.
    pub fn operation<O, F, Fut>(
        mut self,
        operation: O,
        handler: F,
    ) -> Result<Self, NexusRegistrationError>
    where
        O: NexusOperation<Service = S>,
        O::Input: TemporalDeserializable,
        O::Output: TemporalSerializable + Sync,
        F: Fn(TemporalNexusClient, TemporalOperationStartContext, O::Input) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Result<TemporalOperationResult<O::Output>, TemporalOperationError>>
            + Send
            + 'static,
    {
        let name = operation.name().to_owned();
        if self.operations.contains_key(&name) {
            return Err(NexusRegistrationError::DuplicateOperation {
                service: S::name().to_owned(),
                operation: name,
            });
        }
        let handler = Arc::new(handler);
        self.operations.insert(
            name,
            Arc::new(move |context, request, converter| {
                let handler = handler.clone();
                async move {
                    let payload = request.payload.clone().unwrap_or_default();
                    let input = converter
                        .from_payload::<O::Input>(&serialization_context(), payload)
                        .await
                        .map_err(|error| {
                            TemporalOperationError::handler("BAD_REQUEST", error, false)
                        })?;
                    let client = TemporalNexusClient {
                        context: context.clone(),
                        request,
                    };
                    match handler(client, context, input).await? {
                        TemporalOperationResult::Sync(output) => {
                            let payload = converter
                                .to_payload(&serialization_context(), &output)
                                .await
                                .map_err(|error| {
                                    TemporalOperationError::handler("INTERNAL", error, true)
                                })?;
                            Ok(start_operation_response::Variant::SyncSuccess(
                                start_operation_response::Sync {
                                    payload: Some(payload),
                                    links: Vec::new(),
                                },
                            ))
                        }
                        TemporalOperationResult::Async(token) => {
                            Ok(start_operation_response::Variant::AsyncSuccess(
                                #[allow(deprecated)]
                                start_operation_response::Async {
                                    operation_id: token.token.clone(),
                                    operation_token: token.token,
                                    links: vec![token.link],
                                },
                            ))
                        }
                    }
                }
                .boxed()
            }),
        );
        Ok(self)
    }
}

impl<S: NexusService> TemporalNexusServiceHandler for NexusServiceHandler<S> {
    type Service = S;

    fn into_service_handler(
        self: Arc<Self>,
    ) -> Result<NexusServiceHandler<S>, NexusRegistrationError> {
        Ok(Self {
            operations: self.operations.clone(),
            service: PhantomData,
        })
    }
}

/// Information about the request currently being served.
#[derive(Clone, Debug)]
pub struct TemporalOperationStartContext {
    client: Client,
    headers: HashMap<String, String>,
    endpoint: String,
    service: String,
    operation: String,
    request_id: String,
    deadline: Option<SystemTime>,
    cancellation: CancellationToken,
}

impl TemporalOperationStartContext {
    /// Configured Temporal client for this worker.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Headers propagated by the Nexus caller.
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Mutably access propagated headers, for example from an interceptor.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.headers
    }

    /// Endpoint addressed by the caller.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Nexus service wire name.
    pub fn service(&self) -> &str {
        &self.service
    }

    /// Nexus operation wire name.
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Stable request ID supplied by the caller, suitable for idempotent starts.
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Deadline of this request, not the lifetime of an asynchronous operation.
    pub fn deadline(&self) -> Option<SystemTime> {
        self.deadline
    }

    /// Cancellation of this request due to its deadline or worker shutdown.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// An operation failure or a failure to handle a Nexus request.
#[derive(Debug, thiserror::Error)]
pub enum TemporalOperationError {
    /// The operation ran and failed. This is not retried as a handler failure.
    #[error("{0}")]
    Operation(String),
    /// An application failure, including typed details handled by the configured converter.
    #[error(transparent)]
    Application(#[from] Box<ApplicationFailure>),
    /// The operation was canceled.
    #[error("{0}")]
    Canceled(String),
    /// A Nexus handler failure, whose type and retry behavior follow the Nexus protocol.
    #[error("{message}")]
    Handler {
        /// Nexus handler error type, for example `BAD_REQUEST` or `INTERNAL`.
        error_type: String,
        /// Human-readable error message.
        message: String,
        /// Whether the request can be retried.
        retryable: bool,
    },
}

impl TemporalOperationError {
    pub(crate) fn handler(error_type: &str, message: impl ToString, retryable: bool) -> Self {
        Self::Handler {
            error_type: error_type.to_owned(),
            message: message.to_string(),
            retryable,
        }
    }

    fn into_status(self, converter: &DataConverter) -> nexus_task_completion::Status {
        match self {
            Self::Operation(message) => start_status(start_operation_response::Variant::Failure(
                converter.to_failure(
                    &serialization_context(),
                    OutgoingError::Activity(OutgoingActivityError::Application(
                        ApplicationFailure::builder(message)
                            .non_retryable(true)
                            .build()
                            .into(),
                    )),
                ),
            )),
            Self::Application(failure) => start_status(start_operation_response::Variant::Failure(
                converter.to_failure(
                    &serialization_context(),
                    OutgoingError::Activity(OutgoingActivityError::Application(failure)),
                ),
            )),
            Self::Canceled(message) => {
                let mut failure = converter.to_failure(
                    &serialization_context(),
                    OutgoingError::Activity(OutgoingActivityError::Cancelled { details: None }),
                );
                failure.message = message;
                start_status(start_operation_response::Variant::Failure(failure))
            }
            Self::Handler {
                error_type,
                message,
                retryable,
            } => nexus_task_completion::Status::Failure(Failure {
                message,
                failure_info: Some(FailureInfo::NexusHandlerFailureInfo(
                    NexusHandlerFailureInfo {
                        r#type: error_type,
                        retry_behavior: if retryable {
                            NexusHandlerErrorRetryBehavior::Retryable
                        } else {
                            NexusHandlerErrorRetryBehavior::NonRetryable
                        }
                        .into(),
                    },
                )),
                ..Default::default()
            }),
        }
    }
}

fn start_status(variant: start_operation_response::Variant) -> nexus_task_completion::Status {
    nexus_task_completion::Status::Completed(proto::Response {
        variant: Some(response::Variant::StartOperation(
            proto::StartOperationResponse {
                variant: Some(variant),
            },
        )),
    })
}

type OperationHandler = Arc<
    dyn Fn(
            TemporalOperationStartContext,
            proto::StartOperationRequest,
            DataConverter,
        )
            -> BoxFuture<'static, Result<start_operation_response::Variant, TemporalOperationError>>
        + Send
        + Sync,
>;

#[derive(Clone, Default)]
pub(crate) struct NexusServices {
    operations: HashMap<String, HashMap<String, OperationHandler>>,
    interceptors: Vec<Arc<dyn NexusHandlerInterceptor>>,
}

impl std::fmt::Debug for NexusServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.operations.iter().map(|(s, ops)| (s, ops.keys())))
            .finish()
    }
}

impl NexusServices {
    pub(crate) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub(crate) fn interceptor(&mut self, interceptor: Arc<dyn NexusHandlerInterceptor>) {
        self.interceptors.push(interceptor);
    }

    pub(crate) fn register<H: TemporalNexusServiceHandler>(
        &mut self,
        handler: H,
    ) -> Result<(), NexusRegistrationError> {
        let name = H::Service::name();
        if self.operations.contains_key(name) {
            return Err(NexusRegistrationError::DuplicateService(name.to_owned()));
        }
        let handler = Arc::new(handler).into_service_handler()?;
        if handler.operations.is_empty() {
            return Err(NexusRegistrationError::EmptyService(name.to_owned()));
        }
        self.operations.insert(name.to_owned(), handler.operations);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(crate) struct NexusHalf {
    pub(crate) services: NexusServices,
}

impl NexusHalf {
    pub(crate) async fn run(&mut self, common: &CommonWorker) -> Result<(), WorkerRunError> {
        if self.services.is_empty() {
            return Ok(());
        }
        let mut tasks = JoinSet::new();
        let mut cancellations = HashMap::<Vec<u8>, CancellationToken>::new();
        let mut polling = true;
        while polling || !tasks.is_empty() {
            tokio::select! {
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    let (token, result) = completed
                        .expect("join set is nonempty")
                        .map_err(|source| WorkerRunError::Fatal {
                            message: "Nexus task failed".to_owned(),
                            source: Box::new(source),
                        })?;
                    cancellations.remove(&token);
                    result?;
                }
                task = common.worker.poll_nexus_task(), if polling => {
                    let task = match task {
                        Ok(task) => task,
                        Err(PollError::ShutDown) => {
                            polling = false;
                            for cancellation in cancellations.values() {
                                cancellation.cancel();
                            }
                            continue;
                        }
                        Err(source) => return Err(WorkerRunError::Fatal {
                            message: "Nexus polling failed".to_owned(),
                            source: Box::new(source),
                        }),
                    };
                    match task.variant {
                        Some(nexus_task::Variant::CancelTask(cancel)) => {
                            if let Some(token) = cancellations.get(&cancel.task_token) {
                                token.cancel();
                            }
                        }
                        Some(nexus_task::Variant::Task(task_request)) => {
                            let token = task_request.task_token;
                            let cancellation = CancellationToken::new();
                            cancellations.insert(token.clone(), cancellation.clone());
                            let services = self.services.clone();
                            let worker = common.worker.clone();
                            let converter = common.data_converter.clone();
                            let client = worker.get_client_connection()
                                .map(|connection| Client::new(connection, common.client_options.clone()));
                            let deadline = task.request_deadline.and_then(|d| SystemTime::try_from(d).ok());
                            tasks.spawn(async move {
                                let execute = async {
                                    let client = client.ok_or_else(|| TemporalOperationError::handler(
                                        "INTERNAL", "Worker has no client connection", true,
                                    ))?.map_err(|e| TemporalOperationError::handler("INTERNAL", e, true))?;
                                    dispatch(services, client, converter.clone(), task_request.request,
                                        task.endpoint, deadline, cancellation.clone()).await
                                };
                                let status = execute_request(execute, &cancellation, &converter).await;
                                let result = worker.complete_nexus_task(NexusTaskCompletion {
                                    task_token: token.clone(),
                                    status: Some(status),
                                }).await.map_err(|source| WorkerRunError::Fatal {
                                    message: "Nexus completion failed".to_owned(),
                                    source: Box::new(source),
                                });
                                (token, result)
                            });
                        }
                        None => {}
                    }
                }
            }
        }
        Ok(())
    }
}

async fn dispatch(
    services: NexusServices,
    client: Client,
    converter: DataConverter,
    request: Option<proto::Request>,
    endpoint: String,
    deadline: Option<SystemTime>,
    cancellation: CancellationToken,
) -> Result<nexus_task_completion::Status, TemporalOperationError> {
    let request = request.ok_or_else(|| {
        TemporalOperationError::handler("BAD_REQUEST", "Missing Nexus request", false)
    })?;
    match request.variant {
        Some(request::Variant::StartOperation(start)) => {
            let service = services.operations.get(&start.service).ok_or_else(|| {
                TemporalOperationError::handler("NOT_FOUND", "Unknown Nexus service", false)
            })?;
            let handler = service.get(&start.operation).ok_or_else(|| {
                TemporalOperationError::handler("NOT_FOUND", "Unknown Nexus operation", false)
            })?;
            let mut context = TemporalOperationStartContext {
                client: client.clone(),
                headers: request.header,
                endpoint,
                service: start.service.clone(),
                operation: start.operation.clone(),
                request_id: start.request_id.clone(),
                deadline,
                cancellation,
            };
            for interceptor in &services.interceptors {
                interceptor.on_start(&mut context).await?;
            }
            let result = handler(context.clone(), start, converter).await;
            for interceptor in services.interceptors.iter().rev() {
                interceptor
                    .on_finish(&context, result.as_ref().map(|_| ()))
                    .await;
            }
            result.map(start_status)
        }
        Some(request::Variant::CancelOperation(cancel)) => {
            let service = services.operations.get(&cancel.service).ok_or_else(|| {
                TemporalOperationError::handler("NOT_FOUND", "Unknown Nexus service", false)
            })?;
            if !service.contains_key(&cancel.operation) {
                return Err(TemporalOperationError::handler(
                    "NOT_FOUND",
                    "Unknown Nexus operation",
                    false,
                ));
            }
            let token = WorkflowOperationToken::decode_token(&cancel.operation_token)?;
            if token.namespace != client.namespace() {
                return Err(TemporalOperationError::handler(
                    "BAD_REQUEST",
                    "Operation token targets a different namespace",
                    false,
                ));
            }
            WorkflowExecutionInfo::builder()
                .namespace(token.namespace)
                .workflow_id(token.workflow_id)
                .build()
                .bind_untyped(client)
                .cancel(WorkflowCancelOptions::default())
                .await
                .or_else(|error| match error {
                    // Retention may remove a completed workflow before a delayed cancel arrives.
                    WorkflowInteractionError::NotFound(_) => Ok(()),
                    error => Err(error),
                })
                .map_err(|error| TemporalOperationError::handler("INTERNAL", error, true))?;
            Ok(nexus_task_completion::Status::Completed(proto::Response {
                variant: Some(response::Variant::CancelOperation(
                    proto::CancelOperationResponse {},
                )),
            }))
        }
        None => Err(TemporalOperationError::handler(
            "BAD_REQUEST",
            "Missing request variant",
            false,
        )),
    }
}

fn serialization_context() -> SerializationContextData {
    SerializationContextData::Nexus(NexusSerializationContext::new())
}

async fn execute_request(
    execute: impl Future<Output = Result<nexus_task_completion::Status, TemporalOperationError>>,
    cancellation: &CancellationToken,
    converter: &DataConverter,
) -> nexus_task_completion::Status {
    let execute = async {
        match execute.await {
            Ok(status) => status,
            Err(error) => {
                let mut completion = NexusTaskCompletion {
                    task_token: Vec::new(),
                    status: Some(error.into_status(converter)),
                };
                if let Err(error) =
                    encode_payloads(&mut completion, converter.codec(), &serialization_context())
                        .await
                {
                    return TemporalOperationError::handler("INTERNAL", error, true)
                        .into_status(converter);
                }
                completion.status.expect("error completion has a status")
            }
        }
    };
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => nexus_task_completion::Status::AckCancel(true),
        result = AssertUnwindSafe(execute).catch_unwind() => result.unwrap_or_else(|_| {
            TemporalOperationError::handler("INTERNAL", "Nexus handler panicked", true).into_status(converter)
        }),
    }
}

/// Observe handler requests and outcomes, including propagated headers.
///
/// Request cancellation drops the handler future and does not invoke `on_finish`. Interceptors
/// should use drop guards for cleanup that must also happen when a request is canceled.
#[async_trait::async_trait]
pub trait NexusHandlerInterceptor: Send + Sync {
    /// Inspect or update the request context before running the handler.
    async fn on_start(
        &self,
        _context: &mut TemporalOperationStartContext,
    ) -> Result<(), TemporalOperationError> {
        Ok(())
    }

    /// Observe a completed request. Hooks run in reverse registration order.
    async fn on_finish(
        &self,
        _context: &TemporalOperationStartContext,
        _result: Result<(), &TemporalOperationError>,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkerOptions;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use temporalio_client::{
        ClientOptions, Connection, ConnectionOptions,
        callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse},
    };
    use temporalio_common::{
        WorkflowDefinition,
        data_converters::{PayloadCodec, PayloadConversionError, PayloadConverter},
        protos::temporal::api::{
            common::v1::Payload,
            workflowservice::v1::{
                RequestCancelWorkflowExecutionRequest, RequestCancelWorkflowExecutionResponse,
                StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
            },
        },
    };
    use tokio::sync::Notify;

    #[nexus_rpc::service]
    trait Payments {
        fn quote(input: String) -> String;
    }

    fn service() -> NexusServiceHandler<Payments> {
        NexusServiceHandler::new(Payments)
            .operation(Payments::quote, |_, _, input| async move {
                Ok(TemporalOperationResult::Sync(input))
            })
            .unwrap()
    }

    async fn client() -> Client {
        let mut options =
            ConnectionOptions::new(temporalio_client::Url::parse("http://localhost:7233").unwrap())
                .dns_load_balancing(None)
                .service_override(CallbackBasedGrpcService {
                    callback: Arc::new(|_| Box::pin(async { panic!("unexpected RPC") })),
                })
                .build();
        options.set_skip_get_system_info(true);
        Client::new(
            Connection::connect(options).await.unwrap(),
            ClientOptions::new("test").build(),
        )
        .unwrap()
    }

    #[test]
    fn registry_rejects_duplicates_and_enables_only_nexus() {
        let mut services = NexusServices::default();
        assert!(matches!(
            services.register(NexusServiceHandler::new(Payments)),
            Err(NexusRegistrationError::EmptyService(_)),
        ));
        assert!(services.is_empty());
        assert!(matches!(
            service().operation(Payments::quote, |_, _, input| async move {
                Ok(TemporalOperationResult::Sync(input))
            }),
            Err(NexusRegistrationError::DuplicateOperation { .. }),
        ));
        let mut options = WorkerOptions::new("payments")
            .register_nexus_service(service())
            .unwrap()
            .build();
        assert!(matches!(
            options.register_nexus_service(service()),
            Err(NexusRegistrationError::DuplicateService(_)),
        ));
        let config = options
            .to_core_options("test".to_owned(), "test".to_owned())
            .unwrap();
        assert!(config.task_types.enable_nexus);
        assert!(!config.task_types.enable_workflows);
        assert!(!config.task_types.enable_remote_activities);
    }

    struct BackingWorkflow;

    impl WorkflowDefinition for BackingWorkflow {
        type Input = String;
        type Output = String;

        fn name(&self) -> &str {
            "backing"
        }
    }

    impl HasWorkflowDefinition for BackingWorkflow {
        type Run = Self;
    }

    #[tokio::test]
    async fn workflow_backed_start_links_and_cancellation() {
        for with_callback in [true, false] {
            let inbound = proto::Link {
                url: "temporal:///namespaces/test/workflows/caller/caller-run/history?eventID=7&eventType=NexusOperationScheduled".to_owned(),
                r#type: "temporal.api.common.v1.Link.WorkflowEvent".to_owned(),
            };
            let expected_link = proto::workflow_event_link_from_nexus(&inbound).unwrap();
            let cancelled = Arc::new(AtomicBool::new(false));
            let observed_cancel = cancelled.clone();
            let mut options = ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
                .dns_load_balancing(None)
                .service_override(CallbackBasedGrpcService {
                    callback: Arc::new(move |request| {
                        let proto = match request.rpc.as_str() {
                            "StartWorkflowExecution" => {
                                let request =
                                    StartWorkflowExecutionRequest::decode(request.proto).unwrap();
                                assert_eq!(request.request_id, "stable-request");
                                assert_eq!(request.task_queue.unwrap().name, "backing-queue");
                                assert_eq!(request.links, vec![expected_link.clone()]);
                                assert_eq!(
                                    request.completion_callbacks.len(),
                                    usize::from(with_callback)
                                );
                                if with_callback {
                                    let callback = &request.completion_callbacks[0];
                                    assert_eq!(callback.links, request.links);
                                    let Some(callback::Variant::Nexus(callback)) =
                                        &callback.variant
                                    else {
                                        panic!("expected Nexus callback");
                                    };
                                    assert_eq!(callback.url, "http://callback");
                                    assert_eq!(callback.header["trace"], "span");
                                    let token = WorkflowOperationToken {
                                        namespace: "test".to_owned(),
                                        workflow_id: "backing/id".to_owned(),
                                    }
                                    .encode_token();
                                    assert_eq!(callback.header["nexus-operation-token"], token);
                                    assert_eq!(callback.header["nexus-operation-id"], token);
                                }
                                StartWorkflowExecutionResponse {
                                    run_id: "current-run".to_owned(),
                                    first_execution_run_id: "first-run".to_owned(),
                                    ..Default::default()
                                }
                                .encode_to_vec()
                            }
                            "RequestCancelWorkflowExecution" => {
                                let request =
                                    RequestCancelWorkflowExecutionRequest::decode(request.proto)
                                        .unwrap();
                                assert!(request.first_execution_run_id.is_empty());
                                assert_eq!(
                                    request.workflow_execution.unwrap().workflow_id,
                                    "backing/id"
                                );
                                observed_cancel.store(true, Ordering::SeqCst);
                                RequestCancelWorkflowExecutionResponse::default().encode_to_vec()
                            }
                            rpc => panic!("unexpected RPC {rpc}"),
                        };
                        Box::pin(async move {
                            Ok(GrpcSuccessResponse {
                                headers: Default::default(),
                                proto,
                            })
                        })
                    }),
                })
                .build();
            options.set_skip_get_system_info(true);
            let client = Client::new(
                Connection::connect(options).await.unwrap(),
                ClientOptions::new("test").build(),
            )
            .unwrap();
            let mut services = NexusServices::default();
            services
                .register(
                    NexusServiceHandler::new(Payments)
                        .operation(Payments::quote, |client, _, input| async move {
                            client
                                .start_workflow(
                                    BackingWorkflow,
                                    input,
                                    WorkflowStartOptions::new("backing-queue", "backing/id")
                                        .build(),
                                )
                                .await
                        })
                        .unwrap(),
                )
                .unwrap();
            let converter = DataConverter::default();
            let payload = converter
                .to_payload(&serialization_context(), &"input".to_owned())
                .await
                .unwrap();
            let result = dispatch(
                services.clone(),
                client.clone(),
                converter.clone(),
                Some(proto::Request {
                    variant: Some(request::Variant::StartOperation(
                        proto::StartOperationRequest {
                            service: Payments::name().to_owned(),
                            operation: "quote".to_owned(),
                            request_id: "stable-request".to_owned(),
                            callback: if with_callback {
                                "http://callback".to_owned()
                            } else {
                                String::new()
                            },
                            callback_header: HashMap::from([(
                                "trace".to_owned(),
                                "span".to_owned(),
                            )]),
                            links: vec![inbound],
                            payload: Some(payload),
                        },
                    )),
                    ..Default::default()
                }),
                "payments".to_owned(),
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
            let nexus_task_completion::Status::Completed(proto::Response {
                variant:
                    Some(response::Variant::StartOperation(proto::StartOperationResponse {
                        variant: Some(start_operation_response::Variant::AsyncSuccess(result)),
                    })),
            }) = result
            else {
                panic!("expected async success");
            };
            assert_eq!(result.links.len(), 1);
            assert_eq!(
                result.links[0].url,
                "temporal:///namespaces/test/workflows/backing%2Fid/current-run/history?eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference"
            );
            assert_eq!(
                result.operation_token,
                WorkflowOperationToken {
                    namespace: "test".to_owned(),
                    workflow_id: "backing/id".to_owned(),
                }
                .encode_token()
            );
            dispatch(
                services,
                client,
                converter,
                Some(proto::Request {
                    variant: Some(request::Variant::CancelOperation(
                        proto::CancelOperationRequest {
                            service: Payments::name().to_owned(),
                            operation: "quote".to_owned(),
                            operation_token: result.operation_token,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                "payments".to_owned(),
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
            assert!(cancelled.load(Ordering::SeqCst));
        }
    }

    #[derive(Clone, Default)]
    struct CountingCodec {
        encodes: Arc<AtomicUsize>,
        decodes: Arc<AtomicUsize>,
    }

    impl PayloadCodec for CountingCodec {
        fn encode(
            &self,
            context: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            assert!(matches!(context, SerializationContextData::Nexus(_)));
            self.encodes.fetch_add(1, Ordering::SeqCst);
            async { Ok(payloads) }.boxed()
        }

        fn decode(
            &self,
            context: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            assert!(matches!(context, SerializationContextData::Nexus(_)));
            self.decodes.fetch_add(1, Ordering::SeqCst);
            async { Ok(payloads) }.boxed()
        }
    }

    #[tokio::test]
    async fn typed_payloads_apply_nexus_codec_once() {
        let codec = CountingCodec::default();
        let converter = DataConverter::new(
            PayloadConverter::default(),
            temporalio_common::data_converters::DefaultFailureConverter::default(),
            codec.clone(),
        );
        let payload = DataConverter::default()
            .to_payload(&serialization_context(), &"quote".to_owned())
            .await
            .unwrap();
        let mut services = NexusServices::default();
        services.register(service()).unwrap();
        let status = dispatch(
            services,
            client().await,
            converter.clone(),
            Some(proto::Request {
                variant: Some(request::Variant::StartOperation(
                    proto::StartOperationRequest {
                        service: Payments::name().to_owned(),
                        operation: "quote".to_owned(),
                        payload: Some(payload),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }),
            "payments".to_owned(),
            None,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let nexus_task_completion::Status::Completed(proto::Response {
            variant:
                Some(response::Variant::StartOperation(proto::StartOperationResponse {
                    variant: Some(start_operation_response::Variant::SyncSuccess(output)),
                })),
        }) = status
        else {
            panic!("expected synchronous result")
        };
        let value: String = DataConverter::default()
            .from_payload(&serialization_context(), output.payload.unwrap())
            .await
            .unwrap();
        assert_eq!(value, "quote");
        assert_eq!(codec.decodes.load(Ordering::SeqCst), 1);
        assert_eq!(codec.encodes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_drops_handler_and_acknowledges_request() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let started = Notify::new();
        let token = CancellationToken::new();
        let converter = DataConverter::default();
        let execute = async {
            let _guard = Dropped(dropped.clone());
            started.notify_one();
            std::future::pending().await
        };
        let (status, ()) = tokio::join!(execute_request(execute, &token, &converter), async {
            started.notified().await;
            token.cancel();
        },);
        assert!(matches!(
            status,
            nexus_task_completion::Status::AckCancel(true)
        ));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn panics_and_operation_failures_have_distinct_protocol_results() {
        let converter = DataConverter::default();
        let token = CancellationToken::new();
        let panic = execute_request(async { panic!("handler panic") }, &token, &converter).await;
        assert!(matches!(
            panic,
            nexus_task_completion::Status::Failure(Failure {
                failure_info: Some(FailureInfo::NexusHandlerFailureInfo(_)),
                ..
            })
        ));
        let failed = execute_request(
            async { Err(TemporalOperationError::Operation("declined".to_owned())) },
            &token,
            &converter,
        )
        .await;
        assert!(matches!(
            failed,
            nexus_task_completion::Status::Completed(proto::Response {
                variant: Some(response::Variant::StartOperation(
                    proto::StartOperationResponse {
                        variant: Some(start_operation_response::Variant::Failure(Failure {
                            failure_info: Some(FailureInfo::ApplicationFailureInfo(_)),
                            ..
                        })),
                    }
                )),
            })
        ));
    }
}
