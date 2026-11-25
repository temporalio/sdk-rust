//! Experimental Nexus operation handlers and workflow Nexus APIs.
//!
//! This module is experimental and may change without notice.
//!
//! ```no_run
//! use nexus_rpc::service;
//! use temporalio_sdk::{
//!     WorkerOptions,
//!     nexus::{
//!         NexusContext, TemporalNexusClient, TemporalOperationError, TemporalOperationResult,
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
//!         ctx: NexusContext,
//!         _client: TemporalNexusClient,
//!         input: String,
//!     ) -> Result<TemporalOperationResult<String>, TemporalOperationError> {
//!         let _request_id = &ctx.info().request_id;
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

use crate::{ApplicationFailure, CommonWorker, OutgoingError, WorkerRunError, panic_formatter};
use base64::{
    Engine,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig, general_purpose},
};
use futures_util::{FutureExt, future::BoxFuture};
use heck::ToUpperCamelCase;
use nexus_rpc::{HandlerError, HandlerErrorRetryBehavior, HandlerErrorType, OperationError};
pub use nexus_rpc::{NexusOperation, NexusService};
use operation_token::TemporalOperationToken;
use parking_lot::Mutex;
use prost::Name;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap, future::Future, marker::PhantomData, mem, panic::AssertUnwindSafe,
    sync::Arc, time::SystemTime,
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
        DataConverter, GenericPayloadConverter, NexusSerializationContext, PayloadConversionError,
        SerializationContext, SerializationContextData, TemporalDeserializable,
        TemporalSerializable,
    },
    error::OutgoingNexusError,
    external_storage::{retrieve_external_payloads, store_external_payloads},
    payload_visitor::encode_payloads,
    protos::{
        coresdk::nexus::{NexusTaskCompletion, nexus_task, nexus_task_completion},
        temporal::api::{
            common::v1::{Callback, Link, callback, link},
            enums::v1::EventType,
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
    /// A workflow started with [`TemporalNexusClient::start_workflow`] will complete the operation.
    Async(TemporalOperationToken<T>),
}

mod operation_token {
    use std::marker::PhantomData;

    /// Opaque identity of a workflow-backed operation with output `T`.
    #[derive(Debug)]
    pub struct TemporalOperationToken<T> {
        pub(super) token: String,
        pub(super) output: PhantomData<fn() -> T>,
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct WorkflowOperationToken {
    #[serde(rename = "ns")]
    namespace: String,
    #[serde(rename = "wid")]
    workflow_id: String,
    #[serde(rename = "v", skip_serializing_if = "Option::is_none")]
    version: Option<i32>,
    #[serde(rename = "t", default = "WorkflowOperationToken::workflow_run_type")]
    token_type: i32,
}

impl WorkflowOperationToken {
    fn workflow_run_type() -> i32 {
        1
    }

    fn new(namespace: String, workflow_id: String) -> Self {
        Self {
            namespace,
            workflow_id,
            version: None,
            token_type: Self::workflow_run_type(),
        }
    }

    fn encode_token(&self) -> String {
        let json =
            serde_json::to_vec(self).expect("operation token contains only JSON-compatible fields");
        general_purpose::URL_SAFE_NO_PAD.encode(json)
    }

    fn decode_token(token: &str) -> Result<Self, TemporalOperationError> {
        let invalid =
            || TemporalOperationError::handler("BAD_REQUEST", "Invalid operation token", false);
        // Match .NET's Convert.FromBase64String acceptance of padding and ASCII whitespace.
        let encoded: String = token
            .chars()
            .filter(|c| !matches!(c, ' ' | '\t' | '\r' | '\n'))
            .map(|c| match c {
                '-' => '+',
                '_' => '/',
                c => c,
            })
            .collect();
        let bytes = GeneralPurpose::new(
            &base64::alphabet::STANDARD,
            GeneralPurposeConfig::new()
                .with_decode_padding_mode(DecodePaddingMode::Indifferent)
                .with_decode_allow_trailing_bits(true),
        )
        .decode(encoded)
        .map_err(|_| invalid())?;
        let decoded: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if decoded.version.unwrap_or(0) != 0
            || decoded.token_type != Self::workflow_run_type()
            || decoded.namespace.is_empty()
            || decoded.workflow_id.is_empty()
        {
            return Err(invalid());
        }
        Ok(decoded)
    }
}

/// Context of the Nexus request currently being served.
#[derive(Clone, Debug)]
pub struct NexusContext {
    info: NexusInfo,
}

impl NexusContext {
    /// Returns information about the executing Nexus request.
    pub fn info(&self) -> &NexusInfo {
        &self.info
    }
}

/// Request-scoped client for starting workflow-backed Nexus operations.
#[derive(Clone, Debug)]
pub struct TemporalNexusClient {
    client: Client,
    request: proto::StartOperationRequest,
    outbound_links: Arc<Mutex<Vec<proto::Link>>>,
}

/// Information about the Nexus request currently being served.
#[derive(Clone, Debug)]
pub struct NexusInfo {
    /// Headers propagated by the Nexus caller.
    pub headers: HashMap<String, String>,
    /// Endpoint addressed by the caller.
    pub endpoint: String,
    /// Nexus service wire name.
    pub service: String,
    /// Nexus operation wire name.
    pub operation: String,
    /// Stable request ID supplied by the caller, suitable for idempotent starts.
    pub request_id: String,
    /// Deadline of this request, not the lifetime of an asynchronous operation.
    pub deadline: Option<SystemTime>,
    /// Cancellation of this request due to its deadline or worker shutdown.
    pub cancellation_token: CancellationToken,
}

impl TemporalNexusClient {
    /// Configured Temporal client, including the worker's converters and codecs.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Start a workflow that completes this Nexus operation when it completes.
    ///
    /// The Nexus request ID is used as the workflow start RPC's idempotency key.
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
        let token =
            WorkflowOperationToken::new(self.client.namespace(), options.workflow_id.clone())
                .encode_token();
        let workflow_event_type = link::WorkflowEvent::full_name();
        let nexus_operation_type = link::NexusOperation::full_name();
        let links = self
            .request
            .links
            .iter()
            .filter_map(|link| {
                if link.r#type == workflow_event_type {
                    Some(proto::workflow_event_link_from_nexus(link))
                } else if link.r#type == nexus_operation_type {
                    Some(proto::nexus_operation_link_from_nexus(link))
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| TemporalOperationError::handler("BAD_REQUEST", error, false))?;
        // Older servers accept only workflow-event links at the top level of a start request.
        options.links.extend(
            links
                .iter()
                .filter(|link| matches!(link.variant, Some(link::Variant::WorkflowEvent(_))))
                .cloned(),
        );
        if !self.request.callback.is_empty() {
            let mut header = self.request.callback_header.clone();
            // Completion can reach the caller before AsyncSuccess supplies the operation identity.
            header.insert("nexus-operation-token".to_owned(), token.clone());
            options.completion_callbacks.push(Callback {
                variant: Some(callback::Variant::Nexus(callback::Nexus {
                    url: self.request.callback.clone(),
                    header,
                })),
                links,
            });
        }
        let response_link = temporalio_client::__internal::start_nexus_workflow(
            &self.client,
            workflow,
            input,
            options,
            &self.request.request_id,
        )
        .await
        .map_err(TemporalOperationError::from)?;
        if let Some(link) = workflow_event_link_to_nexus(response_link)? {
            self.outbound_links.lock().push(link);
        }
        Ok(TemporalOperationResult::Async(TemporalOperationToken {
            token,
            output: PhantomData,
        }))
    }
}

fn workflow_event_link_to_nexus(
    link: Option<Link>,
) -> Result<Option<proto::Link>, TemporalOperationError> {
    let Some(link::Variant::WorkflowEvent(event)) = link.and_then(|link| link.variant) else {
        return Ok(None);
    };
    let mut url = Url::parse("temporal:///").expect("valid Temporal link base");
    url.path_segments_mut()
        .expect("Temporal link supports path segments")
        .extend([
            "namespaces",
            &event.namespace,
            "workflows",
            &event.workflow_id,
            &event.run_id,
            "history",
        ]);
    if let Some(reference) = event.reference {
        let (key, value, event_type, reference_type) = match reference {
            link::workflow_event::Reference::EventRef(reference) => (
                "eventID",
                reference.event_id.to_string(),
                reference.event_type,
                link::workflow_event::EventReference::NAME,
            ),
            link::workflow_event::Reference::RequestIdRef(reference) => (
                "requestID",
                reference.request_id,
                reference.event_type,
                link::workflow_event::RequestIdReference::NAME,
            ),
        };
        let event_type = EventType::try_from(event_type)
            .map_err(|error| TemporalOperationError::handler("INTERNAL", error, true))?;
        // Other SDKs emit the short PascalCase name, not the full protobuf enum name.
        let event_type = event_type
            .as_str_name()
            .trim_start_matches("EVENT_TYPE_")
            .to_upper_camel_case();
        url.query_pairs_mut()
            .append_pair(key, &value)
            .append_pair("eventType", &event_type)
            .append_pair("referenceType", reference_type);
    }
    Ok(Some(proto::Link {
        url: url.into(),
        r#type: link::WorkflowEvent::full_name(),
    }))
}

/// Invalid Nexus service or operation registration.
#[derive(Debug, thiserror::Error)]
pub enum NexusRegistrationError {
    /// A service has no registered operations.
    #[error("Nexus service {0} has no registered operations")]
    EmptyService(String),
    /// A service with this wire name is already registered.
    #[error("Nexus service {0} is already registered")]
    DuplicateService(String),
    /// An operation with this wire name is already registered in the service.
    #[error("Nexus operation {service}/{operation} is already registered")]
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
///     temporal_nexus_service_handler, temporal_operation, NexusContext, TemporalNexusClient,
///     TemporalOperationError, TemporalOperationResult,
/// };
/// #[service]
/// trait PaymentService { fn quote(input: String) -> String; }
/// struct PaymentHandlers;
/// #[temporal_nexus_service_handler(service = PaymentService)]
/// impl PaymentHandlers {
///     #[temporal_operation]
///     async fn quote(&self, ctx: NexusContext, client: TemporalNexusClient, input: String)
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
        F: Fn(NexusContext, TemporalNexusClient, O::Input) -> Fut + Send + Sync + 'static,
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
            Arc::new(move |context, client, converter| {
                let handler = handler.clone();
                async move {
                    let payload = client.request.payload.clone().unwrap_or_default();
                    let mut decoded = converter
                        .codec()
                        .decode(&serialization_context(), vec![payload])
                        .await
                        .map_err(TemporalOperationError::input_codec_error)?;
                    let payload =
                        decoded
                            .pop()
                            .filter(|_| decoded.is_empty())
                            .ok_or_else(|| {
                                HandlerError::new(
                                    HandlerErrorType::Internal,
                                    "Payload codec must return exactly one Nexus input payload",
                                )
                            })?;
                    let input = converter
                        .payload_converter()
                        .from_payload::<O::Input>(
                            &SerializationContext::new(
                                &serialization_context(),
                                converter.payload_converter(),
                            ),
                            payload,
                        )
                        .map_err(TemporalOperationError::input_conversion_error)?;
                    let outbound_links = client.outbound_links.clone();
                    match handler(context, client, input).await? {
                        TemporalOperationResult::Sync(output) => {
                            let payload = converter
                                .payload_converter()
                                .to_payload(
                                    &SerializationContext::new(
                                        &serialization_context(),
                                        converter.payload_converter(),
                                    ),
                                    &output,
                                )
                                .map_err(TemporalOperationError::from)?;
                            Ok(start_operation_response::Variant::SyncSuccess(
                                start_operation_response::Sync {
                                    payload: Some(payload),
                                    links: mem::take(&mut *outbound_links.lock()),
                                },
                            ))
                        }
                        TemporalOperationResult::Async(token) => {
                            Ok(start_operation_response::Variant::AsyncSuccess(
                                #[allow(deprecated)]
                                start_operation_response::Async {
                                    operation_id: token.token.clone(),
                                    operation_token: token.token,
                                    links: mem::take(&mut *outbound_links.lock()),
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

/// An operation failure or a failure to handle a Nexus request.
#[derive(Debug, thiserror::Error)]
pub enum TemporalOperationError {
    /// A terminal failed or canceled operation, including its application cause.
    #[error(transparent)]
    Operation(#[from] OperationError),
    /// An application failure, reported as a handler failure with its retry policy.
    #[error(transparent)]
    Application(#[from] Box<ApplicationFailure>),
    /// A Nexus handler failure, whose type and retry behavior follow the Nexus protocol.
    #[error(transparent)]
    Handler(#[from] HandlerError),
    /// An implementation error; known Temporal and RPC errors retain their classification.
    #[error(transparent)]
    Unexpected(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl TemporalOperationError {
    pub(crate) fn handler(error_type: &str, message: impl ToString, retryable: bool) -> Self {
        HandlerError::from_raw_type(error_type, message.to_string())
            .with_retry_behavior(if retryable {
                HandlerErrorRetryBehavior::Retryable
            } else {
                HandlerErrorRetryBehavior::NonRetryable
            })
            .into()
    }

    fn into_status(self, converter: &DataConverter) -> nexus_task_completion::Status {
        match self {
            Self::Operation(error) => start_status(start_operation_response::Variant::Failure(
                converter.to_failure(
                    &serialization_context(),
                    OutgoingError::Nexus(Box::new(OutgoingNexusError::Operation(error))),
                ),
            )),
            Self::Application(failure) => Self::Unexpected(failure).into_status(converter),
            Self::Unexpected(error) => {
                let error = match error.downcast::<HandlerError>() {
                    Ok(handler) => return Self::Handler(*handler).into_status(converter),
                    Err(error) => error,
                };
                let error = match error.downcast::<OperationError>() {
                    Ok(operation) => return Self::Operation(*operation).into_status(converter),
                    Err(error) => error,
                };
                let error = match error.downcast::<WorkflowStartError>() {
                    Ok(error) => return Self::from(*error).into_status(converter),
                    Err(error) => error,
                };
                let error = match error.downcast::<WorkflowInteractionError>() {
                    Ok(error) => return Self::from(*error).into_status(converter),
                    Err(error) => error,
                };
                let handler = if let Some(app) = error.downcast_ref::<ApplicationFailure>() {
                    if app.is_non_retryable() {
                        HandlerError::new(
                            HandlerErrorType::Internal,
                            "Handler failed with non-retryable application error",
                        )
                        .with_retry_behavior(HandlerErrorRetryBehavior::NonRetryable)
                    } else {
                        HandlerError::new(HandlerErrorType::Internal, "Internal handler error")
                    }
                } else if let Some(status) =
                    error.downcast_ref::<temporalio_client::tonic::Status>()
                {
                    rpc_handler_error(status)
                } else {
                    HandlerError::new(HandlerErrorType::Internal, "Internal handler error")
                };
                Self::Handler(handler.with_source(error)).into_status(converter)
            }
            Self::Handler(error) => nexus_task_completion::Status::Failure(converter.to_failure(
                &serialization_context(),
                OutgoingError::Nexus(Box::new(OutgoingNexusError::Handler(error))),
            )),
        }
    }

    fn input_conversion_error(error: PayloadConversionError) -> Self {
        let source = match error {
            PayloadConversionError::EncodingError(source) => source,
            error => Box::new(error),
        };
        if source.is::<HandlerError>()
            || source.is::<temporalio_client::tonic::Status>()
            || source.is::<WorkflowStartError>()
            || source.is::<WorkflowInteractionError>()
        {
            return Self::Unexpected(source);
        }
        if let Some(app) = source.downcast_ref::<ApplicationFailure>()
            && !(app.is_non_retryable() && app.type_name() == Some("PayloadValidationError"))
        {
            return Self::Unexpected(source);
        }
        HandlerError::new(HandlerErrorType::BadRequest, "Invalid operation input")
            .with_retry_behavior(HandlerErrorRetryBehavior::NonRetryable)
            .with_source(source)
            .into()
    }

    fn input_codec_error(error: PayloadConversionError) -> Self {
        if let PayloadConversionError::EncodingError(source) = &error
            && source
                .downcast_ref::<ApplicationFailure>()
                .is_some_and(|app| {
                    app.is_non_retryable() && app.type_name() == Some("PayloadValidationError")
                })
        {
            return Self::input_conversion_error(error);
        }
        Self::from(error)
    }
}

impl From<ApplicationFailure> for TemporalOperationError {
    fn from(error: ApplicationFailure) -> Self {
        Self::Application(Box::new(error))
    }
}

impl From<PayloadConversionError> for TemporalOperationError {
    fn from(error: PayloadConversionError) -> Self {
        match error {
            PayloadConversionError::EncodingError(source) => Self::Unexpected(source),
            error => Self::Unexpected(Box::new(error)),
        }
    }
}

impl From<temporalio_client::tonic::Status> for TemporalOperationError {
    fn from(error: temporalio_client::tonic::Status) -> Self {
        Self::Unexpected(Box::new(error))
    }
}

impl From<WorkflowStartError> for TemporalOperationError {
    fn from(error: WorkflowStartError) -> Self {
        match error {
            WorkflowStartError::Rpc(status) => status.into(),
            WorkflowStartError::PayloadConversion(error) => error.into(),
            error @ WorkflowStartError::AlreadyStarted { .. } => {
                HandlerError::new(HandlerErrorType::Internal, error.to_string())
                    .with_retry_behavior(HandlerErrorRetryBehavior::NonRetryable)
                    .with_source(error)
                    .into()
            }
            error => HandlerError::new(HandlerErrorType::Internal, "Internal handler error")
                .with_source(error)
                .into(),
        }
    }
}

impl From<WorkflowInteractionError> for TemporalOperationError {
    fn from(error: WorkflowInteractionError) -> Self {
        match error {
            WorkflowInteractionError::Rpc(status) | WorkflowInteractionError::NotFound(status) => {
                status.into()
            }
            WorkflowInteractionError::PayloadConversion(error) => error.into(),
            WorkflowInteractionError::Other(error) => Self::Unexpected(error),
            error => HandlerError::new(HandlerErrorType::Internal, "Internal handler error")
                .with_source(error)
                .into(),
        }
    }
}

fn rpc_handler_error(status: &temporalio_client::tonic::Status) -> HandlerError {
    let error_type = match status.code() {
        Code::InvalidArgument => HandlerErrorType::BadRequest,
        Code::Aborted | Code::Unavailable => HandlerErrorType::Unavailable,
        Code::NotFound => HandlerErrorType::NotFound,
        Code::ResourceExhausted => HandlerErrorType::ResourceExhausted,
        Code::Unimplemented => HandlerErrorType::NotImplemented,
        Code::DeadlineExceeded => HandlerErrorType::UpstreamTimeout,
        _ => HandlerErrorType::Internal,
    };
    let error = HandlerError::new(error_type, status.message());
    if matches!(
        status.code(),
        Code::AlreadyExists | Code::FailedPrecondition | Code::OutOfRange
    ) {
        error.with_retry_behavior(HandlerErrorRetryBehavior::NonRetryable)
    } else {
        error
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
            NexusContext,
            TemporalNexusClient,
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
                            let namespace = worker.get_config().namespace.clone();
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
                                let status = execute_request(execute, &cancellation, &converter, &namespace).await;
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
        Some(request::Variant::StartOperation(mut start)) => {
            let service = services.operations.get(&start.service).ok_or_else(|| {
                HandlerError::new(
                    HandlerErrorType::NotFound,
                    format!("Unknown Nexus service {:?}", start.service),
                )
            })?;
            let handler = service.get(&start.operation).ok_or_else(|| {
                HandlerError::new(
                    HandlerErrorType::NotFound,
                    format!("Unknown Nexus operation {:?}", start.operation),
                )
            })?;
            let mut info = NexusInfo {
                headers: request.header,
                endpoint,
                service: start.service.clone(),
                operation: start.operation.clone(),
                request_id: start.request_id.clone(),
                deadline,
                cancellation_token: cancellation,
            };
            for interceptor in &services.interceptors {
                interceptor.on_start(&mut info).await?;
            }
            retrieve_external_payloads(&mut start, &converter)
                .await
                .map_err(TemporalOperationError::from)?;
            let result = handler(
                NexusContext { info: info.clone() },
                TemporalNexusClient {
                    client,
                    request: start,
                    outbound_links: Arc::default(),
                },
                converter,
            )
            .await;
            for interceptor in services.interceptors.iter().rev() {
                interceptor
                    .on_finish(&info, result.as_ref().map(|_| ()))
                    .await;
            }
            result.map(start_status)
        }
        Some(request::Variant::CancelOperation(cancel)) => {
            let service = services.operations.get(&cancel.service).ok_or_else(|| {
                HandlerError::new(
                    HandlerErrorType::NotFound,
                    format!("Unknown Nexus service {:?}", cancel.service),
                )
            })?;
            if !service.contains_key(&cancel.operation) {
                return Err(HandlerError::new(
                    HandlerErrorType::NotFound,
                    format!("Unknown Nexus operation {:?}", cancel.operation),
                )
                .into());
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
                .map_err(TemporalOperationError::from)?;
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
    namespace: &str,
) -> nexus_task_completion::Status {
    let execute = async {
        let status = match AssertUnwindSafe(execute).catch_unwind().await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => error.into_status(converter),
            Err(panic) => TemporalOperationError::Unexpected(Box::new(NexusHandlerPanic(
                panic_formatter(panic).to_string(),
            )))
            .into_status(converter),
        };
        let successful = match &status {
            nexus_task_completion::Status::Completed(response) => match response.variant.as_ref() {
                Some(response::Variant::StartOperation(response)) => matches!(
                    response.variant.as_ref(),
                    Some(
                        start_operation_response::Variant::SyncSuccess(_)
                            | start_operation_response::Variant::AsyncSuccess(_)
                    )
                ),
                Some(response::Variant::CancelOperation(_)) => true,
                _ => false,
            },
            _ => false,
        };
        let mut completion = NexusTaskCompletion {
            task_token: Vec::new(),
            status: Some(status),
        };
        if let Err(error) =
            encode_payloads(&mut completion, converter.codec(), &serialization_context()).await
        {
            if !successful {
                return Err(error);
            }
            // A result codec can reject a value with an explicit application/handler error.
            // Make one attempt to report that error; failure of this encoding uses the raw fallback.
            completion.status = Some(TemporalOperationError::from(error).into_status(converter));
            encode_payloads(&mut completion, converter.codec(), &serialization_context()).await?;
        }
        store_external_payloads(&mut completion, converter, namespace, None).await?;
        Ok::<_, PayloadConversionError>(completion.status.expect("completion has a status"))
    };
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => nexus_task_completion::Status::AckCancel(true),
        result = AssertUnwindSafe(execute).catch_unwind() => match result {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => failure_building_completion(error.to_string()),
            Err(panic) => failure_building_completion(panic_formatter(panic).to_string()),
        },
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Nexus handler panicked: {0}")]
struct NexusHandlerPanic(String);

fn failure_building_completion(error: String) -> nexus_task_completion::Status {
    // Reusing a failing converter or codec here could strand the task and its worker slot.
    tracing::warn!(%error, "Failed building Nexus task completion");
    nexus_task_completion::Status::Failure(Failure {
        message: format!("Failed building Nexus task completion: {error}"),
        failure_info: Some(FailureInfo::NexusHandlerFailureInfo(
            NexusHandlerFailureInfo {
                r#type: HandlerErrorType::Internal.as_str().to_owned(),
                ..Default::default()
            },
        )),
        ..Default::default()
    })
}

/// Observe handler requests and outcomes, including propagated headers.
///
/// Request cancellation drops the handler future and does not invoke `on_finish`. Interceptors
/// should use drop guards for cleanup that must also happen when a request is canceled.
#[async_trait::async_trait]
pub trait NexusHandlerInterceptor: Send + Sync {
    /// Inspect or update request information before running the handler.
    async fn on_start(&self, _info: &mut NexusInfo) -> Result<(), TemporalOperationError> {
        Ok(())
    }

    /// Observe a completed request. Hooks run in reverse registration order.
    async fn on_finish(&self, _info: &NexusInfo, _result: Result<(), &TemporalOperationError>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkerOptions;
    use prost::Message;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use temporalio_client::{
        ClientOptions, Connection, ConnectionOptions,
        callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse},
        tonic::Status,
    };
    use temporalio_common::{
        WorkflowDefinition,
        data_converters::{
            DefaultFailureConverter, ExternalStorage, FailureConverter, PayloadCodec,
            PayloadConversionError, PayloadConverter, StorageDriver, StorageDriverClaim,
            StorageDriverError, StorageDriverRetrieveContext, StorageDriverStoreContext,
        },
        error::IncomingError,
        protos::temporal::api::{
            common::v1::Payload,
            enums::v1::NexusHandlerErrorRetryBehavior,
            workflowservice::v1::{
                RequestCancelWorkflowExecutionRequest, RequestCancelWorkflowExecutionResponse,
                StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
            },
        },
    };
    use tokio::sync::{Barrier, Notify};

    #[nexus_rpc::service]
    trait Payments {
        fn quote(input: String) -> String;
    }

    #[test]
    fn workflow_event_links_round_trip_escaped_path_segments() {
        let link = Link {
            variant: Some(link::Variant::WorkflowEvent(link::WorkflowEvent {
                namespace: "example namespace+".to_owned(),
                workflow_id: "folder/処理?x=%2F".to_owned(),
                run_id: "example run".to_owned(),
                reference: Some(link::workflow_event::Reference::EventRef(
                    link::workflow_event::EventReference {
                        event_id: 42,
                        event_type: EventType::WorkflowExecutionOptionsUpdated as i32,
                    },
                )),
            })),
        };
        let encoded = workflow_event_link_to_nexus(Some(link.clone()))
            .unwrap()
            .unwrap();
        assert_eq!(
            proto::workflow_event_link_from_nexus(&encoded).unwrap(),
            link
        );
    }

    #[test]
    fn operation_tokens_accept_cross_sdk_escaping() {
        for (namespace, workflow_id, expected) in [
            ("ns", "wid", "eyJucyI6Im5zIiwid2lkIjoid2lkIiwidCI6MX0"),
            (
                "ns/with+special",
                "wf?id=1&foo=bar",
                "eyJucyI6Im5zL3dpdGhcdTAwMkJzcGVjaWFsIiwid2lkIjoid2Y_aWQ9MVx1MDAyNmZvbz1iYXIiLCJ0IjoxfQ",
            ),
            (
                "名前",
                "quote\"<>&'`+\\\n😀",
                "eyJucyI6Ilx1NTQwRFx1NTI0RCIsIndpZCI6InF1b3RlXHUwMDIyXHUwMDNDXHUwMDNFXHUwMDI2XHUwMDI3XHUwMDYwXHUwMDJCXFxcblx1RDgzRFx1REUwMCIsInQiOjF9",
            ),
        ] {
            let token = WorkflowOperationToken::new(namespace.to_owned(), workflow_id.to_owned());
            for encoded in [expected, &token.encode_token()] {
                let decoded = WorkflowOperationToken::decode_token(encoded).unwrap();
                assert_eq!(decoded.namespace, namespace);
                assert_eq!(decoded.workflow_id, workflow_id);
            }
        }
        for json in [
            r#"{"t":1,"ns":"\u540d\u524d","wid":"quote\"<>&'`+\\\n\ud83d\ude00"}"#,
            r#"{"t":1,"ns":"名前","wid":"quote\"\u003c\u003e\u0026'`+\\\n😀"}"#,
            r#"{"t":1,"ns":"名前","wid":"quote\"<>&'`+\\\n😀"}"#,
        ] {
            let token = general_purpose::URL_SAFE_NO_PAD.encode(json);
            let decoded = WorkflowOperationToken::decode_token(&token).unwrap();
            assert_eq!(decoded.namespace, "名前");
            assert_eq!(decoded.workflow_id, "quote\"<>&'`+\\\n😀");
        }
    }

    #[test]
    fn operation_tokens_accept_cross_sdk_tokens_and_validate_version_and_type() {
        let python = WorkflowOperationToken::decode_token(
            "eyJ0IjoxLCJucyI6ImRlZmF1bHQiLCJ3aWQiOiJteS13ZiJ9",
        )
        .unwrap();
        assert_eq!(python.namespace, "default");
        assert_eq!(python.workflow_id, "my-wf");
        for json in [
            r#"{"t":1,"ns":"ns","wid":"wid","v":null}"#,
            r#"{"t":1,"ns":"ns","wid":"wid","v":0}"#,
            r#"{"ns":"ns","wid":"wid"}"#,
        ] {
            for engine in [general_purpose::URL_SAFE_NO_PAD, general_purpose::STANDARD] {
                let token = engine.encode(json);
                assert!(WorkflowOperationToken::decode_token(&token).is_ok());
            }
        }
        for json in [
            r#"{"t":1,"ns":"ns","wid":"wid","v":1}"#,
            r#"{"t":3,"ns":"ns","wid":"wid"}"#,
            r#"{"t":0,"ns":"ns","wid":"wid"}"#,
            r#"{"t":null,"ns":"ns","wid":"wid"}"#,
            r#"{"t":1,"ns":"ns","wid":""}"#,
            r#"{"t":1,"ns":"","wid":"wid"}"#,
            r#"{"t":1,"wid":"wid"}"#,
            "null",
            "not json",
        ] {
            let token = general_purpose::URL_SAFE_NO_PAD.encode(json);
            assert!(matches!(
                WorkflowOperationToken::decode_token(&token),
                Err(TemporalOperationError::Handler(error))
                    if error.effective_retry_behavior() == HandlerErrorRetryBehavior::NonRetryable
            ));
        }
        for token in ["", "!!!invalid!!!", "rust-nexus-v1:1234"] {
            assert!(WorkflowOperationToken::decode_token(token).is_err());
        }
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

    struct HeaderInterceptor;

    #[async_trait::async_trait]
    impl NexusHandlerInterceptor for HeaderInterceptor {
        async fn on_start(&self, info: &mut NexusInfo) -> Result<(), TemporalOperationError> {
            assert_eq!(info.headers["caller"], "original");
            info.headers
                .insert("intercepted".to_owned(), "yes".to_owned());
            Ok(())
        }

        async fn on_finish(&self, info: &NexusInfo, result: Result<(), &TemporalOperationError>) {
            assert_eq!(info.headers["intercepted"], "yes");
            assert!(result.is_ok());
        }
    }

    struct InfoHandlers;

    #[temporal_nexus_service_handler(service = Payments)]
    impl InfoHandlers {
        #[temporal_operation]
        async fn quote(
            &self,
            ctx: NexusContext,
            client: TemporalNexusClient,
            input: String,
        ) -> Result<TemporalOperationResult<String>, TemporalOperationError> {
            let info = ctx.info();
            assert_eq!(info.headers["intercepted"], "yes");
            assert_eq!(info.endpoint, "payments");
            assert_eq!(info.service, Payments::name());
            assert_eq!(info.operation, "quote");
            assert_eq!(info.request_id, input);
            assert_eq!(info.deadline, Some(SystemTime::UNIX_EPOCH));
            assert!(!info.cancellation_token.is_cancelled());
            assert_eq!(client.client().namespace(), "test");
            Ok(TemporalOperationResult::Sync(input))
        }
    }

    #[tokio::test]
    async fn macro_handlers_receive_intercepted_nexus_info() {
        let mut services = NexusServices::default();
        services.register(InfoHandlers).unwrap();
        services.interceptor(Arc::new(HeaderInterceptor));
        let converter = DataConverter::default();
        let payload = converter
            .to_payload(&serialization_context(), &"request-id".to_owned())
            .await
            .unwrap();
        let result = dispatch(
            services,
            client().await,
            converter,
            Some(proto::Request {
                header: HashMap::from([("caller".to_owned(), "original".to_owned())]),
                variant: Some(request::Variant::StartOperation(
                    proto::StartOperationRequest {
                        service: Payments::name().to_owned(),
                        operation: "quote".to_owned(),
                        request_id: "request-id".to_owned(),
                        payload: Some(payload),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }),
            "payments".to_owned(),
            Some(SystemTime::UNIX_EPOCH),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(matches!(
            result,
            nexus_task_completion::Status::Completed(_)
        ));
    }

    #[tokio::test]
    async fn outbound_links_are_request_local_for_sync_and_async_results() {
        let barrier = Arc::new(Barrier::new(2));
        let mut services = NexusServices::default();
        services
            .register(
                NexusServiceHandler::new(Payments)
                    .operation(Payments::quote, move |ctx, client, input: String| {
                        let barrier = barrier.clone();
                        async move {
                            for suffix in ["first", "second"] {
                                client.outbound_links.lock().push(proto::Link {
                                    url: format!("{}/{suffix}", ctx.info().request_id),
                                    r#type: "test".to_owned(),
                                });
                            }
                            barrier.wait().await;
                            Ok(if input == "sync" {
                                TemporalOperationResult::Sync(input)
                            } else {
                                TemporalOperationResult::Async(TemporalOperationToken {
                                    token: "operation-token".to_owned(),
                                    output: PhantomData,
                                })
                            })
                        }
                    })
                    .unwrap(),
            )
            .unwrap();
        let client = client().await;
        let converter = DataConverter::default();
        let requests = ["sync", "async"].into_iter().map(|mode| {
            let services = services.clone();
            let client = client.clone();
            let converter = converter.clone();
            async move {
                let payload = converter
                    .to_payload(&serialization_context(), &mode.to_owned())
                    .await
                    .unwrap();
                let result = dispatch(
                    services,
                    client,
                    converter,
                    Some(proto::Request {
                        variant: Some(request::Variant::StartOperation(
                            proto::StartOperationRequest {
                                service: Payments::name().to_owned(),
                                operation: "quote".to_owned(),
                                request_id: mode.to_owned(),
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
                    variant: Some(response::Variant::StartOperation(response)),
                }) = result
                else {
                    panic!("expected start response");
                };
                let links = match response.variant.unwrap() {
                    start_operation_response::Variant::SyncSuccess(result) => {
                        assert_eq!(mode, "sync");
                        result.links
                    }
                    start_operation_response::Variant::AsyncSuccess(result) => {
                        assert_eq!(mode, "async");
                        assert_eq!(result.operation_token, "operation-token");
                        result.links
                    }
                    result => panic!("unexpected response: {result:?}"),
                };
                assert_eq!(
                    links.into_iter().map(|link| link.url).collect::<Vec<_>>(),
                    [format!("{mode}/first"), format!("{mode}/second")],
                );
            }
        });
        futures_util::future::join_all(requests).await;
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
            .to_core_options(
                "test".to_owned(),
                "test".to_owned(),
                &DataConverter::default(),
            )
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

    #[rstest::rstest]
    #[tokio::test]
    async fn workflow_backed_start_links_and_cancellation(
        #[values("missing", "event", "request_id", "workflow")] reference: &str,
    ) {
        for with_callback in [true, false] {
            let response_link = (reference != "missing").then(|| Link {
                variant: Some(link::Variant::WorkflowEvent(link::WorkflowEvent {
                    namespace: "server-namespace".to_owned(),
                    workflow_id: "server/workflow".to_owned(),
                    run_id: "original-run".to_owned(),
                    reference: match reference {
                        "event" => Some(link::workflow_event::Reference::EventRef(
                            link::workflow_event::EventReference {
                                event_id: 42,
                                event_type: EventType::WorkflowExecutionOptionsUpdated as i32,
                            },
                        )),
                        "request_id" => Some(link::workflow_event::Reference::RequestIdRef(
                            link::workflow_event::RequestIdReference {
                                request_id: "server-request".to_owned(),
                                event_type: EventType::WorkflowExecutionOptionsUpdated as i32,
                            },
                        )),
                        _ => None,
                    },
                })),
            });
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
                                    let token = WorkflowOperationToken::new(
                                        "test".to_owned(),
                                        "backing/id".to_owned(),
                                    )
                                    .encode_token();
                                    assert_eq!(callback.header["nexus-operation-token"], token);
                                    assert!(!callback.header.contains_key("nexus-operation-id"));
                                }
                                StartWorkflowExecutionResponse {
                                    run_id: "current-run".to_owned(),
                                    first_execution_run_id: "first-run".to_owned(),
                                    link: response_link.clone(),
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
                        .operation(Payments::quote, |_, client, input| async move {
                            tokio::spawn(async move {
                                client
                                    .start_workflow(
                                        BackingWorkflow,
                                        input,
                                        WorkflowStartOptions::new("backing-queue", "backing/id")
                                            .build(),
                                    )
                                    .await
                            })
                            .await
                            .unwrap()
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
            let expected_url = match reference {
                "missing" => None,
                "event" => Some(
                    "temporal:///namespaces/server-namespace/workflows/server%2Fworkflow/original-run/history?eventID=42&eventType=WorkflowExecutionOptionsUpdated&referenceType=EventReference",
                ),
                "request_id" => Some(
                    "temporal:///namespaces/server-namespace/workflows/server%2Fworkflow/original-run/history?requestID=server-request&eventType=WorkflowExecutionOptionsUpdated&referenceType=RequestIdReference",
                ),
                _ => Some(
                    "temporal:///namespaces/server-namespace/workflows/server%2Fworkflow/original-run/history",
                ),
            };
            assert_eq!(
                result.links,
                expected_url
                    .into_iter()
                    .map(|url| proto::Link {
                        url: url.to_owned(),
                        r#type: "temporal.api.common.v1.Link.WorkflowEvent".to_owned(),
                    })
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                result.operation_token,
                WorkflowOperationToken::new("test".to_owned(), "backing/id".to_owned())
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
        let execute = dispatch(
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
        );
        let status = execute_request(execute, &CancellationToken::new(), &converter, "test").await;
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
        let (status, ()) = tokio::join!(
            execute_request(execute, &token, &converter, "test"),
            async {
                started.notified().await;
                token.cancel();
            },
        );
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
        let panic = execute_request(
            async { panic!("handler panic") },
            &token,
            &converter,
            "test",
        )
        .await;
        assert!(matches!(
            panic,
            nexus_task_completion::Status::Failure(Failure {
                failure_info: Some(FailureInfo::NexusHandlerFailureInfo(_)),
                ..
            })
        ));
        let failed = execute_request(
            async { Err(OperationError::failed("declined").into()) },
            &token,
            &converter,
            "test",
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

    #[test]
    fn nexus_error_causes_and_retry_overrides_survive_conversion() {
        let converter = DataConverter::default();
        for kind in ["failed", "canceled", "handler", "application"] {
            let cause = ApplicationFailure::builder("original cause")
                .type_name("OriginalCause".to_owned())
                .details("typed detail".to_owned())
                .build();
            let error: TemporalOperationError = match kind {
                "failed" => OperationError::failed("operation failed")
                    .with_source(cause)
                    .into(),
                "canceled" => OperationError::canceled("operation canceled")
                    .with_source(cause)
                    .into(),
                "handler" => HandlerError::new(HandlerErrorType::Unavailable, "handler rejected")
                    .with_retry_behavior(HandlerErrorRetryBehavior::NonRetryable)
                    .with_source(cause)
                    .into(),
                _ => TemporalOperationError::Application(Box::new(cause)),
            };
            let failure = match error.into_status(&converter) {
                nexus_task_completion::Status::Failure(failure) => {
                    let Some(FailureInfo::NexusHandlerFailureInfo(info)) = &failure.failure_info
                    else {
                        panic!("expected handler failure");
                    };
                    assert_eq!(
                        info.r#type,
                        if kind == "handler" {
                            "UNAVAILABLE"
                        } else {
                            "INTERNAL"
                        }
                    );
                    assert_eq!(
                        info.retry_behavior,
                        if kind == "handler" {
                            NexusHandlerErrorRetryBehavior::NonRetryable
                        } else {
                            NexusHandlerErrorRetryBehavior::Unspecified
                        } as i32
                    );
                    failure
                }
                nexus_task_completion::Status::Completed(proto::Response {
                    variant:
                        Some(response::Variant::StartOperation(proto::StartOperationResponse {
                            variant: Some(start_operation_response::Variant::Failure(failure)),
                        })),
                }) => {
                    assert_eq!(
                        failure.message,
                        if kind == "failed" {
                            "operation failed"
                        } else {
                            "operation canceled"
                        }
                    );
                    assert_eq!(
                        matches!(
                            failure.failure_info,
                            Some(FailureInfo::CanceledFailureInfo(_))
                        ),
                        kind == "canceled"
                    );
                    failure
                }
                status => panic!("unexpected result: {status:?}"),
            };
            let cause = converter
                .failure_converter()
                .to_error(
                    *failure.cause.expect("original cause must be immediate"),
                    converter.payload_converter(),
                    &serialization_context(),
                )
                .unwrap();
            let temporalio_common::error::IncomingError::Application(cause) = cause else {
                panic!("expected application cause");
            };
            assert_eq!(cause.type_name(), Some("OriginalCause"));
            assert_eq!(cause.to_string(), "original cause");
            assert!(!cause.is_non_retryable());
            assert_eq!(
                cause.details::<String>().unwrap().as_deref(),
                Some("typed detail")
            );
            assert!(cause.cause().is_none());
        }
    }

    #[test]
    fn application_failures_preserve_their_non_retryable_override() {
        for non_retryable in [false, true] {
            let converter = DataConverter::default();
            let error = ApplicationFailure::builder("application error")
                .type_name("ApplicationCause".to_owned())
                .non_retryable(non_retryable)
                .details("detail".to_owned())
                .build();
            let nexus_task_completion::Status::Failure(failure) =
                TemporalOperationError::from(error).into_status(&converter)
            else {
                panic!("application errors must fail the handler, not the operation");
            };
            let Some(FailureInfo::NexusHandlerFailureInfo(info)) = &failure.failure_info else {
                panic!("expected Nexus handler failure");
            };
            assert_eq!(info.r#type, "INTERNAL");
            assert_eq!(
                info.retry_behavior,
                if non_retryable {
                    NexusHandlerErrorRetryBehavior::NonRetryable
                } else {
                    NexusHandlerErrorRetryBehavior::Unspecified
                } as i32
            );
            let cause = converter
                .failure_converter()
                .to_error(
                    *failure.cause.unwrap(),
                    converter.payload_converter(),
                    &serialization_context(),
                )
                .unwrap();
            let IncomingError::Application(cause) = cause else {
                panic!("expected application cause")
            };
            assert_eq!(cause.type_name(), Some("ApplicationCause"));
            assert_eq!(cause.to_string(), "application error");
            assert_eq!(cause.is_non_retryable(), non_retryable);
            assert_eq!(
                cause.details::<String>().unwrap().as_deref(),
                Some("detail")
            );
            assert!(cause.cause().is_none());
        }
    }

    #[test]
    fn grpc_status_mapping_matches_python_and_java() {
        for (code, error_type, non_retryable) in [
            (Code::Cancelled, "INTERNAL", false),
            (Code::Unknown, "INTERNAL", false),
            (Code::InvalidArgument, "BAD_REQUEST", false),
            (Code::DeadlineExceeded, "UPSTREAM_TIMEOUT", false),
            (Code::NotFound, "NOT_FOUND", false),
            (Code::AlreadyExists, "INTERNAL", true),
            (Code::PermissionDenied, "INTERNAL", false),
            (Code::ResourceExhausted, "RESOURCE_EXHAUSTED", false),
            (Code::FailedPrecondition, "INTERNAL", true),
            (Code::Aborted, "UNAVAILABLE", false),
            (Code::OutOfRange, "INTERNAL", true),
            (Code::Unimplemented, "NOT_IMPLEMENTED", false),
            (Code::Internal, "INTERNAL", false),
            (Code::Unavailable, "UNAVAILABLE", false),
            (Code::DataLoss, "INTERNAL", false),
            (Code::Unauthenticated, "INTERNAL", false),
        ] {
            let nexus_task_completion::Status::Failure(failure) =
                TemporalOperationError::from(Status::new(code, "RPC failed"))
                    .into_status(&DataConverter::default())
            else {
                panic!("expected handler failure for {code:?}");
            };
            let Some(FailureInfo::NexusHandlerFailureInfo(info)) = failure.failure_info else {
                panic!("expected handler classification");
            };
            assert_eq!(info.r#type, error_type, "{code:?}");
            assert_eq!(
                info.retry_behavior,
                if non_retryable {
                    NexusHandlerErrorRetryBehavior::NonRetryable
                } else {
                    NexusHandlerErrorRetryBehavior::Unspecified
                } as i32,
                "{code:?}"
            );
            assert_eq!(failure.message, "RPC failed");
            assert!(failure.cause.unwrap().message.contains("RPC failed"));
        }
    }

    #[test]
    fn client_errors_and_explicit_handler_overrides_are_not_reclassified() {
        for error in [
            TemporalOperationError::from(WorkflowStartError::Rpc(Status::unavailable(
                "start failed",
            ))),
            TemporalOperationError::Unexpected(Box::new(WorkflowInteractionError::Rpc(
                Status::unavailable("cancel failed"),
            ))),
        ] {
            let nexus_task_completion::Status::Failure(failure) =
                error.into_status(&DataConverter::default())
            else {
                panic!("expected handler failure");
            };
            assert!(matches!(failure.failure_info,
                Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "UNAVAILABLE"
                    && info.retry_behavior == NexusHandlerErrorRetryBehavior::Unspecified as i32));
        }
        let error = TemporalOperationError::Unexpected(Box::new(
            HandlerError::new(HandlerErrorType::Unavailable, "explicit failure")
                .with_retry_behavior(HandlerErrorRetryBehavior::NonRetryable)
                .with_source(ApplicationFailure::non_retryable("cause")),
        ));
        let nexus_task_completion::Status::Failure(failure) =
            error.into_status(&DataConverter::default())
        else {
            panic!("expected handler failure");
        };
        assert_eq!(failure.message, "explicit failure");
        assert!(matches!(failure.failure_info,
            Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "UNAVAILABLE"
                && info.retry_behavior == NexusHandlerErrorRetryBehavior::NonRetryable as i32));
        assert_eq!(failure.cause.unwrap().message, "cause");
    }

    #[test]
    fn duplicate_workflow_start_is_a_non_retryable_internal_handler_error() {
        let error = TemporalOperationError::from(WorkflowStartError::AlreadyStarted {
            run_id: Some("existing-run".to_owned()),
            source: Status::already_exists("already running"),
        });
        let nexus_task_completion::Status::Failure(failure) =
            error.into_status(&DataConverter::default())
        else {
            panic!("duplicate starts are handler failures, not operation outcomes")
        };
        assert!(matches!(failure.failure_info,
            Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "INTERNAL"
                && info.retry_behavior == NexusHandlerErrorRetryBehavior::NonRetryable as i32));
        assert!(failure.cause.unwrap().message.contains("existing-run"));
    }

    #[derive(Clone, Copy)]
    enum CodecFailure {
        Decode,
        Encode,
        EncodePanic,
        EncodeNonRetryableOnce,
    }

    struct FailingCodec {
        failure: CodecFailure,
        encodes: Arc<AtomicUsize>,
    }

    impl PayloadCodec for FailingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            let attempt = self.encodes.fetch_add(1, Ordering::SeqCst);
            let failure = self.failure;
            async move {
                match failure {
                    CodecFailure::Encode => Err(PayloadConversionError::EncodingError(
                        std::io::Error::other("encode outage").into(),
                    )),
                    CodecFailure::EncodePanic => panic!("encode panic"),
                    CodecFailure::Decode => Ok(payloads),
                    CodecFailure::EncodeNonRetryableOnce if attempt == 0 => {
                        Err(PayloadConversionError::EncodingError(Box::new(
                            ApplicationFailure::builder("result rejected")
                                .non_retryable(true)
                                .type_name("OutputRejected".to_owned())
                                .details("rejection detail")
                                .build(),
                        )))
                    }
                    CodecFailure::EncodeNonRetryableOnce => Ok(payloads),
                }
            }
            .boxed()
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            let failure = self.failure;
            async move {
                match failure {
                    CodecFailure::Decode => Err(PayloadConversionError::EncodingError(
                        std::io::Error::other("decode outage").into(),
                    )),
                    _ => Ok(payloads),
                }
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn input_codec_outages_retry_but_invalid_inputs_do_not() {
        for codec_fails in [false, true] {
            let converter = if codec_fails {
                DataConverter::new(
                    PayloadConverter::default(),
                    DefaultFailureConverter::default(),
                    FailingCodec {
                        failure: CodecFailure::Decode,
                        encodes: Arc::new(AtomicUsize::new(0)),
                    },
                )
            } else {
                DataConverter::default()
            };
            let mut services = NexusServices::default();
            services.register(service()).unwrap();
            let payload = DataConverter::default()
                .to_payload(&serialization_context(), &42_u32)
                .await
                .unwrap();
            let execute = dispatch(
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
            );
            let nexus_task_completion::Status::Failure(failure) =
                execute_request(execute, &CancellationToken::new(), &converter, "test").await
            else {
                panic!("invalid input must not invoke the handler")
            };
            assert!(matches!(failure.failure_info,
                Some(FailureInfo::NexusHandlerFailureInfo(info))
                    if info.r#type == if codec_fails { "INTERNAL" } else { "BAD_REQUEST" }
                    && info.retry_behavior == if codec_fails {
                        NexusHandlerErrorRetryBehavior::Unspecified
                    } else {
                        NexusHandlerErrorRetryBehavior::NonRetryable
                    } as i32));
            assert!(failure.cause.is_some());
        }
    }

    #[test]
    fn explicit_payload_validation_is_permanent_but_output_failures_are_internal() {
        let validation = || {
            PayloadConversionError::EncodingError(Box::new(
                ApplicationFailure::builder("input rejected")
                    .non_retryable(true)
                    .type_name("PayloadValidationError".to_owned())
                    .build(),
            ))
        };
        for error in [
            TemporalOperationError::input_conversion_error(validation()),
            TemporalOperationError::input_codec_error(validation()),
        ] {
            let nexus_task_completion::Status::Failure(failure) =
                error.into_status(&DataConverter::default())
            else {
                panic!("expected validation failure");
            };
            assert!(matches!(failure.failure_info,
                Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "BAD_REQUEST"
                    && info.retry_behavior == NexusHandlerErrorRetryBehavior::NonRetryable as i32));
            assert_eq!(failure.cause.unwrap().message, "input rejected");
        }
        let nexus_task_completion::Status::Failure(failure) =
            TemporalOperationError::from(validation()).into_status(&DataConverter::default())
        else {
            panic!("expected output conversion failure")
        };
        assert!(matches!(failure.failure_info,
            Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "INTERNAL"
                && info.retry_behavior == NexusHandlerErrorRetryBehavior::NonRetryable as i32));
    }

    struct PanickingFailureConverter;

    impl FailureConverter for PanickingFailureConverter {
        fn to_failure(
            &self,
            _: OutgoingError,
            _: &PayloadConverter,
            _: &SerializationContextData,
        ) -> Failure {
            panic!("failure converter panic");
        }

        fn to_error(
            &self,
            _: Failure,
            _: &PayloadConverter,
            _: &SerializationContextData,
        ) -> Result<IncomingError, PayloadConversionError> {
            unreachable!("this test only encodes failures");
        }
    }

    #[tokio::test]
    async fn broken_completion_converters_cannot_strand_the_task() {
        for codec_failure in [
            None,
            Some(CodecFailure::Encode),
            Some(CodecFailure::EncodePanic),
        ] {
            let encodes = Arc::new(AtomicUsize::new(0));
            let converter = if let Some(failure) = codec_failure {
                DataConverter::new(
                    PayloadConverter::default(),
                    DefaultFailureConverter::default(),
                    FailingCodec {
                        failure,
                        encodes: encodes.clone(),
                    },
                )
            } else {
                DataConverter::new(
                    PayloadConverter::default(),
                    PanickingFailureConverter,
                    CountingCodec {
                        encodes: encodes.clone(),
                        ..Default::default()
                    },
                )
            };
            let status = execute_request(
                async {
                    Err(OperationError::failed("original failure")
                        .with_source(
                            ApplicationFailure::builder("cause")
                                .details("detail")
                                .build(),
                        )
                        .into())
                },
                &CancellationToken::new(),
                &converter,
                "test",
            )
            .await;
            let nexus_task_completion::Status::Failure(failure) = status else {
                panic!("Core must always receive a completion");
            };
            assert!(
                failure
                    .message
                    .starts_with("Failed building Nexus task completion:")
            );
            assert!(failure.cause.is_none());
            assert!(failure.encoded_attributes.is_none());
            assert!(matches!(failure.failure_info,
                Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "INTERNAL"
                    && info.retry_behavior == NexusHandlerErrorRetryBehavior::Unspecified as i32));
            assert_eq!(
                encodes.load(Ordering::SeqCst),
                usize::from(codec_failure.is_some())
            );
        }
    }

    #[tokio::test]
    async fn handler_panics_retain_their_diagnostic_cause() {
        let status = execute_request(
            async { panic!("handler diagnostic") },
            &CancellationToken::new(),
            &DataConverter::default(),
            "test",
        )
        .await;
        let nexus_task_completion::Status::Failure(failure) = status else {
            panic!("expected handler failure")
        };
        assert_eq!(failure.message, "Internal handler error");
        assert_eq!(
            failure.cause.unwrap().message,
            "Nexus handler panicked: handler diagnostic"
        );
    }

    #[tokio::test]
    async fn canceling_a_missing_workflow_reports_not_found() {
        let mut options = ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
            .dns_load_balancing(None)
            .service_override(CallbackBasedGrpcService {
                callback: Arc::new(|request| {
                    assert_eq!(request.rpc, "RequestCancelWorkflowExecution");
                    async { Err(Status::not_found("workflow no longer exists")) }.boxed()
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
        services.register(service()).unwrap();
        let converter = DataConverter::default();
        let execute = dispatch(
            services,
            client,
            converter.clone(),
            Some(proto::Request {
                variant: Some(request::Variant::CancelOperation(
                    proto::CancelOperationRequest {
                        service: Payments::name().to_owned(),
                        operation: "quote".to_owned(),
                        operation_token: WorkflowOperationToken::new(
                            "test".to_owned(),
                            "gone".to_owned(),
                        )
                        .encode_token(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }),
            "payments".to_owned(),
            None,
            CancellationToken::new(),
        );
        let nexus_task_completion::Status::Failure(failure) =
            execute_request(execute, &CancellationToken::new(), &converter, "test").await
        else {
            panic!("missing workflows must not be acknowledged as successfully canceled")
        };
        assert!(matches!(failure.failure_info,
            Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "NOT_FOUND"
                && info.retry_behavior == NexusHandlerErrorRetryBehavior::Unspecified as i32));
        assert_eq!(failure.message, "workflow no longer exists");
        assert!(failure.cause.is_some());
    }

    #[tokio::test]
    async fn unavailable_external_input_is_a_retryable_handler_failure() {
        let mut services = NexusServices::default();
        services.register(service()).unwrap();
        let converter = DataConverter::default();
        let execute = dispatch(
            services,
            client().await,
            converter.clone(),
            Some(proto::Request {
                variant: Some(request::Variant::StartOperation(
                    proto::StartOperationRequest {
                        service: Payments::name().to_owned(),
                        operation: "quote".to_owned(),
                        payload: Some(Payload {
                            metadata: HashMap::from([
                                ("encoding".to_owned(), b"json/protobuf".to_vec()),
                                (
                                    "messageType".to_owned(),
                                    b"temporal.api.sdk.v1.ExternalStorageReference".to_vec(),
                                ),
                            ]),
                            data: br#"{"driverName":"missing-driver","claimData":{}}"#.to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }),
            "payments".to_owned(),
            None,
            CancellationToken::new(),
        );
        let nexus_task_completion::Status::Failure(failure) =
            execute_request(execute, &CancellationToken::new(), &converter, "test").await
        else {
            panic!("the handler must not receive an unresolved storage reference")
        };
        assert!(matches!(failure.failure_info,
            Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "INTERNAL"
                && info.retry_behavior == NexusHandlerErrorRetryBehavior::Unspecified as i32));
        assert!(failure.cause.unwrap().message.contains("TMPRL1105"));
    }

    struct FailingStore(Arc<AtomicUsize>);

    impl StorageDriver for FailingStore {
        fn name(&self) -> &str {
            "failing"
        }
        fn driver_type(&self) -> &str {
            "test"
        }

        fn store(
            &self,
            context: &StorageDriverStoreContext,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, StorageDriverError>> {
            assert_eq!(context.namespace.as_deref(), Some("test"));
            self.0.fetch_add(1, Ordering::SeqCst);
            async { Err(StorageDriverError::Other(anyhow::anyhow!("store outage"))) }.boxed()
        }

        fn retrieve(
            &self,
            _: &StorageDriverRetrieveContext,
            _: Vec<StorageDriverClaim>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, StorageDriverError>> {
            unreachable!("this test only stores completions");
        }
    }

    #[tokio::test]
    async fn outbound_storage_failure_is_reported_without_retrying_storage() {
        let stores = Arc::new(AtomicUsize::new(0));
        let storage = ExternalStorage::new(Arc::new(FailingStore(stores.clone())))
            .unwrap()
            .with_payload_size_threshold(0);
        let converter = DataConverter::default().with_external_storage(storage);
        let payload = converter
            .to_payload(&serialization_context(), &"output")
            .await
            .unwrap();
        let status = start_status(start_operation_response::Variant::SyncSuccess(
            start_operation_response::Sync {
                payload: Some(payload),
                links: Vec::new(),
            },
        ));
        let nexus_task_completion::Status::Failure(failure) = execute_request(
            async { Ok(status) },
            &CancellationToken::new(),
            &converter,
            "test",
        )
        .await
        else {
            panic!("failed storage must not send a partial completion")
        };
        assert!(failure.message.contains("store outage"));
        assert!(matches!(failure.failure_info,
            Some(FailureInfo::NexusHandlerFailureInfo(info)) if info.r#type == "INTERNAL"));
        assert!(failure.cause.is_none());
        assert_eq!(stores.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn result_codec_rejection_preserves_application_retry_policy() {
        let encodes = Arc::new(AtomicUsize::new(0));
        let converter = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            FailingCodec {
                failure: CodecFailure::EncodeNonRetryableOnce,
                encodes: encodes.clone(),
            },
        );
        let payload = DataConverter::default()
            .to_payload(&serialization_context(), &"output")
            .await
            .unwrap();
        let response = start_status(start_operation_response::Variant::SyncSuccess(
            start_operation_response::Sync {
                payload: Some(payload),
                links: Vec::new(),
            },
        ));
        let nexus_task_completion::Status::Failure(failure) = execute_request(
            async { Ok(response) },
            &CancellationToken::new(),
            &converter,
            "test",
        )
        .await
        else {
            panic!("rejected output must not be reported as success")
        };
        assert!(matches!(failure.failure_info,
            Some(FailureInfo::NexusHandlerFailureInfo(ref info)) if info.r#type == "INTERNAL"
                && info.retry_behavior == NexusHandlerErrorRetryBehavior::NonRetryable as i32));
        let IncomingError::NexusHandler(handler) = converter
            .failure_converter()
            .to_error(
                failure,
                converter.payload_converter(),
                &serialization_context(),
            )
            .unwrap()
        else {
            panic!("expected handler failure")
        };
        let Some(IncomingError::Application(cause)) = handler.cause() else {
            panic!("expected immediate application cause");
        };
        assert_eq!(cause.type_name(), Some("OutputRejected"));
        assert!(cause.is_non_retryable());
        assert_eq!(
            cause.details::<String>().unwrap().as_deref(),
            Some("rejection detail")
        );
        assert_eq!(encodes.load(Ordering::SeqCst), 2);
    }

    struct BlockingCodec(Arc<Notify>);

    impl PayloadCodec for BlockingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            let started = self.0.clone();
            async move {
                started.notify_one();
                std::future::pending().await
            }
            .boxed()
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            async { Ok(payloads) }.boxed()
        }
    }

    #[tokio::test]
    async fn task_cancellation_during_encoding_is_acknowledged() {
        let started = Arc::new(Notify::new());
        let converter = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            BlockingCodec(started.clone()),
        );
        let payload = DataConverter::default()
            .to_payload(&serialization_context(), &"output")
            .await
            .unwrap();
        let response = start_status(start_operation_response::Variant::SyncSuccess(
            start_operation_response::Sync {
                payload: Some(payload),
                links: Vec::new(),
            },
        ));
        let cancellation = CancellationToken::new();
        let (completion, ()) = tokio::join!(
            execute_request(async { Ok(response) }, &cancellation, &converter, "test"),
            async {
                started.notified().await;
                cancellation.cancel();
            },
        );
        assert!(matches!(
            completion,
            nexus_task_completion::Status::AckCancel(true)
        ));
    }
}
