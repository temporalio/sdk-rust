//! Nexus operation handler support for Temporal workers.

use futures_util::future::BoxFuture;
use std::{collections::HashMap, future::Future, sync::Arc, time::SystemTime};
use temporalio_client::{Client, ClientOptions};
use temporalio_common::{
    data_converters::{
        DataConverter, SerializationContextData, TemporalDeserializable, TemporalSerializable,
    },
    protos::{
        coresdk::nexus::nexus_task_completion,
        temporal::api::{
            common::v1::Payload,
            enums::v1::NexusHandlerErrorRetryBehavior,
            failure::v1::{Failure, NexusHandlerFailureInfo, failure::FailureInfo},
            nexus::v1::{
                CancelOperationResponse, Response, StartOperationResponse, response,
                start_operation_response,
            },
        },
    },
};
use temporalio_sdk_core::Worker as CoreWorker;
use tokio_util::sync::CancellationToken;

/// Context available to a Nexus start-operation handler.
pub struct NexusStartContext {
    /// Name of the Nexus service this operation belongs to.
    pub service: String,
    /// Name of the operation being started.
    pub operation: String,
    /// Caller-supplied idempotency key for this request.
    pub request_id: String,
    /// HTTP-style headers from the incoming request (lowercased keys).
    pub headers: HashMap<String, String>,
    /// Absolute deadline for the handler, parsed from the request-timeout header.
    pub deadline: Option<SystemTime>,
    cancellation_token: CancellationToken,
    worker: Arc<CoreWorker>,
    client_options: ClientOptions,
}

impl NexusStartContext {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        service: String,
        operation: String,
        request_id: String,
        headers: HashMap<String, String>,
        deadline: Option<SystemTime>,
        cancellation_token: CancellationToken,
        worker: Arc<CoreWorker>,
        client_options: ClientOptions,
    ) -> Self {
        Self {
            service,
            operation,
            request_id,
            headers,
            deadline,
            cancellation_token,
            worker,
            client_options,
        }
    }

    /// Returns the cancellation token for this handler invocation.
    /// Cancelled when core times out the task or the worker shuts down.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Resolves when the operation has been cancelled by core (timeout or shutdown).
    pub async fn cancelled(&self) {
        self.cancellation_token.cancelled().await
    }

    /// Returns a client targeting the same namespace and server as this worker.
    pub fn client(&self) -> Client {
        let connection = self.worker.get_client_connection().expect(
            "nexus context client unavailable: worker was not created from a Temporal client",
        );
        Client::new(connection, self.client_options.clone())
            .expect("client construction from a worker connection should be infallible")
    }
}

/// Context available to a Nexus cancel-operation handler.
pub struct NexusCancelContext {
    /// Name of the Nexus service this operation belongs to.
    pub service: String,
    /// Name of the operation being cancelled.
    pub operation: String,
    /// Token identifying the async operation instance to cancel.
    pub operation_token: String,
    /// HTTP-style headers from the incoming request (lowercased keys).
    pub headers: HashMap<String, String>,
    /// Absolute deadline for this cancel handler call.
    pub deadline: Option<SystemTime>,
}

impl NexusCancelContext {
    #[doc(hidden)]
    pub fn new(
        service: String,
        operation: String,
        operation_token: String,
        headers: HashMap<String, String>,
        deadline: Option<SystemTime>,
    ) -> Self {
        Self {
            service,
            operation,
            operation_token,
            headers,
            deadline,
        }
    }
}

/// Result returned by a Nexus start-operation handler.
pub enum NexusOperationResult<T> {
    /// The operation completed synchronously with a result value.
    Sync(T),
    /// The operation will complete asynchronously; the token identifies it.
    Async {
        /// Token the caller can use to reference and cancel the async operation.
        operation_token: String,
    },
}

impl<T> NexusOperationResult<T> {
    /// Convenience constructor for a synchronous result.
    pub fn sync(value: T) -> Self {
        Self::Sync(value)
    }
}

/// Error returned by a Nexus handler.
///
/// Maps to a `NexusHandlerFailureInfo` failure sent back to the server.
#[derive(Debug, thiserror::Error)]
#[error("{error_type}: {message}")]
pub struct NexusHandlerError {
    /// Nexus error type string; see the Nexus spec for predefined values.
    pub error_type: String,
    /// Human-readable message.
    pub message: String,
    /// Whether the caller should retry. Defaults to `Unspecified` (server decides by type).
    pub retry_behavior: NexusHandlerErrorRetryBehavior,
}

impl NexusHandlerError {
    /// Creates a retryable internal handler error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            error_type: "INTERNAL".to_owned(),
            message: message.into(),
            retry_behavior: NexusHandlerErrorRetryBehavior::Unspecified,
        }
    }

    /// Creates a non-retryable bad-request handler error.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            error_type: "BAD_REQUEST".to_owned(),
            message: message.into(),
            retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable,
        }
    }

    /// Creates a non-retryable not-found handler error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            error_type: "NOT_FOUND".to_owned(),
            message: message.into(),
            retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable,
        }
    }
}

pub(crate) type NexusStartInvocation = Arc<
    dyn Fn(
            NexusStartContext,
            Option<Payload>,
            DataConverter,
        ) -> BoxFuture<'static, nexus_task_completion::Status>
        + Send
        + Sync,
>;

pub(crate) type NexusCancelInvocation = Arc<
    dyn Fn(NexusCancelContext) -> BoxFuture<'static, nexus_task_completion::Status> + Send + Sync,
>;

/// Holds registered Nexus operation handlers, keyed by (service name, operation name).
#[derive(Default, Clone)]
pub struct NexusServiceDefinitions {
    start: HashMap<(String, String), NexusStartInvocation>,
    cancel: HashMap<(String, String), NexusCancelInvocation>,
}

impl NexusServiceDefinitions {
    /// Register a typed start handler for a service operation.
    ///
    /// `I` decodes from a single `Payload`, `O` encodes to one. The handler receives a
    /// [`NexusStartContext`] and the decoded input, returning a sync result or an async token.
    pub fn register_operation<I, O, F, Fut>(
        &mut self,
        service: impl Into<String>,
        operation: impl Into<String>,
        handler: F,
    ) where
        I: TemporalDeserializable + Send + 'static,
        O: TemporalSerializable + Send + Sync + 'static,
        F: Fn(NexusStartContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<NexusOperationResult<O>, NexusHandlerError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let key = (service.into(), operation.into());
        self.start.insert(
            key,
            Arc::new(move |ctx, payload, dc| {
                let handler = handler.clone();
                Box::pin(async move {
                    let input: Result<I, _> = match payload {
                        Some(p) => dc.from_payload(&SerializationContextData::Nexus, p).await,
                        None => {
                            // Try to deserialize from an empty payload vec; works for `()`.
                            dc.from_payloads(&SerializationContextData::Nexus, vec![])
                                .await
                        }
                    };
                    let input = match input {
                        Ok(v) => v,
                        Err(e) => {
                            return handler_error_status(NexusHandlerError::bad_request(format!(
                                "failed to deserialize nexus input: {e}"
                            )));
                        }
                    };
                    match handler(ctx, input).await {
                        Ok(NexusOperationResult::Sync(output)) => {
                            let payload = dc
                                .to_payload(&SerializationContextData::Nexus, &output)
                                .await
                                .ok();
                            nexus_task_completion::Status::Completed(Response {
                                variant: Some(response::Variant::StartOperation(
                                    StartOperationResponse {
                                        variant: Some(
                                            start_operation_response::Variant::SyncSuccess(
                                                start_operation_response::Sync {
                                                    payload,
                                                    links: vec![],
                                                },
                                            ),
                                        ),
                                    },
                                )),
                            })
                        }
                        Ok(NexusOperationResult::Async { operation_token }) => {
                            nexus_task_completion::Status::Completed(Response {
                                variant: Some(response::Variant::StartOperation(
                                    StartOperationResponse {
                                        variant: Some(
                                            start_operation_response::Variant::AsyncSuccess(
                                                start_operation_response::Async {
                                                    operation_token,
                                                    links: vec![],
                                                    ..Default::default()
                                                },
                                            ),
                                        ),
                                    },
                                )),
                            })
                        }
                        Err(e) => handler_error_status(e),
                    }
                })
            }),
        );
    }

    /// Register a custom cancel handler for a service operation.
    ///
    /// Cancellation is otherwise acknowledged with a no-op; register one only for custom logic.
    pub fn register_cancel_handler<F, Fut>(
        &mut self,
        service: impl Into<String>,
        operation: impl Into<String>,
        handler: F,
    ) where
        F: Fn(NexusCancelContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), NexusHandlerError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let key = (service.into(), operation.into());
        self.cancel.insert(
            key,
            Arc::new(move |ctx| {
                let handler = handler.clone();
                Box::pin(async move {
                    match handler(ctx).await {
                        Ok(()) => cancel_ok_status(),
                        Err(e) => handler_error_status(e),
                    }
                })
            }),
        );
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.start.is_empty() && self.cancel.is_empty()
    }

    pub(crate) fn get_start(&self, service: &str, operation: &str) -> Option<NexusStartInvocation> {
        self.start
            .get(&(service.to_owned(), operation.to_owned()))
            .cloned()
    }

    pub(crate) fn get_cancel(
        &self,
        service: &str,
        operation: &str,
    ) -> Option<NexusCancelInvocation> {
        self.cancel
            .get(&(service.to_owned(), operation.to_owned()))
            .cloned()
    }

    pub(crate) fn operation_names(&self) -> Vec<(&str, &str)> {
        let mut names: Vec<_> = self
            .start
            .keys()
            .map(|(svc, op)| (svc.as_str(), op.as_str()))
            .collect();
        names.sort_unstable();
        names
    }
}

impl std::fmt::Debug for NexusServiceDefinitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexusServiceDefinitions")
            .field("operations", &self.operation_names())
            .finish()
    }
}

pub(crate) fn handler_error_status(e: NexusHandlerError) -> nexus_task_completion::Status {
    nexus_task_completion::Status::Failure(Failure {
        message: e.message,
        failure_info: Some(FailureInfo::NexusHandlerFailureInfo(
            NexusHandlerFailureInfo {
                r#type: e.error_type,
                retry_behavior: e.retry_behavior as i32,
            },
        )),
        ..Default::default()
    })
}

pub(crate) fn cancel_ok_status() -> nexus_task_completion::Status {
    nexus_task_completion::Status::Completed(Response {
        variant: Some(response::Variant::CancelOperation(
            CancelOperationResponse {},
        )),
    })
}
