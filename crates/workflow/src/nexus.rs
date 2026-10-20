//! Type-safe Nexus service calls from workflows.
//!
//! **Experimental:** Enable the `experimental` feature. This API and the draft Nexus SDK it
//! depends on may change.
//!
//! Calls return [`NexusOperationError`]. Its [`Failed`](NexusOperationError::Failed) variant retains
//! a [`NexusOperationFailureError`] with optional Nexus metadata and a normalized failure reason.
//! Use [`NexusOperationError::as_cancelled`] and [`NexusOperationError::as_timeout`]
//! to inspect cancellation and timeout causes.
//!
//! Services are defined with the Nexus SDK's [`nexus_rpc::service`] macro. Operation markers bind each
//! operation to its service, input, and output types. Calls use the workflow's payload converter;
//! payload codecs are applied at the worker boundary, just as for activities.
//!
//! ```
//! use nexus_rpc::service;
//! use temporalio_workflow::{
//!     WorkflowContext, WorkflowResult,
//!     nexus::{NexusWorkflowClientOptions, NexusWorkflowOperationOptions},
//! };
//!
//! #[service(name = "payments")]
//! pub trait PaymentService {
//!     #[operation(name = "charge")]
//!     fn charge(input: String) -> String;
//! }
//!
//! async fn charge<W>(ctx: &WorkflowContext<W>, input: String) -> WorkflowResult<String> {
//!     let payments = ctx.nexus_client(
//!         PaymentService,
//!         NexusWorkflowClientOptions::new("payments-endpoint").build(),
//!     );
//!     let receipt: String = payments
//!         .execute(
//!             PaymentService::charge,
//!             input,
//!             NexusWorkflowOperationOptions::default(),
//!         )
//!         .await?;
//!
//!     let handle = payments
//!         .start(
//!             PaymentService::charge,
//!             receipt,
//!             NexusWorkflowOperationOptions::default(),
//!         )
//!         .await?;
//!     handle.cancel();
//!     Ok(handle.result().await?)
//! }
//! # fn main() {}
//! ```

pub use crate::NexusOperationCancellationType;
pub use nexus_rpc::{NexusOperation, NexusService};
pub use temporalio_common_wasm::error::{NexusOperationError, NexusOperationFailureError};

use crate::{
    BaseWorkflowContext, EventGroup, NexusOperationOptions, StartedNexusOperation,
    WorkflowCancellationToken,
    workflow_interceptors::{CancellableWorkflowOutboundFuture, WorkflowCancellationHandle},
};
use std::{future, marker::PhantomData, time::Duration};
use temporalio_common_wasm::{
    data_converters::{
        GenericPayloadConverter, NexusOperationDecodeHint, PayloadConversionError,
        SerializationContext, SerializationContextData, TemporalDeserializable,
        TemporalSerializable, WorkflowSerializationContext,
    },
    protos::coresdk::nexus::nexus_operation_result,
};

#[cfg(test)]
mod tests;

/// Options for a workflow client bound to one Nexus service.
#[derive(Debug, Clone, bon::Builder)]
#[builder(start_fn = new, on(String, into))]
#[non_exhaustive]
pub struct NexusWorkflowClientOptions {
    /// Endpoint name registered with the Temporal server.
    #[builder(start_fn)]
    pub endpoint: String,
}

/// Options for an individual Nexus operation called from a workflow.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct NexusWorkflowOperationOptions {
    /// Maximum time from scheduling to completion, including server retries.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Maximum time from scheduling until the handler starts or synchronously completes.
    /// `None` or zero disables this timeout.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum time for an asynchronous operation to complete after starting.
    /// Ignored for synchronous operations; `None` or zero disables this timeout.
    pub start_to_close_timeout: Option<Duration>,
    /// When cancellation is reported. Defaults to waiting for cancellation completion.
    pub cancellation_type: Option<NexusOperationCancellationType>,
    /// Cancellation token for this operation. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
    /// Event Groups to attach in addition to those on the enclosing workflow context.
    #[builder(default)]
    pub event_groups: Vec<EventGroup>,
}

/// A workflow-side Nexus client bound to a service and endpoint.
///
/// Obtain this from [`crate::WorkflowContext::nexus_client`] or
/// [`crate::SyncWorkflowContext::nexus_client`]. It can only call operations declared on `S`.
///
/// An operation from a different service is rejected at compile time:
///
/// ```compile_fail
/// use nexus_rpc::service;
/// use temporalio_workflow::{WorkflowContext, nexus::*};
/// #[service]
/// trait Payments {
///     #[operation]
///     fn charge(input: String) -> String;
/// }
/// #[service]
/// trait Shipping {
///     #[operation]
///     fn ship(input: String) -> String;
/// }
/// async fn wrong_service<W>(ctx: &WorkflowContext<W>) {
///     let payments = ctx.nexus_client(Payments, NexusWorkflowClientOptions::new("payments").build());
///     payments.execute(Shipping::ship, String::new(), Default::default()).await;
/// }
/// # fn main() {}
/// ```
///
/// The operation also determines the input and output types:
///
/// ```compile_fail
/// use nexus_rpc::service;
/// use temporalio_workflow::{WorkflowContext, nexus::*};
/// #[service]
/// trait Payments {
///     #[operation]
///     fn charge(input: String) -> String;
/// }
/// async fn wrong_input<W>(ctx: &WorkflowContext<W>) {
///     let payments = ctx.nexus_client(Payments, NexusWorkflowClientOptions::new("payments").build());
///     payments.execute(Payments::charge, 42_u32, Default::default()).await;
/// }
/// # fn main() {}
/// ```
///
/// ```compile_fail
/// use nexus_rpc::service;
/// use temporalio_workflow::{WorkflowContext, nexus::*};
/// #[service]
/// trait Payments {
///     #[operation]
///     fn charge(input: String) -> String;
/// }
/// async fn wrong_output<W>(ctx: &WorkflowContext<W>) {
///     let payments = ctx.nexus_client(Payments, NexusWorkflowClientOptions::new("payments").build());
///     let _: u32 = payments.execute(Payments::charge, String::new(), Default::default()).await.unwrap();
/// }
/// # fn main() {}
/// ```
pub struct NexusWorkflowClient<S> {
    context: BaseWorkflowContext,
    options: NexusWorkflowClientOptions,
    service: PhantomData<fn() -> S>,
}

impl<S> Clone for NexusWorkflowClient<S> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            options: self.options.clone(),
            service: PhantomData,
        }
    }
}

impl<S: NexusService> NexusWorkflowClient<S> {
    pub(crate) fn new(
        context: BaseWorkflowContext,
        _service: S,
        options: NexusWorkflowClientOptions,
    ) -> Self {
        Self {
            context,
            options,
            service: PhantomData,
        }
    }

    /// Start an operation and wait for the handler to acknowledge it.
    ///
    /// Synchronous operations return a handle with no operation token. The operation is scheduled
    /// when this method is called, as with activities. Dropping the future or handle does not cancel
    /// it; use [`crate::CancellableFuture::cancel`] while starting, the options' cancellation token,
    /// or [`NexusWorkflowOperationHandle::cancel`] after starting.
    pub fn start<O>(
        &self,
        operation: O,
        input: O::Input,
        options: NexusWorkflowOperationOptions,
    ) -> CancellableWorkflowOutboundFuture<
        Result<NexusWorkflowOperationHandle<O::Output>, NexusOperationError>,
    >
    where
        O: NexusOperation<Service = S>,
        O::Input: TemporalSerializable,
        O::Output: TemporalDeserializable,
    {
        let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
        let converter = self.context.payload_converter();
        let payload = match converter
            .to_payload(&SerializationContext::new(&context_data, converter), &input)
        {
            Ok(payload) => payload,
            Err(error) => {
                return CancellableWorkflowOutboundFuture::new(
                    future::ready(Err(error.into())),
                    WorkflowCancellationHandle::noop(),
                );
            }
        };
        self.context
            .start_nexus_operation(
                NexusOperationOptions::builder()
                    .endpoint(self.options.endpoint.clone())
                    .service(S::name())
                    .operation(operation.name())
                    .input(payload)
                    .maybe_schedule_to_close_timeout(options.schedule_to_close_timeout)
                    .maybe_schedule_to_start_timeout(options.schedule_to_start_timeout)
                    .maybe_start_to_close_timeout(options.start_to_close_timeout)
                    .maybe_cancellation_type(options.cancellation_type)
                    .maybe_cancellation_token(options.cancellation_token)
                    .event_groups(options.event_groups)
                    .build(),
            )
            .map(move |result| {
                result.map(|started| NexusWorkflowOperationHandle {
                    started,
                    output: PhantomData,
                })
            })
    }

    /// Start an operation and wait for its typed result.
    ///
    /// Use [`Self::start`] if you need the operation token or explicit cancellation handle.
    pub fn execute<O>(
        &self,
        operation: O,
        input: O::Input,
        options: NexusWorkflowOperationOptions,
    ) -> CancellableWorkflowOutboundFuture<Result<O::Output, NexusOperationError>>
    where
        O: NexusOperation<Service = S>,
        O::Input: TemporalSerializable,
        O::Output: TemporalDeserializable,
    {
        let start = self.start(operation, input, options);
        let cancellation = start.cancellation_handle();
        CancellableWorkflowOutboundFuture::new(
            async move { start.await?.result().await },
            cancellation,
        )
    }
}

/// A started Nexus operation whose result has type `Output`.
pub struct NexusWorkflowOperationHandle<Output> {
    started: StartedNexusOperation,
    output: PhantomData<fn() -> Output>,
}

impl<Output> NexusWorkflowOperationHandle<Output> {
    /// The token returned by an asynchronous handler, or `None` for a synchronous operation.
    pub fn operation_token(&self) -> Option<&str> {
        self.started.operation_token.as_deref()
    }

    /// Request cancellation according to the operation's cancellation type.
    ///
    /// This does not guarantee cancellation: completion may race with the request.
    pub fn cancel(&self) {
        self.started.cancel();
    }
}

impl<Output: TemporalDeserializable + 'static> NexusWorkflowOperationHandle<Output> {
    /// Wait for the operation's result, converting success and failure with the workflow's
    /// configured converters. This may be called more than once.
    pub async fn result(&self) -> Result<Output, NexusOperationError> {
        let result = self.started.result().await;
        let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
        let converter = self.started.base_ctx.data_converter();
        match result.status {
            Some(nexus_operation_result::Status::Completed(success)) => {
                let payload_converter = converter.payload_converter();
                Ok(payload_converter.from_payload(
                    &SerializationContext::new(&context_data, payload_converter),
                    success,
                )?)
            }
            Some(
                nexus_operation_result::Status::Failed(failure)
                | nexus_operation_result::Status::Cancelled(failure)
                | nexus_operation_result::Status::TimedOut(failure),
            ) => Err(converter.to_error(&context_data, failure, NexusOperationDecodeHint)?),
            None => Err(PayloadConversionError::EncodingError(
                std::io::Error::other("Nexus operation completed without a status").into(),
            )
            .into()),
        }
    }
}
