//! Workflow interceptor APIs.

use crate::{
    BaseWorkflowContext,
    runtime::{
        entry::WorkflowError,
        model::{WorkflowResult, WorkflowTermination},
    },
};
use futures_util::{FutureExt, future::LocalBoxFuture};
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use temporalio_common_wasm::{
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData, TemporalSerializable,
    },
    protos::temporal::api::common::v1::Payload,
    search_attributes::SearchAttributes,
};

mod workflow_output_value {
    use super::*;

    pub trait Sealed {
        fn to_workflow_payload(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Payload, PayloadConversionError>;
    }

    impl<T> Sealed for T
    where
        T: Any + TemporalSerializable,
    {
        fn to_workflow_payload(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Payload, PayloadConversionError> {
            context.converter.to_payload(context, self)
        }
    }
}

/// Type-erased workflow output carried through the workflow interceptor chain.
pub trait WorkflowOutputValue: Any + TemporalSerializable + workflow_output_value::Sealed {
    /// Access this value as [`Any`] for type-specific inspection.
    fn as_any(&self) -> &dyn Any;
}

impl<T> WorkflowOutputValue for T
where
    T: Any + TemporalSerializable,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl dyn WorkflowOutputValue {
    /// Attempt to access the workflow output as a concrete type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }

    pub(crate) fn serialize_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        self.to_workflow_payload(context)
    }
}

pub(crate) fn serialize_workflow_output(
    output: &dyn WorkflowOutputValue,
    converter: &PayloadConverter,
) -> Result<Payload, PayloadConversionError> {
    let ctx = SerializationContext {
        data: &SerializationContextData::Workflow,
        converter,
    };
    output.serialize_payload(&ctx)
}

/// Result of an intercepted workflow execution.
pub type ExecuteWorkflowResult = WorkflowResult<Box<dyn WorkflowOutputValue>>;

/// Result of an intercepted signal handler.
pub type HandleSignalResult = Result<(), WorkflowError>;

/// Result of an intercepted update handler.
pub type HandleUpdateResult = Result<Box<dyn WorkflowOutputValue>, WorkflowError>;

/// Result of an intercepted query handler.
pub type HandleQueryResult = Result<Box<dyn WorkflowOutputValue>, WorkflowError>;

/// Result of an intercepted update validator.
pub type ValidateUpdateResult = Result<(), WorkflowError>;

/// Future produced by workflow interceptors.
///
/// The SDK polls a newly created interceptor future once while processing its activation. This
/// runs synchronous interceptor and synchronous handler work through the first genuine
/// [`Poll::Pending`](std::task::Poll::Pending). Async workflow and handler bodies are not entered
/// until normal routine polling. Awaiting a pending workflow future before calling
/// [`WorkflowNext::run`] intentionally delays the underlying handler.
///
/// This type identifies futures that are polled inside workflow execution. It does not make
/// arbitrary Rust futures deterministic. Interceptor implementations must only await workflow
/// scheduler primitives or SDK-provided workflow futures.
pub struct WorkflowInterceptorFuture<'a, T>(LocalBoxFuture<'a, T>);

impl<'a, T> WorkflowInterceptorFuture<'a, T> {
    /// Create a workflow interceptor future from a local future.
    pub fn new(fut: impl Future<Output = T> + 'a) -> Self {
        Self(fut.boxed_local())
    }
}

impl<'a, T> Unpin for WorkflowInterceptorFuture<'a, T> {}

impl<T> Future for WorkflowInterceptorFuture<'_, T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

/// Continuation for a workflow interceptor operation.
pub struct WorkflowNext<'a, I, O> {
    inner: Box<dyn FnOnce(I) -> O + 'a>,
}

impl<'a, I, O> WorkflowNext<'a, I, O> {
    pub(crate) fn new(f: impl FnOnce(I) -> O + 'a) -> Self {
        Self { inner: Box::new(f) }
    }

    /// Continue the call chain with the provided input.
    pub fn run(self, input: I) -> O {
        (self.inner)(input)
    }
}

/// Workflow execution context available to async-capable inbound interceptors.
#[derive(Clone)]
pub struct WorkflowInterceptorContext {
    base: BaseWorkflowContext,
}

impl WorkflowInterceptorContext {
    pub(crate) fn new(base: BaseWorkflowContext) -> Self {
        Self { base }
    }

    /// Return the workflow's unique identifier.
    pub fn workflow_id(&self) -> &str {
        self.base.workflow_id()
    }

    /// Return the run id of this workflow execution.
    pub fn run_id(&self) -> &str {
        self.base.run_id()
    }

    /// Return the namespace the workflow is executing in.
    pub fn namespace(&self) -> &str {
        self.base.namespace()
    }

    /// Return the task queue the workflow is executing in.
    pub fn task_queue(&self) -> &str {
        self.base.task_queue()
    }

    /// Return the workflow type name.
    pub fn workflow_type(&self) -> &str {
        self.base.workflow_type()
    }

    /// Return the current time according to the workflow.
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.base.workflow_time()
    }

    /// Return the length of history so far at this point in the workflow.
    pub fn history_length(&self) -> u32 {
        self.base.history_length()
    }

    /// Return current values for workflow search attributes.
    pub fn search_attributes(&self) -> SearchAttributes {
        self.base.search_attributes()
    }

    /// Returns true if the current workflow task is happening under replay.
    pub fn is_replaying(&self) -> bool {
        self.base.is_replaying()
    }

    /// Returns true if the current work is replaying history events.
    pub fn is_replaying_history_events(&self) -> bool {
        self.base.is_replaying_history_events()
    }

    /// Returns the payload converter used by the worker running this workflow.
    pub fn payload_converter(&self) -> &PayloadConverter {
        self.base.payload_converter()
    }
}

/// Workflow execution context available to sync-only inbound interceptors.
#[derive(Clone)]
pub struct SyncWorkflowInterceptorContext {
    base: BaseWorkflowContext,
}

impl SyncWorkflowInterceptorContext {
    pub(crate) fn new(base: BaseWorkflowContext) -> Self {
        Self { base }
    }

    /// Return the workflow's unique identifier.
    pub fn workflow_id(&self) -> &str {
        self.base.workflow_id()
    }

    /// Return the run id of this workflow execution.
    pub fn run_id(&self) -> &str {
        self.base.run_id()
    }

    /// Return the namespace the workflow is executing in.
    pub fn namespace(&self) -> &str {
        self.base.namespace()
    }

    /// Return the task queue the workflow is executing in.
    pub fn task_queue(&self) -> &str {
        self.base.task_queue()
    }

    /// Return the workflow type name.
    pub fn workflow_type(&self) -> &str {
        self.base.workflow_type()
    }

    /// Return the current time according to the workflow.
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.base.workflow_time()
    }

    /// Return the length of history so far at this point in the workflow.
    pub fn history_length(&self) -> u32 {
        self.base.history_length()
    }

    /// Return current values for workflow search attributes.
    pub fn search_attributes(&self) -> SearchAttributes {
        self.base.search_attributes()
    }

    /// Returns true if the current workflow task is happening under replay.
    pub fn is_replaying(&self) -> bool {
        self.base.is_replaying()
    }

    /// Returns true if the current work is replaying history events.
    pub fn is_replaying_history_events(&self) -> bool {
        self.base.is_replaying_history_events()
    }

    /// Returns the payload converter used by the worker running this workflow.
    pub fn payload_converter(&self) -> &PayloadConverter {
        self.base.payload_converter()
    }
}

struct DecodedInput {
    value: Option<Box<dyn Any>>,
    headers: HashMap<String, Payload>,
}

impl DecodedInput {
    fn new(value: Option<Box<dyn Any>>, headers: HashMap<String, Payload>) -> Self {
        Self { value, headers }
    }

    fn input_ref<T: Any>(&self) -> Option<&T> {
        self.value.as_ref()?.downcast_ref()
    }

    fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.value.as_mut()?.downcast_mut()
    }

    fn headers(&self) -> &HashMap<String, Payload> {
        &self.headers
    }

    fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        &mut self.headers
    }

    fn into_parts(self) -> (Option<Box<dyn Any>>, HashMap<String, Payload>) {
        (self.value, self.headers)
    }
}

/// Input passed to [`WorkflowInboundInterceptor::execute`].
#[non_exhaustive]
pub struct ExecuteWorkflowInput {
    decoded: DecodedInput,
}

impl ExecuteWorkflowInput {
    pub(crate) fn new(value: Option<Box<dyn Any>>, headers: HashMap<String, Payload>) -> Self {
        Self {
            decoded: DecodedInput::new(value, headers),
        }
    }

    pub(crate) fn into_parts(self) -> (Option<Box<dyn Any>>, HashMap<String, Payload>) {
        self.decoded.into_parts()
    }

    /// Attempt to access the decoded workflow input as a concrete type.
    pub fn input_ref<T: Any>(&self) -> Option<&T> {
        self.decoded.input_ref()
    }

    /// Attempt to mutably access the decoded workflow input as a concrete type.
    pub fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.decoded.input_mut()
    }

    /// Headers attached to the workflow execution.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        self.decoded.headers()
    }

    /// Mutably access headers attached to the workflow execution.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        self.decoded.headers_mut()
    }
}

macro_rules! handler_input {
    ($name:ident, $doc:literal, $field:ident, $field_doc:literal) => {
        #[doc = $doc]
        #[non_exhaustive]
        pub struct $name {
            $field: String,
            decoded: DecodedInput,
        }

        impl $name {
            pub(crate) fn new(
                $field: String,
                value: Box<dyn Any>,
                headers: HashMap<String, Payload>,
            ) -> Self {
                Self {
                    $field,
                    decoded: DecodedInput::new(Some(value), headers),
                }
            }

            pub(crate) fn into_parts(self) -> (String, Box<dyn Any>, HashMap<String, Payload>) {
                let (value, headers) = self.decoded.into_parts();
                (
                    self.$field,
                    value.expect("handler input must exist after typed decode"),
                    headers,
                )
            }

            #[doc = $field_doc]
            pub fn name(&self) -> &str {
                &self.$field
            }

            /// Attempt to access the decoded input as a concrete type.
            pub fn input_ref<T: Any>(&self) -> Option<&T> {
                self.decoded.input_ref()
            }

            /// Attempt to mutably access the decoded input as a concrete type.
            pub fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
                self.decoded.input_mut()
            }

            /// Headers attached to this handler invocation.
            pub fn headers(&self) -> &HashMap<String, Payload> {
                self.decoded.headers()
            }

            /// Mutably access headers attached to this handler invocation.
            pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
                self.decoded.headers_mut()
            }
        }
    };
}

handler_input!(
    HandleSignalInput,
    "Input passed to [`WorkflowInboundInterceptor::handle_signal`].",
    signal_name,
    "Return the signal name."
);

handler_input!(
    HandleUpdateInput,
    "Input passed to [`WorkflowInboundInterceptor::handle_update`].",
    update_name,
    "Return the update name."
);

handler_input!(
    HandleQueryInput,
    "Input passed to [`WorkflowInboundInterceptor::handle_query`].",
    query_name,
    "Return the query name."
);

/// Input passed to [`WorkflowInboundInterceptor::validate_update`].
#[non_exhaustive]
pub struct ValidateUpdateInput {
    update_name: String,
    decoded: DecodedInput,
}

impl ValidateUpdateInput {
    pub(crate) fn new(
        update_name: String,
        value: Box<dyn Any>,
        headers: HashMap<String, Payload>,
    ) -> Self {
        Self {
            update_name,
            decoded: DecodedInput::new(Some(value), headers),
        }
    }

    pub(crate) fn into_parts(self) -> (String, Box<dyn Any>, HashMap<String, Payload>) {
        let (value, headers) = self.decoded.into_parts();
        (
            self.update_name,
            value.expect("update validation input must exist after typed decode"),
            headers,
        )
    }

    /// Return the update name.
    pub fn name(&self) -> &str {
        &self.update_name
    }

    /// Attempt to access the decoded input as a concrete type.
    pub fn input_ref<T: Any>(&self) -> Option<&T> {
        self.decoded.input_ref()
    }

    /// Attempt to mutably access the decoded input as a concrete type.
    pub fn input_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.decoded.input_mut()
    }

    /// Headers attached to this update invocation.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        self.decoded.headers()
    }

    /// Mutably access headers attached to this update invocation.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        self.decoded.headers_mut()
    }
}

/// Inbound interceptor for workflow execution and message handlers.
pub trait WorkflowInboundInterceptor: Send + Sync + 'static {
    /// Called to invoke the workflow run method.
    fn execute<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        next.run(input)
    }

    /// Called to invoke a signal handler.
    fn handle_signal<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: HandleSignalInput,
        next: WorkflowNext<
            'a,
            HandleSignalInput,
            WorkflowInterceptorFuture<'a, HandleSignalResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
        next.run(input)
    }

    /// Called to invoke an update handler.
    fn handle_update<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: HandleUpdateInput,
        next: WorkflowNext<
            'a,
            HandleUpdateInput,
            WorkflowInterceptorFuture<'a, HandleUpdateResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleUpdateResult> {
        next.run(input)
    }

    /// Called to invoke a query handler.
    fn handle_query(
        &self,
        _ctx: SyncWorkflowInterceptorContext,
        input: HandleQueryInput,
        next: WorkflowNext<'_, HandleQueryInput, HandleQueryResult>,
    ) -> HandleQueryResult {
        next.run(input)
    }

    /// Called to validate an update.
    fn validate_update(
        &self,
        _ctx: SyncWorkflowInterceptorContext,
        input: ValidateUpdateInput,
        next: WorkflowNext<'_, ValidateUpdateInput, ValidateUpdateResult>,
    ) -> ValidateUpdateResult {
        next.run(input)
    }
}

pub(crate) fn wrong_workflow_input_type(type_name: &'static str) -> WorkflowTermination {
    WorkflowTermination::failed_application(temporalio_common_wasm::error::ApplicationFailure::new(
        anyhow::anyhow!(
            "Workflow inbound interceptor returned arguments with wrong concrete type for workflow {type_name}"
        ),
    ))
}

#[allow(dead_code)]
struct AssertNoSendSyncBounds(PhantomData<*const ()>);
