//! User-definable interceptors are defined in this module

use crate::{
    Worker,
    activities::{ActivityContext, ActivityError, ActivityInfo},
};
use anyhow::bail;
use futures_util::{FutureExt, future::BoxFuture};
use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use temporalio_common::{
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, SerializationContext, TemporalSerializable,
    },
    protos::{
        coresdk::{
            workflow_activation::{WorkflowActivation, remove_from_cache::EvictionReason},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::common::v1::Payload,
    },
};

mod activity_execution_value {
    use super::*;

    pub trait Sealed {
        fn to_activity_payload(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Payload, PayloadConversionError>;
    }

    impl<T> Sealed for T
    where
        T: Any + TemporalSerializable + Send + Sync,
    {
        fn to_activity_payload(
            &self,
            context: &SerializationContext<'_>,
        ) -> Result<Payload, PayloadConversionError> {
            context.converter.to_payload(context, self)
        }
    }
}

/// Implementors can intercept certain actions that happen within the Worker.
///
/// Advanced usage only.
#[async_trait::async_trait(?Send)]
pub trait WorkerInterceptor {
    /// Called every time a workflow activation completes (just before sending the completion to
    /// core).
    async fn on_workflow_activation_completion(&self, _completion: &WorkflowActivationCompletion) {}
    /// Called after the worker has initiated shutdown and the workflow/activity polling loops
    /// have exited, but just before waiting for the inner core worker shutdown
    fn on_shutdown(&self, _sdk_worker: &Worker) {}
    /// Called every time a workflow is about to be activated
    async fn on_workflow_activation(
        &self,
        _activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// Activity execution data passed to [`ActivityInboundInterceptor::execute_activity`].
#[non_exhaustive]
pub struct ExecuteActivityInput {
    context: ActivityContext,
    args: Box<dyn Any + Send + Sync>,
}

impl ExecuteActivityInput {
    pub(crate) fn new(context: ActivityContext, args: Box<dyn Any + Send + Sync>) -> Self {
        Self { context, args }
    }

    pub(crate) fn into_parts(self) -> (ActivityContext, Box<dyn Any + Send + Sync>) {
        (self.context, self.args)
    }

    /// Information about the activity execution.
    pub fn activity_info(&self) -> &ActivityInfo {
        self.context.info()
    }

    /// Headers attached to this activity.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        self.context.headers()
    }

    /// Mutably access headers attached to this activity.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        self.context.headers_mut()
    }

    /// Attempt to access the decoded activity arguments as a concrete type.
    pub fn args_ref<T: Any>(&self) -> Option<&T> {
        self.args.downcast_ref()
    }

    /// Attempt to mutably access the decoded activity arguments as a concrete type.
    pub fn args_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.args.downcast_mut()
    }
}

/// Type-erased activity output returned from [`ActivityInboundInterceptorNext::run`].
pub trait ActivityExecutionValue:
    Any + TemporalSerializable + Send + Sync + activity_execution_value::Sealed
{
    /// Access this value as [`Any`] for type-specific inspection.
    fn as_any(&self) -> &dyn Any;
}

impl<T> ActivityExecutionValue for T
where
    T: Any + TemporalSerializable + Send + Sync,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl dyn ActivityExecutionValue {
    /// Attempt to access the activity output as a concrete type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }

    pub(crate) fn serialize_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        self.to_activity_payload(context)
    }
}

/// Output of an activity execution returned from [`ActivityInboundInterceptorNext::run`].
pub type ExecuteActivityOutput = Result<Box<dyn ActivityExecutionValue>, ActivityError>;

/// The next activity execution step in an interceptor chain.
pub struct ActivityInboundInterceptorNext<'a> {
    run: Box<dyn FnOnce(ExecuteActivityInput) -> BoxFuture<'a, ExecuteActivityOutput> + Send + 'a>,
}

impl<'a> ActivityInboundInterceptorNext<'a> {
    pub(crate) fn new(
        run: impl FnOnce(ExecuteActivityInput) -> BoxFuture<'a, ExecuteActivityOutput> + Send + 'a,
    ) -> Self {
        Self { run: Box::new(run) }
    }

    /// Run the next interceptor or the activity implementation.
    pub fn run(self, input: ExecuteActivityInput) -> BoxFuture<'a, ExecuteActivityOutput> {
        (self.run)(input)
    }
}

/// Implementors can intercept activity execution.
///
/// Advanced usage only.
pub trait ActivityInboundInterceptor: Send + Sync {
    /// Wrap activity execution.
    fn execute_activity<'a, 'b>(
        &'a self,
        input: ExecuteActivityInput,
        next: ActivityInboundInterceptorNext<'b>,
    ) -> BoxFuture<'a, ExecuteActivityOutput>
    where
        'b: 'a,
    {
        async move { next.run(input).await }.boxed()
    }
}

/// Supports the composition of interceptors
pub struct InterceptorWithNext {
    inner: Box<dyn WorkerInterceptor>,
    next: Option<Box<InterceptorWithNext>>,
}

impl InterceptorWithNext {
    /// Create from an existing interceptor, can be used to initialize a chain of interceptors
    pub fn new(inner: Box<dyn WorkerInterceptor>) -> Self {
        Self { inner, next: None }
    }

    /// Sets the next interceptor, and then returns that interceptor, wrapped by
    /// [InterceptorWithNext]. You can keep calling this method on it to extend the chain.
    pub fn set_next(&mut self, next: Box<dyn WorkerInterceptor>) -> &mut InterceptorWithNext {
        self.next.insert(Box::new(Self::new(next)))
    }
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for InterceptorWithNext {
    async fn on_workflow_activation_completion(&self, c: &WorkflowActivationCompletion) {
        self.inner.on_workflow_activation_completion(c).await;
        if let Some(next) = &self.next {
            next.on_workflow_activation_completion(c).await;
        }
    }

    fn on_shutdown(&self, w: &Worker) {
        self.inner.on_shutdown(w);
        if let Some(next) = &self.next {
            next.on_shutdown(w);
        }
    }

    async fn on_workflow_activation(&self, a: &WorkflowActivation) -> Result<(), anyhow::Error> {
        self.inner.on_workflow_activation(a).await?;
        if let Some(next) = &self.next {
            next.on_workflow_activation(a).await?;
        }
        Ok(())
    }
}

/// Supports the composition of activity inbound interceptors.
pub struct ActivityInboundInterceptorWithNext {
    inner: Box<dyn ActivityInboundInterceptor>,
    next: Option<Box<ActivityInboundInterceptorWithNext>>,
}

impl ActivityInboundInterceptorWithNext {
    /// Create from an existing interceptor, can be used to initialize a chain of interceptors.
    pub fn new(inner: Box<dyn ActivityInboundInterceptor>) -> Self {
        Self { inner, next: None }
    }

    /// Sets the next interceptor, and then returns that interceptor, wrapped by
    /// [ActivityInboundInterceptorWithNext]. You can keep calling this method on it to extend the chain.
    pub fn set_next(&mut self, next: Box<dyn ActivityInboundInterceptor>) -> &mut Self {
        self.next.insert(Box::new(Self::new(next)))
    }
}

impl ActivityInboundInterceptor for ActivityInboundInterceptorWithNext {
    fn execute_activity<'a, 'b>(
        &'a self,
        input: ExecuteActivityInput,
        next: ActivityInboundInterceptorNext<'b>,
    ) -> BoxFuture<'a, ExecuteActivityOutput>
    where
        'b: 'a,
    {
        let chain_next = ActivityInboundInterceptorNext::new(move |input| {
            Box::pin(async move {
                match self.next.as_deref() {
                    Some(next_interceptor) => next_interceptor.execute_activity(input, next).await,
                    None => next.run(input).await,
                }
            })
        });
        self.inner.execute_activity(input, chain_next)
    }
}

/// An interceptor which causes the worker's run function to exit early if nondeterminism errors are
/// encountered
pub struct FailOnNondeterminismInterceptor {}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for FailOnNondeterminismInterceptor {
    async fn on_workflow_activation(
        &self,
        activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        if matches!(
            activation.eviction_reason(),
            Some(EvictionReason::Nondeterminism)
        ) {
            bail!("Workflow is being evicted because of nondeterminism! {activation}");
        }
        Ok(())
    }
}

/// An interceptor that allows you to fetch the exit value of the workflow if and when it is set
#[derive(Default)]
pub struct ReturnWorkflowExitValueInterceptor {
    result_value: Arc<OnceLock<Payload>>,
}

impl ReturnWorkflowExitValueInterceptor {
    /// Can be used to fetch the workflow result if/when it is determined
    pub fn result_handle(&self) -> Arc<OnceLock<Payload>> {
        self.result_value.clone()
    }
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for ReturnWorkflowExitValueInterceptor {
    async fn on_workflow_activation_completion(&self, c: &WorkflowActivationCompletion) {
        if let Some(v) = c.complete_workflow_execution_value() {
            let _ = self.result_value.set(v.clone());
        }
    }
}
