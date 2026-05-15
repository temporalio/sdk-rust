//! User-definable interceptors are defined in this module

use crate::{
    Worker,
    activities::{ActivityContext, ActivityError},
};
use anyhow::bail;
use futures_util::future::BoxFuture;
use std::{
    any::Any,
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

/// Activity execution data passed to [`ActivityInterceptor::execute_activity`].
#[non_exhaustive]
pub struct ActivityExecutionInput {
    context: ActivityContext,
    input: Box<dyn Any + Send + Sync>,
}

impl ActivityExecutionInput {
    pub(crate) fn new(context: ActivityContext, input: Box<dyn Any + Send + Sync>) -> Self {
        Self { context, input }
    }

    pub(crate) fn into_parts(self) -> (ActivityContext, Box<dyn Any + Send + Sync>) {
        (self.context, self.input)
    }

    /// Context for the activity execution.
    pub fn context(&self) -> &ActivityContext {
        &self.context
    }

    /// Attempt to access the decoded activity input as a concrete type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.input.downcast_ref()
    }
}

/// Type-erased activity output returned from [`ActivityExecutionNext::run`].
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

/// Output of an activity execution returned from [`ActivityExecutionNext::run`].
pub type ActivityExecutionOutput = Result<Box<dyn ActivityExecutionValue>, ActivityError>;

/// The next activity execution step in an interceptor chain.
pub struct ActivityExecutionNext<'a> {
    run: Box<
        dyn FnOnce(ActivityExecutionInput) -> BoxFuture<'a, ActivityExecutionOutput> + Send + 'a,
    >,
}

impl<'a> ActivityExecutionNext<'a> {
    pub(crate) fn new(
        run: impl FnOnce(ActivityExecutionInput) -> BoxFuture<'a, ActivityExecutionOutput> + Send + 'a,
    ) -> Self {
        Self { run: Box::new(run) }
    }

    /// Run the next interceptor or the activity implementation.
    pub async fn run(self, input: ActivityExecutionInput) -> ActivityExecutionOutput {
        (self.run)(input).await
    }
}

/// Implementors can intercept activity execution.
///
/// Advanced usage only.
#[async_trait::async_trait]
pub trait ActivityInterceptor: Send + Sync {
    /// Wrap activity execution.
    async fn execute_activity(
        &self,
        input: ActivityExecutionInput,
        next: ActivityExecutionNext<'_>,
    ) -> ActivityExecutionOutput {
        next.run(input).await
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

/// Supports the composition of activity interceptors.
pub struct ActivityInterceptorWithNext {
    inner: Box<dyn ActivityInterceptor>,
    next: Option<Box<ActivityInterceptorWithNext>>,
}

impl ActivityInterceptorWithNext {
    /// Create from an existing interceptor, can be used to initialize a chain of interceptors.
    pub fn new(inner: Box<dyn ActivityInterceptor>) -> Self {
        Self { inner, next: None }
    }

    /// Sets the next interceptor, and then returns that interceptor, wrapped by
    /// [ActivityInterceptorWithNext]. You can keep calling this method on it to extend the chain.
    pub fn set_next(&mut self, next: Box<dyn ActivityInterceptor>) -> &mut Self {
        self.next.insert(Box::new(Self::new(next)))
    }
}

#[async_trait::async_trait]
impl ActivityInterceptor for ActivityInterceptorWithNext {
    async fn execute_activity(
        &self,
        input: ActivityExecutionInput,
        next: ActivityExecutionNext<'_>,
    ) -> ActivityExecutionOutput {
        let chain_next = ActivityExecutionNext::new(move |input| {
            Box::pin(async move {
                match self.next.as_deref() {
                    Some(next_interceptor) => next_interceptor.execute_activity(input, next).await,
                    None => next.run(input).await,
                }
            })
        });
        self.inner.execute_activity(input, chain_next).await
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
