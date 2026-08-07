//! Shared SDK error re-exports.

pub use crate::workflow_registry::WorkflowRegistrationError;
use temporalio_client::PluginApplyError;
use temporalio_sdk_core::{
    CompleteActivityError, CompleteWfError, PollError, WorkerValidationError,
};

/// Errors that can occur while creating a worker.
///
/// **Experimental:** This API may change or be removed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerCreateError {
    /// A plugin failed while configuring worker options.
    #[error(transparent)]
    Plugin(#[from] PluginApplyError),
    /// Worker initialization failed after plugin configuration completed.
    #[error("worker initialization failed: {0}")]
    Initialization(#[source] anyhow::Error),
}

/// Errors that can occur while running a worker.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerRunError {
    /// Worker validation failed before polling began.
    #[error("worker validation failed")]
    Validation(#[source] WorkerValidationError),
    /// Polling for a workflow activation failed.
    #[error("workflow polling failed")]
    WorkflowPoll(#[source] PollError),
    /// A worker interceptor failed while processing a workflow activation.
    #[error("workflow activation interceptor failed")]
    WorkflowActivationInterceptor(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// The SDK failed while processing a workflow activation.
    #[error("workflow activation processing failed")]
    WorkflowActivation(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// A workflow future failed.
    #[error("workflow futures encountered an error")]
    WorkflowFutures(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// The workflow completion processor failed.
    #[error("workflow completions processor encountered an error")]
    WorkflowCompletions(#[source] CompleteWfError),
    /// Polling for an activity task failed.
    #[error("activity polling failed")]
    ActivityPoll(#[source] PollError),
    /// Completing an activity task failed.
    #[error("activity completion failed")]
    ActivityCompletion(#[source] Box<CompleteActivityError>),
    /// The SDK failed while handling an activity task.
    #[error("activity task handling failed")]
    ActivityTask(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}
pub use temporalio_common::error::{
    ActivityExecutionError, ApplicationErrorCategory, ApplicationFailure,
    ChildWorkflowExecutionError, ChildWorkflowStartError, OutgoingActivityError, OutgoingError,
    OutgoingWorkflowError, RetryState, TimeoutType, WorkflowSignalError,
};
