//! Shared SDK error re-exports.

pub use crate::workflow_registry::WorkflowRegistrationError;
use temporalio_client::PluginApplyError;

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
pub use temporalio_common::error::{
    ActivityExecutionError, ApplicationErrorCategory, ApplicationFailure,
    ChildWorkflowExecutionError, ChildWorkflowStartError, OutgoingActivityError, OutgoingError,
    OutgoingWorkflowError, RetryState, TimeoutType, WorkflowSignalError,
};
