//! Shared SDK error re-exports.

pub use crate::workflow_registry::WorkflowRegistrationError;
#[cfg(feature = "experimental")]
use temporalio_client::PluginApplyError;
use temporalio_sdk_core::WorkerValidationError as CoreWorkerValidationError;

/// Errors that can occur while creating an SDK runtime.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeError {
    /// Runtime initialization failed.
    #[error("runtime initialization failed: {0}")]
    Initialization(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// No Tokio runtime is active on the current thread.
    #[error("no Tokio runtime is active on the current thread")]
    NoCurrentTokioRuntime,
}

impl RuntimeError {
    pub(crate) fn from_core(error: anyhow::Error) -> Self {
        Self::Initialization(error.into_boxed_dyn_error())
    }
}

/// Errors encountered while validating a worker before polling begins.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerValidationError {
    /// The configured namespace could not be described.
    #[error("namespace {namespace} was not found or otherwise could not be described: {source}")]
    NamespaceDescribeError {
        /// The underlying server error.
        #[source]
        source: temporalio_client::tonic::Status,
        /// The namespace that could not be described.
        namespace: String,
    },
}

impl WorkerValidationError {
    pub(crate) fn from_core(error: CoreWorkerValidationError) -> Self {
        match error {
            CoreWorkerValidationError::NamespaceDescribeError { source, namespace } => {
                Self::NamespaceDescribeError { source, namespace }
            }
        }
    }
}

/// Errors that can occur while creating a worker.
///
/// **Experimental:** This API may change or be removed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerCreateError {
    /// A plugin failed while configuring worker options.
    #[cfg(feature = "experimental")]
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
    #[error("worker validation failed: {0}")]
    Validation(#[source] WorkerValidationError),
    /// An unrecoverable error occurred while running the worker.
    #[error("{message}")]
    Fatal {
        /// Worker operation that could not continue.
        message: String,
        /// Underlying cause.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub use temporalio_common::error::{
    ActivityExecutionError, ApplicationErrorCategory, ApplicationFailure,
    CancelExternalWorkflowError, ChildWorkflowExecutionError, ChildWorkflowStartError,
    OutgoingActivityError, OutgoingError, OutgoingWorkflowError, RetryState, TimeoutType,
    WorkflowSignalError,
};
