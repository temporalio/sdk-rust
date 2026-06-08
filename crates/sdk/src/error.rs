//! Shared SDK error re-exports.

pub use crate::workflow_registry::WorkflowRegistrationError;
pub use temporalio_common::error::{
    ActivityExecutionError, ApplicationErrorCategory, ApplicationFailure,
    ChildWorkflowExecutionError, ChildWorkflowSignalError, ChildWorkflowStartError,
    OutgoingActivityError, OutgoingError, OutgoingWorkflowError,
};
