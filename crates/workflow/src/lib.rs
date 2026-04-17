#![warn(missing_docs)]

//! Temporal workflow authoring APIs and runtime glue.

extern crate self as temporalio_workflow;

pub use temporalio_common_wasm as common;

pub mod runtime;
mod workflow_context;
pub mod workflows;

#[macro_export]
#[doc(hidden)]
macro_rules! __temporal_select {
    ($($tokens:tt)*) => {
        ::futures_util::select_biased! { $($tokens)* }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __temporal_join {
    ($($tokens:tt)*) => {
        ::futures_util::join!($($tokens)*)
    };
}

#[doc(hidden)]
pub use runtime::model::{CancellableID, CancellableIDWithReason, UnblockEvent};
pub use runtime::model::{TimerResult, WorkflowResult, WorkflowTermination};
#[doc(hidden)]
pub use runtime::{SdkWakeGuard, is_sdk_wake};
pub use workflow_context::{
    ActivityCloseTimeouts, ActivityExecutionError, ActivityOptions, BaseWorkflowContext,
    CancellableFuture, ChildWorkflowExecutionError, ChildWorkflowOptions, ChildWorkflowSignalError,
    ContinueAsNewOptions, ExternalWorkflowHandle, LocalActivityOptions, NexusOperationOptions,
    ParentWorkflowInfo, RootWorkflowInfo, Signal, SignalData,
    StartChildWorkflowExecutionFailedCause, StartedChildWorkflow, SyncWorkflowContext,
    TimerOptions, WorkflowContext, WorkflowContextView,
};
