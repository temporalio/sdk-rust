#![warn(missing_docs)] // error if there are missing docs

//! This crate contains the shared definitions and serialization/proto surface needed by the
//! workflow authoring APIs, including WASM-targeted builds.

#[allow(unused_imports)] // Not used by all flag combinations, which is fine.
#[macro_use]
extern crate tracing;

mod activity_definition;
pub mod data_converters;
pub mod error;
mod memo;
mod priority;
mod retry_policy;
mod workflow_execution;
pub mod protos {
    //! Protobuf definitions re-exported from `temporalio-protos`.

    pub use temporalio_protos::*;
}
pub mod search_attributes;
pub mod worker;
mod workflow_definition;

pub use activity_definition::{ActivityDefinition, ActivityError, UntypedActivity};
pub use memo::Memo;
pub use priority::Priority;
pub use retry_policy::RetryPolicy;
pub use search_attributes::{
    SearchAttributeError, SearchAttributeKey, SearchAttributeUpdate, SearchAttributeValue,
    SearchAttributes, Timestamp,
};
pub use worker::WorkerDeploymentVersion;
pub use workflow_definition::{
    HasWorkflowDefinition, QueryDefinition, SignalDefinition, UntypedWorkflow, UpdateDefinition,
    WorkflowDefinition,
};
pub use workflow_execution::WorkflowExecution;

#[allow(unused_macros)]
macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      use tracing::error;
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
#[allow(unused_imports)]
pub(crate) use dbg_panic;
