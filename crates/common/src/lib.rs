#![warn(missing_docs)] // error if there are missing docs

//! This crate contains base-level functionality needed by the other crates in the Temporal Core and
//! Rust SDK.

#[allow(unused_imports)] // Not used by all flag combinations, which is fine.
#[macro_use]
extern crate tracing;

#[cfg(feature = "envconfig")]
pub mod envconfig;
#[doc(hidden)]
pub mod fsm_trait;
pub mod payload_visitor;
pub mod protos;
pub mod telemetry;
pub mod worker;
pub use temporalio_common_wasm::{
    ActivityDefinition, ActivityError, HasWorkflowDefinition, Priority, QueryDefinition,
    SignalDefinition, UntypedWorkflow, UpdateDefinition, WorkerDeploymentVersion,
    WorkflowDefinition, data_converters, error,
};

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      use tracing::error;
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
pub(crate) use dbg_panic;
