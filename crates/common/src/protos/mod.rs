//! Contains the protobuf definitions used as arguments to and return values from interactions with
//! the Temporal Core SDK. Language SDK authors can generate structs using the proto definitions
//! that will match the generated structs in this module.

pub use temporalio_common_wasm::protos::*;

#[cfg(feature = "test-utilities")]
/// Pre-built test histories for common workflow patterns.
pub mod canned_histories;
#[cfg(feature = "history_builders")]
mod history_builder;
#[cfg(feature = "history_builders")]
mod history_info;
#[cfg(feature = "test-utilities")]
pub mod test_utils;

#[cfg(feature = "history_builders")]
pub use history_builder::{
    DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, default_act_sched,
    default_wes_attribs,
};
#[cfg(feature = "history_builders")]
pub use history_info::HistoryInfo;
