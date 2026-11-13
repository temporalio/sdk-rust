#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs. It is experimental.
//!
//! The crate serves the five operations of the Stream service on a Stream Store. A lang bridge
//! builds the runtime and calls it with an operation name and the encoded request.

pub mod digest;
pub mod epoch;
mod error;
pub mod ids;
pub mod memory;
pub mod offset;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use error::{Reason, StreamError, StreamResult};

/// The protos that lang and the crate exchange. `temporalio-protos` generates them, so a value
/// needs no conversion between the crates.
pub mod proto {
    pub use temporalio_common::protos::temporal::api::{
        enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamStatus},
        stream::v1::*,
        streamservice::v1::*,
    };
}
