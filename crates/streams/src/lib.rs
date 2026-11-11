#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs. It is experimental.
//!
//! The crate serves the Stream service on a Stream Store. Lang's bridge builds the runtime per
//! client and calls its typed methods, or its byte form with an operation name and proto bytes.
//! Core has no hook for it. A Workflow makes each call from a Local Activity.

mod dispatch;
mod error;
mod runtime;
pub mod store;
mod writer;

pub use error::{Reason, StreamError, StreamResult};
pub use runtime::{MemoryStreamRuntime, StreamRuntime};
pub use writer::StreamWriter;

/// The protos that lang and the crate exchange. `temporalio-protos` generates them, so a value
/// needs no conversion between the crates.
pub mod proto {
    pub use temporalio_common::protos::{
        coresdk::streams::*,
        temporal::api::{
            enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamStatus},
            stream::v1::*,
            streamservice::v1::*,
        },
    };
}
