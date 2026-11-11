#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs. It is experimental.
//!
//! The crate serves the Stream service on a Stream Store. Lang's bridge builds the runtime per
//! client and calls its typed methods, or its byte form with an operation name and proto bytes.
//! Core has no hook for it. A Workflow makes each call from a Local Activity.

mod error;

pub use error::{Reason, StreamError, StreamResult};

/// The protos that lang and the crate exchange. `temporalio-protos` generates them, so a value
/// needs no conversion between the crates.
pub mod proto {
    pub use temporalio_common::protos::{
        coresdk::streams::*,
        temporal::api::{
            enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamReadStart, StreamStatus},
            stream::v1::*,
            streamservice::v1::*,
        },
    };
}
