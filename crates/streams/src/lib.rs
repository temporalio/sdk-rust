#![warn(missing_docs)]

//! Experimental. What every Store of a Temporal Stream shares: the error of a failed call, the
//! order of a Producer's epochs, and the stored form of a Record.

pub mod epoch;
mod error;

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
