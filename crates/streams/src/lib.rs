#![warn(missing_docs)]

//! Experimental. Contains shared definitions for Temporal Stream Store implementations.

pub mod epoch;
mod error;
pub mod ids;
pub mod record;
mod store;

pub use error::{Reason, StreamError, StreamResult};
pub use store::{MAX_RECORDS, MAX_WAIT, StreamStore};

/// The Stream protos, re-exported from `temporalio-common` so that its values need no conversion.
pub mod proto {
    pub use temporalio_common::protos::temporal::api::{
        enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamStatus},
        stream::v1::*,
        streamservice::v1::*,
    };
}
