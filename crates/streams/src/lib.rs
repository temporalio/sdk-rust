#![warn(missing_docs)]

//! Experimental. Contains the Stream runtime, and shared definitions for Temporal Stream Store
//! implementations.
//!
//! [`StreamRuntime`] checks each Stream call and its answer, and passes the call to a
//! [`StreamStore`]. A lang bridge implements [`StreamStore`] for a Store in its own language.

pub mod epoch;
mod error;
pub mod ids;
mod metrics;
pub mod record;
mod runtime;
mod store;
#[cfg(test)]
mod test_store;
#[cfg(feature = "workflow")]
pub mod workflow;

pub use error::{Reason, StreamError, StreamResult};
pub use runtime::{MAX_SEQUENCE, StreamRuntime};
pub use store::{MAX_RECORDS, MAX_WAIT, StreamStore};

/// The Stream protos, re-exported from `temporalio-common` so that its values need no conversion.
pub mod proto {
    pub use temporalio_common::protos::temporal::api::{
        enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamStatus},
        stream::v1::*,
        streamservice::v1::*,
    };
}
