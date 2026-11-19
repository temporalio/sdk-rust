#![warn(missing_docs)]

//! Experimental. Contains the Stream runtime, and shared definitions for Temporal Stream Store
//! implementations.
//!
//! A Store implements [`StreamStore`]. [`StreamRuntime`] wraps a Store and validates every request
//! before the Store sees it, and every response on the way back. That way all Stores behave the
//! same, and a caller can trust what it gets.

pub mod epoch;
mod error;
pub mod ids;
pub mod record;
mod runtime;
mod store;
#[cfg(test)]
mod test_store;
#[cfg(feature = "workflow")]
pub mod workflow;

pub use error::{Reason, StreamError, StreamResult};
pub use runtime::{MAX_RECORDS, MAX_SEQUENCE, MAX_WAIT, StreamRuntime};
pub use store::StreamStore;

/// Re-exports the Stream protos from `temporalio-common`, so a Store can use them as they are.
pub mod proto {
    pub use temporalio_common::protos::temporal::api::{
        enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamStatus},
        stream::v1::*,
        streamservice::v1::*,
    };
}
