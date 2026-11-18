#![warn(missing_docs)]

//! Experimental. The Stream runtime that Core and the Temporal SDK bridges link. It answers the
//! five operations of the Stream service on a Stream Store, and it has no Store of its own.
//!
//! A [StreamStore] answers each operation whole. The Store applies the rules of the service
//! itself. A lang bridge implements the trait for a Store that is written in that language. The
//! Stores that Temporal provides, and the shared rules that a Store in Rust builds on, are in
//! [temporal-community/sdk-streams](https://github.com/temporal-community/sdk-streams).
//!
//! [StreamRuntime::new] builds the runtime on one Store. It has one typed method for each
//! operation. [StreamRuntime::call] is the byte form of the same methods. The runtime checks each
//! request before the Store sees it, and each answer after it. The checks are the same for every
//! Store.

mod dispatch;
pub mod epoch;
mod error;
pub mod ids;
mod metrics;
pub mod record;
mod runtime;
mod store;
#[cfg(test)]
mod test_store;

pub use error::{Reason, StreamError, StreamResult};
pub use runtime::{MAX_SEQUENCE, StreamRuntime};
pub use store::{MAX_RECORDS, MAX_WAIT, StreamStore};

/// The protos that lang and the crate exchange. `temporalio-protos` generates them, so a value
/// needs no conversion between the crates.
pub mod proto {
    pub use temporalio_common::protos::temporal::api::{
        enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamStatus},
        stream::v1::*,
        streamservice::v1::*,
    };
}
