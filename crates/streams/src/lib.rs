#![warn(missing_docs)]

//! Experimental. Contains the Stream runtime, and shared definitions for Temporal Stream Store
//! implementations.
//!
//! A Store implements [`StreamStore`]. [`StreamRuntime`] wraps a Store and validates every request
//! before the Store sees it, and every response on the way back. That way all Stores behave the
//! same, and a caller can trust what it gets.
//!
//! The docs of this crate use these terms:
//!
//! - A **Stream** is an append-only log of Records. A [`StreamRef`](proto::StreamRef) identifies
//!   it.
//! - A **Record** is one entry of a Stream. It has a body, the Producer that wrote it, and an
//!   offset.
//! - An **offset** is the position of a Record in its Stream. The Store assigns it as opaque
//!   bytes, and the offsets of one Stream sort bytewise in the order of their Records.
//! - A **Store** is the system that keeps the Records, such as Redis.
//! - A **Producer** is one writer of a Stream. It's a Workflow run, an Activity, a Local Activity
//!   or a client.
//! - An **epoch** is one attempt of a Producer. For a Workflow it's a Workflow Task, and for an
//!   Activity it's one attempt of that Activity. [`epoch::epoch_key`] orders the epochs of a
//!   Producer.
//! - A **batch** is the Records of one append. Its `sequence` numbers the batches of an epoch, so
//!   a Store can recognize the last batch when it comes again.

pub mod epoch;
mod error;
pub mod ids;
pub mod record;
mod runtime;
mod store;
#[cfg(test)]
mod test_store;

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
