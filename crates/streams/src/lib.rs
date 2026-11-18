#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs. It is experimental.
//!
//! The crate serves the five operations of the Stream service on a Stream Store. A lang bridge
//! builds the runtime and calls it with an operation name and the encoded request. A Store is an
//! [operations::OperationStore] that answers whole operations.

mod dispatch;
pub mod epoch;
mod error;
pub mod ids;
mod metrics;
pub mod operations;
pub mod record;
mod runtime;
#[cfg(test)]
mod test_store;

pub use error::{Reason, StreamError, StreamResult};
pub use operations::MAX_WAIT;
pub use runtime::{MAX_SEQUENCE, StreamRuntime};

/// The protos that lang and the crate exchange. `temporalio-protos` generates them, so a value
/// needs no conversion between the crates.
pub mod proto {
    pub use temporalio_common::protos::temporal::api::{
        enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamStatus},
        stream::v1::*,
        streamservice::v1::*,
    };
}
