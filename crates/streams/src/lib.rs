#![warn(missing_docs)]

//! Experimental support for Temporal Stream Stores.
//!
//! A **Stream** is an append-only log. Each **Record** has a body, a Producer and an offset.
//! A **Store** assigns opaque offsets that sort bytewise in Stream order. [`StreamStore`]
//! defines its contract. [`StreamRuntime`] validates requests and individual responses, and
//! passes only the Stream id to the Store.
//!
//! A **Producer** is a Workflow run, an Activity, a Local Activity or a client.
//! [`attempt::producer_key`] encodes its kind and ids. [`attempt::attempt_key`] orders attempts
//! of the same Producer. Attempt numbers can restart. Workflow Task attempts also use their
//! started event id and time. Activity attempts use the time the server scheduled the attempt.
//!
//! A **batch** contains the Records of one append. A Store remembers recent batches by
//! Producer, attempt and request id. **Retention** starts at create and restarts when an
//! append writes a batch.

pub mod attempt;
mod error;
pub mod ids;
pub mod record;
mod runtime;
mod store;
#[cfg(test)]
mod test_store;

pub use error::{
    AnyError, AnyReason, AppendError, AppendReason, CreateError, CreateReason, DescribeError,
    DescribeReason, ReadError, ReadReason, StreamError, WireReason,
};
pub use runtime::{ReadPage, StreamRuntime};
pub use store::StreamStore;

/// Re-exports the Stream protos from `temporalio-common`, so a Store can use them as they are. The
/// requests that [`StreamRuntime`] takes are in [`sdk`](crate::proto::sdk).
pub mod proto {
    pub use temporalio_common::protos::temporal::api::{
        enums::v1::{StreamErrorReason, StreamIdConflictPolicy},
        stream::v1::*,
        streamservice::v1::*,
    };

    /// The requests of an SDK, which have the ref of a Stream in place of its id, and the create
    /// response with the ref.
    pub mod sdk {
        pub use temporalio_common::protos::temporal::api::sdk::v1::{
            AppendRecordsRequest, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
            ReadRecordsRequest, StreamRef,
        };
    }
}
