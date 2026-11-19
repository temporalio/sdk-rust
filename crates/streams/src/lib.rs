#![warn(missing_docs)]

//! Experimental. Contains the Stream runtime, and shared definitions for Temporal Stream Store
//! implementations.
//!
//! A Store implements [`StreamStore`], whose docs have the rules of a Store. [`StreamRuntime`]
//! wraps a Store and validates every request before the Store sees it, and every response on the
//! way back. So every Store gets the same valid requests, and no caller gets a malformed response.
//! Each operation fails with its own error type, such as [`AppendError`], which has only the
//! reasons that the operation can return.
//!
//! The docs of this crate use these terms:
//!
//! - A **Stream** is an append-only log of Records.
//! - A **Record** is one entry of a Stream. It has a body, the Producer that wrote it, and an
//!   offset.
//! - An **offset** is the position of a Record in its Stream. The Store assigns it as opaque
//!   bytes, and the offsets of one Stream sort bytewise in the order of their Records.
//! - A **Store** is the system that keeps the Records, such as Redis.
//! - A closed Stream **expires** one **Retention** after its close. An open Stream expires after a
//!   time with no write, if the Store sets such a time. Both times are settings of the Store. A
//!   Store answers a call for an expired Stream with `Expired`, and no longer keeps its Records.
//! - A **Producer** is one writer of a Stream. It's a Workflow run, an Activity, a Local Activity
//!   or a client. Its kind and the ids of that kind identify it, and [`attempt::producer_key`]
//!   encodes them.
//! - An **attempt** is the try of a Producer that made a write. For a Workflow it's a Workflow
//!   Task, and for an Activity or a Local Activity it's one attempt of it. The attempt number
//!   starts at 1 again for each Workflow Task, after an Activity reset, and for a Local Activity
//!   of a retried Workflow Task, so a Store can't order writes by the number alone. An attempt
//!   also has the started event id and the started time of its Workflow Task, or the scheduled
//!   time of the Activity attempt, and [`attempt::attempt_key`] orders the attempts of a Producer
//!   by them.
//! - A **batch** is the Records of one append. Each try of an append sends the same `request_id`,
//!   so a Store writes the batch once while it remembers the id for that Producer of the Stream.

pub mod attempt;
mod error;
pub mod ids;
pub mod record;
mod runtime;
mod store;
#[cfg(test)]
mod test_store;

pub use error::{
    AnyError, AnyReason, AppendError, AppendReason, CloseError, CloseReason, CreateError,
    CreateReason, DescribeError, DescribeReason, ReadError, ReadReason, Reason, StreamError,
    WireReason,
};
pub use runtime::StreamRuntime;
pub use store::StreamStore;

/// Re-exports the Stream protos from `temporalio-common`, so a Store can use them as they are.
pub mod proto {
    pub use temporalio_common::protos::temporal::api::{
        enums::v1::{StreamErrorReason, StreamIdConflictPolicy, StreamStatus},
        stream::v1::*,
        streamservice::v1::*,
    };
}
