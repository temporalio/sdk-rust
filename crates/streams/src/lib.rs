#![warn(missing_docs)]

//! The Stream runtime of Temporal SDKs. It is experimental.
//!
//! The crate serves the Stream service on a Stream Store. Lang's bridge builds the runtime per
//! client and calls it with an operation name and proto bytes. Core has no hook for it. A Workflow
//! makes each call from a Local Activity.

mod error;
pub mod protos;

pub use error::{Reason, StreamError, StreamResult};

/// The protos that lang and the crate exchange.
pub mod proto {
    pub use crate::protos::temporal::{
        api::{enums::v1::*, stream::v1::*, streamservice::v1::*},
        sdk::streams::v1::*,
    };
}
