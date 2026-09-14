#![warn(missing_docs)]

//! Compiled protobuf definitions for the Temporal Rust SDK.
//!
//! This crate remains on a `0.x` version because generated protobuf messages are not marked
//! `#[non_exhaustive]`. Becauase of this constructing a message with a struct literal is
//! unsupported.

pub mod protos;

pub use protos::*;
