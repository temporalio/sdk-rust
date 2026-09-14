# `temporalio-protos`

[![crates.io](https://img.shields.io/crates/v/temporalio-protos.svg)](https://crates.io/crates/temporalio-protos)
[![docs.rs](https://docs.rs/temporalio-protos/badge.svg)](https://docs.rs/temporalio-protos)

Part of [Temporal](https://temporal.io)'s [Rust SDK](https://github.com/temporalio/sdk-rust).

Compiled protobuf definitions for Temporal APIs and SDK Core protocols.

This crate remains on a `0.x` version because generated protobuf messages are not marked
`#[non_exhaustive]`. Adding a protobuf field can therefore be a source-breaking change for code
that constructs a message with a struct literal, so minor releases may contain breaking changes.

Most Rust SDK users should use [`temporalio-client`](https://crates.io/crates/temporalio-client) or
[`temporalio-sdk`](https://crates.io/crates/temporalio-sdk) instead.

Enable the `vendored-protox` feature to compile the protobuf definitions with the pure-Rust
`protox` implementation instead of requiring an installed `protoc` binary.
