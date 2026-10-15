# `temporalio-sdk-core`

[![crates.io](https://img.shields.io/crates/v/temporalio-sdk-core.svg)](https://crates.io/crates/temporalio-sdk-core)
[![docs.rs](https://docs.rs/temporalio-sdk-core/badge.svg)](https://docs.rs/temporalio-sdk-core)

Part of [Temporal](https://temporal.io)'s [Rust SDK](https://github.com/temporalio/sdk-rust).

The shared worker runtime used to build Temporal language SDKs.

Its APIs are intended for SDK implementers and may change without notice. Rust application authors
should use [`temporalio-sdk`](https://crates.io/crates/temporalio-sdk) instead.
## Workflow activation batches

All language SDKs must poll `WorkflowActivationBatch` and return indexed
`WorkflowActivationBatchCompletion` results, including singleton batches. The C bridge always uses
these protobufs; Rust bridges use `poll_workflow_activation_batch` and
`complete_workflow_activation_batch`. No batching feature or capability opt-in is required.

Execute each activation at its own boundary and preserve commands and metadata for each result.
Stop on failure or commands requiring a Core response before continuing; Core sends fresh work for
any unexecuted suffix. Batching does not change history recording and is independent of the
chunking feature. See [the batch protocol](../../arch_docs/workflow_activation_batching.md) for the
execution contract, command barriers, and conservative accumulation rules.
