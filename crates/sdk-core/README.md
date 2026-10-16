# `temporalio-sdk-core`

[![crates.io](https://img.shields.io/crates/v/temporalio-sdk-core.svg)](https://crates.io/crates/temporalio-sdk-core)
[![docs.rs](https://docs.rs/temporalio-sdk-core/badge.svg)](https://docs.rs/temporalio-sdk-core)

Part of [Temporal](https://temporal.io)'s [Rust SDK](https://github.com/temporalio/sdk-rust).

The shared worker runtime used to build Temporal language SDKs.

Its APIs are intended for SDK implementers and may change without notice. Rust application authors
should use [`temporalio-sdk`](https://crates.io/crates/temporalio-sdk) instead.

## Workflow task boundaries

SDK implementers can opt in to preserving every successful workflow task boundary with the
non-default Cargo feature `disable_workflow_task_chunking`. Enable it on `temporalio-sdk-core`
in the language bridge's dependency declaration, or build the C bridge with:

```sh
cargo build -p temporalio-sdk-core-c-bridge --features disable_workflow_task_chunking
```

The feature controls adoption for both new and existing workflows. At a safe live completion,
Core records SDK flag 5 (`DisableWorkflowTaskChunking`) in workflow task completion metadata.
Replay retains legacy chunking before that task and preserves boundaries from that task onward.
Adoption waits for outstanding local activities to finish and requires server SDK metadata
support. A speculative completion does not establish the transition until it appears in history.

Language SDKs must handle activations with no jobs: apply their clock and metadata, run the normal
workflow scheduler and condition checks, and return their completion. Empty activations can
produce commands. Histories that have already transitioned retain this behavior even in builds
without the feature, so turning the feature off only stops adoption by additional workflows.

See [the chunking design](../../arch_docs/workflow_task_chunking.md) for transition details.

## Workflow activation batches

All language SDKs must poll `WorkflowActivationBatch` and return indexed
`WorkflowActivationBatchCompletion` results, including singleton batches. The C bridge always uses
these protobufs; Rust bridges use `poll_workflow_activation_batch` and
`complete_workflow_activation_batch`. No batching feature or capability opt-in is required.

Execute each activation at its own boundary and preserve commands and metadata for each result.
Apply the batch's `allowed_continuation_commands` allowlist and always stop on failure; Core sends
fresh work for any unexecuted suffix. Batching does not change history recording and is independent of the
chunking feature. See [the batch protocol](../../arch_docs/workflow_activation_batching.md) for the
execution contract, command barriers, and conservative accumulation rules.
