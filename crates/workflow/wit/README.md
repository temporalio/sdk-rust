# Workflow Runtime WIT

This directory defines the canonical high-level workflow guest interface for the future WASM SDK.

This is intentionally **not** the existing core activation/completion protocol. Core remains the
worker-facing protocol for the foreseeable future, and other language SDKs will continue to use it.
The Rust worker will translate core activations and completions into this higher-level interface.

## Why this lives in `temporalio-workflow`

`temporalio-workflow` is the crate that workflow implementations compile against. Checking the WIT
in here gives the Rust runtime refactor a concrete target:

- `temporalio_workflow::runtime::*` should evolve toward a direct Rust mirror of these interfaces.
- The native Rust worker should keep calling those Rust traits directly, with no WIT serialization
  in the hot path.
- A future WASM backend should expose the same interface through the component model.

## Layering

The layers are:

1. Core activation/completion protocol
2. Native worker translation layer
3. This WIT-shaped workflow runtime interface
4. Workflow guest code

That translation layer is where activation ordering and other core-specific details stay hidden.
The guest interface here is deliberately higher level:

- the host instantiates a workflow run
- the host applies activation-wide context
- the host notifies signals, cancellation, patches, updates, and operation resolutions
- the guest polls until it blocks or terminates

## Native and WASM execution

The runtime should support two backends behind the same worker translation logic:

- a native backend that invokes Rust traits directly in-process
- a WASM backend that invokes a component implementing `workflow-module`

The goal is one logical execution model with two transport backends, not two independent workers.
