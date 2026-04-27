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

## Stability

The package is published as `temporal:workflow-runtime@0.1.0` and is **not yet stable**. Until
this is bumped to `1.0.0` any release may bump the minor version with breaking changes. Once
external SDKs (Python, TypeScript, Go, etc.) begin compiling guest workflows against this WIT,
breaking changes need to follow normal SemVer discipline: bump the package version, dual-export
the old and new world from the host for a deprecation window, and document the migration path.

Changes that force a major bump include:

- Renaming or removing any record field, variant case, function, interface, or world.
- Changing a field/case/parameter type (including widening `u32` to `u64` etc.).
- Reordering variant cases (the discriminant order is part of the ABI).
- Changing the proto messages encoded into `list<u8>`-typed fields (`failure`, `payload`,
  `workflow-command`, `workflow-activation`, `continue-as-new-request`).

## Synchronous ABI and the WASIp3 future

The current guest interface is intentionally synchronous: `activate` and `poll-routine` both
return ordinary results, and the guest is expected to suspend by returning
`routine-poll-result.made-progress = false`. The host then re-enters `poll-routine` after the
relevant activation lands.

This shape is what stable Wasmtime + `wit-bindgen` support today. When component-model async
(WASIp3 / Preview 3) lands as stable in Wasmtime, the natural evolution is:

- `activate` and `poll-routine` become `async func`.
- `routine-poll-result.made-progress` and the explicit `routine-id`-keyed polling protocol
  likely go away — the host can just `await` each routine directly.

That is a breaking ABI change, so it will require a major-version WIT bump. Any other-language
SDK that ships a guest implementation should expect to regenerate bindings against the new
world at that point.
