# `doc(hidden)` identifiers

This inventory was generated from Rust source with:

```console
rg -n --glob '*.rs' '^\s*#\s*!?\s*\[\s*doc\s*\(\s*hidden\s*\)\s*\]' crates
```

The anchored expression avoids matching `#[doc(hidden)]` mentioned in comments. Identifiers in a
grouped `pub use` are listed separately. The generated function whose source is emitted by
`crates/common/build.rs` is included as well. Locations point to the identifier's declaration or
re-export rather than to the preceding attribute.

| Identifier | Kind | Location |
| --- | --- | --- |
| `temporalio_client::retry::jittered` | function | [`crates/client/src/retry.rs:160`](crates/client/src/retry.rs#L160) |
| `temporalio_client::grpc::PayloadLimitsClient` | struct | [`crates/client/src/grpc.rs:415`](crates/client/src/grpc.rs#L415) |
| `temporalio_client::request_extensions::PayloadErrorLimits` | struct | [`crates/client/src/request_extensions.rs:31`](crates/client/src/request_extensions.rs#L31) |
| `temporalio_client::worker::NamespaceDescriptionSource` | struct | [`crates/client/src/worker.rs:324`](crates/client/src/worker.rs#L324) |
| `temporalio_client::worker::ClientWorkerSet::namespace_description_source` | method | [`crates/client/src/worker.rs:420`](crates/client/src/worker.rs#L420) |
| `temporalio_client::jittered` | re-exported function | [`crates/client/src/lib.rs:47`](crates/client/src/lib.rs#L47) |
| `temporalio_client::MESSAGE_TOO_LARGE_KEY` | static | [`crates/client/src/lib.rs:190`](crates/client/src/lib.rs#L190) |
| `temporalio_client::ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT` | static | [`crates/client/src/lib.rs:193`](crates/client/src/lib.rs#L193) |
| `temporalio_common::fsm_trait` | module | [`crates/common/src/lib.rs:13`](crates/common/src/lib.rs#L13) |
| `temporalio_common::payload_limits` | module | [`crates/common/src/lib.rs:15`](crates/common/src/lib.rs#L15) |
| `temporalio_common::payload_limits::LimitClass` | enum | [`crates/common/src/payload_limits.rs:20`](crates/common/src/payload_limits.rs#L20) |
| `temporalio_common::payload_limits::PayloadLimits` | struct | [`crates/common/src/payload_limits.rs:151`](crates/common/src/payload_limits.rs#L151) |
| `temporalio_common::payload_limits::LimitSeverity` | enum | [`crates/common/src/payload_limits.rs:175`](crates/common/src/payload_limits.rs#L175) |
| `temporalio_common::payload_limits::PayloadLimitViolation` | struct | [`crates/common/src/payload_limits.rs:185`](crates/common/src/payload_limits.rs#L185) |
| `temporalio_common::payload_visitor::validate_known_payload_limits` | generated function | [`crates/common/build.rs:993`](crates/common/build.rs#L993) |
| `temporalio_sdk::WorkerOptions::to_core_options` | method | [`crates/sdk/src/lib.rs:662`](crates/sdk/src/lib.rs#L662) |
| `temporalio_workflow::component::bindings` | module | [`crates/workflow/src/component.rs:31`](crates/workflow/src/component.rs#L31) |
| `temporalio_workflow::__private` | module | [`crates/workflow/src/lib.rs:14`](crates/workflow/src/lib.rs#L14) |
| `temporalio_workflow::InternalPatchActivationCallback` | re-exported type alias | [`crates/workflow/src/lib.rs:88`](crates/workflow/src/lib.rs#L88) |
| `temporalio_workflow::PatchActivationCaller` | re-exported struct | [`crates/workflow/src/lib.rs:88`](crates/workflow/src/lib.rs#L88) |
| `temporalio_workflow::__temporal_select` | macro | [`crates/workflow/src/lib.rs:94`](crates/workflow/src/lib.rs#L94) |
| `temporalio_workflow::__temporal_join` | macro | [`crates/workflow/src/lib.rs:102`](crates/workflow/src/lib.rs#L102) |
| `temporalio_workflow::__temporalio_export_workflow_component` | macro | [`crates/workflow/src/lib.rs:110`](crates/workflow/src/lib.rs#L110) |
| `temporalio_workflow::PatchActivationCaller` | struct | [`crates/workflow/src/workflow_context.rs:221`](crates/workflow/src/workflow_context.rs#L221) |
| `temporalio_workflow::BaseWorkflowContext::from_raw` | method | [`crates/workflow/src/workflow_context.rs:659`](crates/workflow/src/workflow_context.rs#L659) |

The former public `temporalio_workflow::component` and `temporalio_workflow::runtime` modules no
longer appear in this inventory. They are private implementation modules; exports needed by macro
expansions and the SDK are now routed through the explicitly internal
`temporalio_workflow::__private` module. The generated component bindings retain their own
annotation because the exported macro must be able to name them from the workflow author's crate,
even though they are implementation details declared within the private component module.
