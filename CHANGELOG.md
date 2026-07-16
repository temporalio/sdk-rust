<!--
High-level release notes.
Loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

When your PR includes a user-facing change, add an entry below under the
appropriate heading (create the heading if it does not yet exist). Within
each heading content can be free-form. Feel free to include examples, links
to docs, or any other relevant information.

### Added            — new features
### Changed          — changes in existing functionality
### Deprecated       — soon-to-be-removed features
### Breaking Changes — removed or backwards-incompatible features
### Fixed            — notable bug fixes
### Security         — notable security fixes
-->

# Changelog

## [0.5.0]

### Added
* `client()` and `workflow_handle()` helpers to `ActivityContext` for easily obtaining a Temporal client
* Exposed `backoff_start_interval` when continuing as new, which will delay the first task of the
  continued workflow by the configured interval.
* The `tls-ring` / `tls-aws-lc` features now also select the TLS crypto backend for the OTLP metric
  exporter (in addition to the gRPC service client). Previously the OTLP exporter hardcoded the `ring`
  backend regardless of the selected feature, which prevented producing a `ring`-free, `aws-lc-rs`-only
  (FIPS-capable) build. Building with `--no-default-features --features tls-aws-lc,otel` now yields a
  dependency tree free of `ring`.

### Fixed
* `GetSystemInfo` connection initialization now only falls back to empty server capabilities when
  `UNIMPLEMENTED` indicates the RPC method is missing. Other `UNIMPLEMENTED` responses are
  reported as connection errors.
* Connection initialization now retries once with gRPC compression disabled if the eager
  `GetSystemInfo` call fails because the server cannot decompress gzip.
* Awaiting a Nexus operation's result (`StartedNexusOperation::result()`) no longer trips
  nondeterminism detection ("a waker was invoked by a non-SDK source", TMPRL1100) on replay. The
  result future is a `Shared`, whose internal waker machinery must be polled inside an `SdkWakeGuard`
  (as `join_all` already is); it now is. Previously, a workflow that awaited a Nexus operation result
  and then kept running (e.g. parked on a `wait_condition`) would fail its workflow task whenever it
  was replayed — breaking queries and durable recovery for that execution.

### Breaking Changes
* The `ActivityContext` constructor now requires `ClientOptions`.
### Breaking Changes

- Rust SDK `ApplicationFailure` and `WorkflowError` APIs now use boxed `std::error::Error` values instead of
  `anyhow::Error`.

## [Unreleased]

### Added
* Schedule descriptions now expose their configured action via `ScheduleDescription::action()`,
  including start-workflow accessors for workflow type, task queue, workflow ID, raw argument
  payloads, and typed argument decoding through the client's data converter.

### Breaking Changes
* `WorkflowExecution::search_attributes`, `WorkflowExecutionDescription::search_attributes`,
  `ScheduleDescription::search_attributes`, and `ScheduleSummary::search_attributes` now return
  typed `SearchAttributes` instead of raw proto search attributes. Missing search attributes are
  returned as an empty collection instead of `None`.
* Activity and child-workflow failure metadata now exposes activity and workflow type names as
  strings, and workflow executions as the Rust-native `WorkflowExecution` type. `ActivityInfo`
  uses the same Rust-native workflow execution type.
* Workflow status accessors and query rejection errors now use the Rust-native
  `WorkflowExecutionStatus` enum instead of generated protobuf types.
* Activity, child-workflow, and timeout errors now expose Rust-native `RetryState` and
  `TimeoutType` enums instead of generated protobuf enums.
* Workflow and worker options now use Rust-native cancellation, parent-close, workflow-ID reuse,
  versioning, and Nexus cancellation policy enums instead of generated protobuf enums.
* Child workflow cancellation now defaults to `WaitCancellationCompleted` instead of `Abandon`,
  aligning Rust with the Core-based SDKs and Java. Set `ChildWorkflowCancellationType::Abandon`
  explicitly to retain the previous behavior.
* Workflow and activity retry configuration and runtime information now use the Rust-native
  `RetryPolicy` type instead of the generated protobuf message.
* Workflow result failures now expose decoded `IncomingError` values, and cancellation and
  termination details use typed `WorkflowResultDetails` instead of raw payloads.
* Async activity completion, failure, cancellation, and heartbeat methods now convert typed Rust
  values with the client's data converter. Activity heartbeat details are exposed through the
  typed `ActivityHeartbeatDetails` wrapper.
* Workflow and schedule list/description memo accessors now return the typed `Memo` wrapper
  instead of raw protobuf memos.
* Workflow memo reads use the typed `Memo` collection. Upserts accept maps of optional
  `MemoValue`s, where `None` removes a key, and continue-as-new memo replacements use
  `MemoValues`.
* Removed the raw-protobuf `Namespace::into_describe_namespace_request` and
  `WorkerTaskTypes::to_task_queue_types` helpers. These conversions are now internal plumbing.
* `WorkflowContext::workflow_initial_info` and its synchronous counterpart are replaced by
  `info()`, which returns the Rust-native `WorkflowContextView` and includes typed workflow
  priority. The internal `BaseWorkflowContext::new` raw-protobuf boundary is now explicitly named
  `from_raw`.
* Workflow count aggregation groups now provide positional typed `get` and `try_get` accessors
  for search attribute group values over raw payload access.
* Payload/memo size-limit enforcement (experimental), on by default. Workers now proactively
  validate outbound payload/memo sizes against namespace limits before sending to the server.
  If payload/memo-bearing fields exceed the warn threshold, the worker logs a warning; if over the
  error limit, the task completion is failed retryably instead of sent to the server. Both cases log
  `[TMPRL1103]` (at `WARN` and `ERROR` respectively).
  Previously these were sent and the server terminated the workflow / failed the activity
  non-retryably; failing retryably instead lets a corrected workflow or activity be redeployed and
  recover. A deterministically-oversized completion now retries per its retry policy rather than
  failing fast. Tune warn thresholds via `PayloadLimitsOptions`. Opt out of worker error enforcement
  with `WorkerOptions::disable_payload_error_limit`.

### Fixed
* Workflow tasks no longer livelock when a burst of ready async operations exhausts Tokio's
  cooperative scheduling budget.
* OTLP metric export failures are now logged through Core telemetry when OpenTelemetry's periodic
  metric reader reports an export error.
* Worker heartbeat now samples host CPU/memory at the heartbeat interval (only when enabled) rather
  than every 100ms.
