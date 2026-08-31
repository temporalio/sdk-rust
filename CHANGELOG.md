<!--
High-level release notes.
Loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This file serves users of the Rust SDK. crates/sdk-core/CHANGELOG.md serves users of
the other Temporal SDKs, whose workers and clients run on Core.

Log a change here only if a Rust SDK user can observe it: different behavior, an option
they can set, a new log or metric, a different interaction with the server. The question
is what the user sees, not which crate or which files your PR touched.

The Rust SDK runs on Core, so a user-observable change in Core behavior normally belongs
here as well as in the sdk-core changelog, worded for each audience. An internal Rust API
change that no SDK user can observe belongs in neither. The same applies to the crates
Core shares: temporalio-client, temporalio-common, temporalio-common-wasm,
temporalio-macros, and temporalio-protos.

When your PR includes a user-facing change, add an entry below under the
appropriate heading (create the heading if it does not yet exist) in the
Unreleased section — never under a released version. Within each heading content
can be free-form. Feel free to include examples, links to docs, or any other
relevant information.

### Added            — new features
### Changed          — changes in existing functionality
### Deprecated       — soon-to-be-removed features
### Breaking Changes — removed or backwards-incompatible features
### Fixed            — notable bug fixes
### Security         — notable security fixes
-->

# Changelog

## Unreleased

### Added
* `DefaultFailureConverter::new(true)` moves failure messages and stack traces into encoded
  attributes so payload codecs can encrypt them.
* `LocalActivityOptions::include_arguments_in_marker` allows Rust workflows to opt in to
  recording local activity arguments in Workflow history.
* `WorkflowHandle::get_update_handle` creates a typed handle for an existing Workflow Update from
  its update ID, allowing callers to wait for the result independently of the original handle.
* `WorkflowContext::all_handlers_finished` and `SyncWorkflowContext::all_handlers_finished` let
  Rust workflows wait for active signal and update handler chains before completing or continuing
  as new.
* `WorkflowStartOptions::memo` attaches a non-indexed memo when starting a workflow, using the
  same `MemoValues` type already used by continue-as-new and `WorkflowContext::upsert_memo`.
  Values are serialized with the client's payload converter and codec, matching how `describe`
  and `list` read them back.
* `MemoValue` and `MemoValues` are now exported from `temporalio_common` as well as
  `temporalio_workflow`, so the same types can be used from clients and workflows.
* The `temporal_activity_execution_failed` and `temporal_local_activity_execution_failed` worker
  metrics now carry a `failure_reason` attribute. Each is now split into one time series per
  reason, which may affect existing dashboards.
* Workflow task completions larger than the gRPC request size limit are now paginated automatically when the namespace supports it. Paginated workflow task completions require Temporal Server 1.32.0 or later.
* Update-with-Start support: `Client::start_update_with_start_workflow` and
  `Client::execute_update_with_start_workflow` start a workflow and send it an update in one atomic
  operation. `WorkflowUpdateWithStartOptions` requires an ID conflict policy (use `UseExisting` to
  attach an update to an already-running workflow), provides distinct start and update headers,
  and controls the atomic RPC. The operation can be intercepted via
  `ClientInterceptor::update_with_start_workflow`.

### Breaking Changes :boom:
* `DefaultFailureConverter` is no longer a unit struct. Use `DefaultFailureConverter::default()` instead.
* `WorkflowHandle::fetch_history` now returns a lazy `WorkflowHistory` stream instead
  of eagerly fetching every history page. Use `WorkflowHistory::into_events` if eager fetching
  is desired.
* `WorkflowHistory::to_json` is now async, `WorkflowHistoryError` reports fetch and JSON conversion failures; and the eager `events`,
  `Clone`, and `From<WorkflowHistory> for History` APIs have been removed. Replay results expose
  their eagerly fetched events through `ReplayHistory`.
* The following types are now non-exhaustive: `Priority`, `WorkerDeploymentVersion`,
  `WorkerCallbacks`, `WorkflowExecutionInfo`, `ActivityCloseTimeouts`,
  `ActivityExecutionDecodeHint`, child-workflow and signal decode hints,
  `SerializationContext`, `SerializationContextData`, `PayloadConverter`, `IncomingError`,
  `ScheduleSpec`, and `ScheduleOverlapPolicy`. Construct structs using their respective builders
  or constructors (`WorkerCallbacks::new`, `ActivityExecutionDecodeHint::new`, or
  `SerializationContext::new`); use `Default` for `PayloadConverter`; and add wildcard branches
  when matching enums.
* `SerializationContextData::{Workflow, Activity, Nexus}` now contain corresponding context
  structs. `SerializationContextData` is no longer `Copy`.
* Renamed `ActivityCloseTimeouts::Both` to `ActivityCloseTimeouts::ScheduleAndStartToClose`.
* Removed the unused `ActExitValue` type. Use `ActivityError::WillCompleteAsync` to mark an
  activity for asynchronous completion.
* Removed the test-only `FailOnNondeterminismInterceptor` from the public Rust SDK API.
* Environment configuration values (`DataSource`, `ClientConfig`, and related profile, TLS, and
  codec types) are now non-exhaustive. Use their `bon` builders to construct configuration structs,
  and add a wildcard branch when matching `DataSource`.
* Values stored in a `MemoValue` must now be `Send + Sync`. It previously held its value in an
  `Rc` and now uses an `Arc`, so that memos can be built outside a workflow and handed to the
  client. Only affects memo values that are themselves non-`Send`/non-`Sync`, such as those
  holding an `Rc` or `RefCell`.

* `Client::signal_with_start_workflow` starts a workflow and sends a typed signal atomically.

### Breaking Changes :boom:

* Signal-with-start is now invoked with `Client::signal_with_start_workflow`; remove uses of
  `WorkflowStartOptions::start_signal` and `WorkflowStartSignal`.

### Fixed
* `Worker` shutdown no longer loses an activity result it was still reporting to the server. If
  shutdown raced such a completion — most likely while the activity's final heartbeat RPC was
  still in flight — the worker could strand the completion forever: debug builds panicked with
  `Waiting for all slot permits to release took too long!`, and release builds logged that error
  and dropped the result, leaving the server to time the activity out before retrying it.
  Shutdown now drains in-flight completions first.
* Standalone activity result and describe APIs now apply the configured payload codec when
  decoding failures, so encoded failure attributes and details are restored correctly.
* The default payload converter now serializes Serde `null` values such as `Option::None` as
  `binary/null` and accepts both `binary/null` and legacy `json/plain` null payloads.
* The Prometheus exporter now respects `PrometheusExporterOptions::counters_total_suffix`,
  appending `_total` to counter metric names when enabled.
* Workflow start requests now include the client's identity.
* An activity failure caused by oversized final heartbeat details is now counted in the
  `temporal_activity_execution_failed` metric as `failure_reason="PayloadsTooLarge"`. Previously it
  was counted under the reason for the failure the activity itself reported, and was not counted at
  all when that failure was benign, even though the worker reported a payload-limit failure to the
  server.
* Workers configured with a small `max_cached_workflows` no longer briefly stop accepting new
  workflows. Sticky workflow-task pollers could consume every workflow-cache permit and starve the
  non-sticky poller, so the worker would stop picking up new workflows until a poll timed out (up to
  ~60s). The poll balancer now reserves a non-sticky slot against the cache size rather than the
  slot-supplier size.
* The default payload converter now encodes `Vec<u8>` and `Option<Vec<u8>>` as `binary/plain` when
  present.

## [0.7.0] - 2026-08-17

### Added
* Support for running Standalone Activities in Rust SDK Worker.
* Client methods for starting and managing execution of Standalone Activities. 
* `WorkflowTermination::cancelled_with_details` for recording structured details when a Workflow
  Execution completes as cancelled.
* `LoggerFormat` for selecting compact, pretty, or JSON Core console log output. Configured log
  filters continue to apply to JSON output.
* The Rust SDK now has an optional `testing` feature with a typed activity test environment and
  local or external workflow test environments. Local workflow environments manage a Temporal CLI
  dev server and expose shutdown through their local-server type state.
* Worker heartbeats now report the SDK runtime, hosting environments, operating system, and
  architecture once per worker, retrying until the first successful delivery. Runtime options can
  disable this reporting, and language SDK bridges can supply their own runtime details. The Rust
  SDK exposes separate runtime options that omit bridge-only runtime overrides.
* `RpcOptions::builder()` for constructing per-call RPC options.
* `DnsLoadBalancingOptions::builder()` for configuring DNS re-resolution intervals.
* Experimental plugin APIs for packaging reusable client and worker configuration, including data
  converters, interceptors, activities, workflows, and automatic propagation from clients to
  workers.
* `WorkflowCancellationToken` for deterministic cancellation of workflow operations.
* `WorkflowContext::wait_condition_with_options` and `WaitConditionOptions` for waiting with a
  custom cancellation token.
* Support for dynamic client certificate resolution via `TlsOptions::client_cert_resolver`, which
  accepts an `Arc<dyn ResolvesClientCert>` for per-handshake mTLS certificate selection. This enables
  transparent certificate rotation without process restarts — useful for short-lived certificates
  managed by Vault, cert-manager, or HSM-backed signers. `ResolvesClientCert`, `CertifiedKey`, and
  `SignatureScheme` are re-exported from the crate root for convenience.
* Worker heartbeats now report the SDK runtime, hosting environments, operating system, and
  architecture once per worker, retrying until the first successful delivery. Set
  `RuntimeOptions::disable_environment_info` to turn the reporting off.
* Workers now log a `[TMPRL1104]` warning when a workflow task takes longer than 5 seconds. Set
  `TEMPORAL_WORKFLOW_TASK_DURATION_WARN_SECONDS` to change the threshold.
* `SignalWorkflowOptions::summary` attaches a single-line summary to a signal sent to another
  workflow, which the UI and CLI display alongside the resulting history event.

### Changed
* Cancellation errors propagated after workflow cancellation now complete the workflow as cancelled
  instead of failed.
* `WorkflowReplayer` for workflow history replay, including JSON history helpers
  and worker-plugin configuration.
* Experimental worker lifecycle interception through `WorkerInterceptor::run_worker` and
  `WorkerInterceptor::with_workflow_replay_worker`.

### Breaking Changes :boom:
* `WorkflowTermination::Cancelled` now has an optional `details` field. Use
  `WorkflowTermination::cancelled()` to construct a cancellation without details.
* Changes to `ActivityInfo`: instead of `workflow_namespace`, `workflow_execution` and `run_id`,
  there is now `namespace`, `workflow_id`, `workflow_run_id` and `activity_run_id`. 
  Also, `workflow_type` is now `Option<String>`.
* `ActivityIdentifier::ById` was split into 2 variants, `ByIdWorkflow` and `ByIdStandalone`.
  `ActivityIdentifier::by_id` method was renamed to `by_id_workflow`, and `by_id_standalone`
  was added.
* `anyhow::Error` no longer converts directly into `WorkflowTermination`. Wrap an error in
  `ApplicationFailure` to explicitly fail the Workflow Execution.
* `OutgoingWorkflowError` now has a dedicated `PayloadConversion` variant. Converting activity,
  child-workflow, and signal errors lifts their payload-conversion variants into it.
  `OutgoingError`, `OutgoingActivityError`, and `OutgoingWorkflowError` are now non-exhaustive;
  downstream matches must include a wildcard arm.
* Removed `InterceptorWithNext`. Register worker interceptors as an ordered vector instead.
* Ephemeral server APIs now return `EphemeralServerError` instead of `anyhow::Error`, and dev-server
  log format and level use the non-exhaustive `DevServerLogFormat` and `DevServerLogLevel` enums.
* Ephemeral server APIs now return the operation-oriented `EphemeralServerError` instead of
  `anyhow::Error`.
* Activity macro support now exposes instance requirements through `ExecutableActivity`; the
  redundant `HasOnlyStaticMethods` marker trait has been removed.
* `Worker::run` now returns `WorkerRunError` instead of `anyhow::Error`.
  Non-validation failures are reported as `WorkerRunError::Fatal` with a message and source.
* `Logger::Console` now requires a `format: Option<LoggerFormat>` field. Use `None` to preserve the
  previous behavior, including support for `TEMPORAL_CORE_PRETTY_LOGS`.
* `CancellableFuture` and `CancellableFutureWithReason` now use the inherited `Future::Output`
  associated type instead of a generic output parameter.
* `TimerOptions` is now tagged with `#[non_exhaustive]`. Use
  `TimerOptions::builder(duration)` to construct timer options. Passing a `Duration` directly to
  `WorkflowContext::timer` remains supported.
* `RetryOptions` is now tagged with `#[non_exhaustive]`. Use `RetryOptions::builder()` to
  construct retry options.
* `HttpConnectProxyOptions` is now tagged with `#[non_exhaustive]`. Use
  `HttpConnectProxyOptions::new(target_addr)` to construct proxy options.
* `TlsOptions` is now tagged with `#[non_exhaustive]`. Use `TlsOptions::builder()` to construct
  TLS options.
* `ClientTlsOptions` is now tagged with `#[non_exhaustive]`. Use `ClientTlsOptions::builder()` to
  construct client TLS options.
* `ClientKeepAliveOptions` is now tagged with `#[non_exhaustive]`. Use
  `ClientKeepAliveOptions::builder()` to construct keep-alive options.
* `PayloadLimitsOptions` is now tagged with `#[non_exhaustive]`. Use
  `PayloadLimitsOptions::builder()` to construct payload limit options.
* `RegisterNamespaceOptions` is now tagged with `#[non_exhaustive]`. Continue using
  `RegisterNamespaceOptions::builder()` to construct namespace registration options.
* `LoadClientConfigOptions` is now tagged with `#[non_exhaustive]`. Use
  `LoadClientConfigOptions::builder()` to construct client configuration loading options.
* `LoadClientConfigProfileOptions` is now tagged with `#[non_exhaustive]`. Use
  `LoadClientConfigProfileOptions::builder()` to construct profile loading options.
* `ClientConfigFromTOMLOptions` is now tagged with `#[non_exhaustive]`. Use
  `ClientConfigFromTOMLOptions::builder()` to construct TOML parsing options.
* `OtelCollectorOptions` is now tagged with `#[non_exhaustive]`. Use
  `OtelCollectorOptions::builder()` to construct OpenTelemetry collector options.
* `PrometheusExporterOptions` is now tagged with `#[non_exhaustive]`. Use
  `PrometheusExporterOptions::builder()` to construct Prometheus exporter options.
* `WorkerDeploymentOptions` is now tagged with `#[non_exhaustive]`. Use
  `WorkerDeploymentOptions::new(version)` to construct worker deployment options.
* `LocalActivityOptions` is now tagged with `#[non_exhaustive]`. Use
  `LocalActivityOptions::builder()` to construct local activity options.
* `NexusOperationOptions` is now tagged with `#[non_exhaustive]`. Use
  `NexusOperationOptions::builder()` to construct Nexus operation options.
* `WorkflowContext::wait_condition` now returns `Result<(), WorkflowCancellationError>` instead of
  `()` so that workflow cancellation can be propagated to the caller.
* `WorkflowUpdateWaitStage` and `WorkflowStartUpdateOptions::wait_for_stage` have been removed.
  `start_update` now always waits for the update to be accepted; use `execute_update` to wait for
  completion.
* Workflow and activity implementations must now be registered through `WorkerOptions` before
  constructing a `Worker`; the corresponding registration methods on `Worker` have been removed.
* `Worker::run` now returns `WorkerRunError` instead of `anyhow::Error`.
  Non-validation failures are reported as `WorkerRunError::Fatal` with a message and source.

### Fixed
* Rust SDK workers now warn when autoscaling task polling encounters errors continuously for one
  minute. Repeated warnings use exponential backoff up to 15-minute intervals and stop after
  polling recovers.
* Unhandled workflow payload conversion errors now fail the Workflow Task so it can retry instead
  of failing the Workflow Execution. Workflows may still explicitly handle these errors.
* Workers no longer send worker heartbeats or appear in centralized heartbeat reports before
  `Worker::run` begins.
* Local activity resolutions are now delivered to workflows as each activity completes instead of
  waiting for every local activity in the workflow task. This allows sequences of short local
  activities to make progress while a long-running local activity executes in parallel, while
  preserving the resolution ordering recorded in existing histories during replay.
* Try-cancel child workflows no longer cause nondeterminism when they complete or fail after their
  cancellation was requested.
* Panics from update validators now reject the update instead of repeatedly failing workflow
  tasks.
* Workers with `max_cached_workflows` set to 0 no longer stall when a local activity resolves while
  the resolution for an earlier one is still being delivered.
* Rust SDK workers now derive enabled task types from registered workflows and activities. The
  task types can no longer be configured separately, preventing mismatched poll loops from hanging
  worker shutdown.

## [0.6.0] - 2026-08-04

### Added
* `UntypedActivity` for invoking activities by a runtime activity type name with raw input and
  output payloads.
* Typed search attributes through `SearchAttributeKey`, `SearchAttributeUpdate`,
  `SearchAttributes`, and `Timestamp`. Workflow starts, child workflows, continue-as-new,
  workflow reads, and upserts now share this type-safe API.
* `ClientInterceptor` for observing, transforming, or short-circuiting high-level workflow,
  schedule, and async-activity client operations. Per-call `RpcOptions` can set gRPC metadata,
  timeouts, and retry behavior.
* `CoreRuntime` is now re-exported from `temporalio_sdk` as `Runtime`, with the remaining Core
  runtime and worker configuration types under `temporalio_sdk::runtime`, so workers no
  longer need a direct `temporalio-sdk-core` dependency. `Url` is also re-exported from
  `temporalio_client`.
* Workers can configure the maximum number of activity slots reserved for eager execution per
  workflow task with `WorkerOptions::max_eager_activity_reservations_per_workflow_task`.
* `WorkflowInterceptor` for observing, transforming, or short-circuiting inbound workflow calls
  and outbound operations.
* Schedule descriptions now expose their configured action via `ScheduleDescription::action()`,
  including start-workflow accessors for workflow type, task queue, workflow ID, raw argument
  payloads, and typed argument decoding through the client's data converter.
* Added the experimental `WorkerOptions::patch_activation_callback` option for controlling whether
  newly introduced patches activate during rolling deployments.
* `WorkflowContext::random` and `WorkflowContext::uuid4` for deterministic randomness in workflow.
* `ChildWorkflowOptions::builder` and `ChildWorkflowOptions::workflow_id` for constructing
  child workflow options.
* Added `connect_timeout: Option<Duration>` to `ConnectionOptions`.
* `ResourceController` and `ResourceBasedTunerConfig` allow resource controllers to be shared
  across multiple resource-based tuners.
* Workers are now automatically enrolled into poller autoscaling when the namespace advertises the
  `poller_autoscaling_auto_enroll` capability. This only applies to poller types left at their
  default (the worker set neither a fixed poller count nor a poller behavior); explicitly
  configured pollers are left unchanged.
* Updated the bundled Temporal API definitions through API 1.63.4

### Changed
* Renamed `start_activity` and `start_local_activity` to `execute_activity` and
  `execute_local_activity` to better explain semantics. Original methods remain as deprecated
  aliases for the new execute variants.
* `#[workflow(name = ...)]` is now rejected at compile time because it did not override the
  workflow type name. Use `#[run(name = ...)]` instead.

### Breaking Changes :boom:
* `ActivityDefinition::name` is now an instance method returning `&str` instead of an associated
  function returning `&'static str`. Manual implementations must update their method signature.
* `WorkflowStartOptions::search_attributes`, `ChildWorkflowOptions::search_attributes`, and
  `ContinueAsNewOptions::search_attributes` now use typed `SearchAttributes` instead of raw maps or
  protobuf values. `WorkflowContext::upsert_search_attributes` now accepts
  `SearchAttributeUpdate` values.
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
* `ContinueAsNewOptions::headers` has been removed. Workflow interceptors can inspect or modify
  continue-as-new headers through `ContinueAsNewInput`.
* Removed the raw-protobuf `Namespace::into_describe_namespace_request` and
  `WorkerTaskTypes::to_task_queue_types` helpers. These conversions are now internal plumbing.
* `ActivityContext::new` and the raw-protobuf `WorkflowExecution::new` constructor are no longer public.
* `WorkflowContext::workflow_initial_info` and its synchronous counterpart are replaced by
  `info()`, which returns the Rust-native `WorkflowContextView` and includes typed workflow
  priority. The internal `BaseWorkflowContext::new` raw-protobuf boundary is now explicitly named
  `from_raw`.
* `WorkflowContextView` fields are now private and exposed through accessors. Parent workflow
  information now uses `NamespacedWorkflowInfo`, and root workflow information uses
  `WorkflowExecution`.
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
* Most of the low-level `temporalio_common::payload_limits` validation API is now internal,
  including the sink/validation traits, `CollectingSink`, and sizing helpers. `PayloadLimits` error
  thresholds are now byte counts where zero disables enforcement, rather than `Option<usize>`, and
  its default now disables all thresholds.
* `WorkflowContext::random_seed()` and `SyncWorkflowContext::random_seed()` have been removed.
  Use `random::<T>()` or `uuid4()` for deterministic workflow randomness instead.
* `ChildWorkflowOptions::workflow_id` is now `Option<String>`. Wrap explicit IDs in `Some(...)`;
  when omitted, the parent workflow generates a UUID child workflow ID.
* `ChildWorkflowOptions` is now tagged with `#[non_exhaustive]` so additional fields will not be breaking
  changes. Users should switch to `ChildWorkflowOptions::builder()` for constructing these options.
* `PayloadCodec::{encode, decode}` now return `Result<_, PayloadConversionError>`, allowing codecs to fail.
* `TunerHolderOptions::resource_based_options` has been replaced by
  `resource_based_config: Option<ResourceBasedTunerConfig>`.
* `WorkerConfig::{workflow,activity,nexus}_task_poller_behavior` and the corresponding Rust SDK
  `WorkerOptions` fields are now `Option<PollerBehavior>`. `None` means the poller was not explicitly
  configured and is eligible for automatic enrollment into poller autoscaling.
* High-level client methods that previously had no per-call controls now require `RpcOptions` or a
  new options type. This includes schedule handle operations, async-activity completion, failure,
  cancellation, and heartbeat, and `WorkflowUpdateHandle::get_result`.

### Fixed
* `RuntimeOptions::default()` now uses the same 60-second worker heartbeat interval as the
  builder default.
* Workflow tasks no longer livelock when a burst of ready async operations exhausts Tokio's
  cooperative scheduling budget.
* OTLP metric export failures are now logged through Core telemetry when OpenTelemetry's periodic
  metric reader reports an export error.
* Worker heartbeat now samples host CPU/memory at the heartbeat interval (only when enabled) rather
  than every 100ms.
* `WorkflowContext::force_task_fail` calls will be respected over a completion if both happen in the same poll
* Workers no longer advertise a worker control task queue unless the namespace supports worker
  heartbeats and commands and the built-in Nexus command worker is running.
* `GetSystemInfo` connection initialization now only falls back to empty server capabilities when
  `UNIMPLEMENTED` indicates the RPC method is missing, including the message format produced by
  Node gRPC servers. Other `UNIMPLEMENTED` responses are reported as connection errors.
* Connection initialization now retries once with gRPC compression disabled if the eager
  `GetSystemInfo` call fails because the server cannot decompress gzip.
* SDK flags already recorded in workflow history are honored even when current server capabilities
  do not advertise SDK metadata support.
* C-bridge worker shutdown no longer intermittently fails finalization because an asynchronous
  poll, completion, or validation callback still holds a worker reference.
* The `task_slots_used` metric no longer reports a stale, off-by-one value when a task slot is
  released.
* The internal Nexus worker-command poller no longer sends unsupported worker-versioning metadata.
* Resource-based tuners running in cgroups now gate slot admission on anonymous memory instead of
  total current memory, so reclaimable page cache does not permanently starve slot admission.
* Worker shutdown waits for local activities already queued for dispatch instead of dropping them.
* Failed workflow-history fetches that finish after zero-cache eviction now fail the workflow task
  with its preserved task token instead of dropping the token and blocking subsequent polls.

### Security
* Replaced the unmaintained `backoff` dependency with `backon` for exponential retry and poll
  backoff, clearing [RUSTSEC-2025-0012](https://rustsec.org/advisories/RUSTSEC-2025-0012) from
  downstream security audits. Retry timing is preserved: exponential growth,
  `randomization_factor` jitter, and the total retry-time budget behave as before.

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
* Awaiting a Nexus operation's result (`StartedNexusOperation::result()`) no longer trips
  nondeterminism detection ("a waker was invoked by a non-SDK source", TMPRL1100) on replay. The
  result future is a `Shared`, whose internal waker machinery must be polled inside an `SdkWakeGuard`
  (as `join_all` already is); it now is. Previously, a workflow that awaited a Nexus operation result
  and then kept running (e.g. parked on a `wait_condition`) would fail its workflow task whenever it
  was replayed — breaking queries and durable recovery for that execution.

### Breaking Changes
* The `ActivityContext` constructor now requires `ClientOptions`.
* Rust SDK `ApplicationFailure` and `WorkflowError` APIs now use boxed `std::error::Error` values instead of
  `anyhow::Error`.
