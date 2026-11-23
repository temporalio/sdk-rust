# Temporal Rust SDK AWS Lambda integration

This experimental crate runs a Temporal Worker inside each AWS Lambda invocation. Temporal Cloud
invokes the function when tasks arrive; the handler ignores the event payload and polls Temporal.
Both Workflows and Activities use the normal Rust SDK registration APIs. Given `worker_options`
containing your registrations:

```rust,no_run
use temporalio_common::worker::WorkerDeploymentVersion;
use temporalio_sdk::WorkerOptions;
use temporalio_sdk_aws_lambda::LambdaWorker;

# async fn example(worker_options: WorkerOptions) -> Result<(), lambda_runtime::Error> {
let version = WorkerDeploymentVersion::builder()
    .deployment_name("payments")
    .build_id("v1")
    .build();
LambdaWorker::builder(version, worker_options).build()?.run().await?;
# Ok(())
# }
```

Client connection configuration is loaded once at handler construction, from environment variables
and an optional TOML file. File lookup order is `TEMPORAL_CONFIG_FILE`,
`$LAMBDA_TASK_ROOT/temporal.toml`, then `./temporal.toml`. An explicit missing config file is an
error. `TEMPORAL_TASK_QUEUE` supplies the queue when the initial Worker options have an empty queue.
Use `client_options` for explicit connection/client options.

## Versioning and defaults

Worker Deployment Versioning is required and always enabled with the supplied deployment name and
build ID. The default Workflow versioning behavior is **PINNED**, unless a Workflow registration
provides its own behavior. Retain old Lambda deployments until their pinned Workflows complete or
are migrated. Use `default_versioning_behavior(VersioningBehavior::AutoUpgrade)` to opt into
upgrades; changes to auto-upgrading Workflows must remain replay-compatible.

Default limits are 10 Workflow Task slots, 2 Activity slots, 2 Local Activity slots, 2 Workflow Task
pollers, 1 Activity Task poller, 30 cached Workflows, and 5 seconds of graceful shutdown. Rust does
not currently serve Nexus handlers. `lambda_options` customizes limits and shutdown budgets;
`worker_tuner` replaces the fixed-size tuner. Eager Activity execution is disabled.

Deployment identity, enabled versioning, the selected default behavior, disabled eager Activities,
and the graceful shutdown period are enforced by a final internal Worker plugin, after user
plugins run. Other options, including the tuner, can be customized by user plugins.

## Invocation lifecycle

Create the handler once and reuse it. Each invocation receives a fresh client and Worker; a live
Worker or Workflow cache is never retained across invocations. The optional asynchronous
`configure` callback runs on every invocation and returns modified `LambdaInvocationConfig`:

```rust,no_run
# fn configure(builder: temporalio_sdk_aws_lambda::LambdaWorkerBuilder) {
let builder = builder.configure(|mut config| async move {
    config.worker_options.task_queue = "payments-task-queue".to_owned();
    Ok(config)
});
# }
```

The default shutdown reserve is **8.1 seconds**: 5 for graceful Activity completion, 1 for
cancellation and Worker finalization, 2 for telemetry and application hooks, and 100 milliseconds
for posting the response. This deliberately reserves more drain time than the other SDKs'
7-second default. Allow more than 9.1 seconds for an invocation. Configuration and connection
setup count against the work budget, using a single monotonic deadline.

The Worker polls until its shutdown window begins, not until one task completes or the queue is
idle. It requests Activity cancellation after the graceful period and normally finalizes before
hooks run. Activities must cooperate with cancellation; there is no automatic async completion.

Hooks run in registration order, with the automatic telemetry flush first. They share a bounded
budget, receive its remaining duration, and must not block the async executor. Hook errors are
logged; a timed-out hook is dropped. Hooks also run on configuration/connection/Worker errors after
the invocation has started. Resources created in `configure` should use cancellation-safe ownership
and cleanup, since setup can time out.

If drain overruns, shutdown continues alongside hooks until the response deadline. The handler
then requires a process restart, even if draining eventually succeeds, since telemetry may have
flushed too early. `run` posts the invocation error before exiting the runtime loop. When calling
`handle` directly, recycle the execution environment after cancelling its future or receiving an
error with `requires_restart() == true`. Never create a replacement handler in that same process.
Concurrent calls to one handler are rejected.

## OpenTelemetry

Enable `otel` and call `open_telemetry(OpenTelemetryOptions::default())` to configure metrics and
tracing through an AWS Distro for OpenTelemetry (ADOT) Collector Lambda extension. This creates the
SDK runtime and cannot be combined with `runtime`.

The OTLP gRPC endpoint defaults to `OTEL_EXPORTER_OTLP_ENDPOINT`, then `http://localhost:4317`.
HTTPS uses system trust roots. The service name defaults to `OTEL_SERVICE_NAME`, then
`AWS_LAMBDA_FUNCTION_NAME`, then `temporal-lambda-worker`. Metric export interval is 10 seconds;
each collector request has a configurable 1-second timeout.

The helper uses the SDK's OpenTelemetry plugin, deterministic Workflow IDs, replay-suppressing span
processor, and cross-SDK W3C context propagation. Instrument Workflow-starting clients with the SDK's
general OpenTelemetry plugin to connect their traces.

Metrics and traces are force-flushed concurrently after drain; providers remain active across warm
invocations. Flush errors do not fail a successful invocation. Blocking flushes cannot be cancelled;
a single-flight guard prevents later invocations from accumulating additional flush tasks while
either previous export remains running. Delivery is not guaranteed if the collector is unavailable
or AWS freezes the environment.

Initialize an application `tracing` subscriber to receive warnings and export/hook errors. The
integration does not replace your global subscriber. Applications supplying their own runtime can
register hooks to flush their own telemetry.

See [the deployable example](examples/README.md) for build and deployment instructions.
