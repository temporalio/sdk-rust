# Temporal Rust SDK AWS Lambda integration

This experimental crate runs a Temporal Worker for the bounded lifetime of an AWS Lambda
invocation. It creates a new Temporal client and Worker for each invocation, begins graceful
shutdown before the invocation deadline, and then runs registered shutdown hooks.

```rust,no_run
use std::sync::Arc;
use temporalio_common::worker::WorkerDeploymentVersion;
use temporalio_sdk::WorkerOptions;
use temporalio_sdk_aws_lambda::LambdaWorker;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let version = WorkerDeploymentVersion::builder()
    .deployment_name("payments")
    .build_id("2026-08-28")
    .build();
let mut worker_options = WorkerOptions::new("payments-task-queue").build();
// Register workflows and activities on worker_options here.

LambdaWorker::builder(version, worker_options)
    .build()?
    .run()
    .await?;
# Ok(())
# }
```

Client connection settings are loaded from environment variables and an optional `temporal.toml`.
The file lookup order is `TEMPORAL_CONFIG_FILE`, `$LAMBDA_TASK_ROOT/temporal.toml`, and
`./temporal.toml`. `TEMPORAL_TASK_QUEUE` is used when the supplied worker options have an empty task
queue.

The builder applies Lambda-oriented limits by default: 10 Workflow Task slots, 2 Activity slots,
2 Local Activity slots, 2 Workflow Task pollers, 1 Activity Task poller, a
workflow cache size of 30, and a five-second graceful shutdown period. Eager Activity execution is
always disabled, and Worker Deployment Versioning is always enabled. Call `worker_tuner` to
explicitly replace the fixed-size Lambda tuner with a custom tuner.

## Invocation lifecycle

The default shutdown reserve is 8.1 seconds: five for graceful Activity completion, one for
Activity cancellation and Worker finalization, two for telemetry and application hooks, and
100 milliseconds for returning the runtime response.
Polling ends when that reserve begins. Worker drain cannot consume the hook reserve. Configure
`LambdaWorkerOptions` through `lambda_options` to adjust these budgets. Allow more than 9.1 seconds
per invocation to leave at least one second for polling and connection setup.

Worker drain and finalization complete before telemetry flush and application hooks. Hook errors
are logged; a hook that exceeds the shared hook budget is cancelled. A Worker that fails or cannot
drain and finalize makes the handler unusable for warm invocations, and `run` exits the runtime loop
so the process can terminate. If calling `handle` directly, recycle the process when
`LambdaInvocationError::requires_restart()` is true, or after cancelling the handler future.
Calls to the same handler must be sequential.

`build` returns an opaque `LambdaWorkerBuildError`; `handle` returns an opaque
`LambdaInvocationError`. Both preserve the underlying cause through `std::error::Error::source`.

## OpenTelemetry

Enable the `otel` feature and call `open_telemetry` to configure Temporal metrics and tracing for
the AWS Distro for OpenTelemetry (ADOT) Collector Lambda layer:

```rust,no_run
use temporalio_sdk_aws_lambda::otel::OpenTelemetryOptions;

# fn configure(
#     version: temporalio_common::worker::WorkerDeploymentVersion,
#     worker_options: temporalio_sdk::WorkerOptions,
# ) -> Result<(), temporalio_sdk_aws_lambda::LambdaWorkerBuildError> {
let worker = temporalio_sdk_aws_lambda::LambdaWorker::builder(version, worker_options)
    .open_telemetry(OpenTelemetryOptions::default())
    .build()?;
# Ok(())
# }
```

The default OTLP gRPC endpoint is `OTEL_EXPORTER_OTLP_ENDPOINT`, then
`http://localhost:4317`. The service name is taken from `OTEL_SERVICE_NAME`, then
`AWS_LAMBDA_FUNCTION_NAME`, and otherwise defaults to `temporal-lambda-worker`. The helper installs
the SDK's `OpenTelemetryPlugin`, `WorkflowIdGenerator`, and `WorkflowSpanProcessor`. Client,
Workflow, and Activity spans propagate W3C trace context through the cross-SDK `_tracer-data`
header; replay does not export Workflow spans. Instrument clients that start Workflows with the
general plugin to connect their traces. ADOT can convert these W3C IDs for
[AWS X-Ray, which accepts W3C trace IDs without a timestamp prefix](https://docs.aws.amazon.com/xray/latest/api/API_PutTraceSegments.html).

After Worker drain, metrics and spans are force-flushed concurrently while their providers remain
active for warm invocations. Collector requests have a one-second timeout by default, configurable
with `OpenTelemetryOptions::export_timeout`. Export failures are logged and do not fail a successful
invocation. A blocking provider flush cannot be cancelled by an async timeout; the helper prevents
overlapping flushes on a later invocation until both previous calls have finished. This bounds
queued work but does not guarantee delivery when the collector is unavailable or Lambda freezes
the environment.

Attach the ADOT Collector Lambda layer and point `OTEL_EXPORTER_OTLP_ENDPOINT` at its receiver.
The Lambda integration configures Temporal telemetry; application code outside Temporal still
requires separate instrumentation.

Initialize your application's logging subscriber to receive `tracing` warnings and hook/export
errors. The example uses `lambda_runtime::tracing::init_default_subscriber`; the adapter does not
replace a global application subscriber.

`open_telemetry` creates a telemetry-enabled Temporal runtime and therefore cannot be combined with
the builder's `runtime` method. Applications that provide their own runtime can use `shutdown_hook`
to flush their application-owned telemetry providers.

## Deployable example and local testing

The [AWS Lambda example](../sdk/examples/aws_lambda/) lives with the other Rust SDK examples. It
contains a Lambda Worker, a separate Workflow starter, and an AWS deployment walkthrough covering
Temporal-triggered invocation, the ADOT collector layer, CloudWatch metrics, and X-Ray traces.
It also includes a local Runtime Interface Emulator path for development without AWS.
