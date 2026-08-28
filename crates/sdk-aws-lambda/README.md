# Temporal Rust SDK AWS Lambda integration

This experimental crate runs a Temporal Worker for the bounded lifetime of an AWS Lambda
invocation. It creates a new Temporal client and Worker for each invocation, begins graceful
shutdown before the invocation deadline, and then runs registered shutdown hooks.

```rust,no_run
use std::sync::Arc;
use temporalio_common::worker::WorkerDeploymentVersion;
use temporalio_sdk::WorkerOptions;
use temporalio_sdk_aws_lambda::LambdaWorker;

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
2 Local Activity slots, 2 Workflow Task pollers, 1 Activity Task poller, 1 Nexus Task poller, a
workflow cache size of 30, and a five-second graceful shutdown period. Eager Activity execution is
always disabled, and Worker Deployment Versioning is always enabled. Call `worker_tuner` to
explicitly replace the fixed-size Lambda tuner with a custom tuner.

## OpenTelemetry

Enable the `otel` feature to configure Temporal metrics and tracing for the AWS Distro for
OpenTelemetry Collector Lambda layer:

```toml
[dependencies]
temporalio-sdk-aws-lambda = { version = "0.1", features = ["otel"] }
```

```rust,no_run
use temporalio_sdk_aws_lambda::otel::OpenTelemetryOptions;

# fn configure(
#     version: temporalio_common::worker::WorkerDeploymentVersion,
#     worker_options: temporalio_sdk::WorkerOptions,
# ) -> Result<(), temporalio_sdk_aws_lambda::LambdaWorkerError> {
let worker = temporalio_sdk_aws_lambda::LambdaWorker::builder(version, worker_options)
    .open_telemetry(OpenTelemetryOptions::default())
    .build()?;
# Ok(())
# }
```

The default OTLP gRPC endpoint is `OTEL_EXPORTER_OTLP_ENDPOINT`, then
`http://localhost:4317`. The service name is taken from `OTEL_SERVICE_NAME`, then
`AWS_LAMBDA_FUNCTION_NAME`, and otherwise defaults to `temporal-lambda-worker`. Traces use
AWS X-Ray-compatible trace IDs. Metrics and spans are force-flushed within the invocation's
shutdown window while their providers remain active for warm starts.

Attach the ADOT Collector Lambda layer and point `OTEL_EXPORTER_OTLP_ENDPOINT` at its receiver.
The Lambda integration configures Temporal telemetry; application code outside Temporal still
requires separate instrumentation.

`open_telemetry` creates a telemetry-enabled Temporal runtime and therefore cannot be combined with
the builder's `runtime` method. Applications that provide their own runtime can use `shutdown_hook`
to flush their application-owned telemetry providers.
