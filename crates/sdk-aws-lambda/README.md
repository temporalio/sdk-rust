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
2 Local Activity slots, 2 Workflow Task pollers, 1 Activity Task poller, a
workflow cache size of 30, and a five-second graceful shutdown period. Eager Activity execution is
always disabled, and Worker Deployment Versioning is always enabled. Call `worker_tuner` to
explicitly replace the fixed-size Lambda tuner with a custom tuner.

## Invocation lifecycle

The default shutdown reserve is 8.1 seconds: five for graceful Activity completion, one for
Activity cancellation and Worker finalization, two for telemetry and application hooks, and
100 milliseconds for returning the runtime response.
Polling ends when that reserve begins. Worker drain cannot consume the hook reserve. Configure
`LambdaWorkerDefaults` to adjust these budgets, and allow more than 9.1 seconds per invocation
to leave at least one second for polling and connection setup.

Telemetry flush runs before application hooks. Hook errors are logged; a hook that exceeds the
shared hook budget is cancelled. A Worker that fails or cannot drain makes the handler unusable
for warm invocations, and `run` exits the runtime loop so the process can terminate. If calling
`handle` directly, recycle the process after `ShutdownTimedOut`, `WorkerRun`, or cancelling the
handler future. Calls to the same handler must be sequential.

## OpenTelemetry

Enable the `otel` feature and call `open_telemetry` to configure Temporal metrics and tracing for
the AWS Distro for OpenTelemetry (ADOT) Collector Lambda layer:

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

`examples/lambda_worker.rs` registers `GreetingWorkflow` and a greeting Activity. Build its container
from the repository root (Docker BuildKit is required):

```sh
docker build -t temporal-rust-lambda -f crates/sdk-aws-lambda/examples/Dockerfile .
```

For AWS, supply `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TASK_QUEUE`,
`TEMPORAL_DEPLOYMENT_NAME`, and `TEMPORAL_BUILD_ID`, plus authentication/TLS settings as needed.
Configure a Lambda timeout of at least 15 seconds, deploy a matching-architecture image, and
attach/configure the ADOT Collector extension. Mark the Worker Deployment Version current before
starting new Workflows, or use a pinned versioning override on the starting client.

For local testing, start `temporal server start-dev --ip 0.0.0.0` and an OTLP collector. The example
collector configuration (`examples/otel-collector.yaml`) exports to its debug log. Download the
matching Linux binary from the official
[Runtime Interface Emulator releases](https://github.com/aws/aws-lambda-runtime-interface-emulator/releases)
and make it executable. With Docker Desktop, mount it and run:

```sh
docker run --rm -p 9000:8080 \
  -v /absolute/path/aws-lambda-rie:/usr/local/bin/aws-lambda-rie:ro \
  -e AWS_LAMBDA_FUNCTION_TIMEOUT=15 \
  -e TEMPORAL_ADDRESS=host.docker.internal:7233 \
  -e TEMPORAL_NAMESPACE=default -e TEMPORAL_TASK_QUEUE=lambda-greetings \
  -e TEMPORAL_DEPLOYMENT_NAME=lambda-greetings -e TEMPORAL_BUILD_ID=local \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://host.docker.internal:4317 \
  --entrypoint /usr/local/bin/aws-lambda-rie temporal-rust-lambda /var/runtime/bootstrap
```

Invoke it once to register the version, then set that version current and start a Workflow:

```sh
curl --fail -X POST http://localhost:9000/2015-03-31/functions/function/invocations -d '{}'
temporal worker deployment set-current-version --deployment-name lambda-greetings --build-id local --yes
temporal workflow start --type GreetingWorkflow --task-queue lambda-greetings --input '"Temporal"'
curl --fail -X POST http://localhost:9000/2015-03-31/functions/function/invocations -d '{}'
```

The second invocation should complete the Workflow with `"Hello, Temporal!"`; the collector should
receive connected Workflow/Activity spans and Temporal metrics. Repeating the last two commands
tests warm reuse. RIE tests the runtime protocol locally, not AWS freezing or the ADOT extension's
deployment and permissions.

If Docker Desktop resolves `host.docker.internal` to an unreachable IPv6 address, substitute the
host's IPv4 address for the local Temporal and collector endpoints.
