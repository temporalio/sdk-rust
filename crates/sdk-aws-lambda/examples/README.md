# Lambda Worker example

This example runs both a Workflow and an Activity in Lambda. It needs a Temporal namespace with
Serverless Workers enabled and AWS permissions to deploy Lambda functions. No deployment scripts
run automatically.

## Build

Install [cargo-lambda](https://www.cargo-lambda.info/) and its build prerequisites. From the
repository root:

```sh
cargo lambda build --release --x86-64 -p temporalio-sdk-aws-lambda \
  --example aws-lambda-worker --features otel,temporalio-common/vendored-protox \
  --output-format zip
zip -j target/lambda/aws-lambda-worker/bootstrap.zip \
  crates/sdk-aws-lambda/examples/otel-collector-config.yaml
```

Use the resulting `bootstrap.zip` with the Lambda `provided.al2023` runtime, x86_64 architecture,
a timeout of at least 60 seconds, and initially 1024 MB of memory. Attach an appropriate
[ADOT Collector Lambda layer](https://aws-otel.github.io/docs/getting-started/lambda).
Set `OPENTELEMETRY_COLLECTOR_CONFIG_URI=/var/task/otel-collector-config.yaml`.
The execution role needs CloudWatch Logs permissions and permissions for whichever telemetry
exporters you configure. VPC routing must allow access to Temporal and the chosen AWS endpoints.

Set these environment variables:

| Variable | Value |
| --- | --- |
| `TEMPORAL_ADDRESS` | Temporal endpoint, e.g. `namespace.account.tmprl.cloud:7233` |
| `TEMPORAL_NAMESPACE` | Namespace |
| `TEMPORAL_TASK_QUEUE` | `lambda-greeting` |
| `TEMPORAL_DEPLOYMENT_NAME` | `lambda-greeting` |
| `TEMPORAL_BUILD_ID` | Unique release identifier, e.g. `v1` |
| `OTEL_SERVICE_NAME` | `lambda-greeting` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` |

Supply `TEMPORAL_API_KEY` through your deployment's secret handling, or configure mTLS through the
normal Temporal envconfig options. Never put credentials into this repository or an image.

Publish an immutable Lambda version for each distinct Worker build ID. In Temporal Cloud, configure
the Serverless Worker deployment version to invoke the matching qualified Lambda ARN, grant its
invocation role permission to invoke that ARN, and set the deployment's current/ramping routing as
appropriate. Follow the
[Serverless Worker deployment guide](https://docs.temporal.io/production-deployment/worker-deployments/serverless-workers/aws-lambda)
for account-specific provisioning. Keep old Lambda versions available while pinned Workflows use
them. Uploading a function alone does not configure Temporal-triggered invocation.

## Start a Workflow

With connection credentials available locally and the deployment routed to this build:

```sh
export TEMPORAL_TASK_QUEUE=lambda-greeting
export WORKFLOW_ID=lambda-greeting-1
cargo run -p temporalio-sdk-aws-lambda --example aws-lambda-starter --features otel
```

The result should be `Hello, Temporal!`. Check Lambda logs and your collector destination for Worker
metrics and spans. The supplied collector configuration logs telemetry for initial verification;
replace its debug exporter with your CloudWatch/X-Ray configuration for production.

The handler polls until its deadline even when idle. Cancellation-resistant Activities can require
recycling the process; use the integration's `run` entry point rather than swallowing restart errors.
