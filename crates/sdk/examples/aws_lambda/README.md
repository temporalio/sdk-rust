# AWS Lambda Worker

This example runs a versioned Temporal Worker on AWS Lambda. `GreetingWorkflow` calls a greeting
Activity and returns `Hello, Temporal!`. OpenTelemetry metrics and traces go through the AWS Distro
for OpenTelemetry (ADOT) collector extension to CloudWatch and X-Ray.

The starter runs locally, but only starts a Workflow and waits for its result. It does not run a
Worker. The AWS walkthrough exercises actual deployment and Temporal-triggered Lambda invocation,
following the [Java Lambda sample](https://github.com/temporalio/samples-java/tree/main/lambda-worker).

## Prerequisites

- Rust, [Cargo Lambda](https://www.cargo-lambda.info/guide/installation.html), Zig, and `zip` for packaging.
- The Temporal CLI, AWS CLI v2, `jq`, and OpenSSL.
- AWS permissions to manage Lambda, IAM, and CloudFormation, and read logs, metrics, and X-Ray traces.
  These resources incur AWS charges.
- An AWS-hosted Temporal Cloud namespace with Serverless Workers enabled, or a self-hosted Service
  with the [AWS Lambda Worker Controller](https://docs.temporal.io/production-deployment/worker-deployments/serverless-workers/self-hosted-setup)
  configured. A plain development server does not automatically invoke AWS Lambda.
- A collector-only ADOT Lambda layer ARN for your region and `x86_64` architecture, from the
  [ADOT Lambda collector releases](https://github.com/aws-observability/aws-otel-lambda/releases).

Run commands from the repository root. The AWS package uses `provided.al2023` and `x86_64`.
The container in the local-testing section is not the AWS deployment artifact.

## Build

```sh
bash crates/sdk/examples/aws_lambda/build.sh
```

Cargo Lambda cross-compiles the Worker and packages it as `bootstrap`. The script adds
`otel-collector-config.yaml` to `target/lambda/aws-lambda-worker/bootstrap.zip`; the collector layer
loads it at `/var/task/otel-collector-config.yaml`. Starter-only code is not included in the executable.

## Configure environment

Use unique names in a shared account or namespace:

```sh
export AWS_PROFILE=<aws-profile>
export AWS_REGION=us-west-2
export AWS_DEFAULT_REGION="$AWS_REGION"
export ADOT_COLLECTOR_LAYER_ARN=<collector-only-layer-arn-for-this-region-and-x86_64>
export TEMPORAL_ADDRESS=<namespace>.<account>.tmprl.cloud:7233
export TEMPORAL_NAMESPACE=<namespace>.<account>
export TEMPORAL_API_KEY=<development-api-key>
export TEMPORAL_TLS=true
export SUFFIX="$(date -u +%Y%m%d%H%M%S)-$(openssl rand -hex 3)"
export FUNCTION_NAME="temporal-rust-lambda-$SUFFIX"
export EXECUTION_ROLE_NAME="$FUNCTION_NAME-exec"
export STACK_NAME="trl-$SUFFIX"
export EXTERNAL_ID="$(openssl rand -hex 16)"
export TEMPORAL_DEPLOYMENT_NAME="rust-lambda-$SUFFIX"
export TEMPORAL_BUILD_ID="build-$SUFFIX"
export TEMPORAL_TASK_QUEUE="rust-lambda-tq-$SUFFIX"
export TEMPORAL_LAMBDA_WORKFLOW_ID_PREFIX="rust-lambda-wf-$SUFFIX"
```

For a self-hosted Service, use its reachable frontend address, namespace, and TLS/authentication
settings. This development example puts the API key in Lambda environment variables; use your
organization's secret-management approach for production. Never commit credentials or generated
environment files.

## Deploy to AWS

Create the execution role with permissions for function/collector logs and X-Ray export:

```sh
export EXECUTION_ROLE_ARN="$(aws iam create-role --role-name "$EXECUTION_ROLE_NAME" \
  --assume-role-policy-document file://crates/sdk/examples/aws_lambda/deploy/execution-role-trust.json \
  --query 'Role.Arn' --output text)"
aws iam attach-role-policy --role-name "$EXECUTION_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam attach-role-policy --role-name "$EXECUTION_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess
```

Prepare the environment without putting the API key in the AWS CLI's command-line arguments:

```sh
export LAMBDA_ENV_FILE="$(mktemp)"
chmod 600 "$LAMBDA_ENV_FILE"
jq -n '{Variables: {
  TEMPORAL_ADDRESS: env.TEMPORAL_ADDRESS,
  TEMPORAL_NAMESPACE: env.TEMPORAL_NAMESPACE,
  TEMPORAL_API_KEY: (env.TEMPORAL_API_KEY // ""),
  TEMPORAL_TLS: env.TEMPORAL_TLS,
  TEMPORAL_TASK_QUEUE: env.TEMPORAL_TASK_QUEUE,
  TEMPORAL_DEPLOYMENT_NAME: env.TEMPORAL_DEPLOYMENT_NAME,
  TEMPORAL_BUILD_ID: env.TEMPORAL_BUILD_ID,
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://localhost:4317",
  OPENTELEMETRY_COLLECTOR_CONFIG_URI: "/var/task/otel-collector-config.yaml"
}}' > "$LAMBDA_ENV_FILE"

export FUNCTION_BASE_ARN="$(aws lambda create-function \
  --function-name "$FUNCTION_NAME" --runtime provided.al2023 --handler bootstrap \
  --architectures x86_64 --role "$EXECUTION_ROLE_ARN" \
  --zip-file fileb://target/lambda/aws-lambda-worker/bootstrap.zip \
  --timeout 30 --memory-size 1024 --layers "$ADOT_COLLECTOR_LAYER_ARN" \
  --tracing-config Mode=Active --environment "file://$LAMBDA_ENV_FILE" \
  --query 'FunctionArn' --output text)"
aws lambda wait function-active --function-name "$FUNCTION_NAME"
export FUNCTION_VERSION_ARN="$(aws lambda publish-version --function-name "$FUNCTION_NAME" \
  --description "Build ID $TEMPORAL_BUILD_ID" --query 'FunctionArn' --output text)"
```

If Lambda says the new role cannot be assumed, wait for IAM propagation and rerun `create-function`.
If the ZIP exceeds the direct-upload limit, upload it to S3 and use `--code S3Bucket=...,S3Key=...`
instead of `--zip-file`. The 30-second timeout leaves time for polling and the 8.1-second shutdown
reserve; increase it for longer Activities. Each Temporal Build ID references an immutable Lambda
function version, not `$LATEST`.

## Let Temporal invoke the Worker

For Temporal Cloud, create a separate invocation role using the included CloudFormation template:

```sh
aws cloudformation create-stack --stack-name "$STACK_NAME" \
  --template-body file://crates/sdk/examples/aws_lambda/deploy/temporal-cloud-lambda-invoke-role.yaml \
  --parameters "ParameterKey=AssumeRoleExternalId,ParameterValue=$EXTERNAL_ID" \
    "ParameterKey=LambdaFunctionARNs,ParameterValue=$FUNCTION_BASE_ARN:*" \
  --capabilities CAPABILITY_IAM
aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"
export INVOCATION_ROLE_ARN="$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='RoleARN'].OutputValue | [0]" --output text)"
```

This template trusts Temporal Cloud's identities, not a self-hosted Service. For self-hosted
deployments, use the invocation role and external ID from the controller setup instead.

Create the Deployment and Version with Lambda compute configuration:

```sh
temporal worker deployment create --name "$TEMPORAL_DEPLOYMENT_NAME"
temporal worker deployment create-version --deployment-name "$TEMPORAL_DEPLOYMENT_NAME" \
  --build-id "$TEMPORAL_BUILD_ID" --aws-lambda-function-arn "$FUNCTION_VERSION_ARN" \
  --aws-lambda-assume-role-arn "$INVOCATION_ROLE_ARN" \
  --aws-lambda-assume-role-external-id "$EXTERNAL_ID"
```

Invoke once to check cold-start configuration and register the task queue, then route new Workflows:

```sh
aws lambda invoke --function-name "$FUNCTION_VERSION_ARN" \
  --cli-binary-format raw-in-base64-out --payload '{}' --cli-read-timeout 120 \
  /tmp/rust-lambda-response.json
temporal worker deployment set-current-version --deployment-name "$TEMPORAL_DEPLOYMENT_NAME" \
  --build-id "$TEMPORAL_BUILD_ID" --allow-no-pollers --yes
```

The invocation should return `null` with no `FunctionError`; status 200 alone does not indicate
success. Check for normal `START`, `END`, and `REPORT` logs, not a Lambda timeout.

## Start a Workflow

```sh
export TRACE_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cargo run -p temporalio-sdk --features examples --example aws-lambda-starter -- Temporal
```

The starter prints the Workflow ID and `Workflow result: Hello, Temporal!`. No local Worker or manual
invocation runs in this step: Temporal must invoke the deployed Lambda. After that invocation ends,
run the starter again with another name. AWS may reuse the execution environment, but warm reuse is
not guaranteed. Wait for the Lambda `END` log, not just the Workflow result, to exercise a new invocation.

## Verify OpenTelemetry in AWS

```sh
aws logs tail "/aws/lambda/$FUNCTION_NAME" --since 10m
aws cloudwatch list-metrics --namespace TemporalWorkerMetrics \
  --query 'Metrics[].{Name:MetricName,Dimensions:Dimensions}' --output json
aws xray get-trace-summaries --start-time "$TRACE_START" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --filter-expression "service(\"$FUNCTION_NAME\")" --query 'TraceSummaries[].Id'
```

Check collector startup/export logs and SDK metrics for this function's service name. Metrics reach
`TemporalWorkerMetrics` through CloudWatch EMF and may take a few minutes to appear. In X-Ray, inspect
`RunWorkflow:GreetingWorkflow`, `StartActivity:GreetingActivities::greet`, and
`RunActivity:GreetingActivities::greet` spans, not just Lambda's own invocation trace. The starter is
not instrumented, so the Workflow span starts the Temporal trace.
The Worker flushes spans and metrics before returning, keeping providers available for warm invocations.

## Deploy an update

Choose a new `TEMPORAL_BUILD_ID`, regenerate the environment file above, then run:

```sh
bash crates/sdk/examples/aws_lambda/build.sh
aws lambda update-function-configuration --function-name "$FUNCTION_NAME" \
  --environment "file://$LAMBDA_ENV_FILE"
aws lambda wait function-updated --function-name "$FUNCTION_NAME"
aws lambda update-function-code --function-name "$FUNCTION_NAME" \
  --zip-file fileb://target/lambda/aws-lambda-worker/bootstrap.zip
aws lambda wait function-updated --function-name "$FUNCTION_NAME"
export FUNCTION_VERSION_ARN="$(aws lambda publish-version --function-name "$FUNCTION_NAME" \
  --description "Build ID $TEMPORAL_BUILD_ID" --query 'FunctionArn' --output text)"
```

Repeat `create-version`, the initial invocation, and `set-current-version` with the new Build ID and
function ARN. Do not recreate the Deployment. Existing pinned Workflows retain their original Version;
keep old function versions available until those Workflows drain.

## Clean up

Wait for Workflows and invocations to finish. Reset routing, then delete each Build ID created for
this sample before deleting the Deployment:

```sh
temporal worker deployment set-current-version --deployment-name "$TEMPORAL_DEPLOYMENT_NAME" \
  --unversioned --allow-no-pollers --yes
temporal worker deployment delete-version --deployment-name "$TEMPORAL_DEPLOYMENT_NAME" \
  --build-id "$TEMPORAL_BUILD_ID" --skip-drainage
temporal worker deployment delete --name "$TEMPORAL_DEPLOYMENT_NAME"
aws lambda delete-function --function-name "$FUNCTION_NAME"
aws cloudformation delete-stack --stack-name "$STACK_NAME"
aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME"
aws iam detach-role-policy --role-name "$EXECUTION_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam detach-role-policy --role-name "$EXECUTION_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess
aws iam delete-role --role-name "$EXECUTION_ROLE_NAME"
aws logs delete-log-group --log-group-name "/aws/lambda/$FUNCTION_NAME"
rm "$LAMBDA_ENV_FILE"
```

## Local testing

Start `temporal server start-dev --ip 0.0.0.0`, a local collector, and the container with the
[AWS Runtime Interface Emulator](https://github.com/aws/aws-lambda-runtime-interface-emulator/releases).
This exercises the runtime protocol, not AWS deployment, IAM, the collector extension, or freeze/thaw.

```sh
docker build -t temporal-rust-lambda -f crates/sdk/examples/aws_lambda/Dockerfile .
docker run --rm -p 4317:4317 \
  -v "$PWD/crates/sdk/examples/aws_lambda/otel-collector-local.yaml:/etc/otelcol-contrib/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.156.0
```

In another terminal, use a matching-architecture Linux RIE binary:

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

Register and route the version, then run the starter:

```sh
curl --fail -X POST http://localhost:9000/2015-03-31/functions/function/invocations -d '{}'
temporal worker deployment set-current-version --deployment-name lambda-greetings --build-id local --yes
TEMPORAL_ADDRESS=localhost:7233 TEMPORAL_NAMESPACE=default TEMPORAL_TASK_QUEUE=lambda-greetings \
  cargo run -p temporalio-sdk --features examples --example aws-lambda-starter -- Temporal
```

While the starter waits, invoke the container from another terminal:

```sh
curl --fail -X POST http://localhost:9000/2015-03-31/functions/function/invocations -d '{}'
```

The starter prints the greeting and the collector logs Workflow/Activity spans and metrics. Repeat
with another name to exercise warm reuse. If Docker Desktop resolves `host.docker.internal` to an
unreachable IPv6 address, substitute the host's reachable IPv4 address for both endpoints.
