#!/usr/bin/env bash
set -euo pipefail

SAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
REPOSITORY_DIR="$(cd "$SAMPLE_DIR/../../../.." && pwd)"
cd "$REPOSITORY_DIR"

cargo lambda build --release --x86-64 --package temporalio-sdk --example aws-lambda-worker \
  --features examples,experimental,opentelemetry,temporalio-common/vendored-protox \
  --output-format zip
# The collector layer loads its configuration from the function's deployment archive.
zip -j target/lambda/aws-lambda-worker/bootstrap.zip "$SAMPLE_DIR/otel-collector-config.yaml"
