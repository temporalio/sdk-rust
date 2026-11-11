#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

# Must match the features the CI build step uses, or cargo rebuilds inside the starter's timeout.
features="examples,experimental"

mapfile -t examples < <(
  cargo metadata --no-deps --format-version 1 \
    | jq -r '.packages[]
      | select(.name == "temporalio-sdk")
      | .targets[]
      | select(.kind[] == "example")
      | .name'
)

has_example() {
  local expected="$1"
  local example
  for example in "${examples[@]}"; do
    if [[ "$example" == "$expected" ]]; then
      return 0
    fi
  done
  return 1
}

pids=()

cleanup() {
  local pid
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  pids=()
}

trap cleanup EXIT

for starter in "${examples[@]}"; do
  if [[ "$starter" != *-starter ]]; then
    continue
  fi

  sample="${starter%-starter}"
  worker="${sample}-worker"

  if ! has_example "$worker"; then
    echo "Skipping $starter because $worker is not declared"
    continue
  fi

  # Mock services a sample depends on, such as a local S3 server, start before its worker.
  for example in "${examples[@]}"; do
    if [[ "$example" == "$sample"-*mock ]]; then
      cargo run -p temporalio-sdk --features "$features" --example "$example" &
      pids+=($!)
    fi
  done

  cargo run -p temporalio-sdk --features "$features" --example "$worker" &
  pids+=($!)

  set +e
  timeout 20 cargo run -p temporalio-sdk --features "$features" --example "$starter"
  starter_status=$?
  set -e

  cleanup

  if [[ "$starter_status" -ne 0 ]]; then
    exit "$starter_status"
  fi
done
