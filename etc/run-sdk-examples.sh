#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

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

# Runs each `<sample>-starter` example in the package against its `<sample>-worker`. The remaining
# arguments are passed to cargo, and must match the CI build step, or cargo rebuilds inside the
# starter's timeout.
run_examples() {
  local package="$1"
  shift
  local cargo_args=("$@")

  local examples
  mapfile -t examples < <(
    cargo metadata --no-deps --format-version 1 \
      | jq -r --arg package "$package" '.packages[]
        | select(.name == $package)
        | .targets[]
        | select(.kind[] == "example")
        | .name'
  )

  local starter sample worker example
  for starter in "${examples[@]}"; do
    if [[ "$starter" != *-starter ]]; then
      continue
    fi

    sample="${starter%-starter}"
    worker="${sample}-worker"

    if [[ ! " ${examples[*]} " == *" $worker "* ]]; then
      echo "Skipping $starter because $worker is not declared"
      continue
    fi

    # Mock services a sample depends on, such as a local S3 server, start before its worker.
    for example in "${examples[@]}"; do
      if [[ "$example" == "$sample"-*mock ]]; then
        cargo run -p "$package" "${cargo_args[@]}" --example "$example" &
        pids+=($!)
      fi
    done

    cargo run -p "$package" "${cargo_args[@]}" --example "$worker" &
    pids+=($!)

    set +e
    timeout 20 cargo run -p "$package" "${cargo_args[@]}" --example "$starter"
    local starter_status=$?
    set -e

    cleanup

    if [[ "$starter_status" -ne 0 ]]; then
      exit "$starter_status"
    fi
  done
}

run_examples temporalio-sdk --features examples
run_examples temporalio-aws-s3driver
