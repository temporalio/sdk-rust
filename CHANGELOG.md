<!--
High-level release notes.
Loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

When your PR includes a user-facing change, add an entry below under the
appropriate heading (create the heading if it does not yet exist). Within
each heading content can be free-form. Feel free to include examples, links
to docs, or any other relevant information.

### Added            — new features
### Changed          — changes in existing functionality
### Deprecated       — soon-to-be-removed features
### Breaking Changes — removed or backwards-incompatible features
### Fixed            — notable bug fixes
### Security         — notable security fixes
-->

# Changelog

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
* `GetSystemInfo` connection initialization now only falls back to empty server capabilities when
  `UNIMPLEMENTED` indicates the RPC method is missing. Other `UNIMPLEMENTED` responses are
  reported as connection errors.
* Connection initialization now retries once with gRPC compression disabled if the eager
  `GetSystemInfo` call fails because the server cannot decompress gzip.
* Awaiting a Nexus operation's result (`StartedNexusOperation::result()`) no longer trips
  nondeterminism detection ("a waker was invoked by a non-SDK source", TMPRL1100) on replay. The
  result future is a `Shared`, whose internal waker machinery must be polled inside an `SdkWakeGuard`
  (as `join_all` already is); it now is. Previously, a workflow that awaited a Nexus operation result
  and then kept running (e.g. parked on a `wait_condition`) would fail its workflow task whenever it
  was replayed — breaking queries and durable recovery for that execution.

### Breaking Changes
* The `ActivityContext` constructor now requires `ClientOptions`.
### Breaking Changes

- Rust SDK `ApplicationFailure` and `WorkflowError` APIs now use boxed `std::error::Error` values instead of
  `anyhow::Error`.

## [Unreleased]

### Fixed
* OTLP metric export failures are now logged through Core telemetry when OpenTelemetry's periodic
  metric reader reports an export error.
