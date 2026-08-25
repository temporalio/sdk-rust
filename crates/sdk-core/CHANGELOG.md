<!--
High-level release notes.
Loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This file serves users of the other Temporal SDKs, whose workers and clients run on
Core. The repository-root CHANGELOG.md serves users of the Rust SDK.

Log a change here only if a user of one of those SDKs can observe it: different behavior,
an option surfaced to them, a new log or metric, a different interaction with the server.
The question is what the user sees, not which crate or which files your PR touched.

The Rust SDK runs on Core too, so a user-observable change in Core behavior normally
belongs in the root changelog as well, worded for each audience. What belongs only here is
what only the other SDKs' users can see — a Core capability the Rust SDK does not surface,
or a C-bridge change. A Rust-level API change that a language SDK absorbs inside its own
bridge, without its users noticing, belongs in neither file.

When your PR includes a user-facing change, add an entry below under the
appropriate heading (create the heading if it does not yet exist) in the
Unreleased section — never under a released version. Within each heading content
can be free-form. Feel free to include examples, links to docs, or any other
relevant information.

### Added            — new features
### Changed          — changes in existing functionality
### Deprecated       — soon-to-be-removed features
### Breaking Changes — removed or backwards-incompatible features
### Fixed            — notable bug fixes
### Security         — notable security fixes
-->

# Changelog

## Unreleased

### Added
* Core console logs can now be emitted as newline-delimited JSON when an SDK selects the JSON log
  format. Configured log filters continue to apply to JSON output.
* Workflow completion-as-cancelled commands can now carry details for recording on the terminal
  history event.
* Worker heartbeats now report the SDK runtime, hosting environments, operating system, and
  architecture once per worker, retrying until the first successful delivery. Runtime options can
  disable the reporting.
* Workers now log a `[TMPRL1104]` warning when a workflow task takes longer than 5 seconds. Set
  `TEMPORAL_WORKFLOW_TASK_DURATION_WARN_SECONDS` to change the threshold.
* Core now supports attaching `EventGroupMarker`s to most workflow commands.
* The `temporal_activity_execution_failed` and `temporal_local_activity_execution_failed` worker
  metrics now carry a `failure_reason` attribute. Each is now split into one time series per
  reason, which may affect existing dashboards.

### Breaking Changes :boom:
* Activity failures now include the latest heartbeat details atomically instead of force-flushing a
  throttled heartbeat first. Temporal Server 1.16.0 or newer is required to guarantee those details
  are preserved on failure; workers warn when the server does not advertise support.

### Fixed
* The Prometheus exporter now appends `_total` to counter metric names when an SDK enables the
  counter suffix option.
* An activity failure caused by oversized final heartbeat details is now counted in the
  `temporal_activity_execution_failed` metric as `failure_reason="PayloadsTooLarge"`. Previously it
  was counted under the reason for the failure the activity itself reported, and was not counted at
  all when that failure was benign, even though a payload-limit failure was reported instead.
* Workers now warn when autoscaling task polling encounters errors continuously for one minute.
  Repeated warnings use exponential backoff up to 15-minute intervals and stop after polling
  recovers.
* Workers no longer send worker heartbeats or appear in centralized heartbeat reports before they
  begin polling.
* Ephemeral server processes no longer leak on failed start.
* Local activity resolutions are now delivered to workflows as each activity completes instead of
  waiting for every local activity in the workflow task. This allows sequences of short local
  activities to make progress while a long-running local activity executes in parallel, while
  preserving the resolution ordering recorded in existing histories during replay.
* Try-cancel child workflows no longer cause nondeterminism when they complete or fail after their
  cancellation was requested.
* Nexus tasks are now timed out locally even when the server sends a `request-timeout` header that
  falls outside the Nexus duration grammar, such as a negative value for a task whose deadline has
  already elapsed, a sub-millisecond unit, or a multi-unit value like `1m30s`. Previously such a
  header was ignored entirely, so the handler was never told the task had timed out, and a task
  left unanswered could block worker shutdown indefinitely.
