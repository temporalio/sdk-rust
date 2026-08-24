# Workflow Task Chunking

## Logical Workflow Tasks

In history, a physical Workflow Task (WFT) is framed by `WorkflowTaskScheduled`,
`WorkflowTaskStarted`, and `WorkflowTaskCompleted`; inbound events precede it and commands follow.

The worker processes history from a different alignment: the completion and commands of the
previous physical WFT, then new inbound events, then the scheduled/started pair for the current
WFT. Core calls each such processing unit a logical Workflow Task (LWFT).

Some physical WFTs need not become separate LWFTs. A failed or timed-out attempt committed no
workflow decision and can be folded into its retry. An otherwise empty WFT, such as a heartbeat
while a local activity is running, can be swallowed when doing so changes nothing observable by
workflow code. Chunking is the algorithm that determines when this is safe.

## Chunking versions

Core retains the original v1 chunker because changing how an existing history is grouped can
change activation jobs or workflow time and cause nondeterminism. v1 uses a small look-ahead
heuristic to identify empty heartbeat WFTs.

That heuristic is ambiguous around Updates. A live worker first receives an Update as a protocol
message on the current WFT, but replay sees a later `WorkflowExecutionUpdateAccepted` event. The
acceptance may follow other command events, such as `MarkerRecorded`, so a decision based only on
the immediately available events can differ between live execution, full replay, and paginated
replay.

The v2 chunker makes the required evidence explicit:

* An event that can cause workflow code to run prevents the surrounding WFT from being treated as
  an empty heartbeat.
* A pending speculative Update keeps the current live WFT separate.
* On replay, the complete command batch following the successor `WorkflowTaskCompleted` is scanned
  for `WorkflowExecutionUpdateAccepted`; it need not be the first command event.
* If a page ends before the command batch or next boundary is complete, the chunker requests more
  history instead of making a provisional decision.

Rejected Updates leave no acceptance event. Their validator activation may therefore be absent on
replay; validators cannot mutate workflow state or emit commands, so that difference is safe.

## Selecting a version

The first successful `WorkflowTaskCompleted` event is the only version-selection point for a
workflow run. Its `sdk_metadata.core_used_flags` determines the rule for the complete history:

* `WftChunkingV2` present: use v2 from the beginning of the run.
* `WftChunkingV2` absent: use v1 for the lifetime of the run.

Failed and timed-out attempts do not select a version. If `WftChunkingV2` first appears after a
flagless first completion, it has no effect and the run remains on v1. When a partial history does
not include the selection point and Core has not already latched the version for that run, it must
fetch history from event 1 before chunking.

The flag in durable history is authoritative. Worker configuration and an attempted completion do
not select a version because the completion may fail or lose a race.

## Rollout

The v2 reader ships before its writer is enabled. The writer is disabled by default and is opted
in with `TEMPORAL_USE_WFT_CHUNKING_V2=true` (or `1`). Writing requires a server that advertises SDK
metadata support and persists `WorkflowTaskCompleted.sdk_metadata.core_used_flags`; without that
capability, Core must leave the run on v1.

Rollout is reader before writer: first deploy a reader-capable release to every active, standby,
and rollback worker with the writer disabled; enable the writer only after those versions are the
minimum supported worker version.

Older workers can silently apply v1 to a flagged history, so they are not safe rollback targets
after any run adopts v2. Disabling the writer later does not make such workers safe readers.

Runs whose first successful completion is already flagless remain on v1. A long-running workflow
can adopt v2 by using Continue-As-New, which creates a new run with a new first completion.

This uses existing SDK metadata and does not change the Temporal API or history representation.
