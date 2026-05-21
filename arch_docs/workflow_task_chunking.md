# Workflow Task Chunking

## Workflow Tasks Model

Conceptually, Workflow Tasks are usually presented using the following model:

- \[Preceding Events\] (optional)
- WFT Scheduled
- WFT Started
- WFT Completed
- \[Command Events\] (optional)

That is, some events happen that the workflow needs to react to (Preceding Events), so the server
schedules a Workflow Task (WFT Scheduled). A worker then polls the task (WFT Started), and executes
workflow code. The workflow may produce commands, which are sent back to the server when the task
completes (WFT Completed). The server interprets those commands and records the corresponding
history events (Command Events).

Commands and events are both "optional" in the sense that:

- Workflow code, after being woken up, may not do anything and produce no new commands.
- There may be no inbound events, for a few reasons:
  1. The workflow might have been running a long-running local activity (LA). In that case the
     workflow must "WFT heartbeat" — completing the current WFT with no commands while the LA is
     still in progress — in order to avoid timing out the WFT on the server.
  2. The workflow might have received an Update, which is not represented as a history event but
     rather as a "protocol message" attached to the task.
  3. The server can occasionally force a new WFT via some obscure internal APIs.

## Logical Workflow Tasks Model

Though useful as a general mental model, the sequence described previously does not faithfully align
with how Workflow Tasks are actually _processed_ by a Worker.

Indeed, when a given Workflow Task is processed for the first time, that Task has obviously not yet
been completed and no commands have yet been produced, so the corresponding events are not present
in history. The Workflow Task handed to the Worker therefore ends at the WFT Started event. The WFT
Completed and Command Events resulting from that WFT execution only become visible to the Worker on
the _next_ Workflow Task.

Therefore, from the perspective of how the Worker advances through a Workflow history, the
processing unit is better described as follows:

- WFT Completed
- [Command Events] (optional)
- [Preceding Events] (optional)
- WFT Scheduled
- WFT Started

Another particularity of the processing-oriented model is that it is possible, under some
conditions, for multiple consecutive Workflow Tasks to be collapsed into a single processing unit.
It is notably the case of Workflow Tasks that completed unsuccessfully (i.e. failed or timed out),
as well as most Workflow Tasks that belong to a Workflow Task Heartbeat sequence.

This processing-oriented model is what we call **logical Workflow Tasks** (LWFT). Note that LWFT do
not represent a different history: all events in the Workflow Execution history remain present and
in the same order. It only changes how those events are aligned and grouped into processing units as
the Worker advances through history.

// FIXME: What are absolute must-haves for LWFT?

## Workflow History Pagination

There are various ways The Worker obtains Workflow history from the server.

## Chunking history into Logical Workflow Tasks

It is critical that, on replay, Workflow history be split into LWFTs that correctly align with the
LWFTs processed during the original execution. Differences in how events are grouped affect how and
when those events are presented to workflow code. That, in turn, may affect which commands the
workflow produces, or when it produces them, resulting in non-determinism errors.

It is also critical that

Note that we said that LWFTs have to _align_ with the original execution, not that they have to be
_strictly the same_. In some cases, it is

One source of complexity in Core is the chunking of history into "logical" Workflow Tasks.

A LWFT that contains no inbound events, no commands, and no protocol messages is a no-op from the
workflow's perspective. To avoid waking lang for no purpose, Core collapses such empty LWFTs into
the preceding non-empty one.

Deciding which sequences of WFTs are safe to collapse is the job of the WFT chunking algorithm.

Two chunking algorithms exist: **v1** (the legacy heuristic) and **v2** (the current, more rigorous
algorithm). Both ship in this version of Core. Which one a given workflow uses is recorded as the
`WftChunkingV2` SDK flag on its first `WorkflowTaskCompleted` event. Workflows that don't carry the
flag use v1; workflows that do, use v2. v2 is opt-in for new workflows via the
`TEMPORAL_USE_WFT_CHUNKING_V2` environment variable.

## v1 — heartbeat detection heuristic

v1 (`find_end_index_of_next_wft_seq_v1`) walks history events linearly. At each
`WorkflowTaskStarted`, it peeks two events ahead and applies a simple heuristic to
decide whether the current WFT is a "WFT heartbeat" and should therefore be collapsed
into the next one. In essence:

```rust
if !saw_command && next_next_event.event_type() == EventType::WorkflowTaskScheduled {
    // This WFT contained no commands, and history immediately rolls into the next
    // WFTScheduled with no intervening events. Treat it as a heartbeat: don't
    // conclude the current LWFT here, keep walking.
    continue;
}
```

`saw_command` tracks whether any _command event_ — i.e. an event generated by a worker
command, like `ActivityTaskScheduled`, `TimerStarted`, `MarkerRecorded`, etc. — appeared
before the current `WorkflowTaskStarted`. The combination "no commands + no inbound
events between WFTCompleted and the next WFTScheduled" is v1's signature of a heartbeat.

This compact heuristic is the heart of v1's chunking decisions.

### Downsides of v1

v1's compactness comes at the cost of accuracy. Several edge cases led to incorrect
chunking and, in some cases, non-determinism errors (NDE) at replay time:

1. **Updates after empty WFTs.** Workflow Updates are delivered as protocol messages
   attached to a WFT, not as history events. When a server delivers an Update right
   after a WFT heartbeat, v1 sees the heartbeat as "empty" (no commands, nothing
   between WFTCompleted and the next WFTScheduled) and collapses it into the next
   WFT. The Update is then delivered as part of the combined LWFT, but the original
   execution produced two separate activations — the heartbeat, then the Update.
   On replay this discrepancy typically manifests as an NDE.

2. **Brittle handling of paginated history.** v1's heuristic needs to look two events
   past each `WorkflowTaskStarted` to commit a decision. When history is fetched in
   pages, page boundaries can fall mid-decision. v1 returns `Incomplete(...)` in
   that case, but the paginator's handling of `Incomplete` interacts subtly with
   the heuristic, and the chunking outcome can diverge from the original run.

3. **Inbound events could be absorbed into a heartbeat chain.** v1's heuristic
   tracks only _command_ events explicitly (via the `saw_command` flag). Other
   event types that can affect workflow state — signals, timer fires, accepted
   updates, child-workflow events, etc. — are only handled indirectly through the
   two-event look-ahead at each `WorkflowTaskStarted`. In some scenarios — most
   visibly when the heuristic interacts with paginated history, but also in plain
   in-memory chunking when events fall just-so — v1 ended up considering WFTs that
   actually carried inbound events as part of the preceding heartbeat chain.
   Because a collapsed LWFT presents _one_ timestamp (that of the first WFT in
   the chain) to user code, any code that runs for the absorbed WFT observes
   `workflow.now()` at the wrong moment in time, which leads to a non-determinism
   error at replay.

These problems motivated the v2 rewrite.

## v2 — explicit state machine with time-sensitive events

v2 (`find_end_index_of_next_wft_seq_v2`) replaces v1's compact heuristic with an
explicit state machine over the structure of WFT sequences in history. Three ideas
make v2 more robust than v1:

### 1. Time-sensitive events explicitly prevent collapsing

v2 introduces the concept of a **time-sensitive event**: any event that can cause user
workflow code to run on replay. Examples include `WorkflowExecutionStarted`,
`WorkflowExecutionSignaled`, `TimerStarted`/`TimerFired`, `ActivityTaskScheduled` and
its terminal counterparts, `WorkflowExecutionUpdateAccepted`, child-workflow events,
Nexus events, and so on. Events that cannot independently wake user code — WFT framing
events, `MarkerRecorded`, `UpsertWorkflowSearchAttributes`, etc. — are _not_
time-sensitive.

The full classification lives in `HistoryEvent::is_wft_time_sensitive_event` and uses
an exhaustive `match` (no catch-all) so any new event type added to the proto must be
explicitly classified.

While walking events, v2 maintains a `prevent_heartbeat` flag. Once any time-sensitive
event is seen, the flag is latched on and v2 will not collapse the current LWFT into
the next WFT. This replaces v1's implicit "saw a command" check with an explicit,
complete list of which event types force a boundary.

### 2. Pending speculative Updates are taken into account

The chunker is parameterized with `has_pending_speculative_updates` — set true when the
current task carries Update protocol messages that haven't yet produced
`WorkflowExecutionUpdateAccepted` events in history. When set, v2 refuses to collapse
the **last** WFT in a heartbeat chain into the preceding chain, because the pending
Update message must be delivered in its own activation in order to match the original
execution. Earlier (non-last) heartbeats in the same chain can still be collapsed
together.

This is the structural fix for the "Update after heartbeat" class of NDE that v1
caused.

### 3. Bounded look-ahead with explicit `NeedMore`

v2 looks up to five events past the current `WorkflowTaskStarted` to make its
decision, and exposes its outcome via a three-variant enum:

- `Complete(ix)` — a LWFT boundary has been found at the `WorkflowTaskStarted` at
  index `ix`.
- `NeedMore` — not enough events are available to commit a decision; the caller
  should fetch more history pages and retry.
- `Tail` — no more LWFT boundaries exist in the slice; any remaining events are
  trailing matter (terminal `WorkflowExecution*` events, `WorkflowTaskCompleted`
  with commands).

When the look-ahead runs past the end of the slice, v2 either commits a decision
(if the slice covers the last WFT of the workflow) or returns `NeedMore`. This
moves "I'm not sure yet, fetch more" out of the look-ahead's blind spot and into
an explicit return value, which the paginator can act on cleanly.

### How v2 addresses each v1 downside

1. **Update-after-heartbeat NDE.** Fixed structurally: `has_pending_speculative_updates`
   prevents the last WFT in a heartbeat chain from being collapsed.

2. **Pagination correctness.** Fixed by the explicit `NeedMore` return value, which
   gives the paginator a clean signal to fetch more events rather than relying on
   ambiguous `Incomplete` handling.

3. **Inbound events absorbed into a heartbeat chain.** Fixed by
   `is_wft_time_sensitive_event` and the `prevent_heartbeat` flag — every event
   type that can wake user workflow code is enumerated and classified explicitly,
   and seeing any one of them latches the chunker out of heartbeat-collapsing
   mode for the remainder of the current LWFT. A WFT that carries any such event
   can no longer be silently merged with a preceding heartbeat chain, so user
   code always observes `workflow.now()` at the timestamp of the WFT it really
   belongs to.

## Rollout

Per Core's SDK-flag rollout policy, this version of Core ships **support** for v2 but
leaves it off by default. Operators opt in via the `TEMPORAL_USE_WFT_CHUNKING_V2`
environment variable. When set to a truthy value (`"true"` or `"1"`, case-insensitive),
the worker, for each newly started workflow:

- records `WftChunkingV2` in `sdk_metadata.core_used_flags` on the first
  `WorkflowTaskCompleted` event, and
- uses v2 chunking for that workflow from then on, regardless of whether the env
  variable is set on subsequent worker versions (the flag in history is the source
  of truth).

Workflows that started without the flag — either before opt-in, or on a worker where
opt-in was not enabled — continue to use v1 indefinitely. This guarantees that a
worker can be rolled back to the previous version without stranding workflows that
already adopted v2, since the previous version also understood the flag (it just
didn't write it by default).
