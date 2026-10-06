# Workflow Task Chunking

A physical Workflow Task (WFT) starts with `WorkflowTaskScheduled` and
`WorkflowTaskStarted`, then records its completion and any command events. Core
feeds workflow code a logical task ending at `WorkflowTaskStarted`: it processes
commands from the previous completion, inbound events, and the next task's start.

Failed and timed-out attempts made no durable decision and can be folded into a
retry. Successful tasks require more care. A commandless completion can represent
an activity waiter updating workflow state, while a truly empty task can be a
heartbeat for an outstanding local activity. Folding those tasks indiscriminately
can change which state an Update observes during replay.

## Boundary rules

The original chunker remains available as v1 for existing runs. The opt-in v2
scanner preserves a task that consumes inbound events or produces commands. It
folds an otherwise empty heartbeat only after checking the successor's outcome
and complete command batch.

Updates provide additional boundary evidence:

* Live protocol messages carry sequencing event IDs. Their preceding successful
  task must be preserved so Update code runs after that task's activation.
* Durable `WorkflowExecutionUpdateAdmitted` events provide the same evidence on
  replay; failed starts do not become boundaries.
* An accepted Update can follow other commands, including local activity or patch
  markers. The scanner checks the entire batch after `WorkflowTaskCompleted`
  before folding its predecessor. An ignorable unknown command also prevents
  folding, since its effect cannot be determined by this reader.

A page ending within either relevant command batch provides insufficient evidence.
The scanner waits for another page instead of emitting a provisional boundary.
The paginator returns all fetched events, including partial tails. The run appends
new pages to the unconsumed tail and decides its boundaries there. During v2 replay,
local activity markers, patch markers, and accepted Updates are preprocessed only
from the command batch belonging to the current physical task.

## Run version and rollout

`CoreInternalFlags::WftChunkingV2` (4) on the **first successful**
`WorkflowTaskCompleted` selects v2 for the entire run. Its absence selects v1.
Failed attempts select neither. Later completions cannot change that choice, so
there is no mid-run cutover. A new continue-as-new run can select v2.

The choice belongs to the existing workflow machine instance. Sticky tasks reuse
it; cache misses already fetch history from event 1 and recover it from durable
metadata. There is no separate registry of run versions or completion-RPC latch.
An attempted first completion keeps its flag staged until history confirms the
successful completion, so a failed RPC cannot select a version or consume the flag.
Unknown Core flags at the selection point fail the task as incompatible history.

Readers always honor the recorded flag. Writers are disabled by default; set
`TEMPORAL_USE_WFT_CHUNKING_V2=true` or `1` before starting the process to opt in
(case-insensitive `true` is accepted). The process reads this setting once. Only
runs without a successful completion can record the flag, and only when the server
advertises SDK metadata support. Without that capability, execution uses v1.

Deploy readers to every worker that might process these runs, including rollback
versions, before enabling writers. Existing flagless runs keep their original
chunking even on a worker with the writer enabled.

## Remaining limitation

An externally forced task with no observable events or commands may still be
folded. Workflow code that observes only the task's clock can distinguish such a
task from a local activity heartbeat. Fixing that ambiguity requires an explicit
server or SDK indication that a task may be skipped.
