# Workflow activation batches

All Core language integrations exchange `WorkflowActivationBatch` and
`WorkflowActivationBatchCompletion`. This is an unconditional breaking interface change: there is
no batching feature or capability negotiation. The C bridge's existing workflow poll and completion
functions now encode and decode these batch messages. Rust integrations use
`poll_workflow_activation_batch` and `complete_workflow_activation_batch`.

Each nonempty batch belongs to one workflow run. Different runs can execute concurrently, but a
run has only one outstanding batch. SDKs must accept variable batch lengths, including singleton
batches. Continue polling and completing outstanding work during shutdown until polling reports
shutdown. Complete different runs concurrently because completions may need history pagination.

## Language execution and results

Execute each activation on the same workflow instance, in order, using its own clock, replay state,
flags, and workflow metadata. Complete runnable work, microtasks, condition checks, failure checks,
and completion processing at each boundary. An activation with no jobs still advances workflow
execution. Preserve activation timeouts and deadlock detection separately for each activation.
Queries and cache removal retain their existing semantics and isolation.

Return one batch completion containing a nonempty contiguous prefix of
`IndexedWorkflowActivationCompletion` results. The zero-based `activation_index` identifies the
activation that produced each result. Include its complete success or failure and metadata even
when it produced no commands. Preserve commands on every result rather than combining commands
from different activations. Core applies them in order through its existing replay machinery.
Commands that leave no history events, such as starting and cancelling a timer in one activation,
remain valid.

Each batch supplies `allowed_continuation_commands`, an allowlist of `WorkflowCommandKind`
identities. Stop at the first failure or after any command whose kind is absent from that list.
Continue after a success only if all its commands are allowed; an empty list permits only
command-free successes. Unknown command kinds always stop execution, and unknown allowlist values
are ignored. Command-kind values match the command oneof's field numbers and remain stable.

Return that completion as the last result and leave the remaining activations unexecuted. Core
will send fresh work; do not resume the old suffix or rerun the completed prefix. Rust's
`WorkflowActivationBatch::can_continue_after` applies the supplied policy. Languages must not
hard-code command-specific continuation rules: Core can expand the allowlist for future batching
schemes without changing the language execution loop. Core retains the exact dispatched policy
and rejects a response that executes beyond a disallowed command.

## Construction from available history

Core collects the replay activations it can determine from the buffered history, without a fixed
activation-count limit or requiring pagination to finish. A batch can contain the entire replay,
including initialization, or an available suffix. Only completed replay tasks with the necessary
lookahead are included; live work remains separate.

The projection includes signals, empty tasks, timer firings, workflow cancellation, patch
notifications, and resolutions whose command state Core already knows. Timer sequence numbers
come from the recorded timer IDs. Other stateful events are projected using private copies of
their existing machines; Core stops before events whose language sequence mapping or intervening
Core work depends on a command it has not received. Each activation retains its own clock, flags,
SDK version, deployment information, and history metadata, including changes within the batch.

The authoritative machines remain at the first activation boundary until the language returns
actual completions. Before applying each later result, Core processes the preceding commands and
history and compares the resulting activation with the projection, retaining normal replay and
nondeterminism checks. Core currently permits continuation after `StartTimer` and `CancelTimer`;
all other command kinds end the executed prefix even when more activations were supplied.

Legacy chunked activations, live work, queries, and evictions use singleton batches. Local activity
and update dependencies, unsupported events, or missing lookahead end accumulation. This affects
batch size only: the batch protocol is always used whether chunking is enabled or disabled and
regardless of the `disable_workflow_task_chunking` Cargo feature.

Batching adds no history flags, markers, or recording changes. The separate chunking transition
continues to control history boundaries as described in [workflow task chunking](workflow_task_chunking.md).
