Workflows no longer fail workflow tasks when a Nexus operation cancelled with `NexusOperationCancellationType::Abandon` later completes, fails, or times out.
Cancelling a Nexus operation with `Abandon` or `TryCancel` before it starts now resolves its start future with a cancellation failure right away, and the operation's later start or outcome no longer resolves it again.
A Nexus operation using `WaitCancellationRequested` whose cancel request fails is no longer resolved a second time when it later finishes.
