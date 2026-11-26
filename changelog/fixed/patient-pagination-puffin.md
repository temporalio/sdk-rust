Preserve buffered history and patch markers during paginated replay when a cache-miss reload advances beyond a stale workflow task.
Continue paginated replay through workflow tasks with no work for workflow code instead of failing the task.
Reject history updates that would discard unread events, preserving the buffered history for workflow task failure handling.
