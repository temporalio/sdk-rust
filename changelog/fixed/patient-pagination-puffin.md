Prevent replay from skipping buffered history events when a cache-miss reload advances beyond a stale workflow task and needs another page.
Reject history updates that would discard unread events, preserving the buffered history for workflow task failure handling.
