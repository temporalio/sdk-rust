Added buffering for activity worker commands received before their activity task. Cancellation commands are retained in a bounded shared cache for up to one minute and applied when the matching task arrives.
Added the `temporal_worker_command_cache_events` counter to track buffered, expired, and evicted entries by namespace and command type.
