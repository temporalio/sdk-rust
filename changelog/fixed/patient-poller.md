A non-retryable error that ends a long task poll no longer stops the Worker at once. The 60-second grace for such errors now starts at the first one, not when the poll started.
