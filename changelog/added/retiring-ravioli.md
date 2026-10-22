`Worker::run_until` consumes a worker and runs it until a supplied future completes, then shuts it down and releases its worker registration before returning, including when running fails.
