# Standalone Activities

This sample shows activities that run on their own, started directly from a client instead of being
orchestrated by a workflow. The activity and the worker are written exactly as they would be for a
workflow activity; only the way the activity is started differs.

The client crate has no single "execute" call. To run an activity and wait for it, start it and then
await the handle's result.

### Running this sample

1. `temporal server start-dev` to start the Temporal server. Standalone activities require
   Temporal CLI v1.9.1 or later.
2. In another terminal, start the worker:

```bash
  cargo run --features examples --example standalone-activities-worker
```

3. In another terminal, start an activity and wait for its result:

```bash
  cargo run --features examples --example standalone-activities-execute
```

It should print:

    Activity result: Hello, Temporal!

### The other starters

Start an activity without waiting for it, printing the activity and run IDs:

```bash
  cargo run --features examples --example standalone-activities-start
```

Get a handle to an activity started earlier, describe it, and read its result. Run one of the two
starters above first, since this looks up the activity ID they use:

```bash
  cargo run --features examples --example standalone-activities-get-handle
```

List and count the activity executions on this sample's task queue:

```bash
  cargo run --features examples --example standalone-activities-list
  cargo run --features examples --example standalone-activities-count
```
