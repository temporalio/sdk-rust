# temporalio-streams

Experimental. The Stream runtime for Core and the Temporal SDK bridges. It answers the five
operations of the Stream service on a Stream Store, and it has no Store of its own.

## The Store

A `StreamStore` answers each operation whole: `create_stream`, `append_records`, `read_records`,
`close_stream` and `describe_stream`. It takes and gives the messages of the Stream service. The
Store applies the rules of the service itself.

- An append fences a lower epoch of its Producer, and writes a batch that comes again with the
  same Producer, epoch and sequence once.
- A read waits up to its `wait` for a Record when it has none to give, and a close wakes it.
- An offset is opaque bytes, at most 1024, and the offsets of one Stream compare in order.
- A failed call gives a `StreamError` with its reason. `StorageUnavailable` is the one reason
  that says a later try can work.
- `limits` gives the limits of one batch on the Store. The default is the largest batch that any
  Store may take.

A lang bridge implements the trait for a Store that is written in that language. The Stores that
Temporal provides, and the shared rules that a Store in Rust builds on, are in
[temporal-community/sdk-streams](https://github.com/temporal-community/sdk-streams).

## The runtime

`StreamRuntime::new` builds the runtime on one Store. It has one typed method for each operation:
`create_stream`, `append_records`, `read_records`, `close_stream` and `describe_stream`.

The runtime checks each request before the Store sees it, and the answer to a create, an append, a
read or a describe after it. The checks are the same for every Store.

- A Stream id and a Producer id are opaque text of 1 to 4096 bytes. The caller builds both.
- A Workflow Producer appends and closes with its Workflow Task as the epoch, so that a Store can
  refuse a write of an older task.
- A batch is within the limits of its Store. They are at most 1000 Records and 4 MiB for a batch,
  and 1536 KiB for a Record. The sizes are of the stored form, in which each Record has its
  Producer.
- A read gives at most 1000 Records, and the runtime asks the Store to wait at most 60 s. A page
  over the caps of its request is cut.
- The offsets of a page rise, start after the read's `from_offset`, and end at or before its
  `last_offset`.
- An answer that breaks these rules, a create with no ref, or a describe with no status fails with
  `Reason::StorageCorrupt`.

## Errors

A failed call gives a `StreamError` with a `Reason`. `StreamError::code` gives its gRPC code and
`StreamError::details` its encoded `StreamErrorDetails`, so every SDK raises the same error for
the same failure.

## The digest of a read

`digest::records_digest` gives a 16-byte digest of the Records that a read gave. Core keeps it in
History when a Workflow reads a Stream, and checks it when a replay reads the same Records again.
`testdata/record_digests.json` has the shared vectors. `examples/gen_vectors.rs` writes them too.

## The order of epochs

`epoch::epoch_key` gives the epoch of a Producer as 40 bytes that compare in order. A Store fences
by this order, so every Store refuses the same writes. `testdata/epochs.json` has the shared
vectors. `examples/gen_vectors.rs` writes them from a second implementation that shares no code
with the crate. Run it with `cargo run -p temporalio-streams --example gen_vectors`.

## The stored form of a Record

`record::data_record` gives the bytes that a Store keeps for one Record. They have the Producer of
the Record and its body, and no offset. `record::BatchLimits` has the limits of one batch: its
Records, its bytes, and the bytes of one Record. The default is `MAX_BATCH_RECORDS`,
`MAX_BATCH_BYTES` and `MAX_RECORD_BYTES`, and a Store can set less. The sizes are of the stored
form.

## Protos

The crate defines no protos. `temporalio-protos` generates them, and `temporalio_streams::proto`
re-exports them, so a value needs no conversion between the crates.
