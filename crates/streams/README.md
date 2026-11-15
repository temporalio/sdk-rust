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

A lang bridge implements the trait for a Store that is written in that language. The Stores that
Temporal provides, and the shared rules that a Store in Rust builds on, are in
[temporal-community/sdk-streams](https://github.com/temporal-community/sdk-streams).

## The runtime

`StreamRuntime::new` builds the runtime on one Store. It has one typed method for each operation.
`StreamRuntime::call` is the byte form of the same methods. It takes `CreateStream`,
`AppendRecords`, `CloseStream`, `ReadRecords` or `DescribeStream` with the encoded request and
gives the encoded response. Any other name fails with `Reason::Unsupported`.

The runtime checks each request before the Store sees it, and the answer to a create, an append, a
read or a describe after it. The checks are the same for every Store.

- A Stream id and a Producer id are opaque text of 1 to 4096 bytes. The caller builds both.
- A Workflow Producer appends and closes with its Workflow Task as the epoch, so that a Store can
  refuse a write of an older task.
- A batch has at most 1000 Records and 4 MiB, and a Record at most 1536 KiB. The sizes are of the
  stored form, in which each Record has its Producer.
- A read gives at most 1000 Records, and the runtime asks the Store to wait at most 60 s. A page
  over the caps of its request is cut.
- The offsets of a page rise, start after the read's `from_offset`, and end at or before its
  `last_offset`.
- An answer that breaks these rules, a create with no ref, or a describe with no status fails with
  `Reason::StorageCorrupt`.

## The digest of a read

`digest::records_digest` gives a 16-byte digest of the Records that a read gave. Core keeps it in
History when a Workflow reads a Stream, and checks it when a replay reads the same Records again.
`testdata/record_digests.json` has the shared vectors. `examples/gen_vectors.rs` writes them too.

## The order of epochs

`epoch::epoch_key` gives the epoch of a Producer as 40 bytes that compare in order. A Store fences
by this order, so every Store refuses the same writes. `testdata/epochs.json` has the shared
vectors. `examples/gen_vectors.rs` writes them from a second implementation that shares no code
with the crate. Run it with `cargo run -p temporalio-streams --example gen_vectors`.

## Protos

The crate defines no protos. `temporalio-protos` generates them, and `temporalio_streams::proto`
re-exports them, so a value needs no conversion between the crates.
