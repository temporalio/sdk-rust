# temporalio-streams

Experimental. What every Store of a Temporal Stream shares: the error of a failed call, the order
of a Producer's epochs, and the stored form of a Record.

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

## Errors

A failed call gives a `StreamError` with a `Reason`. `StreamError::code` gives its gRPC code and
`StreamError::details` its encoded `StreamErrorDetails`, so every SDK raises the same error for
the same failure.

## The order of epochs

`epoch::epoch_key` gives the epoch of a Producer as 40 bytes that compare in order. A Store fences
by this order, so every Store refuses the same writes. `testdata/epochs.json` has the shared
vectors. `examples/gen_vectors.rs` writes them from a second implementation that shares no code
with the crate. Run it with `cargo run -p temporalio-streams --example gen_vectors`.

## The stored form of a Record

`record::data_record` gives the bytes that a Store keeps for one Record. They have the Producer of
the Record and its body, and no offset. `record::check_bounds` checks a batch against
`MAX_BATCH_RECORDS`, `MAX_BATCH_BYTES` and `MAX_RECORD_BYTES`. The sizes are of the stored form.

## Protos

The crate defines no protos. `temporalio-protos` generates them, and `temporalio_streams::proto`
re-exports them, so a value needs no conversion between the crates.
