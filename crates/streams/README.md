# temporalio-streams

Experimental. The Stream runtime that the Temporal SDKs link into their bridges. It answers the five
operations of the Stream service on a Store. A bridge builds the runtime per client, as a plain
object, and calls `StreamRuntime::call` with an operation name and the encoded request. The call
gives back the encoded response. Core has no Stream code.

## Protos and shared vectors

The crate defines no protos. `temporalio-protos` generates them, and `temporalio_streams::proto`
re-exports them, so a value needs no conversion between the crates.

- `temporal/api/stream/v1/message.proto`, `temporal/api/streamservice/v1/request_response.proto`
  and `temporal/api/enums/v1/stream.proto` are the Stream protos of the API. They are in
  `crates/protos/protos/api_upstream`.
- `testdata/*.json` are the shared vectors. They cover the Store keys of a Stream id, the meta
  field of a Producer, offsets, epoch keys and the batch digest of a dedupe entry.
  `examples/gen_vectors.rs` writes them from a second implementation, and the crate's tests read
  each file. Run it with `cargo run -p temporalio-streams --example gen_vectors`. It shares no
  code with the crate. Each Store reads `fields.json` through the shared Store suite.
  `digests.json` is the golden of the crate's batch digest.

A Stream id and a Producer id are opaque text. The caller builds both. The runtime refuses an
empty id of either kind, and one over 4096 bytes. A Producer id names one Producer in one Stream.
A Store keeps the fence of a Producer by kind and id, and an epoch has no run. So the id of a
run-based kind must hold its run id. A caller that uses one id in two runs gets `FENCED` in the
second run when that run's epoch is lower.

## The runtime

`StreamRuntime` serves the Stream service on one Store, with one typed method for each operation.
`StreamRuntime::call` is the byte form of the same methods. It takes `CreateStream`,
`AppendRecords`, `CloseStream`, `ReadRecords` or `DescribeStream`, and any other name fails with
`Reason::Unsupported`. A Store crate builds the runtime with `StreamRuntime::new`, the name that
lang registered the Store under, and its Store. A `store::LazyStore` connects its inner Store on
the first call that needs it. So a bridge can build the runtime with no async runtime.
`MemoryStreamRuntime::new` builds it on the memory Store, from a `MemoryStreamOptions`.

## Stores and the shared suites

The memory Store is for tests. It keeps the key layout of the Redis Store and applies the same rules
in the same order. The `test-support` feature exports the shared suites in `testing`. A Store crate
implements `testing::TestStore` for its Store and runs each suite with `testing::run`, so that
every Store keeps one set of rules. This crate runs them on the memory Store in its unit tests.
