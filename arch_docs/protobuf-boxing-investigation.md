# Descriptor-driven boxing experiment

This is a research implementation for #1653, not a demonstrated performance improvement.
It selects fields from descriptors instead of maintaining a field-name list.

The model uses approximate default 64-bit Prost layouts. It processes children before
parents, selects large message-valued oneof arms jointly, then boxes the largest eligible
ordinary fields while the enclosing message exceeds a budget. Name ordering breaks ties.
Recursive fields already boxed by Prost, repeated fields and maps are excluded from new
selections. Synthetic optional oneofs use ordinary field paths.

The experimental thresholds are 128 estimated bytes per field, 64 bytes of minimum
estimated savings, and a 512-byte enclosing-message budget. These are not tuned defaults.
The model does not support arbitrary custom type mappings or predict Rust ABI layout.
Schema changes can change existing selections transitively and therefore affect the
public generated Rust API.

On aarch64-apple-darwin with Rust 1.95, the full protos pipeline reduced a mirror of the
unchanged `UpdateMsg` enum from 624 to 328 inline bytes. `Rejection` changed from 624 to
328, `Response` from 352 to 72, and `Outcome` from 304 to 24. The full descriptor set
selected 162 paths. Client and Core changes adapt to the generated field types; boxed
payloads still participate in validation, collection and asynchronous visiting.

A populated rejection decode sample added one allocation and 304 requested bytes.
The standalone probe also found that delta for construction and Any encoding of
rejections and failed responses, with no additional allocations for acceptance and
successful responses. These are sample-specific results, not RSS or production traffic.
There is no demonstrated runtime improvement.

`UpdateMsg` is serialized immediately. The outbox stores encoded protocol messages,
so its smaller inline size does not establish resident queue-memory savings. Production
message frequencies, replay, payload distributions and other affected types remain
unmeasured.

## Reproduction

Selector and existing protos unit tests:

```sh
cargo test -p temporalio-protos --lib --features vendored-protox,serde_serialize,grpc-clients
```

The standalone probe generates baseline and selected types from the same real update
proto import closure and checks wire equivalence for four populated message branches:

```sh
cargo run --manifest-path tools/boxing-probe/Cargo.toml
```

It reports allocation/reallocation calls and requested bytes for construction plus Any
encoding and for decoding, separately. The frequency table is synthetic sensitivity
analysis, not a production estimate. The probe uses standard Prost well-known types;
it does not duplicate the full repository's WKT, tonic and pbjson generation settings.

Validation also included a Core-library check with default features disabled and focused
full-pipeline JSON and boxed-payload traversal probes. The Workflow library also passes a focused check with experimental APIs enabled.
The full all-feature `cargo lint` attempt does not pass: additional Core configurations
still have generated Box-type mismatches (45 diagnostics in the latest attempt).
Default-feature, server integration and cross-platform validation remain incomplete.
This branch is for design review, not ready to merge.

The generator writes its selected paths and reasons to `boxing-selections.txt` in its
build output directory. `TEMPORAL_BOXING_AUDIT` can select another report path.
