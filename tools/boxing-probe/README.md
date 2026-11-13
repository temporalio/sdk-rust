# Protobuf boxing allocation probe

Run from the repository root:

```sh
cargo run --manifest-path tools/boxing-probe/Cargo.toml
```

This generates baseline and descriptor-selected types from the same Temporal update
proto import closure. It checks populated acceptance, rejection, successful response
and failed response wire representations, then reports allocation calls and requested
bytes for construction/encoding and decoding separately. Reallocations count as one
call and their full requested size. Deallocation is included in each operation, but
this does not measure live heap, RSS or execution speed.

The printed frequency combinations are synthetic. They describe completed,
nonreplayed updates only, with one acceptance and one response per accepted update,
and one rejection per rejected update. They do not account for pending updates,
retries, replay or the other generated types changed by the policy.

This probe uses standard Prost well-known types, not the complete SDK generation
configuration. See `arch_docs/protobuf-boxing-investigation.md` for scope and limitations.
