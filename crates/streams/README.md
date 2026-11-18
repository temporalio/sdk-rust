# temporalio-streams

Experimental. What every Store of a Temporal Stream shares: the error of a failed call, the order
of a Producer's epochs, and the stored form of a Record.

## Errors

A failed call gives a `StreamError` with a `Reason`. `StreamError::code` gives its gRPC code and
`StreamError::details` its encoded `StreamErrorDetails`, so every SDK raises the same error for
the same failure.

## Protos

The crate defines no protos. `temporalio-protos` generates them, and `temporalio_streams::proto`
re-exports them, so a value needs no conversion between the crates.
