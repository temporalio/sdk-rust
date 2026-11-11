# temporalio-streams

Experimental. The Stream runtime that the Temporal SDKs link into their bridges. A bridge builds
the runtime per client, as a plain object, and calls its typed methods, or its byte form with a
method name and proto bytes. A Workflow makes each Stream call from a Local Activity. Core has no
Stream code.

## Protos

The crate defines no protos. `temporalio-protos` generates them, and `temporalio_streams::proto`
re-exports them, so a value needs no conversion between the crates.

- `temporal/api/stream/v1/message.proto`, `temporal/api/streamservice/v1/request_response.proto`
  and `temporal/api/enums/v1/stream.proto` are the Stream protos of the API. They are in
  `crates/protos/protos/api_upstream`.
- `crates/protos/protos/local/temporal/sdk/core/streams/streams.proto`, in the package
  `coresdk.streams`, has the crate's own messages, such as those of a writer.
