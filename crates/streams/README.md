# temporalio-streams

Experimental. The Stream runtime that the Temporal SDKs link into their bridges. A bridge builds
the runtime per client, as a plain object, and calls it with a method name and proto bytes. A
Workflow makes each Stream call from a Local Activity. Core has no Stream code.

## Protos

- `protos/temporal/api/stream/v1/message.proto`,
  `protos/temporal/api/streamservice/v1/request_response.proto` and
  `protos/temporal/api/enums/v1/stream.proto` are the Stream protos of the API proposal, byte for
  byte. The API protos of this repo don't have them yet, so the crate generates them. Lang
  generates the same files. They import API protos that the build reads from the descriptor set
  of `temporalio-protos`, and that map to the types of `temporalio-common`. So the packaged crate
  needs no file outside itself.
- `protos/temporal/sdk/streams/v1/runtime.proto` has the crate's own calls, such as the writer
  calls.
