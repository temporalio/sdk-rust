# Nexus worker failure handling

This comparison covers the experimental Rust Nexus worker's start/cancel dispatch, failure
conversion, and completion boundary. It does not claim complete Nexus API parity.

## Reference implementations

The comparison used these pinned source revisions:

- [Python worker](https://github.com/temporalio/sdk-python/blob/968c3c9dcf508ceca3f4e34b9ea910425351f6c5/temporalio/worker/_nexus.py)
  and [error-chain tests](https://github.com/temporalio/sdk-python/blob/968c3c9dcf508ceca3f4e34b9ea910425351f6c5/tests/nexus/test_workflow_caller_error_chains.py).
- [Java task handler](https://github.com/temporalio/sdk-java/blob/53c9217b614a010858cf25df9907f1120fba3a9d/temporal-sdk/src/main/java/io/temporal/internal/nexus/NexusTaskHandlerImpl.java),
  [payload serializer](https://github.com/temporalio/sdk-java/blob/53c9217b614a010858cf25df9907f1120fba3a9d/temporal-sdk/src/main/java/io/temporal/internal/nexus/PayloadSerializer.java),
  and [worker completion handling](https://github.com/temporalio/sdk-java/blob/53c9217b614a010858cf25df9907f1120fba3a9d/temporal-sdk/src/main/java/io/temporal/internal/worker/NexusWorker.java).

## Shared rules

| Failure | Rust behavior |
| --- | --- |
| Explicit failed operation | Complete the start request with an `OperationError` application failure and the original immediate cause. This is a terminal operation outcome, not a retryable handler failure. |
| Explicit canceled operation | Complete the start request with a canceled failure, preserving the message and immediate cause without adding cancellation details. |
| Explicit handler error | Fail the task with its original Nexus type, retry override, message, stack trace, and cause. Preserve nested handler errors as handler errors. |
| Retryable application error or unexpected error | `INTERNAL` handler failure, using the default retry behavior and retaining the original error as its cause. |
| Non-retryable application error | `INTERNAL` handler failure with an explicit non-retryable override; preserve the application error's metadata and details as the immediate cause. |
| Unknown service or operation | `NOT_FOUND`, unspecified retry behavior (non-retryable by the protocol default), with a diagnostic identifying the missing name. |
| Input payload cannot deserialize | Non-retryable `BAD_REQUEST`; do not invoke the handler. |
| Input codec outage | Retryable `INTERNAL`, not permanent `BAD_REQUEST`. |
| Explicit non-retryable `PayloadValidationError` during input conversion | Non-retryable `BAD_REQUEST`, including when raised by the input codec. |
| Output serialization failure | Handler failure, preserving explicit application/handler classification; unexpected failures use retryable `INTERNAL`. |
| External-storage retrieval failure | Retryable `INTERNAL`; do not pass unresolved references to the handler. |
| Result codec rejects a successful result | Translate the error to a handler failure, preserving an explicit retry policy, and attempt to encode that error completion once. |
| Failure encoding error, outbound storage failure, or failure-converter panic | Return a minimal retryable `INTERNAL` completion without calling the broken converter, codec, or storage again. |
| Task deadline/shutdown cancellation | Acknowledge Core's task cancellation instead of turning it into an operation-canceled result. |
| Missing workflow during cancellation | Report `NOT_FOUND`, rather than acknowledging a cancellation that was not performed. |

Failures enter the configured failure converter with their Nexus classification already present.
Messages and stack traces must not be rewritten after conversion: converters may already have
moved them into encoded attributes. Nested causes use the same payload converter and serialization
context, and the completion boundary applies the payload codec once to success and failure payloads.

Core already retries/reports transport errors while sending Nexus completions and converts to the
legacy Nexus failure format when the server does not advertise Temporal failure responses. A
malformed completion is an SDK error, not an ordinary handler failure.

## gRPC errors from handlers and Temporal client calls

These mappings agree between the Python and Java implementations:

| gRPC status | Nexus handler type | Explicit retry override |
| --- | --- | --- |
| `INVALID_ARGUMENT` | `BAD_REQUEST` | Unspecified |
| `ALREADY_EXISTS`, `FAILED_PRECONDITION`, `OUT_OF_RANGE` | `INTERNAL` | Non-retryable |
| `ABORTED`, `UNAVAILABLE` | `UNAVAILABLE` | Unspecified |
| `NOT_FOUND` | `NOT_FOUND` | Unspecified |
| `RESOURCE_EXHAUSTED` | `RESOURCE_EXHAUSTED` | Unspecified |
| `UNIMPLEMENTED` | `NOT_IMPLEMENTED` | Unspecified |
| `DEADLINE_EXCEEDED` | `UPSTREAM_TIMEOUT` | Unspecified |
| `CANCELLED`, `DATA_LOSS`, `INTERNAL`, `UNKNOWN`, `UNAUTHENTICATED`, `PERMISSION_DENIED` | `INTERNAL` | Unspecified |

Authentication failures in a handler's call to Temporal are not authentication failures of the Nexus
caller. They therefore use retryable `INTERNAL`, not the non-retryable Nexus authentication types.

## Reference differences and limits

- Python sets the synthetic failed-operation wrapper's application `non_retryable` flag to true;
  Java leaves it false. Rust retains Java's convention. The operation outcome is terminal in all
  three SDKs regardless of this application flag.
- Python maps its typed workflow-already-started exception to non-retryable `INTERNAL`; Java maps
  workflow exceptions other than not-found to `BAD_REQUEST`. Rust follows Python for the typed
  already-started error. All three stop retrying it.
- Python and Java differ in some explicit application-error treatment inside custom input
  converters. Rust preserves explicit handler/RPC errors or application retry overrides, as Java does;
  all three recognize the non-retryable `PayloadValidationError` marker as invalid caller input.
- Rust reports malformed workflow-operation tokens as permanent `BAD_REQUEST`, matching Java's
  handler type. Python's Temporal operation handler uses non-retryable `INTERNAL`; its older
  workflow-run handler uses `NOT_FOUND`. None retries malformed tokens.
- Generic exception type names, stack traces, and diagnostics are language-specific. Portable
  application type names should be supplied explicitly through `ApplicationFailure`.
- This work does not expand Rust's existing external-storage limitation for codec-encoded
  references, or the currently empty Nexus serialization-context metadata.

Regression coverage lives in the Nexus worker unit tests, the shared failure-converter tests, and
the Rust ports of the eleven Nexus features in the `features` repository.
