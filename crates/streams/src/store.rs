use crate::{
    StreamResult,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse,
    },
};

/// A Stream Store, which implements the five operations of the Stream service.
///
/// Callers rely on a Store to follow these rules:
///
/// - Appends are idempotent for the last batch of an epoch. If that batch comes again with the same
///   `sequence` and Records, the Store doesn't write it a second time, and it returns the offsets
///   of the first write. A batch with a `sequence` of zero is never deduplicated.
/// - Closing a Stream that is already closed succeeds.
/// - A read returns the Records after `from_offset`. Each Record has a body and a Producer with an
///   id and a kind, and is at most [`MAX_RECORD_BYTES`](crate::record::MAX_RECORD_BYTES) in its
///   stored form. Offsets sort bytewise in the order of the Records.
/// - A page's `last_offset` is where the next read continues. It's never below `from_offset` or the
///   offset of a Record in the page. It can be past entries that aren't Records, but it must not
///   skip a Record. A page is `done` when the Stream is closed and the page has its last Record.
/// - A read with no Records to return waits up to `wait` for one, and a close ends the wait.
/// - An append returns the first and last offsets of its Records, a create returns the ref of its
///   Stream, and a describe returns a status.
/// - Every offset in a response is at most [`MAX_OFFSET_BYTES`](crate::ids::MAX_OFFSET_BYTES) long,
///   and the offset of a Record is never empty.
/// - When its storage fails or doesn't respond in time, the Store returns
///   [`Reason::StorageUnavailable`](crate::Reason::StorageUnavailable), which tells the caller that
///   a retry may succeed.
///
/// Fencing is optional. A Store may refuse a write from an older epoch of a Producer with
/// [`Reason::Fenced`](crate::Reason::Fenced). Every Record has its Producer and epoch either way,
/// so a reader can tell the attempts apart without it.
///
/// [`StreamRuntime`](crate::StreamRuntime) wraps a Store. It validates each response against the
/// rules above, and fails one that breaks them with
/// [`Reason::StorageCorrupt`](crate::Reason::StorageCorrupt). It also validates each request first,
/// so a Store can assume that:
///
/// - The Stream id is 1 to [`MAX_STREAM_ID_BYTES`](crate::ids::MAX_STREAM_ID_BYTES) bytes, and the
///   Producer id is 1 to [`MAX_PRODUCER_ID_BYTES`](crate::ids::MAX_PRODUCER_ID_BYTES) bytes.
///   Nothing bounds `request_id` or a run id.
/// - `from_offset` is at most [`MAX_OFFSET_BYTES`](crate::ids::MAX_OFFSET_BYTES) bytes.
/// - The Producer has the fields that its kind requires, and `sequence` is between 0 and
///   [`MAX_SEQUENCE`](crate::MAX_SEQUENCE).
/// - Every Record of a batch has a body, and the batch fits the
///   [`BatchLimits`](crate::record::BatchLimits) of the runtime.
/// - A read's `max_records` is between 1 and [`MAX_RECORDS`](crate::MAX_RECORDS), its `max_bytes`
///   is 0 or more, and its `wait` is unset or at most [`MAX_WAIT`](crate::MAX_WAIT).
/// - A create's conflict policy is one that the proto defines.
#[async_trait::async_trait]
pub trait StreamStore: Send + Sync + 'static {
    /// Creates a Stream, or returns the existing open one if the conflict policy allows it.
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse>;

    /// Appends one batch from one Producer.
    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse>;

    /// Reads one page of Records after `from_offset`.
    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse>;

    /// Closes a Stream.
    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse>;

    /// Returns the status and the newest offset of a Stream.
    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse>;
}
