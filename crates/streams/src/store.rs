//! The trait that a Stream Store implements.

use crate::{
    StreamResult,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse,
    },
    record::BatchLimits,
};
use std::time::Duration;

/// The cap on the `wait` of a read request. A longer wait is cut to it. The runtime has no timer
/// of its own, so a Store that never answers holds the read.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The cap on the `max_records` of a read request, and the page size when the request sets none.
pub const MAX_RECORDS: usize = 1000;

/// A Stream Store. It runs each operation of the Stream service with the service's messages, and
/// applies the service's rules itself.
///
/// - A retried batch with the same Producer, epoch and sequence is written once.
/// - A Store can refuse an append or a close of a lower epoch of its Producer, with
///   [`Reason::Fenced`](crate::Reason::Fenced). It doesn't have to. Each Record has its Producer
///   and its epoch, so a reader can tell the attempts apart.
/// - A read with no Records to return waits up to its `wait`, and a close wakes it.
/// - Offsets are opaque bytes, at most [`MAX_OFFSET_BYTES`](crate::ids::MAX_OFFSET_BYTES) long,
///   and the offsets of one Stream sort in the order of its Records.
/// - A failed call returns a [`StreamError`](crate::StreamError). Use
///   [`Reason::StorageUnavailable`](crate::Reason::StorageUnavailable) when the Store failed or
///   didn't answer, so the caller knows that a retry can succeed.
///
/// A lang bridge implements this trait for a Store in its own language. The Stores that Temporal
/// provides, and shared code for Stores in Rust, are in
/// [temporal-community/sdk-streams](https://github.com/temporal-community/sdk-streams).
#[async_trait::async_trait]
pub trait StreamStore: Send + Sync + 'static {
    /// Creates a Stream, or gets the open one when the conflict policy allows it.
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse>;

    /// Appends one batch as one Producer.
    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse>;

    /// Reads one page after `from_offset`, and waits up to `wait` for a Record when there is none.
    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse>;

    /// Closes a Stream.
    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse>;

    /// Returns the status and the newest offset of a Stream.
    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse>;

    /// The limits of one batch on this Store. The runtime reads them once, when it is built, and
    /// refuses a batch over them before the Store sees it. The default is the largest batch that
    /// any Store may take.
    fn limits(&self) -> BatchLimits {
        BatchLimits::default()
    }
}
