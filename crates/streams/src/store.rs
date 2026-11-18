//! The Store of the Stream runtime.

use crate::{
    StreamResult,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse,
    },
};
use std::time::Duration;

/// The longest wait that the runtime asks of a Store for one read. The runtime sets no timer of its
/// own, so a Store that never answers holds the read.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The most Records in one page of a read.
pub const MAX_RECORDS: usize = 1000;

/// A Stream Store. It answers each operation of the Stream service whole: `create_stream`,
/// `append_records`, `read_records`, `close_stream` and `describe_stream`. It takes and gives the
/// messages of the Stream service. The Store applies the rules of the service itself.
///
/// - An append fences a lower epoch of its Producer, and writes a batch that comes again with the
///   same Producer, epoch and sequence once.
/// - A read waits up to its `wait` for a Record when it has none to give, and a close wakes it.
/// - An offset is opaque bytes, at most [MAX_OFFSET_BYTES](crate::ids::MAX_OFFSET_BYTES), and the
///   offsets of one Stream compare in order.
/// - A failed call gives a [StreamError](crate::StreamError) with its reason.
///   [StorageUnavailable](crate::Reason::StorageUnavailable) is the one reason that says a later
///   try can work.
///
/// A lang bridge implements the trait for a Store that is written in that language. The Stores that
/// Temporal provides, and the shared rules that a Store in Rust builds on, are in
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

    /// Gives the status and the newest offset of a Stream.
    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse>;
}
