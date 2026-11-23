use crate::{
    AppendError, CloseError, CreateError, DescribeError, ReadError,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse,
    },
};
use futures_util::future::BoxFuture;

/// A Stream Store, which serves the operations of the Nexus service
/// `temporal.api.streamservice.v1.StreamService`. The API repo defines that service in
/// `nexus/stream-service.wit`, and [`proto`](crate::proto) has its request and response messages.
/// The [crate docs](crate) define the terms that these docs use.
///
/// # The rules of a Store
///
/// Callers rely on a Store to follow these rules. Core reads a Stream for Workflow code and reads
/// it again in a replay, and both depend on the rules of the log.
///
/// The log:
///
/// - A Store writes a batch whole or not at all. The Records of a batch are next to each other in
///   the Stream, in the order of the request.
/// - Offsets sort bytewise in the order of the Records. Every offset in a response is at most
///   [`MAX_OFFSET_BYTES`](crate::ids::MAX_OFFSET_BYTES) long, and the offset of a Record is never
///   empty.
/// - Once a response returned an offset, no Record ever appears at or below it later, even from an
///   append that ran at the same time as that call.
/// - A Store doesn't change or remove a Record before its Stream expires.
///
/// Producers and attempts:
///
/// - Two Producers are the same when [`producer_key`](crate::attempt::producer_key) gives the same
///   bytes for both.
/// - Two attempts of a Producer are the same when [`attempt_key`](crate::attempt::attempt_key)
///   gives the same bytes for both, and the byte order of two keys is the order of their attempts.
/// - Fencing is optional. A Store that fences keeps the newest attempt of each Producer that
///   appended to a Stream, and refuses an append or a close of that Producer from an earlier
///   attempt. Only an append changes the newest attempt, and a close doesn't. Every Record has its
///   Producer and attempt either way, so a reader can tell the attempts apart without it.
///
/// A create:
///
/// - A create for an id with no Stream makes the Stream and returns `created` true.
/// - A create with the `request_id` of the create that made the Stream is a retry of it. A create
///   with an empty `request_id` is never a retry.
/// - A Store checks a create for an id that has a Stream in this order, and the first check that
///   applies decides the answer.
///   1. The Stream expired, so the create fails with
///      [`CreateReason::Expired`](crate::CreateReason::Expired).
///   2. The Stream is closed, so the create fails with
///      [`CreateReason::Closed`](crate::CreateReason::Closed).
///   3. The create is a retry, so it returns `created` true.
///   4. The policy is `UseExisting`, so the create returns `created` false. With `Fail`, or with
///      no policy, it fails with
///      [`CreateReason::AlreadyExists`](crate::CreateReason::AlreadyExists).
/// - A Store may forget an expired Stream after a time of its choice. From then on it answers a
///   call for that id with `NotFound`, and a create for the id can succeed.
///
/// An append:
///
/// - A Store remembers the request ids of the recent batches of each Producer of a Stream. How
///   many is a setting of the Store, and it's at least the last batch. An append with the same
///   Producer and attempt as a remembered batch, and with its `request_id`, is a resend. A try
///   that comes after more batches than the Store remembers isn't one, and neither is an append
///   with an empty `request_id`. A Store that doesn't fence compares Producers and attempts for
///   this rule too.
/// - A Store checks an append in this order, and the first check that applies decides the answer.
///   1. The Stream doesn't exist or it expired, so the append fails with
///      [`AppendReason::NotFound`](crate::AppendReason::NotFound) or
///      [`AppendReason::Expired`](crate::AppendReason::Expired).
///   2. The append is a resend, so the Store writes nothing and returns the offsets of the first
///      write of that batch. It doesn't compare the Records, and the Stream can be closed by now.
///   3. The Store fences, and a later attempt of the Producer already wrote. The append fails with
///      [`AppendReason::Fenced`](crate::AppendReason::Fenced).
///   4. The Stream is closed, so the append fails with
///      [`AppendReason::Closed`](crate::AppendReason::Closed).
/// - An append returns the offsets of its first and its last Record, and the first is never above
///   the last.
///
/// A close:
///
/// - A Store checks a close in this order, and the first check that applies decides the answer.
///   1. The Stream doesn't exist or it expired, so the close fails with
///      [`CloseReason::NotFound`](crate::CloseReason::NotFound) or
///      [`CloseReason::Expired`](crate::CloseReason::Expired).
///   2. The Stream is already closed, so the close succeeds.
///   3. The Store fences, and a later attempt of the Producer already wrote. The close fails with
///      [`CloseReason::Fenced`](crate::CloseReason::Fenced).
///
/// A read:
///
/// - A read returns the Records after `from_offset`, in the order of the Stream. Each Record has
///   its body and the Producer of its append, field for field.
/// - A Store should return at most `max_records` Records, and Records of at most `max_bytes` in
///   total, where zero is no limit. A page always has its first Record, even when that Record
///   alone is over `max_bytes`. The runtime counts a Record as the page encodes it, which is its
///   encoded size with the tag and the length of its field. It trims a page that is over a limit,
///   so a Store that ignores the limits reads Records that no caller gets.
/// - A page's `last_offset` is where the next read continues. It's never below `from_offset` or the
///   offset of a Record in the page. It can be past entries of the Store's log that aren't
///   Records, such as the entry of a close, but it must not skip a Record.
/// - A page is `done` when the Stream is closed and no Record comes after the page. That holds for
///   an empty page too.
/// - A read with nothing after `from_offset` answers once an entry comes after it, once the Stream
///   is closed, or once its `wait` is up. With no `wait`, or one of zero, it answers immediately.
/// - A Store takes every offset that a response returned as the `from_offset` of a read. That's
///   the offset of a Record, an offset of an append, and a `last_offset`. It fails a read with
///   [`ReadReason::OffsetTooHigh`](crate::ReadReason::OffsetTooHigh) only when `from_offset` is
///   past the end of the Stream, which a caller sees after the Store lost its newest Records, for
///   example in a failover.
///
/// A describe:
///
/// - A describe returns the status of the Stream.
/// - Its `last_offset` is never below the offset of the newest Record. As in a page, it can be past
///   entries of the Store's log that aren't Records.
///
/// When the Store fails or doesn't respond in time, it returns
/// [`AnyReason::StoreUnavailable`](crate::AnyReason::StoreUnavailable), which tells the caller
/// that a retry may succeed. Core tries a call of Workflow code again after that reason, with no
/// limit in time, and Workflow code never gets it. So a Store returns it only for a failure that
/// may be gone at a later try.
///
/// # How callers call a Store
///
/// Calls come at the same time from many tasks, and a read can wait while appends run. A caller
/// can drop the future of any call at any `await`. Core does so at the deadline of a try, and when
/// Workflow code stops a read. The call may still take effect, and the Store must stay usable
/// after it. For example, a Store must not reuse a connection if it didn't read the answer of the
/// last call on it. A resend can arrive while its first try still runs, and the Store still writes
/// the batch once.
///
/// # What the runtime checks
///
/// [`StreamRuntime`](crate::StreamRuntime) wraps a Store. It checks each response against the rules
/// that a single response can break, and fails a response that breaks one with
/// [`AnyReason::StoreCorrupt`](crate::AnyReason::StoreCorrupt). Those are the rules about offsets,
/// about the body and the Producer of a Record, and about the status of a describe. It can't check
/// a rule that no single response shows, such as the resend rule, or that a page leaves no Record
/// out. A read that gives a Record larger than the Record size of the runtime's
/// [`StoreLimits`](crate::record::StoreLimits), in its stored form, fails with
/// [`AnyReason::Unsupported`](crate::AnyReason::Unsupported), since a runtime with a larger Record
/// size may take it.
/// The runtime also validates each request first, so a Store can assume that:
///
/// - The Stream id is 1 to [`MAX_STREAM_ID_BYTES`](crate::ids::MAX_STREAM_ID_BYTES) bytes. Nothing
///   bounds `request_id`.
/// - `from_offset` is at most [`MAX_OFFSET_BYTES`](crate::ids::MAX_OFFSET_BYTES) bytes.
/// - The Producer of an append or a close has a kind, every id of that kind, and the fields of its
///   attempt. Nothing bounds an id of a close's Producer. The Record size bounds the ids of an
///   append's Producer, since the size of a Record counts its Producer.
/// - A batch has at least one Record, every Record has a body, and the batch fits the
///   [`StoreLimits`](crate::record::StoreLimits) of the runtime. Those are the limits of the
///   caller, from the Stream config of its SDK, so a Store with a lower limit checks its own.
/// - A read's `max_records`, `max_bytes` and `wait` are not negative. They are as the caller set
///   them, so nothing else bounds them, and a `max_records` of 0 leaves the page size to the Store.
/// - A create's conflict policy is `Fail`, `UseExisting` or none.
// The methods return boxed futures, so the trait stays dyn-compatible for the runtime's
// `Arc<dyn StreamStore>` and needs no macro crate in the public API.
pub trait StreamStore: Send + Sync + 'static {
    /// Creates a Stream, or finds the open Stream that already has the id.
    fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>>;

    /// Appends one batch from one Producer.
    fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>>;

    /// Reads one page of Records after `from_offset`.
    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>>;

    /// Closes a Stream.
    fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> BoxFuture<'_, Result<CloseStreamResponse, CloseError>>;

    /// Returns the status of a Stream, and the offset of its newest Record as `last_offset`.
    fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>>;
}
