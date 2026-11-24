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
/// The request id of a create or an append is in the envelope of its Nexus call, so those methods
/// take it next to the request. The [crate docs](crate) define the terms that these docs use.
///
/// # The contract of a Store
///
/// Callers rely on a Store to keep this contract. Core reads a Stream for Workflow code and reads
/// it again in a replay, and both depend on the contract of the log.
///
/// The log:
///
/// - A Store writes a whole batch (transactionally). The Records of a batch are next to each other
///   in the Stream, in the order of the request.
/// - Offsets sort bytewise in the order of the Records. Every offset in a response is at most
///   [`MAX_OFFSET_BYTES`](crate::ids::MAX_OFFSET_BYTES) long, and the offset of a Record is never
///   empty.
/// - Once a response returned an offset, no Record ever appears at or below it later, even from an
///   append that ran at the same time as that call.
/// - A Store doesn't change or remove a Record before its Stream expires.
///
/// Producers and attempts:
///
/// - Two Producers are the same when their [`producer_key`](crate::attempt::producer_key) is the
///   same.
/// - Two attempts of a Producer are the same when [`attempt_key`](crate::attempt::attempt_key)
///   gives the same bytes for both, and the byte order of their keys determines the order of the
///   attempts.
/// - Fencing is optional. A Store that fences keeps the newest attempt of each Producer that
///   appended to a Stream, and refuses an append or a close of that Producer from an earlier
///   attempt. Only an append changes the newest attempt, and a close doesn't. Every Record has its
///   Producer and attempt either way, so a reader can tell the attempts apart without it.
///
/// A create:
///
/// - A create for an id that has no Stream yet makes the Stream and returns `created` true.
/// - A create returns the ref as the request had it.
/// - A create with the request id of the create that made the Stream is a retry of it. A create
///   with an empty request id is not a retry.
/// - A Store checks a create for an id that has a Stream in this order, and the first check that
///   applies determines the answer.
///   1. The Stream expired, so the create fails with
///      [`CreateReason::Expired`](crate::CreateReason::Expired).
///   2. The Stream is closed, so the create fails with
///      [`CreateReason::Closed`](crate::CreateReason::Closed).
///   3. The create is a retry, so it returns `created` true.
///   4. The policy is `UseExisting`, so the create returns `created` false. With `Fail`, or with
///      no policy, it fails with
///      [`CreateReason::AlreadyExists`](crate::CreateReason::AlreadyExists).
/// - A Store may forget an expired Stream after some time beyond its configured retention. From
///   then on it answers a call for that id with `NotFound`, and a create for the id can succeed.
///
/// An append:
///
/// - A try of an append can write its batch and then lose its answer. The caller then sends the
///   append again, and without a check the Stream would have the batch twice. So each append has
///   a request id, and every try of it sends the same one.
/// - A Store remembers the request id of the recent batches of each Producer. An append whose
///   Producer, attempt and request id match a remembered batch is a resend, and the Store doesn't
///   write it again.
/// - The Store remembers more than the last batch, because a resend can come late. A Producer can
///   send more than one append at the same time, so the retry of one append can come after a
///   later append was written. A dropped connection can also deliver an old try after newer
///   batches.
/// - How many batches a Store remembers is a setting of the Store, at least 1. A resend that comes
///   after that many newer batches of its Producer is written again.
/// - An append with an empty request id is not a resend. A Store that doesn't fence still
///   compares the Producer and the attempt, not only the request id.
/// - A Store checks an append in this order, and the first check that applies determines the
///   answer.
///   1. The Stream doesn't exist or it expired, so the append fails with
///      [`AppendReason::NotFound`](crate::AppendReason::NotFound) or
///      [`AppendReason::Expired`](crate::AppendReason::Expired).
///   2. The append is a resend, so the Store doesn't write anything and returns the offsets of the
///      first write of that batch. It doesn't compare the Records, and the Stream can be closed by
///      now.
///   3. The Store fences, and a later attempt of the Producer already wrote. The append fails with
///      [`AppendReason::Fenced`](crate::AppendReason::Fenced).
///   4. The Stream is closed, so the append fails with
///      [`AppendReason::Closed`](crate::AppendReason::Closed).
/// - An append returns the offsets of its first and its last Record, and the first cannot be above
///   the last.
///
/// A close:
///
/// - A Store checks a close in this order, and the first check that applies determines the answer.
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
///   its body and the Producer of its append, unchanged.
/// - A Store should return at most `max_records` Records, and Records of at most `max_bytes` in
///   total (zero means no limit). A page always has its first Record, even when that Record alone
///   is over `max_bytes`. The runtime measures a Record as the page encodes it, with the tag and
///   the length of its field. It trims a page that is over a limit.
/// - A page's `last_offset` is where the next read continues. It's never below `from_offset` or the
///   offset of a Record in the page, and it never skips a Record.
/// - A page is `done` when the Stream is closed and no Record comes after the page. That holds for
///   an empty page too.
/// - A read with nothing after `from_offset` answers once 1) an entry becomes available, 2) the
///   Stream is closed, or 3) its `wait` is up. With no `wait` (or a zero `wait`), it answers
///   immediately.
/// - A Store takes every offset that a response returned as the `from_offset` of a read. These
///   are 1) the offset of a Record, 2) an offset of an append, and 3) a `last_offset`. It fails a
///   read with [`ReadReason::OffsetTooHigh`](crate::ReadReason::OffsetTooHigh) only when
///   `from_offset` is past the end of the Stream, which a caller sees after the Store lost its
///   newest Records, for example in a failover.
///
/// A describe:
///
/// - A describe returns the status of the Stream.
/// - Its `last_offset` is never below the offset of the newest Record.
///
/// When the Store fails or doesn't respond in time, it returns
/// [`AnyReason::StoreUnavailable`](crate::AnyReason::StoreUnavailable), which tells the caller
/// that a retry may succeed. Core tries a call of Workflow code again after that reason, with no
/// time limit, and Workflow code never gets that error. So a Store returns it only for a failure
/// that can be resolved at a later try.
///
/// # How callers call a Store
///
/// Calls come at the same time from many tasks. A read with a `wait` can stay open until a Record
/// comes, and that Record comes from an append that runs at the same time. So a waiting read must
/// not block the appends of the Store.
///
/// A caller can drop the future of any call at any `await`. Core does so at the deadline of a try,
/// and when Workflow code stops a read. The call may still take effect, and the Store must stay
/// usable after it. For example, a Store must not reuse a connection if it didn't read the answer
/// of the last call on it. A resend can arrive while its first try still runs, and the Store still
/// writes the batch once.
///
/// # What the runtime checks
///
/// [`StreamRuntime`](crate::StreamRuntime) wraps a Store. It checks each response against the parts
/// of the contract that one response shows, and fails a response that breaks them with
/// [`AnyReason::StoreCorrupt`](crate::AnyReason::StoreCorrupt). Those are the parts about 1) the
/// Stream id of the ref of a create, 2) offsets, 3) the body and the Producer of a Record, and 4)
/// the status of a describe.
/// It can't check a part that needs more than one response, such as the resend rule, or that a
/// page skips no Record. A read that gives a Record larger than the Record size of the runtime's
/// [`StoreLimits`](crate::record::StoreLimits), in its stored form, fails with
/// [`AnyReason::Unsupported`](crate::AnyReason::Unsupported), since a runtime with a larger Record
/// size may take it.
/// The runtime also validates each request first, so a Store can assume that:
///
/// - The request has a ref, and its `id` is 1 to
///   [`MAX_STREAM_ID_BYTES`](crate::ids::MAX_STREAM_ID_BYTES) bytes. The runtime ignores the
///   `store` of the ref, which only an SDK reads. There's no bound for the request id.
/// - `from_offset` is at most [`MAX_OFFSET_BYTES`](crate::ids::MAX_OFFSET_BYTES) bytes.
/// - The Producer of an append or a close has a kind, every id of that kind, and the fields of its
///   attempt. Each time of the attempt has nanos from 0 to 999,999,999. The Record size bounds the
///   ids of an append's Producer, since the size of a Record counts its Producer.
/// - A batch has at least one Record, every Record has a body, and the batch fits the
///   [`StoreLimits`](crate::record::StoreLimits) of the runtime. Those are the limits of the
///   caller, from the Stream config of its SDK, so a Store with a lower limit checks its own
///   limits.
/// - A read's `max_records`, `max_bytes` and `wait` are non-negative.
/// - A create's conflict policy is `Fail`, `UseExisting` or none.
// The methods return boxed futures, so the trait stays dyn-compatible for the runtime's
// `Arc<dyn StreamStore>` and needs no macro crate in the public API.
pub trait StreamStore: Send + Sync + 'static {
    /// Creates a Stream, or finds the open Stream that already has the id. Every try of one create
    /// has the same `request_id`.
    fn create_stream(
        &self,
        request: CreateStreamRequest,
        request_id: String,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>>;

    /// Appends one batch from one Producer. Every try of one append has the same `request_id`.
    fn append_records(
        &self,
        request: AppendRecordsRequest,
        request_id: String,
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
