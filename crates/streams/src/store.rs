use crate::{
    AppendError, CreateError, DescribeError, ReadError,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CreateStreamRequest, CreateStreamResponse,
        DescribeStreamRequest, DescribeStreamResponse, ReadRecordsRequest, ReadRecordsResponse,
    },
};
use futures_util::future::BoxFuture;

/// A Store for Temporal Streams. The SDK resolves a Stream ref to a Store and passes it the
/// Stream id. A Nexus operation carries the create or append request id in its envelope. These
/// methods take it as a separate argument. See the [Store request and response messages][wire].
///
/// [wire]: https://github.com/temporalio/api/tree/21815b9/temporal/api/streamservice/v1
///
/// # Record order
///
/// Write the entire batch atomically. Keep its Records consecutive and in request order.
/// Each Record has a nonempty offset. Offsets sort bytewise and fit in
/// [`MAX_OFFSET_BYTES`](crate::ids::MAX_OFFSET_BYTES). After a call returns an offset, no append
/// may insert a Record at or before it, including an append in flight when the call returned.
/// Keep each Record unchanged until its Stream expires.
///
/// [`producer_key`](crate::attempt::producer_key) identifies a Producer.
/// [`attempt_key`](crate::attempt::attempt_key) identifies and orders attempts of that Producer.
/// A Store may fence older attempts. Always keep the Producer and attempt with the Record.
///
/// # Concurrent calls and failures
///
/// A waiting read must let other calls append. A caller can drop a future at any await. The call
/// may still take effect. Handle a resend even when the first try is still running. Reuse a
/// connection only after consuming its pending response.
///
/// Return [`AnyReason::StoreUnavailable`](crate::AnyReason::StoreUnavailable) for an outage that
/// a later try may resolve. Core retries Workflow appends and fails the Workflow Task if the
/// Store is still unavailable at its heartbeat. A read outage at the read limit or final check
/// also fails the task.
///
/// # Runtime checks
///
/// [`StreamRuntime`](crate::StreamRuntime) checks each request before dispatch. A Store can rely
/// on these properties when called through the runtime.
///
/// - A Stream id has 1 to [`MAX_STREAM_ID_BYTES`](crate::ids::MAX_STREAM_ID_BYTES) bytes.
///   Request ids have no length limit.
/// - A read has a valid offset length, nonnegative page limits and a nonnegative wait.
/// - A create has a known conflict policy.
/// - An append has a complete Producer, at least one Record and a body for each Record.
///   [`StoreLimits::validate_append`](crate::record::StoreLimits::validate_append) checks the
///   Producer and the caller's size limits. A Store with lower limits must check its own.
///
/// The runtime checks response offsets and the presence of a body and Producer on each Record
/// it returns. A malformed response gives [`StoreCorrupt`](crate::AnyReason::StoreCorrupt).
/// A Record over the caller's limit gives [`Unsupported`](crate::AnyReason::Unsupported).
/// The Store remains responsible for guarantees that span calls, including resend detection
/// and returning every Record in order.
pub trait StreamStore: Send + Sync + 'static {
    /// Create a Stream and return `created = true`. For an existing id, check these cases in order.
    ///
    /// 1. If the Stream has expired, return [`Expired`](crate::CreateReason::Expired).
    /// 2. If the nonempty `request_id` matches the original create, return `created = true` again.
    /// 3. With `UseExisting`, return `created = false`. With `Fail` or no policy, return
    ///    [`CreateReason::AlreadyExists`](crate::CreateReason::AlreadyExists).
    ///
    /// A Store may eventually forget an expired Stream. Calls then return `NotFound` and a new
    /// create can succeed.
    fn create_stream(
        &self,
        request: CreateStreamRequest,
        request_id: String,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>>;

    /// Append one batch from one Producer. Return the offsets of its first and last Records.
    ///
    /// 1. Return [`AppendReason::NotFound`](crate::AppendReason::NotFound) or
    ///    [`AppendReason::Expired`](crate::AppendReason::Expired) for a missing or expired Stream.
    /// 2. If the Producer, attempt and nonempty `request_id` match a remembered batch, return its
    ///    original offsets. Do not compare the Records or extend Retention.
    /// 3. A Store that fences must reject an older attempt after a newer one wrote. Return
    ///    [`AppendReason::Fenced`](crate::AppendReason::Fenced).
    /// 4. Otherwise write the batch and restart Retention.
    ///
    /// Keep a configurable window of recent batches per Stream and Producer. It should hold more
    /// than one batch. A retry outside that window can write again. An empty request id always
    /// bypasses resend detection.
    fn append_records(
        &self,
        request: AppendRecordsRequest,
        request_id: String,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>>;

    /// Read one page after `from_offset`. Return the Records in Stream order with the body and
    /// Producer of each append.
    ///
    /// Respect `max_records` and `max_bytes` when set. Zero leaves the limit to the Store. Keep
    /// the first Record even if it alone exceeds `max_bytes`. The runtime counts the encoded
    /// Record with its field tag and length. It trims excess Records and may request an extra
    /// Record to learn whether another page is ready.
    ///
    /// The next read starts at `last_offset`. It must not skip a Record or go backwards. If
    /// omitted, the runtime uses the final Record's offset, or `from_offset` for an empty page.
    /// Accept any offset returned by a read, append or describe. Return
    /// [`ReadReason::OffsetTooHigh`](crate::ReadReason::OffsetTooHigh) only for an offset beyond
    /// the end of the Stream.
    ///
    /// If no Record is ready, wait for an entry or until `wait` expires. With zero or no wait,
    /// return immediately.
    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>>;

    /// Return an empty `last_offset` for an empty Stream. Otherwise return the newest Record's
    /// offset or a position after it that skips no later Record.
    fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>>;
}
