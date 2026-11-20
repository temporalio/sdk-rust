use crate::proto::{StreamErrorDetails, StreamErrorReason};
use prost::Message;
use std::fmt;

/// A reason that any Stream call can return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AnyReason {
    /// The request is malformed. The runtime checks each request before the Store gets it.
    Invalid,
    /// The Store failed or didn't respond. A retry may succeed.
    StorageUnavailable,
    /// The Store returned invalid data, and a retry would read the same data again. The runtime
    /// returns it for an answer that breaks the rules of [`StreamStore`](crate::StreamStore). An
    /// append or a create that fails this way may still have taken effect.
    StorageCorrupt,
    /// The Store doesn't support something that the call needs.
    Unsupported,
    /// The Stream was written with a newer format version than this build understands.
    FormatTooNew,
}

/// The reasons that a create returns, besides the ones of [`AnyReason`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CreateReason {
    /// The Stream already exists, and the create used the `Fail` conflict policy.
    AlreadyExists,
    /// The caller isn't allowed to create this Stream.
    NotOwner,
    /// The Stream exists and is closed, whatever the conflict policy.
    Closed,
    /// The Stream exists and expired, whatever the conflict policy.
    Expired,
}

/// The reasons that an append returns, besides the ones of [`AnyReason`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendReason {
    /// The append is for a Stream that nobody created.
    NotFound,
    /// The Stream expired, after its Retention or after the idle TTL of its Producer.
    Expired,
    /// The Stream is closed.
    Closed,
    /// A newer epoch of the batch's Producer has already written to the Stream. Only a Store that
    /// fences returns it.
    Fenced,
    /// The batch's `sequence` is lower than the last one of its epoch.
    SequenceTooLow,
    /// The batch has the `sequence` of the last batch of its epoch, with other Records.
    SeqDivergent,
    /// A Record or the batch is larger than the Store allows. The runtime returns it for a batch
    /// over its [`BatchLimits`](crate::record::BatchLimits).
    TooLarge,
}

/// The reasons that a read returns, besides the ones of [`AnyReason`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadReason {
    /// The read is for a Stream that nobody created.
    NotFound,
    /// The Stream expired, after its Retention or after the idle TTL of its Producer.
    Expired,
    /// The read's `from_offset` is past the newest offset of the Stream, for example after a
    /// failover lost the newest Records. A read from it works again once the Stream has Records up
    /// to it.
    OffsetTooHigh,
}

/// The reasons that a close returns, besides the ones of [`AnyReason`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseReason {
    /// The close is for a Stream that nobody created.
    NotFound,
    /// The Stream expired, after its Retention or after the idle TTL of its Producer.
    Expired,
    /// A newer epoch of the closing Producer has already written to the Stream. Only a Store that
    /// fences returns it.
    Fenced,
    /// The caller isn't allowed to close this Stream.
    NotOwner,
}

/// The reasons that a describe returns, besides the ones of [`AnyReason`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DescribeReason {
    /// The describe is for a Stream that nobody created.
    NotFound,
    /// The Stream expired, after its Retention or after the idle TTL of its Producer. A Store that
    /// keeps the status of an expired Stream answers with the `Expired` status instead.
    Expired,
}

/// Why a Stream call failed. `R` is the reason enum of the call's operation, such as
/// [`AppendReason`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason<R> {
    /// A reason of the call's operation.
    Op(R),
    /// A reason that any call can return.
    Any(AnyReason),
}

impl<R> From<AnyReason> for Reason<R> {
    fn from(reason: AnyReason) -> Self {
        Self::Any(reason)
    }
}

/// The error that a failed Stream call returns. `R` is the type of its reason, which is a
/// [`Reason`] of the call's operation, or [`AnyReason`] for a check that all operations share.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamError<R> {
    /// Why the call failed.
    pub reason: R,
    /// What went wrong, for logs and for the error that a lang SDK raises.
    pub message: String,
}

/// The error of a create.
pub type CreateError = StreamError<Reason<CreateReason>>;
/// The error of an append.
pub type AppendError = StreamError<Reason<AppendReason>>;
/// The error of a read.
pub type ReadError = StreamError<Reason<ReadReason>>;
/// The error of a close.
pub type CloseError = StreamError<Reason<CloseReason>>;
/// The error of a describe.
pub type DescribeError = StreamError<Reason<DescribeReason>>;
/// An error that any call can return. `?` converts it into the error of an operation.
pub type AnyError = StreamError<AnyReason>;

impl<R> StreamError<R> {
    /// Creates an error with the given reason and message.
    pub fn new(reason: impl Into<R>, message: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            message: message.into(),
        }
    }
}

impl<R: From<AnyReason>> StreamError<R> {
    /// Creates an [`AnyReason::Invalid`] error.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(AnyReason::Invalid, message)
    }

    /// Creates an [`AnyReason::StorageUnavailable`] error.
    pub fn storage_unavailable(message: impl Into<String>) -> Self {
        Self::new(AnyReason::StorageUnavailable, message)
    }

    /// Creates an [`AnyReason::StorageCorrupt`] error.
    pub fn storage_corrupt(message: impl Into<String>) -> Self {
        Self::new(AnyReason::StorageCorrupt, message)
    }
}

impl<R: Copy + Into<StreamErrorReason>> StreamError<R> {
    /// Returns the encoded [`StreamErrorDetails`], for a gRPC status or a Nexus operation failure.
    /// Lang SDKs read the reason from these details instead of parsing the message.
    pub fn details(&self) -> Vec<u8> {
        StreamErrorDetails {
            reason: self.reason.into() as i32,
        }
        .encode_to_vec()
    }

    /// Returns the reason as a stable string, such as `stream_not_found` for
    /// `STREAM_ERROR_REASON_NOT_FOUND`. The error prints it in front of its message.
    pub(crate) fn token(&self) -> String {
        let name = self.reason.into().as_str_name();
        let name = name.trim_start_matches("STREAM_ERROR_REASON_");
        format!("stream_{}", name.to_ascii_lowercase())
    }
}

impl<R: Copy + Into<StreamErrorReason>> fmt::Display for StreamError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.token(), self.message)
    }
}

impl<R: Copy + fmt::Debug + Into<StreamErrorReason>> std::error::Error for StreamError<R> {}

impl<R> From<AnyError> for StreamError<Reason<R>> {
    fn from(error: AnyError) -> Self {
        Self::new(error.reason, error.message)
    }
}

impl From<AnyReason> for StreamErrorReason {
    fn from(reason: AnyReason) -> Self {
        match reason {
            AnyReason::Invalid => Self::Invalid,
            AnyReason::StorageUnavailable => Self::StorageUnavailable,
            AnyReason::StorageCorrupt => Self::StorageCorrupt,
            AnyReason::Unsupported => Self::Unsupported,
            AnyReason::FormatTooNew => Self::FormatTooNew,
        }
    }
}

impl From<CreateReason> for StreamErrorReason {
    fn from(reason: CreateReason) -> Self {
        match reason {
            CreateReason::AlreadyExists => Self::AlreadyExists,
            CreateReason::NotOwner => Self::NotOwner,
            CreateReason::Closed => Self::Closed,
            CreateReason::Expired => Self::Expired,
        }
    }
}

impl From<AppendReason> for StreamErrorReason {
    fn from(reason: AppendReason) -> Self {
        match reason {
            AppendReason::NotFound => Self::NotFound,
            AppendReason::Expired => Self::Expired,
            AppendReason::Closed => Self::Closed,
            AppendReason::Fenced => Self::Fenced,
            AppendReason::SequenceTooLow => Self::SequenceTooLow,
            AppendReason::SeqDivergent => Self::SeqDivergent,
            AppendReason::TooLarge => Self::TooLarge,
        }
    }
}

impl From<ReadReason> for StreamErrorReason {
    fn from(reason: ReadReason) -> Self {
        match reason {
            ReadReason::NotFound => Self::NotFound,
            ReadReason::Expired => Self::Expired,
            ReadReason::OffsetTooHigh => Self::OffsetTooHigh,
        }
    }
}

impl From<CloseReason> for StreamErrorReason {
    fn from(reason: CloseReason) -> Self {
        match reason {
            CloseReason::NotFound => Self::NotFound,
            CloseReason::Expired => Self::Expired,
            CloseReason::Fenced => Self::Fenced,
            CloseReason::NotOwner => Self::NotOwner,
        }
    }
}

impl From<DescribeReason> for StreamErrorReason {
    fn from(reason: DescribeReason) -> Self {
        match reason {
            DescribeReason::NotFound => Self::NotFound,
            DescribeReason::Expired => Self::Expired,
        }
    }
}

impl<R: Into<StreamErrorReason>> From<Reason<R>> for StreamErrorReason {
    fn from(reason: Reason<R>) -> Self {
        match reason {
            Reason::Op(reason) => reason.into(),
            Reason::Any(reason) => reason.into(),
        }
    }
}
