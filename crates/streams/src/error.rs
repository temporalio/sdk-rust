use crate::proto::{StreamErrorDetails, StreamErrorReason};
use prost::Message;

/// The result type of a Stream call.
pub type StreamResult<T> = Result<T, StreamError>;

/// The reason a Stream call failed. There's one variant for each [`StreamErrorReason`] except
/// `Unspecified`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    /// The Stream doesn't exist.
    NotFound,
    /// The Stream expired, after its Retention or after its Producer idle TTL.
    Expired,
    /// The Stream is closed.
    Closed,
    /// A newer epoch of the same Producer has already written to the Stream.
    Fenced,
    /// The batch's `sequence` is lower than the last one written in its epoch.
    SequenceTooLow,
    /// The batch reuses the `sequence` of the last batch of its epoch, with different content.
    SeqDivergent,
    /// The caller isn't allowed to create or close this Stream.
    NotOwner,
    /// The Stream was written with a newer format version than this build understands.
    FormatTooNew,
    /// The offset is past the end of the Stream, for example after a failover lost the newest
    /// Records.
    OffsetTooHigh,
    /// A Record or a batch is larger than the Store allows.
    TooLarge,
    /// The request is malformed.
    Invalid,
    /// The Store failed or didn't respond. A retry may succeed.
    StorageUnavailable,
    /// The Store doesn't support something that the call needs.
    Unsupported,
    /// The Stream already exists, and the create used the `Fail` conflict policy.
    AlreadyExists,
    /// The Store returned invalid data, and a retry would read the same data again. An append or
    /// a create that fails this way may still have taken effect.
    StorageCorrupt,
}

impl Reason {
    /// Returns the reason as a stable string that starts with `stream_`. A [`StreamError`] prints
    /// it in front of its message.
    pub fn token(self) -> &'static str {
        match self {
            Reason::NotFound => "stream_not_found",
            Reason::Expired => "stream_expired",
            Reason::Closed => "stream_closed",
            Reason::Fenced => "stream_fenced",
            Reason::SequenceTooLow => "stream_sequence_too_low",
            Reason::SeqDivergent => "stream_seq_divergent",
            Reason::NotOwner => "stream_not_owner",
            Reason::FormatTooNew => "stream_format_too_new",
            Reason::OffsetTooHigh => "stream_offset_too_high",
            Reason::TooLarge => "stream_too_large",
            Reason::Invalid => "stream_invalid",
            Reason::StorageUnavailable => "stream_storage_unavailable",
            Reason::Unsupported => "stream_unsupported",
            Reason::AlreadyExists => "stream_already_exists",
            Reason::StorageCorrupt => "stream_storage_corrupt",
        }
    }

    /// Returns the matching [`StreamErrorReason`].
    pub fn proto(self) -> StreamErrorReason {
        match self {
            Reason::NotFound => StreamErrorReason::NotFound,
            Reason::Expired => StreamErrorReason::Expired,
            Reason::Closed => StreamErrorReason::Closed,
            Reason::Fenced => StreamErrorReason::Fenced,
            Reason::SequenceTooLow => StreamErrorReason::SequenceTooLow,
            Reason::SeqDivergent => StreamErrorReason::SeqDivergent,
            Reason::NotOwner => StreamErrorReason::NotOwner,
            Reason::FormatTooNew => StreamErrorReason::FormatTooNew,
            Reason::OffsetTooHigh => StreamErrorReason::OffsetTooHigh,
            Reason::TooLarge => StreamErrorReason::TooLarge,
            Reason::Invalid => StreamErrorReason::Invalid,
            Reason::StorageUnavailable => StreamErrorReason::StorageUnavailable,
            Reason::Unsupported => StreamErrorReason::Unsupported,
            Reason::AlreadyExists => StreamErrorReason::AlreadyExists,
            Reason::StorageCorrupt => StreamErrorReason::StorageCorrupt,
        }
    }
}

/// The error that a failed Stream call returns.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message}", reason.token())]
pub struct StreamError {
    /// Why the call failed.
    pub reason: Reason,
    /// What went wrong, for logs and for the error that a lang SDK raises.
    pub message: String,
}

impl StreamError {
    /// Creates an error with the given reason and message.
    pub fn new(reason: Reason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }

    /// Creates a [`Reason::Invalid`] error.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(Reason::Invalid, message)
    }

    /// Creates a [`Reason::StorageUnavailable`] error.
    pub fn storage_unavailable(message: impl Into<String>) -> Self {
        Self::new(Reason::StorageUnavailable, message)
    }

    /// Creates a [`Reason::StorageCorrupt`] error.
    pub fn storage_corrupt(message: impl Into<String>) -> Self {
        Self::new(Reason::StorageCorrupt, message)
    }

    /// Returns the encoded [`StreamErrorDetails`], for a gRPC status or a Nexus operation failure.
    /// Lang SDKs read the reason from these details instead of parsing the message.
    pub fn details(&self) -> Vec<u8> {
        StreamErrorDetails {
            reason: self.reason.proto() as i32,
        }
        .encode_to_vec()
    }
}
