//! The error that a failed Stream call returns, and its gRPC form for lang SDKs.

use crate::proto::{StreamErrorDetails, StreamErrorReason};
use prost::Message;
use tonic::Code;

/// The result of a fallible call in this crate.
pub type StreamResult<T> = Result<T, StreamError>;

/// Why a Stream call failed. Each reason maps to one gRPC status code and one
/// [`StreamErrorReason`] value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    /// The Stream was never created.
    NotFound,
    /// The Stream's Retention or the Producer's idle TTL has passed.
    Expired,
    /// The Stream is closed.
    Closed,
    /// A higher epoch of the same Producer already wrote to the Stream.
    Fenced,
    /// The batch's `sequence` is lower than the last one that its epoch wrote.
    SequenceTooLow,
    /// The batch repeats the last `sequence` of its epoch, but with different content.
    SeqDivergent,
    /// The caller isn't allowed to create or close this Stream.
    NotOwner,
    /// The Stream uses a newer format version than this Store can read.
    FormatTooNew,
    /// The offset is past the end of the Stream, for example after a failover lost the newest
    /// Records.
    OffsetTooHigh,
    /// A Record or a batch is over one of the Store's limits.
    TooLarge,
    /// The request is malformed.
    Invalid,
    /// The Store failed or didn't answer. A retry can succeed.
    StorageUnavailable,
    /// The Store doesn't support what the call needs.
    Unsupported,
    /// A create with the `Fail` conflict policy asks for the id of a Stream that exists.
    AlreadyExists,
    /// The Store returned data that isn't valid. A retry reads the same data, so it fails too.
    StorageCorrupt,
}

impl Reason {
    /// The prefix of the error's `Display` text. Lang SDKs read the reason from
    /// [`StreamErrorDetails`], not from the text.
    pub fn token(self) -> &'static str {
        match self {
            Reason::NotFound => "stream_not_found",
            Reason::Expired => "stream_expired",
            Reason::Closed => "stream_closed",
            Reason::Fenced => "stream_fenced",
            Reason::SequenceTooLow => "stream_sequence_too_low",
            Reason::SeqDivergent => "seq_divergent",
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

    /// The matching [`StreamErrorReason`] value.
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

    /// The gRPC status code of the reason, as documented on [`StreamErrorReason`].
    pub fn code(self) -> Code {
        match self {
            Reason::NotFound | Reason::Expired => Code::NotFound,
            Reason::Closed
            | Reason::Fenced
            | Reason::SequenceTooLow
            | Reason::SeqDivergent
            | Reason::NotOwner
            | Reason::FormatTooNew => Code::FailedPrecondition,
            Reason::TooLarge | Reason::Invalid => Code::InvalidArgument,
            // A read at this offset succeeds again once the Stream has Records up to it.
            Reason::OffsetTooHigh => Code::OutOfRange,
            Reason::StorageUnavailable => Code::Unavailable,
            Reason::Unsupported => Code::Unimplemented,
            Reason::AlreadyExists => Code::AlreadyExists,
            Reason::StorageCorrupt => Code::DataLoss,
        }
    }
}

/// Fails for `Unspecified`, which has no matching reason.
impl TryFrom<StreamErrorReason> for Reason {
    type Error = StreamError;

    fn try_from(reason: StreamErrorReason) -> StreamResult<Self> {
        Ok(match reason {
            StreamErrorReason::Unspecified => {
                return Err(StreamError::invalid("The error reason is unspecified"));
            }
            StreamErrorReason::NotFound => Reason::NotFound,
            StreamErrorReason::Expired => Reason::Expired,
            StreamErrorReason::Closed => Reason::Closed,
            StreamErrorReason::Fenced => Reason::Fenced,
            StreamErrorReason::SequenceTooLow => Reason::SequenceTooLow,
            StreamErrorReason::SeqDivergent => Reason::SeqDivergent,
            StreamErrorReason::NotOwner => Reason::NotOwner,
            StreamErrorReason::FormatTooNew => Reason::FormatTooNew,
            StreamErrorReason::OffsetTooHigh => Reason::OffsetTooHigh,
            StreamErrorReason::TooLarge => Reason::TooLarge,
            StreamErrorReason::Invalid => Reason::Invalid,
            StreamErrorReason::StorageUnavailable => Reason::StorageUnavailable,
            StreamErrorReason::Unsupported => Reason::Unsupported,
            StreamErrorReason::AlreadyExists => Reason::AlreadyExists,
            StreamErrorReason::StorageCorrupt => Reason::StorageCorrupt,
        })
    }
}

/// Fails for `Unspecified` and for a number that isn't a [`StreamErrorReason`] value.
impl TryFrom<i32> for Reason {
    type Error = StreamError;

    fn try_from(value: i32) -> StreamResult<Self> {
        StreamErrorReason::try_from(value)
            .map_err(|_| StreamError::invalid(format!("The error reason {value} is unknown")))?
            .try_into()
    }
}

/// The error of a failed Stream call.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message}", reason.token())]
pub struct StreamError {
    /// Why the call failed.
    pub reason: Reason,
    /// What happened. It goes into logs and into the error that lang SDKs raise.
    pub message: String,
}

impl StreamError {
    /// Creates an error.
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

    /// The gRPC status code that lang SDKs raise the error with.
    pub fn code(&self) -> Code {
        self.reason.code()
    }

    /// The encoded [`StreamErrorDetails`], for a gRPC status or a Nexus operation failure. Lang
    /// SDKs read the reason from it.
    pub fn details(&self) -> Vec<u8> {
        StreamErrorDetails {
            reason: self.reason.proto() as i32,
        }
        .encode_to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reason_round_trips_through_its_proto_value_and_the_details() {
        for proto in (1..100).filter_map(|n| StreamErrorReason::try_from(n).ok()) {
            let reason = Reason::try_from(proto).unwrap();
            assert_eq!(reason.proto(), proto);
            let details = StreamError::new(reason, "x").details();
            let details = StreamErrorDetails::decode(details.as_slice()).unwrap();
            assert_eq!(Reason::try_from(details.reason).unwrap(), reason);
        }
    }

    #[test]
    fn an_unspecified_or_unknown_reason_is_invalid() {
        for (value, message) in [
            (0, "The error reason is unspecified"),
            (16, "The error reason 16 is unknown"),
            (-1, "The error reason -1 is unknown"),
        ] {
            let error = Reason::try_from(value).unwrap_err();
            assert_eq!(error.reason, Reason::Invalid);
            assert_eq!(error.message, message);
        }
        let error = Reason::try_from(StreamErrorReason::Unspecified).unwrap_err();
        assert_eq!(error.message, "The error reason is unspecified");
    }
}
