//! The error of every failed call, and its form for lang.

use crate::proto::{StreamErrorDetails, StreamErrorReason};
use prost::Message;
use tonic::Code;

/// The result of every fallible call in the crate.
pub type StreamResult<T> = Result<T, StreamError>;

/// Why a call failed. Each reason has one status code and one case of `StreamErrorReason`, and lang
/// maps the reason to one error class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    /// Nobody created the Stream.
    NotFound,
    /// The Stream's Retention or Producer idle TTL passed.
    Expired,
    /// The Stream is closed.
    Closed,
    /// The Producer's epoch is lower than the newest one.
    Fenced,
    /// The batch has a lower `sequence` than the last one of its epoch.
    SequenceTooLow,
    /// The batch has the `sequence` of the last one of its epoch, and other content.
    SeqDivergent,
    /// The caller may not create or close this Stream.
    NotOwner,
    /// A newer format version wrote the Stream.
    FormatTooNew,
    /// The offset is past the newest offset, for example after a failover lost the newest Records.
    OffsetTooHigh,
    /// A Record or a batch is over the Store's limit.
    TooLarge,
    /// The request is malformed.
    Invalid,
    /// The Store failed or did not answer. A retry can succeed.
    StorageUnavailable,
    /// The Store does not support what the call asks for.
    Unsupported,
    /// A create that doesn't use an existing Stream asks for the id of a Stream that exists.
    AlreadyExists,
    /// The Store gave back data that isn't valid. A retry reads the same data.
    StorageCorrupt,
}

impl Reason {
    /// The token that starts the error's text. Lang reads the reason from
    /// `StreamErrorDetails.reason`, not from this text.
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

    /// The reason in the error details.
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

    /// The status code of the reason, as the comments of [StreamErrorReason] give it.
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
            // A read from the offset works again once the Stream has Records up to it.
            Reason::OffsetTooHigh => Code::OutOfRange,
            Reason::StorageUnavailable => Code::Unavailable,
            Reason::Unsupported => Code::Unimplemented,
            Reason::AlreadyExists => Code::AlreadyExists,
            Reason::StorageCorrupt => Code::DataLoss,
        }
    }
}

/// The reason of a failure that lang gives as the proto enum. The unspecified value is an error.
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

/// The reason of a failure from the number of its proto value. The unspecified value and a value
/// that the enum doesn't have are errors.
impl TryFrom<i32> for Reason {
    type Error = StreamError;

    fn try_from(value: i32) -> StreamResult<Self> {
        StreamErrorReason::try_from(value)
            .map_err(|_| StreamError::invalid(format!("The error reason {value} is unknown")))?
            .try_into()
    }
}

/// A failed Stream call.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message}", reason.token())]
pub struct StreamError {
    /// Why the call failed.
    pub reason: Reason,
    /// What happened, for a log or for the text of the error that lang raises.
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

    /// A malformed request.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(Reason::Invalid, message)
    }

    /// A Store that failed or did not answer.
    pub fn storage_unavailable(message: impl Into<String>) -> Self {
        Self::new(Reason::StorageUnavailable, message)
    }

    /// Store data that isn't valid.
    pub fn storage_corrupt(message: impl Into<String>) -> Self {
        Self::new(Reason::StorageCorrupt, message)
    }

    /// The status code that lang raises the error with.
    pub fn code(&self) -> Code {
        self.reason.code()
    }

    /// The encoded [StreamErrorDetails], which lang reads the reason from, as from the details of
    /// a gRPC status.
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
    fn an_error_carries_its_code_and_reason_to_lang() {
        let error = StreamError::new(Reason::FormatTooNew, "format 2 is newer than 1");
        assert_eq!(error.code(), Code::FailedPrecondition);
        let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
        assert_eq!(details.reason(), StreamErrorReason::FormatTooNew);
    }

    #[test]
    fn every_reason_has_its_proto_value_status_code_and_token() {
        use StreamErrorReason as Proto;
        let table = [
            (
                Reason::NotFound,
                Proto::NotFound,
                1,
                Code::NotFound,
                "stream_not_found",
            ),
            (
                Reason::Expired,
                Proto::Expired,
                2,
                Code::NotFound,
                "stream_expired",
            ),
            (
                Reason::Closed,
                Proto::Closed,
                3,
                Code::FailedPrecondition,
                "stream_closed",
            ),
            (
                Reason::Fenced,
                Proto::Fenced,
                4,
                Code::FailedPrecondition,
                "stream_fenced",
            ),
            (
                Reason::SequenceTooLow,
                Proto::SequenceTooLow,
                5,
                Code::FailedPrecondition,
                "stream_sequence_too_low",
            ),
            (
                Reason::SeqDivergent,
                Proto::SeqDivergent,
                6,
                Code::FailedPrecondition,
                "seq_divergent",
            ),
            (
                Reason::NotOwner,
                Proto::NotOwner,
                7,
                Code::FailedPrecondition,
                "stream_not_owner",
            ),
            (
                Reason::FormatTooNew,
                Proto::FormatTooNew,
                8,
                Code::FailedPrecondition,
                "stream_format_too_new",
            ),
            (
                Reason::OffsetTooHigh,
                Proto::OffsetTooHigh,
                9,
                Code::OutOfRange,
                "stream_offset_too_high",
            ),
            (
                Reason::TooLarge,
                Proto::TooLarge,
                10,
                Code::InvalidArgument,
                "stream_too_large",
            ),
            (
                Reason::Invalid,
                Proto::Invalid,
                11,
                Code::InvalidArgument,
                "stream_invalid",
            ),
            (
                Reason::StorageUnavailable,
                Proto::StorageUnavailable,
                12,
                Code::Unavailable,
                "stream_storage_unavailable",
            ),
            (
                Reason::Unsupported,
                Proto::Unsupported,
                13,
                Code::Unimplemented,
                "stream_unsupported",
            ),
            (
                Reason::AlreadyExists,
                Proto::AlreadyExists,
                14,
                Code::AlreadyExists,
                "stream_already_exists",
            ),
            (
                Reason::StorageCorrupt,
                Proto::StorageCorrupt,
                15,
                Code::DataLoss,
                "stream_storage_corrupt",
            ),
        ];
        // The table has a row for each value of the proto enum, except the unspecified one.
        let known = (0..100).filter(|n| Proto::try_from(*n).is_ok()).count();
        assert_eq!(table.len(), known - 1);
        for (reason, proto, number, code, token) in table {
            assert_eq!(reason.proto(), proto);
            assert_eq!(proto as i32, number);
            assert_eq!(reason.code(), code);
            assert_eq!(reason.token(), token);
            assert_eq!(Reason::try_from(proto).unwrap(), reason);
            assert_eq!(Reason::try_from(number).unwrap(), reason);
            let error = StreamError::new(reason, "x");
            let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
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
