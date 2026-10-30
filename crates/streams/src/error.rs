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
    /// Nobody created the Stream, even after `create_wait`.
    NotFound,
    /// The Stream's Retention or safety TTL passed.
    Expired,
    /// The Stream is closed.
    Closed,
    /// The Producer's epoch is lower than the newest one, its Workflow Task was withdrawn, or a
    /// Workflow retry withdrew its run.
    Fenced,
    /// The batch has a lower `sequence` than the last one of its epoch.
    Sequence,
    /// The batch has the `sequence` of the last one of its epoch, and other content.
    SeqDivergent,
    /// The Stream's owner doesn't allow this Producer to close it.
    NotOwner,
    /// A newer format version wrote the Stream.
    FormatTooNew,
    /// The offset is past the newest offset + 1, for example after a failover lost the newest
    /// Records.
    Offset,
    /// A Record or a batch is over the Store's limit.
    TooLarge,
    /// The request is malformed.
    Invalid,
    /// The Store failed or did not answer.
    Storage,
    /// No writer has the id.
    UnknownWriter,
    /// The Store lacks a capability that the call needs.
    Unsupported,
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
            Reason::Sequence => "stream_sequence",
            Reason::SeqDivergent => "seq_divergent",
            Reason::NotOwner => "stream_not_owner",
            Reason::FormatTooNew => "stream_format_too_new",
            Reason::Offset => "stream_offset",
            Reason::TooLarge => "stream_too_large",
            Reason::Invalid => "stream_invalid",
            Reason::Storage => "stream_storage",
            Reason::UnknownWriter => "stream_unknown_writer",
            Reason::Unsupported => "stream_unsupported",
        }
    }

    /// The reason in the error details.
    pub fn proto(self) -> StreamErrorReason {
        match self {
            Reason::NotFound => StreamErrorReason::NotFound,
            Reason::Expired => StreamErrorReason::Expired,
            Reason::Closed => StreamErrorReason::Closed,
            Reason::Fenced => StreamErrorReason::Fenced,
            Reason::Sequence => StreamErrorReason::Sequence,
            Reason::SeqDivergent => StreamErrorReason::SeqDivergent,
            Reason::NotOwner => StreamErrorReason::NotOwner,
            Reason::FormatTooNew => StreamErrorReason::FormatTooNew,
            Reason::Offset => StreamErrorReason::Offset,
            Reason::TooLarge => StreamErrorReason::TooLarge,
            Reason::Invalid => StreamErrorReason::Invalid,
            Reason::Storage => StreamErrorReason::Storage,
            Reason::UnknownWriter => StreamErrorReason::UnknownWriter,
            Reason::Unsupported => StreamErrorReason::Unsupported,
        }
    }

    /// The status code of the reason, as in the proposal's Appendix A.
    pub fn code(self) -> Code {
        match self {
            Reason::NotFound | Reason::Expired | Reason::UnknownWriter => Code::NotFound,
            Reason::Closed
            | Reason::Fenced
            | Reason::Sequence
            | Reason::SeqDivergent
            | Reason::NotOwner
            | Reason::FormatTooNew => Code::FailedPrecondition,
            Reason::TooLarge | Reason::Invalid => Code::InvalidArgument,
            // A read from the offset works again once the Stream has Records up to it.
            Reason::Offset => Code::OutOfRange,
            Reason::Storage => Code::Unavailable,
            Reason::Unsupported => Code::Unimplemented,
        }
    }
}

/// A failed Stream call.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message}", reason.token())]
pub struct StreamError {
    /// Why the call failed.
    pub reason: Reason,
    /// What happened, for a log or the facade error.
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

    /// A Store failure.
    pub fn storage(message: impl Into<String>) -> Self {
        Self::new(Reason::Storage, message)
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
}
