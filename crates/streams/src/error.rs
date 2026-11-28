use crate::proto::{StreamErrorDetails, StreamErrorReason};
use enum_iterator::Sequence;
use prost::Message;
use std::fmt;

/// A reason that any Stream call can return.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Sequence)]
#[non_exhaustive]
pub enum AnyReason {
    /// The request is malformed. The runtime checks each request before the Store gets it.
    InvalidArgument,
    /// The Store failed or didn't respond. A retry may succeed.
    StoreUnavailable,
    /// The Store returned invalid data, and a retry would read the same data again. The runtime
    /// returns it for an answer that breaks the contract of [`StreamStore`](crate::StreamStore). An
    /// append or a create that fails this way may still have taken effect.
    StoreCorrupt,
    /// The Store doesn't support something that the call needs. The runtime returns it for a
    /// Record that is larger than its limits take.
    Unsupported,
    /// The Stream was written with a newer format version than this build understands.
    FormatTooNew,
}

/// The reasons that a create returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Sequence)]
#[non_exhaustive]
pub enum CreateReason {
    /// The Stream already exists, and the create used the `Fail` conflict policy.
    AlreadyExists,
    /// The Stream expired. A create fails for it with either conflict policy.
    Expired,
    /// A reason that any call can return.
    Other(AnyReason),
}

/// The reasons that an append returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Sequence)]
#[non_exhaustive]
pub enum AppendReason {
    /// The append is for a Stream that nobody created, or for one that the Store forgot after it
    /// expired.
    NotFound,
    /// The Stream expired.
    Expired,
    /// A later attempt of the batch's Producer has already written to the Stream. Only a Store that
    /// fences returns it.
    Fenced,
    /// A Record or the batch is larger than the Store allows. The runtime returns it for a batch
    /// over its [`StoreLimits`](crate::record::StoreLimits).
    TooLarge,
    /// A reason that any call can return.
    Other(AnyReason),
}

/// The reasons that a read returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Sequence)]
#[non_exhaustive]
pub enum ReadReason {
    /// The read is for a Stream that nobody created, or for one that the Store forgot after it
    /// expired.
    NotFound,
    /// The Stream expired.
    Expired,
    /// The read's `from_offset` is past the end of the Stream. A read from it works again once the
    /// Stream has Records up to it.
    OffsetTooHigh,
    /// A reason that any call can return.
    Other(AnyReason),
}

/// The reasons that a describe returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Sequence)]
#[non_exhaustive]
pub enum DescribeReason {
    /// The describe is for a Stream that nobody created, or for one that the Store forgot after it
    /// expired.
    NotFound,
    /// The Stream expired.
    Expired,
    /// A reason that any call can return.
    Other(AnyReason),
}

/// The error that a failed Stream call returns. `R` is the reason enum of the call's operation,
/// such as [`AppendReason`], or [`AnyReason`] for a check that all operations share.
///
/// A Store fails a call with a reason of its operation, or with one that any call can return:
///
/// ```
/// use temporalio_streams::{AnyError, AnyReason, AppendError, AppendReason, ReadError};
///
/// let expired = AppendError::operation(AppendReason::Expired, "The Stream expired");
/// assert_eq!(expired.to_string(), "stream_expired: The Stream expired");
///
/// let down = ReadError::store_unavailable("Redis didn't answer");
///
/// // An `AnyError` becomes the error of any operation, which is what `?` does with it.
/// let invalid: AppendError = AnyError::invalid_argument("The Stream id is empty").into();
/// assert_eq!(
///     invalid.reason,
///     AppendReason::Other(AnyReason::InvalidArgument)
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub struct StreamError<R> {
    /// Why the call failed.
    pub reason: R,
    /// What went wrong, for logs and for the error that a lang SDK raises.
    pub message: String,
}

/// The error of a create.
pub type CreateError = StreamError<CreateReason>;
/// The error of an append.
pub type AppendError = StreamError<AppendReason>;
/// The error of a read.
pub type ReadError = StreamError<ReadReason>;
/// The error of a describe.
pub type DescribeError = StreamError<DescribeReason>;
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

impl<R: WireReason> StreamError<R> {
    /// Creates an error with a reason of the call's operation.
    pub fn operation(reason: R, message: impl Into<String>) -> Self {
        Self::new(reason, message)
    }
}

impl<R: From<AnyReason>> StreamError<R> {
    /// Creates an [`AnyReason::InvalidArgument`] error.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(AnyReason::InvalidArgument, message)
    }

    /// Creates an [`AnyReason::StoreUnavailable`] error.
    pub fn store_unavailable(message: impl Into<String>) -> Self {
        Self::new(AnyReason::StoreUnavailable, message)
    }

    /// Creates an [`AnyReason::StoreCorrupt`] error.
    pub fn store_corrupt(message: impl Into<String>) -> Self {
        Self::new(AnyReason::StoreCorrupt, message)
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
    pub fn token(&self) -> String {
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

impl<R: WireReason> From<AnyError> for StreamError<R> {
    fn from(error: AnyError) -> Self {
        Self::new(error.reason, error.message)
    }
}

impl From<AnyReason> for StreamErrorReason {
    fn from(reason: AnyReason) -> Self {
        match reason {
            AnyReason::InvalidArgument => Self::InvalidArgument,
            AnyReason::StoreUnavailable => Self::StoreUnavailable,
            AnyReason::StoreCorrupt => Self::StoreCorrupt,
            AnyReason::Unsupported => Self::Unsupported,
            AnyReason::FormatTooNew => Self::FormatTooNew,
        }
    }
}

impl From<CreateReason> for StreamErrorReason {
    fn from(reason: CreateReason) -> Self {
        match reason {
            CreateReason::AlreadyExists => Self::AlreadyExists,
            CreateReason::Expired => Self::Expired,
            CreateReason::Other(reason) => reason.into(),
        }
    }
}

impl From<AppendReason> for StreamErrorReason {
    fn from(reason: AppendReason) -> Self {
        match reason {
            AppendReason::NotFound => Self::NotFound,
            AppendReason::Expired => Self::Expired,
            AppendReason::Fenced => Self::Fenced,
            AppendReason::TooLarge => Self::TooLarge,
            AppendReason::Other(reason) => reason.into(),
        }
    }
}

impl From<ReadReason> for StreamErrorReason {
    fn from(reason: ReadReason) -> Self {
        match reason {
            ReadReason::NotFound => Self::NotFound,
            ReadReason::Expired => Self::Expired,
            ReadReason::OffsetTooHigh => Self::OffsetTooHigh,
            ReadReason::Other(reason) => reason.into(),
        }
    }
}

impl From<DescribeReason> for StreamErrorReason {
    fn from(reason: DescribeReason) -> Self {
        match reason {
            DescribeReason::NotFound => Self::NotFound,
            DescribeReason::Expired => Self::Expired,
            DescribeReason::Other(reason) => reason.into(),
        }
    }
}

/// The reason enum of one operation, such as [`AppendReason`]. [`StreamError::from_wire`] finds the
/// reason of a number among the values of the enum.
pub trait WireReason:
    Sequence + Copy + From<AnyReason> + Into<StreamErrorReason> + 'static
{
}

macro_rules! operation_reasons {
    ($($reason:ident),*) => {$(
        impl From<AnyReason> for $reason {
            fn from(reason: AnyReason) -> Self {
                Self::Other(reason)
            }
        }

        impl WireReason for $reason {}
    )*};
}

operation_reasons!(CreateReason, AppendReason, ReadReason, DescribeReason);

impl<R: WireReason> StreamError<R> {
    /// Creates the error of an operation from the number of a `StreamErrorReason` and a message.
    /// The bridge of a lang SDK uses it to return the error of a Store that is written in the
    /// SDK's language. Zero, an unknown number and a reason that the operation can't return give
    /// [`AnyReason::StoreCorrupt`], with the number in the message.
    pub fn from_wire(reason: i32, message: impl Into<String>) -> Self {
        let message = message.into();
        let wire = StreamErrorReason::try_from(reason).ok();
        let found =
            wire.and_then(|wire| enum_iterator::all::<R>().find(|reason| (*reason).into() == wire));
        match found {
            Some(reason) => Self::new(reason, message),
            None => Self::store_corrupt(format!(
                "The Store returned the reason {reason}, which this operation can't return, with \
                 the message `{message}`"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use StreamErrorReason as Wire;

    #[test]
    fn an_operation_gets_only_the_reasons_that_it_can_return() {
        fn check<R: WireReason + fmt::Debug>() {
            let of_operation = enum_iterator::all::<R>().map(Into::into);
            let of_any = enum_iterator::all::<AnyReason>().map(Into::into);
            let returns: Vec<Wire> = of_operation.chain(of_any).collect();
            // The numbers of the proto, with zero, and one that the proto doesn't have.
            let unknown = (1..).find(|number| Wire::try_from(*number).is_err());
            for number in 0..=unknown.unwrap() {
                let error = StreamError::<R>::from_wire(number, "It failed");
                let returned = (Wire::try_from(number).ok()).filter(|wire| returns.contains(wire));
                let expected = match returned {
                    Some(wire) => (wire, "It failed".to_string()),
                    None => (
                        Wire::StoreCorrupt,
                        format!(
                            "The Store returned the reason {number}, which this operation can't \
                             return, with the message `It failed`"
                        ),
                    ),
                };
                assert_eq!((error.reason.into(), error.message), expected);
            }
        }
        check::<CreateReason>();
        check::<AppendReason>();
        check::<ReadReason>();
        check::<DescribeReason>();
    }
}
