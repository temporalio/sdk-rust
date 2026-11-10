//! Request extensions for tonic gRPC requests.
//!
//! These types can be inserted into tonic request extensions to modify behavior of retires or other
//! request handling logic.

use crate::RetryOptions;
use std::{fmt::Debug, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

/// A request extension that, when set, should make the retry behavior consider this call to be a
/// worker task long poll.
#[derive(Copy, Clone, Debug)]
pub struct IsWorkerTaskLongPoll;

/// A request extension that, when set, and a call is being processed by a retrying client, allows
/// the caller to request certain matching errors to short-circuit-return immediately and not follow
/// normal retry logic.
#[derive(Copy, Clone, Debug)]
pub struct NoRetryOnMatching {
    /// Return true if the passed-in gRPC error should be immediately returned to the caller
    pub predicate: fn(&tonic::Status) -> bool,
}

/// A request extension that forces overriding the current retry policy of the [crate::Connection].
#[derive(Clone, Debug)]
pub struct RetryConfigForCall(pub RetryOptions);

/// A request extension that forces overriding the current retry policy of the [crate::Connection]
/// used in throttling (resource exhausted) scenarios.
#[derive(Clone, Debug)]
pub struct ThrottleRetryConfigForCall(pub RetryOptions);

/// A request extension that signals a semaphore between retries. Semaphore must be initialized with
/// 0 permits. When a retry is scheduled, one permit is released. Before retry, the request blocks
/// until it can acquire one permit. Effectively, if other code blocks on the same semaphore, it
/// will only be run between attempts, not during an attempt and not after the call completes.
#[derive(Clone, Debug)]
pub struct RetrySemaphore(pub Arc<Semaphore>);

/// Per-call payload/memo size error limits, attached to a request's extensions by a caller that
/// wants error-level enforcement on this call.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PayloadErrorLimits {
    /// Blob (payload) size error threshold, in bytes.
    pub blob: usize,
    /// Memo size error threshold, in bytes.
    pub memo: usize,
}

/// Extension trait for tonic requests to set default timeouts
pub trait RequestExt {
    /// Set a timeout for a request if one is not already specified in the metadata
    fn set_default_timeout(&mut self, duration: Duration);
}

impl<T> RequestExt for tonic::Request<T> {
    fn set_default_timeout(&mut self, duration: Duration) {
        if !self.metadata().contains_key("grpc-timeout") {
            self.set_timeout(duration)
        }
    }
}
