//! Unstable runtime-facing APIs for workflow hosts and future WASM integrations.
//!
//! These modules collect the parts of the workflow crate that are intended for SDK/runtime glue
//! rather than normal workflow authors.

use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub mod entry;
pub mod guest;
pub mod host;
pub mod instance;
pub mod model;
pub mod types;

thread_local! {
    static SDK_WAKE_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// Guard that marks the current scope as an SDK-initiated wake source.
#[doc(hidden)]
pub struct SdkWakeGuard {
    _priv: (),
}

impl SdkWakeGuard {
    #[doc(hidden)]
    pub fn new() -> Self {
        SDK_WAKE_DEPTH.with(|c| c.set(c.get() + 1));
        Self { _priv: () }
    }
}

impl Drop for SdkWakeGuard {
    fn drop(&mut self) {
        SDK_WAKE_DEPTH.with(|c| c.set(c.get() - 1));
    }
}

#[doc(hidden)]
pub fn is_sdk_wake() -> bool {
    SDK_WAKE_DEPTH.with(|c| c.get() > 0)
}

/// A future wrapper that activates [`SdkWakeGuard`] during poll. Use this around futures whose
/// internal waker machinery would otherwise trigger false positives in nondeterminism detection.
pub(crate) struct SdkGuardedFuture<F>(pub(crate) F);

impl<F: Future + Unpin> Future for SdkGuardedFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = SdkWakeGuard::new();
        Pin::new(&mut self.0).poll(cx)
    }
}
