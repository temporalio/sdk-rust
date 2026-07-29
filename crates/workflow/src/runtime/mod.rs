//! Unstable runtime-facing APIs for workflow hosts and future WASM integrations.
//!
//! These modules collect the parts of the workflow crate that are intended for SDK/runtime glue
//! rather than normal workflow authors.

use crate::runtime::types::RoutinePendingState;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    rc::Rc,
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
    static CURRENT_INTERCEPTED_FUTURE: RefCell<Option<InterceptedFutureStatus>> =
        const { RefCell::new(None) };
}

/// Distinguishes the construction pre-poll from routine polls so the we can avoid
/// entering async handler code during construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InterceptedFuturePollKind {
    Construction,
    Routine,
}

#[derive(Clone, Copy)]
struct InterceptedFutureStatusInner {
    state: RoutinePendingState,
    poll_kind: InterceptedFuturePollKind,
    handler_result_ready: bool,
}

/// Shares poll progress between the outer chain, its terminal handler boundary, and SDK command
/// futures nested anywhere inside the chain.
///
/// This state lives outside [`crate::workflow_interceptors::WorkflowInterceptorFuture`] because
/// interceptors may replace that public wrapper while composing the chain.
#[derive(Clone)]
pub(crate) struct InterceptedFutureStatus(Rc<Cell<InterceptedFutureStatusInner>>);

impl InterceptedFutureStatus {
    /// Starts above the terminal boundary because no part of the chain has been polled yet.
    pub(crate) fn new() -> Self {
        Self(Rc::new(Cell::new(InterceptedFutureStatusInner {
            state: RoutinePendingState::Interceptor,
            poll_kind: InterceptedFuturePollKind::Routine,
            handler_result_ready: false,
        })))
    }

    /// Discards activation evidence from the previous poll so a completed SDK wait followed by an
    /// unrelated pending future is not incorrectly treated as activation-backed.
    ///
    /// Reaching the handler is permanent, so that state is intentionally preserved.
    pub(crate) fn reset_for_poll(&self) {
        let mut status = self.0.get();
        if status.state != RoutinePendingState::Handler {
            status.state = RoutinePendingState::Interceptor;
            self.0.set(status);
        }
    }

    /// Makes the terminal-boundary transition sticky because subsequent pending work belongs to
    /// normal workflow or handler execution rather than interceptor construction.
    pub(crate) fn mark_handler(&self) {
        let mut status = self.0.get();
        status.state = RoutinePendingState::Handler;
        self.0.set(status);
    }

    /// Returns the evidence collected during the latest poll for activation-completion decisions.
    pub(crate) fn state(&self) -> RoutinePendingState {
        self.0.get().state
    }

    /// Allows an already-computed synchronous handler result to flow through construction without
    /// requiring the boundary to probe an arbitrary handler future.
    pub(crate) fn mark_handler_result_ready(&self) {
        let mut status = self.0.get();
        status.handler_result_ready = true;
        self.0.set(status);
    }

    /// Lets the terminal boundary distinguish a known-ready result from an arbitrary future that
    /// must not be probed during construction.
    pub(crate) fn handler_result_ready(&self) -> bool {
        self.0.get().handler_result_ready
    }

    /// Lets the terminal boundary block only the construction pre-poll, not the first routine poll
    /// that happens to reach the boundary.
    pub(crate) fn poll_kind(&self) -> InterceptedFuturePollKind {
        self.0.get().poll_kind
    }

    /// Records an activation-producing SDK wait without overwriting the stronger handler state.
    fn mark_activation(&self) {
        let mut status = self.0.get();
        if status.state == RoutinePendingState::Interceptor {
            status.state = RoutinePendingState::InterceptorWithActivation;
            self.0.set(status);
        }
    }
}

/// Restores the previously tracked future when a poll exits, including through panic unwinding,
/// so nested polls cannot attribute SDK waits to the wrong interceptor chain.
pub(crate) struct InterceptedFuturePollGuard {
    status: InterceptedFutureStatus,
    previous_status: Option<InterceptedFutureStatus>,
    previous_poll_kind: InterceptedFuturePollKind,
}

impl InterceptedFuturePollGuard {
    /// Installs both pieces of poll context together so the handler boundary and SDK futures
    /// attribute their observations to the same intercepted chain.
    pub(crate) fn new(
        status: InterceptedFutureStatus,
        poll_kind: InterceptedFuturePollKind,
    ) -> Self {
        let mut inner_status = status.0.get();
        let previous_poll_kind = inner_status.poll_kind;
        inner_status.poll_kind = poll_kind;
        status.0.set(inner_status);
        let previous_status =
            CURRENT_INTERCEPTED_FUTURE.with(|current| current.replace(Some(status.clone())));
        Self {
            status,
            previous_status,
            previous_poll_kind,
        }
    }
}

impl Drop for InterceptedFuturePollGuard {
    fn drop(&mut self) {
        let mut status = self.status.0.get();
        status.poll_kind = self.previous_poll_kind;
        self.status.0.set(status);
        let previous_status = self.previous_status.take();
        CURRENT_INTERCEPTED_FUTURE.with(|current| *current.borrow_mut() = previous_status);
    }
}

/// Marks the currently polled interceptor as activation-backed when an SDK command future parks.
pub(crate) fn mark_intercepted_future_activation() {
    CURRENT_INTERCEPTED_FUTURE.with(|current| {
        if let Some(status) = current.borrow().as_ref() {
            status.mark_activation();
        }
    });
}

/// Records that synchronous dispatch already computed the handler result, allowing the
/// boundary to be ready without polling async handlers.
pub(crate) fn mark_intercepted_handler_ready() {
    CURRENT_INTERCEPTED_FUTURE.with(|current| {
        if let Some(status) = current.borrow().as_ref() {
            status.mark_handler_result_ready();
        }
    });
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
