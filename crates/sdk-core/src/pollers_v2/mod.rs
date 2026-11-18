#![allow(unused)]

pub(crate) mod req_resp;

use std::{
    future,
    num::NonZero,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::{
    FutureExt, Stream, StreamExt, TryFutureExt,
    future::{BoxFuture, LocalBoxFuture},
    stream::FuturesUnordered,
};
use itertools::Either;
use pin_project::pin_project;
use tokio::{join, sync::mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Asynchronously produces values. Similar to `Stream`, except being empty is not a terminal state:
/// even after returning [`PollerResult::Drained`], poller can continue producing values on
/// subsequent polls.
///
/// When a poller is drained, it means there aren't any values ready or expected to be ready
/// eventually at the time of poll. It also guarantees the poller doesn't contain any unresolved
/// futures internally. In other words, a drained `Poller` can be safely shut down without risk of
/// losing any work in progress, such as undelivered worker tasks.
///
/// WARNING: There is no way to check this invariant, but code using pollers must be structured such
/// that a drained poller can only be repopulated by the code that pulls from that poller. For
/// example, WFT completion poller is polled by Workflow Worker and can be drained if there are no
/// pending completions, but the Worker can receive more WFTs which will add new pending completions
/// and un-drain completion poller. When poller is populated by external source, such as server task
/// polls or eager executions, it must keep returning `Pending` instead of `Drained` until the
/// external source is closed.
pub(crate) trait Poller {
    type Item;

    /// Attempts to pull out the next value of this poller.
    ///
    /// Implementors must ensure that when returning [`PollerResult::Drained`], the poller doesn't
    /// contain any unresolved futures internally.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item>;

    /// Shuts down the poller. After calling this method, the poller should be treated as dropped,
    /// and no other methods called on it.
    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Wraps this poller in a pinned box with a [`Send`] bound.
    fn boxed<'a>(self) -> BoxedPoller<'a, Self::Item>
    where
        Self: Sized + Send + 'a,
    {
        Box::pin(self)
    }

    /// Wraps this poller in a pinned box without a [`Send`] bound.
    fn boxed_local<'a>(self) -> BoxedLocalPoller<'a, Self::Item>
    where
        Self: Sized + 'a,
    {
        Box::pin(self)
    }

    /// Transforms this poller such that [`PollerResult::Drained`] is replaced with
    /// [`PollerResult::Pending`] if the provided predicate returns true. Useful for preventing the
    /// poller from being marked as drained prematurely.
    fn pending_when<F>(self, f: F) -> PendingWhen<Self, F>
    where
        Self: Sized,
        F: FnMut() -> bool,
    {
        PendingWhen { inner: self, f }
    }

    /// Maps values using the provided function. See also [`Self::then`].
    fn map<F, R>(self, f: F) -> Map<Self, F, R>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> R,
    {
        Map { inner: self, f }
    }

    /// Maps values using the provided asynchronous function. See also [`Self::map`].
    fn then<F, Fut>(self, f: F) -> Then<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future,
    {
        Then {
            inner: self,
            f,
            fut: None,
        }
    }

    /// Filters values using the provided predicate.
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> bool,
    {
        Filter { inner: self, f }
    }

    /// Filters and maps values at the same time using the provided function.
    fn filter_map<F, R>(self, f: F) -> FilterMap<Self, F, R>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Option<R>,
    {
        FilterMap { inner: self, f }
    }

    /// Calls the provided function for every value returned by this poller.
    fn inspect<F>(self, f: F) -> Inspect<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item),
    {
        Inspect { inner: self, f }
    }

    /// Awaits provided future before shutting down. Even if the future returns an error, full
    /// shutdown is performed (except if the future panics).
    fn on_shutdown<Fut>(self, fut: Fut) -> OnShutdown<Self, Fut>
    where
        Self: Sized,
        Fut: Future<Output = anyhow::Result<()>> + Send,
    {
        OnShutdown {
            inner: self,
            fut: Some(fut),
        }
    }

    /// Buffers multiple futures from this poller and awaits them at the same time, returning values
    /// in the order of completion of the futures. The number of buffered futures never exceeds
    /// specified maximum.
    fn buffer_unordered(self, max: usize) -> BufferUnordered<Self>
    where
        Self: Sized,
        Self::Item: Future,
    {
        BufferUnordered {
            inner: self,
            in_progress_queue: FuturesUnordered::new(),
            max,
        }
    }

    /// Wraps the poller in [`Either::Left`]. Can be used together with [`Self::right_poller`] to
    /// select between different poller types.
    fn left_poller<P>(self) -> Either<Self, P>
    where
        Self: Sized,
        P: Poller<Item = Self::Item>,
    {
        Either::Left(self)
    }

    /// Wraps the poller in [`Either::Right`]. Can be used together with [`Self::left_poller`] to
    /// select between different poller types.
    fn right_poller<P>(self) -> Either<P, Self>
    where
        Self: Sized,
        P: Poller<Item = Self::Item>,
    {
        Either::Right(self)
    }
}

#[must_use = "this `PollerResult` may be a `Pending` variant, which should be handled"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PollerResult<T> {
    /// Poller returned a value.
    Ready(T),
    /// Poller is actively waiting for a value to become available.
    Pending,
    /// Poller is drained and doesn't expect any more values at the moment.
    Drained,
}

impl<T> PollerResult<T> {
    /// Convert ready result using provided function
    pub(crate) fn map<R>(self, f: impl FnOnce(T) -> R) -> PollerResult<R> {
        match self {
            Self::Ready(v) => PollerResult::Ready(f(v)),
            Self::Pending => PollerResult::Pending,
            Self::Drained => PollerResult::Drained,
        }
    }
}

impl<T> From<PollerResult<T>> for std::task::Poll<T> {
    fn from(value: PollerResult<T>) -> Self {
        if let PollerResult::Ready(v) = value {
            Self::Ready(v)
        } else {
            Self::Pending
        }
    }
}

impl<P> Poller for Pin<P>
where
    P: DerefMut + Unpin,
    P::Target: Poller,
{
    type Item = <P::Target as Poller>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> PollerResult<Self::Item> {
        self.get_mut().as_mut().poll_next(cx)
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        self.get_mut().as_mut().shutdown()
    }
}

pub(crate) type BoxedPoller<'a, T> = Pin<Box<dyn Poller<Item = T> + Send + 'a>>;
pub(crate) type BoxedLocalPoller<'a, T> = Pin<Box<dyn Poller<Item = T> + 'a>>;

#[pin_project]
pub(crate) struct PendingWhen<P, F>
where
    P: Poller,
    F: FnMut() -> bool,
{
    #[pin]
    inner: P,
    f: F,
}

impl<P, F> Poller for PendingWhen<P, F>
where
    P: Poller,
    F: FnMut() -> bool,
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            PollerResult::Ready(v) => PollerResult::Ready(v),
            PollerResult::Pending => PollerResult::Pending,
            PollerResult::Drained => {
                if (this.f)() {
                    PollerResult::Pending
                } else {
                    PollerResult::Drained
                }
            }
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        self.project().inner.shutdown()
    }
}

#[pin_project]
pub(crate) struct Map<P, F, R>
where
    P: Poller,
    F: FnMut(P::Item) -> R,
{
    #[pin]
    inner: P,
    f: F,
}

impl<P, F, R> Poller for Map<P, F, R>
where
    P: Poller,
    F: FnMut(P::Item) -> R,
{
    type Item = R;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let this = self.project();
        this.inner.poll_next(cx).map(this.f)
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        self.project().inner.shutdown()
    }
}

#[pin_project]
pub(crate) struct Then<P, F, Fut>
where
    P: Poller,
    F: FnMut(P::Item) -> Fut,
    Fut: Future,
{
    #[pin]
    inner: P,
    f: F,
    #[pin]
    fut: Option<F::Output>,
}

impl<P, F, Fut> Poller for Then<P, F, Fut>
where
    P: Poller,
    F: FnMut(P::Item) -> Fut,
    Fut: Future,
{
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        if this.fut.is_none() {
            match this.inner.poll_next(cx) {
                PollerResult::Ready(v) => this.fut.set(Some((this.f)(v))),
                PollerResult::Pending => return PollerResult::Pending,
                PollerResult::Drained => return PollerResult::Drained,
            }
        }
        match this.fut.as_mut().as_pin_mut().unwrap().poll(cx) {
            Poll::Ready(v) => {
                this.fut.set(None);
                PollerResult::Ready(v)
            }
            Poll::Pending => PollerResult::Pending,
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        let mut this = self.project();
        let inner_shutdown = this.inner.shutdown();
        let fut_was_some = this.fut.is_some();
        this.fut.set(None);
        Box::pin(async move {
            inner_shutdown.await?;
            if fut_was_some {
                bail!("poller was not drained before shutdown");
            }
            Ok(())
        })
    }
}

#[pin_project]
pub(crate) struct Filter<P, F>
where
    P: Poller,
    F: FnMut(&P::Item) -> bool,
{
    #[pin]
    inner: P,
    f: F,
}

impl<P, F> Poller for Filter<P, F>
where
    P: Poller,
    F: FnMut(&P::Item) -> bool,
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        loop {
            let result = this.inner.as_mut().poll_next(cx);
            if let PollerResult::Ready(v) = &result
                && !(this.f)(v)
            {
                continue;
            }
            return result;
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        self.project().inner.shutdown()
    }
}

#[pin_project]
pub(crate) struct FilterMap<P, F, R>
where
    P: Poller,
    F: FnMut(P::Item) -> Option<R>,
{
    #[pin]
    inner: P,
    f: F,
}

impl<P, F, R> Poller for FilterMap<P, F, R>
where
    P: Poller,
    F: FnMut(P::Item) -> Option<R>,
{
    type Item = R;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        loop {
            return match this.inner.as_mut().poll_next(cx) {
                PollerResult::Ready(v) => match (this.f)(v) {
                    Some(v) => PollerResult::Ready(v),
                    None => continue,
                },
                PollerResult::Pending => PollerResult::Pending,
                PollerResult::Drained => PollerResult::Drained,
            };
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        self.project().inner.shutdown()
    }
}

#[pin_project]
pub(crate) struct Inspect<P, F>
where
    P: Poller,
    F: FnMut(&P::Item),
{
    #[pin]
    inner: P,
    f: F,
}

impl<P, F> Poller for Inspect<P, F>
where
    P: Poller,
    F: FnMut(&P::Item),
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        loop {
            let result = this.inner.as_mut().poll_next(cx);
            if let PollerResult::Ready(v) = &result {
                (this.f)(v);
            }
            return result;
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        self.project().inner.shutdown()
    }
}

impl<P1, P2> Poller for Either<P1, P2>
where
    P1: Poller,
    P2: Poller<Item = P1::Item>,
{
    type Item = P1::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        match self.as_pin_mut() {
            Either::Left(p) => p.poll_next(cx),
            Either::Right(p) => p.poll_next(cx),
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        match self.as_pin_mut() {
            Either::Left(p) => p.shutdown(),
            Either::Right(p) => p.shutdown(),
        }
    }
}

#[pin_project]
pub(crate) struct OnShutdown<P, Fut>
where
    P: Poller,
    Fut: Future<Output = anyhow::Result<()>> + Send,
{
    #[pin]
    inner: P,
    fut: Option<Fut>,
}

impl<P, Fut> Poller for OnShutdown<P, Fut>
where
    P: Poller,
    Fut: Future<Output = anyhow::Result<()>> + Send,
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        self.project().inner.poll_next(cx)
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        let mut this = self.project();
        let fut = this.fut.take();
        let shutdown_fut = this.inner.shutdown();
        async move {
            let fut = fut.ok_or_else(|| anyhow!("poller shutdown called twice"))?;
            let (fut_result, shutdown_result) = join!(fut, shutdown_fut);
            fut_result.and(shutdown_result)
        }
        .boxed()
    }
}

#[pin_project]
pub(crate) struct BufferUnordered<P>
where
    P: Poller,
    P::Item: Future,
{
    #[pin]
    inner: P,
    #[pin]
    in_progress_queue: FuturesUnordered<P::Item>,
    max: usize,
}

impl<P> Poller for BufferUnordered<P>
where
    P: Poller,
    P::Item: Future,
{
    type Item = <P::Item as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let max = self.max;
        let mut this = self.project();
        let mut drained = false;
        while this.in_progress_queue.len() < max {
            match this.inner.as_mut().poll_next(cx) {
                PollerResult::Ready(fut) => this.in_progress_queue.push(fut),
                PollerResult::Pending => break,
                PollerResult::Drained => {
                    drained = true;
                    break;
                }
            }
        }
        match this.in_progress_queue.poll_next(cx) {
            Poll::Ready(Some(v)) => PollerResult::Ready(v),
            Poll::Ready(None) if drained => PollerResult::Drained,
            Poll::Ready(None) | Poll::Pending => PollerResult::Pending,
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        self.project().inner.shutdown()
    }
}

/// Creates [`Poller`] from [`Stream`]. After the stream terminates, the poller will always be
/// drained.
pub(crate) fn from_stream<S>(stream: S) -> FromStream<S>
where
    S: Stream,
{
    FromStream {
        stream,
        is_terminated: false,
    }
}

/// Returned by [`from_stream`].
#[pin_project]
pub(crate) struct FromStream<S>
where
    S: Stream,
{
    #[pin]
    stream: S,
    is_terminated: bool,
}

impl<S> Poller for FromStream<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        if self.is_terminated {
            PollerResult::Drained
        } else {
            let this = self.project();
            match this.stream.poll_next(cx) {
                Poll::Ready(Some(v)) => PollerResult::Ready(v),
                Poll::Ready(None) => {
                    *this.is_terminated = true;
                    PollerResult::Drained
                }
                Poll::Pending => PollerResult::Pending,
            }
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        future::ready(if self.is_terminated {
            Ok(())
        } else {
            Err(anyhow!("poller was not drained before shutdown"))
        })
        .boxed()
    }
}

/// Returned by [`from_unbounded_receiver`].
pub(crate) type FromUnboundedReceiver<T> = FromStream<UnboundedReceiverStream<T>>;

/// Creates [`Poller`] from [`mpsc::UnboundedReceiver`]. The poller is drained when the channel is
/// closed and empty.
pub(crate) fn from_unbounded_receiver<T>(
    receiver: mpsc::UnboundedReceiver<T>,
) -> FromUnboundedReceiver<T> {
    from_stream(UnboundedReceiverStream::new(receiver))
}

/// Creates [`Poller`] from [`mpsc::UnboundedReceiver`]. Whenever the channel is empty, the poller
/// will return [`PollerResult::Drained`].
///
/// WARNING: Make sure all senders of the channel are controlled by the code polling the poller, or
/// use [`Poller::pending_when`] to control when an empty channel should be treated as pending.
pub(crate) fn from_unbounded_receiver_drainable<T>(
    receiver: mpsc::UnboundedReceiver<T>,
) -> FromUnboundedReceiverDrainable<T> {
    FromUnboundedReceiverDrainable { receiver }
}

pub(crate) struct FromUnboundedReceiverDrainable<T> {
    receiver: mpsc::UnboundedReceiver<T>,
}

impl<T> Poller for FromUnboundedReceiverDrainable<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        match self.receiver.try_recv() {
            Ok(v) => PollerResult::Ready(v),
            Err(mpsc::error::TryRecvError::Disconnected | mpsc::error::TryRecvError::Empty) => {
                PollerResult::Drained
            }
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        future::ready(if !self.receiver.is_empty() {
            Err(anyhow!("poller was not drained before shutdown"))
        } else {
            Ok(())
        })
        .boxed()
    }
}

/// Combines two [`Poller`] objects. Pulls values from whichever poller is ready. If both are ready,
/// the pollers are pulled alternately. The combined poller drained if both pollers are drained at
/// the same time.
pub(crate) fn select<P1, P2>(poller1: P1, poller2: P2) -> Select<P1, P2>
where
    P1: Poller,
    P2: Poller<Item = P1::Item>,
{
    Select {
        poller1,
        poller2,
        biased: false,
        reverse_poll_order: false,
    }
}

/// Combines two [`Poller`] objects. Pulls values from whichever poller is ready. If both are ready,
/// the first poller has priority. The combined poller drained if both pollers are drained at the
/// same time.
pub(crate) fn select_biased<P1, P2>(poller1: P1, poller2: P2) -> Select<P1, P2>
where
    P1: Poller,
    P2: Poller<Item = P1::Item>,
{
    Select {
        poller1,
        poller2,
        biased: true,
        reverse_poll_order: false,
    }
}

#[pin_project]
pub(crate) struct Select<P1, P2>
where
    P1: Poller,
    P2: Poller<Item = P1::Item>,
{
    #[pin]
    poller1: P1,
    #[pin]
    poller2: P2,
    biased: bool,
    reverse_poll_order: bool,
}

impl<P1, P2> Poller for Select<P1, P2>
where
    P1: Poller,
    P2: Poller<Item = P1::Item>,
{
    type Item = P1::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let (biased, reverse_poll_order) = (self.biased, self.reverse_poll_order);
        let this = self.project();

        macro_rules! do_poll {
            ($first:expr, $second:expr, $new_reverse:expr) => {{
                let first_pending = match $first.poll_next(cx) {
                    PollerResult::Ready(v) => {
                        if !biased {
                            *this.reverse_poll_order = $new_reverse;
                        }
                        return PollerResult::Ready(v);
                    }
                    PollerResult::Pending => true,
                    PollerResult::Drained => false,
                };
                match $second.poll_next(cx) {
                    PollerResult::Ready(v) => {
                        if !biased {
                            *this.reverse_poll_order = !$new_reverse;
                        }
                        PollerResult::Ready(v)
                    }
                    PollerResult::Pending => PollerResult::Pending,
                    PollerResult::Drained if first_pending => PollerResult::Pending,
                    PollerResult::Drained => PollerResult::Drained,
                }
            }};
        }

        if reverse_poll_order {
            do_poll!(this.poller2, this.poller1, false)
        } else {
            do_poll!(this.poller1, this.poller2, true)
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
        let this = self.project();
        let shutdown1 = this.poller1.shutdown();
        let shutdown2 = this.poller2.shutdown();
        async move {
            let (result1, result2) = join!(shutdown1, shutdown2);
            result1.and(result2)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, stream, task::noop_waker_ref};
    use std::{
        cell::Cell,
        collections::VecDeque,
        pin::pin,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::Waker,
    };
    use tokio::sync::oneshot;

    macro_rules! assert_polls {
        (
            $poller:expr;
            $(
                $($expected:ident)::+ $(($($args:expr),* $(,)?))? $(+ $after:expr)?;
            )*
        ) => {{
            let poller = &mut $poller;
            let mut cx = Context::from_waker(noop_waker_ref());
            $(
                assert_eq!(
                    poller.as_mut().poll_next(&mut cx),
                    $($expected)::+ $(($($args),*))?
                );
                $($after;)?
            )*
        }};
    }

    #[pin_project]
    struct ScriptedPoller<T> {
        steps: VecDeque<PollerResult<T>>,
        polls: Arc<AtomicUsize>,
        shut_down: Arc<AtomicBool>,
    }

    impl<T> ScriptedPoller<T> {
        fn new(steps: impl IntoIterator<Item = PollerResult<T>>) -> Self {
            Self {
                steps: steps.into_iter().collect(),
                polls: Arc::new(AtomicUsize::new(0)),
                shut_down: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl<T> Poller for ScriptedPoller<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> PollerResult<Self::Item> {
            assert!(!self.shut_down.load(Ordering::SeqCst));
            self.polls.fetch_add(1, Ordering::SeqCst);
            self.project()
                .steps
                .pop_front()
                .unwrap_or(PollerResult::Drained)
        }

        fn shutdown(mut self: Pin<&mut Self>) -> BoxFuture<'_, anyhow::Result<()>> {
            let mut cx = Context::from_waker(noop_waker_ref());
            assert!(matches!(
                self.as_mut().poll_next(&mut cx),
                PollerResult::Drained
            ));
            let shut_down = self.shut_down.clone();
            async move {
                shut_down.store(true, Ordering::SeqCst);
                Ok(())
            }
            .boxed()
        }
    }

    #[test]
    fn map_only_transforms_ready_values() {
        let calls = Cell::new(0);
        let mut poller = pin!(
            ScriptedPoller::new([
                PollerResult::Pending,
                PollerResult::Ready(3),
                PollerResult::Drained,
                PollerResult::Ready(5),
            ])
            .map(|v| {
                calls.set(calls.get() + 1);
                v.to_string()
            })
        );

        assert_polls! { poller;
            PollerResult::Pending + assert_eq!(calls.get(), 0);
            PollerResult::Ready("3".to_string()) + assert_eq!(calls.get(), 1);
            PollerResult::Drained + assert_eq!(calls.get(), 1);
            PollerResult::Ready("5".to_string()) + assert_eq!(calls.get(), 2);
        }
    }

    #[test]
    fn filter_only_skips_ready_values() {
        let calls = Cell::new(0);
        let mut poller = pin!(
            ScriptedPoller::new([
                PollerResult::Pending,
                PollerResult::Ready(1),
                PollerResult::Pending,
                PollerResult::Ready(-2),
                PollerResult::Drained,
                PollerResult::Ready(-3),
                PollerResult::Ready(-4),
                PollerResult::Pending,
                PollerResult::Ready(-5),
                PollerResult::Ready(6),
                PollerResult::Ready(-7),
            ])
            .filter(|v| {
                calls.set(calls.get() + 1);
                *v > 0
            })
        );

        assert_polls! { poller;
            PollerResult::Pending + assert_eq!(calls.get(), 0);
            PollerResult::Ready(1) + assert_eq!(calls.get(), 1);
            PollerResult::Pending + assert_eq!(calls.get(), 1);
            PollerResult::Drained + assert_eq!(calls.get(), 2);
            PollerResult::Pending + assert_eq!(calls.get(), 4);
            PollerResult::Ready(6) + assert_eq!(calls.get(), 6);
            PollerResult::Drained + assert_eq!(calls.get(), 7);
        }
    }

    #[test]
    fn filter_map_only_transforms_ready_values() {
        let calls = Cell::new(0);
        let mut poller = pin!(
            ScriptedPoller::new([
                PollerResult::Pending,
                PollerResult::Ready(1),
                PollerResult::Pending,
                PollerResult::Ready(-2),
                PollerResult::Drained,
                PollerResult::Ready(-3),
                PollerResult::Ready(-4),
                PollerResult::Pending,
                PollerResult::Ready(-5),
                PollerResult::Ready(6),
                PollerResult::Ready(-7),
            ])
            .filter_map(|v| {
                calls.set(calls.get() + 1);
                (v > 0).then(|| v.to_string())
            })
        );

        assert_polls! { poller;
            PollerResult::Pending + assert_eq!(calls.get(), 0);
            PollerResult::Ready("1".to_string()) + assert_eq!(calls.get(), 1);
            PollerResult::Pending + assert_eq!(calls.get(), 1);
            PollerResult::Drained + assert_eq!(calls.get(), 2);
            PollerResult::Pending + assert_eq!(calls.get(), 4);
            PollerResult::Ready("6".to_string()) + assert_eq!(calls.get(), 6);
            PollerResult::Drained + assert_eq!(calls.get(), 7);
        }
    }

    #[test]
    fn inspect_only_called_for_ready_values() {
        let calls = Cell::new(0);
        let mut poller = pin!(
            ScriptedPoller::new([
                PollerResult::Pending,
                PollerResult::Ready(3),
                PollerResult::Drained,
                PollerResult::Ready(5),
            ])
            .inspect(|v| {
                calls.set(calls.get() + 1);
            })
        );

        assert_polls! { poller;
            PollerResult::Pending + assert_eq!(calls.get(), 0);
            PollerResult::Ready(3) + assert_eq!(calls.get(), 1);
            PollerResult::Drained + assert_eq!(calls.get(), 1);
            PollerResult::Ready(5) + assert_eq!(calls.get(), 2);
        }
    }

    #[test]
    fn pending_when_only_calls_predicate_when_drained() {
        let calls = Cell::new(0);
        let mut poller = pin!(
            ScriptedPoller::new([
                PollerResult::Ready(1),
                PollerResult::Pending,
                PollerResult::Drained,
                PollerResult::Drained,
                PollerResult::Ready(2),
            ])
            .pending_when(|| {
                calls.set(calls.get() + 1);
                match calls.get() {
                    1 => true,
                    2 => false,
                    _ => panic!("predicate called for a ready or pending poller"),
                }
            })
        );

        assert_polls! { poller;
            PollerResult::Ready(1);
            PollerResult::Pending;
            PollerResult::Pending;
            PollerResult::Drained;
            PollerResult::Ready(2);
        }
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn then_retains_pending_future_and_drops_it_when_ready() {
        struct DropGuard(Rc<Cell<usize>>);

        impl Drop for DropGuard {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let source = ScriptedPoller::new([
            PollerResult::Pending,
            PollerResult::Ready(2),
            PollerResult::Drained,
            PollerResult::Ready(3),
        ]);
        let polls = source.polls.clone();
        let drops = Rc::new(Cell::new(0));
        let (tx, rx) = oneshot::channel::<()>();
        let mut gate = Some(rx);
        let mut poller = pin!(source.then(|v| {
            let mut gate = gate.take();
            let guard = DropGuard(drops.clone());
            futures::future::poll_fn(move |cx| {
                let _keep_until_future_is_dropped = &guard;
                if let Some(gate) = gate.as_mut() {
                    std::task::ready!(Pin::new(gate).poll(cx)).unwrap();
                }
                Poll::Ready(v * 10)
            })
        }));

        let mut cx = Context::from_waker(noop_waker_ref());
        assert_eq!(poller.as_mut().poll_next(&mut cx), PollerResult::Pending);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        for _ in 0..2 {
            assert_eq!(poller.as_mut().poll_next(&mut cx), PollerResult::Pending);
            assert_eq!(polls.load(Ordering::SeqCst), 2);
            assert_eq!(drops.get(), 0);
        }
        tx.send(()).unwrap();
        assert_eq!(poller.as_mut().poll_next(&mut cx), PollerResult::Ready(20));
        assert_eq!(drops.get(), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(poller.as_mut().poll_next(&mut cx), PollerResult::Drained);
        assert_eq!(poller.as_mut().poll_next(&mut cx), PollerResult::Ready(30));
        assert_eq!(drops.get(), 2);
        assert_eq!(poller.as_mut().poll_next(&mut cx), PollerResult::Drained);
    }

    #[tokio::test]
    async fn from_stream_wakes_for_values_and_stays_drained_after_close() {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let mut poller = pin!(from_stream(
            stream::iter([rx1, rx2]).then(async |x| x.await.unwrap())
        ));
        let mut cx = Context::from_waker(noop_waker_ref());

        assert_polls! { poller;
            PollerResult::Pending;
        }

        tx1.send(1);
        assert_polls! { poller;
            PollerResult::Ready(1);
            PollerResult::Pending;
        }

        tx2.send(2);
        assert_polls! { poller;
            PollerResult::Ready(2);
            PollerResult::Drained;
        }
    }

    #[test]
    fn drainable_receiver_can_be_repopulated_and_drains_queued_values_on_close() {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut poller = pin!(from_unbounded_receiver_drainable(rx));

        assert_polls! { poller;
            PollerResult::Drained;
        }

        tx.send(1).unwrap();
        assert_polls! { poller;
            PollerResult::Ready(1);
            PollerResult::Drained;
        }

        tx.send(2).unwrap();
        tx.send(3).unwrap();
        drop(tx);
        assert_polls! { poller;
            PollerResult::Ready(2);
            PollerResult::Ready(3);
            PollerResult::Drained;
        }
    }

    #[rstest::rstest]
    fn boxed_non_unpin_pollers(#[values(true, false)] local: bool) {
        let source = from_stream(stream::iter([1, 2])).then(async |v| v * 2);
        let mut poller = if local {
            source.boxed_local()
        } else {
            source.boxed()
        };

        assert_polls! { poller;
            PollerResult::Ready(2);
            PollerResult::Ready(4);
            PollerResult::Drained;
        }
    }

    #[rstest::rstest]
    #[case::left(true, 4)]
    #[case::right(false, 6)]
    fn either_delegates_both_variants_with_non_unpin_pollers(
        #[case] left: bool,
        #[case] expected: i32,
    ) {
        let source = from_stream(stream::iter([2]));
        let mut poller = pin!(if left {
            source.then(async |v| v * 2).left_poller()
        } else {
            source.then(async |v| v * 3).right_poller()
        });
        assert_polls! { poller;
            PollerResult::Ready(expected);
            PollerResult::Drained;
        }
    }

    #[rstest::rstest]
    #[case::ready_ready(PollerResult::Ready(1), PollerResult::Ready(1), PollerResult::Ready(1))]
    #[case::ready_pending(PollerResult::Ready(1), PollerResult::Pending, PollerResult::Ready(1))]
    #[case::pending_ready(PollerResult::Pending, PollerResult::Ready(1), PollerResult::Ready(1))]
    #[case::ready_drained(PollerResult::Ready(1), PollerResult::Drained, PollerResult::Ready(1))]
    #[case::drained_ready(PollerResult::Drained, PollerResult::Ready(1), PollerResult::Ready(1))]
    #[case::pending_pending(PollerResult::Pending, PollerResult::Pending, PollerResult::Pending)]
    #[case::pending_drained(PollerResult::Pending, PollerResult::Drained, PollerResult::Pending)]
    #[case::drained_pending(PollerResult::Drained, PollerResult::Pending, PollerResult::Pending)]
    #[case::drained_drained(PollerResult::Drained, PollerResult::Drained, PollerResult::Drained)]
    fn select_prioritizes_ready_then_pending_over_drained(
        #[values(false, true)] biased: bool,
        #[case] first_poller_result: PollerResult<i32>,
        #[case] second_poller_result: PollerResult<i32>,
        #[case] expected_result: PollerResult<i32>,
    ) {
        let first = ScriptedPoller::new([first_poller_result]);
        let second = ScriptedPoller::new([second_poller_result]);
        let mut poller = if biased {
            select_biased(first, second).boxed_local()
        } else {
            select(first, second).boxed_local()
        };
        assert_polls! { poller;
            expected_result;
        }
    }

    #[test]
    fn select_alternates_after_either_source_yields() {
        let first = ScriptedPoller::new([
            PollerResult::Pending,
            PollerResult::Ready(1),
            PollerResult::Ready(2),
        ]);
        let second = ScriptedPoller::new([
            PollerResult::Ready(3),
            PollerResult::Ready(4),
            PollerResult::Ready(5),
        ]);
        let first_polls = first.polls.clone();
        let mut poller = pin!(select(first, second));

        assert_polls! { poller;
            PollerResult::Ready(3);
            PollerResult::Ready(1);
            PollerResult::Ready(4);
            PollerResult::Ready(2);
            PollerResult::Ready(5) + assert_eq!(first_polls.load(Ordering::SeqCst), 3);
            PollerResult::Drained;
        }
    }

    #[test]
    fn select_biased_exhausts_first_source_before_second() {
        let first = ScriptedPoller::new([PollerResult::Ready(1), PollerResult::Ready(2)]);
        let second = ScriptedPoller::new([PollerResult::Ready(3), PollerResult::Ready(4)]);
        let second_polls = second.polls.clone();
        let mut poller = pin!(select_biased(first, second));

        assert_polls! { poller;
            PollerResult::Ready(1) + assert_eq!(second_polls.load(Ordering::SeqCst), 0);
            PollerResult::Ready(2) + assert_eq!(second_polls.load(Ordering::SeqCst), 0);
            PollerResult::Ready(3);
            PollerResult::Ready(4);
            PollerResult::Drained;
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn shutdown_propagates_through_adapters(#[values(false, true)] biased: bool) {
        let source1 = ScriptedPoller::new([PollerResult::Ready(async { 1 })]);
        let source2 = ScriptedPoller::<future::Ready<i32>>::new([]);
        let shut_down1 = source1.shut_down.clone();
        let shut_down2 = source2.shut_down.clone();
        let (shutdown_tx1, shutdown_rx1) = oneshot::channel();
        let (shutdown_tx2, shutdown_rx2) = oneshot::channel();

        // exercising every adapter method
        let poller1 = source1
            .boxed()
            .boxed_local()
            .pending_when(|| false)
            .map(|x| x)
            .then(async |x| x)
            .filter(|_| true)
            .inspect(|_| ())
            .on_shutdown(async move {
                shutdown_tx1.send(());
                Ok(())
            })
            .buffer_unordered(1);

        // exercising every adapter method in reverse order (except boxing due to Send bound)
        let poller2 = source2
            .buffer_unordered(1)
            .on_shutdown(async move {
                shutdown_tx2.send(());
                Ok(())
            })
            .inspect(|_| ())
            .filter(|_| true)
            .then(async |x| x)
            .map(|x| x)
            .pending_when(|| false)
            .boxed()
            .boxed_local();

        // exercising select/select_biased and left_poller/right_poller
        let mut poller = pin!(if biased {
            select(poller1, poller2).left_poller()
        } else {
            select_biased(poller1, poller2).right_poller()
        });

        assert_polls! { poller; PollerResult::Ready(1); }

        fn assert_send<T>(_: &T)
        where
            T: Send,
        {
        }

        let shutdown_fut = poller.shutdown();
        assert_send(&shutdown_fut);
        shutdown_fut.await;
        assert!(shut_down1.load(Ordering::SeqCst));
        assert!(shut_down2.load(Ordering::SeqCst));
        shutdown_rx1.now_or_never().unwrap().unwrap();
        shutdown_rx2.now_or_never().unwrap().unwrap();
    }
}
