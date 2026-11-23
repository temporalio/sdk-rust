#![allow(unused)]

pub(crate) mod req_resp;

use std::{
    future,
    marker::PhantomData,
    num::NonZero,
    ops::DerefMut,
    pin::{Pin, pin},
    task::{Context, Poll},
};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::{
    FutureExt, Stream, StreamExt, TryFutureExt,
    future::{BoxFuture, LocalBoxFuture},
    stream::{FusedStream, FuturesUnordered},
};
use itertools::Either;
use pin_project::pin_project;
use tokio::{join, sync::mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::abstractions::dbg_panic;

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
    type Data<'a>: Unpin
    where
        Self: Sized + 'a;

    /// Attempts to pull out the next value of this poller.
    ///
    /// Implementors must ensure that when returning [`PollerResult::Drained`], the poller doesn't
    /// contain any unresolved futures internally.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item>;

    /// Shuts down the poller. After calling this method, the poller should be treated as dropped,
    /// and no other methods called on it.
    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>>;

    /// Gets the associated data of this poller. Usually set by [`Self::with_data`], which exposed
    /// data through a mutable reference. See also [`Self::data_unpinned`].
    fn data(self: Pin<&mut Self>) -> Self::Data<'_>
    where
        Self: Sized;

    /// Gets the associated data of this poller through an unpinned reference. Can be called before
    /// pinning the poller. Usually set by [`Self::with_data`], which exposed data through a mutable
    /// reference. See also [`Self::data`].
    fn data_unpinned(self: &mut Self) -> Self::Data<'_>
    where
        Self: Sized;

    /// Wraps this poller in a pinned box with a [`Send`] bound.
    ///
    /// Because [`Self::data`] is passed through static dispatch, boxing a poller makes data
    /// inaccessible. However, new data can be added by calling [`Self::with_data`] again.
    fn boxed<'a>(self) -> BoxedPoller<'a, Self::Item>
    where
        Self: Sized + Send + 'a,
    {
        Box::pin(self)
    }

    /// Wraps this poller in a pinned box without a [`Send`] bound.
    ///
    /// Because [`Self::data`] is passed through static dispatch, boxing a poller makes data
    /// inaccessible. However, new data can be added by calling [`Self::with_data`] again.
    fn boxed_local<'a>(self) -> BoxedLocalPoller<'a, Self::Item>
    where
        Self: Sized + 'a,
    {
        Box::pin(self)
    }

    /// Transforms this poller such that [`PollerResult::Drained`] is replaced with
    /// [`PollerResult::Pending`] if the provided predicate returns true.
    ///
    /// Useful for preventing the poller from being marked as drained prematurely.
    fn pending_when<F>(self, f: F) -> PendingWhen<Self, F>
    where
        Self: Sized,
        F: FnMut(DataRef<'_, Self>) -> bool,
    {
        PendingWhen { inner: self, f }
    }

    /// Maps values using the provided function. See also [`Self::then`].
    fn map<F, R>(self, f: F) -> Map<Self, F, R>
    where
        Self: Sized,
        F: FnMut(DataRef<'_, Self>, Self::Item) -> R,
    {
        Map { inner: self, f }
    }

    /// Maps values using the provided asynchronous function. See also [`Self::map`].
    fn then<F, Fut>(self, f: F) -> Then<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(DataRef<'_, Self>, Self::Item) -> Fut,
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
        F: for<'a> FnMut(DataRef<'a, Self>, &'a Self::Item) -> bool,
    {
        Filter { inner: self, f }
    }

    /// Filters and maps values at the same time using the provided function.
    fn filter_map<F, R>(self, f: F) -> FilterMap<Self, F>
    where
        Self: Sized,
        F: FnMut(DataRef<'_, Self>, Self::Item) -> Option<R>,
    {
        FilterMap { inner: self, f }
    }

    /// Calls the provided function for every value returned by this poller.
    fn inspect<F>(self, f: F) -> Inspect<Self, F>
    where
        Self: Sized,
        F: for<'a> FnMut(DataRef<'a, Self>, &'a Self::Item),
    {
        Inspect { inner: self, f }
    }

    /// Calls the function with poller data at shutdown and awaits its future alongside the inner
    /// shutdown. Both futures run to completion even if one returns an error.
    fn on_shutdown<F, Fut>(self, f: F) -> OnShutdown<Self, F, Fut>
    where
        Self: Sized,
        F: FnOnce(DataRef<'_, Self>) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send,
    {
        OnShutdown {
            inner: self,
            f: Some(f),
            future: PhantomData,
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

    /// Associates data with this poller. It can be later accessed through a mutable reference by
    /// adapter methods, e.g. [`Self::pending_when`] or [`Self::map`]. This allows sharing mutable
    /// state between different adapter method calls.
    ///
    /// The function is passed the previous [`Self::data`] as argument. Note that the previous data
    /// is made inaccessible unless exposed in new data.
    ///
    /// Because data is passed through static dispatch, boxing a poller makes data inaccessible.
    /// However, calling `with_data` on a boxed poller still works.
    fn with_data<T>(mut self, data_fn: impl FnOnce(DataRef<'_, Self>) -> T) -> WithData<Self, T>
    where
        Self: Sized,
        T: Unpin,
    {
        let data = data_fn(DataRef(self.data_unpinned()));
        WithData { inner: self, data }
    }

    /// Converts this poller into a fused stream. As soon as the poller drains the first time, the
    /// stream ends and the poller is shut down.
    ///
    /// WARNING: Do not combine multiple pollers as streams. This will prevent them from reopening
    /// after draining once. Generally, this should be the last method in an adapter chain, and only
    /// used if awaiting shutdown on stream read is the desired behavior.
    fn into_stream_auto_shutdown(mut self) -> IntoStreamAutoShutdown<Self>
    where
        Self: Sized,
    {
        IntoStreamAutoShutdown {
            inner: self,
            shutdown: None,
            shutdown_completed: false,
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

/// Result of [`Poller::poll_next`]. Like [`Poll`], except can also indicate drained status.
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

/// Wraps poller data for use by adapter methods. See [`Poller::with_data`].
#[derive(derive_more::Deref, derive_more::DerefMut)]
pub(crate) struct DataRef<'a, P: Poller + 'a>(P::Data<'a>);

/// Macro that can be used to implement [`Poller::shutdown`] by forwarding it to inner poller.
/// Requires a `project` method (see [`pin_project`]).
macro_rules! forward_shutdown {
    ($field:ident) => {
        fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
            self.project().$field.shutdown()
        }
    };
}

/// Macro that can be used to implement [`Poller::Data`] and related methods by forwarding them to
/// inner poller. Requires a `project` method (see [`pin_project`]).
macro_rules! forward_data {
    ($field:ident: $inner_ty:ty) => {
        type Data<'__data_lifetime>
            = <$inner_ty as Poller>::Data<'__data_lifetime>
        where
            Self: '__data_lifetime;

        fn data(self: Pin<&mut Self>) -> Self::Data<'_> {
            self.project().$field.data()
        }

        fn data_unpinned(&mut self) -> Self::Data<'_> {
            self.$field.data_unpinned()
        }
    };
}

/// Macro that can be used to implement [`Poller::Data`] and related methods by returning `()`.
macro_rules! unit_data {
    () => {
        type Data<'__data_lifetime>
            = ()
        where
            Self: '__data_lifetime;

        fn data(self: Pin<&mut Self>) {}

        fn data_unpinned(&mut self) {}
    };
}

impl<P> Poller for Pin<P>
where
    P: DerefMut + Unpin,
    P::Target: Poller + Sized,
{
    type Item = <P::Target as Poller>::Item;
    type Data<'a>
        = <P::Target as Poller>::Data<'a>
    where
        Self: 'a;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> PollerResult<Self::Item> {
        self.get_mut().as_mut().poll_next(cx)
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        self.get_mut().as_mut().shutdown()
    }

    fn data(self: Pin<&mut Self>) -> Self::Data<'_> {
        self.get_mut().as_mut().data()
    }

    fn data_unpinned(&mut self) -> Self::Data<'_> {
        self.as_mut().data()
    }
}

/// Returned by [`Poller::boxed`].
pub(crate) type BoxedPoller<'a, T> = Pin<Box<dyn Poller<Item = T> + Send + 'a>>;

impl<'a, T> Poller for BoxedPoller<'a, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> PollerResult<Self::Item> {
        self.get_mut().as_mut().poll_next(cx)
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        self.get_mut().as_mut().shutdown()
    }

    unit_data!();
}

/// Returned by [`Poller::boxed_local`].
pub(crate) type BoxedLocalPoller<'a, T> = Pin<Box<dyn Poller<Item = T> + 'a>>;

impl<'a, T> Poller for BoxedLocalPoller<'a, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> PollerResult<Self::Item> {
        self.get_mut().as_mut().poll_next(cx)
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        self.get_mut().as_mut().shutdown()
    }

    unit_data!();
}

/// Returned by [`Poller::pending_when`].
#[pin_project]
pub(crate) struct PendingWhen<P, F> {
    #[pin]
    inner: P,
    f: F,
}

impl<P, F> Poller for PendingWhen<P, F>
where
    P: Poller,
    F: FnMut(DataRef<'_, P>) -> bool,
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        match this.inner.as_mut().poll_next(cx) {
            PollerResult::Ready(v) => PollerResult::Ready(v),
            PollerResult::Pending => PollerResult::Pending,
            PollerResult::Drained => {
                if (this.f)(DataRef(this.inner.data())) {
                    PollerResult::Pending
                } else {
                    PollerResult::Drained
                }
            }
        }
    }

    forward_shutdown!(inner);
    forward_data!(inner: P);
}

/// Returned by [`Poller::map`].
#[pin_project]
pub(crate) struct Map<P, F, R>
where
    P: Poller,
    F: FnMut(DataRef<'_, P>, P::Item) -> R,
{
    #[pin]
    inner: P,
    f: F,
}

impl<P, F, R> Poller for Map<P, F, R>
where
    P: Poller,
    F: FnMut(DataRef<'_, P>, P::Item) -> R,
{
    type Item = R;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        this.inner
            .as_mut()
            .poll_next(cx)
            .map(|v| (this.f)(DataRef(this.inner.data()), v))
    }

    forward_shutdown!(inner);
    forward_data!(inner: P);
}

/// Returned by [`Poller::then`].
#[pin_project]
pub(crate) struct Then<P, F, Fut>
where
    Fut: Future,
{
    #[pin]
    inner: P,
    f: F,
    #[pin]
    fut: Option<Fut>,
}

impl<P, F, Fut> Poller for Then<P, F, Fut>
where
    P: Poller,
    F: FnMut(DataRef<'_, P>, P::Item) -> Fut,
    Fut: Future,
{
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        if this.fut.is_none() {
            match this.inner.as_mut().poll_next(cx) {
                PollerResult::Ready(v) => {
                    this.fut.set(Some((this.f)(DataRef(this.inner.data()), v)));
                }
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

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
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

    forward_data!(inner: P);
}

/// Returned by [`Poller::filter`].
#[pin_project]
pub(crate) struct Filter<P, F> {
    #[pin]
    inner: P,
    f: F,
}

impl<P, F> Poller for Filter<P, F>
where
    P: Poller,
    F: for<'a> FnMut(DataRef<'a, P>, &'a P::Item) -> bool,
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        loop {
            let result = this.inner.as_mut().poll_next(cx);
            if let PollerResult::Ready(v) = &result
                && !(this.f)(DataRef(this.inner.as_mut().data()), v)
            {
                continue;
            }
            return result;
        }
    }

    forward_shutdown!(inner);
    forward_data!(inner: P);
}

/// Returned by [`Poller::filter_map`].
#[pin_project]
pub(crate) struct FilterMap<P, F> {
    #[pin]
    inner: P,
    f: F,
}

impl<P, F, R> Poller for FilterMap<P, F>
where
    P: Poller,
    F: FnMut(DataRef<'_, P>, P::Item) -> Option<R>,
{
    type Item = R;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        loop {
            return match this.inner.as_mut().poll_next(cx) {
                PollerResult::Ready(v) => match (this.f)(DataRef(this.inner.as_mut().data()), v) {
                    Some(v) => PollerResult::Ready(v),
                    None => continue,
                },
                PollerResult::Pending => PollerResult::Pending,
                PollerResult::Drained => PollerResult::Drained,
            };
        }
    }

    forward_shutdown!(inner);
    forward_data!(inner: P);
}

/// Returned by [`Poller::inspect`].
#[pin_project]
pub(crate) struct Inspect<P, F>
where
    P: Poller,
    F: for<'a> FnMut(DataRef<'a, P>, &'a P::Item),
{
    #[pin]
    inner: P,
    f: F,
}

impl<P, F> Poller for Inspect<P, F>
where
    P: Poller,
    F: for<'a> FnMut(DataRef<'a, P>, &'a P::Item),
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        let mut this = self.project();
        let result = this.inner.as_mut().poll_next(cx);
        if let PollerResult::Ready(v) = &result {
            (this.f)(DataRef(this.inner.data()), v);
        }
        result
    }

    forward_shutdown!(inner);
    forward_data!(inner: P);
}

impl<P1, P2> Poller for Either<P1, P2>
where
    P1: Poller,
    P2: Poller<Item = P1::Item>,
{
    type Item = P1::Item;
    type Data<'a>
        = Either<P1::Data<'a>, P2::Data<'a>>
    where
        P1: 'a,
        P2: 'a;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        match self.as_pin_mut() {
            Either::Left(p) => p.poll_next(cx),
            Either::Right(p) => p.poll_next(cx),
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        match self.as_pin_mut() {
            Either::Left(p) => p.shutdown(),
            Either::Right(p) => p.shutdown(),
        }
    }

    fn data(self: Pin<&mut Self>) -> Self::Data<'_> {
        self.as_pin_mut().map_either(|p| p.data(), |p| p.data())
    }

    fn data_unpinned(self: &mut Self) -> Self::Data<'_> {
        self.as_mut()
            .map_either(|p| p.data_unpinned(), |p| p.data_unpinned())
    }
}

/// Returned by [`Poller::on_shutdown`].
#[pin_project]
pub(crate) struct OnShutdown<P, F, Fut> {
    #[pin]
    inner: P,
    f: Option<F>,
    future: PhantomData<fn() -> Fut>,
}

impl<P, F, Fut> Poller for OnShutdown<P, F, Fut>
where
    P: Poller,
    F: FnOnce(DataRef<'_, P>) -> Fut,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        self.project().inner.poll_next(cx)
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        let mut this = self.project();
        let fut = this
            .f
            .take()
            .map(|f| f(DataRef(this.inner.as_mut().data())));
        let shutdown_fut = this.inner.shutdown();
        async move {
            let fut = fut.ok_or_else(|| anyhow!("poller shutdown called twice"))?;
            let (fut_result, shutdown_result) = join!(fut, shutdown_fut);
            fut_result.and(shutdown_result)
        }
        .boxed()
    }

    forward_data!(inner: P);
}

/// Returned by [`Poller::with_data`].
#[pin_project]
pub(crate) struct WithData<P, T>
where
    P: Poller,
    T: Unpin,
{
    #[pin]
    inner: P,
    data: T,
}

impl<P, T> Poller for WithData<P, T>
where
    P: Poller,
    T: Unpin,
{
    type Item = P::Item;
    type Data<'a>
        = &'a mut T
    where
        Self: 'a;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        self.project().inner.poll_next(cx)
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        self.project().inner.shutdown()
    }

    fn data(self: Pin<&mut Self>) -> Self::Data<'_> {
        self.project().data
    }

    fn data_unpinned(self: &mut Self) -> Self::Data<'_> {
        &mut self.data
    }
}

/// Returned by [`Poller::buffer_unordered`].
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

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        let is_empty = self.in_progress_queue.is_empty();
        let shutdown = self.project().inner.shutdown();
        async move {
            shutdown.await?;
            is_empty.ok_or_else(|| anyhow!("poller was not drained before shutdown"))
        }
        .boxed()
    }

    forward_data!(inner: P);
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

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        future::ready(if self.is_terminated {
            Ok(())
        } else {
            Err(anyhow!("poller was not drained before shutdown"))
        })
        .boxed()
    }

    unit_data!();
}

/// Creates [`Poller`] from [`mpsc::UnboundedReceiver`]. The poller is drained when the channel is
/// closed and empty.
pub(crate) fn from_unbounded_receiver<T>(
    receiver: mpsc::UnboundedReceiver<T>,
) -> FromUnboundedReceiver<T> {
    from_stream(UnboundedReceiverStream::new(receiver))
}

/// Returned by [`from_unbounded_receiver`].
pub(crate) type FromUnboundedReceiver<T> = FromStream<UnboundedReceiverStream<T>>;

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

/// Returned by [`from_unbounded_receiver_drainable`].
pub(crate) struct FromUnboundedReceiverDrainable<T> {
    receiver: mpsc::UnboundedReceiver<T>,
}

impl<T> Poller for FromUnboundedReceiverDrainable<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> PollerResult<Self::Item> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(v)) => PollerResult::Ready(v),
            _ => PollerResult::Drained,
        }
    }

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        future::ready(
            self.receiver
                .is_empty()
                .ok_or_else(|| anyhow!("poller was not drained before shutdown")),
        )
        .boxed()
    }

    unit_data!();
}

/// Combines two [`Poller`] objects. Pulls values from whichever poller is ready. If both are ready,
/// the pollers are pulled alternately. The combined poller drained if both pollers are drained at
/// the same time.
///
/// Combined poller's data is a pair of data from both pollers.
///
/// See also [`select_biased`].
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
///
/// Combined poller's data is a pair of data from both pollers.
///
/// See also [`select`].
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
    type Data<'a>
        = (P1::Data<'a>, P2::Data<'a>)
    where
        Self: 'a;

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

    fn shutdown(self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        let this = self.project();
        let shutdown1 = this.poller1.shutdown();
        let shutdown2 = this.poller2.shutdown();
        async move {
            let (result1, result2) = join!(shutdown1, shutdown2);
            result1.and(result2)
        }
        .boxed()
    }

    fn data(self: Pin<&mut Self>) -> Self::Data<'_> {
        let this = self.project();
        (this.poller1.data(), this.poller2.data())
    }

    fn data_unpinned(self: &mut Self) -> Self::Data<'_> {
        (self.poller1.data_unpinned(), self.poller2.data_unpinned())
    }
}

/// Returned by [`Poller::into_stream_auto_shutdown`].
#[pin_project]
pub(crate) struct IntoStreamAutoShutdown<P>
where
    P: Poller,
{
    #[pin]
    inner: P,
    shutdown: Option<BoxFuture<'static, anyhow::Result<()>>>,
    shutdown_completed: bool,
}

impl<P> Stream for IntoStreamAutoShutdown<P>
where
    P: Poller,
{
    type Item = P::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.shutdown_completed {
            return Poll::Ready(None);
        }

        let mut this = self.project();
        let shutdown = match this.shutdown {
            Some(shutdown) => shutdown,
            shutdown @ None => match this.inner.as_mut().poll_next(cx) {
                PollerResult::Ready(v) => return Poll::Ready(Some(v)),
                PollerResult::Pending => return Poll::Pending,
                PollerResult::Drained => shutdown.insert(this.inner.shutdown()),
            },
        };

        match shutdown.as_mut().poll(cx) {
            Poll::Ready(result) => {
                *this.shutdown_completed = true;
                if let Err(e) = result {
                    dbg_panic!("error during poller shutdown: {}", e);
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<P> FusedStream for IntoStreamAutoShutdown<P>
where
    P: Poller,
{
    fn is_terminated(&self) -> bool {
        self.shutdown_completed
    }
}

#[cfg(test)]
mod tests {
    use crate::advance_fut;

    use super::*;
    use futures::{StreamExt, stream, task::noop_waker_ref};
    use std::{
        borrow::Cow,
        cell::Cell,
        collections::{HashSet, VecDeque},
        pin::pin,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::Waker,
    };
    use tokio::sync::{Semaphore, oneshot};

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
        check_drained_on_shutdown: bool,
    }

    impl<T> ScriptedPoller<T> {
        fn new(steps: impl IntoIterator<Item = PollerResult<T>>) -> Self {
            Self {
                steps: steps.into_iter().collect(),
                polls: Arc::new(AtomicUsize::new(0)),
                shut_down: Arc::new(AtomicBool::new(false)),
                check_drained_on_shutdown: true,
            }
        }
    }

    impl<T> Poller for ScriptedPoller<T> {
        type Item = T;
        type Data<'a>
            = ()
        where
            Self: 'a;

        fn data(self: Pin<&mut Self>) {}

        fn data_unpinned(&mut self) {}

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> PollerResult<Self::Item> {
            assert!(!self.shut_down.load(Ordering::SeqCst));
            self.polls.fetch_add(1, Ordering::SeqCst);
            self.project()
                .steps
                .pop_front()
                .unwrap_or(PollerResult::Drained)
        }

        fn shutdown(mut self: Pin<&mut Self>) -> BoxFuture<'static, anyhow::Result<()>> {
            if self.check_drained_on_shutdown {
                let mut cx = Context::from_waker(noop_waker_ref());
                assert!(matches!(
                    self.as_mut().poll_next(&mut cx),
                    PollerResult::Drained
                ));
            }

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
            .map(|_, v| {
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
            .with_data(|_| &calls)
            .filter(|calls, v| {
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
            .with_data(|_| &calls)
            .filter_map(|calls, v| {
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
            .with_data(|_| &calls)
            .inspect(|calls, _| {
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
            .pending_when(|_| {
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
        let multiplier = Cell::new(10);
        let (tx, rx) = oneshot::channel::<()>();
        let mut gate = Some(rx);
        let mut poller = pin!(source.with_data(|_| &multiplier).then(|data, v| {
            let value = v * data.get();
            let mut gate = gate.take();
            let guard = DropGuard(drops.clone());
            futures::future::poll_fn(move |cx| {
                let _keep_until_future_is_dropped = &guard;
                if let Some(gate) = gate.as_mut() {
                    std::task::ready!(Pin::new(gate).poll(cx)).unwrap();
                }
                Poll::Ready(value)
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
    async fn buffer_unordered_buffers_up_to_max() {
        /*
         * Scenario:
         * - first future gets blocked
         * - - poller buffers first future and 2 more futures
         * - 1 future completes
         * - - poller returns 1 result and buffers 1 future
         * - 4 futures complete
         * - - poller returns 2 results
         * - - poller buffers and immediately returns 2 more results
         * - - poller tries to buffer 2 more futures but only 1 is remaining
         * - 1 future completes
         * - - poller returns 1 result
         * - first future completes
         * - - poller returns 1 result and drains
         */
        let (fut1_started_tx, fut1_started_rx) = oneshot::channel();
        let (fut1_unblock_tx, fut1_unblock_rx) = oneshot::channel();

        let sem = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicUsize::new(0));

        let stream = stream::iter([async move {
            fut1_started_tx.send(());
            fut1_unblock_rx.await.unwrap();
            1
        }
        .left_future()])
        .chain(stream::iter(2..=7).map(|v| {
            let sem = sem.clone();
            let started = started.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                sem.acquire().await.unwrap().forget();
                v
            }
            .right_future()
        }));

        let mut poller = pin!(from_stream(stream).buffer_unordered(3));
        let mut completed = HashSet::new();
        let mut cx = Context::from_waker(noop_waker_ref());

        macro_rules! assert_completions {
            ($n:expr, $final_result:expr) => {{
                let mut last_completed = 0;
                for _ in 0..$n {
                    let PollerResult::Ready(v) = poller.as_mut().poll_next(&mut cx) else {
                        panic!();
                    };
                    assert!(completed.insert(v));
                    last_completed = v;
                }
                assert_eq!(poller.as_mut().poll_next(&mut cx), $final_result);
                last_completed
            }};
        }

        assert_eq!(poller.as_mut().poll_next(&mut cx), PollerResult::Pending);
        fut1_started_rx.now_or_never().unwrap().unwrap();
        assert_eq!(started.load(Ordering::SeqCst), 2);

        sem.add_permits(1);
        let v = assert_completions!(1, PollerResult::Pending);
        assert!(v == 2 || v == 3);
        assert_eq!(started.load(Ordering::SeqCst), 3);

        sem.add_permits(4);
        assert_completions!(4, PollerResult::Pending);
        assert_eq!(started.load(Ordering::SeqCst), 6);

        sem.add_permits(1);
        assert_completions!(1, PollerResult::Pending);
        assert_eq!(started.load(Ordering::SeqCst), 6);

        assert!(completed.iter().all(|&v| v != 1));
        fut1_unblock_tx.send(());
        let v = assert_completions!(1, PollerResult::Drained);
        assert_eq!(v, 1);
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
        let source = from_stream(stream::iter([1, 2])).then(|_, v| async move { v * 2 });
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
            source.then(|_, v| async move { v * 2 }).left_poller()
        } else {
            source.then(|_, v| async move { v * 3 }).right_poller()
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
            .pending_when(|_| false)
            .map(|_, x| x)
            .then(|_, x| async move { x })
            .filter(|_, _| true)
            .filter_map(|_, x| Some(x))
            .inspect(|_, _| ())
            .on_shutdown(move |_| async move {
                shutdown_tx1.send(());
                Ok(())
            })
            .buffer_unordered(1)
            .with_data(|_| ());

        // exercising adapters in reverse order (except boxing due to Send bound)
        let poller2 = source2
            .with_data(|_| ())
            .buffer_unordered(1)
            .on_shutdown(move |_| async move {
                shutdown_tx2.send(());
                Ok(())
            })
            .inspect(|_, _| ())
            .filter_map(|_, x| Some(x))
            .filter(|_, _| true)
            .then(|_, x| async move { x })
            .map(|_, x| x)
            .pending_when(|_| false)
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

    #[rstest::rstest]
    #[tokio::test]
    async fn data_propagates_through_adapters(#[values(false, true)] biased: bool) {
        let source1 = ScriptedPoller::new([PollerResult::Ready(async { 1 })]);
        let source2 = ScriptedPoller::<future::Ready<i32>>::new([]);
        // A few data objects with different types
        let data1_first = "data 1 first".to_string();
        let data1_second = Cow::Borrowed("data 1 second");
        let data2_first = ("data 2 first".to_string(), 21);
        let data2_second = (22, "data 2 second".to_string());

        // exercising every adapter method except boxes
        let poller1 = source1
            .with_data({
                let data = data1_first.clone();
                |old_data| {
                    let _: () = *old_data;
                    data
                }
            })
            .pending_when(|data| {
                assert_eq!(**data, data1_first);
                false
            })
            .map(|data, x| {
                assert_eq!(**data, data1_first);
                x
            })
            .then(|data, x| {
                assert_eq!(**data, data1_first);
                async move { x }
            })
            .filter(|data, _| {
                assert_eq!(**data, data1_first);
                true
            })
            .filter_map(|data, x| {
                assert_eq!(**data, data1_first);
                Some(x)
            })
            .inspect(|data, _| assert_eq!(**data, data1_first))
            .on_shutdown(|data| {
                assert_eq!(**data, data1_first);
                future::ready(Ok(()))
            })
            .buffer_unordered(1)
            .with_data({
                let data = data1_second.clone();
                |old_data| {
                    let _: &String = &*old_data;
                    assert_eq!(**old_data, data1_first);
                    data
                }
            });

        // exercising adapter methods in reverse order
        let poller2 = source2
            .with_data({
                let data = data2_first.clone();
                |old_data| {
                    let _: () = *old_data;
                    data
                }
            })
            .buffer_unordered(1)
            .on_shutdown(|data| {
                assert_eq!(**data, data2_first);
                future::ready(Ok(()))
            })
            .inspect(|data, _| assert_eq!(**data, data2_first))
            .filter_map(|data, x| {
                assert_eq!(**data, data2_first);
                Some(x)
            })
            .filter(|data, _| {
                assert_eq!(**data, data2_first);
                true
            })
            .then(|data, x| {
                assert_eq!(**data, data2_first);
                async move { x }
            })
            .map(|data, x| {
                assert_eq!(**data, data2_first);
                x
            })
            .pending_when(|data| {
                assert_eq!(**data, data2_first);
                false
            })
            .with_data({
                let data = data2_second.clone();
                |old_data| {
                    let _: &(String, i32) = &*old_data;
                    assert_eq!(**old_data, data2_first);
                    data
                }
            });

        // exercising select/select_biased and left_poller/right_poller
        let mut poller = pin!(if biased {
            select(poller1, poller2).left_poller()
        } else {
            select_biased(poller1, poller2).right_poller()
        });

        assert_polls! { poller; PollerResult::Ready(1); }
        assert_eq!(
            poller.data().into_inner(),
            (&mut data1_second.clone(), &mut data2_second.clone())
        );
    }

    #[tokio::test]
    async fn check_drained_on_shutdown_shuts_down_on_first_drained() {
        let mut poller = ScriptedPoller::new([
            PollerResult::Ready(async { 1 }.boxed()),
            PollerResult::Drained,
            PollerResult::Ready(async { panic!("polled too much") }.boxed()),
        ]);
        poller.check_drained_on_shutdown = false;
        let polls = poller.polls.clone();
        let shut_down = poller.shut_down.clone();
        let (finish_shutdown_tx, finish_shutdown_rx) = oneshot::channel();

        let stream = poller
            .then(|_, f| f)
            .on_shutdown(move |_| async {
                finish_shutdown_rx.await.unwrap();
                Ok(())
            })
            .into_stream_auto_shutdown();

        let mut stream = pin!(stream);

        assert_eq!(stream.next().await, Some(1));
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(!shut_down.load(Ordering::SeqCst));

        let next = pin!(stream.next());
        advance_fut!(next);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(shut_down.load(Ordering::SeqCst));

        finish_shutdown_tx.send(()).unwrap();
        assert_eq!(next.await, None);
        assert_eq!(polls.load(Ordering::SeqCst), 2);

        assert_eq!(stream.next().await, None);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }
}
