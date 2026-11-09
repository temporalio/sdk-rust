use super::{LongPollBuffer, Poller, PollerError, PollerResult};
use crate::{
    WorkerConfig,
    abstractions::OwnedMeteredSemPermit,
    worker::{PollerBehavior, SlotKind},
};
use futures_util::{
    Stream, StreamExt,
    stream::{self, BoxStream},
};
use std::{fmt::Debug, future, sync::OnceLock};
use sync_wrapper::SyncWrapper;
use tokio::sync::{oneshot, watch};

/// Given an already-resolved task poller `behavior` (see
/// [crate::worker::resolve_effective_behavior]), applies the sticky/non-sticky split when it is a
/// [PollerBehavior::SimpleMaximum]; other behaviors are returned unchanged.
pub(crate) fn sticky_poller_behavior(
    behavior: PollerBehavior,
    config: &WorkerConfig,
    is_sticky: bool,
) -> PollerBehavior {
    fn calc_max_nonsticky(max_polls: usize, ratio: f32) -> usize {
        ((max_polls as f32 * ratio) as usize).max(1)
    }

    if let PollerBehavior::SimpleMaximum(m) = behavior {
        if !is_sticky {
            PollerBehavior::SimpleMaximum(calc_max_nonsticky(
                m,
                config.nonsticky_to_sticky_poll_ratio,
            ))
        } else {
            PollerBehavior::SimpleMaximum(
                m.saturating_sub(calc_max_nonsticky(m, config.nonsticky_to_sticky_poll_ratio))
                    .max(1),
            )
        }
    } else {
        behavior
    }
}

type StickyTaskPollerStream<T, SK> =
    BoxStream<'static, PollerResult<(T, OwnedMeteredSemPermit<SK>)>>;

/// A poller capable of polling on a sticky and a nonsticky queue simultaneously.
pub(crate) struct StickyTaskPoller<T, SK: SlotKind> {
    stream: SyncWrapper<StickyTaskPollerStream<T, SK>>,
    normal_poller_rx: oneshot::Receiver<Box<LongPollBuffer<T, SK>>>,
    sticky_poller_rx: Option<oneshot::Receiver<Box<LongPollBuffer<T, SK>>>>,
}

impl<T, SK> StickyTaskPoller<T, SK>
where
    T: Send + Sync + Debug + 'static,
    SK: SlotKind + 'static,
{
    pub(crate) fn new(
        normal_poller: LongPollBuffer<T, SK>,
        sticky_poller: Option<LongPollBuffer<T, SK>>,
    ) -> Self {
        let (normal_poller_tx, normal_poller_rx) = oneshot::channel();
        let normal_stream = Self::streamify(normal_poller, normal_poller_tx, false);

        let (combined_stream, sticky_poller_rx) = if let Some(sticky_poller) = sticky_poller {
            let (sticky_poller_tx, sticky_poller_rx) = oneshot::channel();
            let sticky_stream = Self::streamify(sticky_poller, sticky_poller_tx, true);
            (
                stream::select(normal_stream, sticky_stream).left_stream(),
                Some(sticky_poller_rx),
            )
        } else {
            (normal_stream.right_stream(), None)
        };

        // Each poller will send its own PollerStopped; we're suppressing that and sending a single
        // one at the very end.
        let stream = combined_stream
            .filter(|r| future::ready(!matches!(r, Err(PollerError::PollerStopped))))
            .chain(stream::once(future::ready(Err(PollerError::PollerStopped))))
            .boxed()
            .into();

        Self {
            stream,
            normal_poller_rx,
            sticky_poller_rx,
        }
    }

    fn streamify<P, Item>(
        poller: P,
        tx: oneshot::Sender<Box<P>>,
        is_sticky: bool,
    ) -> impl Stream<Item = PollerResult<Item>>
    where
        P: Poller<Item>,
        Item: Send + Sync + 'static,
    {
        struct State<P_> {
            poller: Box<P_>,
            tx: oneshot::Sender<Box<P_>>,
            is_sticky: bool,
        }
        let state = State {
            poller: Box::new(poller),
            tx,
            is_sticky,
        };

        stream::unfold(state, async |mut state| match state.poller.poll().await {
            Some(r) => Some((r, state)),
            None => {
                if state.tx.send(state.poller).is_err() {
                    Self::warn_channel_closed(state.is_sticky);
                }
                None
            }
        })
    }

    fn warn_channel_closed(is_sticky: bool) {
        warn!(
            "{} poller shutdown channel closed unexpectedly, worker might not shut down cleanly",
            if is_sticky { "Sticky" } else { "Normal" }
        );
    }
}

#[async_trait::async_trait]
impl<T, SK> Poller<(T, OwnedMeteredSemPermit<SK>)> for StickyTaskPoller<T, SK>
where
    T: Send + Sync + Debug + 'static,
    SK: SlotKind,
{
    async fn poll(&mut self) -> Option<PollerResult<(T, OwnedMeteredSemPermit<SK>)>> {
        self.stream.get_mut().next().await
    }

    async fn shutdown(self) {
        match self.normal_poller_rx.await {
            Ok(p) => p.shutdown().await,
            Err(_) => Self::warn_channel_closed(false),
        }

        if let Some(rx) = self.sticky_poller_rx {
            match rx.await {
                Ok(p) => p.shutdown().await,
                Err(_) => Self::warn_channel_closed(true),
            }
        }
    }

    async fn shutdown_box(self: Box<Self>) {
        self.shutdown().await;
    }
}

/// Info that needs to be shared across the sticky and non-sticky pollers to prioritize sticky
/// when appropriate
pub(crate) struct StickyPollerShared {
    last_seen_sticky_backlog: (watch::Receiver<usize>, watch::Sender<usize>),
    sticky_active: OnceLock<watch::Receiver<usize>>,
    sticky_target: OnceLock<watch::Receiver<usize>>,
    non_sticky_active: OnceLock<watch::Receiver<usize>>,
    max_slots: Option<usize>,
}
impl StickyPollerShared {
    pub(crate) fn new(max_slots: Option<usize>) -> Self {
        let (tx, rx) = watch::channel(0);
        Self {
            last_seen_sticky_backlog: (rx, tx),
            sticky_active: OnceLock::new(),
            sticky_target: OnceLock::new(),
            non_sticky_active: OnceLock::new(),
            max_slots,
        }
    }
    pub(crate) fn set_sticky_active(
        &self,
        active_rx: watch::Receiver<usize>,
        target_rx: watch::Receiver<usize>,
    ) {
        let _ = self.sticky_active.set(active_rx);
        let _ = self.sticky_target.set(target_rx);
    }
    pub(crate) fn set_non_sticky_active(&self, active_rx: watch::Receiver<usize>) {
        let _ = self.non_sticky_active.set(active_rx);
    }
    /// Makes either the sticky or non-sticky poller wait pre-permit-acquisition so that we can
    /// balance which kind of queue we poll appropriately.
    pub(crate) async fn wait_if_needed(&self, is_sticky: bool) {
        // We need to make sure there's at least one poller of both kinds available. So, we check
        // that we won't end up using every available permit with one kind of poller. In practice
        // this is only ever likely to be an issue with very small numbers of slots.
        if let Some(max_slots) = self.max_slots
            && let Some(sticky_active) = self.sticky_active.get()
            && let Some(sticky_target) = self.sticky_target.get()
            && let Some(non_sticky_active) = self.non_sticky_active.get()
        {
            let mut sticky_active = sticky_active.clone();
            let mut sticky_target = sticky_target.clone();
            let mut non_sticky_active = non_sticky_active.clone();
            let mut sticky_backlog = self.last_seen_sticky_backlog.0.clone();

            loop {
                let num_sticky_active = *sticky_active.borrow_and_update();
                let num_sticky_target = *sticky_target.borrow_and_update();
                let num_non_sticky_active = *non_sticky_active.borrow_and_update();
                let num_sticky_backlog = *sticky_backlog.borrow_and_update();
                let sticky_should_be_chosen = num_sticky_backlog > 1
                    && num_sticky_backlog > num_sticky_active
                    && num_sticky_active < num_sticky_target;

                let allow = || {
                    if !is_sticky {
                        // There should always be at least one non-sticky poller.
                        if num_non_sticky_active == 0 {
                            return true;
                        }

                        // Do not allow an additional non-sticky poller to prevent starting a first sticky poller.
                        if num_sticky_active == 0 && num_non_sticky_active + 1 >= max_slots {
                            return false;
                        }

                        // If there's a meaningful sticky backlog, prioritize sticky.
                        if sticky_should_be_chosen {
                            return false;
                        }
                    } else {
                        // There should always be at least one sticky poller.
                        if num_sticky_active == 0 {
                            return true;
                        }

                        // Do not reserve a slot that the sticky scaler cannot use.
                        if num_sticky_active >= num_sticky_target {
                            return false;
                        }

                        // Do not allow an additional sticky poller to prevent starting a first non-sticky poller.
                        if num_non_sticky_active == 0 && num_sticky_active + 1 >= max_slots {
                            return false;
                        }

                        // If there's a meaningful sticky backlog, prioritize sticky.
                        if sticky_should_be_chosen {
                            return true;
                        }
                    }

                    // Just balance the two poller types.
                    // FIXME: Do we need anything more here, to ensure proper balancing?
                    if num_sticky_active + num_non_sticky_active < max_slots {
                        return true;
                    }

                    false
                };

                if allow() {
                    return;
                }

                tokio::select! {
                    _ = sticky_active.changed() => (),
                    _ = sticky_target.changed() => (),
                    _ = non_sticky_active.changed() => (),
                    _ = sticky_backlog.changed() => (),
                }
            }
        }
    }

    pub(crate) fn record_sticky_backlog(&self, v: usize) {
        let _ = self.last_seen_sticky_backlog.1.send(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        WorkflowSlotKind, advance_fut, pollers::PollWorkflowTaskQueueResponse,
        test_help::test_worker_cfg,
    };
    use futures_util::FutureExt;

    #[test]
    fn max_polls_calculated_properly() {
        let cfg = {
            let mut cfg = test_worker_cfg().build().unwrap();
            cfg.workflow_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(5_usize));
            cfg
        };
        assert_eq!(
            sticky_poller_behavior(PollerBehavior::SimpleMaximum(5), &cfg, false),
            PollerBehavior::SimpleMaximum(1)
        );
        assert_eq!(
            sticky_poller_behavior(PollerBehavior::SimpleMaximum(5), &cfg, true),
            PollerBehavior::SimpleMaximum(4)
        );
    }

    #[rstest::rstest]
    #[case::normal_ends_first(false)]
    #[case::sticky_ends_first(true)]
    #[tokio::test]
    async fn drains_both_normal_and_sticky_pollers_before_returning_none(
        #[case] sticky_ends_first: bool,
    ) {
        let first_msg = "first message";
        let first_error = Err(tonic::Status::internal(first_msg).into());
        let first_poller =
            LongPollBuffer::<PollWorkflowTaskQueueResponse, WorkflowSlotKind>::mock([
                future::ready(first_error),
            ]);

        let second_msg = "second message";
        let second_error = Err(tonic::Status::internal(second_msg).into());
        let (do_second_poll_tx, do_second_poll_rx) = oneshot::channel::<()>();
        let second_poller = LongPollBuffer::mock([async move {
            do_second_poll_rx.await.unwrap();
            second_error
        }]);

        let mut poller = if sticky_ends_first {
            StickyTaskPoller::new(second_poller, Some(first_poller))
        } else {
            StickyTaskPoller::new(first_poller, Some(second_poller))
        };

        assert_matches!(poller.poll().await, Some(Err(PollerError::Tonic(e))) if e.message() == first_msg);

        {
            let poll_fut = poller.poll();
            advance_fut!(poll_fut);
            do_second_poll_tx.send(()).unwrap();
            assert_matches!(poll_fut.await, Some(Err(PollerError::Tonic(e))) if e.message() == second_msg);
        }

        assert_matches!(poller.poll().await, Some(Err(PollerError::PollerStopped)));
        assert_matches!(poller.poll().await, None);

        poller.shutdown().await;
    }
}
