use super::{LongPollBuffer, Poller};
use crate::{
    WorkerConfig,
    abstractions::OwnedMeteredSemPermit,
    worker::{PollerBehavior, SlotKind},
};
use std::{fmt::Debug, sync::OnceLock};
use tokio::sync::watch;

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

/// A poller capable of polling on a sticky and a nonsticky queue simultaneously.
#[derive(derive_more::Constructor)]
pub(crate) struct StickyTaskPoller<T, SK: SlotKind> {
    normal_poller: LongPollBuffer<T, SK>,
    sticky_poller: Option<LongPollBuffer<T, SK>>,
}

#[async_trait::async_trait]
impl<T, SK> Poller<(T, OwnedMeteredSemPermit<SK>)> for StickyTaskPoller<T, SK>
where
    T: Send + Sync + Debug + 'static,
    SK: SlotKind + 'static,
{
    async fn poll(&self) -> Option<super::Result<(T, OwnedMeteredSemPermit<SK>)>> {
        if let Some(sq) = self.sticky_poller.as_ref() {
            tokio::select! {
                r = self.normal_poller.poll() => r,
                r = sq.poll() => r,
            }
        } else {
            self.normal_poller.poll().await
        }
    }

    fn notify_shutdown(&self) {
        self.normal_poller.notify_shutdown();
        if let Some(sq) = self.sticky_poller.as_ref() {
            sq.notify_shutdown();
        }
    }

    async fn shutdown(mut self) {
        self.normal_poller.shutdown().await;
        if let Some(sq) = self.sticky_poller {
            sq.shutdown().await;
        }
    }

    async fn shutdown_box(self: Box<Self>) {
        let this = *self;
        this.shutdown().await;
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
    use crate::test_help::test_worker_cfg;

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
}
