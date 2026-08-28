use crate::{
    MetricsContext, WorkerConfig,
    abstractions::{MeteredPermitDealer, OwnedMeteredSemPermit},
    pollers::{BoxedWFPoller, LongPollBuffer, Poller, WorkflowTaskOptions, WorkflowTaskPoller},
    protosext::ValidPollWFTQResponse,
    telemetry::metrics::{workflow_poller, workflow_sticky_poller},
    worker::{NamespaceCapabilities, WorkflowSlotKind, client::WorkerClient, wft_poller_behavior},
};
use crossbeam_utils::atomic::AtomicCell;
use futures_util::{Stream, stream};
use std::{
    sync::{Arc, OnceLock},
    time::SystemTime,
};
use temporalio_common::protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[allow(clippy::too_many_arguments)]
pub(crate) fn make_wft_poller(
    config: &WorkerConfig,
    sticky_queue_name: &Option<String>,
    client: &Arc<dyn WorkerClient>,
    metrics: &MetricsContext,
    shutdown_token: &CancellationToken,
    wft_slots: &MeteredPermitDealer<WorkflowSlotKind>,
    last_successful_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
    sticky_last_successful_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
    capabilities: Arc<NamespaceCapabilities>,
) -> impl Stream<
    Item = Result<
        (
            ValidPollWFTQResponse,
            OwnedMeteredSemPermit<WorkflowSlotKind>,
        ),
        tonic::Status,
    >,
> + Sized
+ 'static {
    let wft_metrics = metrics.with_new_attrs([workflow_poller()]);
    let effective_behavior = crate::worker::resolve_effective_behavior(
        config.workflow_task_poller_behavior,
        &capabilities,
    );
    let wft_poller_shared = if sticky_queue_name.is_some() {
        // Balance on the limit `acquire_owned` actually enforces (min of slot supplier and cache
        // size). Using only the slot supplier lets a small cache starve the non-sticky poller.
        let balance_limit = [wft_slots.available_permits(), wft_slots.max_permits()]
            .into_iter()
            .flatten()
            .min();
        Some(Arc::new(WFTPollerShared::new(balance_limit)))
    } else {
        None
    };
    let wf_task_poll_buffer = LongPollBuffer::new_workflow_task(
        client.clone(),
        config.task_queue.clone(),
        None,
        wft_poller_behavior(effective_behavior, config, false),
        wft_slots.clone(),
        shutdown_token.child_token(),
        Some(move |np| {
            wft_metrics.record_num_pollers(np);
        }),
        WorkflowTaskOptions {
            wft_poller_shared: wft_poller_shared.clone(),
        },
        last_successful_poll_time,
        capabilities.clone(),
    );
    let sticky_queue_poller = sticky_queue_name.as_ref().map(|sqn| {
        let sticky_metrics = metrics.with_new_attrs([workflow_sticky_poller()]);
        LongPollBuffer::new_workflow_task(
            client.clone(),
            config.task_queue.clone(),
            Some(sqn.clone()),
            wft_poller_behavior(effective_behavior, config, true),
            wft_slots.clone().into_sticky(),
            shutdown_token.child_token(),
            Some(move |np| {
                sticky_metrics.record_num_pollers(np);
            }),
            WorkflowTaskOptions { wft_poller_shared },
            sticky_last_successful_poll_time,
            capabilities,
        )
    });
    let wf_task_poll_buffer = Box::new(WorkflowTaskPoller::new(
        wf_task_poll_buffer,
        sticky_queue_poller,
    ));
    new_wft_poller(wf_task_poll_buffer, metrics.clone())
}

/// Info that needs to be shared across the sticky and non-sticky wft pollers to prioritize sticky
/// when appropriate
pub(crate) struct WFTPollerShared {
    last_seen_sticky_backlog: (watch::Receiver<usize>, watch::Sender<usize>),
    sticky_active: OnceLock<watch::Receiver<usize>>,
    non_sticky_active: OnceLock<watch::Receiver<usize>>,
    max_slots: Option<usize>,
}
impl WFTPollerShared {
    pub(crate) fn new(max_slots: Option<usize>) -> Self {
        let (tx, rx) = watch::channel(0);
        Self {
            last_seen_sticky_backlog: (rx, tx),
            sticky_active: OnceLock::new(),
            non_sticky_active: OnceLock::new(),
            max_slots,
        }
    }
    pub(crate) fn set_sticky_active(&self, rx: watch::Receiver<usize>) {
        let _ = self.sticky_active.set(rx);
    }
    pub(crate) fn set_non_sticky_active(&self, rx: watch::Receiver<usize>) {
        let _ = self.non_sticky_active.set(rx);
    }
    /// Makes either the sticky or non-sticky poller wait pre-permit-acquisition so that we can
    /// balance which kind of queue we poll appropriately.
    pub(crate) async fn wait_if_needed(&self, is_sticky: bool) {
        // We need to make sure there's at least one poller of both kinds available. So, we check
        // that we won't end up using every available permit with one kind of poller. In practice
        // this is only ever likely to be an issue with very small numbers of slots.
        if let Some(max_slots) = self.max_slots
            && let Some((sticky_active, non_sticky_active)) =
                self.sticky_active.get().zip(self.non_sticky_active.get())
        {
            let mut sticky_active = sticky_active.clone();
            let mut non_sticky_active = non_sticky_active.clone();
            let mut sticky_backlog = self.last_seen_sticky_backlog.0.clone();

            loop {
                let num_sticky_active = *sticky_active.borrow_and_update();
                let num_non_sticky_active = *non_sticky_active.borrow_and_update();
                let num_sticky_backlog = *sticky_backlog.borrow_and_update();

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
                        if num_sticky_backlog > 1 && num_sticky_backlog > num_sticky_active {
                            return false;
                        }
                    } else {
                        // There should always be at least one sticky poller.
                        if num_sticky_active == 0 {
                            return true;
                        }

                        // Do not allow an additional sticky poller to prevent starting a first non-sticky poller.
                        if num_non_sticky_active == 0 && num_sticky_active + 1 >= max_slots {
                            return false;
                        }

                        // If there's a meaningful sticky backlog, prioritize sticky.
                        if num_sticky_backlog > 1 && num_sticky_backlog > num_sticky_active {
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

fn new_wft_poller(
    poller: BoxedWFPoller,
    metrics: MetricsContext,
) -> impl Stream<
    Item = Result<
        (
            ValidPollWFTQResponse,
            OwnedMeteredSemPermit<WorkflowSlotKind>,
        ),
        tonic::Status,
    >,
> {
    stream::unfold((poller, metrics), |(poller, metrics)| async move {
        loop {
            return match poller.poll().await {
                Some(Ok((wft, permit))) => {
                    if wft == PollWorkflowTaskQueueResponse::default() {
                        // We get the default proto in the event that the long poll times out.
                        debug!("Poll wft timeout");
                        metrics.wf_tq_poll_empty();
                        continue;
                    }
                    if let Some(dur) = wft.sched_to_start() {
                        metrics.wf_task_sched_to_start_latency(dur);
                    }
                    let work = match validate_wft(wft) {
                        Ok(w) => w,
                        Err(e) => {
                            error!(error=?e, "Server returned an unparseable workflow task");
                            continue;
                        }
                    };
                    metrics.wf_tq_poll_ok();
                    Some((Ok((work, permit)), (poller, metrics)))
                }
                Some(Err(e)) => {
                    warn!(error=?e, "Error while polling for workflow tasks");
                    Some((Err(e), (poller, metrics)))
                }
                // If poller returns None, it's dead, thus we also return None to terminate
                // this stream.
                None => {
                    // Make sure we call the actual shutdown function here to propagate any
                    // panics inside the polling tasks as errors.
                    poller.shutdown_box().await;
                    None
                }
            };
        }
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn validate_wft(
    wft: PollWorkflowTaskQueueResponse,
) -> Result<ValidPollWFTQResponse, tonic::Status> {
    wft.try_into().map_err(|resp| {
        tonic::Status::new(
            tonic::Code::DataLoss,
            format!("Server returned a poll WFT response we couldn't interpret: {resp:?}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer,
        pollers::MockPermittedPollBuffer,
        replay::TestHistoryBuilder,
        test_help::{ResponseType, hist_to_poll_resp, mock_poller, test_worker_cfg},
        worker::{
            PollerBehavior, WorkflowSlotKind, client::mocks::mock_manual_worker_client,
            tuner::FixedSizeSlotSupplier,
        },
    };
    use futures_util::{FutureExt, StreamExt, pin_mut};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use temporalio_common::protos::temporal::api::enums::v1::EventType;

    #[tokio::test]
    async fn poll_timeouts_do_not_produce_responses() {
        let mut mock_poller = mock_poller();
        mock_poller
            .expect_poll()
            .times(1)
            .returning(|| Some(Ok(PollWorkflowTaskQueueResponse::default())));
        mock_poller.expect_poll().times(1).returning(|| None);
        mock_poller.expect_shutdown().times(1).returning(|| ());
        let sem = Arc::new(fixed_size_permit_dealer::<WorkflowSlotKind>(10));
        let stream = new_wft_poller(
            Box::new(MockPermittedPollBuffer::new(sem, mock_poller)),
            MetricsContext::no_op(),
        );
        pin_mut!(stream);
        assert_matches!(stream.next().await, None);
    }

    /// When the underlying poller returns None (indicating shutdown), the wrapping WFT stream
    /// should also return None to terminate.
    #[tokio::test]
    async fn poller_returning_none_terminates_wft_stream() {
        let mut mock_poller = mock_poller();
        mock_poller.expect_poll().times(1).returning(|| None);
        mock_poller.expect_shutdown().times(1).returning(|| ());

        let sem = Arc::new(fixed_size_permit_dealer::<WorkflowSlotKind>(10));

        let stream = new_wft_poller(
            Box::new(MockPermittedPollBuffer::new(sem, mock_poller)),
            MetricsContext::no_op(),
        );
        pin_mut!(stream);

        assert_matches!(stream.next().await, None);
    }

    /// Cache (`max_permits`) of 2 under a supplier of 10: the balancer must reserve a poll slot
    /// against the cache, not the supplier, or a saturated sticky poller starves the non-sticky one.
    /// Sticky long-polls forever (holding permits) while non-sticky repeatedly times out and must
    /// re-acquire; without the reservation sticky recaptures every freed permit, so non-sticky never
    /// delivers its eventual real task. (A single fresh-start poll won't reproduce this: a free
    /// permit is always available at startup.)
    #[tokio::test]
    async fn small_cache_does_not_starve_nonsticky_poller() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        let real_task = hist_to_poll_resp(&t, "wf-id", ResponseType::AllHistory).resp;

        // Non-sticky times out (empty response) this many times before its real task, giving the
        // sticky poller ample opportunity to grab every freed permit if nothing is reserved.
        let nonsticky_timeouts = Arc::new(AtomicUsize::new(0));
        let mut client = mock_manual_worker_client();
        client
            .expect_poll_workflow_task()
            .returning(move |_po, wfo| {
                if wfo.sticky_queue_name.is_some() {
                    // Sticky poll never returns -> holds its permit for the test's duration.
                    std::future::pending().boxed()
                } else if nonsticky_timeouts.fetch_add(1, Ordering::SeqCst) < 20 {
                    // Poll timeout: empty response releases the permit so it must be re-acquired.
                    async { Ok(PollWorkflowTaskQueueResponse::default()) }.boxed()
                } else {
                    let real_task = real_task.clone();
                    async move { Ok(real_task) }.boxed()
                }
            });

        let wft_slots = MeteredPermitDealer::<WorkflowSlotKind>::new(
            Arc::new(FixedSizeSlotSupplier::new(10)),
            MetricsContext::no_op(),
            Some(2),
            Arc::new(Default::default()),
            None,
        );
        // Poller max > 1 so the sticky poller alone could otherwise claim every permit.
        let cfg = {
            let mut cfg = test_worker_cfg().build().unwrap();
            cfg.workflow_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(5_usize));
            cfg
        };

        let client: Arc<dyn WorkerClient> = Arc::new(client);
        let stream = make_wft_poller(
            &cfg,
            &Some("sticky-q".to_string()),
            &client,
            &MetricsContext::no_op(),
            &CancellationToken::new(),
            &wft_slots,
            Arc::new(AtomicCell::new(None)),
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::default()),
        );
        pin_mut!(stream);
        let got = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await
            .expect("non-sticky poll must be delivered; a small cache is starving it")
            .expect("stream should yield a task");
        assert!(got.is_ok());
    }

    #[tokio::test]
    async fn poll_errors_do_produce_responses() {
        let mut mock_poller = mock_poller();
        mock_poller
            .expect_poll()
            .times(1)
            .returning(|| Some(Err(tonic::Status::internal("ahhh"))));
        let sem = Arc::new(fixed_size_permit_dealer::<WorkflowSlotKind>(10));
        let stream = new_wft_poller(
            Box::new(MockPermittedPollBuffer::new(sem, mock_poller)),
            MetricsContext::no_op(),
        );
        pin_mut!(stream);
        assert_matches!(stream.next().await, Some(Err(_)));
    }
}
