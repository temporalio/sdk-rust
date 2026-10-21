use crate::{
    MetricsContext, WorkerConfig,
    abstractions::MeteredPermitDealer,
    pollers::{
        ActivityTaskOptions, LongPollBuffer, PollRateLimiter, StickyPollerShared, StickyTaskPoller,
        sticky_poller_behavior,
    },
    telemetry::metrics::{activity_poller, activity_sticky_poller},
    worker::{ActivitySlotKind, NamespaceCapabilities, client::WorkerClient},
};
use crossbeam_utils::atomic::AtomicCell;
use std::{sync::Arc, time::SystemTime};
use temporalio_common::protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;
use tokio_util::sync::CancellationToken;

#[allow(clippy::too_many_arguments)]
pub(crate) fn make_activity_poller(
    config: &WorkerConfig,
    sticky_queue: Option<&str>,
    client: &Arc<dyn WorkerClient>,
    metrics: &MetricsContext,
    shutdown_token: &CancellationToken,
    act_slots: &MeteredPermitDealer<ActivitySlotKind>,
    last_successful_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
    capabilities: Arc<NamespaceCapabilities>,
) -> StickyTaskPoller<PollActivityTaskQueueResponse, ActivitySlotKind> {
    let effective_behavior = crate::worker::resolve_effective_behavior(
        config.activity_task_poller_behavior,
        &capabilities,
    );
    let poller_shared =
        sticky_queue.map(|_| Arc::new(StickyPollerShared::new(act_slots.available_permits())));
    let rate_limiter = config
        .max_worker_activities_per_second
        .map(|rate| Arc::new(PollRateLimiter::new(rate)));
    let act_metrics = metrics.with_new_attrs([activity_poller()]);
    let normal_poller = LongPollBuffer::new_activity_task(
        client.clone(),
        config.task_queue.clone(),
        None,
        if sticky_queue.is_some() {
            sticky_poller_behavior(effective_behavior, config, false)
        } else {
            effective_behavior
        },
        act_slots.clone(),
        shutdown_token.child_token(),
        Some(move |np| act_metrics.record_num_pollers(np)),
        ActivityTaskOptions {
            rate_limiter: rate_limiter.clone(),
            max_tps: config.max_task_queue_activities_per_second,
            poller_shared: poller_shared.clone(),
        },
        last_successful_poll_time.clone(),
        capabilities.clone(),
    );
    let sticky_poller = sticky_queue.map(|sqn| {
        let sticky_metrics = metrics.with_new_attrs([activity_sticky_poller()]);
        LongPollBuffer::new_activity_task(
            client.clone(),
            config.task_queue.clone(),
            Some(sqn.to_owned()),
            sticky_poller_behavior(effective_behavior, config, true),
            act_slots.clone().into_sticky(),
            shutdown_token.child_token(),
            Some(move |np| sticky_metrics.record_num_pollers(np)),
            ActivityTaskOptions {
                rate_limiter,
                max_tps: config.max_task_queue_activities_per_second,
                poller_shared,
            },
            last_successful_poll_time,
            capabilities,
        )
    });
    StickyTaskPoller::new(normal_poller, sticky_poller)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer,
        pollers::Poller,
        test_help::test_worker_cfg,
        worker::{PollerBehavior, client::mocks::mock_manual_worker_client},
    };
    use futures_util::FutureExt;
    use std::{future::pending, time::Duration};
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test(start_paused = true)]
    async fn session_activity_pollers_share_capacity() {
        let (tx, mut rx) = unbounded_channel();
        let mut client = mock_manual_worker_client();
        client
            .expect_poll_activity_task()
            .times(2)
            .returning(move |poll, options| {
                tx.send((
                    poll.task_queue,
                    options.sticky_queue_name,
                    tokio::time::Instant::now(),
                ))
                .unwrap();
                pending().boxed()
            });
        let client: Arc<dyn WorkerClient> = Arc::new(client);
        let config = WorkerConfig::builder()
            .namespace("default")
            .task_queue("normal")
            .versioning_strategy(crate::WorkerVersioningStrategy::None {
                build_id: String::new(),
            })
            .task_types(temporalio_common::worker::WorkerTaskTypes::all())
            .max_cached_workflows(2_usize)
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(10))
            .max_worker_activities_per_second(2.0)
            .session_worker_options(crate::SessionWorkerOptions::default())
            .build()
            .unwrap();
        let slots = fixed_size_permit_dealer(2);
        let poller = Arc::new(make_activity_poller(
            &config,
            Some("sticky"),
            &client,
            &MetricsContext::no_op(),
            &CancellationToken::new(),
            &slots,
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::default()),
        ));
        let polling = tokio::spawn({
            let poller = poller.clone();
            async move { poller.poll().await }
        });
        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.0, config.task_queue);
        assert_eq!(second.0, config.task_queue);
        assert_ne!(first.1, second.1);
        assert!(second.2.duration_since(first.2) >= Duration::from_millis(500));
        assert!(first.1.as_deref() == Some("sticky") || second.1.as_deref() == Some("sticky"));
        assert_eq!(slots.available_permits(), Some(0));
        poller.notify_shutdown();
        assert!(polling.await.unwrap().is_none());
        match Arc::try_unwrap(poller) {
            Ok(poller) => poller.shutdown().await,
            Err(_) => panic!("poller still referenced"),
        }
        assert_eq!(slots.available_permits(), Some(2));
    }

    #[tokio::test]
    async fn ordinary_activity_polling_keeps_full_poller_budget() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (tx, mut rx) = unbounded_channel();
            let mut client = mock_manual_worker_client();
            client
                .expect_poll_activity_task()
                .times(5)
                .returning(move |_, options| {
                    assert!(options.sticky_queue_name.is_none());
                    tx.send(()).unwrap();
                    pending().boxed()
                });
            let client: Arc<dyn WorkerClient> = Arc::new(client);
            let config = test_worker_cfg()
                .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(5))
                .build()
                .unwrap();
            let poller = Arc::new(make_activity_poller(
                &config,
                None,
                &client,
                &MetricsContext::no_op(),
                &CancellationToken::new(),
                &fixed_size_permit_dealer(10),
                Arc::new(AtomicCell::new(None)),
                Arc::new(NamespaceCapabilities::default()),
            ));
            let polling = tokio::spawn({
                let poller = poller.clone();
                async move { poller.poll().await }
            });
            for _ in 0..5 {
                rx.recv().await.unwrap();
            }
            poller.notify_shutdown();
            assert!(polling.await.unwrap().is_none());
            match Arc::try_unwrap(poller) {
                Ok(poller) => poller.shutdown().await,
                Err(_) => panic!("poller still referenced"),
            }
        })
        .await
        .expect("ordinary workers should start the full configured poller budget");
    }
}
