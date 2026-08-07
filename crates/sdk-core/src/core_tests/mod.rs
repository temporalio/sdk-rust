mod activity_tasks;
mod queries;
mod replay_flag;
mod updates;
mod workers;
mod workflow_cancels;
mod workflow_tasks;

use crate::{
    PollError, Worker,
    replay::{TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, build_mock_pollers, mock_worker, single_hist_mock_sg, test_worker_cfg,
    },
    worker::{
        PollerBehavior,
        client::mocks::{
            DEFAULT_WORKERS_REGISTRY, MockManualWorkerClient, mock_manual_worker_client,
            mock_worker_client,
        },
    },
};
use futures_util::FutureExt;
use std::{
    future,
    sync::{Arc, LazyLock},
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        enums::v1::EventType,
        history::v1::WorkflowExecutionOptionsUpdatedEventAttributes,
        namespace::v1::{NamespaceInfo, namespace_info::Capabilities},
        workflowservice::v1::{
            DescribeNamespaceResponse, PollActivityTaskQueueResponse,
            RecordActivityTaskHeartbeatResponse,
        },
    },
};
use temporalio_common::worker::WorkerTaskTypes;
use tokio::{
    sync::{Barrier, Notify},
    time::sleep,
};

#[tokio::test]
async fn after_shutdown_server_is_not_polled() {
    let t = canned_histories::single_timer("fake_timer");
    let mh = MockPollCfg::from_resp_batches("fake_wf_id", t, [1], mock_worker_client());
    let mut mock = build_mock_pollers(mh);
    // Just so we don't have to deal w/ cache overflow
    mock.worker_cfg(|cfg| cfg.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
        .await
        .unwrap();
    worker.shutdown().await;
    assert_matches!(
        worker.poll_workflow_activation().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.finalize_shutdown().await;
}

// Better than cloning a billion arcs...
static BARR: LazyLock<Barrier> = LazyLock::new(|| Barrier::new(3));

#[tokio::test]
async fn shutdown_interrupts_both_polls() {
    let mut mock_client = mock_manual_worker_client();
    mock_client
        .expect_poll_activity_task()
        .times(1)
        .returning(move |_, _| {
            async move {
                BARR.wait().await;
                sleep(Duration::from_secs(1)).await;
                Ok(Default::default())
            }
            .boxed()
        });
    mock_client
        .expect_poll_workflow_task()
        .times(1)
        .returning(move |_, _| {
            async move {
                BARR.wait().await;
                sleep(Duration::from_secs(1)).await;
                Ok(Default::default())
            }
            .boxed()
        });

    let worker = Worker::new_test(
        {
            let mut cfg = test_worker_cfg()
                // Need only 1 concurrent pollers for mock expectations to work here
                .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
                .build()
                .unwrap();
            cfg.workflow_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(1_usize));
            cfg
        },
        mock_client,
    );
    tokio::join! {
        async {
            assert_matches!(worker.poll_activity_task().await.unwrap_err(),
                            PollError::ShutDown);
        },
        async {
            assert_matches!(worker.poll_workflow_activation().await.unwrap_err(),
                            PollError::ShutDown);
        },
        async {
            // Give polling a bit to get stuck, then shutdown
            BARR.wait().await;
            worker.shutdown().await;
        }
    };
}

#[tokio::test]
async fn graceful_activity_poll_shutdown_handles_unimplemented_shutdown_worker() {
    let activity_poll_started = Arc::new(Notify::new());
    let activity_poll_started_clone = activity_poll_started.clone();
    let shutdown_worker_called = Arc::new(Notify::new());
    let shutdown_worker_called_clone = shutdown_worker_called.clone();

    let mut mock_client = MockManualWorkerClient::new();
    mock_client.expect_capabilities().returning(|| None);
    mock_client
        .expect_workers()
        .returning(|| DEFAULT_WORKERS_REGISTRY.clone());
    mock_client.expect_is_mock().returning(|| true);
    mock_client
        .expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    mock_client
        .expect_identity()
        .returning(|| "test-identity".to_string());
    mock_client
        .expect_worker_grouping_key()
        .returning(uuid::Uuid::new_v4);
    mock_client
        .expect_worker_instance_key()
        .returning(uuid::Uuid::new_v4);
    mock_client
        .expect_describe_namespace()
        .times(1)
        .returning(|| {
            async {
                Ok(DescribeNamespaceResponse {
                    namespace_info: Some(NamespaceInfo {
                        capabilities: Some(Capabilities {
                            worker_poll_complete_on_shutdown: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
            }
            .boxed()
        });
    mock_client
        .expect_shutdown_worker()
        .times(1)
        .returning(move |_, _, _, _| {
            let shutdown_worker_called = shutdown_worker_called_clone.clone();
            async move {
                shutdown_worker_called.notify_one();
                Err(tonic::Status::unimplemented(
                    "ShutdownWorker disabled by server",
                ))
            }
            .boxed()
        });
    mock_client
        .expect_poll_activity_task()
        .times(1)
        .returning(move |_, _| {
            let activity_poll_started = activity_poll_started_clone.clone();
            async move {
                activity_poll_started.notify_one();
                future::pending::<Result<PollActivityTaskQueueResponse, tonic::Status>>().await
            }
            .boxed()
        });
    mock_client
        .expect_record_activity_heartbeat()
        .returning(|_, _| async { Ok(RecordActivityTaskHeartbeatResponse::default()) }.boxed());

    let mut cfg = test_worker_cfg()
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
        .build()
        .unwrap();
    cfg.task_types = WorkerTaskTypes::activity_only();
    let worker = Worker::new_test(cfg, mock_client);
    worker.validate().await.unwrap();

    let poll_fut = async { worker.poll_activity_task().await };
    let shutdown_fut = async {
        activity_poll_started.notified().await;
        worker.initiate_shutdown();
        shutdown_worker_called.notified().await;
    };

    let (poll_result, _) = tokio::time::timeout(Duration::from_millis(500), async {
        tokio::join!(poll_fut, shutdown_fut)
    })
    .await
    .expect("activity poll remained pending after shutdown_worker returned UNIMPLEMENTED");

    assert_matches!(poll_result.unwrap_err(), PollError::ShutDown);
}

#[tokio::test]
async fn ignores_workflow_options_updated_event() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add(WorkflowExecutionOptionsUpdatedEventAttributes::default());
    t.last_event().unwrap().worker_may_ignore = true;
    t.add_full_wf_task();

    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg("whatever", t, [1], mock, true);
    mock.worker_cfg(|w| w.max_cached_workflows = 1);
    let core = mock_worker(mock);
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
}
