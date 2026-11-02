use crate::{
    Worker,
    replay::TestHistoryBuilder,
    test_help::{
        ResponseType, WorkerExt, WorkerTestHelpers, hist_to_poll_resp, schedule_local_activity_cmd,
        test_worker_cfg,
    },
    worker::{
        TaskPollers, WorkerHeartbeatOptions,
        client::{
            WorkflowTaskCompletion,
            mocks::{DEFAULT_TEST_CAPABILITIES, MockManualWorkerClient},
        },
    },
};
use futures_util::FutureExt;
use prost::Message;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::worker::{ClientWorkerSet, WorkflowHeartbeatNotifications};
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        workflow_activation::{WorkflowActivation, workflow_activation_job::Variant},
        workflow_commands::{ActivityCancellationType, workflow_command},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::Payload,
        namespace::v1::{NamespaceInfo, namespace_info::Capabilities},
        nexus::v1::{Request, StartOperationRequest, request, response, start_operation_response},
        nexusservices::workerservice::v1::{ExecuteCommandsRequest, ExecuteCommandsResponse},
        worker::v1::{
            WorkerCommand, WorkflowTaskHeartbeatNotificationCommand, worker_command,
            worker_command_result, workflow_task_heartbeat_notification_result::Outcome,
        },
        workflowservice::v1::{
            DescribeNamespaceResponse, PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
            RespondWorkflowTaskCompletedResponse,
        },
    },
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

struct Fixture {
    worker: Worker,
    workers: Arc<Mutex<Arc<ClientWorkerSet>>>,
    namespace: String,
    commands: UnboundedSender<PollNexusTaskQueueResponse>,
    acknowledgments: UnboundedReceiver<ExecuteCommandsResponse>,
    completions: UnboundedReceiver<WorkflowTaskCompletion>,
    polls: UnboundedSender<PollWorkflowTaskQueueResponse>,
    replacement: Arc<Mutex<Option<PollWorkflowTaskQueueResponse>>>,
    fail_completion: Arc<AtomicBool>,
}

impl Fixture {
    fn new(first: PollWorkflowTaskQueueResponse, cache_size: usize, eager: bool) -> Self {
        let mut client = MockManualWorkerClient::new();
        let workers = Arc::new(Mutex::new(Arc::new(ClientWorkerSet::new())));
        let client_workers = workers.clone();
        client
            .expect_workers()
            .returning(move || client_workers.lock().unwrap().clone());
        client
            .expect_capabilities()
            .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
        client.expect_is_mock().return_const(false);
        client
            .expect_shutdown_worker()
            .returning(|_, _, _, _| async { Ok(Default::default()) }.boxed());
        client
            .expect_sdk_name_and_version()
            .returning(|| ("test-core".to_owned(), "0.0.0".to_owned()));
        client
            .expect_identity()
            .returning(|| "whn-worker".to_owned());
        client
            .expect_worker_grouping_key()
            .returning(uuid::Uuid::new_v4);
        client
            .expect_worker_instance_key()
            .returning(uuid::Uuid::new_v4);
        client
            .expect_set_heartbeat_client_fields()
            .returning(|_| ());
        let (commands, command_rx) = unbounded_channel();
        let command_rx = Arc::new(tokio::sync::Mutex::new(command_rx));
        client
            .expect_poll_nexus_task()
            .returning(move |_, options| {
                assert!(options.worker_commands_queue);
                let rx = command_rx.clone();
                async move { Ok(rx.lock().await.recv().await.unwrap_or_default()) }.boxed()
            });
        client.expect_describe_namespace().returning(|| {
            async {
                Ok(DescribeNamespaceResponse {
                    namespace_info: Some(NamespaceInfo {
                        capabilities: Some(Capabilities {
                            worker_heartbeats: true,
                            worker_commands: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
            }
            .boxed()
        });
        client
            .expect_record_worker_heartbeat()
            .returning(|_, _| async { Ok(Default::default()) }.boxed());
        let (ack_tx, acknowledgments) = unbounded_channel();
        client
            .expect_complete_nexus_task()
            .returning(move |_, response| {
                let Some(response::Variant::StartOperation(start)) = response.variant else {
                    panic!("expected command response");
                };
                let Some(start_operation_response::Variant::SyncSuccess(sync)) = start.variant
                else {
                    panic!("expected sync response");
                };
                let data = sync.payload.unwrap().data;
                ack_tx
                    .send(ExecuteCommandsResponse::decode(data.as_slice()).unwrap())
                    .unwrap();
                async { Ok(Default::default()) }.boxed()
            });
        let (complete_tx, completions) = unbounded_channel();
        let replacement = Arc::new(Mutex::new(None));
        let next = replacement.clone();
        let fail_completion = Arc::new(AtomicBool::new(false));
        let fail = fail_completion.clone();
        let routing = workers.clone();
        client
            .expect_fail_workflow_task()
            .returning(|_, _, _| async { Ok(Default::default()) }.boxed());
        client
            .expect_respond_legacy_query()
            .returning(|_, _| async { Ok(Default::default()) }.boxed());
        client
            .expect_complete_workflow_task()
            .returning(move |request, _| {
                assert_eq!(
                    routing
                        .lock()
                        .unwrap()
                        .workflow_heartbeat_notifications(crate::test_help::NAMESPACE)
                        .unwrap()
                        .notify(&request.task_token),
                    Outcome::NoWorkflowTask
                );
                complete_tx.send(request).unwrap();
                let fail = fail.swap(false, Ordering::Relaxed);
                let workflow_task = next.lock().unwrap().take();
                async move {
                    if fail {
                        return Err(tonic::Status::not_found("expired WFT"));
                    }
                    Ok(RespondWorkflowTaskCompletedResponse {
                        workflow_task,
                        ..Default::default()
                    })
                }
                .boxed()
            });
        let (polls, poll_rx) = unbounded_channel();
        let poll_rx = Arc::new(tokio::sync::Mutex::new(poll_rx));
        client.expect_poll_workflow_task().returning(move |_, _| {
            let rx = poll_rx.clone();
            async move { Ok(rx.lock().await.recv().await.unwrap_or_default()) }.boxed()
        });
        if !eager {
            polls.send(first.clone()).unwrap();
        }
        let mut config = test_worker_cfg().build().unwrap();
        config.max_cached_workflows = cache_size;
        config.task_types = temporalio_common::worker::WorkerTaskTypes {
            enable_workflows: true,
            enable_local_activities: true,
            enable_remote_activities: false,
            enable_nexus: false,
        };
        let namespace = config.namespace.clone();
        let worker = Worker::new_with_pollers(
            config,
            crate::sticky_q_name_for_worker("whn", cache_size),
            Arc::new(client),
            TaskPollers::Real,
            None,
            Some(WorkerHeartbeatOptions {
                interval: Duration::from_secs(60),
                environment_info: None,
            }),
            false,
        )
        .unwrap();
        worker.mark_started();
        if eager {
            worker
                .client_worker_registrator
                .slot_provider
                .try_reserve_wft_slot()
                .unwrap()
                .schedule_wft(first)
                .unwrap();
        }
        Self {
            worker,
            workers,
            namespace,
            commands,
            acknowledgments,
            completions,
            polls,
            replacement,
            fail_completion,
        }
    }

    fn router(&self) -> Arc<WorkflowHeartbeatNotifications> {
        self.workers
            .lock()
            .unwrap()
            .workflow_heartbeat_notifications(&self.namespace)
            .unwrap()
    }

    async fn notify(&mut self, tokens: &[Vec<u8>]) -> Vec<Outcome> {
        self.commands
            .send(PollNexusTaskQueueResponse {
                task_token: vec![99],
                request: Some(Request {
                    variant: Some(request::Variant::StartOperation(StartOperationRequest {
                        payload: Some(Payload {
                            data: ExecuteCommandsRequest {
                                commands: tokens
                                    .iter()
                                    .map(|token| WorkerCommand {
                                        r#type: Some(
                                            worker_command::Type::WorkflowTaskHeartbeatNotification(
                                                WorkflowTaskHeartbeatNotificationCommand {
                                                    task_token: token.clone(),
                                                },
                                            ),
                                        ),
                                    })
                                    .collect(),
                            }
                            .encode_to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(5), self.acknowledgments.recv())
            .await
            .unwrap()
            .unwrap();
        response
            .results
            .into_iter()
            .map(|result| match result.r#type.unwrap() {
                worker_command_result::Type::WorkflowTaskHeartbeatNotification(result) => {
                    Outcome::try_from(result.outcome).unwrap()
                }
                other => panic!("unexpected result: {other:?}"),
            })
            .collect()
    }

    async fn complete(
        &self,
        activation: &WorkflowActivation,
        commands: Vec<workflow_command::Variant>,
    ) {
        self.worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                activation.run_id.clone(),
                commands,
            ))
            .await
            .unwrap();
    }
}

fn history(timeout: Duration) -> (TestHistoryBuilder, PollWorkflowTaskQueueResponse) {
    let mut history = TestHistoryBuilder::default();
    history.add_wfe_started_with_wft_timeout(timeout);
    history.add_workflow_task_scheduled_and_started();
    let mut first = hist_to_poll_resp(&history, "whn", ResponseType::AllHistory).resp;
    first.task_token = vec![1];
    (history, first)
}

fn local_activity() -> workflow_command::Variant {
    schedule_local_activity_cmd(
        1,
        "1",
        ActivityCancellationType::TryCancel,
        Duration::from_secs(120),
    )
}

#[tokio::test]
async fn notification_routes_to_replacement_clients_worker_set() {
    let (mut history, first) = history(Duration::from_secs(60));
    let mut fixture = Fixture::new(first, 1, false);
    let activation = fixture.worker.poll_workflow_activation().await.unwrap();
    fixture.complete(&activation, vec![]).await;
    fixture.completions.recv().await.unwrap();

    // Mirror the registry transfer performed by Worker::replace_client.
    let old_workers = fixture.workers.lock().unwrap().clone();
    let key = fixture.worker.worker_instance_key;
    old_workers.unregister_slot_provider(key).unwrap();
    let worker = old_workers.finalize_unregister(key).unwrap();
    let new_workers = Arc::new(ClientWorkerSet::new());
    *fixture.workers.lock().unwrap() = new_workers.clone();
    new_workers.register_worker(worker, false).unwrap();

    history.add_workflow_task_completed();
    history.add_we_signaled("interrupt", vec![]);
    history.add_workflow_task_scheduled_and_started();
    let mut next = hist_to_poll_resp(&history, "whn", ResponseType::ToTaskNum(2)).resp;
    next.task_token = vec![2];
    fixture.polls.send(next).unwrap();
    let activation = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.worker.poll_workflow_activation(),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.complete(&activation, vec![local_activity()]).await;
    let activity = fixture.worker.poll_activity_task().await.unwrap();
    assert_eq!(fixture.notify(&[vec![2]]).await, vec![Outcome::Handled]);
    let request = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            request = fixture.completions.recv() => request.unwrap(),
            activation = fixture.worker.poll_workflow_activation() => panic!("unexpected activation: {activation:?}"),
        }
    })
    .await
    .unwrap();
    assert_eq!(request.task_token, vec![2].into());
    assert!(request.force_create_new_workflow_task);
    fixture
        .worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: activity.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    fixture.worker.drain_pollers_and_shutdown().await;
}

#[rstest::rstest]
#[case::polled(false)]
#[case::eager(true)]
#[tokio::test]
async fn command_heartbeats_and_delivers_signal_while_local_activity_is_running(
    #[case] eager: bool,
) {
    let (mut history, first) = history(Duration::from_secs(60));
    let mut fixture = Fixture::new(first, 1, eager);
    let activation = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.worker.poll_workflow_activation(),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.complete(&activation, vec![local_activity()]).await;
    let activity = fixture.worker.poll_activity_task().await.unwrap();

    history.add_workflow_task_completed();
    history.add_we_signaled("interrupt", vec![]);
    history.add_workflow_task_scheduled_and_started();
    let mut next = hist_to_poll_resp(&history, "whn", ResponseType::ToTaskNum(2)).resp;
    next.task_token = vec![2];
    *fixture.replacement.lock().unwrap() = Some(next);
    assert_eq!(
        fixture.notify(&[vec![1], vec![1], vec![8]]).await,
        vec![
            Outcome::Handled,
            Outcome::AlreadyReceived,
            Outcome::TaskTokenMismatch
        ]
    );
    let (request, signal) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            fixture.completions.recv(),
            fixture.worker.poll_workflow_activation()
        )
    })
    .await
    .unwrap();
    let request = request.unwrap();
    let signal = signal.unwrap();
    assert_eq!(request.task_token, vec![1].into());
    assert!(request.force_create_new_workflow_task);
    assert!(request.return_new_workflow_task);
    assert!(signal.jobs.iter().any(|job| matches!(&job.variant, Some(Variant::SignalWorkflow(signal)) if signal.signal_name == "interrupt")));
    assert_eq!(
        fixture.router().notify(&vec![1].into()),
        Outcome::TaskTokenMismatch
    );

    // Acceptance during a signal activation must not report its WFT until that activation completes.
    assert_eq!(
        fixture.notify(&[vec![2], vec![2]]).await,
        vec![Outcome::Handled, Outcome::AlreadyReceived]
    );
    assert!(fixture.completions.try_recv().is_err());
    fixture.complete(&signal, vec![]).await;
    let request = fixture.completions.recv().await.unwrap();
    assert_eq!(request.task_token, vec![2].into());
    assert!(request.force_create_new_workflow_task);
    assert_eq!(
        fixture.router().notify(&vec![2].into()),
        Outcome::NoWorkflowTask
    );

    history.add_workflow_task_completed();
    history.add_workflow_task_scheduled_and_started();
    let mut third = hist_to_poll_resp(&history, "whn", ResponseType::ToTaskNum(3)).resp;
    third.task_token = vec![3];
    fixture.polls.send(third).unwrap();
    fixture
        .worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: activity.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    let resolved = fixture.worker.poll_workflow_activation().await.unwrap();
    fixture.worker.complete_execution(&resolved.run_id).await;
    let request = fixture.completions.recv().await.unwrap();
    assert!(!request.force_create_new_workflow_task);
    assert_eq!(
        fixture.router().notify(&vec![3].into()),
        Outcome::NoWorkflowTask
    );
    fixture.worker.drain_pollers_and_shutdown().await;
}

#[rstest::rstest]
#[case::no_local_activity(false)]
#[case::local_activity_scheduled_after_notification(true)]
#[tokio::test]
async fn notification_waits_for_activation_and_rechecks_local_activity_work(
    #[case] schedule_la: bool,
    #[values(0, 1)] cache_size: usize,
) {
    let (_, first) = history(Duration::from_secs(60));
    let mut fixture = Fixture::new(first, cache_size, false);
    let activation = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.worker.poll_workflow_activation(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Handled]);
    assert!(fixture.completions.try_recv().is_err());
    fixture
        .complete(
            &activation,
            if schedule_la {
                vec![local_activity()]
            } else {
                vec![workflow_command::Variant::CompleteWorkflowExecution(
                    Default::default(),
                )]
            },
        )
        .await;
    let request = tokio::time::timeout(Duration::from_secs(5), fixture.completions.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.force_create_new_workflow_task, schedule_la);
    assert_eq!(
        fixture.router().notify(&vec![1].into()),
        Outcome::NoWorkflowTask
    );
    if schedule_la {
        let activity = fixture.worker.poll_activity_task().await.unwrap();
        fixture
            .worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: activity.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
    }
    fixture.worker.drain_pollers_and_shutdown().await;
}

#[rstest::rstest]
#[case::timer_only(false)]
#[case::after_notification(true)]
#[tokio::test]
async fn timed_fallback_gets_a_fresh_deadline_after_replacement(#[case] notify_first: bool) {
    let (mut history, first) = history(Duration::from_millis(200));
    let mut fixture = Fixture::new(first, 1, false);
    let activation = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.worker.poll_workflow_activation(),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.complete(&activation, vec![local_activity()]).await;
    history.add_workflow_task_completed();
    history.add_workflow_task_scheduled_and_started();
    let mut next = hist_to_poll_resp(&history, "whn", ResponseType::ToTaskNum(2)).resp;
    next.task_token = vec![2];
    *fixture.replacement.lock().unwrap() = Some(next);
    if notify_first {
        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Handled]);
    }
    let mut first_completed = None;
    for token in [vec![1], vec![2]] {
        let request = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                request = fixture.completions.recv() => request.unwrap(),
                activation = fixture.worker.poll_workflow_activation() => panic!("unexpected activation: {activation:?}"),
            }
        }).await.unwrap();
        assert!(request.force_create_new_workflow_task);
        assert_eq!(request.task_token, token.into());
        if let Some(first_completed) = first_completed {
            assert!(
                std::time::Instant::now().duration_since(first_completed)
                    >= Duration::from_millis(100)
            );
        } else {
            first_completed = Some(std::time::Instant::now());
        }
    }
    assert_eq!(
        fixture.router().notify(&vec![2].into()),
        Outcome::NoWorkflowTask
    );
    let activity = fixture.worker.poll_activity_task().await.unwrap();
    fixture
        .worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: activity.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    fixture.worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn heartbeat_rpc_failure_retires_token_and_evicts_without_losing_activity_cleanup() {
    let (_, first) = history(Duration::from_secs(60));
    let mut fixture = Fixture::new(first, 1, false);
    let activation = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.worker.poll_workflow_activation(),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.complete(&activation, vec![local_activity()]).await;
    let activity = fixture.worker.poll_activity_task().await.unwrap();
    fixture.fail_completion.store(true, Ordering::Relaxed);
    assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Handled]);
    let (eviction, cancellation) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            fixture.worker.poll_workflow_activation(),
            fixture.worker.poll_activity_task()
        )
    })
    .await
    .unwrap();
    let eviction = eviction.unwrap();
    assert!(eviction.is_only_eviction());
    assert_eq!(cancellation.unwrap().task_token, activity.task_token);
    assert_eq!(
        fixture.router().notify(&vec![1].into()),
        Outcome::NoWorkflowTask
    );
    fixture.complete(&eviction, vec![]).await;
    fixture
        .worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: activity.task_token,
            result: Some(ActivityExecutionResult::cancel_from_details(None)),
        })
        .await
        .unwrap();
    fixture.worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn activation_failure_retires_accepted_notification() {
    let (_, first) = history(Duration::from_secs(60));
    let mut fixture = Fixture::new(first, 1, false);
    let activation = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.worker.poll_workflow_activation(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Handled]);
    fixture
        .worker
        .complete_workflow_activation(WorkflowActivationCompletion::fail(
            activation.run_id,
            temporalio_common::protos::temporal::api::failure::v1::Failure::application_failure(
                "workflow panic".to_owned(),
                false,
            ),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(
        fixture.router().notify(&vec![1].into()),
        Outcome::NoWorkflowTask
    );
    assert!(fixture.completions.try_recv().is_err());
    fixture.worker.drain_pollers_and_shutdown().await;
}

#[rstest::rstest]
#[tokio::test]
async fn query_tasks_register_only_normal_workflow_task_tokens(
    #[values(false, true)] standalone: bool,
) {
    let (_, mut first) = history(Duration::from_secs(60));
    let query = temporalio_common::protos::temporal::api::query::v1::WorkflowQuery {
        query_type: "state".to_owned(),
        ..Default::default()
    };
    if standalone {
        first.query = Some(query);
        first.started_event_id = 0;
    } else {
        first.queries.insert("query".to_owned(), query);
    }
    let mut fixture = Fixture::new(first, 1, false);
    let mut activation = fixture.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        fixture.notify(&[vec![1]]).await,
        vec![if standalone {
            Outcome::NoWorkflowTask
        } else {
            Outcome::Handled
        }]
    );
    if !activation
        .jobs
        .iter()
        .any(|job| matches!(job.variant, Some(Variant::QueryWorkflow(_))))
    {
        fixture.complete(&activation, vec![]).await;
        activation = fixture.worker.poll_workflow_activation().await.unwrap();
    }
    let id = activation
        .jobs
        .iter()
        .find_map(|job| match &job.variant {
            Some(Variant::QueryWorkflow(query)) => Some(query.query_id.clone()),
            _ => None,
        })
        .unwrap();
    fixture
        .complete(&activation, vec![crate::test_help::query_ok(id, "state")])
        .await;
    if !standalone {
        let request = fixture.completions.recv().await.unwrap();
        assert!(!request.force_create_new_workflow_task);
        assert_eq!(request.query_responses.len(), 1);
    }
    assert_eq!(
        fixture.router().notify(&vec![1].into()),
        Outcome::NoWorkflowTask
    );
    fixture.worker.drain_pollers_and_shutdown().await;
}
