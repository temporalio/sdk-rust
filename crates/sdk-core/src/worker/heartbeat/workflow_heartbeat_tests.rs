use crate::{
    PollError, Worker, init_replay_worker,
    internal_flags::CoreInternalFlags,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        ResponseType, WorkerExt, WorkerTestHelpers, hist_to_poll_resp, schedule_activity_cmd,
        schedule_local_activity_cmd, test_worker_cfg,
    },
    worker::{
        TaskPollers, WorkerHeartbeatOptions,
        client::{
            WorkflowTaskCompletion,
            mocks::{DEFAULT_TEST_CAPABILITIES, MockManualWorkerClient},
        },
    },
};
use futures_util::{FutureExt, stream};
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
        activity_result::{ActivityExecutionResult, activity_resolution},
        activity_task::activity_task,
        workflow_activation::{
            WorkflowActivation, remove_from_cache::EvictionReason, workflow_activation_job::Variant,
        },
        workflow_commands::{ActivityCancellationType, RequestCancelActivity, workflow_command},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command,
        common::v1::{Payload, WorkflowExecution},
        history::v1::{History, MarkerRecordedEventAttributes, history_event},
        namespace::v1::{NamespaceInfo, namespace_info::Capabilities},
        nexus::v1::{Request, StartOperationRequest, request, response, start_operation_response},
        nexusservices::workerservice::v1::{ExecuteCommandsRequest, ExecuteCommandsResponse},
        worker::v1::{
            WorkerCommand, WorkflowHeartbeatNotificationCommand, worker_command,
            worker_command_result, workflow_heartbeat_notification_result::Outcome,
        },
        workflowservice::v1::{
            DescribeNamespaceResponse, GetWorkflowExecutionHistoryResponse,
            PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
            RespondWorkflowTaskCompletedResponse,
        },
    },
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot,
};

struct Fixture {
    worker: Worker,
    workers: Arc<Mutex<Arc<ClientWorkerSet>>>,
    namespace: String,
    execution: WorkflowExecution,
    commands: UnboundedSender<PollNexusTaskQueueResponse>,
    acknowledgments: UnboundedReceiver<ExecuteCommandsResponse>,
    completions: UnboundedReceiver<WorkflowTaskCompletion>,
    polls: UnboundedSender<PollWorkflowTaskQueueResponse>,
    replacement: Arc<Mutex<Option<PollWorkflowTaskQueueResponse>>>,
    fail_completion: Arc<AtomicBool>,
    history_fetches: UnboundedReceiver<(
        Vec<u8>,
        oneshot::Sender<GetWorkflowExecutionHistoryResponse>,
    )>,
}

impl Fixture {
    fn new(first: PollWorkflowTaskQueueResponse, cache_size: usize, eager: bool) -> Self {
        let execution = first.workflow_execution.clone().unwrap();
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
        let (history_tx, history_fetches) = unbounded_channel();
        client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, token| {
                let (tx, rx) = oneshot::channel();
                history_tx.send((token, tx)).unwrap();
                async move { Ok(rx.await.unwrap()) }.boxed()
            });
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
        let routing_execution = execution.clone();
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
                        .notify(&WorkflowHeartbeatNotificationCommand {
                            task_token: request.task_token.clone().into_inner(),
                            execution: Some(routing_execution.clone()),
                        })
                        .outcome,
                    Outcome::TaskTokenMismatch as i32
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
            execution,
            commands,
            acknowledgments,
            completions,
            polls,
            replacement,
            fail_completion,
            history_fetches,
        }
    }

    fn router(&self) -> Arc<WorkflowHeartbeatNotifications> {
        self.workers
            .lock()
            .unwrap()
            .workflow_heartbeat_notifications(&self.namespace)
            .unwrap()
    }

    fn notification_outcome(&self, token: Vec<u8>) -> Outcome {
        self.router()
            .notify(&WorkflowHeartbeatNotificationCommand {
                task_token: token,
                execution: Some(self.execution.clone()),
            })
            .outcome()
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
                                            worker_command::Type::WorkflowHeartbeatNotification(
                                                WorkflowHeartbeatNotificationCommand {
                                                    task_token: token.clone(),
                                                    execution: Some(self.execution.clone()),
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
                worker_command_result::Type::WorkflowHeartbeatNotification(result) => {
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

fn heartbeat_workflow_step(
    activation: &WorkflowActivation,
) -> (Vec<String>, Vec<workflow_command::Variant>) {
    let mut jobs = vec![];
    let mut commands = vec![];
    for job in &activation.jobs {
        match job.variant.as_ref().unwrap() {
            Variant::InitializeWorkflow(_) => {
                jobs.push("init".to_owned());
                commands.extend((1..=2).map(|seq| {
                    schedule_local_activity_cmd(
                        seq,
                        &seq.to_string(),
                        ActivityCancellationType::TryCancel,
                        Duration::from_secs(120),
                    )
                }));
            }
            Variant::SignalWorkflow(signal) => jobs.push(signal.signal_name.clone()),
            Variant::ResolveActivity(resolution) => {
                assert!(resolution.is_local);
                let Some(activity_resolution::Status::Completed(result)) = resolution
                    .result
                    .as_ref()
                    .and_then(|result| result.status.as_ref())
                else {
                    panic!("unexpected LA result: {resolution:?}");
                };
                assert_eq!(
                    result.result.as_ref().unwrap().data,
                    vec![resolution.seq as u8]
                );
                jobs.push(format!("la-{}", resolution.seq));
                if resolution.seq == 1 {
                    commands.push(workflow_command::Variant::CompleteWorkflowExecution(
                        Default::default(),
                    ));
                }
            }
            unexpected => panic!("unexpected workflow job: {unexpected:?}"),
        }
    }
    (jobs, commands)
}

fn record_heartbeat_completion(history: &mut TestHistoryBuilder, request: WorkflowTaskCompletion) {
    history.add_workflow_task_completed();
    let completed_id = history.current_event_id();
    let Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(completed)) =
        &mut history.last_event().unwrap().attributes
    else {
        unreachable!();
    };
    completed.sdk_metadata = Some(request.sdk_metadata);
    for command in request.commands {
        match command.attributes.unwrap() {
            command::Attributes::RecordMarkerCommandAttributes(marker) => {
                history.add(MarkerRecordedEventAttributes {
                    marker_name: marker.marker_name,
                    details: marker.details,
                    failure: marker.failure,
                    workflow_task_completed_event_id: completed_id,
                    ..Default::default()
                });
            }
            command::Attributes::CompleteWorkflowExecutionCommandAttributes(_) => {
                history.add_workflow_execution_completed();
            }
            unexpected => panic!("unexpected recorded command: {unexpected:?}"),
        }
    }
}

type ActivationTrace = (Vec<String>, Option<prost_types::Timestamp>);

async fn replay_recorded_history(
    history: TestHistoryBuilder,
    step: impl Fn(&WorkflowActivation) -> (Vec<String>, Vec<workflow_command::Variant>),
) -> Vec<ActivationTrace> {
    let replay_worker = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream::iter([HistoryForReplay::new(
            history.get_full_history_info().unwrap(),
            "whn",
        )]),
    ))
    .unwrap();
    let mut replayed = vec![];
    loop {
        let activation = match replay_worker.poll_workflow_activation().await {
            Ok(activation) => activation,
            Err(PollError::ShutDown) => break,
            Err(error) => panic!("replay failed: {error:?}"),
        };
        let commands = if activation.is_only_eviction() {
            let Some(Variant::RemoveFromCache(eviction)) = &activation.jobs[0].variant else {
                unreachable!();
            };
            assert!(
                matches!(
                    eviction.reason(),
                    EvictionReason::LangRequested | EvictionReason::WorkflowExecutionEnding
                ),
                "replay eviction: {eviction:?}"
            );
            vec![]
        } else {
            let (jobs, commands) = step(&activation);
            replayed.push((jobs, activation.timestamp));
            commands
        };
        replay_worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                activation.run_id,
                commands,
            ))
            .await
            .unwrap();
    }
    replay_worker.shutdown().await;
    replayed
}

#[derive(Clone, Copy, PartialEq)]
enum ResolutionTiming {
    DuringSignal,
    BetweenTasks,
    BeforeHeartbeatAutocomplete,
}

fn queried_workflow_step(
    activation: &WorkflowActivation,
) -> (Vec<String>, Vec<workflow_command::Variant>) {
    let mut jobs = vec![];
    let mut commands = vec![];
    for job in &activation.jobs {
        match job.variant.as_ref().unwrap() {
            Variant::InitializeWorkflow(_) => {
                jobs.push("init".to_owned());
                commands.push(local_activity());
            }
            Variant::SignalWorkflow(signal) => {
                jobs.push(signal.signal_name.clone());
                if signal.signal_name == "signal" {
                    commands.push(schedule_activity_cmd(
                        2,
                        "whn",
                        "cancelled",
                        ActivityCancellationType::TryCancel,
                        Duration::from_secs(60),
                        Duration::from_secs(60),
                    ));
                    commands.push(RequestCancelActivity { seq: 2 }.into());
                }
            }
            Variant::ResolveActivity(result) => {
                jobs.push(format!(
                    "{}-{}",
                    if result.is_local { "la" } else { "act" },
                    result.seq
                ));
                if result.is_local {
                    commands.push(workflow_command::Variant::CompleteWorkflowExecution(
                        Default::default(),
                    ));
                }
            }
            Variant::QueryWorkflow(query) => {
                commands.push(crate::test_help::query_ok(&query.query_id, "answer"))
            }
            other => panic!("unexpected query workflow job: {other:?}"),
        }
    }
    (jobs, commands)
}

#[derive(Clone, Copy, PartialEq)]
enum QueryResolutionTiming {
    Before,
    During,
}

#[rstest::rstest]
#[case::before_query(QueryResolutionTiming::Before)]
#[case::during_query(QueryResolutionTiming::During)]
#[tokio::test]
async fn whn_query_response_preserves_pending_work(#[case] timing: QueryResolutionTiming) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut history, _) = history(Duration::from_secs(60));
        history.add_workflow_task_completed();
        history.add_we_signaled("signal", vec![]);
        history.add_workflow_task_scheduled_and_started();
        let mut first = hist_to_poll_resp(&history, "whn", ResponseType::AllHistory).resp;
        first.task_token = vec![1];
        first.queries.insert("pending".to_owned(), temporalio_common::protos::temporal::api::query::v1::WorkflowQuery {
            query_type: "state".to_owned(), ..Default::default()
        });
        let mut fixture = Fixture::new(first, 1, false);
        let mut live = vec![];
        let initialize = fixture.worker.poll_workflow_activation().await.unwrap();
        assert!(initialize.is_replaying);
        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
        let (jobs, commands) = queried_workflow_step(&initialize);
        live.push((jobs, initialize.timestamp));
        fixture.complete(&initialize, commands).await;
        let signal = fixture.worker.poll_workflow_activation().await.unwrap();
        let (jobs, commands) = queried_workflow_step(&signal);
        assert_eq!(jobs, ["signal"]);
        live.push((jobs, signal.timestamp));
        let activity = fixture.worker.poll_activity_task().await.unwrap();
        if timing == QueryResolutionTiming::Before {
            fixture.worker.complete_activity_task(ActivityTaskCompletion {
                task_token: activity.task_token.clone(),
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            }).await.unwrap();
        }
        fixture.complete(&signal, commands).await;
        let query = loop {
            let activation = tokio::select! {
                biased;
                request = fixture.completions.recv() => panic!("reported before pending work and query: {request:?}"),
                activation = fixture.worker.poll_workflow_activation() => activation.unwrap(),
            };
            if matches!(activation.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::QueryWorkflow(_)))) {
                break activation;
            }
            let (jobs, commands) = queried_workflow_step(&activation);
            live.push((jobs, activation.timestamp));
            fixture.complete(&activation, commands).await;
        };
        assert!(live.iter().flat_map(|(jobs, _)| jobs).any(|job| job == "act-2"));
        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
        if timing == QueryResolutionTiming::During {
            fixture.worker.complete_activity_task(ActivityTaskCompletion {
                task_token: activity.task_token.clone(),
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            }).await.unwrap();
        }
        let (_, commands) = queried_workflow_step(&query);
        assert!(fixture.completions.try_recv().is_err());
        fixture.complete(&query, commands).await;
        let request = fixture.completions.recv().await.unwrap();
        assert_eq!(request.query_responses.len(), 1);
        assert_eq!(request.query_responses[0].query_id, "pending");
        assert!(!request.notification_triggered_heartbeat);
        assert_eq!(request.force_create_new_workflow_task, timing != QueryResolutionTiming::Before);
        record_heartbeat_completion(&mut history, request);
        if timing != QueryResolutionTiming::Before {
            history.add_we_signaled("replacement", vec![]);
            history.add_workflow_task_scheduled_and_started();
            let mut next = hist_to_poll_resp(&history, "whn", ResponseType::OneTask(3)).resp;
            next.task_token = vec![2];
            fixture.polls.send(next).unwrap();
            let resolved = fixture.worker.poll_workflow_activation().await.unwrap();
            let (jobs, commands) = queried_workflow_step(&resolved);
            assert_eq!(jobs, ["replacement", "la-1"]);
            let result = resolved.jobs.iter().find_map(|job| match &job.variant {
                Some(Variant::ResolveActivity(result)) if result.is_local => result.result.as_ref(),
                _ => None,
            }).unwrap();
            assert!(matches!(&result.status, Some(activity_resolution::Status::Completed(_))));
            live.push((jobs, resolved.timestamp));
            fixture.complete(&resolved, commands).await;
            let request = fixture.completions.recv().await.unwrap();
            assert!(!request.force_create_new_workflow_task);
            record_heartbeat_completion(&mut history, request);
        }
        fixture.worker.drain_pollers_and_shutdown().await;
        assert_eq!(replay_recorded_history(history, queried_workflow_step).await, live);
    }).await.unwrap();
}

#[rstest::rstest]
#[case::during_signal_activation(ResolutionTiming::DuringSignal)]
#[case::between_workflow_tasks(ResolutionTiming::BetweenTasks)]
#[case::ready_before_autocomplete(ResolutionTiming::BeforeHeartbeatAutocomplete)]
#[tokio::test]
async fn repeated_whn_history_replays_local_activity_and_signal_order(
    #[case] timing: ResolutionTiming,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut history, first) = history(Duration::from_secs(60));
        let mut fixture = Fixture::new(first, 1, false);
        let mut live = vec![];
        let initialize = fixture.worker.poll_workflow_activation().await.unwrap();
        let (jobs, commands) = heartbeat_workflow_step(&initialize);
        live.push((jobs, initialize.timestamp));
        fixture.complete(&initialize, commands).await;
        let first_activity = fixture.worker.poll_activity_task().await.unwrap();
        let second_activity = fixture.worker.poll_activity_task().await.unwrap();
        let (long, short) = if matches!(first_activity.variant.as_ref(),
            Some(activity_task::Variant::Start(start)) if start.activity_id == "1") {
            (first_activity, second_activity)
        } else {
            (second_activity, first_activity)
        };
        assert!(matches!(short.variant.as_ref(),
            Some(activity_task::Variant::Start(start)) if start.activity_id == "2"));

        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
        if timing == ResolutionTiming::BeforeHeartbeatAutocomplete {
            fixture.worker.complete_activity_task(ActivityTaskCompletion {
                task_token: short.task_token.clone(),
                result: Some(ActivityExecutionResult::ok(vec![2].into())),
            }).await.unwrap();
            let resolved = tokio::select! {
                biased;
                request = fixture.completions.recv() => panic!("heartbeat before ready LA activation: {request:?}"),
                activation = fixture.worker.poll_workflow_activation() => activation.unwrap(),
            };
            let (jobs, commands) = heartbeat_workflow_step(&resolved);
            assert_eq!(jobs, ["la-2"]);
            live.push((jobs, resolved.timestamp));
            fixture.complete(&resolved, commands).await;
        }
        let first_complete = tokio::select! {
            request = fixture.completions.recv() => request.unwrap(),
            activation = fixture.worker.poll_workflow_activation() => panic!("unexpected activation: {activation:?}"),
        };
        assert!(first_complete.force_create_new_workflow_task);
        record_heartbeat_completion(&mut history, first_complete);
        history.add_we_signaled("first-signal", vec![]);
        history.add_workflow_task_scheduled_and_started();
        let mut next = hist_to_poll_resp(&history, "whn", ResponseType::OneTask(2)).resp;
        next.task_token = vec![2];
        fixture.polls.send(next).unwrap();
        let signal = fixture.worker.poll_workflow_activation().await.unwrap();
        let (jobs, commands) = heartbeat_workflow_step(&signal);
        live.push((jobs, signal.timestamp));
        assert_eq!(fixture.notify(&[vec![2]]).await, vec![Outcome::Accepted]);
        if timing == ResolutionTiming::DuringSignal {
            fixture.worker.complete_activity_task(ActivityTaskCompletion {
                task_token: short.task_token.clone(),
                result: Some(ActivityExecutionResult::ok(vec![2].into())),
            }).await.unwrap();
        }
        fixture.complete(&signal, commands).await;
        if timing == ResolutionTiming::DuringSignal {
            let resolved = tokio::select! {
                biased;
                request = fixture.completions.recv() => panic!("heartbeat before LA activation: {request:?}"),
                activation = fixture.worker.poll_workflow_activation() => activation.unwrap(),
            };
            let (jobs, commands) = heartbeat_workflow_step(&resolved);
            assert_eq!(jobs, ["la-2"]);
            live.push((jobs, resolved.timestamp));
            fixture.complete(&resolved, commands).await;
        }
        let second_complete = fixture.completions.recv().await.unwrap();
        assert!(second_complete.force_create_new_workflow_task);
        record_heartbeat_completion(&mut history, second_complete);
        if timing == ResolutionTiming::BetweenTasks {
            fixture.worker.complete_activity_task(ActivityTaskCompletion {
                task_token: short.task_token,
                result: Some(ActivityExecutionResult::ok(vec![2].into())),
            }).await.unwrap();
        }
        history.add_we_signaled("second-signal", vec![]);
        history.add_workflow_task_scheduled_and_started();
        let mut next = hist_to_poll_resp(&history, "whn", ResponseType::OneTask(3)).resp;
        next.task_token = vec![3];
        fixture.polls.send(next).unwrap();
        let signal = fixture.worker.poll_workflow_activation().await.unwrap();
        let (jobs, commands) = heartbeat_workflow_step(&signal);
        assert_eq!(jobs, if timing == ResolutionTiming::BetweenTasks {
            vec!["second-signal", "la-2"]
        } else {
            vec!["second-signal"]
        });
        live.push((jobs, signal.timestamp));
        fixture.complete(&signal, commands).await;
        assert_eq!(fixture.notify(&[vec![3]]).await, vec![Outcome::Accepted]);
        fixture.worker.complete_activity_task(ActivityTaskCompletion {
            task_token: long.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        }).await.unwrap();
        let resolved = tokio::select! {
            biased;
            request = fixture.completions.recv() => panic!("heartbeat before final LA activation: {request:?}"),
            activation = fixture.worker.poll_workflow_activation() => activation.unwrap(),
        };
        let (jobs, commands) = heartbeat_workflow_step(&resolved);
        assert_eq!(jobs, ["la-1"]);
        live.push((jobs, resolved.timestamp));
        fixture.complete(&resolved, commands).await;
        let final_complete = fixture.completions.recv().await.unwrap();
        assert!(!final_complete.force_create_new_workflow_task);
        record_heartbeat_completion(&mut history, final_complete);
        fixture.worker.drain_pollers_and_shutdown().await;

        assert_eq!(replay_recorded_history(history, heartbeat_workflow_step).await, live);
    }).await.unwrap();
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
    assert_eq!(fixture.notify(&[vec![2]]).await, vec![Outcome::Accepted]);
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
#[case::polled(false, false)]
#[case::eager(true, false)]
#[case::polled_multiple_activations(false, true)]
#[case::eager_multiple_activations(true, true)]
#[tokio::test]
async fn command_heartbeats_and_delivers_signal_while_local_activity_is_running(
    #[case] eager: bool,
    #[case] multiple_activations: bool,
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
            Outcome::Accepted,
            Outcome::Accepted,
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
        fixture.notification_outcome(vec![1]),
        Outcome::TaskTokenMismatch
    );

    // Acceptance during a signal activation must not report its WFT until that activation completes.
    assert_eq!(
        fixture.notify(&[vec![2], vec![2]]).await,
        vec![Outcome::Accepted, Outcome::Accepted]
    );
    assert!(fixture.completions.try_recv().is_err());
    let commands = if multiple_activations {
        vec![
            schedule_activity_cmd(
                2,
                "whn",
                "cancelled-activity",
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            RequestCancelActivity { seq: 2 }.into(),
        ]
    } else {
        vec![]
    };
    fixture.complete(&signal, commands).await;
    if multiple_activations {
        // Cancelling an unsent activity creates another job without completing the running LA.
        let cancelled = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                biased;
                request = fixture.completions.recv() => {
                    panic!("WFT reported before its cancellation activation: {request:?}");
                }
                activation = fixture.worker.poll_workflow_activation() => activation.unwrap(),
            }
        })
        .await
        .unwrap();
        assert_eq!(cancelled.jobs.len(), 1);
        let Some(Variant::ResolveActivity(resolution)) = &cancelled.jobs[0].variant else {
            panic!("expected activity cancellation, got {cancelled:?}");
        };
        assert_eq!(resolution.seq, 2);
        assert!(!resolution.is_local);
        assert!(matches!(
            resolution.result.as_ref().unwrap().status,
            Some(activity_resolution::Status::Cancelled(_))
        ));
        assert!(fixture.completions.try_recv().is_err());
        fixture.complete(&cancelled, vec![]).await;
    }
    let request = fixture.completions.recv().await.unwrap();
    assert_eq!(request.task_token, vec![2].into());
    assert!(request.force_create_new_workflow_task);
    assert!(request.commands.is_empty());
    assert_eq!(
        fixture.notification_outcome(vec![2]),
        Outcome::TaskTokenMismatch
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
        fixture.notification_outcome(vec![3]),
        Outcome::TaskTokenMismatch
    );
    fixture.worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn paginated_replacement_waits_for_history_while_local_activity_is_running() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut history, first) = history(Duration::from_secs(60));
        let mut fixture = Fixture::new(first, 1, false);
        let activation = fixture.worker.poll_workflow_activation().await.unwrap();
        fixture.complete(&activation, vec![local_activity()]).await;
        let activity = fixture.worker.poll_activity_task().await.unwrap();

        history.add_workflow_task_completed();
        history.add_we_signaled("after-page", vec![]);
        history.add_workflow_task_scheduled_and_started();
        let mut next = hist_to_poll_resp(&history, "whn", ResponseType::ToTaskNum(2)).resp;
        next.task_token = vec![2];
        let rest = next.history.as_mut().unwrap().events.split_off(4);
        next.next_page_token = vec![1];
        *fixture.replacement.lock().unwrap() = Some(next);
        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);

        let signal = {
            let activation = fixture.worker.poll_workflow_activation();
            tokio::pin!(activation);
            let (request, (page_token, release_page)) = tokio::select! {
                activation = &mut activation => {
                    panic!("activation before history was fetched: {activation:?}");
                }
                observed = async { tokio::join!(fixture.completions.recv(), fixture.history_fetches.recv()) } => {
                    (observed.0.unwrap(), observed.1.unwrap())
                }
            };
            assert_eq!(request.task_token, vec![1].into());
            assert!(request.force_create_new_workflow_task);
            assert_eq!(page_token, vec![1]);
            // The replacement cannot accept WHN until its history has been fetched and adopted.
            assert_eq!(
                vec![fixture.notification_outcome(vec![1]), fixture.notification_outcome(vec![2])],
                vec![Outcome::TaskTokenMismatch, Outcome::TaskTokenMismatch]
            );
            assert!(fixture.completions.try_recv().is_err());
            release_page.send(GetWorkflowExecutionHistoryResponse {
                history: Some(History { events: rest }),
                ..Default::default()
            }).unwrap();

            activation.await.unwrap()
        };
        assert!(matches!(&signal.jobs.as_slice(), [job]
            if matches!(&job.variant, Some(Variant::SignalWorkflow(s)) if s.signal_name == "after-page")));
        assert_eq!(fixture.notify(&[vec![2]]).await, vec![Outcome::Accepted]);
        assert!(fixture.completions.try_recv().is_err());
        fixture.complete(&signal, vec![]).await;
        let request = fixture.completions.recv().await.unwrap();
        assert_eq!(request.task_token, vec![2].into());
        assert!(request.force_create_new_workflow_task);

        history.add_workflow_task_completed();
        history.add_workflow_task_scheduled_and_started();
        let mut third = hist_to_poll_resp(&history, "whn", ResponseType::ToTaskNum(3)).resp;
        third.task_token = vec![3];
        fixture.polls.send(third).unwrap();
        fixture.worker.complete_activity_task(ActivityTaskCompletion {
            task_token: activity.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        }).await.unwrap();
        let resolved = fixture.worker.poll_workflow_activation().await.unwrap();
        assert!(matches!(&resolved.jobs.as_slice(), [job]
            if matches!(&job.variant, Some(Variant::ResolveActivity(r)) if r.is_local && r.seq == 1)));
        fixture.worker.complete_execution(&resolved.run_id).await;
        assert!(!fixture.completions.recv().await.unwrap().force_create_new_workflow_task);
        fixture.worker.drain_pollers_and_shutdown().await;
    }).await.unwrap();
}

#[rstest::rstest]
#[case::chunked(false)]
#[case::unchunked(true)]
#[tokio::test]
async fn notification_survives_paginated_replay_until_local_activity_is_running(
    #[case] unchunked: bool,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut history, _) = history(Duration::from_secs(60));
        history.add_workflow_task_completed();
        if unchunked {
            history.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
        }
        history.add_we_signaled("replayed", vec![]);
        history.add_full_wf_task();
        history.add_we_signaled("live", vec![]);
        history.add_workflow_task_scheduled_and_started();
        let mut first = hist_to_poll_resp(&history, "whn", ResponseType::AllHistory).resp;
        first.task_token = vec![1];
        let rest = first.history.as_mut().unwrap().events.split_off(4);
        first.next_page_token = vec![1];
        let mut fixture = Fixture::new(first, 1, false);

        let initialize = {
            let activation = fixture.worker.poll_workflow_activation();
            tokio::pin!(activation);
            let (page_token, release_page) = tokio::select! {
                activation = &mut activation => {
                    panic!("replay began before history was fetched: {activation:?}");
                }
                fetch = fixture.history_fetches.recv() => fetch.unwrap(),
            };
            assert_eq!(page_token, vec![1]);
            assert_eq!(fixture.notification_outcome(vec![1]), Outcome::TaskTokenMismatch);
            assert!(fixture.completions.try_recv().is_err());
            release_page.send(GetWorkflowExecutionHistoryResponse {
                history: Some(History { events: rest }),
                ..Default::default()
            }).unwrap();

            activation.await.unwrap()
        };
        assert!(initialize.is_replaying);
        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
        fixture.complete(&initialize, vec![local_activity()]).await;
        let mut signals = vec![];
        let activity = loop {
            let activation = tokio::select! {
                biased;
                request = fixture.completions.recv() => {
                    panic!("WHN reported before replay activations finished: {request:?}");
                }
                activation = fixture.worker.poll_workflow_activation() => activation.unwrap(),
            };
            for job in &activation.jobs {
                let Some(Variant::SignalWorkflow(signal)) = &job.variant else {
                    panic!("unexpected replay job: {job:?}");
                };
                signals.push(signal.signal_name.clone());
            }
            assert!(fixture.completions.try_recv().is_err());
            if !activation.is_replaying {
                assert_eq!(signals, ["replayed", "live"]);
                let activity = fixture.worker.poll_activity_task().await.unwrap();
                fixture.complete(&activation, vec![]).await;
                break activity;
            }
            fixture.complete(&activation, vec![]).await;
        };
        let request = fixture.completions.recv().await.unwrap();
        assert_eq!(request.task_token, vec![1].into());
        assert!(request.force_create_new_workflow_task);

        history.add_workflow_task_completed();
        history.add_workflow_task_scheduled_and_started();
        let mut next = hist_to_poll_resp(&history, "whn", ResponseType::ToTaskNum(4)).resp;
        next.task_token = vec![2];
        fixture.polls.send(next).unwrap();
        fixture.worker.complete_activity_task(ActivityTaskCompletion {
            task_token: activity.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        }).await.unwrap();
        let resolved = fixture.worker.poll_workflow_activation().await.unwrap();
        assert!(matches!(&resolved.jobs.as_slice(), [job]
            if matches!(&job.variant, Some(Variant::ResolveActivity(r)) if r.is_local && r.seq == 1)));
        fixture.worker.complete_execution(&resolved.run_id).await;
        assert!(!fixture.completions.recv().await.unwrap().force_create_new_workflow_task);
        fixture.worker.drain_pollers_and_shutdown().await;
    }).await.unwrap();
}

#[rstest::rstest]
#[case::chunked(false)]
#[case::unchunked(true)]
#[tokio::test]
async fn notification_waits_for_post_adoption_fetch_after_stale_cache_miss(
    #[case] unchunked: bool,
    #[values(false, true)] with_query: bool,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut history, original) = history(Duration::from_secs(60));
        history.add_workflow_task_completed();
        if unchunked {
            history.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
        }
        history.add_we_signaled("polled", vec![]);
        history.add_workflow_task_scheduled_and_started();
        let polled_started_id = history.current_event_id();
        let mut first = hist_to_poll_resp(&history, "whn", ResponseType::OneTask(2)).resp;
        first.task_token = vec![1];
        assert_eq!(first.previous_started_event_id, original.started_event_id);
        assert_eq!(first.started_event_id, polled_started_id);
        if with_query {
            first.queries.insert(
                "pending".to_owned(),
                temporalio_common::protos::temporal::api::query::v1::WorkflowQuery {
                    query_type: "state".to_owned(),
                    ..Default::default()
                },
            );
        }

        // A cache-miss reload starts a fresh history read, which can outlive the polled task.
        history.add_workflow_task_timed_out();
        history.add_we_signaled("after-timeout", vec![]);
        history.add_full_wf_task();
        history.add_we_signaled("later", vec![]);
        history.add_full_wf_task();
        // The server persists the buffered signal and next WFT scheduling in one batch.
        history.add_we_signaled("last", vec![]);
        history.add_workflow_task_scheduled();
        let initial_page_len = history.current_event_id() as usize;
        history.add_workflow_task_started();
        let mut snapshot = history.get_full_history_info().unwrap().into_events();
        let rest = snapshot.split_off(initial_page_len);
        let mut fixture = Fixture::new(first, 1, false);
        fixture.fail_completion.store(true, Ordering::Relaxed);

        let initialize = {
            let activation = fixture.worker.poll_workflow_activation();
            tokio::pin!(activation);
            let (token, release) = tokio::select! {
                activation = &mut activation => panic!("cache miss did not fetch: {activation:?}"),
                fetch = fixture.history_fetches.recv() => fetch.unwrap(),
            };
            assert!(token.is_empty());
            release
                .send(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: snapshot }),
                    next_page_token: vec![2],
                    ..Default::default()
                })
                .unwrap();
            tokio::select! {
                activation = &mut activation => activation.unwrap(),
                fetch = fixture.history_fetches.recv() => {
                    panic!("expected adoption before the next fetch: {fetch:?}");
                }
            }
        };
        assert!(initialize.is_replaying);
        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
        fixture.complete(&initialize, vec![local_activity()]).await;

        let mut rest = Some(rest);
        let mut signals = vec![];
        let activity =
            loop {
                let activation = tokio::select! {
                    biased;
                    request = fixture.completions.recv() => panic!("early heartbeat: {request:?}"),
                    activation = fixture.worker.poll_workflow_activation() => activation.unwrap(),
                };
                for job in &activation.jobs {
                    let Some(Variant::SignalWorkflow(signal)) = &job.variant else {
                        panic!("unexpected replay job: {job:?}");
                    };
                    signals.push(signal.signal_name.clone());
                }
                if !activation.is_replaying {
                    assert!(
                        rest.is_none(),
                        "replay ended before the remaining page arrived"
                    );
                    assert_eq!(signals, ["polled", "after-timeout", "later", "last"]);
                    let activity = fixture.worker.poll_activity_task().await.unwrap();
                    fixture.complete(&activation, vec![]).await;
                    if with_query {
                        assert!(fixture.completions.try_recv().is_err());
                        let query = fixture.worker.poll_workflow_activation().await.unwrap();
                        assert!(matches!(query.jobs.as_slice(), [job]
                        if matches!(job.variant, Some(Variant::QueryWorkflow(_)))));
                        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
                        fixture
                            .complete(
                                &query,
                                vec![crate::test_help::query_ok("pending", "answer")],
                            )
                            .await;
                    }
                    break activity;
                }
                let completion = fixture.worker.complete_workflow_activation(
                    WorkflowActivationCompletion::empty(activation.run_id),
                );
                tokio::pin!(completion);
                tokio::select! {
                    result = &mut completion => result.unwrap(),
                    fetch = fixture.history_fetches.recv() => {
                        let (token, release) = fetch.unwrap();
                        assert_eq!(token, vec![2]);
                        assert_eq!(fixture.notification_outcome(vec![1]), Outcome::Accepted);
                        assert!(fixture.completions.try_recv().is_err());
                        release.send(GetWorkflowExecutionHistoryResponse {
                            history: Some(History { events: rest.take().unwrap() }),
                            ..Default::default()
                        }).unwrap();
                        completion.await.unwrap();
                    }
                }
            };
        let request = fixture.completions.recv().await.unwrap();
        assert_eq!(request.task_token, vec![1].into());
        assert!(request.force_create_new_workflow_task);
        assert_eq!(request.query_responses.len(), usize::from(with_query));
        let (eviction, cancellation) = tokio::join!(
            fixture.worker.poll_workflow_activation(),
            fixture.worker.poll_activity_task(),
        );
        let eviction = eviction.unwrap();
        assert!(eviction.is_only_eviction());
        assert_eq!(cancellation.unwrap().task_token, activity.task_token);
        assert_eq!(
            fixture.notification_outcome(vec![1]),
            Outcome::TaskTokenMismatch
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
    })
    .await
    .unwrap();
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
    assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
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
    assert_eq!(request.notification_triggered_heartbeat, schedule_la);
    assert_eq!(
        fixture.notification_outcome(vec![1]),
        Outcome::TaskTokenMismatch
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
    let (mut history, first) = history(Duration::from_secs(1));
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
        assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
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
        assert_eq!(
            request.notification_triggered_heartbeat,
            notify_first && token == vec![1]
        );
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
        fixture.notification_outcome(vec![2]),
        Outcome::TaskTokenMismatch
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
    assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
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
        fixture.notification_outcome(vec![1]),
        Outcome::TaskTokenMismatch
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
    assert_eq!(fixture.notify(&[vec![1]]).await, vec![Outcome::Accepted]);
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
        fixture.notification_outcome(vec![1]),
        Outcome::TaskTokenMismatch
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
            Outcome::TaskTokenMismatch
        } else {
            Outcome::Accepted
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
        fixture.notification_outcome(vec![1]),
        Outcome::TaskTokenMismatch
    );
    fixture.worker.drain_pollers_and_shutdown().await;
}
