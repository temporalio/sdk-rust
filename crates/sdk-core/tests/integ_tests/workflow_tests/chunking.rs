use crate::common::{get_integ_client, integ_namespace};
use futures_util::{StreamExt, stream};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{NamespacedClient, grpc::WorkflowService};
use temporalio_common::protos::{
    coresdk::{
        AsJsonPayloadExt,
        workflow_activation::{WorkflowActivation, workflow_activation_job::Variant},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::{WorkflowExecution, WorkflowType},
        enums::v1::{EventType, TaskQueueKind, TaskQueueType},
        history::v1::{
            History, TimerStartedEventAttributes, WorkflowExecutionCompletedEventAttributes,
        },
        sdk::v1::WorkflowTaskCompletedMetadata,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
        workflowservice::v1::{
            DescribeTaskQueueRequest, GetWorkflowExecutionHistoryRequest,
            PollWorkflowTaskQueueRequest, RespondWorkflowTaskCompletedRequest,
            SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
            TerminateWorkflowExecutionRequest,
        },
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    LocalActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::WorkerInterceptor,
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    test_help::{CoreInternalFlags, MocksHolder, WorkerExt, mock_worker, mock_worker_client},
};
use tonic::IntoRequest;

struct HeartbeatActivity;

#[activities]
impl HeartbeatActivity {
    #[activity]
    async fn run(self: Arc<Self>, _: ActivityContext, _: ()) -> Result<String, ActivityError> {
        panic!("Replay must resolve this activity from its recorded marker");
    }
}

#[workflow]
#[derive(Default)]
struct HeartbeatWorkflow;

#[workflow_methods]
impl HeartbeatWorkflow {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        ctx.timer(Duration::from_millis(1)).await;
        ctx.timer(Duration::from_millis(1)).await;
        let clock = ctx.clone();
        let started_at = ctx.workflow_time().unwrap();
        let (result, condition) = temporalio_sdk::workflows::join!(
            ctx.execute_local_activity(HeartbeatActivity::run, (), LocalActivityOptions::default()),
            ctx.wait_condition(move |_| clock.workflow_time().unwrap() > started_at)
        );
        condition?;
        Ok(result?)
    }
}

struct NoEmptyActivations;

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for NoEmptyActivations {
    async fn on_workflow_activation(&self, activation: &WorkflowActivation) -> anyhow::Result<()> {
        assert!(!activation.jobs.is_empty());
        Ok(())
    }
}

// Derived from a live run with adoption disabled: two timers, two LA heartbeat completions,
// then the LA marker and workflow result in the fifth task. The activity was released only after
// task 4's completion appeared in history. Jobless heartbeats must not activate lang or shift
// the LA's activation index when the chunking flag is added.
fn heartbeat_history(unchunked: bool) -> History {
    let mut history = TestHistoryBuilder::default();
    history.add_wfe_started_with_wft_timeout(Duration::from_secs(2));
    for task in 1..=5 {
        history.add_full_wf_task();
        let mut flags = if task == 1 {
            vec![
                CoreInternalFlags::IdAndTypeDeterminismChecks,
                CoreInternalFlags::UpsertSearchAttributeOnPatch,
                CoreInternalFlags::MoveTerminalCommands,
                CoreInternalFlags::NexusOperationDeterminismChecks,
            ]
        } else {
            vec![]
        };
        if task == 1 && unchunked {
            flags.push(CoreInternalFlags::DisableWorkflowTaskChunking);
        }
        history.set_flags_last_wft(&flags, &[]);
        if task <= 2 {
            let timer_id = task.to_string();
            let started = history.add(TimerStartedEventAttributes {
                timer_id: timer_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(1).try_into().unwrap()),
                workflow_task_completed_event_id: if task == 1 { 4 } else { 9 },
            });
            history.add_timer_fired(started, timer_id);
        }
    }
    history.add_local_activity_marker(
        1,
        "1",
        Some("done".as_json_payload().unwrap()),
        None,
        |marker| {
            marker.activity_type = "HeartbeatActivity::run".to_owned();
            marker.complete_time = Some(prost_types::Timestamp {
                seconds: 20,
                nanos: 0,
            });
            marker.original_schedule_time = Some(prost_types::Timestamp {
                seconds: 13,
                nanos: 0,
            });
            marker.activation_index = Some(0);
        },
    );
    history.add(WorkflowExecutionCompletedEventAttributes {
        workflow_task_completed_event_id: 20,
        result: Some("done".as_json_payload().unwrap().into()),
        ..Default::default()
    });
    let mut events = history.get_full_history_info().unwrap().into_events();
    for event in &mut events {
        // Preserve advancing workflow clocks without depending on the wall clock during replay.
        event.event_time = Some(prost_types::Timestamp {
            seconds: event.event_id,
            nanos: 0,
        });
    }
    History { events }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[case::unchunked(true)]
#[case::legacy(false)]
#[tokio::test]
async fn workflow_task_chunking_replays_compatible_histories(#[case] unchunked: bool) {
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<HeartbeatWorkflow>()
            .unwrap()
            .worker_interceptor(NoEmptyActivations)
            .build(),
    )
    .unwrap();
    replayer
        .replay_workflow(heartbeat_history(unchunked).into())
        .await
        .unwrap();
}

#[rstest::rstest]
#[case::chunked(false)]
#[case::unchunked(true)]
#[tokio::test]
async fn stale_cache_miss_replays_server_history_newer_than_polled_task(#[case] unchunked: bool) {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut client = get_integ_client(integ_namespace(), None).await;
        let namespace = client.namespace().to_owned();
        let workflow_id = format!("stale-cache-miss-{}", uuid::Uuid::new_v4());
        let normal_queue = TaskQueue {
            name: workflow_id.clone(),
            kind: TaskQueueKind::Normal.into(),
            ..Default::default()
        };
        let sticky_queue = TaskQueue {
            name: format!("{workflow_id}-sticky"),
            normal_name: normal_queue.name.clone(),
            kind: TaskQueueKind::Sticky.into(),
        };
        let started = client
            .start_workflow_execution(
                StartWorkflowExecutionRequest {
                    namespace: namespace.clone(),
                    workflow_id: workflow_id.clone(),
                    workflow_type: Some(WorkflowType {
                        name: DEFAULT_WORKFLOW_TYPE.to_owned(),
                    }),
                    task_queue: Some(normal_queue.clone()),
                    workflow_task_timeout: Some(Duration::from_secs(2).try_into().unwrap()),
                    request_id: uuid::Uuid::new_v4().to_string(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();
        let execution = WorkflowExecution {
            workflow_id,
            run_id: started.run_id,
        };
        let poll = |queue: TaskQueue| {
            let mut client = client.clone();
            let namespace = namespace.clone();
            async move {
                client
                    .poll_workflow_task_queue(
                        PollWorkflowTaskQueueRequest {
                            namespace,
                            task_queue: Some(queue),
                            identity: "stale-cache-miss".to_owned(),
                            ..Default::default()
                        }
                        .into_request(),
                    )
                    .await
                    .unwrap()
                    .into_inner()
            }
        };
        let signal = |name: &str| {
            let mut client = client.clone();
            let namespace = namespace.clone();
            let execution = execution.clone();
            let signal_name = name.to_owned();
            async move {
                client
                    .signal_workflow_execution(
                        SignalWorkflowExecutionRequest {
                            namespace,
                            workflow_execution: Some(execution),
                            signal_name,
                            request_id: uuid::Uuid::new_v4().to_string(),
                            ..Default::default()
                        }
                        .into_request(),
                    )
                    .await
                    .unwrap();
            }
        };
        let complete =
            |token: Vec<u8>,
             sticky_attributes: Option<StickyExecutionAttributes>,
             sdk_metadata: Option<WorkflowTaskCompletedMetadata>| {
                let mut client = client.clone();
                let namespace = namespace.clone();
                async move {
                    client
                        .respond_workflow_task_completed(
                            RespondWorkflowTaskCompletedRequest {
                                namespace,
                                task_token: token,
                                identity: "stale-cache-miss".to_owned(),
                                sticky_attributes,
                                force_create_new_workflow_task: true,
                                sdk_metadata,
                                ..Default::default()
                            }
                            .into_request(),
                        )
                        .await
                        .unwrap();
                }
            };
        let sticky_poll = tokio::spawn(poll(sticky_queue.clone()));
        loop {
            let description = client
                .clone()
                .describe_task_queue(
                    DescribeTaskQueueRequest {
                        namespace: namespace.clone(),
                        task_queue: Some(sticky_queue.clone()),
                        task_queue_type: TaskQueueType::Workflow.into(),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .unwrap()
                .into_inner();
            if !description.pollers.is_empty() {
                break;
            }
        }
        let first = poll(normal_queue.clone()).await;
        signal("polled").await;
        complete(
            first.task_token,
            Some(StickyExecutionAttributes {
                worker_task_queue: Some(sticky_queue.clone()),
                schedule_to_start_timeout: Some(Duration::from_secs(30).try_into().unwrap()),
            }),
            unchunked.then_some(WorkflowTaskCompletedMetadata {
                core_used_flags: vec![CoreInternalFlags::DisableWorkflowTaskChunking as u32],
                ..Default::default()
            }),
        )
        .await;
        let stale = sticky_poll.await.unwrap();
        assert_eq!(stale.previous_started_event_id, first.started_event_id);
        assert!(stale.history.as_ref().unwrap().events[0].event_id > 1);
        assert!(stale.next_page_token.is_empty());

        // A buffered signal makes the timeout and retry visible in persisted history.
        signal("after-timeout").await;
        let retry = poll(normal_queue.clone()).await;
        signal("later").await;
        complete(retry.task_token, None, None).await;
        let next = poll(normal_queue.clone()).await;
        signal("last").await;
        complete(next.task_token, None, None).await;
        // Signals persist individually while the next task is scheduled, without a running
        // task buffering them into one batch. Exceed the default history page size.
        const PAGE_FILLER_SIGNALS: usize = 1100;
        for _ in 0..PAGE_FILLER_SIGNALS {
            signal("padding").await;
        }
        let current = poll(normal_queue.clone()).await;
        assert!(current.started_event_id > stale.started_event_id);

        let mut pages = VecDeque::new();
        let mut token = vec![];
        loop {
            let response = client
                .get_workflow_execution_history(
                    GetWorkflowExecutionHistoryRequest {
                        namespace: namespace.clone(),
                        execution: Some(execution.clone()),
                        next_page_token: token.clone(),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .unwrap()
                .into_inner();
            let next = response.next_page_token.clone();
            pages.push_back((token, response));
            if next.is_empty() {
                break;
            }
            token = next;
        }
        assert!(
            pages.len() > 1,
            "fresh history must span default-sized server pages"
        );
        assert!(
            pages
                .iter()
                .flat_map(|(_, response)| &response.history.as_ref().unwrap().events)
                .any(|event| event.event_type() == EventType::WorkflowTaskTimedOut)
        );
        assert_eq!(
            pages
                .back()
                .unwrap()
                .1
                .history
                .as_ref()
                .unwrap()
                .events
                .last()
                .unwrap()
                .event_id,
            current.started_event_id
        );
        let expired = client
            .respond_workflow_task_completed(
                RespondWorkflowTaskCompletedRequest {
                    namespace: namespace.clone(),
                    task_token: stale.task_token.clone(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap_err();
        assert_eq!(expired.code(), tonic::Code::NotFound);
        let total_pages = pages.len();
        let fetches = Arc::new(AtomicUsize::new(0));
        let count = fetches.clone();
        // Preserve the server's poll IDs and page cuts while controlling when Core sees them.
        let mut mock = mock_worker_client();
        mock.expect_get_workflow_execution_history()
            .returning(move |_, _, token| {
                let (expected_token, response) =
                    pages.pop_front().expect("unexpected extra page fetch");
                assert_eq!(token, expected_token);
                count.fetch_add(1, Ordering::Relaxed);
                Ok(response)
            });
        let (reported, mut reports) = tokio::sync::mpsc::unbounded_channel();
        mock.expect_complete_workflow_task()
            .times(1)
            .returning(move |request, _| {
                reported.send(request).unwrap();
                Err(expired.clone())
            });
        mock.expect_fail_workflow_task()
            .returning(|_, _, failure| panic!("server history failed replay: {failure:?}"));
        let stale_token = stale.task_token.clone();
        let mocks =
            MocksHolder::from_wft_stream(mock, stream::iter([stale]).chain(stream::pending()));
        let core = mock_worker(mocks);
        let initial = core.poll_workflow_activation().await.unwrap();
        assert!(initial.is_replaying);
        assert!(
            fetches.load(Ordering::Relaxed) < total_pages,
            "server history must be adopted before all pages are fetched"
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(initial.run_id))
            .await
            .unwrap();
        let mut signals = 0;
        loop {
            let activation = core.poll_workflow_activation().await.unwrap();
            let eviction = activation.eviction_reason().is_some();
            for job in &activation.jobs {
                if matches!(job.variant, Some(Variant::SignalWorkflow(_))) {
                    signals += 1;
                }
            }
            core.complete_workflow_activation(WorkflowActivationCompletion::empty(
                activation.run_id,
            ))
            .await
            .unwrap();
            if eviction {
                break;
            }
        }
        assert_eq!(signals, PAGE_FILLER_SIGNALS + 4);
        assert_eq!(fetches.load(Ordering::Relaxed), total_pages);
        let report = reports
            .try_recv()
            .expect("history must finish before stale-task reporting");
        assert_eq!(report.task_token.into_inner(), stale_token);
        core.drain_pollers_and_shutdown().await;
        client
            .terminate_workflow_execution(
                TerminateWorkflowExecutionRequest {
                    namespace,
                    workflow_execution: Some(execution),
                    reason: "stale cache miss verification completed".to_owned(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
