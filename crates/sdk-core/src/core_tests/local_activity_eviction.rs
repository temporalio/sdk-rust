use crate::{
    PollError, WorkflowTaskCompletion, init_replay_worker,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MocksHolder, ResponseType, WorkerExt, hist_to_poll_resp, mock_worker, query_ok,
        schedule_local_activity_cmd, start_timer_cmd, test_worker_cfg,
    },
    worker::client::mocks::mock_worker_client,
};
use futures_util::stream;
use std::time::Duration;
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::{ActivityExecutionResult, activity_resolution},
        activity_task::activity_task,
        common::NamespacedWorkflowExecution,
        workflow_activation::{
            WorkflowActivation, remove_from_cache::EvictionReason, workflow_activation_job,
        },
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, SignalExternalWorkflowExecution,
            signal_external_workflow_execution, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command,
        common::v1::Payload,
        enums::v1::CommandType,
        history::v1::{MarkerRecordedEventAttributes, history_event},
        query::v1::WorkflowQuery,
        workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

const WFID: &str = "la-eviction";

/// Starts a timer, then schedules local activity 1 and signals another workflow in the same
/// activation, and completes once the local activity resolves.
fn workflow(activation: &WorkflowActivation) -> Vec<workflow_command::Variant> {
    let mut commands = vec![];
    for job in &activation.jobs {
        match job.variant.as_ref().unwrap() {
            workflow_activation_job::Variant::InitializeWorkflow(_) => {
                commands.push(start_timer_cmd(1, Duration::from_secs(1)));
            }
            workflow_activation_job::Variant::FireTimer(_) => {
                commands.push(schedule_local_activity_cmd(
                    1,
                    "1",
                    ActivityCancellationType::TryCancel,
                    Duration::from_secs(60),
                ));
                commands.push(
                    SignalExternalWorkflowExecution {
                        seq: 1,
                        target: Some(
                            signal_external_workflow_execution::Target::WorkflowExecution(
                                NamespacedWorkflowExecution {
                                    namespace: "default".to_owned(),
                                    workflow_id: "signal-target".to_owned(),
                                    run_id: String::new(),
                                },
                            ),
                        ),
                        signal_name: "wake".to_owned(),
                        ..Default::default()
                    }
                    .into(),
                );
            }
            workflow_activation_job::Variant::ResolveActivity(_) => {
                commands.push(CompleteWorkflowExecution { result: None }.into());
            }
            workflow_activation_job::Variant::QueryWorkflow(q) => {
                commands.push(query_ok(q.query_id.clone(), "answer"));
            }
            workflow_activation_job::Variant::ResolveSignalExternalWorkflow(_)
            | workflow_activation_job::Variant::RemoveFromCache(_) => {}
            unexpected => panic!("Unexpected activation job: {unexpected:?}"),
        }
    }
    commands
}

fn task(history: &TestHistoryBuilder, query: bool) -> PollWorkflowTaskQueueResponse {
    let mut task = hist_to_poll_resp(history, WFID, ResponseType::AllHistory).resp;
    if query {
        task.queries.insert(
            "q".to_owned(),
            WorkflowQuery {
                query_type: "query-type".to_owned(),
                ..Default::default()
            },
        );
    }
    task
}

/// Appends the events the server records for a workflow task completion.
fn record_completion(t: &mut TestHistoryBuilder, completion: &WorkflowTaskCompletion) {
    t.add_workflow_task_completed();
    let wft_completed_id = t.current_event_id();
    for command in &completion.commands {
        match command.attributes.clone().unwrap() {
            command::Attributes::StartTimerCommandAttributes(timer) => {
                t.add_timer_started(timer.timer_id);
            }
            command::Attributes::RecordMarkerCommandAttributes(marker) => {
                t.add(history_event::Attributes::MarkerRecordedEventAttributes(
                    MarkerRecordedEventAttributes {
                        marker_name: marker.marker_name,
                        details: marker.details,
                        header: marker.header,
                        failure: marker.failure,
                        workflow_task_completed_event_id: wft_completed_id,
                    },
                ));
            }
            command::Attributes::SignalExternalWorkflowExecutionCommandAttributes(signal) => {
                t.add_signal_wf(
                    signal.signal_name,
                    signal.execution.map(|e| e.workflow_id).unwrap_or_default(),
                    "",
                );
            }
            command::Attributes::CompleteWorkflowExecutionCommandAttributes(_) => {
                t.add_workflow_execution_completed();
            }
            unexpected => panic!("Unexpected command: {unexpected:?}"),
        }
    }
}

fn resolved_result(activation: &WorkflowActivation) -> Option<Vec<u8>> {
    activation.jobs.iter().find_map(|j| match &j.variant {
        Some(workflow_activation_job::Variant::ResolveActivity(r)) => {
            match r.result.as_ref()?.status.as_ref()? {
                activity_resolution::Status::Completed(ok) => {
                    Some(ok.result.as_ref()?.data.clone())
                }
                other => panic!("Expected a completed local activity, got {other:?}"),
            }
        }
        _ => None,
    })
}

/// The local activity requested in WFT 8 is still queued, never polled, when its run is evicted.
/// The rebuilt run replays to WFT 13 without a marker, so it requests the same seq again. Only that
/// request may reach lang, and its result is what the run must record. Without the eviction, the
/// request from WFT 8 is the one that runs.
#[rstest::rstest]
#[case::evicted_before_polled(true)]
#[case::kept_in_cache(false)]
#[tokio::test]
async fn local_activity_queued_before_eviction_runs_only_for_rebuilt_run(#[case] evict: bool) {
    let mut history = TestHistoryBuilder::default();
    history.add_wfe_started_with_wft_timeout(Duration::from_secs(1));
    history.add_workflow_task_scheduled_and_started();
    let (tasks, next_tasks) = mpsc::unbounded_channel();
    tasks.send(task(&history, false)).unwrap();
    let (completion_tx, mut completions) = mpsc::unbounded_channel();
    let mut client = mock_worker_client();
    client
        .expect_complete_workflow_task()
        .returning(move |completion, _| {
            completion_tx.send(completion).unwrap();
            Ok(Default::default())
        });
    let mut mock = MocksHolder::from_wft_stream(client, UnboundedReceiverStream::new(next_tasks));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);
    let complete = async |activation: &WorkflowActivation| {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id.clone(),
            workflow(activation),
        ))
        .await
        .unwrap();
    };

    let activation = core.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    complete(&activation).await;
    record_completion(&mut history, &completions.recv().await.unwrap());
    history.add_timer_fired(history.current_event_id(), "1".to_owned());
    history.add_workflow_task_scheduled_and_started();
    // Answering the attached query makes Core report this task without waiting on the local
    // activity, so the run gets past WFT 8 while the activity is never polled.
    tasks.send(task(&history, true)).unwrap();
    complete(&core.poll_workflow_activation().await.unwrap()).await;
    complete(&core.poll_workflow_activation().await.unwrap()).await;
    let completion = completions.recv().await.unwrap();
    assert!(completion.force_create_new_workflow_task);
    assert_eq!(
        completion
            .commands
            .iter()
            .map(|c| c.command_type())
            .collect::<Vec<_>>(),
        [CommandType::SignalExternalWorkflowExecution]
    );
    record_completion(&mut history, &completion);
    let initiated_id = history.current_event_id();
    history.add_workflow_task_scheduled();
    history.add_external_signal_completed(initiated_id);

    if evict {
        core.request_workflow_eviction(&run_id);
        let eviction = core.poll_workflow_activation().await.unwrap();
        assert_eq!(eviction.run_id, run_id);
        assert_eq!(
            eviction.eviction_reason(),
            Some(EvictionReason::LangRequested)
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
            .await
            .unwrap();
    }
    history.add_workflow_task_started();
    assert_eq!(history.current_event_id(), 13);
    tasks.send(task(&history, false)).unwrap();
    let mut rebuilt = false;
    loop {
        let activation = core.poll_workflow_activation().await.unwrap();
        assert_eq!(activation.run_id, run_id);
        rebuilt |= activation.jobs.iter().any(|j| {
            matches!(
                j.variant,
                Some(workflow_activation_job::Variant::InitializeWorkflow(_))
            )
        });
        let acked = activation.jobs.iter().any(|j| {
            matches!(
                j.variant,
                Some(workflow_activation_job::Variant::ResolveSignalExternalWorkflow(_))
            )
        });
        complete(&activation).await;
        if acked {
            break;
        }
    }
    assert_eq!(rebuilt, evict);

    let (requested_in, result) = if evict {
        (13, b"from-WFT13".to_vec())
    } else {
        (8, b"from-WFT8".to_vec())
    };
    let la = core.poll_activity_task().await.unwrap();
    let Some(activity_task::Variant::Start(start)) = &la.variant else {
        panic!("Expected a local activity start, got {la:?}");
    };
    assert_eq!(start.activity_id, "1");
    assert_eq!(start.workflow_execution.as_ref().unwrap().run_id, run_id);
    assert_eq!(
        start.workflow_task.as_ref().unwrap().started_event_id,
        requested_in,
        "local activity dispatched for the wrong workflow task: {la:?}"
    );
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: la.task_token,
        result: Some(ActivityExecutionResult::ok(Payload {
            data: result.clone(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let activation = core.poll_workflow_activation().await.unwrap();
    assert_eq!(resolved_result(&activation), Some(result.clone()));
    complete(&activation).await;
    let completion = completions.recv().await.unwrap();
    let marker = completion
        .commands
        .iter()
        .find_map(|c| match c.attributes.as_ref() {
            Some(command::Attributes::RecordMarkerCommandAttributes(m)) => Some(m),
            _ => None,
        })
        .expect("local activity marker");
    assert_eq!(marker.details["result"].payloads[0].data, result);
    record_completion(&mut history, &completion);
    // Shutdown fails if any other local activity start is still waiting to be polled
    core.drain_pollers_and_shutdown().await;

    let replay = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream::iter([HistoryForReplay::from(history)]),
    ))
    .unwrap();
    let mut replayed_result = None;
    loop {
        let activation = match replay.poll_workflow_activation().await {
            Ok(activation) => activation,
            Err(PollError::ShutDown) => break,
            Err(e) => panic!("Replay poll failed: {e:?}"),
        };
        assert_ne!(
            activation.eviction_reason(),
            Some(EvictionReason::Nondeterminism),
            "Replay diverged: {activation:?}"
        );
        replayed_result = replayed_result.or(resolved_result(&activation));
        replay
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                activation.run_id.clone(),
                workflow(&activation),
            ))
            .await
            .unwrap();
    }
    assert_eq!(replayed_result, Some(result));
}
