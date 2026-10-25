use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers, build_mock_pollers,
        hist_to_poll_resp, mock_worker, schedule_local_activity_cmd, single_hist_mock_sg,
        start_timer_cmd,
    },
    worker::client::mocks::mock_worker_client,
};
use std::time::{Duration, SystemTime};
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        activity_task::{ActivityTask, Start, WorkflowTaskAttempt, activity_task},
        workflow_activation::WorkflowActivation,
        workflow_commands::{ActivityCancellationType, ScheduleLocalActivity, workflow_command},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::RetryPolicy,
        enums::v1::{EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::history_event::Attributes,
        workflowservice::v1::GetWorkflowExecutionHistoryResponse,
    },
};

async fn complete(
    worker: &crate::Worker,
    activation: &WorkflowActivation,
    commands: Vec<workflow_command::Variant>,
) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id.clone(),
            commands,
        ))
        .await
        .unwrap();
}

fn la_cmd(seq: u32) -> workflow_command::Variant {
    schedule_local_activity_cmd(
        seq,
        &seq.to_string(),
        ActivityCancellationType::TryCancel,
        Duration::from_secs(60),
    )
}

fn start_of(task: &ActivityTask) -> &Start {
    match &task.variant {
        Some(activity_task::Variant::Start(start)) => start,
        other => panic!("Expected an activity start, got {other:?}"),
    }
}

fn dispatching(start: &Start) -> (i64, u32) {
    let task = start
        .workflow_task
        .as_ref()
        .expect("a local activity names its task");
    (task.started_event_id, task.attempt)
}

async fn finish(worker: crate::Worker, task: ActivityTask) {
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&resolved.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

/// The time of a history event, in the form of the poll response's started time.
fn event_time(t: &TestHistoryBuilder, event_id: i64) -> SystemTime {
    t.get_full_history_info().unwrap().events()[(event_id - 1) as usize]
        .event_time
        .unwrap()
        .try_into()
        .unwrap()
}

#[tokio::test]
async fn a_local_activity_names_the_live_task_that_dispatched_it() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    // The first task completed on its third attempt. History has no event of the first two.
    t.modify_event(2, |event| {
        if let Some(Attributes::WorkflowTaskScheduledEventAttributes(a)) = &mut event.attributes {
            a.attempt = 3;
        }
    });
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let started_time: SystemTime = t
        .get_full_history_info()
        .unwrap()
        .as_poll_wft_response()
        .started_time
        .unwrap()
        .try_into()
        .unwrap();
    let completed_time = event_time(&t, 3);
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    complete(&worker, &fired, vec![la_cmd(1)]).await;

    let la = worker.poll_activity_task().await.unwrap();
    let start = start_of(&la);
    assert!(start.is_local);
    assert_eq!(
        start.workflow_task,
        Some(WorkflowTaskAttempt {
            started_event_id: 8,
            attempt: 1,
            started_time: Some(started_time.into()),
            previous_started_event_id: 3,
            failed_tasks: vec![],
            previous_attempt: 3,
            previous_started_time: Some(completed_time.into()),
        })
    );
    finish(worker, la).await;
}

#[tokio::test]
async fn a_retry_that_core_runs_keeps_the_dispatching_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![
            ScheduleLocalActivity {
                seq: 1,
                activity_id: "1".to_string(),
                activity_type: "act".to_string(),
                start_to_close_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
                retry_policy: Some(RetryPolicy {
                    initial_interval: Some(Duration::from_millis(10).try_into().unwrap()),
                    maximum_attempts: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }
            .into(),
        ],
    )
    .await;

    let first_attempt = worker.poll_activity_task().await.unwrap();
    assert_eq!(start_of(&first_attempt).attempt, 1);
    assert_eq!(dispatching(start_of(&first_attempt)), (3, 1));
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: first_attempt.task_token,
            result: Some(ActivityExecutionResult::fail(Failure::application_failure(
                "retry me".to_string(),
                false,
            ))),
        })
        .await
        .unwrap();
    let second_attempt = worker.poll_activity_task().await.unwrap();
    assert_eq!(start_of(&second_attempt).attempt, 2);
    assert_eq!(dispatching(start_of(&second_attempt)), (3, 1));
    finish(worker, second_attempt).await;
}

#[tokio::test]
async fn local_activities_of_one_activation_name_the_same_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![la_cmd(1), la_cmd(2)]).await;

    let one = worker.poll_activity_task().await.unwrap();
    let two = worker.poll_activity_task().await.unwrap();
    assert_eq!(dispatching(start_of(&one)), (3, 1));
    assert_eq!(start_of(&one).workflow_task, start_of(&two).workflow_task);
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: one.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &resolved, vec![]).await;
    finish(worker, two).await;
}

#[tokio::test]
async fn failed_tasks_from_every_history_page_are_named() {
    let wf_id = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(60));
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Failure::default(),
    );
    t.add_full_wf_task();
    let timer_started = t.add_by_type(EventType::TimerStarted);
    let second_page_end = timer_started as usize;
    t.add_timer_fired(timer_started, "2".to_string());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_and_started();

    // The poll response has the first page and each history fetch adds one page. The failed task
    // is on the second page and the timed out task is on the last.
    let mut first_page = hist_to_poll_resp(&t, wf_id, ResponseType::AllHistory).resp;
    first_page.history.as_mut().unwrap().events.truncate(6);
    first_page.next_page_token = vec![1];
    let mut second_page: GetWorkflowExecutionHistoryResponse =
        t.get_full_history_info().unwrap().into();
    second_page
        .history
        .as_mut()
        .unwrap()
        .events
        .truncate(second_page_end);
    second_page.next_page_token = vec![2];
    let last_page: GetWorkflowExecutionHistoryResponse = t.get_full_history_info().unwrap().into();
    let completed_time = event_time(&t, 11);
    let mut client = mock_worker_client();
    client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| Ok(second_page.clone()))
        .times(1);
    client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| Ok(last_page.clone()))
        .times(1);
    let mut mock = single_hist_mock_sg(wf_id, t, [ResponseType::Raw(first_page)], client, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(fired.is_replaying);
    complete(
        &worker,
        &fired,
        vec![start_timer_cmd(2, Duration::from_secs(1))],
    )
    .await;
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    complete(&worker, &fired, vec![la_cmd(1)]).await;

    let la = worker.poll_activity_task().await.unwrap();
    let task = start_of(&la).workflow_task.as_ref().unwrap();
    assert_eq!(
        (task.started_event_id, task.previous_started_event_id),
        (19, 11)
    );
    assert_eq!(task.failed_tasks, [8, 16]);
    assert_eq!(task.previous_started_time, Some(completed_time.into()));
    finish(worker, la).await;
}
