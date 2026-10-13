use crate::{
    internal_flags::CoreInternalFlags,
    prost_dur,
    replay::{DEFAULT_ACTIVITY_TYPE, TestHistoryBuilder},
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, build_mock_pollers, hist_to_poll_resp,
        mock_worker, query_ok,
    },
    worker::{Worker, client::mocks::mock_worker_client},
};

use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivation, WorkflowActivationJob, workflow_activation_job},
        workflow_commands::{
            CompleteWorkflowExecution, ScheduleActivity, StartTimer, UpdateResponse,
            update_response::Response,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::Payload,
        enums::v1::{EventType, UpdateAdmittedEventOrigin},
        failure::v1::Failure,
        history::v1::{
            WorkflowExecutionUpdateAcceptedEventAttributes,
            WorkflowExecutionUpdateAdmittedEventAttributes,
        },
        query::v1::WorkflowQuery,
        update::v1::{Acceptance, Input, Meta, Rejection, Request},
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
};

fn add_reapplied_update_admitted(t: &mut TestHistoryBuilder, update_id: &str) -> i64 {
    t.add(WorkflowExecutionUpdateAdmittedEventAttributes {
        request: Some(Request {
            meta: Some(Meta {
                update_id: update_id.to_string(),
                identity: "fake".to_string(),
            }),
            input: Some(Input {
                header: None,
                name: "update".to_string(),
                args: None,
            }),
            ..Default::default()
        }),
        origin: UpdateAdmittedEventOrigin::Reapply as i32,
    })
}

fn chunking_history() -> TestHistoryBuilder {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.set_flags_first_wft(&[CoreInternalFlags::WftChunkingV2], &[]);
    history
}

fn chunking_worker(
    history: TestHistoryBuilder,
    updates: &[(&str, i64)],
    failures: usize,
    query: Option<&str>,
) -> Worker {
    let mut response = hist_to_poll_resp(&history, "chunking", ResponseType::AllHistory);
    for (id, position) in updates {
        response.add_update_request(id, *position);
    }
    if let Some(id) = query {
        response.queries.insert(
            id.to_string(),
            WorkflowQuery {
                query_type: "ready".to_string(),
                ..Default::default()
            },
        );
    }
    let mut polls = MockPollCfg::from_resp_batches(
        "chunking",
        history,
        vec![response.resp; failures + 1],
        mock_worker_client(),
    );
    polls.num_expected_fails = failures;
    if query.is_some() {
        polls.num_expected_completions = Some(1.into());
        polls.completion_mock_fn = Some(Box::new(|completion| {
            assert!(completion.commands.is_empty());
            assert_eq!(completion.query_responses.len(), 1);
            Ok(Default::default())
        }));
    }
    let mut mock = build_mock_pollers(polls);
    mock.worker_cfg(|wc| wc.max_cached_workflows = usize::from(query.is_none()));
    mock_worker(mock)
}

async fn expect_activation(core: &Worker, expected: &[&str]) -> WorkflowActivation {
    let task = core.poll_workflow_activation().await.unwrap();
    let jobs = task
        .jobs
        .iter()
        .map(|job| match job.variant.as_ref().unwrap() {
            workflow_activation_job::Variant::InitializeWorkflow(_) => "initialize".to_string(),
            workflow_activation_job::Variant::SignalWorkflow(signal) => {
                format!("signal:{}", signal.signal_name)
            }
            workflow_activation_job::Variant::DoUpdate(update) => format!("update:{}", update.id),
            workflow_activation_job::Variant::FireTimer(timer) => format!("timer:{}", timer.seq),
            workflow_activation_job::Variant::ResolveActivity(activity) => {
                format!("activity:{}", activity.seq)
            }
            workflow_activation_job::Variant::QueryWorkflow(query) => {
                format!("query:{}", query.query_id)
            }
            workflow_activation_job::Variant::RemoveFromCache(_) => "eviction".to_string(),
            other => panic!("unexpected activation job: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(jobs, expected);
    task
}

async fn complete_activation(core: &Worker, jobs: &[&str]) -> WorkflowActivation {
    let task = expect_activation(core, jobs).await;
    complete_empty(core, task.run_id.clone()).await;
    task
}

async fn complete_empty(core: &Worker, run_id: String) {
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(run_id))
        .await
        .unwrap();
}

#[derive(Clone, Copy, Debug)]
enum FollowupTask {
    LiveUpdate,
    AdmittedUpdate,
    Query,
}

#[tokio::test]
async fn replay_with_empty_first_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task();
    let accept_id = t.add_update_accepted("upd1", "update");
    t.add_we_signaled("hi", vec![]);
    t.add_full_wf_task();
    t.add_update_completed(accept_id);
    t.add_workflow_execution_completed();

    let mock = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    // In this task imagine we are waiting on the first update being sent, hence no commands come
    // out, and on replay the first activation should only be init.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: "upd1".to_string(),
            response: Some(Response::Accepted(())),
        }
        .into(),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Completed(Payload::default())),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn initial_request_sent_back(#[values(false, true)] reject: bool) {
    let wfid = "fakeid";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    let upd_req_body = poll_resp.add_update_request(update_id, 1);

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(move |mut resp, _| {
            let msg = resp.messages.pop().unwrap();
            let orig_req = if reject {
                let acceptance = msg.body.unwrap().to_msg::<Rejection>().unwrap();
                acceptance.rejected_request.unwrap()
            } else {
                let acceptance = msg.body.unwrap().to_msg::<Acceptance>().unwrap();
                acceptance.accepted_request.unwrap()
            };
            assert_eq!(orig_req, upd_req_body);
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });
    let mh = MockPollCfg::from_resp_batches(wfid, t, [poll_resp], mock_client);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    let resp = if reject {
        Response::Rejected(Default::default())
    } else {
        Response::Accepted(())
    };
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: update_id.to_string(),
            response: Some(resp),
        }
        .into(),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn speculative_wft_with_command_event() {
    let wfid = "fakeid";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_activity_task_scheduled("act1");

    let mut spec_task_hist = t.clone();
    spec_task_hist.add_workflow_task_scheduled_and_started();

    let mut real_hist = t.clone();
    real_hist.add_we_signaled("hi", vec![]);
    let later_update_sequencing_event_id = real_hist.current_event_id();
    real_hist.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let later_update_id = "upd-2";
    let mut speculative_task = hist_to_poll_resp(&spec_task_hist, wfid, ResponseType::OneTask(2));
    speculative_task.add_update_request(update_id, 1);
    let mut real_task = hist_to_poll_resp(&real_hist, wfid, ResponseType::OneTask(3));
    real_task.add_update_request(later_update_id, later_update_sequencing_event_id);
    // Verify the speculative task contains the activity scheduled event
    assert_eq!(
        speculative_task.history.as_ref().unwrap().events[1].event_type,
        EventType::ActivityTaskScheduled as i32
    );

    let mock_client = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wfid,
        real_hist,
        [
            ResponseType::ToTaskNum(1),
            speculative_task.into(),
            real_task.into(),
        ],
        mock_client,
    );
    let mut completes = 0;
    mh.completion_mock_fn = Some(Box::new(move |_| {
        completes += 1;
        let mut r = RespondWorkflowTaskCompletedResponse::default();
        if completes == 2 {
            // The second response (the update rejection) needs to indicate that the last started
            // wft ID should be reset.
            r.reset_history_event_id = 3;
        }
        Ok(r)
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleActivity {
            activity_id: "act1".to_string(),
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    // Receive the task containing and reject the update
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: update_id.to_string(),
            response: Some(Response::Rejected(Default::default())),
        }
        .into(),
    ))
    .await
    .unwrap();

    // Now we'll get another task with the "real" history containing the signal
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(signal)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(update)),
            },
        ] if signal.signal_name == "hi" && update.id == later_update_id
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: later_update_id.to_string(),
                response: Some(Response::Rejected(Default::default())),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn replay_with_signal_and_update_same_task() {
    // Imitating a signal creating a command before update validator runs
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("hi", vec![]);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    let accept_id = t.add_update_accepted("upd1", "update");
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_update_completed(accept_id);
    t.add_workflow_execution_completed();

    let mock = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    // In this task imagine we are waiting on the first update being sent, hence no commands come
    // out, and on replay the first activation should only be init.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
            }
        ]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
            }
            .into(),
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Accepted(())),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Completed(Payload::default())),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn repeated_admitted_update_replays_once_before_accepted() {
    let update_id = "reapplied-update";
    let mut t = chunking_history();
    t.add_full_wf_task();
    add_reapplied_update_admitted(&mut t, update_id);
    let latest_admitted_event_id = add_reapplied_update_admitted(&mut t, update_id);
    t.add_full_wf_task();
    t.add(WorkflowExecutionUpdateAcceptedEventAttributes {
        protocol_instance_id: update_id.to_string(),
        accepted_request_message_id: format!("{update_id}/request"),
        accepted_request_sequencing_event_id: latest_admitted_event_id,
        accepted_request: None,
    });
    let core = chunking_worker(t, &[], 0, None);
    complete_activation(&core, &["initialize"]).await;
    expect_activation(&core, &["update:reapplied-update"]).await;
}

#[rstest::rstest]
#[case::adjacent_live(0, 1, FollowupTask::LiveUpdate)]
#[case::live_after_one_signal(1, 1, FollowupTask::LiveUpdate)]
#[case::two_live_same_boundary(0, 2, FollowupTask::LiveUpdate)]
#[case::adjacent_admitted(0, 1, FollowupTask::AdmittedUpdate)]
#[case::admitted_after_one_signal(1, 1, FollowupTask::AdmittedUpdate)]
#[case::two_admitted_same_boundary(0, 2, FollowupTask::AdmittedUpdate)]
#[case::query_after_empty_wft(0, 0, FollowupTask::Query)]
#[tokio::test]
async fn commandless_activity_resolution_replays_before_followup_task(
    #[case] intervening_signal_count: usize,
    #[case] update_count: usize,
    #[case] followup: FollowupTask,
) {
    let update_ids = (0..update_count)
        .map(|index| format!("upd-{index}"))
        .collect::<Vec<_>>();
    let mut t = chunking_history();
    let timer_started_event_id = t.add_timer_started("1".to_string());
    t.add_we_signaled("process", vec![]);
    t.add_full_wf_task();
    let activity_scheduled_event_id = t.add_activity_task_scheduled("act1");

    // Advance the replay boundary past ActivityTaskScheduled so the WFT which processes the
    // activity completion has no command events and is eligible for empty-WFT folding.
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    let activity_started_event_id = t.add_activity_task_started(activity_scheduled_event_id);
    t.add_activity_task_completed(
        activity_scheduled_event_id,
        activity_started_event_id,
        Payload::default(),
    );

    // This WFT represents the language resuming its activity waiter and making an in-memory-only
    // state change. Its completion therefore adds no command event to history.
    t.add_workflow_task_scheduled_and_started();
    let activity_resolution_history_length = t.current_event_id() as u32;
    t.add_workflow_task_completed();
    t.add_workflow_task_scheduled();
    for index in 0..intervening_signal_count {
        t.add_we_signaled(&format!("before-update-{index}"), vec![]);
    }
    let position = t.current_event_id();
    if matches!(followup, FollowupTask::AdmittedUpdate) {
        for id in &update_ids {
            add_reapplied_update_admitted(&mut t, id);
        }
    }
    t.add_workflow_task_started();
    let final_history_length = t.current_event_id() as u32;
    let updates = if matches!(followup, FollowupTask::LiveUpdate) {
        update_ids
            .iter()
            .map(|id| (id.as_str(), position))
            .collect()
    } else {
        vec![]
    };
    let query = matches!(followup, FollowupTask::Query).then_some("ready");
    let core = chunking_worker(t, &updates, 0, query);

    let task = expect_activation(&core, &["initialize"]).await;
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        StartTimer {
            seq: 1,
            start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
        }
        .into(),
    ))
    .await
    .unwrap();
    let task = expect_activation(&core, &["signal:process"]).await;
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleActivity {
            seq: 1,
            activity_id: "act1".to_string(),
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    complete_activation(&core, &["timer:1"]).await;
    let task = complete_activation(&core, &["activity:1"]).await;
    assert!(task.is_replaying);
    assert_eq!(task.history_length, activity_resolution_history_length);

    if let Some(query_id) = query {
        // #1606 has no Update sequencing evidence: a buffered query creates the empty task
        // which used to make the preceding activity resolution run again as non-replay.
        let task = expect_activation(&core, &["query:ready"]).await;
        assert!(task.is_replaying);
        assert_eq!(task.history_length, final_history_length);
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            query_ok(query_id, "true"),
        ))
        .await
        .unwrap();
        core.shutdown().await;
        return;
    }

    let expected = (0..intervening_signal_count)
        .map(|index| format!("signal:before-update-{index}"))
        .chain(update_ids.iter().map(|id| format!("update:{id}")))
        .collect::<Vec<_>>();
    let task = expect_activation(
        &core,
        &expected.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await;
    assert!(!task.is_replaying);
    assert_eq!(task.history_length, final_history_length);
}

#[tokio::test]
async fn failed_historical_activation_withholds_live_update_until_retry_completes() {
    let update_id = "live-after-retry";
    let mut t = chunking_history();
    t.add_we_signaled("historical-boundary", vec![]);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled();
    let position = t.current_event_id();
    t.add_workflow_task_started();
    let core = chunking_worker(t, &[(update_id, position)], 1, None);

    complete_activation(&core, &["initialize"]).await;
    let task = expect_activation(&core, &["signal:historical-boundary"]).await;
    assert!(task.is_replaying);
    core.complete_workflow_activation(WorkflowActivationCompletion::fail(
        task.run_id,
        Failure {
            message: "fail historical catch-up once".to_string(),
            ..Default::default()
        },
        None,
    ))
    .await
    .unwrap();
    let task = complete_activation(&core, &["eviction"]).await;
    assert!(task.is_only_eviction());
    complete_activation(&core, &["initialize"]).await;
    let task = complete_activation(&core, &["signal:historical-boundary"]).await;
    assert!(task.is_replaying);
    let task = expect_activation(&core, &["update:live-after-retry"]).await;
    assert!(!task.is_replaying);
}

#[tokio::test]
async fn live_updates_at_distinct_positions_wait_for_each_boundary() {
    let mut t = chunking_history();
    let mut updates = vec![];
    let mut boundaries = vec![];
    for (signal, update) in [
        ("first-boundary", "first-live-update"),
        ("second-boundary", "second-live-update"),
    ] {
        t.add_we_signaled(signal, vec![]);
        t.add_full_wf_task();
        let signal_boundary = t.current_event_id() as u32 - 1;
        t.add_workflow_task_scheduled();
        updates.push((update, t.current_event_id()));
        t.add_workflow_task_started();
        boundaries.push((signal_boundary, t.current_event_id() as u32));
        if updates.len() == 1 {
            t.add_workflow_task_completed();
        }
    }
    let core = chunking_worker(t, &updates, 0, None);
    complete_activation(&core, &["initialize"]).await;
    for ((signal, update), (signal_boundary, update_boundary)) in [
        ("first-boundary", "first-live-update"),
        ("second-boundary", "second-live-update"),
    ]
    .into_iter()
    .zip(boundaries)
    {
        let task = complete_activation(&core, &[&format!("signal:{signal}")]).await;
        assert_eq!(task.history_length, signal_boundary);
        let task = expect_activation(&core, &[&format!("update:{update}")]).await;
        assert_eq!(task.history_length, update_boundary);
        if update == "first-live-update" {
            core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                UpdateResponse {
                    protocol_instance_id: update.to_string(),
                    response: Some(Response::Rejected(Default::default())),
                }
                .into(),
            ))
            .await
            .unwrap();
        }
    }
}

#[tokio::test]
async fn historical_accepted_then_later_live_update_preserve_boundaries() {
    let historical_update_id = "historical-accepted";
    let live_update_id = "later-live";
    let mut t = chunking_history();
    t.add_full_wf_task();
    t.add_update_accepted(historical_update_id, "update");
    t.add_we_signaled("after-historical-update", vec![]);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled();
    let position = t.current_event_id();
    t.add_workflow_task_started();
    let history_length = t.current_event_id() as u32;
    let core = chunking_worker(t, &[(live_update_id, position)], 0, None);

    complete_activation(&core, &["initialize"]).await;
    let task = expect_activation(&core, &["update:historical-accepted"]).await;
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: historical_update_id.to_string(),
            response: Some(Response::Accepted(())),
        }
        .into(),
    ))
    .await
    .unwrap();
    complete_activation(&core, &["signal:after-historical-update"]).await;
    let task = expect_activation(&core, &["update:later-live"]).await;
    assert_eq!(task.history_length, history_length);
}

#[tokio::test]
async fn historical_admitted_and_later_live_update_preserve_protocol_order() {
    let mut t = chunking_history();
    t.add_full_wf_task();
    t.add_workflow_task_scheduled();
    add_reapplied_update_admitted(&mut t, "historical-admitted");
    t.add_we_signaled("between-sources", vec![]);
    let position = t.current_event_id();
    t.add_workflow_task_started();
    let core = chunking_worker(t, &[("later-live", position)], 0, None);
    complete_activation(&core, &["initialize"]).await;
    expect_activation(
        &core,
        &[
            "update:historical-admitted",
            "signal:between-sources",
            "update:later-live",
        ],
    )
    .await;
}

#[tokio::test]
async fn update_activation_has_update_id() {
    let wfid = "fakeid";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    poll_resp.add_update_request(update_id, 1);

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    let mh = MockPollCfg::from_resp_batches(wfid, t, [poll_resp], mock_client);
    let core = mock_worker(build_mock_pollers(mh));

    let task = core.poll_workflow_activation().await.unwrap();
    let update = task
        .jobs
        .iter()
        .find_map(|job| match job.variant.as_ref() {
            Some(workflow_activation_job::Variant::DoUpdate(update)) => Some(update),
            _ => None,
        })
        .expect("activation should contain an update");
    assert_eq!(update.id, update_id);

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: update_id.to_string(),
            response: Some(Response::Accepted(())),
        }
        .into(),
    ))
    .await
    .unwrap();
}
