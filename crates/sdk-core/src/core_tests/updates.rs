use crate::{
    prost_dur,
    replay::{DEFAULT_ACTIVITY_TYPE, TestHistoryBuilder},
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, build_mock_pollers, hist_to_poll_resp,
        mock_worker,
    },
    worker::{Worker, client::mocks::mock_worker_client},
};

use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
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

async fn poll_and_complete_empty_initialization(core: &Worker) {
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
    complete_empty(core, task.run_id).await;
}

async fn complete_empty(core: &Worker, run_id: String) {
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(run_id))
        .await
        .unwrap();
}

#[derive(Clone, Copy, Debug)]
enum UpdateEvidence {
    Live,
    Admitted,
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
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
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

    let mock = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    poll_and_complete_empty_initialization(&core).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(update)),
        }] if update.id == update_id
    );
}

#[rstest::rstest]
#[case::adjacent_live(0, 1, UpdateEvidence::Live)]
#[case::live_after_one_signal(1, 1, UpdateEvidence::Live)]
#[case::two_live_same_boundary(0, 2, UpdateEvidence::Live)]
#[case::adjacent_admitted(0, 1, UpdateEvidence::Admitted)]
#[case::admitted_after_one_signal(1, 1, UpdateEvidence::Admitted)]
#[case::two_admitted_same_boundary(0, 2, UpdateEvidence::Admitted)]
#[tokio::test]
async fn update_after_commandless_activity_resolution_wft_is_separate(
    #[case] intervening_signal_count: usize,
    #[case] update_count: usize,
    #[case] evidence: UpdateEvidence,
) {
    let wfid = format!("commandless-{intervening_signal_count}-{update_count}-{evidence:?}");
    let update_ids = (0..update_count)
        .map(|index| format!("upd-{index}"))
        .collect::<Vec<_>>();
    let mut t = TestHistoryBuilder::default();

    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
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
    t.add_workflow_task_scheduled();
    t.add_workflow_task_started();
    let activity_resolution_history_length = t.current_event_id() as u32;
    t.add_workflow_task_completed();

    t.add_workflow_task_scheduled();
    for signal_index in 0..intervening_signal_count {
        let signal_name = format!("before-update-{signal_index}");
        t.add_we_signaled(&signal_name, vec![]);
    }
    let update_sequencing_event_id = t.current_event_id();
    if matches!(evidence, UpdateEvidence::Admitted) {
        for update_id in &update_ids {
            add_reapplied_update_admitted(&mut t, update_id);
        }
    }
    t.add_workflow_task_started();
    let final_history_length = t.current_event_id() as u32;

    let mut poll_resp = hist_to_poll_resp(&t, &wfid, ResponseType::AllHistory);
    if matches!(evidence, UpdateEvidence::Live) {
        for update_id in &update_ids {
            poll_resp.add_update_request(update_id, update_sequencing_event_id);
        }
    }

    let mh = MockPollCfg::from_resp_batches(&wfid, t, [poll_resp], mock_worker_client());
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
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

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
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

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    complete_empty(&core, task.run_id).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_eq!(task.history_length, activity_resolution_history_length);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(activity)),
        }] if activity.seq == 1
    );
    complete_empty(&core, task.run_id).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
    assert_eq!(task.history_length, final_history_length);
    let observed_jobs = task
        .jobs
        .iter()
        .map(|job| match job.variant.as_ref() {
            Some(workflow_activation_job::Variant::SignalWorkflow(signal)) => {
                format!("signal:{}", signal.signal_name)
            }
            Some(workflow_activation_job::Variant::DoUpdate(update)) => {
                format!("update:{}", update.id)
            }
            other => panic!("unexpected job after commandless boundary: {other:?}"),
        })
        .collect::<Vec<_>>();
    let expected_jobs = (0..intervening_signal_count)
        .map(|index| format!("signal:before-update-{index}"))
        .chain(
            update_ids
                .iter()
                .map(|update_id| format!("update:{update_id}")),
        )
        .collect::<Vec<_>>();
    assert_eq!(observed_jobs, expected_jobs);
}

#[tokio::test]
async fn failed_historical_activation_withholds_live_update_until_retry_completes() {
    let wfid = "failed-historical-activation-retry";
    let update_id = "live-after-retry";
    let mut t = TestHistoryBuilder::default();

    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("historical-boundary", vec![]);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled();
    let update_sequencing_event_id = t.current_event_id();
    t.add_workflow_task_started();

    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    poll_resp.add_update_request(update_id, update_sequencing_event_id);
    let poll_resp = poll_resp.resp;

    let mut mh = MockPollCfg::from_resp_batches(
        wfid,
        t,
        [poll_resp.clone(), poll_resp],
        mock_worker_client(),
    );
    mh.num_expected_fails = 1;
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    poll_and_complete_empty_initialization(&core).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(signal)),
        }] if signal.signal_name == "historical-boundary"
    );
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

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_only_eviction());
    complete_empty(&core, task.run_id).await;

    poll_and_complete_empty_initialization(&core).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(signal)),
        }] if signal.signal_name == "historical-boundary"
    );
    complete_empty(&core, task.run_id).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(update)),
        }] if update.id == update_id
    );
}

#[tokio::test]
async fn live_updates_at_distinct_positions_wait_for_each_boundary() {
    let wfid = "live-updates-at-distinct-positions";
    let first_update_id = "first-live-update";
    let second_update_id = "second-live-update";
    let mut t = TestHistoryBuilder::default();

    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    t.add_we_signaled("first-boundary", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let first_boundary_history_length = t.current_event_id() as u32;
    t.add_workflow_task_completed();

    t.add_workflow_task_scheduled();
    let first_update_sequencing_event_id = t.current_event_id();
    t.add_workflow_task_started();
    let first_update_history_length = t.current_event_id() as u32;
    t.add_workflow_task_completed();

    t.add_we_signaled("second-boundary", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let second_boundary_history_length = t.current_event_id() as u32;
    t.add_workflow_task_completed();

    t.add_workflow_task_scheduled();
    let second_update_sequencing_event_id = t.current_event_id();
    t.add_workflow_task_started();
    let second_update_history_length = t.current_event_id() as u32;

    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    poll_resp.add_update_request(first_update_id, first_update_sequencing_event_id);
    poll_resp.add_update_request(second_update_id, second_update_sequencing_event_id);

    let mh = MockPollCfg::from_resp_batches(wfid, t, [poll_resp], mock_worker_client());
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    poll_and_complete_empty_initialization(&core).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(task.history_length, first_boundary_history_length);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(signal)),
        }] if signal.signal_name == "first-boundary"
    );
    complete_empty(&core, task.run_id).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(task.history_length, first_update_history_length);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(update)),
        }] if update.id == first_update_id
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: first_update_id.to_string(),
            response: Some(Response::Rejected(Default::default())),
        }
        .into(),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(task.history_length, second_boundary_history_length);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(signal)),
        }] if signal.signal_name == "second-boundary"
    );
    complete_empty(&core, task.run_id).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(task.history_length, second_update_history_length);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(update)),
        }] if update.id == second_update_id
    );
}

#[tokio::test]
async fn historical_accepted_then_later_live_update_preserve_boundaries() {
    let wfid = "historical-accepted-then-live";
    let historical_update_id = "historical-accepted";
    let live_update_id = "later-live";
    let mut t = TestHistoryBuilder::default();

    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_update_accepted(historical_update_id, "update");

    t.add_we_signaled("after-historical-update", vec![]);
    t.add_full_wf_task();

    t.add_workflow_task_scheduled();
    let live_update_sequencing_event_id = t.current_event_id();
    t.add_workflow_task_started();
    let live_update_history_length = t.current_event_id() as u32;

    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    poll_resp.add_update_request(live_update_id, live_update_sequencing_event_id);

    let mh = MockPollCfg::from_resp_batches(wfid, t, [poll_resp], mock_worker_client());
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    poll_and_complete_empty_initialization(&core).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(update)),
        }] if update.id == historical_update_id
    );
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

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(signal)),
        }] if signal.signal_name == "after-historical-update"
    );
    complete_empty(&core, task.run_id).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(task.history_length, live_update_history_length);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(update)),
        }] if update.id == live_update_id
    );
}

#[tokio::test]
async fn historical_admitted_and_later_live_update_preserve_protocol_order() {
    let wfid = "historical-admitted-and-live";
    let admitted_update_id = "historical-admitted";
    let live_update_id = "later-live";
    let mut t = TestHistoryBuilder::default();

    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_workflow_task_scheduled();
    add_reapplied_update_admitted(&mut t, admitted_update_id);
    t.add_we_signaled("between-sources", vec![]);
    let live_update_sequencing_event_id = t.current_event_id();
    t.add_workflow_task_started();

    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    poll_resp.add_update_request(live_update_id, live_update_sequencing_event_id);

    let mh = MockPollCfg::from_resp_batches(wfid, t, [poll_resp], mock_worker_client());
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    poll_and_complete_empty_initialization(&core).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(admitted)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(signal)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(live)),
            },
        ] if admitted.id == admitted_update_id
            && signal.signal_name == "between-sources"
            && live.id == live_update_id
    );
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
