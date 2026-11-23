use crate::{
    PollError, Worker, init_replay_worker,
    internal_flags::CoreInternalFlags,
    prost_dur,
    replay::{DEFAULT_ACTIVITY_TYPE, HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, WorkerExt, WorkerTestHelpers,
        build_mock_pollers, hist_to_poll_resp, mock_worker, test_worker_cfg,
    },
    worker::client::mocks::mock_worker_client,
};
use futures_util::stream;
use parking_lot::Mutex;
use std::sync::Arc;
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{
            WorkflowActivation, WorkflowActivationJob, remove_from_cache::EvictionReason,
            workflow_activation_job,
        },
        workflow_commands::{
            CompleteWorkflowExecution, ScheduleActivity, SetPatchMarker, StartTimer,
            UpdateResponse, update_response::Response, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::{Command, command},
        common::v1::Payload,
        enums::v1::{CommandType, EventType, TimeoutType},
        history::v1::history_event::Attributes,
        nexus::v1::PropagatedSerializationContext,
        sdk::v1::WorkflowTaskCompletedMetadata,
        update::v1::{Acceptance, Rejection},
        workflowservice::v1::{
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    utilities::pack_any,
};

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
    real_hist.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let mut speculative_task = hist_to_poll_resp(&spec_task_hist, wfid, ResponseType::OneTask(2));
    speculative_task.add_update_request(update_id, 1);
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
            ResponseType::OneTask(3),
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
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_execution(&task.run_id).await;
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

#[tokio::test]
async fn update_activation_has_propagated_nexus_serialization_context() {
    let wfid = "fakeid";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let ctx = PropagatedSerializationContext {
        endpoint: "my-endpoint".to_string(),
        service: "my-service".to_string(),
        operation: "my-operation".to_string(),
    };
    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    let mut request = poll_resp.add_update_request(update_id, 1);
    request.propagated_nexus_serialization_context = Some(ctx.clone());
    poll_resp.messages.last_mut().unwrap().body = Some(
        pack_any(
            "type.googleapis.com/temporal.api.update.v1.Request".to_string(),
            &request,
        )
        .unwrap(),
    );

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
    assert_eq!(update.propagated_nexus_serialization_context, Some(ctx));

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

const REJECTED_UPDATE_ID: &str = "upd-reject";
const ACCEPTED_UPDATE_ID: &str = "upd-accept";

type CapturedCompletion = (Vec<Command>, WorkflowTaskCompletedMetadata);

/// Builds the tasks the server hands out for a workflow started with a start delay that receives
/// updates during the delay. The first task is speculative and carries an update the worker
/// rejects; the server drops that completion without a `WorkflowTaskCompleted` event or a
/// `reset_history_event_id`. The next task follows the dropped one's schedule-to-start timeout and
/// carries an update the worker accepts. Returns the persisted history alongside both tasks.
fn discarded_first_task_rejection_tasks() -> (TestHistoryBuilder, [PollWorkflowTaskQueueResponse; 2])
{
    let mut base = TestHistoryBuilder::default();
    base.add_by_type(EventType::WorkflowExecutionStarted);

    let mut speculative = base.clone();
    speculative.add_workflow_task_scheduled_and_started();
    let mut speculative_task =
        hist_to_poll_resp(&speculative, "fakeid", ResponseType::AllHistory).resp;
    speculative_task.add_update_request(REJECTED_UPDATE_ID, 2);

    let mut persisted = base;
    persisted.add_workflow_task_scheduled();
    persisted.add_workflow_task_timed_out();
    if let Some(Attributes::WorkflowTaskTimedOutEventAttributes(a)) =
        persisted.last_event().and_then(|e| e.attributes.as_mut())
    {
        a.set_timeout_type(TimeoutType::ScheduleToStart);
    }
    persisted.add_workflow_task_scheduled_and_started();
    let mut accept_task = hist_to_poll_resp(&persisted, "fakeid", ResponseType::AllHistory).resp;
    accept_task.add_update_request(ACCEPTED_UPDATE_ID, 4);

    (persisted, [speculative_task, accept_task])
}

fn cached_worker_capturing_completions(
    persisted: &TestHistoryBuilder,
    tasks: [PollWorkflowTaskQueueResponse; 2],
) -> (Worker, Arc<Mutex<Vec<CapturedCompletion>>>) {
    let completions = Arc::new(Mutex::new(vec![]));
    let completions_c = completions.clone();
    let mut mh =
        MockPollCfg::from_resp_batches("fakeid", persisted.clone(), tasks, mock_worker_client());
    mh.completion_mock_fn = Some(Box::new(move |c| {
        completions_c
            .lock()
            .push((c.commands.clone(), c.sdk_metadata.clone()));
        Ok(RespondWorkflowTaskCompletedResponse::default())
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    (mock_worker(mock), completions)
}

/// Core evicts the run after its first task only rejected updates, since the server may have
/// discarded that completion. The accept task then starts the run over.
async fn evicted_then_reinitialized(core: &Worker) -> WorkflowActivation {
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert!(!act.is_replaying);
    assert_matches!(
        act.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
            }
        ]
    );
    act
}

/// Returns the dropped rejection completion and the persisted update completion
fn rejection_and_update_completions(
    completions: &Mutex<Vec<CapturedCompletion>>,
) -> (CapturedCompletion, CapturedCompletion) {
    let completions = completions.lock().clone();
    assert_eq!(completions.len(), 2);
    // The server may only drop a task that could not have changed workflow state
    assert!(completions[0].0.is_empty());
    (completions[0].clone(), completions[1].clone())
}

/// What a replay of the persisted history did
struct ReplayOutcome {
    /// Replay activations that delivered the accepted update
    update_activations: Vec<WorkflowActivation>,
    /// Every eviction, with how many accepted-update activations preceded it
    evictions: Vec<(EvictionReason, String, usize)>,
}

impl ReplayOutcome {
    /// Asserts that replay delivered the accepted update exactly once, then ran to the end of
    /// history. The replay worker evicts a run only after it stops replaying, so a single
    /// `LangRequested` eviction after the update is its routine cleanup. Any other eviction
    /// (nondeterminism, lang failure, fatal, ...) means replay failed.
    fn assert_replayed_update(&self, context: &str) {
        assert_eq!(
            self.update_activations.len(),
            1,
            "Replay must deliver the accepted update exactly once. {context}"
        );
        assert!(
            self.update_activations[0].is_replaying,
            "The accepted update must be delivered while replaying. {context}"
        );
        assert_matches!(
            self.evictions.as_slice(),
            [(EvictionReason::LangRequested, _, 1)],
            "Replay must finish with only the replay worker's routine eviction, after the update. \
             Evictions: {:?}. {context}",
            self.evictions
        );
    }
}

/// Appends what the server persists from the update completion and replays the result on a fresh
/// worker, recording the accepted-update activations and all evictions. `respond` produces lang's
/// commands for the replay activation that delivers the accepted update.
async fn replay_persisted_update_completion(
    mut history: TestHistoryBuilder,
    (commands, metadata): &CapturedCompletion,
    patch_id: &str,
    mut respond: impl FnMut(&WorkflowActivation) -> Vec<workflow_command::Variant>,
) -> ReplayOutcome {
    history.add_workflow_task_completed();
    if let Some(Attributes::WorkflowTaskCompletedEventAttributes(a)) =
        history.last_event().and_then(|e| e.attributes.as_mut())
    {
        a.sdk_metadata = Some(metadata.clone());
    }
    let mut accepted_event_id = None;
    for cmd in commands {
        match cmd.command_type() {
            CommandType::ProtocolMessage => match accepted_event_id {
                None => {
                    accepted_event_id =
                        Some(history.add_update_accepted(ACCEPTED_UPDATE_ID, "update"))
                }
                Some(id) => history.add_update_completed(id),
            },
            CommandType::RecordMarker => history.add_has_change_marker(patch_id, false),
            CommandType::UpsertWorkflowSearchAttributes => {
                history.add_upsert_search_attrs_for_patch(&[patch_id.to_string()])
            }
            other => panic!("Unexpected command in update completion: {other:?}"),
        }
    }

    let replay_worker = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream::iter([HistoryForReplay::from(history)]),
    ))
    .unwrap();
    let mut outcome = ReplayOutcome {
        update_activations: vec![],
        evictions: vec![],
    };
    loop {
        let act = match replay_worker.poll_workflow_activation().await {
            Ok(act) => act,
            Err(PollError::ShutDown) => break,
            Err(e) => panic!("Unexpected replay poll error: {e:?}"),
        };
        let mut cmds = vec![];
        for job in &act.jobs {
            match job.variant.as_ref() {
                Some(workflow_activation_job::Variant::DoUpdate(u))
                    if u.protocol_instance_id == ACCEPTED_UPDATE_ID =>
                {
                    outcome.update_activations.push(act.clone());
                    cmds.extend(respond(&act));
                }
                Some(workflow_activation_job::Variant::RemoveFromCache(r)) => {
                    outcome.evictions.push((
                        r.reason(),
                        r.message.clone(),
                        outcome.update_activations.len(),
                    ));
                }
                _ => {}
            }
        }
        replay_worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(act.run_id, cmds))
            .await
            .unwrap();
    }
    outcome
}

fn reject_update() -> workflow_command::Variant {
    UpdateResponse {
        protocol_instance_id: REJECTED_UPDATE_ID.to_string(),
        response: Some(Response::Rejected(Default::default())),
    }
    .into()
}

fn accept_update(patch_id: Option<&str>) -> Vec<workflow_command::Variant> {
    let mut cmds = vec![
        UpdateResponse {
            protocol_instance_id: ACCEPTED_UPDATE_ID.to_string(),
            response: Some(Response::Accepted(())),
        }
        .into(),
    ];
    if let Some(patch_id) = patch_id {
        cmds.push(
            SetPatchMarker {
                patch_id: patch_id.to_string(),
                deprecated: false,
            }
            .into(),
        );
    }
    cmds.push(
        UpdateResponse {
            protocol_instance_id: ACCEPTED_UPDATE_ID.to_string(),
            response: Some(Response::Completed(Payload::default())),
        }
        .into(),
    );
    cmds
}

/// The SDK flags sent with a dropped first-task rejection are not in history, so the history
/// persisted from the next completion must still replay a `patched` call, including the search
/// attribute upsert Core adds for it.
#[tokio::test]
async fn patch_after_discarded_first_task_rejection_replays() {
    let patch_id = "my-patch";
    let (persisted, tasks) = discarded_first_task_rejection_tasks();
    let (core, completions) = cached_worker_capturing_completions(&persisted, tasks);

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
            }
        ]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        reject_update(),
    ))
    .await
    .unwrap();

    let act = evicted_then_reinitialized(&core).await;
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        accept_update(Some(patch_id)),
    ))
    .await
    .unwrap();
    core.drain_pollers_and_shutdown().await;

    let ((_, rejection_metadata), update_completion) =
        rejection_and_update_completions(&completions);
    let upsert_flag = CoreInternalFlags::UpsertSearchAttributeOnPatch as u32;
    // Without this the dropped completion would not have carried the flag this test is about
    assert!(
        rejection_metadata.core_used_flags.contains(&upsert_flag),
        "The dropped rejection completion must have sent the flag. Got: {rejection_metadata:?}"
    );
    // The live `patched` call must produce its marker followed by the search attribute upsert, or
    // the replay below would not exercise the flag at all
    let command_types: Vec<_> = update_completion
        .0
        .iter()
        .map(|c| c.command_type())
        .collect();
    let marker_at = command_types
        .iter()
        .position(|t| *t == CommandType::RecordMarker);
    let upsert_at = command_types
        .iter()
        .position(|t| *t == CommandType::UpsertWorkflowSearchAttributes);
    assert_matches!(
        (marker_at, upsert_at),
        (Some(m), Some(u)) if m < u,
        "Update completion must record the patch marker and then upsert. Got: {command_types:?}"
    );
    assert_matches!(
        update_completion.0[upsert_at.unwrap()].attributes.as_ref(),
        Some(command::Attributes::UpsertWorkflowSearchAttributesCommandAttributes(a))
            if a.search_attributes
                .as_ref()
                .is_some_and(|sa| sa.indexed_fields.contains_key("TemporalChangeVersion"))
    );
    let outcome =
        replay_persisted_update_completion(persisted, &update_completion, patch_id, |_| {
            accept_update(Some(patch_id))
        })
        .await;
    let update_metadata = update_completion.1;

    outcome.assert_replayed_update(&format!(
        "History persisted from the update completion must replay deterministically. \
         Rejection completion metadata: {rejection_metadata:?}, \
         update completion metadata: {update_metadata:?}"
    ));
    assert!(
        outcome.update_activations[0].jobs.iter().any(|j| matches!(
            j.variant.as_ref(),
            Some(workflow_activation_job::Variant::NotifyHasPatch(p)) if p.patch_id == patch_id
        )),
        "Replay must see the persisted patch marker alongside the update"
    );
    assert!(
        update_metadata.core_used_flags.contains(&upsert_flag),
        "No earlier WorkflowTaskCompleted event is in history, so the completion that upserts \
         the patch search attribute must record the flag. Got: {update_metadata:?}"
    );
}

/// Decides whether a lang SDK uses a default-enabled lang flag on an activation, returning whether
/// the flag is used and whether it must be reported to Core. A flag listed in
/// `available_internal_flags` is used whether or not the activation is replaying. Lang does not
/// report it again, because Core already tracks it and keeps sending it with workflow task
/// completions until history records it. A flag missing from that list keeps the old behavior
/// when replaying, since history never recorded it, and is used and reported when live.
fn default_enabled_lang_flag(act: &WorkflowActivation, flag: u32) -> (bool, bool) {
    if act.available_internal_flags.contains(&flag) {
        (true, false)
    } else if act.is_replaying {
        (false, false)
    } else {
        (true, true)
    }
}

/// Lang flags sent with a dropped first-task rejection are not in history either. A lang SDK that
/// does not report flags already listed in `available_internal_flags` must still make the same
/// flag-dependent choice when the next persisted task replays from scratch as it made live, so the
/// persisted completion has to carry the flag whether or not lang reports it again. This checks
/// flag availability and selection, not lang's resulting commands.
#[tokio::test]
async fn lang_flag_selection_after_discarded_first_task_rejection_matches_replay() {
    let lang_flag = 2;
    let (persisted, tasks) = discarded_first_task_rejection_tasks();
    let (core, completions) = cached_worker_capturing_completions(&persisted, tasks);

    let act = core.poll_workflow_activation().await.unwrap();
    let (_, report) = default_enabled_lang_flag(&act, lang_flag);
    let mut completion = WorkflowActivationCompletion::from_cmd(act.run_id, reject_update());
    if report {
        completion.add_internal_flags(lang_flag);
    }
    core.complete_workflow_activation(completion).await.unwrap();

    let act = evicted_then_reinitialized(&core).await;
    let live_available = act.available_internal_flags.clone();
    let (live_uses_flag, report) = default_enabled_lang_flag(&act, lang_flag);
    let mut completion = WorkflowActivationCompletion::from_cmds(act.run_id, accept_update(None));
    if report {
        completion.add_internal_flags(lang_flag);
    }
    core.complete_workflow_activation(completion).await.unwrap();
    core.drain_pollers_and_shutdown().await;

    let ((_, rejection_metadata), update_completion) =
        rejection_and_update_completions(&completions);
    // Without this the dropped completion would not have carried the flag this test is about
    assert!(
        rejection_metadata.lang_used_flags.contains(&lang_flag),
        "The dropped rejection completion must have sent lang flag {lang_flag}. \
         Got: {rejection_metadata:?}"
    );
    let mut replay_choice = None;
    let outcome = replay_persisted_update_completion(persisted, &update_completion, "", |act| {
        replay_choice = Some((
            default_enabled_lang_flag(act, lang_flag).0,
            act.available_internal_flags.clone(),
        ));
        accept_update(None)
    })
    .await;
    let update_metadata = update_completion.1;

    outcome.assert_replayed_update(&format!("Update completion metadata: {update_metadata:?}"));
    let (replay_uses_flag, replay_available) =
        replay_choice.expect("Replay must deliver the accepted update");
    assert_eq!(
        replay_uses_flag, live_uses_flag,
        "Lang flag {lang_flag} selection must match between the live task and its cold replay. \
         Live available: {live_available:?}, replay available: {replay_available:?}, \
         rejection completion metadata: {rejection_metadata:?}, \
         update completion metadata: {update_metadata:?}"
    );
    // The live task used the flag, and history has no earlier record of it, so the persisted
    // completion is the one that must carry it
    assert!(
        update_metadata.lang_used_flags.contains(&lang_flag),
        "The persisted update completion must record lang flag {lang_flag}. \
         Got: {update_metadata:?}"
    );
}

/// Lang's handling of a signal that starts a timer only when lang flag `flag` is not in effect
fn signal_cmds_unless_flag(act: &WorkflowActivation, flag: u32) -> Vec<workflow_command::Variant> {
    if act.available_internal_flags.contains(&flag) {
        vec![]
    } else {
        vec![
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
            }
            .into(),
        ]
    }
}

/// A lang flag sent with a discarded rejection completion is not in history, so another worker
/// can complete later tasks without it. A cached run that later replays those tasks must make the
/// same flag-dependent choice as that worker and as a cold replay, or it fails with a
/// nondeterminism error that a fresh worker would not hit.
#[tokio::test]
async fn cached_replay_of_other_workers_task_ignores_discarded_lang_flags() {
    let lang_flag = 2;
    let mut prefix = TestHistoryBuilder::default();
    prefix.add_by_type(EventType::WorkflowExecutionStarted);
    prefix.add_full_wf_task();
    if let Some(Attributes::WorkflowTaskCompletedEventAttributes(a)) =
        prefix.last_event().and_then(|e| e.attributes.as_mut())
    {
        a.sdk_metadata = Some(WorkflowTaskCompletedMetadata {
            core_used_flags: vec![1, 2, 3, 4],
            ..Default::default()
        });
    }
    let mut speculative = prefix.clone();
    speculative.add_workflow_task_scheduled_and_started();
    let mut speculative_task =
        hist_to_poll_resp(&speculative, "fakeid", ResponseType::AllHistory).resp;
    speculative_task.add_update_request(REJECTED_UPDATE_ID, 5);
    // Another worker handled the signal without the flag and so started the timer
    let mut persisted = prefix;
    persisted.add_we_signaled("sig", vec![]);
    persisted.add_full_wf_task();
    let timer_started_event_id = persisted.add_timer_started("1".to_string());
    persisted.add_workflow_task_scheduled_and_started();
    let tasks = [
        hist_to_poll_resp(&persisted, "fakeid", ResponseType::ToTaskNum(1)).resp,
        speculative_task,
        hist_to_poll_resp(&persisted, "fakeid", ResponseType::AllHistory).resp,
    ];

    let completions = Arc::new(Mutex::new(vec![]));
    let completions_c = completions.clone();
    let mut mh =
        MockPollCfg::from_resp_batches("fakeid", persisted.clone(), tasks, mock_worker_client());
    mh.completion_mock_fn = Some(Box::new(move |c| {
        let mut completions = completions_c.lock();
        completions.push((c.commands.clone(), c.sdk_metadata.clone()));
        // The server discards the speculative task's rejection
        let reset_history_event_id = if completions.len() == 2 { 3 } else { 0 };
        Ok(RespondWorkflowTaskCompletedResponse {
            reset_history_event_id,
            ..Default::default()
        })
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();

    let act = core.poll_workflow_activation().await.unwrap();
    assert!(!act.is_replaying);
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
        }]
    );
    let mut completion = WorkflowActivationCompletion::from_cmd(act.run_id, reject_update());
    completion.add_internal_flags(lang_flag);
    core.complete_workflow_activation(completion).await.unwrap();

    // Only the signal job (no initialization or eviction) shows the run stayed cached
    let act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(act.run_id, run_id);
    assert!(act.is_replaying);
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    let cached_available = act.available_internal_flags.clone();
    let cmds = signal_cmds_unless_flag(&act, lang_flag);
    // A nondeterminism error here would fail the task, which the mock client does not expect
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(act.run_id, cmds))
        .await
        .unwrap();
    core.drain_pollers_and_shutdown().await;

    let completions = completions.lock().clone();
    assert_eq!(completions.len(), 3, "Completions: {completions:?}");
    assert!(
        completions[1].1.lang_used_flags.contains(&lang_flag),
        "The discarded rejection completion must have sent lang flag {lang_flag}. \
         Got: {:?}",
        completions[1].1
    );
    assert!(
        !cached_available.contains(&lang_flag),
        "Cached replay must not offer lang flag {lang_flag}, which history never recorded. \
         Got: {cached_available:?}"
    );
    // The timer matched TimerStarted from history, so it is not sent again
    assert!(
        completions[2].0.is_empty(),
        "Cached replay must match the timer to event {timer_started_event_id}. Got: {:?}",
        completions[2].0
    );
    assert!(
        completions[2].1.lang_used_flags.contains(&lang_flag),
        "History still lacks lang flag {lang_flag}, so it must be sent again. Got: {:?}",
        completions[2].1
    );

    let replay_worker = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream::iter([HistoryForReplay::from(persisted)]),
    ))
    .unwrap();
    let mut signal_activations = vec![];
    let mut evictions = vec![];
    loop {
        let act = match replay_worker.poll_workflow_activation().await {
            Ok(act) => act,
            Err(PollError::ShutDown) => break,
            Err(e) => panic!("Unexpected replay poll error: {e:?}"),
        };
        let mut cmds = vec![];
        for job in &act.jobs {
            match job.variant.as_ref() {
                Some(workflow_activation_job::Variant::SignalWorkflow(_)) => {
                    signal_activations.push(act.clone());
                    cmds = signal_cmds_unless_flag(&act, lang_flag);
                }
                Some(workflow_activation_job::Variant::RemoveFromCache(r)) => {
                    evictions.push((r.reason(), r.message.clone(), signal_activations.len()));
                }
                _ => {}
            }
        }
        replay_worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(act.run_id, cmds))
            .await
            .unwrap();
    }
    assert_eq!(signal_activations.len(), 1);
    assert_eq!(
        signal_activations[0].available_internal_flags, cached_available,
        "Cold replay must offer the same lang flags as the cached replay"
    );
    // The replay worker evicts after replay finishes; any other eviction means replay failed
    assert_matches!(
        evictions.as_slice(),
        [(EvictionReason::LangRequested, _, 1)],
        "Evictions: {evictions:?}"
    );
}
