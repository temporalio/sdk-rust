use crate::{
    CompleteWfError, PollError, Worker,
    internal_flags::CoreInternalFlags,
    replay::{TestHistoryBuilder, default_act_sched},
    test_help::{
        MockPollCfg, ResponseType, build_mock_pollers, hist_to_poll_resp, mock_worker,
        start_timer_cmd,
    },
    worker::client::mocks::mock_worker_client,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::workflow_activation_job::Variant,
        workflow_commands::{
            CancelTimer, CompleteWorkflowExecution, SetPatchMarker, workflow_command,
        },
        workflow_completion::{
            IndexedWorkflowActivationCompletion, WorkflowActivationBatchCompletion,
            WorkflowActivationCompletion,
        },
    },
    temporal::api::{
        enums::v1::EventType,
        history::v1::history_event::Attributes,
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};

fn history(task_count: usize, unchunked: bool) -> TestHistoryBuilder {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    let timer = history.add_timer_started("1".to_owned());
    if unchunked {
        history.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
    }
    for index in 0..task_count {
        if index % 2 == 0 {
            history.add_we_signaled("tick", vec![]);
        }
        history.add_full_wf_task();
    }
    history.add_timer_fired(timer, "1".to_owned());
    history.add_workflow_task_scheduled_and_started();
    history
}

fn indexed(
    index: usize,
    completion: WorkflowActivationCompletion,
) -> IndexedWorkflowActivationCompletion {
    IndexedWorkflowActivationCompletion {
        activation_index: index as u32,
        completion: Some(completion),
    }
}

async fn finish(core: Worker, run_id: String) {
    core.request_workflow_eviction(&run_id);
    let batch = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(batch.activations.len(), 1);
    assert!(batch.activations[0].is_only_eviction());
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(0, WorkflowActivationCompletion::empty(run_id))],
    })
    .await
    .unwrap();
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_activation_batch().await,
        Err(PollError::ShutDown)
    );
    core.finalize_shutdown().await;
}

async fn initialize(core: &Worker) -> String {
    let batch = core.poll_workflow_activation_batch().await.unwrap();
    assert_matches!(
        batch.activations[0].jobs[0].variant,
        Some(Variant::InitializeWorkflow(_))
    );
    let run_id = batch.run_id;
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(
            0,
            WorkflowActivationCompletion::from_cmd(
                run_id.clone(),
                start_timer_cmd(1, Duration::from_secs(1)),
            ),
        )],
    })
    .await
    .unwrap();
    run_id
}

#[rstest::rstest]
#[case(4)]
#[case(600)]
#[tokio::test]
async fn batches_apply_unrecorded_commands_at_each_boundary(#[case] task_count: usize) {
    let mut cfg = MockPollCfg::from_resps(history(task_count, true), [task_count + 2]);
    cfg.completion_mock_fn = Some(Box::new(|completion| {
        assert!(completion.sdk_metadata.lang_used_flags.contains(&1000));
        assert!(
            completion
                .sdk_metadata
                .core_used_flags
                .iter()
                .all(|flag| *flag <= 5)
        );
        Ok(RespondWorkflowTaskCompletedResponse::default())
    }));
    let mut mocks = build_mock_pollers(cfg);
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let run_id = initialize(&core).await;
    let mut replayed = 0;
    let mut batches = 0;
    loop {
        let batch = core.poll_workflow_activation_batch().await.unwrap();
        batches += 1;
        let mut results = vec![];
        let live = !batch.activations.last().unwrap().is_replaying;
        if !live {
            assert!(batch.activations.len() > 1);
        }
        for (index, activation) in batch.activations.into_iter().enumerate() {
            let mut completion = if activation.is_replaying {
                let seq = 100 + replayed;
                replayed += 1;
                WorkflowActivationCompletion::from_cmds(
                    run_id.clone(),
                    vec![
                        start_timer_cmd(seq as u32, Duration::from_secs(1)),
                        workflow_command::Variant::CancelTimer(CancelTimer { seq: seq as u32 }),
                    ],
                )
            } else {
                assert_eq!(index, 0);
                assert!(matches!(
                    activation.jobs[0].variant,
                    Some(Variant::FireTimer(_))
                ));
                WorkflowActivationCompletion::from_cmd(
                    run_id.clone(),
                    workflow_command::Variant::CompleteWorkflowExecution(
                        CompleteWorkflowExecution::default(),
                    ),
                )
            };
            if let Some(temporalio_common::protos::coresdk::workflow_completion::workflow_activation_completion::Status::Successful(success)) = completion.status.as_mut() {
                success.used_internal_flags.push(1000);
            }
            results.push(indexed(index, completion));
        }
        core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
            run_id: run_id.clone(),
            completions: results,
        })
        .await
        .unwrap();
        if live {
            break;
        }
    }
    assert_eq!(replayed, task_count);
    assert_eq!(batches, 2);
    finish(core, run_id).await;
}

#[tokio::test]
async fn chunked_replay_uses_singletons() {
    let mut mocks = build_mock_pollers(MockPollCfg::from_resps(history(8, false), [10]));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let run_id = initialize(&core).await;
    loop {
        let batch = core.poll_workflow_activation_batch().await.unwrap();
        assert_eq!(batch.activations.len(), 1);
        let live = !batch.activations[0].is_replaying;
        core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
            run_id: run_id.clone(),
            completions: vec![indexed(0, WorkflowActivationCompletion::empty(&run_id))],
        })
        .await
        .unwrap();
        if live {
            break;
        }
    }
    finish(core, run_id).await;
}

#[tokio::test]
async fn invalid_indices_and_dependency_crossings_do_not_advance_batch() {
    let mut mocks = build_mock_pollers(MockPollCfg::from_resps(history(4, true), [6]));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let run_id = initialize(&core).await;
    let batch = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(batch.activations.len(), 4);
    for results in [
        vec![],
        vec![indexed(1, WorkflowActivationCompletion::empty(&run_id))],
        vec![indexed(0, WorkflowActivationCompletion::empty("other-run"))],
        vec![IndexedWorkflowActivationCompletion {
            activation_index: 0,
            completion: None,
        }],
        vec![
            indexed(
                0,
                WorkflowActivationCompletion::from_cmd(
                    run_id.clone(),
                    workflow_command::Variant::CompleteWorkflowExecution(
                        CompleteWorkflowExecution::default(),
                    ),
                ),
            ),
            indexed(1, WorkflowActivationCompletion::empty(&run_id)),
        ],
    ] {
        assert_matches!(
            core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
                run_id: run_id.clone(),
                completions: results,
            })
            .await,
            Err(CompleteWfError::MalformedWorkflowCompletion { .. })
        );
    }
    // A partial success is useful when a command requires an intervening Core response.
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(0, WorkflowActivationCompletion::empty(&run_id))],
    })
    .await
    .unwrap();
    let suffix = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(suffix.activations, batch.activations[1..]);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: suffix
            .activations
            .into_iter()
            .enumerate()
            .map(|(index, _)| indexed(index, WorkflowActivationCompletion::empty(&run_id)))
            .collect(),
    })
    .await
    .unwrap();
    let live = core.poll_workflow_activation_batch().await.unwrap();
    assert!(!live.activations[0].is_replaying);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(0, WorkflowActivationCompletion::empty(&run_id))],
    })
    .await
    .unwrap();
    finish(core, run_id).await;
}

#[tokio::test]
async fn batch_preserves_metadata_changes() {
    let mut history = history(5, true);
    history.set_flags_last_wft(&[], &[700]);
    let last_completion = history
        .get_full_history_info()
        .unwrap()
        .events()
        .iter()
        .rfind(|e| e.event_type() == EventType::WorkflowTaskCompleted)
        .unwrap()
        .event_id;
    history.modify_event(last_completion, |event| {
        let Some(Attributes::WorkflowTaskCompletedEventAttributes(attrs)) = &mut event.attributes
        else {
            unreachable!()
        };
        attrs.sdk_metadata.as_mut().unwrap().sdk_version = "new-sdk".to_owned();
        attrs.worker_deployment_name = "deployment".to_owned();
        attrs.deployment_version = Some(
            temporalio_common::protos::temporal::api::deployment::v1::WorkerDeploymentVersion {
                deployment_name: "deployment".to_owned(),
                build_id: "new-build".to_owned(),
            },
        );
    });
    let mut mocks = build_mock_pollers(MockPollCfg::from_resps(history, [7]));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let run_id = initialize(&core).await;
    let batch = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(batch.activations.len(), 5);
    assert!(
        batch.activations[..4]
            .iter()
            .all(|a| !a.available_internal_flags.contains(&700))
    );
    let last = batch.activations.last().unwrap();
    assert!(last.available_internal_flags.contains(&700));
    assert_eq!(last.last_sdk_version, "new-sdk");
    let version = last.deployment_version_for_current_task.as_ref().unwrap();
    assert_eq!(version.deployment_name, "deployment");
    assert_eq!(version.build_id, "new-build");
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: batch
            .activations
            .into_iter()
            .enumerate()
            .map(|(index, _)| indexed(index, WorkflowActivationCompletion::empty(&run_id)))
            .collect(),
    })
    .await
    .unwrap();
    let live = core.poll_workflow_activation_batch().await.unwrap();
    assert!(!live.activations[0].is_replaying);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(0, WorkflowActivationCompletion::empty(&run_id))],
    })
    .await
    .unwrap();
    finish(core, run_id).await;
}

#[tokio::test]
async fn entire_timer_replay_is_one_batch() {
    let task_count = 600;
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
    for seq in 1..=task_count {
        let started = history.add_timer_started(seq.to_string());
        history.add_timer_fired(started, seq.to_string());
        if seq == task_count {
            history.add_workflow_task_scheduled_and_started();
        } else {
            history.add_full_wf_task();
        }
    }
    let mut mocks = build_mock_pollers(MockPollCfg::from_resps(history, [task_count + 1]));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let batch = core.poll_workflow_activation_batch().await.unwrap();
    let run_id = batch.run_id;
    assert_eq!(batch.activations.len(), task_count);
    assert_matches!(
        batch.activations[0].jobs[0].variant,
        Some(Variant::InitializeWorkflow(_))
    );
    for (index, activation) in batch.activations.iter().enumerate().skip(1) {
        assert_matches!(activation.jobs[0].variant, Some(Variant::FireTimer(ref timer)) => timer.seq as usize == index);
    }
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: batch
            .activations
            .into_iter()
            .enumerate()
            .map(|(index, _)| {
                indexed(
                    index,
                    WorkflowActivationCompletion::from_cmd(
                        &run_id,
                        start_timer_cmd(index as u32 + 1, Duration::from_secs(1)),
                    ),
                )
            })
            .collect(),
    })
    .await
    .unwrap();
    let live = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(live.activations.len(), 1);
    assert!(!live.activations[0].is_replaying);
    assert_matches!(live.activations[0].jobs[0].variant, Some(Variant::FireTimer(ref timer)) => timer.seq as usize == task_count);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(
            0,
            WorkflowActivationCompletion::from_cmd(
                &run_id,
                workflow_command::Variant::CompleteWorkflowExecution(
                    CompleteWorkflowExecution::default(),
                ),
            ),
        )],
    })
    .await
    .unwrap();
    finish(core, run_id).await;
}

#[tokio::test]
async fn known_activity_resolution_and_workflow_cancellation_are_batched() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
    let activity = history.add_activity_task_scheduled("activity-id");
    history.add_we_signaled("tick", vec![]);
    history.add_full_wf_task();
    let started = history.add_activity_task_started(activity);
    history.add_activity_task_completed(activity, started, Default::default());
    history.add_full_wf_task();
    history.add_cancel_requested();
    history.add_full_wf_task();
    history.add_workflow_task_scheduled_and_started();
    let mut mocks = build_mock_pollers(MockPollCfg::from_resps(history, [5]));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let initial = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(initial.activations.len(), 1);
    let run_id = initial.run_id;
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(
            0,
            WorkflowActivationCompletion::from_cmd(
                &run_id,
                workflow_command::Variant::ScheduleActivity(
                    temporalio_common::protos::coresdk::workflow_commands::ScheduleActivity {
                        seq: 42,
                        activity_id: "activity-id".to_owned(),
                        ..default_act_sched()
                    },
                ),
            ),
        )],
    })
    .await
    .unwrap();
    let batch = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(batch.activations.len(), 3);
    assert_matches!(
        batch.activations[0].jobs[0].variant,
        Some(Variant::SignalWorkflow(_))
    );
    assert_matches!(batch.activations[1].jobs[0].variant, Some(Variant::ResolveActivity(ref activity)) => activity.seq == 42);
    assert_matches!(
        batch.activations[2].jobs[0].variant,
        Some(Variant::CancelWorkflow(_))
    );
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: batch
            .activations
            .into_iter()
            .enumerate()
            .map(|(index, _)| indexed(index, WorkflowActivationCompletion::empty(&run_id)))
            .collect(),
    })
    .await
    .unwrap();
    let live = core.poll_workflow_activation_batch().await.unwrap();
    assert!(!live.activations[0].is_replaying);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(0, WorkflowActivationCompletion::empty(&run_id))],
    })
    .await
    .unwrap();
    finish(core, run_id).await;
}

#[tokio::test]
async fn batches_use_buffered_history_before_pagination_finishes() {
    let history = history(12, true);
    let mut response =
        hist_to_poll_resp(&history, "wfid".to_owned(), ResponseType::AllHistory).resp;
    // Keep the replay watermark in the first page so extraction can yield before fetching the rest.
    response.previous_started_event_id = 3;
    let events = response.history.as_ref().unwrap().events.clone();
    let cutoff = events
        .iter()
        .filter(|e| e.event_type() == EventType::WorkflowTaskStarted)
        .nth(7)
        .unwrap()
        .event_id as usize;
    response.history.as_mut().unwrap().events.truncate(5);
    response.next_page_token = vec![1];
    let first_page = GetWorkflowExecutionHistoryResponse {
        history: Some(
            temporalio_common::protos::temporal::api::history::v1::History {
                events: events[5..cutoff].to_vec(),
            },
        ),
        next_page_token: vec![2],
        ..Default::default()
    };
    let last_page = GetWorkflowExecutionHistoryResponse {
        history: Some(
            temporalio_common::protos::temporal::api::history::v1::History {
                events: events[cutoff..].to_vec(),
            },
        ),
        ..Default::default()
    };
    let fetches = Arc::new(AtomicUsize::new(0));
    let observed_fetches = fetches.clone();
    let mut client = mock_worker_client();
    client
        .expect_get_workflow_execution_history()
        .times(2)
        .returning(move |_, _, token| {
            observed_fetches.fetch_add(1, Ordering::SeqCst);
            match token.as_slice() {
                [1] => Ok(first_page.clone()),
                [2] => Ok(last_page.clone()),
                _ => panic!("Unexpected page token: {token:?}"),
            }
        });
    let mut mocks = build_mock_pollers(MockPollCfg::from_resp_batches(
        "wfid",
        history,
        [ResponseType::Raw(response)],
        client,
    ));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let mut batch = core.poll_workflow_activation_batch().await.unwrap();
    assert!(
        batch.activations.len() > 1,
        "{batch:?}; fetches={}",
        fetches.load(Ordering::SeqCst)
    );
    assert!(batch.activations.len() < 13);
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    let run_id = batch.run_id.clone();
    let mut replayed = 0;
    loop {
        let live = !batch.activations.last().unwrap().is_replaying;
        let results = batch
            .activations
            .into_iter()
            .enumerate()
            .map(|(index, activation)| {
                let completion = if matches!(
                    activation.jobs.first().and_then(|j| j.variant.as_ref()),
                    Some(Variant::InitializeWorkflow(_))
                ) {
                    replayed += 1;
                    WorkflowActivationCompletion::from_cmd(
                        &run_id,
                        start_timer_cmd(1, Duration::from_secs(1)),
                    )
                } else {
                    replayed += usize::from(activation.is_replaying);
                    WorkflowActivationCompletion::empty(&run_id)
                };
                indexed(index, completion)
            })
            .collect();
        core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
            run_id: run_id.clone(),
            completions: results,
        })
        .await
        .unwrap();
        if live {
            break;
        }
        batch = core.poll_workflow_activation_batch().await.unwrap();
    }
    assert_eq!(replayed, 13);
    assert_eq!(fetches.load(Ordering::SeqCst), 2);
    finish(core, run_id).await;
}

#[tokio::test]
async fn unknown_activity_sequence_requires_an_early_batch_return() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
    history.add_we_signaled("schedule", vec![]);
    history.add_full_wf_task();
    let activity = history.add_activity_task_scheduled("7");
    let started = history.add_activity_task_started(activity);
    history.add_activity_task_completed(activity, started, Default::default());
    history.add_full_wf_task();
    history.add_full_wf_task();
    history.add_workflow_task_scheduled_and_started();
    let mut mocks = build_mock_pollers(MockPollCfg::from_resps(history, [5]));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let initial = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(initial.activations.len(), 2);
    let run_id = initial.run_id;
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![
            indexed(0, WorkflowActivationCompletion::empty(&run_id)),
            indexed(
                1,
                WorkflowActivationCompletion::from_cmd(
                    &run_id,
                    workflow_command::Variant::ScheduleActivity(
                        temporalio_common::protos::coresdk::workflow_commands::ScheduleActivity {
                            seq: 42,
                            activity_id: "7".to_owned(),
                            ..default_act_sched()
                        },
                    ),
                ),
            ),
        ],
    })
    .await
    .unwrap();
    let suffix = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(suffix.activations.len(), 2);
    assert_matches!(suffix.activations[0].jobs[0].variant,
        Some(Variant::ResolveActivity(ref activity)) => activity.seq == 42);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: suffix
            .activations
            .into_iter()
            .enumerate()
            .map(|(index, _)| indexed(index, WorkflowActivationCompletion::empty(&run_id)))
            .collect(),
    })
    .await
    .unwrap();
    let live = core.poll_workflow_activation_batch().await.unwrap();
    assert!(!live.activations[0].is_replaying);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(0, WorkflowActivationCompletion::empty(&run_id))],
    })
    .await
    .unwrap();
    finish(core, run_id).await;
}

#[tokio::test]
async fn patch_lookahead_is_included_before_early_return() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
    history.add_we_signaled("patch", vec![]);
    history.add_full_wf_task();
    history.add_has_change_marker("patch-id", false);
    history.add_full_wf_task();
    history.add_workflow_task_scheduled_and_started();
    let mut mocks = build_mock_pollers(MockPollCfg::from_resps(history, [4]));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let initial = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(initial.activations.len(), 3);
    assert_matches!(initial.activations[1].jobs[0].variant,
        Some(Variant::NotifyHasPatch(ref patch)) => patch.patch_id == "patch-id");
    assert_matches!(
        initial.activations[1].jobs[1].variant,
        Some(Variant::SignalWorkflow(_))
    );
    let run_id = initial.run_id;
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![
            indexed(0, WorkflowActivationCompletion::empty(&run_id)),
            indexed(
                1,
                WorkflowActivationCompletion::from_cmd(
                    &run_id,
                    workflow_command::Variant::SetPatchMarker(SetPatchMarker {
                        patch_id: "patch-id".to_owned(),
                        deprecated: false,
                    }),
                ),
            ),
        ],
    })
    .await
    .unwrap();
    let suffix = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(suffix.activations, initial.activations[2..]);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(0, WorkflowActivationCompletion::empty(&run_id))],
    })
    .await
    .unwrap();
    let live = core.poll_workflow_activation_batch().await.unwrap();
    assert!(!live.activations[0].is_replaying);
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: vec![indexed(0, WorkflowActivationCompletion::empty(&run_id))],
    })
    .await
    .unwrap();
    finish(core, run_id).await;
}

#[rstest::rstest]
#[case::language_failure(false)]
#[case::replay_nondeterminism(true)]
#[tokio::test]
async fn batch_failure_evicts_without_running_suffix(#[case] nondeterminism: bool) {
    let mut cfg = MockPollCfg::from_resps(history(4, true), [6]);
    cfg.num_expected_fails = 1;
    cfg.num_expected_completions = Some(0.into());
    let mut mocks = build_mock_pollers(cfg);
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let run_id = initialize(&core).await;
    let batch = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(batch.activations.len(), 4);
    let results = if nondeterminism {
        vec![
            indexed(
                0,
                WorkflowActivationCompletion::from_cmd(
                    run_id.clone(),
                    workflow_command::Variant::CancelTimer(CancelTimer { seq: 9999 }),
                ),
            ),
            indexed(1, WorkflowActivationCompletion::empty(&run_id)),
        ]
    } else {
        vec![
            indexed(0, WorkflowActivationCompletion::empty(&run_id)),
            indexed(
                1,
                WorkflowActivationCompletion::fail(
                    run_id.clone(),
                    temporalio_common::protos::temporal::api::failure::v1::Failure {
                        message: "activation failed".to_owned(),
                        ..Default::default()
                    },
                    None,
                ),
            ),
        ]
    };
    core.complete_workflow_activation_batch(WorkflowActivationBatchCompletion {
        run_id: run_id.clone(),
        completions: results,
    })
    .await
    .unwrap();
    finish(core, run_id).await;
}
