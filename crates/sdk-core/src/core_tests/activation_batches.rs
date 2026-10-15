use crate::{
    CompleteWfError, PollError, Worker,
    internal_flags::CoreInternalFlags,
    replay::TestHistoryBuilder,
    test_help::{MockPollCfg, build_mock_pollers, mock_worker, start_timer_cmd},
};
use std::time::Duration;
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::workflow_activation_job::Variant,
        workflow_commands::{CancelTimer, CompleteWorkflowExecution, workflow_command},
        workflow_completion::{
            IndexedWorkflowActivationCompletion, WorkflowActivationBatchCompletion,
            WorkflowActivationCompletion,
        },
    },
    temporal::api::{
        enums::v1::EventType, workflowservice::v1::RespondWorkflowTaskCompletedResponse,
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
    assert_eq!(batch.activations.len(), 1);
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
    assert!(batches < task_count);
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
async fn metadata_changes_are_batch_barriers() {
    let mut history = history(5, true);
    history.set_flags_last_wft(&[], &[700]);
    let mut mocks = build_mock_pollers(MockPollCfg::from_resps(history, [7]));
    mocks.make_wft_stream_interminable();
    let core = mock_worker(mocks);
    let run_id = initialize(&core).await;
    let batch = core.poll_workflow_activation_batch().await.unwrap();
    assert_eq!(batch.activations.len(), 4);
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
    let last_replay = core.poll_workflow_activation_batch().await.unwrap();
    assert!(
        last_replay.activations[0]
            .available_internal_flags
            .contains(&700)
    );
    assert_eq!(last_replay.activations.len(), 1);
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
