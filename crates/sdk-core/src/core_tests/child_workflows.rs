use crate::{
    internal_flags::CoreInternalFlags,
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkflowCachingPolicy::NonSticky, build_fake_worker,
        build_mock_pollers, gen_assert_and_reply, mock_worker, poll_and_reply,
    },
};
use assert_matches::assert_matches;
use temporalio_common::protos::{
    coresdk::{
        child_workflow::{
            ChildWorkflowCancellationType, ChildWorkflowResult, child_workflow_result,
        },
        common::NamespacedWorkflowExecution,
        workflow_activation::{
            WorkflowActivation, WorkflowActivationJob, resolve_child_workflow_execution_start,
            workflow_activation_job::Variant,
        },
        workflow_commands::{
            CancelChildWorkflowExecution, CompleteWorkflowExecution, StartChildWorkflowExecution,
        },
    },
    temporal::api::{
        common::v1::WorkflowExecution,
        enums::v1::EventType,
        history::v1::{
            ChildWorkflowExecutionCompletedEventAttributes,
            ChildWorkflowExecutionStartedEventAttributes,
            StartChildWorkflowExecutionInitiatedEventAttributes,
        },
    },
};

#[rstest::rstest]
#[case::live(false, false)]
#[case::replay(true, false)]
#[case::replay_upgraded_after_cancel(true, true)]
#[tokio::test]
async fn child_cancel_wait_requested_resolves_on_acknowledgment(
    #[case] replay: bool,
    #[case] legacy_cancel: bool,
    #[values(false, true)] finish_before_ack: bool,
) {
    let child_id = "child-1";
    let child_type = "child";
    let seq = 1;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let initiated_event_id = t.add(StartChildWorkflowExecutionInitiatedEventAttributes {
        workflow_id: child_id.to_string(),
        workflow_type: Some(child_type.into()),
        ..Default::default()
    });
    let started_event_id = t.add(ChildWorkflowExecutionStartedEventAttributes {
        initiated_event_id,
        workflow_execution: Some(WorkflowExecution {
            workflow_id: child_id.to_string(),
            run_id: "child-run".to_string(),
        }),
        ..Default::default()
    });
    t.add_full_wf_task();
    let flag = CoreInternalFlags::ChildWorkflowCancelOnRequest;
    if replay && !legacy_cancel {
        t.set_flags_last_wft(&[flag], &[]);
    }
    let cancel_id = t.add_cancel_external_wf(NamespacedWorkflowExecution {
        workflow_id: child_id.to_string(),
        ..Default::default()
    });
    let completed = ChildWorkflowExecutionCompletedEventAttributes {
        initiated_event_id,
        started_event_id,
        ..Default::default()
    };
    let (first, second) = if finish_before_ack {
        t.add(completed.clone());
        ("outcome", "ack")
    } else {
        t.add_cancel_external_wf_completed(cancel_id);
        ("ack", "outcome")
    };
    t.add_we_signaled(first, vec![]);
    t.add_full_wf_task();
    if legacy_cancel {
        // An upgraded worker can record the flag while the legacy cancellation is in flight.
        t.set_flags_last_wft(&[flag], &[]);
    }
    if finish_before_ack {
        t.add_cancel_external_wf_completed(cancel_id);
    } else {
        t.add(completed);
    }
    t.add_we_signaled(second, vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let batches = if replay {
        vec![ResponseType::AllHistory]
    } else {
        std::iter::once(ResponseType::ToTaskNum(1))
            .chain((2..=4).map(ResponseType::OneTask))
            .collect()
    };
    let mut mock = build_mock_pollers(MockPollCfg::from_resps(t, batches));
    mock.worker_cfg(|config| config.max_cached_workflows = 1);
    let core = mock_worker(mock);
    let check_notification = |act: &WorkflowActivation, signal_name, expect_resolution| {
        assert_eq!(
            act.jobs.len(),
            1 + usize::from(expect_resolution),
            "{act:?}"
        );
        assert!(
            act.jobs.iter().any(|job| matches!(
                &job.variant,
                Some(Variant::SignalWorkflow(signal)) if signal.signal_name == signal_name
            )),
            "{act:?}"
        );
        if expect_resolution {
            assert!(
                act.jobs.iter().any(|job| {
                    let Some(Variant::ResolveChildWorkflowExecution(result)) = &job.variant else {
                        return false;
                    };
                    let Some(ChildWorkflowResult {
                        status: Some(status),
                    }) = &result.result
                    else {
                        return false;
                    };
                    result.seq == seq
                        && matches!(
                            (signal_name, status),
                            ("ack", child_workflow_result::Status::Cancelled(_))
                                | ("outcome", child_workflow_result::Status::Completed(_))
                        )
                }),
                "expected child resolution at {signal_name}, got {act:?}"
            );
        }
    };
    let expect_first_resolution = finish_before_ack || !legacy_cancel;
    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &|act| {
                    assert!(matches!(
                        act.jobs[0].variant,
                        Some(Variant::InitializeWorkflow(_))
                    ));
                },
                vec![
                    StartChildWorkflowExecution {
                        seq,
                        workflow_id: child_id.to_string(),
                        workflow_type: child_type.to_string(),
                        cancellation_type: ChildWorkflowCancellationType::WaitCancellationRequested
                            as i32,
                        ..Default::default()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &|act| {
                    assert!(matches!(
                        act.jobs[0].variant,
                        Some(Variant::ResolveChildWorkflowExecutionStart(_))
                    ));
                },
                vec![
                    CancelChildWorkflowExecution {
                        child_workflow_seq: seq,
                        ..Default::default()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &|act| check_notification(act, first, expect_first_resolution),
                vec![],
            ),
            gen_assert_and_reply(
                &|act| check_notification(act, second, !expect_first_resolution),
                vec![CompleteWorkflowExecution::default().into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn child_cancel_wait_requested_acknowledged_before_start() {
    let child_id = "child-1";
    let child_type = "child";
    let seq = 1;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let initiated_event_id = t.add(StartChildWorkflowExecutionInitiatedEventAttributes {
        workflow_id: child_id.to_string(),
        workflow_type: Some(child_type.into()),
        ..Default::default()
    });
    t.add_cancel_requested();
    t.add_full_wf_task();
    t.set_flags_last_wft(&[CoreInternalFlags::ChildWorkflowCancelOnRequest], &[]);
    let cancel_id = t.add_cancel_external_wf(NamespacedWorkflowExecution {
        workflow_id: child_id.to_string(),
        ..Default::default()
    });
    t.add_cancel_external_wf_completed(cancel_id);
    t.add_full_wf_task();
    let started_event_id = t.add(ChildWorkflowExecutionStartedEventAttributes {
        initiated_event_id,
        workflow_execution: Some(WorkflowExecution {
            workflow_id: child_id.to_string(),
            run_id: "child-run".to_string(),
        }),
        ..Default::default()
    });
    t.add(ChildWorkflowExecutionCompletedEventAttributes {
        initiated_event_id,
        started_event_id,
        ..Default::default()
    });
    t.add_we_signaled("finish", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let core = build_fake_worker("parent", t, [ResponseType::AllHistory]);
    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &|act| {
                    assert_matches!(
                        act.jobs.as_slice(),
                        [WorkflowActivationJob { variant: Some(Variant::InitializeWorkflow(_)) }]
                    );
                },
                vec![StartChildWorkflowExecution {
                    seq,
                    workflow_id: child_id.to_string(),
                    workflow_type: child_type.to_string(),
                    cancellation_type: ChildWorkflowCancellationType::WaitCancellationRequested
                        as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &|act| {
                    assert_matches!(
                        act.jobs.as_slice(),
                        [WorkflowActivationJob { variant: Some(Variant::CancelWorkflow(_)) }]
                    );
                },
                vec![CancelChildWorkflowExecution {
                    child_workflow_seq: seq,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &|act| {
                    assert_matches!(
                        act.jobs.as_slice(),
                        [WorkflowActivationJob { variant: Some(Variant::ResolveChildWorkflowExecutionStart(start)) }]
                        if start.seq == seq
                            && matches!(start.status, Some(resolve_child_workflow_execution_start::Status::Cancelled(_)))
                    );
                },
                vec![],
            ),
            gen_assert_and_reply(
                &|act| {
                    assert_matches!(
                        act.jobs.as_slice(),
                        [WorkflowActivationJob { variant: Some(Variant::SignalWorkflow(signal)) }]
                        if signal.signal_name == "finish"
                    );
                },
                vec![CompleteWorkflowExecution::default().into()],
            ),
        ],
    )
    .await;
}
