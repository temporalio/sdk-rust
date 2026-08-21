use crate::{
    replay::{TestHistoryBuilder, canned_histories, default_act_sched},
    test_help::{MockPollCfg, build_mock_pollers, mock_worker, start_timer_cmd},
};
use std::time::Duration;
use temporalio_common::protos::{
    coresdk::{
        AsJsonPayloadExt,
        child_workflow::ChildWorkflowCancellationType,
        nexus::NexusOperationCancellationType,
        workflow_commands::{
            ActivityCancellationType, CancelChildWorkflowExecution, CancelTimer,
            CompleteWorkflowExecution, RequestCancelActivity, RequestCancelNexusOperation,
            ScheduleActivity, ScheduleNexusOperation, SetPatchMarker, StartChildWorkflowExecution,
            WorkflowCommand, workflow_command,
        },
        workflow_completion::{WorkflowActivationCompletion, workflow_activation_completion},
    },
    temporal::api::{
        command::v1::Command,
        common::v1::Payload,
        enums::v1::{CommandType, EventType},
        history::v1::{
            NexusOperationCancelRequestedEventAttributes, NexusOperationCanceledEventAttributes,
            NexusOperationScheduledEventAttributes, history_event,
        },
        sdk::v1::{
            EventGroupMarker, UserMetadata,
            event_group_marker::{Label, Variant},
        },
    },
};

fn plain(cmd: impl Into<workflow_command::Variant>) -> WorkflowCommand {
    cmd.into().into()
}

/// Tag a command with a marker and a summary both derived from `group`, so that a single name
/// identifies the annotations expected downstream and both fields are checked to travel together.
fn annotate(cmd: impl Into<workflow_command::Variant>, group: &str) -> WorkflowCommand {
    let mut cmd = plain(cmd);
    cmd.event_group_markers = vec![EventGroupMarker {
        variant: Some(Variant::Label(Label {
            id: group.to_string(),
            label: Some(group.as_json_payload().unwrap()),
        })),
    }];
    cmd.user_metadata = Some(UserMetadata {
        summary: Some(group.as_json_payload().unwrap()),
        details: None,
    });
    cmd
}

#[track_caller]
fn assert_annotated(cmd: &Command, group: &str) {
    let expected = annotate(CompleteWorkflowExecution::default(), group);
    assert_eq!(cmd.event_group_markers, expected.event_group_markers);
    assert_eq!(cmd.user_metadata, expected.user_metadata);
}

fn complete(run_id: String, cmds: Vec<WorkflowCommand>) -> WorkflowActivationCompletion {
    WorkflowActivationCompletion {
        run_id,
        status: Some(workflow_activation_completion::Status::Successful(
            cmds.into(),
        )),
        ..Default::default()
    }
}

#[rstest::rstest]
#[tokio::test]
async fn cancel_timer_command_is_annotated(#[values(false, true)] lang_annotates_cancel: bool) {
    let cancelled_timer_seq = 2;
    let t = canned_histories::cancel_timer("1", &cancelled_timer_seq.to_string());
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_group = if lang_annotates_cancel {
        "cancel-group"
    } else {
        "timer-group"
    };
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|_| {}).then(move |wft| {
            assert_eq!(wft.commands[0].command_type(), CommandType::CancelTimer);
            assert_annotated(&wft.commands[0], expected_group);
        });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![
            annotate(
                start_timer_cmd(cancelled_timer_seq, Duration::from_secs(1)),
                "timer-group",
            ),
            plain(start_timer_cmd(1, Duration::from_secs(1))),
        ],
    ))
    .await
    .unwrap();

    let cancel = CancelTimer {
        seq: cancelled_timer_seq,
    };
    let cancel = if lang_annotates_cancel {
        annotate(cancel, "cancel-group")
    } else {
        plain(cancel)
    };
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![cancel, plain(CompleteWorkflowExecution::default())],
    ))
    .await
    .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn cancel_activity_command_is_annotated(#[values(false, true)] lang_annotates_cancel: bool) {
    let activity_seq = 1;
    let t = canned_histories::cancel_scheduled_activity_with_activity_task_cancel(
        "fake_activity",
        "signal",
    );
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_group = if lang_annotates_cancel {
        "cancel-group"
    } else {
        "activity-group"
    };
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|_| {}).then(move |wft| {
            assert_eq!(
                wft.commands[0].command_type(),
                CommandType::RequestCancelActivityTask
            );
            assert_annotated(&wft.commands[0], expected_group);
        });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![annotate(
            ScheduleActivity {
                seq: activity_seq,
                activity_id: "fake_activity".to_string(),
                cancellation_type: ActivityCancellationType::WaitCancellationCompleted as i32,
                ..default_act_sched()
            },
            "activity-group",
        )],
    ))
    .await
    .unwrap();

    let cancel = RequestCancelActivity { seq: activity_seq };
    let cancel = if lang_annotates_cancel {
        annotate(cancel, "cancel-group")
    } else {
        plain(cancel)
    };
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(act.run_id, vec![cancel]))
        .await
        .unwrap();

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![plain(CompleteWorkflowExecution::default())],
    ))
    .await
    .unwrap();
}

/// Cancelling a child is doubly indirect: the child machine asks Core to create a whole other
/// machine for the external cancel, and that machine's command is the one the server sees.
#[rstest::rstest]
#[tokio::test]
async fn cancel_child_workflow_command_is_annotated(
    #[values(false, true)] lang_annotates_cancel: bool,
) {
    let child_wf_id = "child-1";
    let child_seq = 1;
    let t = canned_histories::single_child_workflow_try_cancelled(child_wf_id);
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_group = if lang_annotates_cancel {
        "cancel-group"
    } else {
        "child-group"
    };
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|_| {}).then(move |wft| {
            assert_eq!(
                wft.commands[0].command_type(),
                CommandType::RequestCancelExternalWorkflowExecution
            );
            assert_annotated(&wft.commands[0], expected_group);
        });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![annotate(
            StartChildWorkflowExecution {
                seq: child_seq,
                workflow_id: child_wf_id.to_string(),
                workflow_type: "child".to_string(),
                cancellation_type: ChildWorkflowCancellationType::TryCancel as i32,
                ..Default::default()
            },
            "child-group",
        )],
    ))
    .await
    .unwrap();

    let cancel = CancelChildWorkflowExecution {
        child_workflow_seq: child_seq,
        reason: "because".to_string(),
    };
    let cancel = if lang_annotates_cancel {
        annotate(cancel, "cancel-group")
    } else {
        plain(cancel)
    };
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(act.run_id, vec![cancel]))
        .await
        .unwrap();

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![plain(CompleteWorkflowExecution::default())],
    ))
    .await
    .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn cancel_nexus_operation_command_is_annotated(
    #[values(false, true)] lang_annotates_cancel: bool,
) {
    let nexus_seq = 1;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add(NexusOperationScheduledEventAttributes {
        endpoint: "endpoint".to_string(),
        service: "service".to_string(),
        operation: "operation".to_string(),
        ..Default::default()
    });
    t.add_we_signaled(
        "signal",
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
            external_payloads: Default::default(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::NexusOperationCancelRequestedEventAttributes(
            NexusOperationCancelRequestedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add(
        history_event::Attributes::NexusOperationCanceledEventAttributes(
            NexusOperationCanceledEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_group = if lang_annotates_cancel {
        "cancel-group"
    } else {
        "nexus-group"
    };
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|_| {}).then(move |wft| {
            assert_eq!(
                wft.commands[0].command_type(),
                CommandType::RequestCancelNexusOperation
            );
            assert_annotated(&wft.commands[0], expected_group);
        });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![annotate(
            ScheduleNexusOperation {
                seq: nexus_seq,
                endpoint: "endpoint".to_string(),
                service: "service".to_string(),
                operation: "operation".to_string(),
                cancellation_type: NexusOperationCancellationType::WaitCancellationCompleted as i32,
                ..Default::default()
            },
            "nexus-group",
        )],
    ))
    .await
    .unwrap();

    let cancel = RequestCancelNexusOperation { seq: nexus_seq };
    let cancel = if lang_annotates_cancel {
        annotate(cancel, "cancel-group")
    } else {
        plain(cancel)
    };
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(act.run_id, vec![cancel]))
        .await
        .unwrap();

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![plain(CompleteWorkflowExecution::default())],
    ))
    .await
    .unwrap();
}

/// The `TemporalChangeVersion` upsert exists only to make the patch searchable, so it belongs to
/// the same group as the patch marker rather than to no group at all.
#[tokio::test]
async fn patch_search_attribute_upsert_is_annotated() {
    let patch_id = "the-patch";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_has_change_marker(patch_id, false);
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|wft| {
            assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
            assert_annotated(&wft.commands[0], "patch-group");
            assert_eq!(
                wft.commands[1].command_type(),
                CommandType::UpsertWorkflowSearchAttributes
            );
            assert_annotated(&wft.commands[1], "patch-group");
        });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(complete(
        act.run_id,
        vec![
            annotate(
                SetPatchMarker {
                    patch_id: patch_id.to_string(),
                    deprecated: false,
                },
                "patch-group",
            ),
            plain(CompleteWorkflowExecution::default()),
        ],
    ))
    .await
    .unwrap();
}
