//! Verify that `EventGroupMarker`s attached to lang-side options propagate all the
//! way down to the server-side `Command`s issued by Core. One mocked test per command
//! kind we currently expose `event_group_markers` on: activity, child workflow, timer,
//! local activity.
//!
//! Plus one end-to-end test against a real server, verifying that the markers also
//! land on the resulting `HistoryEvent` (i.e. the server persists what we send).
//!
//! Event Groups are not implemented in the Rust SDK, so these tests build markers as raw
//! protos and set them through the `#[doc(hidden)]` `event_group_markers` option fields,
//! which exist for that purpose only.

use std::time::Duration;

use crate::common::{
    CoreWfStarter, activity_functions::StdActivities, build_fake_sdk_with_options,
    mock_sdk_cfg_with_options,
};
use temporalio_client::{UntypedWorkflow, WorkflowStartOptions};
use temporalio_common::{
    data_converters::RawValue,
    protos::{
        coresdk::AsJsonPayloadExt,
        temporal::api::{
            enums::v1::{CommandType, EventType},
            sdk::v1::{
                EventGroupMarker,
                event_group_marker::{Label, Variant},
            },
        },
    },
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, TimerOptions, WorkflowContext,
    WorkflowResult,
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, canned_histories},
    test_help::MockPollCfg,
};

#[tokio::test]
async fn pass_event_group_markers_on_schedule_activity() {
    let t = canned_histories::single_activity("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let expected_markers = vec![label_marker("activity-group", "activity-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::ScheduleActivityTask
                );
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    #[workflow]
    struct ActivityWithGroupWorkflow {
        event_group_markers: Vec<EventGroupMarker>,
    }

    #[workflow_methods(factory_only)]
    impl ActivityWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let event_group_markers = ctx.state(|wf| wf.event_group_markers.clone());
            ctx.execute_activity(
                StdActivities::default,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .event_group_markers(event_group_markers)
                    .build(),
            )
            .await?;
            Ok(())
        }
    }

    let mut worker = mock_sdk_cfg_with_options(
        mock_cfg,
        |_| {},
        |options| {
            options
                .register_workflow_with_factory(move || ActivityWithGroupWorkflow {
                    event_group_markers: expected_markers.clone(),
                })
                .unwrap();
        },
    );
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn pass_event_group_markers_on_start_child_workflow() {
    let wf_id = "1";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_child_workflow(wf_id);
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_markers = vec![label_marker("child-group", "child-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
                );
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    #[workflow]
    struct ChildWithGroupWorkflow {
        child_wf_id: String,
        event_group_markers: Vec<EventGroupMarker>,
    }

    #[workflow_methods(factory_only)]
    impl ChildWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let (child_wf_id, event_group_markers) =
                ctx.state(|wf| (wf.child_wf_id.clone(), wf.event_group_markers.clone()));
            ctx.start_child_workflow(
                UntypedWorkflow::new("child"),
                RawValue::new(vec![]),
                ChildWorkflowOptions::builder()
                    .workflow_id(child_wf_id)
                    .event_group_markers(event_group_markers)
                    .build(),
            )
            .await?;
            Ok(())
        }
    }

    let child_wf_id = wf_id.to_string();
    let event_group_markers_for_wf = expected_markers.clone();
    let mut worker = mock_sdk_cfg_with_options(
        mock_cfg,
        |_| {},
        |options| {
            options
                .register_workflow_with_factory(move || ChildWithGroupWorkflow {
                    child_wf_id: child_wf_id.clone(),
                    event_group_markers: event_group_markers_for_wf.clone(),
                })
                .unwrap();
        },
    );
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn pass_event_group_markers_on_start_timer() {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let expected_markers = vec![label_marker("timer-group", "timer-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    #[workflow]
    struct TimerWithGroupWorkflow {
        event_group_markers: Vec<EventGroupMarker>,
    }

    #[workflow_methods(factory_only)]
    impl TimerWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let event_group_markers = ctx.state(|wf| wf.event_group_markers.clone());
            ctx.timer(
                TimerOptions::builder(Duration::from_secs(1))
                    .event_group_markers(event_group_markers)
                    .build(),
            )
            .await;
            Ok(())
        }
    }

    let event_group_markers_for_wf = expected_markers.clone();
    let mut worker = mock_sdk_cfg_with_options(
        mock_cfg,
        |_| {},
        |options| {
            options
                .register_workflow_with_factory(move || TimerWithGroupWorkflow {
                    event_group_markers: event_group_markers_for_wf.clone(),
                })
                .unwrap();
        },
    );
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

/// Local activities pose some particular challenges: the corresponding `RecordMarker` command
/// only gets created at a later point, after the local activity completes execution.
/// server, so instead of a command of their own they produce a `RecordMarker` command that
/// Core synthesizes when the activity resolves. Markers attached to the `ScheduleLocalActivity`
/// command have to survive that indirection and end up on the marker command.
#[tokio::test]
async fn pass_event_group_markers_on_schedule_local_activity() {
    let t = canned_histories::single_local_activity("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_markers = vec![label_marker("local-activity-group", "local-activity-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        // The activity resolves within the same workflow task that scheduled it, so the marker
        // command is flushed together with the workflow completion rather than on its own.
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 2);
            assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
            assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            assert_eq!(
                wft.commands[1].command_type(),
                CommandType::CompleteWorkflowExecution
            );
            assert!(wft.commands[1].event_group_markers.is_empty());
        });
    });

    #[workflow]
    struct LocalActivityWithGroupWorkflow {
        event_group_markers: Vec<EventGroupMarker>,
    }

    #[workflow_methods(factory_only)]
    impl LocalActivityWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let event_group_markers = ctx.state(|wf| wf.event_group_markers.clone());
            ctx.execute_local_activity(
                StdActivities::default,
                (),
                LocalActivityOptions::builder()
                    .event_group_markers(event_group_markers)
                    .build(),
            )
            .await?;
            Ok(())
        }
    }

    // Unlike the tests above, this one drives a plain SDK worker off the canned history rather
    // than submitting a workflow: the local activity must actually run for a marker to be
    // recorded, so the worker needs the activity implementation registered too.
    let mut worker = build_fake_sdk_with_options(mock_cfg, |options| {
        options
            .register_workflow_with_factory(move || LocalActivityWithGroupWorkflow {
                event_group_markers: expected_markers.clone(),
            })
            .unwrap()
            .register_activities(StdActivities);
    });
    worker.run().await.unwrap();
}

// Constants used by the real-server test below; defining them at module scope so the
// workflow body and the assertion can construct the same marker independently.
const PERSIST_TEST_MARKER_ID: &str = "persist-test";
const PERSIST_TEST_MARKER_LABEL: &str = "persist-test-label";
const PERSIST_TEST_LA_MARKER_ID: &str = "persist-test-la";
const PERSIST_TEST_LA_MARKER_LABEL: &str = "persist-test-la-label";

#[workflow]
#[derive(Default)]
pub(crate) struct ActivityEventGroupPersistsWf;

#[workflow_methods]
impl ActivityEventGroupPersistsWf {
    #[run(name = "event_group_markers_persist_to_history_events")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.execute_activity(
            StdActivities::default,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .event_group_markers(vec![label_marker(
                    PERSIST_TEST_MARKER_ID,
                    PERSIST_TEST_MARKER_LABEL,
                )])
                .build(),
        )
        .await?;
        ctx.execute_local_activity(
            StdActivities::default,
            (),
            LocalActivityOptions::builder()
                .start_to_close_timeout(Duration::from_secs(5))
                .event_group_markers(vec![label_marker(
                    PERSIST_TEST_LA_MARKER_ID,
                    PERSIST_TEST_LA_MARKER_LABEL,
                )])
                .build(),
        )
        .await?;
        Ok(())
    }
}

/// End-to-end: a marker attached to a command must also land on the resulting history event
/// after the server persists it. Covers both an ordinary activity (`ActivityTaskScheduled`) and
/// a local activity, which surfaces as the `MarkerRecorded` event Core writes on resolution.
#[tokio::test]
async fn event_group_markers_persist_to_history_events() {
    let wf_name = "event_group_markers_persist_to_history_events";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_activities(StdActivities)
        .register_workflow::<ActivityEventGroupPersistsWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let scheduled_events: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::ActivityTaskScheduled)
        .collect();
    assert_eq!(scheduled_events.len(), 1);
    assert_eq!(
        scheduled_events[0].event_group_markers,
        vec![label_marker(
            PERSIST_TEST_MARKER_ID,
            PERSIST_TEST_MARKER_LABEL
        )]
    );

    let marker_events: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::MarkerRecorded)
        .collect();
    assert_eq!(marker_events.len(), 1);
    assert_eq!(
        marker_events[0].event_group_markers,
        vec![label_marker(
            PERSIST_TEST_LA_MARKER_ID,
            PERSIST_TEST_LA_MARKER_LABEL
        )]
    );
}

fn label_marker(id: &str, label: &str) -> EventGroupMarker {
    EventGroupMarker {
        variant: Some(Variant::Label(Label {
            id: id.to_string(),
            label: Some(label.as_json_payload().unwrap()),
        })),
    } as EventGroupMarker
}
