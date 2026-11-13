use std::{sync::Arc, time::Duration};
use temporalio_common::protos::{
    coresdk::{AsJsonPayloadExt, workflow_activation::WorkflowActivation},
    temporal::api::history::v1::{
        History, TimerStartedEventAttributes, WorkflowExecutionCompletedEventAttributes,
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    LocalActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::WorkerInterceptor,
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    test_help::CoreInternalFlags,
};

struct HeartbeatActivity;

#[activities]
impl HeartbeatActivity {
    #[activity]
    async fn run(self: Arc<Self>, _: ActivityContext, _: ()) -> Result<String, ActivityError> {
        panic!("Replay must resolve this activity from its recorded marker");
    }
}

#[workflow]
#[derive(Default)]
struct HeartbeatWorkflow;

#[workflow_methods]
impl HeartbeatWorkflow {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        // Both timer tasks permit safe flag adoption before any LA is outstanding.
        ctx.timer(Duration::from_millis(1)).await;
        ctx.timer(Duration::from_millis(1)).await;
        let clock = ctx.clone();
        let started_at = ctx.workflow_time().unwrap();
        let (result, condition) = temporalio_sdk::workflows::join!(
            ctx.execute_local_activity(HeartbeatActivity::run, (), LocalActivityOptions::default()),
            ctx.wait_condition(move |_| clock.workflow_time().unwrap() > started_at)
        );
        condition?;
        Ok(result?)
    }
}

struct NoEmptyActivations;

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for NoEmptyActivations {
    async fn on_workflow_activation(&self, activation: &WorkflowActivation) -> anyhow::Result<()> {
        assert!(!activation.jobs.is_empty());
        Ok(())
    }
}

// Derived from a live run with adoption disabled: two timers, two LA heartbeat completions,
// then the LA marker and workflow result in the fifth task. The activity was released only after
// task 4's completion appeared in history. Jobless heartbeats must not activate lang or shift
// the LA's activation index when the chunking flag is added.
fn heartbeat_history(flag_task: Option<usize>) -> History {
    let mut history = TestHistoryBuilder::default();
    history.add_wfe_started_with_wft_timeout(Duration::from_secs(2));
    for task in 1..=5 {
        history.add_full_wf_task();
        let mut flags = if task == 1 {
            vec![
                CoreInternalFlags::IdAndTypeDeterminismChecks,
                CoreInternalFlags::UpsertSearchAttributeOnPatch,
                CoreInternalFlags::MoveTerminalCommands,
                CoreInternalFlags::NexusOperationDeterminismChecks,
            ]
        } else {
            vec![]
        };
        if Some(task) == flag_task {
            flags.push(CoreInternalFlags::DisableWorkflowTaskChunking);
        }
        history.set_flags_last_wft(&flags, &[]);
        if task <= 2 {
            let timer_id = task.to_string();
            let started = history.add(TimerStartedEventAttributes {
                timer_id: timer_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(1).try_into().unwrap()),
                workflow_task_completed_event_id: if task == 1 { 4 } else { 9 },
            });
            history.add_timer_fired(started, timer_id);
        }
    }
    history.add_local_activity_marker(
        1,
        "1",
        Some("done".as_json_payload().unwrap()),
        None,
        |marker| {
            marker.activity_type = "HeartbeatActivity::run".to_owned();
            marker.complete_time = Some(prost_types::Timestamp {
                seconds: 20,
                nanos: 0,
            });
            marker.original_schedule_time = Some(prost_types::Timestamp {
                seconds: 13,
                nanos: 0,
            });
            marker.activation_index = Some(0);
        },
    );
    history.add(WorkflowExecutionCompletedEventAttributes {
        workflow_task_completed_event_id: 20,
        result: Some("done".as_json_payload().unwrap().into()),
        ..Default::default()
    });
    let mut events = history.get_full_history_info().unwrap().into_events();
    for event in &mut events {
        // Preserve advancing workflow clocks without depending on the wall clock during replay.
        event.event_time = Some(prost_types::Timestamp {
            seconds: event.event_id,
            nanos: 0,
        });
    }
    History { events }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[case::flag_from_start(Some(1))]
#[case::never_flagged(None)]
#[case::flag_before_heartbeats(Some(2))]
#[tokio::test]
async fn workflow_task_chunking_replays_compatible_histories(#[case] flag_task: Option<usize>) {
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<HeartbeatWorkflow>()
            .unwrap()
            .worker_interceptor(NoEmptyActivations)
            .build(),
    )
    .unwrap();
    replayer
        .replay_workflow(heartbeat_history(flag_task).into())
        .await
        .unwrap();
}
