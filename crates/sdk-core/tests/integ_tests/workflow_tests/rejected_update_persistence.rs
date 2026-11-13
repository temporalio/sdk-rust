use crate::common::{CoreWfStarter, activity_functions::StdActivities};
use assert_matches::assert_matches;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use temporalio_client::{WorkflowExecuteUpdateOptions, errors::WorkflowUpdateError};
use temporalio_common::protos::{
    coresdk::{
        common::extract_local_activity_marker_details,
        workflow_activation::{WorkflowActivation, workflow_activation_job::Variant},
        workflow_commands::{update_response, workflow_command},
        workflow_completion::{WorkflowActivationCompletion, workflow_activation_completion},
    },
    temporal::api::{
        enums::v1::EventType,
        history::v1::{History, history_event::Attributes},
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, LocalActivityOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::WorkerInterceptor,
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};
use temporalio_sdk_core::test_help::CoreInternalFlags;
use tokio_util::sync::CancellationToken;

struct GatedActivity {
    started: CancellationToken,
    release: CancellationToken,
}

#[activities]
impl GatedActivity {
    #[activity]
    async fn run(self: Arc<Self>, _: ActivityContext, _: ()) -> Result<(), ActivityError> {
        self.started.cancel();
        self.release.cancelled().await;
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct RejectedUpdatePersistenceWf;

#[workflow_methods]
impl RejectedUpdatePersistenceWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        // Let flag adoption finish before an LA can prevent a safe chunking transition.
        ctx.timer(Duration::from_millis(1)).await;
        ctx.timer(Duration::from_millis(1)).await;
        let clock = ctx.clone();
        let started_at = ctx.workflow_time().unwrap();
        let (activity, condition) = temporalio_sdk::workflows::join!(
            ctx.execute_local_activity(GatedActivity::run, (), LocalActivityOptions::default(),),
            async {
                ctx.wait_condition(move |_| clock.workflow_time().unwrap() > started_at)
                    .await?;
                assert!(ctx.patched("clock-condition"));
                // A clock-derived ID makes an early condition wakeup observable to replay's
                // determinism checks, rather than just buffering an otherwise identical command.
                ctx.execute_activity(
                    StdActivities::default,
                    (),
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                        .activity_id(format!("clock-{:?}", ctx.workflow_time().unwrap()))
                        .build(),
                )
                .await?;
                Ok::<_, temporalio_sdk::WorkflowTermination>(())
            }
        );
        activity?;
        condition?;
        Ok(())
    }

    #[update_validator(reject)]
    fn validate_reject(
        &self,
        _: &WorkflowContextView,
        _: &(),
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("rejected".into())
    }

    #[update]
    fn reject(&mut self, _: &mut SyncWorkflowContext<Self>, _: ()) {}
}

#[derive(Clone, Default)]
struct RejectionTask(
    Arc<Mutex<Option<prost_types::Timestamp>>>,
    Option<CancellationToken>,
);

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for RejectionTask {
    async fn on_workflow_activation(&self, activation: &WorkflowActivation) -> anyhow::Result<()> {
        assert!(!activation.jobs.is_empty());
        if activation
            .jobs
            .iter()
            .any(|j| matches!(j.variant, Some(Variant::DoUpdate(_))))
        {
            assert!(!activation.is_replaying);
            assert_eq!(
                activation.jobs.len(),
                1,
                "The disputed task must be update-only"
            );
            *self.0.lock().unwrap() = activation.timestamp;
        }
        Ok(())
    }

    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        if let Some(release) = &self.1
            && let Some(workflow_activation_completion::Status::Successful(success)) =
                &completion.status
            && success.commands.iter().any(|command| {
                matches!(
                    &command.variant,
                    Some(workflow_command::Variant::UpdateResponse(response))
                        if matches!(response.response, Some(update_response::Response::Rejected(_)))
                )
            })
        {
            // Resolve before this WFT completes to record the LA resolution after its rejected update.
            release.cancel();
        }
    }
}

// The pending LA forces heartbeat continuation tasks, which persist even when their only activation
// jobs are rejected updates.
#[rstest::rstest]
#[case::resolution_in_next_task(false)]
#[case::resolution_in_same_task(true)]
#[tokio::test]
async fn rejected_update_on_forced_task_replays_real_history(
    #[case] resolve_in_rejected_task: bool,
) {
    let mut starter = CoreWfStarter::new("rejected_update_on_forced_task_replays_real_history");
    starter.workflow_options.task_timeout = Some(Duration::from_secs(2));
    starter
        .sdk_config
        .register_workflow::<RejectedUpdatePersistenceWf>()
        .unwrap();
    starter.sdk_config.register_activities(StdActivities);
    let started = CancellationToken::new();
    let release = CancellationToken::new();
    starter.sdk_config.register_activities(GatedActivity {
        started: started.clone(),
        release: release.clone(),
    });
    let mut worker = starter.worker().await;
    let rejection_task = RejectionTask(
        Default::default(),
        resolve_in_rejected_task.then(|| release.clone()),
    );
    let handle = worker
        .submit_workflow(
            RejectedUpdatePersistenceWf::run,
            (),
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();
    let exercise = async {
        // Queue the update while the LA is blocked so it arrives on a heartbeat-forced task.
        started.cancelled().await;
        let rejection = handle
            .execute_update(
                RejectedUpdatePersistenceWf::reject,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap_err();
        assert_matches!(rejection, WorkflowUpdateError::Failed(failure) if failure.message == "rejected");
        let timestamp = rejection_task
            .0
            .lock()
            .unwrap()
            .expect("update activation observed");
        let events = handle
            .fetch_history(Default::default())
            .into_events()
            .await
            .unwrap();
        let started = events
            .iter()
            .find(|e| {
                e.event_type() == EventType::WorkflowTaskStarted && e.event_time == Some(timestamp)
            })
            .expect("server persisted the rejected-update-only task's start");
        assert_matches!(
            started.attributes.as_ref(),
            Some(Attributes::WorkflowTaskStartedEventAttributes(a))
                if a.request_id == "request-from-RespondWorkflowTaskCompleted"
        );
        let completed = events
            .iter()
            .find(|e| {
                matches!(
                    e.attributes.as_ref(),
                    Some(Attributes::WorkflowTaskCompletedEventAttributes(a))
                        if a.started_event_id == started.event_id
                )
            })
            .expect("server persisted the rejected-update-only task's completion");
        assert_eq!(completed.event_id, started.event_id + 1);
        if resolve_in_rejected_task {
            let marker = events
                .iter()
                .find_map(|event| match &event.attributes {
                    Some(Attributes::MarkerRecordedEventAttributes(attrs))
                        if attrs.marker_name == "core_local_activity"
                            && attrs.workflow_task_completed_event_id == completed.event_id =>
                    {
                        Some(attrs)
                    }
                    _ => None,
                })
                .expect("LA result must be recorded in the rejected update's task");
            let (data, _) = extract_local_activity_marker_details(&mut marker.details.clone());
            assert_eq!(data.unwrap().activation_index, Some(1));
        } else {
            assert_eq!(
                events
                    .iter()
                    .find(|e| e.event_id == completed.event_id + 1)
                    .unwrap()
                    .event_type(),
                EventType::WorkflowTaskScheduled,
                "rejected-update-only tasks must not advance workflow code",
            );
            assert!(
                !events
                    .iter()
                    .any(|e| e.event_type() == EventType::MarkerRecorded)
            );
        }
        assert!(
            !events
                .iter()
                .any(|e| e.event_type() == EventType::WorkflowExecutionUpdateAccepted)
        );
        if cfg!(feature = "disable_workflow_task_chunking") {
            let before_la = events
                .iter()
                .rfind(|e| e.event_type() == EventType::TimerFired)
                .unwrap()
                .event_id;
            assert!(
                events.iter().any(|e| matches!(
                    e.attributes.as_ref(),
                    Some(Attributes::WorkflowTaskCompletedEventAttributes(a))
                        if e.event_id < before_la
                            && a.sdk_metadata.as_ref().is_some_and(|m| m.core_used_flags.contains(
                                &(CoreInternalFlags::DisableWorkflowTaskChunking as u32)
                            ))
                )),
                "flag 5 must be recorded before the LA starts"
            );
        }
        // Holding the LA until the server has committed this task prevents its marker
        // from supplying a replay job that could conceal the update-only/empty-activation mismatch.
        release.cancel();
    };
    let ((), result) = tokio::join!(
        exercise,
        worker.run_until_done_intercepted(Some(rejection_task.clone()))
    );
    result.unwrap();
    handle.get_result(Default::default()).await.unwrap();
    let events = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<RejectedUpdatePersistenceWf>()
            .unwrap()
            .worker_interceptor(rejection_task.clone())
            .build(),
    )
    .unwrap();
    replayer
        .replay_workflow(History { events }.into())
        .await
        .unwrap();
}
