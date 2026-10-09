use crate::common::CoreWfStarter;
use rstest::rstest;
use std::{sync::Arc, time::Duration};
use temporalio_client::{ActivityIdentifier, WorkflowStartOptions};
use temporalio_common::{
    error::ApplicationFailure,
    protos::{
        coresdk::workflow_commands::ActivityCancellationType,
        temporal::api::common::v1::RetryPolicy,
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityExecutionError, ActivityOptions, CancellableFuture, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum Outcome {
    Success,
    Failure,
    Cancellation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentifierType {
    TaskToken,
    ById,
}

#[rstest]
#[tokio::test]
async fn async_activity_completions(
    #[values(Outcome::Success, Outcome::Failure, Outcome::Cancellation)] outcome: Outcome,
    #[values(IdentifierType::TaskToken, IdentifierType::ById)] identifier_type: IdentifierType,
) {
    let wf_name = format!("async_activity_{outcome:?}_{identifier_type:?}");
    let mut starter = CoreWfStarter::new(&wf_name);
    // Speeds up cancel test
    starter.set_core_cfg_mutator(|wc| wc.max_heartbeat_throttle_interval = Duration::from_secs(1));
    let async_response = "agence";

    #[derive(Clone)]
    struct SharedActivityInfo {
        task_token: Vec<u8>,
        workflow_id: Option<String>,
        workflow_run_id: Option<String>,
        activity_id: String,
    }

    let (info_tx, mut info_rx) = mpsc::channel::<SharedActivityInfo>(1);

    struct AsyncActivities {
        info_tx: mpsc::Sender<SharedActivityInfo>,
    }
    #[activities]
    impl AsyncActivities {
        #[activity]
        async fn complete_async_activity(
            self: Arc<Self>,
            ctx: ActivityContext,
            expected_outcome: Outcome,
        ) -> Result<String, ActivityError> {
            // For cancellation, wait until the workflow has requested cancellation
            if expected_outcome == Outcome::Cancellation {
                tokio::select! {
                    _ = async {
                        loop {
                            let _ = ctx.record_heartbeat(()).await;
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    } => (),
                    _ = ctx.cancelled() => (),
                }
            }

            let activity_info = ctx.info();
            let info = SharedActivityInfo {
                task_token: activity_info.task_token.clone(),
                workflow_id: activity_info.workflow_id.clone(),
                workflow_run_id: activity_info.workflow_run_id.clone(),
                activity_id: activity_info.activity_id.clone(),
            };
            let _ = self.info_tx.send(info).await;
            Err(ActivityError::WillCompleteAsync)
        }
    }

    starter
        .sdk_config
        .register_activities(AsyncActivities { info_tx });

    #[workflow]
    #[derive(Default)]
    struct AsyncCompletionWorkflow;

    #[workflow_methods]
    impl AsyncCompletionWorkflow {
        #[run]
        async fn run(
            ctx: &mut WorkflowContext<Self>,
            expected_outcome: Outcome,
        ) -> WorkflowResult<()> {
            let async_response = "agence";
            let activity_future = ctx.execute_activity(
                AsyncActivities::complete_async_activity,
                expected_outcome,
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                    .retry_policy(RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    })
                    .cancellation_type(ActivityCancellationType::WaitCancellationCompleted)
                    .build(),
            );

            // For cancellation, wait a bit to let the activity start, then request cancel
            if expected_outcome == Outcome::Cancellation {
                ctx.timer(Duration::from_millis(1)).await;
                activity_future.cancel();
            }

            let activity_result = activity_future.await;

            match expected_outcome {
                Outcome::Success => {
                    assert_eq!(activity_result.expect("expected success"), async_response);
                }
                Outcome::Failure => {
                    let err = activity_result.expect_err("expected failure");
                    if let ActivityExecutionError::Failed(failure) = err {
                        // The failure we sent is wrapped as the cause
                        let cause = failure.cause().expect("cause should be present");
                        assert_eq!(cause.failure().message, "async failure reason");
                    } else {
                        panic!("expected Failed, got {err:?}");
                    }
                }
                Outcome::Cancellation => {
                    let err = activity_result.expect_err("expected cancellation");
                    assert!(
                        matches!(err, ActivityExecutionError::Cancelled(_)),
                        "expected Cancelled, got {err:?}"
                    );
                }
            }
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_workflow::<AsyncCompletionWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;
    let client = starter.get_core_client().await;

    let completion_task = tokio::spawn(async move {
        let info = info_rx.recv().await.expect("should receive activity info");

        eprintln!(
            "DEBUG: Received activity info - task_token_len={}, workflow_id={:?}, run_id={:?}, activity_id={}",
            info.task_token.len(),
            info.workflow_id,
            info.workflow_run_id,
            info.activity_id
        );

        let identifier = match identifier_type {
            IdentifierType::TaskToken => {
                eprintln!("DEBUG: Using TaskToken identifier");
                ActivityIdentifier::TaskToken(info.task_token.into())
            }
            IdentifierType::ById => {
                eprintln!("DEBUG: Using ById identifier");
                ActivityIdentifier::by_id_workflow(
                    info.workflow_id.unwrap(),
                    info.workflow_run_id.unwrap(),
                    info.activity_id,
                )
            }
        };

        let handle = client.get_async_activity_handle(identifier);
        eprintln!("DEBUG: Calling {:?} on handle", outcome);

        let result = match outcome {
            Outcome::Success => {
                handle
                    .complete(Some(async_response.to_owned()), Default::default())
                    .await
            }
            Outcome::Failure => {
                handle
                    .fail(
                        ApplicationFailure::builder(std::io::Error::other("async failure reason"))
                            .type_name("TestFailure".to_owned())
                            .build(),
                        None::<()>,
                        Default::default(),
                    )
                    .await
            }
            Outcome::Cancellation => {
                handle
                    .report_cancelation(None::<()>, Default::default())
                    .await
            }
        };
        if let Err(e) = &result {
            eprintln!(
                "ERROR: async activity completion failed: {e:?} (outcome={outcome:?}, identifier={identifier_type:?})"
            );
        }
        result.expect("async activity completion should succeed");
    });

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            AsyncCompletionWorkflow::run,
            outcome,
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    completion_task.await.unwrap();
}
