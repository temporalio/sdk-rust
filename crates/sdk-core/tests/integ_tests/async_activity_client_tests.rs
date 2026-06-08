use crate::common::CoreWfStarter;
use rstest::rstest;
use std::{sync::Arc, time::Duration};
use temporalio_client::{ActivityIdentifier, WorkflowStartOptions};
use temporalio_common::protos::{
    coresdk::{AsJsonPayloadExt, workflow_commands::ActivityCancellationType},
    temporal::api::{
        common::v1::RetryPolicy,
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
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
        workflow_id: String,
        run_id: String,
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
                            ctx.record_heartbeat(vec![]);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    } => (),
                    _ = ctx.cancelled() => (),
                }
            }

            let activity_info = ctx.info();
            let wf_exec = activity_info.workflow_execution.as_ref().unwrap();
            let info = SharedActivityInfo {
                task_token: activity_info.task_token.clone(),
                workflow_id: wf_exec.workflow_id.clone(),
                run_id: wf_exec.run_id.clone(),
                activity_id: activity_info.activity_id.clone(),
            };
            let _ = self.info_tx.send(info).await;
            Err(ActivityError::WillCompleteAsync)
        }
    }

    starter
        .sdk_config
        .register_activities(AsyncActivities { info_tx });

    let mut worker = starter.worker().await;
    let client = starter.get_client().await;

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
            let activity_future = ctx.start_activity(
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

    worker
        .register_workflow::<AsyncCompletionWorkflow>()
        .unwrap();

    let completion_task = tokio::spawn(async move {
        let info = info_rx.recv().await.expect("should receive activity info");

        eprintln!(
            "DEBUG: Received activity info - task_token_len={}, workflow_id={}, run_id={}, activity_id={}",
            info.task_token.len(),
            info.workflow_id,
            info.run_id,
            info.activity_id
        );

        let identifier = match identifier_type {
            IdentifierType::TaskToken => {
                eprintln!("DEBUG: Using TaskToken identifier");
                ActivityIdentifier::TaskToken(info.task_token.into())
            }
            IdentifierType::ById => {
                eprintln!("DEBUG: Using ById identifier");
                ActivityIdentifier::by_id(info.workflow_id, info.run_id, info.activity_id)
            }
        };

        let handle = client.get_async_activity_handle(identifier);
        eprintln!("DEBUG: Calling {:?} on handle", outcome);

        let result = match outcome {
            Outcome::Success => {
                handle
                    .complete(Some(async_response.as_json_payload().unwrap().into()))
                    .await
            }
            Outcome::Failure => {
                handle
                    .fail(
                        Failure {
                            message: "async failure reason".to_string(),
                            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                                ApplicationFailureInfo {
                                    r#type: "TestFailure".to_string(),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        },
                        None,
                    )
                    .await
            }
            Outcome::Cancellation => handle.report_cancelation(None).await,
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
