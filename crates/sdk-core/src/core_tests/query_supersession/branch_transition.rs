mod cache_admission;
mod histories;

use super::*;
use crate::{
    replay::TestHistoryBuilder, test_help::hist_to_poll_resp,
    worker::client::mocks::mock_manual_worker_client,
};
use futures_util::{FutureExt, StreamExt};
use std::time::Duration;
use temporalio_common::protos::temporal::api::{
    enums::v1::{EventType, TimeoutType},
    history::v1::WorkflowTaskTimedOutEventAttributes,
};
use tokio::sync::oneshot;

#[tokio::test]
async fn worker_keeps_polled_task_from_a_shorter_winning_history_branch() {
    tokio::time::timeout(Duration::from_secs(5), exercise_branch_transition(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn worker_keeps_completion_task_when_a_losing_branch_poll_arrives_late() {
    tokio::time::timeout(Duration::from_secs(5), exercise_branch_transition(true))
        .await
        .unwrap();
}

async fn exercise_branch_transition(returned_wins: bool) {
    let histories::BranchTasks {
        initial,
        losing: losing_response,
        losing_retry,
        winning,
    } = histories::branch_tasks(returned_wins);
    let initial_token = initial.task_token.clone().into();
    let winning_token = winning.task_token.clone().into();
    let losing_token = losing_retry.task_token.clone().into();
    let (completion_started_tx, completion_started_rx) = oneshot::channel();
    let (release_completion_tx, release_completion_rx) = oneshot::channel();
    let mut completion_started_tx = Some(completion_started_tx);
    let mut release_completion_rx = Some(release_completion_rx);
    let completion_response = RespondWorkflowTaskCompletedResponse {
        workflow_task: returned_wins.then(|| winning.clone()),
        ..Default::default()
    };
    let final_poll = if returned_wins { losing_retry } else { winning };
    let mut client = mock_manual_worker_client();
    client
        .expect_complete_workflow_task()
        .times(2..=3)
        .returning(move |completion, _| {
            if completion.task_token == initial_token {
                completion_started_tx.take().unwrap().send(()).unwrap();
                let release = release_completion_rx.take().unwrap();
                let response = completion_response.clone();
                async move {
                    release.await.unwrap();
                    Ok(response)
                }
                .boxed()
            } else if returned_wins && completion.task_token == losing_token {
                async { Err(tonic::Status::not_found("losing branch task expired")) }.boxed()
            } else {
                assert_eq!(completion.task_token, winning_token);
                async { Ok(Default::default()) }.boxed()
            }
        });
    if returned_wins {
        client.expect_fail_workflow_task().returning(|token, _, _| {
            assert_eq!(token, b"losing-branch-retry".to_vec().into());
            async { Err(tonic::Status::not_found("losing branch task expired")) }.boxed()
        });
    }
    let responses = stream::iter([initial])
        .chain(stream::once(async move {
            completion_started_rx.await.unwrap();
            losing_response
        }))
        .chain(stream::iter([final_poll]));
    let slots = Arc::new(ObservedSlots::default());
    let worker = mock_worker(fixtures::mocks_with_slots(
        client,
        responses,
        slots.clone(),
        10,
    ));
    let activation = worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        activation.jobs.as_slice(),
        [job] if matches!(job.variant, Some(Variant::InitializeWorkflow(_)))
    ));
    tokio::join!(
        fixtures::complete_empty(&worker, activation.run_id),
        async {
            // Observe the arbitration itself, whether it releases the losing or winning slot.
            slots.released.notified().await;
            assert!(
                slots
                    .releases
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|id| *id == 1 || *id == 2)
            );
            release_completion_tx.send(()).unwrap();
        },
    );
    let next = worker.poll_workflow_activation().await.unwrap();
    fixtures::complete_empty(&worker, next.run_id).await;
    assert!(matches!(
        next.jobs.as_slice(),
        [job] if matches!(&job.variant, Some(Variant::SignalWorkflow(signal)) if signal.signal_name == "winning")
    ));
    fixtures::drain_allowed_activations_until_shutdown(&worker, |job| {
        matches!(
            &job.variant,
            Some(Variant::RemoveFromCache(_)) | Some(Variant::SignalWorkflow(_))
        )
    })
    .await;
    worker.shutdown().await;
    slots.assert_released_once(3);
}
