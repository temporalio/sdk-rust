use super::*;
use crate::{
    replay::TestHistoryBuilder, test_help::hist_to_poll_resp,
    worker::client::mocks::mock_manual_worker_client,
};
use futures_util::{FutureExt, StreamExt};
use std::time::Duration;
use temporalio_common::protos::temporal::api::{
    enums::v1::{EventType, TimeoutType},
    history::v1::{WorkflowTaskTimedOutEventAttributes, history_event::Attributes},
    query::v1::WorkflowQuery,
    workflowservice::v1::PollWorkflowTaskQueueResponse,
};
use tokio::sync::oneshot;

#[tokio::test]
async fn worker_keeps_newer_polled_task_when_completion_returns_older_task() {
    tokio::time::timeout(Duration::from_secs(5), exercise_delayed_completion(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn worker_keeps_newer_polled_task_when_completion_returns_older_task_during_eviction() {
    tokio::time::timeout(Duration::from_secs(5), exercise_delayed_completion(true))
        .await
        .unwrap();
}

async fn exercise_delayed_completion(evict: bool) {
    let (initial, returned, superseded, query, newer) = timed_out_tasks();
    assert!(initial.started_event_id < returned.started_event_id);
    assert!(returned.started_event_id < superseded.started_event_id);
    assert!(superseded.started_event_id < newer.started_event_id);
    let initial_token = initial.task_token.clone().into();
    let returned_token = returned.task_token.clone().into();
    let newer_token = newer.task_token.clone().into();
    let query_token = query.task_token.clone().into();
    let (completion_started_tx, completion_started_rx) = oneshot::channel();
    let (release_completion_tx, release_completion_rx) = oneshot::channel();
    let mut completion_started_tx = Some(completion_started_tx);
    let mut release_completion_rx = Some(release_completion_rx);
    let mut client = mock_manual_worker_client();
    client
        .expect_complete_workflow_task()
        .times(3)
        .returning(move |completion, _| {
            if completion.task_token == initial_token {
                completion_started_tx.take().unwrap().send(()).unwrap();
                let release = release_completion_rx.take().unwrap();
                let response = RespondWorkflowTaskCompletedResponse {
                    workflow_task: Some(returned.clone()),
                    ..Default::default()
                };
                async move {
                    release.await.unwrap();
                    Ok(response)
                }
                .boxed()
            } else if completion.task_token == returned_token {
                async { Err(tonic::Status::not_found("returned task expired")) }.boxed()
            } else {
                assert_eq!(completion.task_token, newer_token);
                async { Ok(Default::default()) }.boxed()
            }
        });
    client
        .expect_respond_legacy_query()
        .times(1)
        .returning(move |token, result| {
            assert_eq!(token, query_token);
            assert!(matches!(result, LegacyQueryResult::Succeeded(_)));
            async { Ok(Default::default()) }.boxed()
        });
    let slots = Arc::new(ObservedSlots::default());
    let responses = stream::iter([initial])
        .chain(stream::once(async move {
            completion_started_rx.await.unwrap();
            superseded
        }))
        .chain(stream::iter([query, newer]));
    let worker = mock_worker(fixtures::mocks_with_slots(
        client,
        responses,
        slots.clone(),
        10,
    ));
    let initial_activation = worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        initial_activation.jobs.as_slice(),
        [job] if matches!(job.variant, Some(Variant::InitializeWorkflow(_)))
    ));
    let initial_clock = initial_activation.timestamp;
    let run_id = initial_activation.run_id.clone();
    tokio::join!(
        fixtures::complete_empty(&worker, initial_activation.run_id),
        async {
            // A slot is released only after the newer poll replaces buffered work.
            slots.replaced.notified().await;
            if evict {
                worker.request_workflow_eviction(&run_id);
            }
            release_completion_tx.send(()).unwrap();
        },
    );
    let mut queries_answered = 0;
    let mut evictions = 0;
    loop {
        let activation = worker.poll_workflow_activation().await.unwrap();
        let query = activation.jobs.iter().find_map(|job| match &job.variant {
            Some(Variant::QueryWorkflow(query)) => Some(query),
            _ => None,
        });
        if let Some(query) = query {
            assert_eq!(query.query_type, "approval");
            assert_eq!(activation.timestamp, initial_clock);
            queries_answered += 1;
            fixtures::answer_query(&worker, activation.run_id, "accepted").await;
        } else {
            evictions += usize::from(
                activation
                    .jobs
                    .iter()
                    .any(|job| matches!(job.variant, Some(Variant::RemoveFromCache(_)))),
            );
            let latest = activation.jobs.iter().any(|job| {
                matches!(
                    &job.variant,
                    Some(Variant::SignalWorkflow(signal)) if signal.signal_name == "latest"
                )
            });
            if latest {
                assert_eq!(queries_answered, 1);
            }
            fixtures::complete_empty(&worker, activation.run_id).await;
            if latest {
                break;
            }
        }
    }
    worker.shutdown().await;
    assert_eq!(evictions, usize::from(evict) + 1);
    slots.assert_released_once(4);
}

fn timed_out_tasks() -> (
    PollWorkflowTaskQueueResponse,
    PollWorkflowTaskQueueResponse,
    PollWorkflowTaskQueueResponse,
    PollWorkflowTaskQueueResponse,
    PollWorkflowTaskQueueResponse,
) {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    let mut initial = hist_to_poll_resp(&history, "delayed-completion", 1.into()).resp;
    history.add_we_signaled("next", vec![]);
    history.add_workflow_task_scheduled_and_started();
    let mut returned = hist_to_poll_resp(&history, "delayed-completion", 2.into()).resp;
    history.add(WorkflowTaskTimedOutEventAttributes {
        scheduled_event_id: returned.started_event_id - 1,
        started_event_id: returned.started_event_id,
        timeout_type: TimeoutType::StartToClose as i32,
    });
    history.add_workflow_task_scheduled_and_started();
    let mut superseded = hist_to_poll_resp(&history, "delayed-completion", 2.into()).resp;
    history.add(WorkflowTaskTimedOutEventAttributes {
        scheduled_event_id: superseded.started_event_id - 1,
        started_event_id: superseded.started_event_id,
        timeout_type: TimeoutType::StartToClose as i32,
    });
    history.add_we_signaled("latest", vec![]);
    history.add_workflow_task_scheduled_and_started();
    let mut newer = hist_to_poll_resp(&history, "delayed-completion", 2.into()).resp;
    for response in [&mut initial, &mut returned, &mut superseded, &mut newer] {
        response.attempt = 1;
        for event in &mut response.history.as_mut().unwrap().events {
            if let Some(Attributes::WorkflowTaskScheduledEventAttributes(attrs)) =
                &mut event.attributes
            {
                attrs.attempt = 1;
            }
        }
    }
    initial.task_token = b"initial-task".to_vec();
    returned.task_token = b"late-returned-task".to_vec();
    superseded.task_token = b"superseded-polled-task".to_vec();
    newer.task_token = b"newer-polled-task".to_vec();
    let mut query = initial.clone();
    query.task_token = b"live-query".to_vec();
    query.previous_started_event_id = initial.started_event_id;
    query.started_event_id = 0;
    query.query = Some(WorkflowQuery {
        query_type: "approval".to_string(),
        ..Default::default()
    });
    (initial, returned, superseded, query, newer)
}
