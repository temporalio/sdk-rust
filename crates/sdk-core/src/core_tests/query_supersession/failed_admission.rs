use super::*;
use crate::worker::client::mocks::mock_manual_worker_client;
use futures_util::{FutureExt, StreamExt};
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::test]
async fn worker_preserves_buffered_work_after_task_history_fetch_fails() {
    tokio::time::timeout(Duration::from_secs(5), exercise_failed_admission())
        .await
        .unwrap();
}

async fn exercise_failed_admission() {
    let (initial, replaced, query, replacement) = fixtures::task_sequence();
    let initial_token = initial.task_token.clone().into();
    let mut returned = replacement.clone();
    returned.task_token = b"returned-task".to_vec();
    let history_start = returned.previous_started_event_id + 1;
    returned
        .history
        .as_mut()
        .unwrap()
        .events
        .retain(|event| event.event_id >= history_start);
    let (fetch_started_tx, fetch_started_rx) = oneshot::channel();
    let (release_fetch_tx, release_fetch_rx) = oneshot::channel();
    let mut fetch_started_tx = Some(fetch_started_tx);
    let mut release_fetch_rx = Some(release_fetch_rx);
    let reports = Arc::new(Mutex::new(Vec::new()));
    let completions = reports.clone();
    let mut client = mock_manual_worker_client();
    client
        .expect_complete_workflow_task()
        .times(2)
        .returning(move |completion, _| {
            completions
                .lock()
                .unwrap()
                .push(completion.task_token.clone());
            let response = if completion.task_token == initial_token {
                RespondWorkflowTaskCompletedResponse {
                    workflow_task: Some(returned.clone()),
                    ..Default::default()
                }
            } else {
                assert_eq!(completion.task_token, b"replacement-task".to_vec().into());
                Default::default()
            };
            async move { Ok(response) }.boxed()
        });
    let failures = reports.clone();
    client
        .expect_fail_workflow_task()
        .times(1)
        .returning(move |token, _, _| {
            assert_eq!(token, b"returned-task".to_vec().into());
            failures.lock().unwrap().push(token);
            async { Ok(Default::default()) }.boxed()
        });
    client
        .expect_respond_legacy_query()
        .times(1)
        .returning(|token, result| {
            assert_eq!(token, b"live-query".to_vec().into());
            assert!(matches!(result, LegacyQueryResult::Succeeded(_)));
            async { Ok(Default::default()) }.boxed()
        });
    client
        .expect_get_workflow_execution_history()
        .times(1)
        .returning(move |_, _, _| {
            fetch_started_tx.take().unwrap().send(()).unwrap();
            let release = release_fetch_rx.take().unwrap();
            async move {
                release.await.unwrap();
                Err(tonic::Status::unavailable("history fetch failed"))
            }
            .boxed()
        });
    let responses = stream::iter([initial])
        .chain(stream::once(async move {
            fetch_started_rx.await.unwrap();
            replaced
        }))
        .chain(stream::iter([query, replacement]));
    let slots = Arc::new(ObservedSlots::default());
    let worker = mock_worker(fixtures::mocks_with_slots(
        client,
        responses,
        slots.clone(),
        10,
    ));
    let initial = worker.poll_workflow_activation().await.unwrap();
    worker.request_workflow_eviction(&initial.run_id);
    fixtures::complete_empty(&worker, initial.run_id).await;
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert!(
        matches!(eviction.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::RemoveFromCache(_))))
    );
    fixtures::complete_empty(&worker, eviction.run_id).await;
    let (replay, ()) = tokio::join!(worker.poll_workflow_activation(), async {
        slots.replaced.notified().await;
        release_fetch_tx.send(()).unwrap();
    });
    let replay = replay.unwrap();
    assert!(matches!(
        replay.jobs[0].variant,
        Some(Variant::InitializeWorkflow(_))
    ));
    fixtures::complete_empty(&worker, replay.run_id).await;
    let query = worker.poll_workflow_activation().await.unwrap();
    assert!(
        matches!(query.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::QueryWorkflow(_))))
    );
    fixtures::answer_query(&worker, query.run_id, "accepted").await;
    let next = worker.poll_workflow_activation().await.unwrap();
    assert!(
        matches!(next.jobs.as_slice(), [job] if matches!(&job.variant, Some(Variant::SignalWorkflow(signal)) if signal.signal_name == "next"))
    );
    fixtures::complete_empty(&worker, next.run_id).await;
    worker.shutdown().await;
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 3);
    assert_eq!(reports[1], b"returned-task".to_vec().into());
    assert_eq!(reports[2], b"replacement-task".to_vec().into());
    slots.assert_released_once(4);
}
