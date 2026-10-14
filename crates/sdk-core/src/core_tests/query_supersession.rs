mod branch_transition;
mod delayed_completion;
mod failed_admission;
mod fixtures;
mod older_query;
mod slots;

use crate::{
    test_help::mock_worker,
    worker::client::{LegacyQueryResult, mocks::mock_worker_client},
};
use futures_util::stream;
use slots::ObservedSlots;
use std::sync::{Arc, Mutex};
use temporalio_common::protos::{
    coresdk::workflow_activation::workflow_activation_job::Variant,
    temporal::api::workflowservice::v1::{
        GetWorkflowExecutionHistoryResponse, RespondWorkflowTaskCompletedResponse,
    },
};

enum Scenario {
    Cached,
    EvictedFullHistory,
    EvictedEmptyHistory,
    CompletionReturnedTask,
    EvictedPartialHistory,
    QueryReplyNotFound,
    EvictedCompletionReturnedTask,
}

#[rstest::rstest]
#[case::cached(Scenario::Cached)]
#[case::evicted_full_history(Scenario::EvictedFullHistory)]
#[case::evicted_empty_history(Scenario::EvictedEmptyHistory)]
#[case::completion_returned_task(Scenario::CompletionReturnedTask)]
#[case::evicted_partial_history(Scenario::EvictedPartialHistory)]
#[case::query_reply_not_found(Scenario::QueryReplyNotFound)]
#[case::evicted_completion_returned_task(Scenario::EvictedCompletionReturnedTask)]
#[tokio::test]
async fn worker_query_supersession_scenario(#[case] scenario: Scenario) {
    let evict = matches!(
        scenario,
        Scenario::EvictedFullHistory
            | Scenario::EvictedEmptyHistory
            | Scenario::EvictedPartialHistory
            | Scenario::QueryReplyNotFound
            | Scenario::EvictedCompletionReturnedTask
    );
    let empty_query_history = matches!(
        scenario,
        Scenario::EvictedEmptyHistory | Scenario::EvictedPartialHistory
    );
    let returned_task = matches!(
        scenario,
        Scenario::CompletionReturnedTask | Scenario::EvictedCompletionReturnedTask
    );
    let partial_query_history = matches!(scenario, Scenario::EvictedPartialHistory);
    let query_not_found = matches!(scenario, Scenario::QueryReplyNotFound);
    let (initial, replaced, mut query, replacement) = fixtures::task_sequence();
    let mut client = mock_worker_client();
    if empty_query_history {
        let query_history = initial.history.clone();
        if partial_query_history {
            query
                .history
                .as_mut()
                .unwrap()
                .events
                .retain(|event| event.event_id == query.previous_started_event_id);
        } else {
            query.history = Some(Default::default());
        }
        client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: query_history.clone(),
                    ..Default::default()
                })
            });
    }
    let reports = Arc::new(Mutex::new(Vec::new()));
    let recorded_reports = reports.clone();
    let initial_token: temporalio_common::protos::TaskToken = initial.task_token.clone().into();
    let mut returned = replacement.clone();
    returned.task_token = b"returned-task".to_vec();
    returned.started_time.as_mut().unwrap().seconds += 10;
    returned
        .history
        .as_mut()
        .unwrap()
        .events
        .last_mut()
        .unwrap()
        .event_time = returned.started_time;
    let expected_next_clock = if returned_task {
        returned.started_time
    } else {
        replacement.started_time
    };
    client
        .expect_complete_workflow_task()
        .returning(move |completion, _shutdown_token| {
            if returned_task && completion.task_token == initial_token {
                Ok(RespondWorkflowTaskCompletedResponse {
                    workflow_task: Some(returned.clone()),
                    ..Default::default()
                })
            } else {
                if returned_task {
                    let mut reports = recorded_reports.lock().unwrap();
                    let expected = if reports.is_empty() {
                        b"returned-task".as_slice()
                    } else {
                        b"replacement-task".as_slice()
                    };
                    assert_eq!(completion.task_token, expected.to_vec().into());
                    reports.push(completion.task_token);
                }
                Ok(Default::default())
            }
        });
    if returned_task {
        client.expect_fail_workflow_task().returning(|token, _, _| {
            assert_eq!(token, b"replacement-task".to_vec().into());
            Err(tonic::Status::not_found("polled task expired"))
        });
    }
    let query_count = if evict { 2 } else { 1 };
    let answers = Arc::new(Mutex::new(Vec::new()));
    let recorded_answers = answers.clone();
    client
        .expect_respond_legacy_query()
        .times(query_count)
        .returning(move |token, result| {
            assert!(matches!(result, LegacyQueryResult::Succeeded(_)));
            recorded_answers.lock().unwrap().push(token);
            if query_not_found && recorded_answers.lock().unwrap().len() == 1 {
                return Err(tonic::Status::not_found("query waiter expired"));
            }
            Ok(Default::default())
        });
    let slots = Arc::new(ObservedSlots::default());
    let mut second_query = query.clone();
    second_query.task_token = b"second-live-query".to_vec();
    let mut responses = vec![initial, replaced, query];
    if evict {
        responses.push(second_query);
    }
    responses.push(replacement);
    let worker = mock_worker(fixtures::mocks_with_slots(
        client,
        stream::iter(responses),
        slots.clone(),
        10,
    ));
    let initial = worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        initial.jobs[0].variant,
        Some(Variant::InitializeWorkflow(_))
    ));
    let initialized_clock = initial.timestamp;
    tokio::time::timeout(std::time::Duration::from_secs(5), slots.replaced.notified())
        .await
        .unwrap();
    if evict {
        worker.request_workflow_eviction(&initial.run_id);
    }
    fixtures::complete_empty(&worker, initial.run_id).await;
    if evict {
        let eviction = worker.poll_workflow_activation().await.unwrap();
        assert!(
            matches!(eviction.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::RemoveFromCache(_))))
        );
        fixtures::complete_empty(&worker, eviction.run_id).await;
        let replay = worker.poll_workflow_activation().await.unwrap();
        assert!(matches!(
            replay.jobs[0].variant,
            Some(Variant::InitializeWorkflow(_))
        ));
        fixtures::complete_empty(&worker, replay.run_id).await;
    }
    for _ in 0..query_count {
        let query = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            worker.poll_workflow_activation(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            matches!(query.jobs.as_slice(), [job] if matches!(&job.variant, Some(Variant::QueryWorkflow(q)) if q.query_type == "approval"))
        );
        assert_eq!(
            query.timestamp, initialized_clock,
            "query uses the initialized workflow clock"
        );
        fixtures::answer_query(&worker, query.run_id, "accepted").await;
    }
    let next = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        worker.poll_workflow_activation(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(next.jobs.as_slice(), [job] if matches!(&job.variant, Some(Variant::SignalWorkflow(s)) if s.signal_name == "next"))
    );
    assert_eq!(next.timestamp, expected_next_clock);
    fixtures::complete_empty(&worker, next.run_id).await;
    fixtures::drain_allowed_activations_until_shutdown(&worker, |job| {
        matches!(job.variant, Some(Variant::RemoveFromCache(_)))
    })
    .await;
    worker.shutdown().await;
    if returned_task {
        assert_eq!(
            reports.lock().unwrap().first(),
            Some(&b"returned-task".to_vec().into())
        );
    }
    slots.assert_released_once(3 + query_count);
    let answers = answers.lock().unwrap();
    assert_eq!(answers[0], b"live-query".to_vec().into());
    if evict {
        assert_eq!(answers[1], b"second-live-query".to_vec().into());
    }
}
