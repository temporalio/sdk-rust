use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        MocksHolder, ResponseType, WorkerTestHelpers, drain_pollers_and_shutdown,
        hist_to_poll_resp, mock_worker, query_ok,
    },
    worker::{LEGACY_QUERY_ID, client::mocks::mock_worker_client},
};
use futures_util::{StreamExt, stream};
use std::{future, time::Duration};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{enums::v1::EventType, query::v1::WorkflowQuery},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[rstest::rstest]
#[case::uncached_one(1, 0)]
#[case::uncached_two(2, 0)]
#[case::uncached_three(3, 0)]
#[case::small_cache_one(1, 1)]
#[case::small_cache_two(2, 1)]
#[case::large_cache_one(1, 10)]
#[case::large_cache_two(2, 10)]
#[case::large_cache_three(3, 10)]
#[tokio::test]
async fn preserves_queries_buffered_during_same_run_eviction(
    #[case] query_count: usize,
    #[case] cache_capacity: usize,
    #[values(false, true)] first_query_needs_fetch: bool,
    #[values(false, true)] shutdown_during_eviction: bool,
) {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.add_workflow_execution_completed();
    let full_history = history.get_full_history_info().unwrap();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    sender
        .send(hist_to_poll_resp(&history, "evicted", 1.into()).resp)
        .unwrap();
    let mut client = mock_worker_client();
    client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(Default::default()));
    client
        .expect_respond_legacy_query()
        .times(query_count)
        .returning(|_, _| Ok(Default::default()));
    client.expect_get_workflow_execution_history().returning(move |_, _, _| {
        Ok(temporalio_common::protos::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryResponse {
            history: Some(full_history.clone().into()),
            ..Default::default()
        })
    });
    let (buffered_tx, buffered_rx) = tokio::sync::oneshot::channel();
    let input = UnboundedReceiverStream::new(receiver).chain(stream::once(async move {
        buffered_tx.send(()).unwrap();
        future::pending().await
    }));
    let mut mocks = MocksHolder::from_wft_stream(client, input);
    mocks.worker_cfg(|cfg| {
        cfg.max_cached_workflows = cache_capacity;
        // With one extractor future, polling past the last response means WFStream
        // has processed it; the channel then synchronizes with the processing thread.
        cfg.fetching_concurrency = 1;
    });
    let core = mock_worker(mocks);
    let initial_permits = core.available_wft_permits();
    let task = core.poll_workflow_activation().await.unwrap();
    let run_id = task.run_id.clone();
    core.complete_execution(&run_id).await;
    let eviction = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        eviction.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    for query in 0..query_count {
        let mut task = hist_to_poll_resp(&history, "evicted", ResponseType::AllHistory).resp;
        task.task_token = format!("query-{query}").into_bytes();
        if first_query_needs_fetch && query == 0 {
            task.history = Some(Default::default());
        }
        task.started_event_id = 0;
        task.query = Some(WorkflowQuery {
            query_type: query.to_string(),
            ..Default::default()
        });
        sender.send(task).unwrap();
    }
    drop(sender);
    tokio::time::timeout(Duration::from_secs(5), buffered_rx)
        .await
        .expect("all queries must be buffered before acknowledging eviction")
        .unwrap();
    if shutdown_during_eviction {
        core.initiate_shutdown();
    }
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();
    let replay = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        replay.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
    core.complete_execution(&replay.run_id).await;
    let mut seen = std::collections::HashSet::new();
    for query in 0..query_count {
        let mut next_query = None;
        for _ in 0..3 {
            let task =
                tokio::time::timeout(Duration::from_secs(1), core.poll_workflow_activation())
                    .await
                    .expect("every buffered Query must reach a language activation")
                    .unwrap();
            assert_eq!(task.run_id, run_id);
            match task.jobs.as_slice() {
                [
                    WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
                    },
                ] => core
                    .complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
                    .await
                    .unwrap(),
                [
                    WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
                    },
                ] => core.complete_execution(&task.run_id).await,
                _ => {
                    next_query = Some(task);
                    break;
                }
            }
        }
        let task = next_query.expect("at most one eviction and replay may precede each Query");
        assert_eq!(task.run_id, run_id);
        let observed = assert_matches!(task.jobs.as_slice(), [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q.query_type.parse::<usize>().unwrap());
        assert!(observed < query_count && seen.insert(observed));
        if !first_query_needs_fetch {
            assert_eq!(observed, query);
        }
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            query_ok(LEGACY_QUERY_ID.to_string(), "constant"),
        ))
        .await
        .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), drain_pollers_and_shutdown(&core))
        .await
        .expect("serviced queries must not prevent shutdown");
    assert_eq!(core.available_wft_permits(), initial_permits);
    core.finalize_shutdown().await;
}
