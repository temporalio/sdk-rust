use super::{
    BufferedTasks, PermittedWFT, history_update::HistoryPaginator, wft_poller::validate_wft,
};
use crate::{
    abstractions::tests::fixed_size_permit_dealer,
    replay::TestHistoryBuilder,
    test_help::hist_to_poll_resp,
    worker::{WorkflowSlotKind, client::mocks::mock_worker_client},
};
use std::sync::Arc;
use temporalio_common::protos::{
    coresdk::WorkflowSlotInfo,
    temporal::api::{enums::v1::EventType, query::v1::WorkflowQuery},
};

async fn task(id: u32, query: bool) -> PermittedWFT {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    let mut response = hist_to_poll_resp(&history, "buffer-order", 1.into()).resp;
    response.attempt = id as i32;
    if query {
        response.started_event_id = 0;
        response.query = Some(WorkflowQuery::default());
    }
    let (paginator, work) = HistoryPaginator::from_poll(
        validate_wft(response).unwrap(),
        Arc::new(mock_worker_client()),
    )
    .await
    .unwrap();
    let dealer = fixed_size_permit_dealer::<WorkflowSlotKind>(1);
    let permit = dealer.acquire_owned().await.into_used(WorkflowSlotInfo {
        workflow_type: work.workflow_type.clone(),
        is_sticky: false,
    });
    PermittedWFT {
        work,
        permit,
        paginator,
    }
}

#[rstest::rstest]
#[tokio::test]
async fn transferring_buffer_preserves_query_groups(
    #[values(false, true)] current_queries: bool,
    #[values(false, true)] superseded: bool,
) {
    let mut original = BufferedTasks::default();
    if current_queries {
        original.buffer(task(1, true).await);
        original.buffer(task(2, true).await);
    }
    original.buffer(task(3, false).await);
    original.buffer(task(4, true).await);
    original.buffer(task(5, true).await);
    assert_eq!(
        original.get_next_wft().unwrap().work.attempt,
        if current_queries { 1 } else { 4 }
    );
    let mut restored = BufferedTasks::default();
    for task in original.into_buffer_order() {
        restored.buffer(task);
    }
    if superseded {
        restored.buffer(task(6, false).await);
    }
    let mut actual = vec![];
    while let Some(task) = restored.get_next_wft() {
        actual.push(task.work.attempt);
    }
    let expected = match (current_queries, superseded) {
        (true, false) => vec![2, 4, 5, 3],
        (false, false) => vec![5, 3],
        (true, true) => vec![2, 6],
        (false, true) => vec![6],
    };
    assert_eq!(actual, expected);
}
