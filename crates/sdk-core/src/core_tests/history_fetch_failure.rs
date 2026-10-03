use crate::{
    PollError, Worker,
    replay::TestHistoryBuilder,
    test_help::{MocksHolder, ResponseType, hist_to_poll_resp, mock_worker},
    worker::{
        SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplier,
        SlotSupplierPermit, TunerBuilder, WorkflowSlotKind,
        client::{LegacyQueryResult, mocks::mock_manual_worker_client},
    },
};
use futures_util::{FutureExt, StreamExt, stream};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::{
    TaskToken,
    temporal::api::{
        enums::v1::{EventType, WorkflowTaskFailedCause},
        query::v1::WorkflowQuery,
        workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
};
use tokio::sync::Notify;

#[derive(Default)]
struct ObservedWorkflowSlots {
    releases: AtomicUsize,
}

#[async_trait::async_trait]
impl SlotSupplier for ObservedWorkflowSlots {
    type SlotKind = WorkflowSlotKind;

    async fn reserve_slot(&self, _: &dyn SlotReservationContext) -> SlotSupplierPermit {
        SlotSupplierPermit::default()
    }

    fn try_reserve_slot(&self, _: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        Some(SlotSupplierPermit::default())
    }

    fn mark_slot_used(&self, _: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {}

    fn release_slot(&self, _: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}

fn worker_with_response(
    client: impl crate::WorkerClient + 'static,
    response: PollWorkflowTaskQueueResponse,
    slots: Arc<ObservedWorkflowSlots>,
) -> Worker {
    let mut mocks =
        MocksHolder::from_wft_stream(client, stream::iter([response]).chain(stream::pending()));
    mocks.worker_cfg(move |config| {
        config.tuner = Some(Arc::new(
            TunerBuilder::default()
                .workflow_slot_supplier(slots)
                .build(),
        ));
    });
    mock_worker(mocks)
}

async fn hold_report_and_check_slot(
    worker: Worker,
    report_started: Arc<Notify>,
    release_report: Arc<Notify>,
    slots: Arc<ObservedWorkflowSlots>,
) {
    let worker = Arc::new(worker);
    let poll_worker = worker.clone();
    let poll = tokio::spawn(async move { poll_worker.poll_workflow_activation().await });
    tokio::time::timeout(Duration::from_secs(5), report_started.notified())
        .await
        .expect("worker did not start reporting the history fetch failure");
    let releases_while_reporting = slots.releases.load(Ordering::SeqCst);
    worker.initiate_shutdown();
    release_report.notify_one();
    let poll_result = tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("worker did not finish after the failure report")
        .expect("worker polling task panicked");
    assert!(matches!(poll_result, Err(PollError::ShutDown)));
    let worker = Arc::try_unwrap(worker)
        .unwrap_or_else(|_| panic!("worker polling task retained its Worker reference"));
    worker.finalize_shutdown().await;
    assert_eq!(releases_while_reporting, 0);
    assert_eq!(slots.releases.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_initial_history_pagination_answers_legacy_query_on_original_token() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    let mut response = hist_to_poll_resp(
        &history,
        "failed-initial-history-pagination",
        ResponseType::ToTaskNum(1),
    )
    .resp;
    response.previous_started_event_id = response.started_event_id;
    response.started_event_id = 0;
    response.task_token = b"initial-query-token".to_vec();
    response.query = Some(WorkflowQuery {
        query_type: "lookup".to_string(),
        query_args: None,
        header: None,
    });
    response.next_page_token = vec![1];
    response.history.as_mut().unwrap().events.truncate(2);

    let report_started = Arc::new(Notify::new());
    let release_report = Arc::new(Notify::new());
    let mut client = mock_manual_worker_client();
    client
        .expect_get_workflow_execution_history()
        .returning(|_, _, _| {
            async { Err(tonic::Status::unavailable("history fetch failed")) }.boxed()
        })
        .times(1);
    let report_signal = report_started.clone();
    let report_release = release_report.clone();
    client
        .expect_respond_legacy_query()
        .returning(move |token, result| {
            assert_eq!(token, TaskToken::from(b"initial-query-token".to_vec()));
            assert!(matches!(result, LegacyQueryResult::Failed(_)));
            let report_signal = report_signal.clone();
            let report_release = report_release.clone();
            async move {
                report_signal.notify_one();
                report_release.notified().await;
                Ok(Default::default())
            }
            .boxed()
        });

    let slots = Arc::new(ObservedWorkflowSlots::default());
    let worker = worker_with_response(client, response, slots.clone());
    hold_report_and_check_slot(worker, report_started, release_report, slots).await;
}

#[tokio::test]
async fn failed_full_history_admission_reports_original_workflow_task_token() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.add_we_signaled("next", vec![]);
    history.add_workflow_task_scheduled_and_started();
    let mut response = hist_to_poll_resp(
        &history,
        "failed-full-history-admission",
        ResponseType::OneTask(2),
    )
    .resp;
    response.task_token = b"full-history-task-token".to_vec();

    let report_started = Arc::new(Notify::new());
    let release_report = Arc::new(Notify::new());
    let mut client = mock_manual_worker_client();
    client
        .expect_get_workflow_execution_history()
        .returning(|_, _, _| {
            async { Err(tonic::Status::unavailable("history fetch failed")) }.boxed()
        })
        .times(1);
    let report_signal = report_started.clone();
    let report_release = release_report.clone();
    client
        .expect_fail_workflow_task()
        .returning(move |token, cause, _| {
            assert_eq!(token, TaskToken::from(b"full-history-task-token".to_vec()));
            assert_eq!(
                cause,
                WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
            );
            let report_signal = report_signal.clone();
            let report_release = report_release.clone();
            async move {
                report_signal.notify_one();
                report_release.notified().await;
                Ok(Default::default())
            }
            .boxed()
        });

    let slots = Arc::new(ObservedWorkflowSlots::default());
    let worker = worker_with_response(client, response, slots.clone());
    hold_report_and_check_slot(worker, report_started, release_report, slots).await;
}
