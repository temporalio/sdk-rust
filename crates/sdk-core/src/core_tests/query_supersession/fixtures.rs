use crate::{
    Worker,
    replay::TestHistoryBuilder,
    test_help::{MocksHolder, hist_to_poll_resp, query_ok},
    worker::{LEGACY_QUERY_ID, client::WorkerClient, tuner::TunerBuilder},
};
use futures_util::Stream;
use std::{sync::Arc, time::Duration};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::WorkflowActivationJob,
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        enums::v1::EventType, query::v1::WorkflowQuery,
        workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
};

use super::slots::ObservedSlots;

pub(super) fn mocks_with_slots(
    client: impl WorkerClient + 'static,
    responses: impl Stream<Item = PollWorkflowTaskQueueResponse> + Send + 'static,
    slots: Arc<ObservedSlots>,
    cache_capacity: usize,
) -> MocksHolder {
    let mut mocks = MocksHolder::from_wft_stream(client, responses);
    mocks.worker_cfg(|config| {
        config.max_cached_workflows = cache_capacity;
        config.tuner = Some(Arc::new(
            TunerBuilder::default()
                .workflow_slot_supplier(slots)
                .build(),
        ));
    });
    mocks
}

pub(super) async fn complete_empty(worker: &Worker, run_id: String) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id))
        .await
        .unwrap();
}

pub(super) async fn answer_query(worker: &Worker, run_id: String, result: &str) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id,
            query_ok(LEGACY_QUERY_ID.to_string(), result),
        ))
        .await
        .unwrap();
}

pub(super) async fn drain_allowed_activations_until_shutdown(
    worker: &Worker,
    allowed_job: impl Fn(&WorkflowActivationJob) -> bool,
) {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), worker.poll_workflow_activation())
            .await
            .unwrap()
        {
            Ok(activation) => {
                assert!(
                    activation.jobs.iter().all(&allowed_job),
                    "unexpected activation while draining: {:?}",
                    activation.jobs
                );
                complete_empty(worker, activation.run_id).await;
            }
            Err(crate::PollError::ShutDown) => break,
            Err(error) => panic!("unexpected poll failure: {error:?}"),
        }
    }
}

pub(super) fn task_sequence() -> (
    PollWorkflowTaskQueueResponse,
    PollWorkflowTaskQueueResponse,
    PollWorkflowTaskQueueResponse,
    PollWorkflowTaskQueueResponse,
) {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.add_we_signaled("next", vec![]);
    history.add_full_wf_task();
    let mut initial = hist_to_poll_resp(&history, "supersession", 1.into()).resp;
    let mut replaced = hist_to_poll_resp(&history, "supersession", 2.into()).resp;
    for response in [&mut initial, &mut replaced] {
        for event in &mut response.history.as_mut().unwrap().events {
            event.event_time = Some(prost_types::Timestamp {
                seconds: 1_700_000_000 + event.event_id,
                nanos: 0,
            });
        }
        response.started_time = response
            .history
            .as_ref()
            .unwrap()
            .events
            .last()
            .unwrap()
            .event_time;
    }
    replaced.task_token = b"replaced-task".to_vec();
    let mut query = initial.clone();
    query.task_token = b"live-query".to_vec();
    query.previous_started_event_id = initial.started_event_id;
    query.started_event_id = 0;
    query.query = Some(WorkflowQuery {
        query_type: "approval".to_string(),
        ..Default::default()
    });
    let mut replacement = replaced.clone();
    replacement.task_token = b"replacement-task".to_vec();
    replacement.started_time.as_mut().unwrap().seconds += 10;
    replacement
        .history
        .as_mut()
        .unwrap()
        .events
        .last_mut()
        .unwrap()
        .event_time = replacement.started_time;
    assert_ne!(replacement.started_time, replaced.started_time);
    (initial, replaced, query, replacement)
}
