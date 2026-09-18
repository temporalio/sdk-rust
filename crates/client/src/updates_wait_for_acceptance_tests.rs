use crate::{
    NamespacedClient, RpcOptions, UntypedUpdate, UntypedWorkflow, WorkflowExecutionInfo,
    WorkflowHandle, WorkflowStartUpdateOptions, errors::WorkflowUpdateError, grpc::WorkflowService,
};
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use temporalio_common::{
    data_converters::RawValue,
    protos::temporal::api::{
        common::v1::{Payload, WorkflowExecution},
        enums::v1::UpdateWorkflowExecutionLifecycleStage as Stage,
        update::v1::UpdateRef,
        workflowservice::v1::{UpdateWorkflowExecutionRequest, UpdateWorkflowExecutionResponse},
    },
};
use tonic::{Request, Response, Status};

#[derive(Default)]
struct State {
    responses: VecDeque<Result<UpdateWorkflowExecutionResponse, Status>>,
    requests: Vec<UpdateWorkflowExecutionRequest>,
}

#[derive(Clone)]
struct MockUpdateClient(Arc<Mutex<State>>);

impl NamespacedClient for MockUpdateClient {
    fn namespace(&self) -> String {
        "ns".into()
    }
    fn identity(&self) -> String {
        "identity".into()
    }
}

impl WorkflowService for MockUpdateClient {
    fn update_workflow_execution(
        &mut self,
        request: Request<UpdateWorkflowExecutionRequest>,
    ) -> BoxFuture<'_, Result<Response<UpdateWorkflowExecutionResponse>, Status>> {
        // Each retry creates a new tonic request, so its RPC deadline must be reapplied.
        assert!(request.metadata().contains_key("grpc-timeout"));
        let mut state = self.0.lock();
        state.requests.push(request.into_inner());
        let response = state.responses.pop_front().expect("unexpected submission");
        Box::pin(async { response.map(Response::new) })
    }
}

// Successful acceptance long-polls may expire repeatedly before the update is accepted.
#[rstest::rstest]
#[case::retries_until_accepted(
    vec![Stage::Unspecified, Stage::Admitted, Stage::Admitted, Stage::Accepted],
    None
)]
#[case::already_accepted(vec![Stage::Accepted], None)]
#[case::already_completed(vec![Stage::Completed], None)]
#[case::rpc_error(vec![Stage::Admitted], Some(Status::unavailable("transport failure")))]
#[tokio::test]
async fn ordinary_update_waits_for_acceptance_with_the_same_request(
    #[case] stages: Vec<Stage>,
    #[case] error: Option<Status>,
) {
    let mut responses: VecDeque<_> = stages
        .iter()
        .map(|stage| {
            Ok(UpdateWorkflowExecutionResponse {
                stage: *stage as i32,
                update_ref: Some(UpdateRef {
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: "wf".into(),
                        run_id: "run".into(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
        })
        .collect();
    // Only successful responses below Accepted should trigger another submission.
    if let Some(error) = error.clone() {
        responses.push_back(Err(error));
    }
    let expected_requests = responses.len();
    let state = Arc::new(Mutex::new(State {
        responses,
        ..Default::default()
    }));
    let handle = WorkflowHandle::<_, UntypedWorkflow>::new(
        MockUpdateClient(state.clone()),
        WorkflowExecutionInfo::builder()
            .namespace("ns")
            .workflow_id("wf")
            .build(),
    );
    let result = handle
        .start_update(
            UntypedUpdate::new("handler"),
            RawValue::new(vec![Payload {
                data: vec![1, 2, 3],
                ..Default::default()
            }]),
            WorkflowStartUpdateOptions::builder()
                .rpc_options(
                    RpcOptions::builder()
                        .timeout(Duration::from_secs(30))
                        .build(),
                )
                .build(),
        )
        .await;
    let state = state.lock();
    assert_eq!(state.requests.len(), expected_requests);
    let first = &state.requests[0];
    // A retry must remain the same logical update, including its payload and run targeting.
    assert!(state.requests.iter().all(|request| request == first));
    assert!(first.workflow_execution.as_ref().unwrap().run_id.is_empty());
    let update_id = &first
        .request
        .as_ref()
        .unwrap()
        .meta
        .as_ref()
        .unwrap()
        .update_id;
    assert!(!update_id.is_empty());
    match (error, result) {
        (None, Ok(handle)) => {
            assert_eq!(handle.id(), update_id);
            assert_eq!(handle.workflow_run_id(), Some("run"));
        }
        (Some(expected), Err(WorkflowUpdateError::Rpc(actual))) => {
            assert_eq!(actual.code(), expected.code());
            assert_eq!(actual.message(), expected.message());
        }
        _ => panic!("unexpected update result"),
    }
}
