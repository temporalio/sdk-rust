// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
mod common;
mod shared_tests;

use common::get_cloud_client;
use temporalio_client::{NamespacedClient, grpc::WorkflowService};
use temporalio_common::protos::temporal::api::workflowservice::v1::ListWorkflowExecutionsRequest;
use tonic::IntoRequest;

#[tokio::test]
async fn tls_test() {
    let mut con = get_cloud_client().await;
    con.list_workflow_executions(
        ListWorkflowExecutionsRequest {
            namespace: con.namespace(),
            page_size: 100,
            ..Default::default()
        }
        .into_request(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn grpc_message_too_large_test() {
    shared_tests::grpc_message_too_large().await
}

#[tokio::test]
async fn priority_values_sent_to_server() {
    shared_tests::priority::priority_values_sent_to_server().await
}

#[tokio::test]
async fn shutdown_during_active_timer_activity_workflows() {
    shared_tests::shutdown_during_active_timer_activity_workflows().await
}

#[tokio::test]
async fn activity_cancel_delivered_without_heartbeat() {
    shared_tests::activity_cancel_delivered_without_heartbeat().await
}
