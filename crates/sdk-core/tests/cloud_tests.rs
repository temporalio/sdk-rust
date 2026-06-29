#[path = "cloud_tests/api_endpoint_probes.rs"]
mod api_endpoint_probes;
// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
mod common;
mod shared_tests;

use common::{get_cloud_client, get_cloud_client_with_compression};
use temporalio_client::{GrpcCompression, NamespacedClient, grpc::WorkflowService};
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
async fn grpc_compression() {
    shared_tests::grpc_compression().await
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

#[tokio::test]
async fn all_cloud_api_upstream_endpoints_default_client_settings() {
    all_cloud_workflow_rpc_probes_covered();

    let gzip_client = get_cloud_client().await;
    let uncompressed_client = get_cloud_client_with_compression(GrpcCompression::None).await;
    let failures = api_endpoint_probes::run_all(&gzip_client, &uncompressed_client).await;

    if !failures.is_empty() {
        panic!(
            "Cloud endpoint probes failed:\n{}",
            failures
                .into_iter()
                .map(|failure| format!("- {failure}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

#[test]
fn all_cloud_workflow_rpc_probes_covered() {
    api_endpoint_probes::assert_all_workflow_rpc_probes_covered();
}
