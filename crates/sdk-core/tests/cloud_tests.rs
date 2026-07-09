// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
mod common;
mod shared_tests;

use common::get_cloud_client;
use temporalio_client::{
    NamespacedClient,
    grpc::{HealthService, WorkflowService},
};
use temporalio_common::protos::{
    grpc::health::v1::HealthCheckRequest, temporal::api::workflowservice::v1::*,
};
use tonic::{IntoRequest, Response};

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
async fn default_client_gzip_supported_by_system_info_and_health_check() {
    let mut client = get_cloud_client().await;
    let system_info = client
        .get_system_info(GetSystemInfoRequest::default().into_request())
        .await
        .expect("GetSystemInfo must succeed with the default Cloud client");
    assert_gzip_response("GetSystemInfo", &system_info);

    let health_check =
        HealthService::check(&mut client, HealthCheckRequest::default().into_request())
            .await
            .expect("HealthCheck must succeed with the default Cloud client");
    assert_gzip_response("HealthCheck", &health_check);
}

fn assert_gzip_response<Resp>(rpc_name: &str, response: &Response<Resp>) {
    assert_eq!(
        response
            .metadata()
            .get("grpc-encoding")
            .and_then(|value| value.to_str().ok()),
        Some("gzip"),
        "{rpc_name} must use gzip with the default Cloud client"
    );
}
