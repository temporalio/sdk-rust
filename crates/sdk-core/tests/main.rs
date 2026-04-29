//! Integration tests

#[macro_use]
extern crate rstest;
#[macro_use]
extern crate assert_matches;

mod common;

#[cfg(test)]
mod shared_tests;

#[cfg(test)]
mod integ_tests {
    mod async_activity_client_tests;
    mod client_tests;
    mod data_converter_tests;
    mod ephemeral_server_tests;
    mod heartbeat_tests;
    mod metrics_tests;
    mod pagination_tests;
    mod poll_loop_tests;
    mod polling_tests;
    mod queries_tests;
    mod schedule_tests;
    mod update_tests;
    mod visibility_tests;
    mod worker_heartbeat_tests;
    mod worker_tests;
    mod worker_versioning_tests;
    mod workflow_client_tests;
    mod workflow_tests;

    use crate::common::{
        CoreWfStarter, get_integ_runtime_options, get_integ_server_options,
        get_integ_telem_options, rand_6_chars,
    };
    use std::time::Duration;
    use temporalio_client::{
        Connection, NamespacedClient,
        grpc::{OperatorService, WorkflowService},
    };
    use temporalio_common::{
        protos::temporal::api::{
            nexus::v1::{EndpointSpec, EndpointTarget, endpoint_target},
            operatorservice::v1::CreateNexusEndpointRequest,
            workflowservice::v1::ListNamespacesRequest,
        },
        worker::WorkerTaskTypes,
    };
    use temporalio_sdk_core::{CoreRuntime, WorkerConfig, WorkerVersioningStrategy, init_worker};
    use tonic::IntoRequest;

    // Create a worker like a bridge would (unwraps aside)
    #[tokio::test]
    #[ignore] // Really a compile time check more than anything
    async fn lang_bridge_example() {
        let opts = get_integ_server_options();
        let runtime =
            CoreRuntime::new_assume_tokio(get_integ_runtime_options(get_integ_telem_options()))
                .unwrap();
        let mut connection = Connection::connect(opts).await.unwrap();

        let _worker = init_worker(
            &runtime,
            WorkerConfig::builder()
                .namespace("default")
                .task_queue("Wheee!")
                .task_types(WorkerTaskTypes::all())
                .versioning_strategy(WorkerVersioningStrategy::None {
                    build_id: "test".to_owned(),
                })
                .build()
                .unwrap(),
            // clone the connection if you intend to use it later
            connection.clone(),
        );

        // Do things with worker or connection
        let _ = connection
            .list_namespaces(ListNamespacesRequest::default().into_request())
            .await;
    }

    pub(crate) async fn mk_nexus_endpoint(starter: &mut CoreWfStarter) -> String {
        let client = starter.get_client().await;
        let endpoint = format!("mycoolendpoint-{}", rand_6_chars());
        client
            .connection()
            .clone()
            .create_nexus_endpoint(
                CreateNexusEndpointRequest {
                    spec: Some(EndpointSpec {
                        name: endpoint.to_owned(),
                        description: None,
                        target: Some(EndpointTarget {
                            variant: Some(endpoint_target::Variant::Worker(
                                endpoint_target::Worker {
                                    namespace: client.namespace(),
                                    task_queue: starter.get_task_queue().to_owned(),
                                },
                            )),
                        }),
                    }),
                }
                .into_request(),
            )
            .await
            .unwrap();
        // Endpoint creation can (as of server 1.25.2 at least) return before they are actually usable.
        tokio::time::sleep(Duration::from_millis(800)).await;
        endpoint
    }
}
