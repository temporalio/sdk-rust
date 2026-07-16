use crate::common::{CoreWfStarter, NAMESPACE, eventually, get_integ_client};
use assert_matches::assert_matches;
use std::{sync::Arc, time::Duration};
use temporalio_client::{NamespacedClient, RegisterNamespaceOptions, grpc::WorkflowService};
use temporalio_common::protos::{
    coresdk::workflow_activation::{WorkflowActivationJob, workflow_activation_job},
    temporal::api::{
        filter::v1::{StartTimeFilter, WorkflowExecutionFilter},
        workflowservice::v1::{
            DescribeNamespaceRequest, ListClosedWorkflowExecutionsRequest,
            ListOpenWorkflowExecutionsRequest, RegisterNamespaceRequest,
            list_closed_workflow_executions_request, list_open_workflow_executions_request,
        },
    },
};
use temporalio_sdk_core::test_help::{WorkerTestHelpers, drain_pollers_and_shutdown};
use tokio::time::sleep;
use tonic::IntoRequest;

#[tokio::test]
async fn client_list_open_closed_workflow_executions() {
    let wf_name = "client_list_open_closed_workflow_executions".to_owned();
    let mut starter = CoreWfStarter::new(&wf_name);
    let core = starter.get_worker().await;
    let mut client = starter.get_client().await;

    let earliest = std::time::SystemTime::now();
    let latest = earliest + Duration::from_secs(60);

    // start workflow
    let run_id = starter.start_wf_with_id(wf_name.to_owned()).await;
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );

    // List above OPEN workflow
    let start_time_filter = StartTimeFilter {
        earliest_time: Some(earliest).map(|t| t.into()),
        latest_time: Some(latest).map(|t| t.into()),
    };
    let open_workflows = eventually(
        || {
            let mut cc = client.clone();
            let filter = list_open_workflow_executions_request::Filters::ExecutionFilter(
                WorkflowExecutionFilter {
                    workflow_id: wf_name.clone(),
                    run_id: "".to_owned(),
                },
            );
            async move {
                let open_workflows = cc
                    .list_open_workflow_executions(
                        ListOpenWorkflowExecutionsRequest {
                            namespace: cc.namespace(),
                            maximum_page_size: 1,
                            start_time_filter: Some(start_time_filter),
                            filters: Some(filter.clone()),
                            ..Default::default()
                        }
                        .into_request(),
                    )
                    .await
                    .unwrap()
                    .into_inner();
                if open_workflows.executions.len() == 1 {
                    Ok(open_workflows)
                } else {
                    Err(format!(
                        "Expected 1 open workflow, got {}",
                        open_workflows.executions.len()
                    ))
                }
            }
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let workflow = open_workflows.executions[0].clone();
    assert_eq!(workflow.execution.as_ref().unwrap().workflow_id, wf_name);

    // Complete workflow
    core.complete_execution(&task.run_id).await;
    core.handle_eviction().await;
    drain_pollers_and_shutdown(&core).await;

    // List above CLOSED workflow. Visibility doesn't always update immediately so we give this a
    // few tries.
    let mut passed = false;
    for _ in 1..=5 {
        let closed_workflows = client
            .list_closed_workflow_executions(
                ListClosedWorkflowExecutionsRequest {
                    namespace: client.namespace(),
                    maximum_page_size: 1,
                    start_time_filter: Some(StartTimeFilter {
                        earliest_time: Some(earliest).map(|t| t.into()),
                        latest_time: Some(latest).map(|t| t.into()),
                    }),
                    filters: Some(
                        list_closed_workflow_executions_request::Filters::ExecutionFilter(
                            WorkflowExecutionFilter {
                                workflow_id: wf_name.clone(),
                                run_id: run_id.clone(),
                            },
                        ),
                    ),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();
        if closed_workflows.executions.len() == 1 {
            let workflow = &closed_workflows.executions[0];
            if workflow.execution.as_ref().unwrap().workflow_id == wf_name {
                passed = true;
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(passed);
}

#[tokio::test]
async fn client_create_namespace() {
    let client = Arc::new(get_integ_client(NAMESPACE.to_string(), None).await);

    let register_options = RegisterNamespaceOptions::builder()
        .namespace(uuid::Uuid::new_v4().to_string())
        .description("it's alive")
        .build();

    let req: RegisterNamespaceRequest = register_options.clone().into();
    WorkflowService::register_namespace(&mut client.as_ref().clone(), req.into_request())
        .await
        .unwrap();

    //#Hack, not sure how else to wait for a proper response.  RegisterNamespace isn't safe to read
    //after write
    let mut attempts = 0;
    let wait_time = Duration::from_secs(1);
    loop {
        attempts += 1;
        let resp = WorkflowService::describe_namespace(
            &mut client.as_ref().clone(),
            DescribeNamespaceRequest {
                namespace: register_options.namespace.clone(),
                ..Default::default()
            }
            .into_request(),
        )
        .await;

        match resp {
            Ok(n) => {
                let namespace_info = n.into_inner().namespace_info.unwrap();
                assert_eq!(namespace_info.name, register_options.namespace);
                assert_eq!(namespace_info.description, register_options.description);
                return;
            }
            _ => {
                if attempts == 12 {
                    panic!("failed to query registered namespace");
                }
                sleep(wait_time).await
            }
        }
    }
}

#[tokio::test]
async fn client_describe_namespace() {
    let client = Arc::new(get_integ_client(NAMESPACE.to_string(), None).await);

    let namespace_result = WorkflowService::describe_namespace(
        &mut client.as_ref().clone(),
        DescribeNamespaceRequest {
            namespace: NAMESPACE.to_owned(),
            ..Default::default()
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(namespace_result.namespace_info.unwrap().name, NAMESPACE);
}
