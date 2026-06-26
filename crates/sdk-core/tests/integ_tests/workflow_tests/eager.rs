use crate::common::{CoreWfStarter, NAMESPACE, get_integ_connection};
use std::time::Duration;
use temporalio_client::{Client, NamespacedClient, WorkflowStartOptions, grpc::WorkflowService};
use temporalio_common::{
    protos::temporal::api::{
        common::v1::WorkflowType,
        enums::v1::TaskQueueKind,
        taskqueue::v1::TaskQueue,
        workflowservice::v1::{StartWorkflowExecutionRequest, StartWorkflowExecutionResponse},
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};
use tonic::IntoRequest;

#[workflow]
#[derive(Default)]
pub(crate) struct EagerWf;

#[workflow_methods]
impl EagerWf {
    #[run(name = "eager_wf_start")]
    pub(crate) async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn eager_wf_start() {
    let wf_name = "eager_wf_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.workflow_options.enable_eager_workflow_start = true;
    // hang the test if eager task dispatch failed
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1500));
    let mut worker = starter.worker().await;
    worker.register_workflow::<EagerWf>().unwrap();
    let task_queue = starter.get_task_queue().to_string();
    let res = eager_start(
        wf_name,
        task_queue,
        &starter.get_client().await,
        starter.workflow_options.clone(),
    )
    .await;
    res.eager_workflow_task
        .as_ref()
        .expect("no eager workflow task");
    worker.expect_workflow_completion(starter.get_task_queue(), Some(res.run_id));
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn eager_wf_start_different_clients() {
    let wf_name = "eager_wf_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.workflow_options.enable_eager_workflow_start = true;
    // hang the test if wf task needs retry
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1500));
    let mut worker = starter.worker().await;
    worker.register_workflow::<EagerWf>().unwrap();

    let connection = get_integ_connection(None).await;
    let client_opts = temporalio_client::ClientOptions::new(NAMESPACE).build();
    let client = Client::new(connection, client_opts).unwrap();
    let task_queue = starter.get_task_queue().to_string();
    let res = eager_start(
        wf_name,
        task_queue,
        &client,
        starter.workflow_options.clone(),
    )
    .await;
    // different clients means no eager_wf_start.
    assert!(res.eager_workflow_task.is_none());

    //wf task delivered through default path
    worker.expect_workflow_completion(starter.get_task_queue(), Some(res.run_id));
    worker.run_until_done().await.unwrap();
}

pub(crate) async fn eager_start(
    wf_name: impl Into<String>,
    task_queue: String,
    client: &Client,
    options: WorkflowStartOptions,
) -> StartWorkflowExecutionResponse {
    assert!(options.enable_eager_workflow_start);
    client
        .clone()
        .start_workflow_execution(
            StartWorkflowExecutionRequest {
                namespace: client.namespace(),
                input: None,
                workflow_id: task_queue.clone(),
                workflow_type: Some(WorkflowType {
                    name: wf_name.into(),
                }),
                task_queue: Some(TaskQueue {
                    name: task_queue.clone(),
                    kind: TaskQueueKind::Unspecified as i32,
                    normal_name: "".to_string(),
                }),
                request_id: uuid::Uuid::new_v4().to_string(),
                workflow_id_reuse_policy: options.id_reuse_policy as i32,
                workflow_id_conflict_policy: options.id_conflict_policy as i32,
                workflow_execution_timeout: options
                    .execution_timeout
                    .and_then(|d| d.try_into().ok()),
                workflow_run_timeout: options.run_timeout.and_then(|d| d.try_into().ok()),
                workflow_task_timeout: options.task_timeout.and_then(|d| d.try_into().ok()),
                search_attributes: options.search_attributes.map(|d| d.into_proto()),
                cron_schedule: options.cron_schedule.unwrap_or_default(),
                request_eager_execution: options.enable_eager_workflow_start,
                retry_policy: options.retry_policy,
                links: options.links,
                completion_callbacks: options.completion_callbacks,
                priority: Some(options.priority.into()),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap()
        .into_inner()
}
