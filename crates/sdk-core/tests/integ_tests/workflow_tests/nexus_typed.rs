use crate::{
    common::{CoreWfStarter, WorkflowHandleExt, rand_6_chars},
    integ_tests::mk_nexus_endpoint,
};
use assert_matches::assert_matches;
use nexus_rpc::service;
use std::time::Duration;
use temporalio_client::{UntypedWorkflow, WorkflowCancelOptions, WorkflowStartOptions};
use temporalio_common::{
    data_converters::{
        GenericPayloadConverter, PayloadConverter, SerializationContext, SerializationContextData,
        WorkflowSerializationContext,
    },
    protos::{
        coresdk::{
            AsJsonPayloadExt, FromJsonPayloadExt,
            nexus::{NexusTaskCompletion, nexus_task_completion},
        },
        temporal::api::{
            common::v1::{Callback, callback},
            nexus::v1::{
                CancelOperationResponse, Response, StartOperationResponse, request, response,
                start_operation_response, workflow_event_link_from_nexus,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    WorkflowContext, WorkflowContextView, WorkflowResult, WorkflowTermination,
    nexus::{NexusOperationOptions, NexusServiceClientOptions},
};
use temporalio_sdk_core::PollError;
use tokio::join;

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TypedInput {
    message: String,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TypedOutput {
    reply: String,
}

#[service(name = "typed-wire-service")]
pub(crate) trait TypedService {
    #[operation(name = "typed-wire-operation")]
    fn greet(input: TypedInput) -> TypedOutput;
}

#[workflow]
struct NexusTypedWf {
    endpoint: String,
}

#[workflow_methods]
impl NexusTypedWf {
    #[init]
    fn new(_ctx: &WorkflowContextView, endpoint: String) -> Self {
        Self { endpoint }
    }

    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<TypedOutput> {
        let client = ctx.nexus_client(
            TypedService,
            NexusServiceClientOptions::new(ctx.state(|wf| wf.endpoint.clone())).build(),
        );
        let first: TypedOutput = client
            .execute(
                TypedService::greet,
                TypedInput {
                    message: "execute".to_owned(),
                },
                NexusOperationOptions::builder()
                    .schedule_to_close_timeout(Duration::from_secs(10))
                    .build(),
            )
            .await?;
        assert_eq!(first.reply, "executed");
        let handle = client
            .start(
                TypedService::greet,
                TypedInput {
                    message: first.reply,
                },
                NexusOperationOptions::builder()
                    .schedule_to_close_timeout(Duration::from_secs(10))
                    .build(),
            )
            .await?;
        assert_eq!(handle.operation_token(), None);
        let result: TypedOutput = handle.result().await?;
        assert_eq!(result.reply, "started");
        Ok(result)
    }
}

#[temporalio_macros::cloud_test_exclusion(
    crate::CloudTestExclusionReason::RequiresCloudProvisioning,
    "Creates a Nexus endpoint through the Operator Service, unavailable to isolated Cloud namespace credentials."
)]
#[tokio::test]
async fn nexus_typed() {
    let mut starter = CoreWfStarter::new("nexus_typed");
    starter.set_core_task_types(WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    });
    starter
        .sdk_config
        .register_workflow::<NexusTypedWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    let core_worker = starter.get_core_worker().await;
    let endpoint = mk_nexus_endpoint(&mut starter).await;
    let wf_handle = worker
        .submit_workflow(
            NexusTypedWf::run,
            endpoint,
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();

    let nexus_tasks = async {
        for (message, reply) in [("execute", "executed"), ("executed", "started")] {
            let task = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
            let start = assert_matches!(
                task.request.unwrap().variant.unwrap(),
                request::Variant::StartOperation(start) => start
            );
            assert_eq!(start.service, "typed-wire-service");
            assert_eq!(start.operation, "typed-wire-operation");
            assert_eq!(
                TypedInput::from_json_payload(start.payload.as_ref().unwrap()).unwrap(),
                TypedInput {
                    message: message.to_owned(),
                }
            );
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: task.task_token,
                    status: Some(nexus_task_completion::Status::Completed(Response {
                        variant: Some(response::Variant::StartOperation(StartOperationResponse {
                            variant: Some(start_operation_response::Variant::SyncSuccess(
                                start_operation_response::Sync {
                                    payload: Some(
                                        TypedOutput {
                                            reply: reply.to_owned(),
                                        }
                                        .as_json_payload()
                                        .unwrap(),
                                    ),
                                    links: vec![],
                                },
                            )),
                        })),
                    })),
                })
                .await
                .unwrap();
        }
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };
    join!(nexus_tasks, async {
        worker.run_until_done().await.unwrap();
    });
    wf_handle
        .fetch_history_and_replay(&mut worker)
        .await
        .unwrap();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum TypedAsyncOutcome {
    Succeed,
    Cancel,
}

#[workflow]
struct NexusTypedAsyncWf {
    endpoint: String,
    outcome: TypedAsyncOutcome,
}

#[workflow_methods]
impl NexusTypedAsyncWf {
    #[init]
    fn new(_ctx: &WorkflowContextView, (endpoint, outcome): (String, TypedAsyncOutcome)) -> Self {
        Self { endpoint, outcome }
    }

    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let client = ctx.nexus_client(
            TypedService,
            NexusServiceClientOptions::new(ctx.state(|wf| wf.endpoint.clone())).build(),
        );
        let handle = client
            .start(
                TypedService::greet,
                TypedInput {
                    message: "async".to_owned(),
                },
                NexusOperationOptions::builder()
                    .schedule_to_close_timeout(Duration::from_secs(60))
                    .build(),
            )
            .await?;
        assert_eq!(handle.operation_token(), Some("typed-async-operation"));
        match ctx.state(|wf| wf.outcome) {
            TypedAsyncOutcome::Succeed => {
                let result: TypedOutput = handle.result().await?;
                assert_eq!(result.reply, "completed async");
            }
            TypedAsyncOutcome::Cancel => {
                handle.cancel();
                let error = handle.result().await.unwrap_err();
                assert!(error.as_cancelled().is_some());
            }
        }
        Ok(())
    }
}

#[workflow]
struct TypedAsyncCompleter {
    outcome: TypedAsyncOutcome,
}

#[workflow_methods]
impl TypedAsyncCompleter {
    #[init]
    fn new(_ctx: &WorkflowContextView, outcome: TypedAsyncOutcome) -> Self {
        Self { outcome }
    }

    #[allow(dead_code)] // Started via untyped submitter handle
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<TypedOutput> {
        match ctx.state(|wf| wf.outcome) {
            TypedAsyncOutcome::Succeed => Ok(TypedOutput {
                reply: "completed async".to_owned(),
            }),
            TypedAsyncOutcome::Cancel => {
                ctx.cancelled().await;
                Err(WorkflowTermination::cancelled())
            }
        }
    }
}

#[temporalio_macros::cloud_test_exclusion(
    crate::CloudTestExclusionReason::RequiresCloudProvisioning,
    "Creates a Nexus endpoint through the Operator Service, unavailable to isolated Cloud namespace credentials."
)]
#[rstest::rstest]
#[tokio::test]
async fn nexus_typed_async(
    #[values(TypedAsyncOutcome::Succeed, TypedAsyncOutcome::Cancel)] outcome: TypedAsyncOutcome,
) {
    let mut starter = CoreWfStarter::new("nexus_typed_async");
    starter.set_core_task_types(WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    });
    starter
        .sdk_config
        .register_workflow::<NexusTypedAsyncWf>()
        .unwrap();
    starter
        .sdk_config
        .register_workflow::<TypedAsyncCompleter>()
        .unwrap();
    let mut worker = starter.worker().await;
    let core_worker = starter.get_core_worker().await;
    let endpoint = mk_nexus_endpoint(&mut starter).await;
    let submitter = worker.get_submitter_handle();
    let converter = PayloadConverter::default();
    let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
    let ser_ctx = SerializationContext::new(&context_data, &converter);
    let wf_handle = worker
        .submit_workflow(
            NexusTypedAsyncWf::run,
            (endpoint, outcome),
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();
    let client = starter.get_core_client().await;

    let nexus_tasks = async {
        let task = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        let start = assert_matches!(
            task.request.unwrap().variant.unwrap(),
            request::Variant::StartOperation(start) => start
        );
        assert_eq!(start.service, "typed-wire-service");
        assert_eq!(start.operation, "typed-wire-operation");
        assert_eq!(
            TypedInput::from_json_payload(start.payload.as_ref().unwrap()).unwrap(),
            TypedInput {
                message: "async".to_owned(),
            }
        );
        let links = start
            .links
            .iter()
            .map(workflow_event_link_from_nexus)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let completer_id = format!("typed-completer-{}", rand_6_chars());
        submitter
            .submit_wf(
                TypedAsyncCompleter::name(),
                vec![converter.to_payload(&ser_ctx, &outcome).unwrap()],
                WorkflowStartOptions::new(
                    starter.get_task_queue().to_owned(),
                    completer_id.clone(),
                )
                .completion_callbacks(vec![Callback {
                    variant: Some(callback::Variant::Nexus(callback::Nexus {
                        url: start.callback,
                        header: start.callback_header,
                    })),
                    links: links.clone(),
                }])
                .links(links)
                .build(),
            )
            .await
            .unwrap();
        core_worker
            .complete_nexus_task(NexusTaskCompletion {
                task_token: task.task_token,
                status: Some(nexus_task_completion::Status::Completed(Response {
                    variant: Some(response::Variant::StartOperation(StartOperationResponse {
                        variant: Some(start_operation_response::Variant::AsyncSuccess(
                            #[allow(deprecated)]
                            start_operation_response::Async {
                                operation_id: "typed-async-operation".to_owned(),
                                links: vec![],
                                operation_token: "typed-async-operation".to_owned(),
                            },
                        )),
                    })),
                })),
            })
            .await
            .unwrap();
        if outcome == TypedAsyncOutcome::Cancel {
            let task = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
            let cancel = assert_matches!(
                task.request.unwrap().variant.unwrap(),
                request::Variant::CancelOperation(cancel) => cancel
            );
            assert_eq!(cancel.operation_token, "typed-async-operation");
            client
                .get_workflow_handle::<UntypedWorkflow>(completer_id)
                .cancel(
                    WorkflowCancelOptions::builder()
                        .reason("typed nexus cancel")
                        .build(),
                )
                .await
                .unwrap();
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: task.task_token,
                    status: Some(nexus_task_completion::Status::Completed(Response {
                        variant: Some(response::Variant::CancelOperation(
                            CancelOperationResponse {},
                        )),
                    })),
                })
                .await
                .unwrap();
        }
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };
    join!(nexus_tasks, async {
        worker.run_until_done().await.unwrap();
    });
    wf_handle
        .fetch_history_and_replay(&mut worker)
        .await
        .unwrap();
}
