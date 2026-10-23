use crate::common::{CoreWfStarter, get_integ_connection, integ_namespace, rand_6_chars};
use anyhow::anyhow;
use futures::future::{self, BoxFuture};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use temporalio_client::{
    Client, ClientOptions, WorkflowExecuteUpdateOptions, WorkflowStartOptions,
    errors::WorkflowStartError, grpc::WorkflowService,
};
use temporalio_common::{
    data_converters::{
        DataConverter, ExternalStorage, ExternalStorageError, StorageDriver, StorageDriverClaim,
        StorageDriverError, StorageDriverRetrieveContext, StorageDriverStoreContext,
    },
    protos::temporal::api::{
        common::v1::{Payload, WorkflowExecution},
        history::v1::history_event::Attributes,
        workflowservice::v1::GetWorkflowExecutionHistoryRequest,
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, Runtime, SyncWorkflowContext, WorkerOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use tonic::IntoRequest;

const REFERENCE_MESSAGE_TYPE: &[u8] = b"temporal.api.sdk.v1.ExternalStorageReference";

#[derive(Default)]
struct InMemoryDriver {
    stored: Mutex<HashMap<String, Payload>>,
}

impl StorageDriver for InMemoryDriver {
    fn name(&self) -> &str {
        "memory"
    }

    fn driver_type(&self) -> &str {
        "test.memory"
    }

    fn store(
        &self,
        _context: &StorageDriverStoreContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, StorageDriverError>> {
        let mut stored = self.stored.lock().unwrap();
        let claims = payloads
            .into_iter()
            .map(|payload| {
                let key = format!("k{}", stored.len());
                stored.insert(key.clone(), payload);
                StorageDriverClaim::new(HashMap::from([("key".to_owned(), key)]))
            })
            .collect();
        Box::pin(future::ready(Ok(claims)))
    }

    fn retrieve(
        &self,
        _context: &StorageDriverRetrieveContext,
        claims: Vec<StorageDriverClaim>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, StorageDriverError>> {
        let stored = self.stored.lock().unwrap();
        let payloads = claims
            .iter()
            .map(|claim| {
                stored
                    .get(&claim.claim_data["key"])
                    .cloned()
                    .ok_or_else(|| {
                        StorageDriverError::from(anyhow!(
                            "nothing stored for {:?}",
                            claim.claim_data
                        ))
                    })
            })
            .collect();
        Box::pin(future::ready(payloads))
    }
}

struct UnreachableDriver;

impl StorageDriver for UnreachableDriver {
    fn name(&self) -> &str {
        "unreachable"
    }

    fn driver_type(&self) -> &str {
        "test.unreachable"
    }

    fn store(
        &self,
        _context: &StorageDriverStoreContext,
        _payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, StorageDriverError>> {
        Box::pin(future::ready(Err(anyhow!("bucket unreachable").into())))
    }

    fn retrieve(
        &self,
        _context: &StorageDriverRetrieveContext,
        _claims: Vec<StorageDriverClaim>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, StorageDriverError>> {
        Box::pin(future::ready(Err(anyhow!("bucket unreachable").into())))
    }
}

struct LargePayloadActivities;
#[activities]
impl LargePayloadActivities {
    #[activity]
    async fn echo(_ctx: ActivityContext, input: String) -> Result<String, ActivityError> {
        Ok(input)
    }
}

#[workflow]
#[derive(Default)]
struct LargePayloadWorkflow;
#[workflow_methods]
impl LargePayloadWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        let output = ctx
            .execute_activity(
                LargePayloadActivities::echo,
                input,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
        Ok(output)
    }
}

#[workflow]
#[derive(Default)]
struct LargeUpdateWorkflow {
    done: bool,
}
#[workflow_methods]
impl LargeUpdateWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.done).await?;
        Ok(())
    }

    #[update]
    fn echo(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: String) -> String {
        self.done = true;
        input
    }
}

fn is_reference(payload: &Payload) -> bool {
    payload.metadata.get("messageType").map(Vec::as_slice) == Some(REFERENCE_MESSAGE_TYPE)
}

async fn client_with_storage(driver: Arc<InMemoryDriver>) -> Client {
    let storage = ExternalStorage::new(driver)
        .unwrap()
        .with_payload_size_threshold(1024);
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(DataConverter::default().with_external_storage(storage))
        .build();
    Client::new(get_integ_connection(None).await, client_opts).unwrap()
}

#[tokio::test]
async fn large_payloads_round_trip_through_external_storage() {
    let driver = Arc::new(InMemoryDriver::default());
    let client = client_with_storage(driver.clone()).await;

    let mut starter =
        CoreWfStarter::new_with_overrides(LargePayloadWorkflow::name(), None, Some(client.clone()));
    starter
        .sdk_config
        .register_activities(LargePayloadActivities);
    starter
        .sdk_config
        .register_workflow::<LargePayloadWorkflow>()
        .unwrap();
    let workflow_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let input = "x".repeat(10_000);
    let handle = worker
        .submit_workflow(
            LargePayloadWorkflow::run,
            input.clone(),
            WorkflowStartOptions::new(starter.get_task_queue(), workflow_id.clone()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    assert_eq!(handle.get_result(Default::default()).await.unwrap(), input);
    // The workflow's input, the activity's input and result, and the workflow's result.
    assert_eq!(driver.stored.lock().unwrap().len(), 4);

    // Read past the client, which would otherwise swap the references back for the payloads.
    let history = WorkflowService::get_workflow_execution_history(
        &mut client.clone(),
        GetWorkflowExecutionHistoryRequest {
            namespace: integ_namespace(),
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id: String::new(),
            }),
            ..Default::default()
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner()
    .history
    .unwrap();
    let mut workflow_input = None;
    let mut activity_input = None;
    let mut workflow_result = None;
    for event in history.events {
        match event.attributes {
            Some(Attributes::WorkflowExecutionStartedEventAttributes(attrs)) => {
                workflow_input = attrs.input.map(|p| p.payloads[0].clone());
            }
            Some(Attributes::ActivityTaskScheduledEventAttributes(attrs)) => {
                activity_input = attrs.input.map(|p| p.payloads[0].clone());
            }
            Some(Attributes::WorkflowExecutionCompletedEventAttributes(attrs)) => {
                workflow_result = attrs.result.map(|p| p.payloads[0].clone());
            }
            _ => {}
        }
    }
    assert!(is_reference(&workflow_input.unwrap()));
    assert!(is_reference(&activity_input.unwrap()));
    assert!(is_reference(&workflow_result.unwrap()));
}

#[tokio::test]
async fn large_update_arguments_and_results_round_trip_through_external_storage() {
    let driver = Arc::new(InMemoryDriver::default());
    let client = client_with_storage(driver.clone()).await;
    let task_queue = format!("large_update_{}", rand_6_chars());
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let worker_options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<LargeUpdateWorkflow>()
        .unwrap()
        .build();
    let worker = temporalio_sdk::Worker::new(&runtime, client.clone(), worker_options).unwrap();

    let input = "y".repeat(10_000);
    worker
        .run_until(async {
            let handle = client
                .start_workflow(
                    LargeUpdateWorkflow::run,
                    (),
                    WorkflowStartOptions::new(task_queue.clone(), task_queue.clone()).build(),
                )
                .await
                .unwrap();
            let output = handle
                .execute_update(
                    LargeUpdateWorkflow::echo,
                    input.clone(),
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(output, input);
            handle.get_result(Default::default()).await.unwrap();
        })
        .await
        .unwrap();

    // The update's argument, stored by the client, and its result, stored by the worker.
    assert_eq!(driver.stored.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_failing_store_is_reported_as_an_external_storage_error() {
    let storage = ExternalStorage::new(Arc::new(UnreachableDriver))
        .unwrap()
        .with_payload_size_threshold(1024);
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(DataConverter::default().with_external_storage(storage))
        .build();
    let client = Client::new(get_integ_connection(None).await, client_opts).unwrap();
    let task_queue = format!("failing_store_{}", rand_6_chars());

    let Err(err) = client
        .start_workflow(
            LargePayloadWorkflow::run,
            "x".repeat(10_000),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
    else {
        panic!("starting the workflow should fail when its input cannot be stored");
    };

    assert!(
        matches!(
            err,
            WorkflowStartError::ExternalStorage(ExternalStorageError::Driver(_))
        ),
        "got {err:?}"
    );
}
