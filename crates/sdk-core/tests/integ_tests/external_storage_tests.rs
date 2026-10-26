use crate::common::{
    CoreWfStarter, eventually, get_integ_connection, integ_namespace, rand_6_chars,
};
use anyhow::anyhow;
use futures::{
    TryStreamExt,
    future::{self, BoxFuture},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use temporalio_client::{
    ActivityIdentifier, Client, ClientOptions, WorkflowDescribeOptions,
    WorkflowExecuteUpdateOptions, WorkflowListOptions, WorkflowQueryOptions, WorkflowSignalOptions,
    WorkflowStartOptions,
    errors::{WorkflowGetResultError, WorkflowStartError},
    schedules::{CreateScheduleOptions, ScheduleAction, ScheduleDescriptionAction, ScheduleSpec},
};
use temporalio_common::{
    MemoValues,
    data_converters::{
        DataConverter, ExternalStorage, ExternalStorageError, PayloadConversionError,
        StorageDriver, StorageDriverClaim, StorageDriverError, StorageDriverRetrieveContext,
        StorageDriverStoreContext,
    },
    protos::temporal::api::{common::v1::Payload, history::v1::history_event::Attributes},
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, Runtime, SyncWorkflowContext, WorkerOptions, WorkflowContext,
    WorkflowContextView, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use tokio::sync::mpsc;

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

#[workflow]
#[derive(Default)]
struct LargeSignalWorkflow {
    value: Option<String>,
}
#[workflow_methods]
impl LargeSignalWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        ctx.wait_condition(|s| s.value.is_some()).await?;
        Ok(ctx.state(|s| s.value.clone().unwrap_or_default()))
    }

    #[signal]
    fn deliver(&mut self, _ctx: &mut SyncWorkflowContext<Self>, value: String) {
        self.value = Some(value);
    }

    #[query]
    fn current(&self, _ctx: &WorkflowContextView) -> String {
        self.value.clone().unwrap_or_default()
    }
}

struct AsyncCompletionActivities {
    task_token: mpsc::Sender<Vec<u8>>,
}
#[activities]
impl AsyncCompletionActivities {
    #[activity]
    async fn complete_later(
        self: Arc<Self>,
        ctx: ActivityContext,
    ) -> Result<String, ActivityError> {
        let _ = self.task_token.send(ctx.info().task_token.clone()).await;
        Err(ActivityError::WillCompleteAsync)
    }
}

#[workflow]
#[derive(Default)]
struct AsyncCompletionWorkflow;
#[workflow_methods]
impl AsyncCompletionWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let output = ctx
            .execute_activity(
                AsyncCompletionActivities::complete_later,
                (),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(30)),
            )
            .await?;
        Ok(output)
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

    let history = starter.get_history().await;
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

    let client_without_storage = Client::new(
        get_integ_connection(None).await,
        ClientOptions::new(integ_namespace()).build(),
    )
    .unwrap();
    let err = client_without_storage
        .get_workflow_handle::<LargePayloadWorkflow>(workflow_id)
        .get_result(Default::default())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            WorkflowGetResultError::PayloadConversion(PayloadConversionError::ExternalStorage(
                ExternalStorageError::NotConfigured
            ))
        ),
        "got {err:?}"
    );
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
            WorkflowStartError::PayloadConversion(PayloadConversionError::ExternalStorage(
                ExternalStorageError::Driver(_)
            ))
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn signals_queries_and_memos_round_trip_through_external_storage() {
    let driver = Arc::new(InMemoryDriver::default());
    let client = client_with_storage(driver.clone()).await;
    let task_queue = format!("large_signal_{}", rand_6_chars());
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let worker_options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<LargeSignalWorkflow>()
        .unwrap()
        .build();
    let worker = temporalio_sdk::Worker::new(&runtime, client.clone(), worker_options).unwrap();

    let value = "s".repeat(10_000);
    let memo_value = "m".repeat(10_000);
    let mut memo = MemoValues::new();
    memo.insert("big", memo_value.clone());
    worker
        .run_until(async {
            let handle = client
                .start_workflow(
                    LargeSignalWorkflow::run,
                    (),
                    WorkflowStartOptions::new(task_queue.clone(), task_queue.clone())
                        .memo(memo)
                        .build(),
                )
                .await
                .unwrap();
            handle
                .signal(
                    LargeSignalWorkflow::deliver,
                    value.clone(),
                    WorkflowSignalOptions::default(),
                )
                .await
                .unwrap();
            let queried = handle
                .query(
                    LargeSignalWorkflow::current,
                    (),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(queried, value);
            assert_eq!(handle.get_result(Default::default()).await.unwrap(), value);
        })
        .await
        .unwrap();

    let handle = client.get_workflow_handle::<LargeSignalWorkflow>(task_queue.clone());
    let description = handle
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap();
    assert_eq!(
        description.memo().get::<String>("big").unwrap(),
        Some(memo_value.clone())
    );

    let listed = eventually(
        || {
            let client = client.clone();
            let query = format!("WorkflowId = '{task_queue}'");
            async move {
                let executions: Vec<_> = client
                    .list_workflows(query, WorkflowListOptions::default())
                    .try_collect()
                    .await
                    .map_err(|e| e.to_string())?;
                executions
                    .into_iter()
                    .next()
                    .ok_or_else(|| "workflow not listed yet".to_owned())
            }
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(
        listed.memo().get::<String>("big").unwrap(),
        Some(memo_value)
    );

    let events = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let mut memo_payload = None;
    let mut signal_input = None;
    for event in events {
        match event.attributes {
            Some(Attributes::WorkflowExecutionStartedEventAttributes(attrs)) => {
                memo_payload = attrs.memo.and_then(|m| m.fields.get("big").cloned());
            }
            Some(Attributes::WorkflowExecutionSignaledEventAttributes(attrs)) => {
                signal_input = attrs.input.map(|p| p.payloads[0].clone());
            }
            _ => {}
        }
    }
    assert!(is_reference(&memo_payload.unwrap()));
    assert!(is_reference(&signal_input.unwrap()));
}

#[tokio::test]
async fn schedule_arguments_round_trip_through_external_storage() {
    let driver = Arc::new(InMemoryDriver::default());
    let client = client_with_storage(driver.clone()).await;
    let schedule_id = format!("large_schedule_{}", rand_6_chars());
    let input = "a".repeat(10_000);

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    LargePayloadWorkflow::run,
                    input.clone(),
                    "unused-task-queue",
                    format!("wf-{schedule_id}"),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(driver.stored.lock().unwrap().len(), 1);

    let description = handle.describe(Default::default()).await.unwrap();
    let ScheduleDescriptionAction::StartWorkflow(action) = description.action() else {
        panic!("expected a start workflow action");
    };
    let stored: String = action
        .args()
        .await
        .unwrap()
        .expect("input should be present");
    assert_eq!(stored, input);

    handle.delete(Default::default()).await.unwrap();
}

#[tokio::test]
async fn async_activity_results_round_trip_through_external_storage() {
    let driver = Arc::new(InMemoryDriver::default());
    let client = client_with_storage(driver.clone()).await;
    let task_queue = format!("large_async_activity_{}", rand_6_chars());
    let (token_tx, mut token_rx) = mpsc::channel(1);
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let worker_options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<AsyncCompletionWorkflow>()
        .unwrap()
        .register_activities(AsyncCompletionActivities {
            task_token: token_tx,
        })
        .build();
    let worker = temporalio_sdk::Worker::new(&runtime, client.clone(), worker_options).unwrap();

    let result = "r".repeat(10_000);
    worker
        .run_until(async {
            let handle = client
                .start_workflow(
                    AsyncCompletionWorkflow::run,
                    (),
                    WorkflowStartOptions::new(task_queue.clone(), task_queue.clone()).build(),
                )
                .await
                .unwrap();
            let token = token_rx.recv().await.unwrap();
            client
                .get_async_activity_handle(ActivityIdentifier::TaskToken(token.into()))
                .complete(Some(result.clone()), Default::default())
                .await
                .unwrap();
            assert_eq!(handle.get_result(Default::default()).await.unwrap(), result);
        })
        .await
        .unwrap();

    let events = client
        .get_workflow_handle::<AsyncCompletionWorkflow>(task_queue)
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let activity_result = events.into_iter().find_map(|event| match event.attributes {
        Some(Attributes::ActivityTaskCompletedEventAttributes(attrs)) => {
            attrs.result.map(|p| p.payloads[0].clone())
        }
        _ => None,
    });
    assert!(is_reference(&activity_result.unwrap()));
}
