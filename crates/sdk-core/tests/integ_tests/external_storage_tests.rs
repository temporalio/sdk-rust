use crate::{
    common::{
        CoreWfStarter, eventually, get_integ_connection, get_integ_runtime_options,
        integ_namespace, prom_metrics, rand_6_chars,
    },
    integ_tests::metrics_tests::get_text,
};
use anyhow::anyhow;
use futures::{
    TryStreamExt,
    future::{self, BoxFuture},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    ActivityIdentifier, ActivityStartOptions, Client, ClientOptions, WorkflowDescribeOptions,
    WorkflowExecuteUpdateOptions, WorkflowListOptions, WorkflowQueryOptions, WorkflowSignalOptions,
    WorkflowStartOptions, WorkflowTerminateOptions,
    errors::{WorkflowGetResultError, WorkflowStartError},
    schedules::{CreateScheduleOptions, ScheduleAction, ScheduleDescriptionAction, ScheduleSpec},
};
use temporalio_common::{
    MemoValues,
    data_converters::{
        DataConverter, DefaultFailureConverter, ExternalStorage, ExternalStorageError,
        PayloadCodec, PayloadConversionError, PayloadConverter, SerializationContextData,
        StorageDriver, StorageDriverActivityInfo, StorageDriverClaim, StorageDriverError,
        StorageDriverRetrieveContext, StorageDriverStoreContext, StorageDriverTargetInfo,
        StorageDriverWorkflowInfo,
    },
    protos::{
        coresdk::{
            common::ExternalStorageMetrics, workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::{Payload, RetryPolicy},
            enums::v1::WorkflowTaskFailedCause,
            failure::v1::failure::FailureInfo,
            history::v1::history_event::Attributes,
        },
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, ContinueAsNewOptions, Runtime, SyncWorkflowContext,
    WorkerOptions, WorkflowContext, WorkflowContextView, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::WorkerInterceptor,
};
use temporalio_sdk_core::CoreRuntime;
use tokio::sync::mpsc;

const REFERENCE_MESSAGE_TYPE: &[u8] = b"temporal.api.sdk.v1.ExternalStorageReference";

#[derive(Default)]
struct InMemoryDriver {
    stored: Mutex<HashMap<String, Payload>>,
    targets: Mutex<Vec<Option<StorageDriverTargetInfo>>>,
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
        context: &StorageDriverStoreContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, StorageDriverError>> {
        self.targets.lock().unwrap().push(context.target.clone());
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

const INJECTED_FAILURE: &str = "injected storage failure";

/// Fails the store or retrieve call with the given 1-based number, once. Every other call is
/// served from memory.
#[derive(Default)]
struct FailingDriver {
    inner: InMemoryDriver,
    fail_store: Option<usize>,
    fail_retrieve: Option<usize>,
    stores: AtomicUsize,
    retrieves: AtomicUsize,
}

impl StorageDriver for FailingDriver {
    fn name(&self) -> &str {
        "failing"
    }

    fn driver_type(&self) -> &str {
        "test.failing"
    }

    fn store(
        &self,
        context: &StorageDriverStoreContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, StorageDriverError>> {
        let call = self.stores.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_store == Some(call) {
            return Box::pin(future::ready(Err(anyhow!(INJECTED_FAILURE).into())));
        }
        self.inner.store(context, payloads)
    }

    fn retrieve(
        &self,
        context: &StorageDriverRetrieveContext,
        claims: Vec<StorageDriverClaim>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, StorageDriverError>> {
        let call = self.retrieves.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_retrieve == Some(call) {
            return Box::pin(future::ready(Err(anyhow!(INJECTED_FAILURE).into())));
        }
        self.inner.retrieve(context, claims)
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

#[derive(Clone, Serialize, Deserialize)]
struct TargetsInput {
    value: String,
    child_id: String,
    continued: bool,
}

#[workflow]
#[derive(Default)]
struct TargetsParentWorkflow;
#[workflow_methods]
impl TargetsParentWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: TargetsInput) -> WorkflowResult<String> {
        if input.continued {
            return Ok(input.value);
        }
        let echoed = ctx
            .execute_activity(
                LargePayloadActivities::echo,
                input.value.clone(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
        let child = ctx
            .start_child_workflow(
                TargetsChildWorkflow::run,
                echoed.clone(),
                ChildWorkflowOptions::builder()
                    .workflow_id(input.child_id.clone())
                    .build(),
            )
            .await
            .expect("child should start");
        ctx.external_workflow(input.child_id.clone(), None)
            .signal(TargetsChildWorkflow::deliver, echoed, Default::default())
            .await
            .expect("child should be signalled");
        let value = child.result().await?;
        match ctx.continue_as_new(
            TargetsInput {
                value,
                continued: true,
                ..input
            },
            ContinueAsNewOptions::default(),
        )? {}
    }
}

#[workflow]
#[derive(Default)]
struct TargetsChildWorkflow {
    delivered: bool,
}
#[workflow_methods]
impl TargetsChildWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        ctx.wait_condition(|s| s.delivered).await?;
        Ok(input)
    }

    #[signal]
    fn deliver(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _value: String) {
        self.delivered = true;
    }
}

struct HeartbeatActivities;
#[activities]
impl HeartbeatActivities {
    #[activity]
    async fn heartbeat_until_timeout(ctx: ActivityContext) -> Result<(), ActivityError> {
        ctx.record_heartbeat("h".repeat(10_000)).await?;
        ctx.cancelled().await;
        Ok(())
    }

    #[activity]
    async fn report_heartbeat_failure(ctx: ActivityContext) -> Result<String, ActivityError> {
        Ok(match ctx.record_heartbeat("h".repeat(10_000)).await {
            Ok(()) => String::new(),
            Err(err) => err.to_string(),
        })
    }
}

#[workflow]
#[derive(Default)]
struct HeartbeatFailureWorkflow;
#[workflow_methods]
impl HeartbeatFailureWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        Ok(ctx
            .execute_activity(
                HeartbeatActivities::report_heartbeat_failure,
                (),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?)
    }
}

#[workflow]
#[derive(Default)]
struct HeartbeatDetailsWorkflow;
#[workflow_methods]
impl HeartbeatDetailsWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let err = ctx
            .execute_activity(
                HeartbeatActivities::heartbeat_until_timeout,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                    .heartbeat_timeout(Duration::from_secs(1))
                    .retry_policy(RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    })
                    .build(),
            )
            .await
            .expect_err("activity should time out");
        let timeout = err.as_timeout().expect("activity should time out");
        Ok(timeout
            .last_heartbeat_details()?
            .expect("heartbeat details should be present"))
    }
}

/// Fails unless `recorded` holds exactly the `expected` targets, in any order.
fn assert_same_targets(
    recorded: Vec<Option<StorageDriverTargetInfo>>,
    expected: Vec<Option<StorageDriverTargetInfo>>,
) {
    let mut unmatched = expected;
    for target in recorded {
        let Some(index) = unmatched.iter().position(|e| *e == target) else {
            panic!("unexpected target {target:?}, still expecting {unmatched:?}");
        };
        unmatched.remove(index);
    }
    assert!(unmatched.is_empty(), "never stored for {unmatched:?}");
}

fn is_reference(payload: &Payload) -> bool {
    payload.metadata.get("messageType").map(Vec::as_slice) == Some(REFERENCE_MESSAGE_TYPE)
}

async fn client_with_storage(driver: Arc<dyn StorageDriver>) -> Client {
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

    handle
        .update(
            |update| {
                update.set_note("updated");
            },
            Default::default(),
        )
        .await
        .unwrap();
    handle
        .send_update(description.into_update(), Default::default())
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

type RecordedStorageMetrics = Vec<(
    Option<ExternalStorageMetrics>,
    Option<ExternalStorageMetrics>,
)>;

struct StorageMetricsRecorder(Arc<Mutex<RecordedStorageMetrics>>);

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for StorageMetricsRecorder {
    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        self.0.lock().unwrap().push((
            completion.payload_download_metrics.clone(),
            completion.payload_upload_metrics.clone(),
        ));
    }
}

#[tokio::test]
async fn workflow_completions_report_what_external_storage_moved() {
    let client = client_with_storage(Arc::new(InMemoryDriver::default())).await;

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

    worker
        .submit_workflow(
            LargePayloadWorkflow::run,
            "m".repeat(10_000),
            WorkflowStartOptions::new(starter.get_task_queue(), workflow_id).build(),
        )
        .await
        .unwrap();
    let recorded = Arc::new(Mutex::new(vec![]));
    worker
        .run_until_done_intercepted(Some(StorageMetricsRecorder(Arc::clone(&recorded))))
        .await
        .unwrap();

    let recorded = recorded.lock().unwrap();
    // The workflow's input and the activity's result were retrieved, and the activity's input and
    // the workflow's result were stored.
    for (direction, reported) in [
        (
            "download",
            recorded
                .iter()
                .filter_map(|(d, _)| d.as_ref())
                .collect::<Vec<_>>(),
        ),
        (
            "upload",
            recorded
                .iter()
                .filter_map(|(_, u)| u.as_ref())
                .collect::<Vec<_>>(),
        ),
    ] {
        let payload_count: u64 = reported.iter().map(|m| m.payload_count).sum();
        assert_eq!(payload_count, 2, "{direction}: {reported:?}");
        for metrics in reported {
            assert_eq!(
                metrics.driver_names,
                vec!["memory".to_owned()],
                "{direction}"
            );
            assert!(
                metrics.total_size_bytes > 10_000,
                "{direction}: {metrics:?}"
            );
            assert!(metrics.total_duration.is_some(), "{direction}: {metrics:?}");
        }
    }
}

/// Fails the first decode of a workflow's payloads, after they have been retrieved.
#[derive(Default)]
struct FailFirstWorkflowDecode(AtomicBool);

impl PayloadCodec for FailFirstWorkflowDecode {
    fn encode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        Box::pin(future::ready(Ok(payloads)))
    }

    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        let fail = matches!(context, SerializationContextData::Workflow(_))
            && !payloads.is_empty()
            && !self.0.swap(true, Ordering::SeqCst);
        Box::pin(future::ready(if fail {
            Err(PayloadConversionError::EncodingError(
                "intentional decode failure".into(),
            ))
        } else {
            Ok(payloads)
        }))
    }
}

#[tokio::test]
async fn a_failed_activation_decode_reports_what_was_retrieved() {
    let storage = ExternalStorage::new(Arc::new(InMemoryDriver::default()))
        .unwrap()
        .with_payload_size_threshold(1024);
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter::default(),
        FailFirstWorkflowDecode::default(),
    )
    .with_external_storage(storage);
    let client = Client::new(
        get_integ_connection(None).await,
        ClientOptions::new(integ_namespace())
            .data_converter(data_converter)
            .build(),
    )
    .unwrap();

    let mut starter =
        CoreWfStarter::new_with_overrides("failed_activation_decode", None, Some(client.clone()));
    starter
        .sdk_config
        .register_activities(LargePayloadActivities);
    starter
        .sdk_config
        .register_workflow::<LargePayloadWorkflow>()
        .unwrap();
    let workflow_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    worker
        .submit_workflow(
            LargePayloadWorkflow::run,
            "d".repeat(10_000),
            WorkflowStartOptions::new(starter.get_task_queue(), workflow_id).build(),
        )
        .await
        .unwrap();
    let recorded = Arc::new(Mutex::new(vec![]));
    worker
        .run_until_done_intercepted(Some(StorageMetricsRecorder(Arc::clone(&recorded))))
        .await
        .unwrap();

    assert!(
        starter
            .get_history()
            .await
            .events
            .iter()
            .any(|event| matches!(
                event.attributes,
                Some(Attributes::WorkflowTaskFailedEventAttributes(_))
            )),
        "the first workflow task should have failed"
    );
    // The failed completion stores nothing, which tells it apart from the retry's, which stores
    // the activity's input.
    let recorded = recorded.lock().unwrap();
    let (download, upload) = &recorded[0];
    assert_eq!(
        download.as_ref().map(|m| m.payload_count),
        Some(1),
        "{recorded:?}"
    );
    assert_eq!(upload, &None, "{recorded:?}");
}

#[tokio::test]
async fn drivers_are_told_which_workflow_a_payload_is_stored_for() {
    let driver = Arc::new(InMemoryDriver::default());
    let client = client_with_storage(driver.clone()).await;
    let task_queue = format!("store_targets_{}", rand_6_chars());
    let workflow_id = task_queue.clone();
    let child_id = format!("{workflow_id}-child");
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let worker_options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<TargetsParentWorkflow>()
        .unwrap()
        .register_workflow::<TargetsChildWorkflow>()
        .unwrap()
        .register_activities(LargePayloadActivities)
        .build();
    let worker = temporalio_sdk::Worker::new(&runtime, client.clone(), worker_options).unwrap();

    let value = "t".repeat(10_000);
    let mut first_run_id = String::new();
    worker
        .run_until(async {
            let handle = client
                .start_workflow(
                    TargetsParentWorkflow::run,
                    TargetsInput {
                        value: value.clone(),
                        child_id: child_id.clone(),
                        continued: false,
                    },
                    WorkflowStartOptions::new(task_queue.clone(), workflow_id.clone()).build(),
                )
                .await
                .unwrap();
            assert_eq!(handle.get_result(Default::default()).await.unwrap(), value);
            first_run_id = handle.run_id().unwrap().to_owned();
        })
        .await
        .unwrap();
    let last_run_id = client
        .get_workflow_handle::<TargetsParentWorkflow>(workflow_id.clone())
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap()
        .run_id()
        .to_owned();

    let workflow = |id: &str, run_id: Option<&str>, workflow_type: Option<&str>| {
        Some(StorageDriverTargetInfo::Workflow(
            StorageDriverWorkflowInfo::builder()
                .namespace(integ_namespace())
                .id(id.to_owned())
                .maybe_run_id(run_id.map(str::to_owned))
                .maybe_workflow_type(workflow_type.map(str::to_owned))
                .build(),
        ))
    };
    let parent_type = Some(TargetsParentWorkflow::name());
    let child_type = Some(TargetsChildWorkflow::name());
    let first_run = Some(first_run_id.as_str());
    assert_same_targets(
        driver.targets.lock().unwrap().clone(),
        vec![
            // Started by the client, before any run exists.
            workflow(&workflow_id, None, parent_type),
            // The activity's input and its result both belong to the workflow that scheduled it.
            workflow(&workflow_id, first_run, parent_type),
            workflow(&workflow_id, first_run, parent_type),
            workflow(&child_id, None, child_type),
            workflow(&child_id, None, None),
            // A child's result is delivered to its parent.
            workflow(&workflow_id, first_run, None),
            // Continuing as new hands its input to a run that does not exist yet.
            workflow(&workflow_id, None, parent_type),
            workflow(&workflow_id, Some(&last_run_id), parent_type),
        ],
    );
}

#[tokio::test]
async fn drivers_are_told_which_standalone_activity_a_payload_is_stored_for() {
    let driver = Arc::new(InMemoryDriver::default());
    let client = client_with_storage(driver.clone()).await;
    let task_queue = format!("standalone_store_targets_{}", rand_6_chars());
    let activity_id = task_queue.clone();
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let worker_options = WorkerOptions::new(task_queue.clone())
        .register_activities(LargePayloadActivities)
        .build();
    let worker = temporalio_sdk::Worker::new(&runtime, client.clone(), worker_options).unwrap();

    let input = "u".repeat(10_000);
    let mut run_id = String::new();
    worker
        .run_until(async {
            let handle = client
                .start_activity(
                    LargePayloadActivities::echo,
                    input.clone(),
                    ActivityStartOptions::with_schedule_to_close_timeout(
                        task_queue.clone(),
                        activity_id.clone(),
                        Duration::from_secs(60),
                    )
                    .build(),
                )
                .await
                .unwrap();
            assert_eq!(handle.result().await.unwrap(), input);
            run_id = handle.run_id().unwrap().to_owned();
        })
        .await
        .unwrap();

    let activity = |run_id: Option<&str>| {
        Some(StorageDriverTargetInfo::Activity(
            StorageDriverActivityInfo::builder()
                .namespace(integ_namespace())
                .id(activity_id.clone())
                .maybe_run_id(run_id.map(str::to_owned))
                .activity_type(LargePayloadActivities::echo.name().to_owned())
                .build(),
        ))
    };
    assert_same_targets(
        driver.targets.lock().unwrap().clone(),
        vec![activity(None), activity(Some(&run_id))],
    );
}

#[tokio::test]
async fn heartbeat_details_round_trip_through_external_storage() {
    let driver = Arc::new(InMemoryDriver::default());
    let client = client_with_storage(driver.clone()).await;

    let mut starter = CoreWfStarter::new_with_overrides(
        HeartbeatDetailsWorkflow::name(),
        None,
        Some(client.clone()),
    );
    starter.sdk_config.register_activities(HeartbeatActivities);
    starter
        .sdk_config
        .register_workflow::<HeartbeatDetailsWorkflow>()
        .unwrap();
    let workflow_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            HeartbeatDetailsWorkflow::run,
            (),
            WorkflowStartOptions::new(starter.get_task_queue(), workflow_id.clone()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    assert_eq!(
        handle.get_result(Default::default()).await.unwrap(),
        "h".repeat(10_000)
    );

    let history = starter.get_history().await;
    let last_heartbeat_details = history
        .events
        .into_iter()
        .find_map(|event| match event.attributes {
            Some(Attributes::ActivityTaskTimedOutEventAttributes(attrs)) => {
                match attrs.failure.and_then(|failure| failure.failure_info) {
                    Some(FailureInfo::TimeoutFailureInfo(info)) => info.last_heartbeat_details,
                    _ => None,
                }
            }
            _ => None,
        })
        .expect("the activity's timeout should carry its last heartbeat details");
    assert!(is_reference(&last_heartbeat_details.payloads[0]));

    let workflow_target = Some(StorageDriverTargetInfo::Workflow(
        StorageDriverWorkflowInfo::builder()
            .namespace(integ_namespace())
            .id(workflow_id)
            .run_id(handle.run_id().unwrap().to_owned())
            .workflow_type(HeartbeatDetailsWorkflow::name().to_owned())
            .build(),
    ));
    // The heartbeat details, and the workflow's result that repeats them.
    assert_same_targets(
        driver.targets.lock().unwrap().clone(),
        vec![workflow_target.clone(), workflow_target],
    );
}

#[derive(Debug, Clone, Copy)]
enum FailedTask {
    Workflow,
    Activity,
}

// The client stores the workflow's input, the workflow stores the activity's input, the
// activity stores its result and the workflow stores its own. Retrieval follows the same
// order, so a call number picks out exactly one task path.
#[rstest::rstest]
#[case::workflow_task_retrieve(None, Some(1), FailedTask::Workflow)]
#[case::workflow_task_store(Some(2), None, FailedTask::Workflow)]
#[case::activity_task_retrieve(None, Some(2), FailedTask::Activity)]
#[case::activity_task_store(Some(3), None, FailedTask::Activity)]
#[tokio::test]
async fn a_storage_failure_fails_the_task_which_is_retried(
    #[case] fail_store: Option<usize>,
    #[case] fail_retrieve: Option<usize>,
    #[case] failed_task: FailedTask,
) {
    let driver = Arc::new(FailingDriver {
        fail_store,
        fail_retrieve,
        ..Default::default()
    });
    let client = client_with_storage(driver).await;
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let runtime = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(
        &format!("storage_failure_{failed_task:?}"),
        Some(runtime),
        Some(client.clone()),
    );
    starter
        .sdk_config
        .register_activities(LargePayloadActivities);
    starter
        .sdk_config
        .register_workflow::<LargePayloadWorkflow>()
        .unwrap();
    let workflow_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let input = "f".repeat(10_000);
    let handle = worker
        .submit_workflow(
            LargePayloadWorkflow::run,
            input.clone(),
            WorkflowStartOptions::new(starter.get_task_queue(), workflow_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert_eq!(handle.get_result(Default::default()).await.unwrap(), input);

    let events = starter.get_history().await.events;
    match failed_task {
        FailedTask::Workflow => {
            let failed = events
                .iter()
                .find_map(|event| match &event.attributes {
                    Some(Attributes::WorkflowTaskFailedEventAttributes(attrs)) => Some(attrs),
                    _ => None,
                })
                .expect("the workflow task should have failed");
            assert!(
                failed
                    .failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains(INJECTED_FAILURE),
                "{failed:?}"
            );
            assert_eq!(
                failed.cause(),
                WorkflowTaskFailedCause::ExternalStorageFailure
            );
        }
        FailedTask::Activity => {
            let started = events
                .iter()
                .find_map(|event| match &event.attributes {
                    Some(Attributes::ActivityTaskStartedEventAttributes(attrs)) => Some(attrs),
                    _ => None,
                })
                .expect("the activity should have started");
            assert_eq!(started.attempt, 2, "the failed attempt should be retried");
            assert!(
                started
                    .last_failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains(INJECTED_FAILURE),
                "{started:?}"
            );
        }
    }

    let failure_metric = match failed_task {
        FailedTask::Workflow => "temporal_workflow_task_execution_failed{",
        FailedTask::Activity => "temporal_activity_execution_failed{",
    };
    let metric_line = eventually(
        || {
            let endpoint = format!("http://{addr}/metrics");
            async move {
                get_text(endpoint)
                    .await
                    .lines()
                    .find(|l| l.starts_with(failure_metric))
                    .map(ToString::to_string)
                    .ok_or("the task failure has not been counted")
            }
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(
        metric_line.contains("failure_reason=\"ExternalStorageError\""),
        "{metric_line}"
    );
}

#[tokio::test]
async fn a_reference_reaching_a_worker_without_storage_fails_the_workflow_task() {
    let client = client_with_storage(Arc::new(InMemoryDriver::default())).await;
    let worker_client = Client::new(
        get_integ_connection(None).await,
        ClientOptions::new(integ_namespace()).build(),
    )
    .unwrap();
    let task_queue = format!("storage_not_configured_{}", rand_6_chars());
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let worker_options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<LargePayloadWorkflow>()
        .unwrap()
        .register_activities(LargePayloadActivities)
        .build();
    let worker = temporalio_sdk::Worker::new(&runtime, worker_client, worker_options).unwrap();

    worker
        .run_until(async {
            let handle = client
                .start_workflow(
                    LargePayloadWorkflow::run,
                    "n".repeat(10_000),
                    WorkflowStartOptions::new(task_queue.clone(), task_queue.clone()).build(),
                )
                .await
                .unwrap();
            let failed = eventually(
                || {
                    let handle = &handle;
                    async move {
                        handle
                            .fetch_history(Default::default())
                            .into_events()
                            .await
                            .unwrap()
                            .into_iter()
                            .find_map(|event| match event.attributes {
                                Some(Attributes::WorkflowTaskFailedEventAttributes(attrs)) => {
                                    Some(attrs)
                                }
                                _ => None,
                            })
                            .ok_or("the workflow task has not failed")
                    }
                },
                Duration::from_secs(10),
            )
            .await
            .unwrap();
            assert!(
                failed
                    .failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("TMPRL1105"),
                "{failed:?}"
            );
            assert_eq!(
                failed.cause(),
                WorkflowTaskFailedCause::ExternalStorageFailure
            );
            handle
                .terminate(WorkflowTerminateOptions::default())
                .await
                .unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn a_failed_heartbeat_store_is_returned_to_the_activity() {
    let driver = Arc::new(FailingDriver {
        fail_store: Some(1),
        ..Default::default()
    });
    let client = client_with_storage(driver).await;

    let mut starter = CoreWfStarter::new_with_overrides(
        HeartbeatFailureWorkflow::name(),
        None,
        Some(client.clone()),
    );
    starter.sdk_config.register_activities(HeartbeatActivities);
    starter
        .sdk_config
        .register_workflow::<HeartbeatFailureWorkflow>()
        .unwrap();
    let workflow_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            HeartbeatFailureWorkflow::run,
            (),
            WorkflowStartOptions::new(starter.get_task_queue(), workflow_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let reported = handle.get_result(Default::default()).await.unwrap();
    assert!(reported.contains(INJECTED_FAILURE), "{reported}");
}
