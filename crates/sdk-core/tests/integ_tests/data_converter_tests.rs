use crate::common::{CoreWfStarter, get_integ_connection, integ_namespace};
use futures::{FutureExt, future::BoxFuture};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    Client, ClientOptions, UntypedWorkflow, WorkflowDescribeOptions, WorkflowExecuteUpdateOptions,
    WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
    errors::{WorkflowGetResultError, WorkflowUpdateError},
};
use temporalio_common::{
    data_converters::{
        DataConverter, DefaultFailureConverter, DefaultPayloadCodec, FailureConverter, MultiArgs2,
        PayloadCodec, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData, TemporalDeserializable, TemporalSerializable,
    },
    error::{IncomingError, OutgoingError},
    protos::{
        coresdk::AsJsonPayloadExt,
        temporal::api::{
            common::v1::{Payload, RetryPolicy},
            failure::v1::failure::FailureInfo,
            history::v1::history_event::Attributes,
        },
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ApplicationFailure, CancellableFuture, MemoValue, SyncWorkflowContext,
    WorkflowContext, WorkflowContextView, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

#[derive(Clone, Debug)]
struct TrackedWrapper(TrackedValue);
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct TrackedValue {
    data: String,
    serialize_count: u32,
    deserialize_count: u32,
}

impl TrackedValue {
    fn new(data: String) -> Self {
        Self {
            data,
            serialize_count: 0,
            deserialize_count: 0,
        }
    }
}

impl TemporalSerializable for TrackedWrapper {
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        let mut wire = self.0.clone();
        wire.serialize_count += 1;
        let json = serde_json::to_vec(&wire)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))?;
        Ok(Payload {
            metadata: {
                let mut hm = std::collections::HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: json,
            external_payloads: vec![],
        })
    }
}

impl TemporalDeserializable for TrackedWrapper {
    fn from_payload(
        _: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError> {
        let wire: TrackedValue = serde_json::from_slice(&payload.data)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))?;
        let mut val = wire.clone();
        val.deserialize_count += 1;
        Ok(TrackedWrapper(val))
    }
}

struct TestActivities;
#[activities]
impl TestActivities {
    #[activity]
    async fn process_tracked(
        _ctx: ActivityContext,
        mut input: TrackedWrapper,
    ) -> Result<TrackedWrapper, ActivityError> {
        input.0.data = format!("activity-processed:{}", input.0.data);
        Ok(input)
    }
}

struct FailurePayloadActivities;
#[activities]
impl FailurePayloadActivities {
    #[activity]
    async fn cancel_with_tracked_details(ctx: ActivityContext) -> Result<(), ActivityError> {
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = ctx.cancelled() => break,
                _ = ticker.tick() => ctx.record_heartbeat(()).await?,
            }
        }
        Err(ActivityError::cancelled_with_details(
            "codec-cancel-details".to_string(),
        ))
    }

    #[activity]
    async fn heartbeat_then_timeout(ctx: ActivityContext) -> Result<(), ActivityError> {
        ctx.record_heartbeat(TrackedWrapper(TrackedValue::new(
            "codec-heartbeat-details".to_string(),
        )))
        .await?;
        ctx.cancelled().await;
        Ok(())
    }
}

const FAILURE_CONVERTER_ERROR_MESSAGE: &str = "intentional failure converter error";
const WORKFLOW_FAILURE_MESSAGE: &str = "workflow converter fallback failure";
const ACTIVITY_PANIC_MESSAGE: &str = "activity converter fallback panic";
const QUERY_FAILURE_MESSAGE: &str = "query converter fallback failure";
const UPDATE_VALIDATOR_FAILURE_MESSAGE: &str = "update validator converter fallback failure";
const UPDATE_HANDLER_FAILURE_MESSAGE: &str = "update handler converter fallback failure";
const CODEC_RETRY_MARKER: &str = "codec-retry-test";

#[derive(Debug)]
struct FailingFailureConverter;

impl FailureConverter for FailingFailureConverter {
    fn to_failure(
        &self,
        err: OutgoingError,
        _payload_converter: &PayloadConverter,
        _context: &SerializationContextData,
    ) -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        temporalio_common::protos::temporal::api::failure::v1::Failure::application_failure(
            format!(
                "Failed converting error to failure: Encoding error: {FAILURE_CONVERTER_ERROR_MESSAGE}, original error message: {}",
                err
            ),
            false,
        )
    }

    fn to_error(
        &self,
        failure: temporalio_common::protos::temporal::api::failure::v1::Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<IncomingError, PayloadConversionError> {
        DefaultFailureConverter.to_error(failure, payload_converter, context)
    }
}

async fn starter_with_failing_failure_converter(test_name: &str) -> CoreWfStarter {
    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        FailingFailureConverter,
        DefaultPayloadCodec,
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();
    CoreWfStarter::new_with_overrides(test_name, None, Some(client))
}

#[workflow]
#[derive(Default)]
struct DataConverterTestWorkflow;
#[workflow_methods]
impl DataConverterTestWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: TrackedWrapper,
    ) -> WorkflowResult<TrackedWrapper> {
        let output = ctx
            .execute_activity(
                TestActivities::process_tracked,
                input,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;

        Ok(output)
    }
}

#[workflow]
#[derive(Default)]
struct DescribeDataConverterWorkflow;
#[workflow_methods]
impl DescribeDataConverterWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: TrackedWrapper,
    ) -> WorkflowResult<TrackedWrapper> {
        ctx.upsert_memo([
            ("tracked", Some(MemoValue::new(input.0.data.clone()))),
            ("wrapped", Some(MemoValue::new(input.clone()))),
            ("removed", Some(MemoValue::new(true))),
        ])?;
        ctx.upsert_memo([("removed", None)])?;
        let output = ctx
            .execute_activity(
                TestActivities::process_tracked,
                input,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;

        Ok(output)
    }
}

#[workflow]
#[derive(Default)]
struct CancellationDetailsWorkflow;
#[workflow_methods]
impl CancellationDetailsWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<TrackedWrapper> {
        let act = ctx.execute_activity(
            FailurePayloadActivities::cancel_with_tracked_details,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .heartbeat_timeout(Duration::from_secs(1))
                .cancellation_type(
                    temporalio_common::protos::coresdk::workflow_commands::ActivityCancellationType::WaitCancellationCompleted,
                )
                .build(),
        );
        // WaitCancellationCompleted only carries activity-supplied details if the worker has
        // started the activity and observed the cancellation.
        ctx.timer(Duration::from_millis(100)).await;
        act.cancel();
        let err = act.await.expect_err("activity should be cancelled");
        let Some(cancelled) = err.as_cancelled() else {
            panic!("expected cancelled failure, got {err:?}");
        };
        let details = cancelled
            .details::<String>()
            .expect("cancellation details should decode")
            .expect("cancellation details should be present");
        Ok(TrackedWrapper(TrackedValue::new(details)))
    }
}

#[workflow]
#[derive(Default)]
struct HeartbeatDetailsWorkflow;
#[workflow_methods]
impl HeartbeatDetailsWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<TrackedWrapper> {
        let err = ctx
            .execute_activity(
                FailurePayloadActivities::heartbeat_then_timeout,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .heartbeat_timeout(Duration::from_secs(1))
                    .retry_policy(RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    })
                    .build(),
            )
            .await
            .expect_err("activity should time out");
        let timeout = err.as_timeout().expect("activity should timeout");
        let details = timeout
            .last_heartbeat_details()?
            .expect("heartbeat details should be present");
        Ok(TrackedWrapper(details))
    }
}

#[workflow]
#[derive(Default)]
struct WorkflowFailureFallbackWorkflow;
#[workflow_methods]
impl WorkflowFailureFallbackWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Err(ApplicationFailure::new(WORKFLOW_FAILURE_MESSAGE).into())
    }
}

struct PanicActivities;
#[activities]
impl PanicActivities {
    #[activity]
    async fn panic_activity(_ctx: ActivityContext, _input: String) -> Result<(), ActivityError> {
        panic!("{ACTIVITY_PANIC_MESSAGE}");
    }
}

#[workflow]
#[derive(Default)]
struct ActivityPanicFallbackWorkflow;
#[workflow_methods]
impl ActivityPanicFallbackWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let _ = ctx
            .execute_activity(
                PanicActivities::panic_activity,
                String::new(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .retry_policy(RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    })
                    .build(),
            )
            .await
            .expect_err("activity should fail");
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct QueryUpdateFailureFallbackWorkflow {
    finish: bool,
}

#[workflow_methods]
impl QueryUpdateFailureFallbackWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.finish).await?;
        Ok(())
    }

    #[signal]
    fn finish(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.finish = true;
    }

    #[query]
    fn fail_query(
        &self,
        _ctx: &WorkflowContextView,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err(QUERY_FAILURE_MESSAGE.into())
    }

    #[update_validator(fail_validated_update)]
    fn validate_fail_validated_update(
        &self,
        _ctx: &WorkflowContextView,
        _input: &(),
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err(UPDATE_VALIDATOR_FAILURE_MESSAGE.into())
    }

    #[update]
    fn fail_validated_update(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: ()) {}

    #[update]
    async fn fail_update(
        _ctx: &mut WorkflowContext<Self>,
        _input: (),
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err(UPDATE_HANDLER_FAILURE_MESSAGE.into())
    }
}

#[tokio::test]
async fn custom_failure_converter_fallback_applied_to_workflow_failures() {
    let wf_name = WorkflowFailureFallbackWorkflow::name();
    let mut starter = starter_with_failing_failure_converter(wf_name).await;
    starter
        .sdk_config
        .register_workflow::<WorkflowFailureFallbackWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            WorkflowFailureFallbackWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let failure = match handle.get_result(Default::default()).await.unwrap_err() {
        WorkflowGetResultError::Failed(failure) => failure,
        err => panic!("unexpected workflow result error: {err:?}"),
    };
    assert_eq!(
        failure.failure().message,
        format!(
            "Failed converting error to failure: Encoding error: {FAILURE_CONVERTER_ERROR_MESSAGE}, original error message: {WORKFLOW_FAILURE_MESSAGE}"
        )
    );
    assert!(matches!(
        failure.failure().failure_info.as_ref(),
        Some(FailureInfo::ApplicationFailureInfo(_))
    ));
}

#[tokio::test]
async fn custom_failure_converter_fallback_applied_to_activity_panic_failures() {
    let wf_name = ActivityPanicFallbackWorkflow::name();
    let mut starter = starter_with_failing_failure_converter(wf_name).await;
    starter.sdk_config.register_activities(PanicActivities);
    starter
        .sdk_config
        .register_workflow::<ActivityPanicFallbackWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ActivityPanicFallbackWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();

    let history = handle.fetch_history(Default::default()).await.unwrap();
    let activity_failure = history
        .into_events()
        .into_iter()
        .find_map(|event| match event.attributes {
            Some(Attributes::ActivityTaskFailedEventAttributes(attrs)) => attrs.failure,
            _ => None,
        })
        .expect("workflow history should contain an activity failure");

    assert_eq!(
        activity_failure.message,
        format!(
            "Failed converting error to failure: Encoding error: {FAILURE_CONVERTER_ERROR_MESSAGE}, original error message: Activity function panicked: {ACTIVITY_PANIC_MESSAGE}"
        )
    );
    assert!(matches!(
        activity_failure.failure_info,
        Some(FailureInfo::ApplicationFailureInfo(_))
    ));
}

#[tokio::test]
async fn custom_failure_converter_fallback_applied_to_query_failures() {
    let wf_name = QueryUpdateFailureFallbackWorkflow::name();
    let mut starter = starter_with_failing_failure_converter(wf_name).await;
    starter
        .sdk_config
        .register_workflow::<QueryUpdateFailureFallbackWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            QueryUpdateFailureFallbackWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();

    let query_and_finish = async {
        let err = handle
            .query(
                QueryUpdateFailureFallbackWorkflow::fail_query,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
            .expect_err("query should fail");
        assert!(err.to_string().contains(&format!(
            "Failed converting error to failure: Encoding error: {FAILURE_CONVERTER_ERROR_MESSAGE}, original error message: {QUERY_FAILURE_MESSAGE}"
        )));
        handle
            .signal(
                QueryUpdateFailureFallbackWorkflow::finish,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(query_and_finish, worker.run_until_done());
    worker_res.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}

#[tokio::test]
async fn custom_failure_converter_fallback_applied_to_update_validation_failures() {
    let wf_name = QueryUpdateFailureFallbackWorkflow::name();
    let mut starter = starter_with_failing_failure_converter(wf_name).await;
    starter
        .sdk_config
        .register_workflow::<QueryUpdateFailureFallbackWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            QueryUpdateFailureFallbackWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, format!("{wf_name}_validator")).build(),
        )
        .await
        .unwrap();

    let update_and_finish = async {
        let err = handle
            .execute_update(
                QueryUpdateFailureFallbackWorkflow::fail_validated_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .expect_err("update should be rejected");
        let WorkflowUpdateError::Failed(failure) = err else {
            panic!("expected failed update error");
        };
        assert_eq!(
            failure.message,
            format!(
                "Failed converting error to failure: Encoding error: {FAILURE_CONVERTER_ERROR_MESSAGE}, original error message: {UPDATE_VALIDATOR_FAILURE_MESSAGE}"
            )
        );
        handle
            .signal(
                QueryUpdateFailureFallbackWorkflow::finish,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(update_and_finish, worker.run_until_done());
    worker_res.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}

#[tokio::test]
async fn custom_failure_converter_fallback_applied_to_update_handler_failures() {
    let wf_name = QueryUpdateFailureFallbackWorkflow::name();
    let mut starter = starter_with_failing_failure_converter(wf_name).await;
    starter
        .sdk_config
        .register_workflow::<QueryUpdateFailureFallbackWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            QueryUpdateFailureFallbackWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, format!("{wf_name}_handler")).build(),
        )
        .await
        .unwrap();

    let update_and_finish = async {
        let err = handle
            .execute_update(
                QueryUpdateFailureFallbackWorkflow::fail_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .expect_err("update should fail");
        let WorkflowUpdateError::Failed(failure) = err else {
            panic!("expected failed update error");
        };
        assert_eq!(
            failure.message,
            format!(
                "Failed converting error to failure: Encoding error: {FAILURE_CONVERTER_ERROR_MESSAGE}, original error message: {UPDATE_HANDLER_FAILURE_MESSAGE}"
            )
        );
        handle
            .signal(
                QueryUpdateFailureFallbackWorkflow::finish,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(update_and_finish, worker.run_until_done());
    worker_res.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}

#[tokio::test]
async fn data_converter_tracks_serialization_points() {
    let wf_name = DataConverterTestWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(TestActivities);
    starter
        .sdk_config
        .register_workflow::<DataConverterTestWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;

    let input = TrackedValue::new("test-input".to_string());
    assert_eq!(input.serialize_count, 0);
    assert_eq!(input.deserialize_count, 0);

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            DataConverterTestWorkflow::run,
            TrackedWrapper(input),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let output = handle.get_result(Default::default()).await.unwrap().0;

    assert!(
        output.data.contains("activity-processed:test-input"),
        "Activity should have processed the input, got: {}",
        output.data
    );

    // Expected path:
    // 1. Client serializes input (serialize_count: 0 -> 1)
    // 2. Workflow deserializes input (deserialize_count: 0 -> 1)
    // 3. Workflow serializes for activity call (serialize_count: 1 -> 2)
    // 4. Activity deserializes input (deserialize_count: 1 -> 2)
    // 5. Activity serializes result (serialize_count: 2 -> 3)
    // 6. Workflow deserializes activity result (deserialize_count: 2 -> 3)
    // 7. Workflow serializes final result (serialize_count: 3 -> 4)
    // 8. Client deserializes result (deserialize_count: 3 -> 4)

    assert!(
        output.serialize_count == 4,
        "Value should have been serialized 4 times, was serialized {} times",
        output.serialize_count
    );
    assert!(
        output.deserialize_count == 4,
        "Value should have been deserialized 4 times, was deserialized {} times",
        output.deserialize_count
    );
}

#[workflow]
#[derive(Default)]
struct MultiArgs2Workflow;
#[workflow_methods]
impl MultiArgs2Workflow {
    #[run]
    async fn run(
        _ctx: &mut WorkflowContext<Self>,
        input: MultiArgs2<String, i32>,
    ) -> WorkflowResult<String> {
        Ok(format!("received: {} and {}", input.0, input.1))
    }
}

#[tokio::test]
async fn multi_args_serializes_as_multiple_payloads() {
    let wf_name = MultiArgs2Workflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<MultiArgs2Workflow>()
        .unwrap();
    let mut worker = starter.worker().await;

    let input = MultiArgs2("hello".to_string(), 42);

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            MultiArgs2Workflow::run,
            input,
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let output = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(output, "received: hello and 42");

    // Verify the workflow history contains multiple payloads in the input
    let client = starter.get_core_client().await;
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(wf_name)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();

    let workflow_started_event = events
        .iter()
        .find_map(|e| {
            if let Attributes::WorkflowExecutionStartedEventAttributes(attrs) =
                e.attributes.as_ref().unwrap()
            {
                Some(attrs)
            } else {
                None
            }
        })
        .expect("Should find WorkflowExecutionStarted event");

    let input_payloads = workflow_started_event
        .input
        .as_ref()
        .expect("Should have input payloads");

    assert_eq!(
        input_payloads.payloads.len(),
        2,
        "MultiArgs2<A, B> should produce 2 payloads, got {}",
        input_payloads.payloads.len()
    );

    // Verify the content of each payload
    let first_payload_data: String =
        serde_json::from_slice(&input_payloads.payloads[0].data).unwrap();
    assert_eq!(first_payload_data, "hello");

    let second_payload_data: i32 =
        serde_json::from_slice(&input_payloads.payloads[1].data).unwrap();
    assert_eq!(second_payload_data, 42);
}

/// A codec that XORs payload data with a key and tracks encode/decode operations.
struct XorCodec {
    key: u8,
    gate_on_metadata: bool,
    encode_count: AtomicUsize,
    decode_count: AtomicUsize,
}

impl XorCodec {
    fn new(key: u8) -> Self {
        Self {
            gate_on_metadata: true,
            key,
            encode_count: AtomicUsize::new(0),
            decode_count: AtomicUsize::new(0),
        }
    }

    fn new_with_metadata_gate(key: u8, gate_on_metadata: bool) -> Self {
        Self {
            key,
            gate_on_metadata,
            encode_count: AtomicUsize::new(0),
            decode_count: AtomicUsize::new(0),
        }
    }

    fn encode_count(&self) -> usize {
        self.encode_count.load(Ordering::SeqCst)
    }

    fn decode_count(&self) -> usize {
        self.decode_count.load(Ordering::SeqCst)
    }
}

impl PayloadCodec for XorCodec {
    fn encode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        let count = payloads.len();
        eprintln!("XorCodec::encode called with {} payloads", count);
        self.encode_count.fetch_add(count, Ordering::SeqCst);
        let key = self.key;
        let gate_on_metadata = self.gate_on_metadata;
        async move {
            Ok(payloads
                .into_iter()
                .map(|mut p| {
                    p.data = p.data.iter().map(|b| b ^ key).collect();
                    if gate_on_metadata {
                        p.metadata.insert("xor_encoded".to_string(), vec![key]);
                    }
                    p
                })
                .collect())
        }
        .boxed()
    }

    fn decode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        let count = payloads.len();
        eprintln!("XorCodec::decode called with {} payloads", count);
        self.decode_count.fetch_add(count, Ordering::SeqCst);
        let key = self.key;
        let gate_on_metadata = self.gate_on_metadata;
        async move {
            Ok(payloads
                .into_iter()
                .map(|mut p| {
                    if !gate_on_metadata || p.metadata.remove("xor_encoded").is_some() {
                        p.data = p.data.iter().map(|b| b ^ key).collect();
                    }
                    p
                })
                .collect())
        }
        .boxed()
    }
}

#[derive(Clone, Copy, Debug)]
enum CodecFailurePoint {
    WorkflowEncode,
    WorkflowDecode,
    ActivityEncode,
    ActivityDecode,
}

struct FailOnceCodec {
    failure_point: CodecFailurePoint,
    failed: AtomicBool,
    matching_calls: AtomicUsize,
}

impl FailOnceCodec {
    fn new(failure_point: CodecFailurePoint) -> Self {
        Self {
            failure_point,
            failed: AtomicBool::new(false),
            matching_calls: AtomicUsize::new(0),
        }
    }
}

impl PayloadCodec for FailOnceCodec {
    fn encode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        let matches_context = matches!(
            (self.failure_point, context),
            (
                CodecFailurePoint::WorkflowEncode,
                SerializationContextData::Workflow
            ) | (
                CodecFailurePoint::ActivityEncode,
                SerializationContextData::Activity
            )
        );
        let marker = if matches!(self.failure_point, CodecFailurePoint::WorkflowEncode) {
            b"activity-processed:codec-retry-test".as_slice()
        } else {
            CODEC_RETRY_MARKER.as_bytes()
        };
        let matches_payload = payloads.iter().any(|payload| {
            payload
                .data
                .windows(marker.len())
                .any(|data| data == marker)
        });
        let should_fail = matches_context && matches_payload;
        if should_fail {
            self.matching_calls.fetch_add(1, Ordering::SeqCst);
        }
        let should_fail = should_fail && !self.failed.swap(true, Ordering::SeqCst);
        async move {
            if should_fail {
                Err(PayloadConversionError::EncodingError(
                    "intentional codec encode failure".into(),
                ))
            } else {
                Ok(payloads)
            }
        }
        .boxed()
    }

    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        let matches_context = matches!(
            (self.failure_point, context),
            (
                CodecFailurePoint::WorkflowDecode,
                SerializationContextData::Workflow
            ) | (
                CodecFailurePoint::ActivityDecode,
                SerializationContextData::Activity
            )
        );
        let matches_payload = payloads.iter().any(|payload| {
            payload
                .data
                .windows(CODEC_RETRY_MARKER.len())
                .any(|data| data == CODEC_RETRY_MARKER.as_bytes())
        });
        let should_fail = matches_context && matches_payload;
        if should_fail {
            self.matching_calls.fetch_add(1, Ordering::SeqCst);
        }
        let should_fail = should_fail && !self.failed.swap(true, Ordering::SeqCst);
        async move {
            if should_fail {
                Err(PayloadConversionError::EncodingError(
                    "intentional codec decode failure".into(),
                ))
            } else {
                Ok(payloads)
            }
        }
        .boxed()
    }
}

#[rstest::rstest]
#[case::workflow_encode(CodecFailurePoint::WorkflowEncode)]
#[case::workflow_decode(CodecFailurePoint::WorkflowDecode)]
#[case::activity_encode(CodecFailurePoint::ActivityEncode)]
#[case::activity_decode(CodecFailurePoint::ActivityDecode)]
#[tokio::test]
async fn codec_errors_fail_tasks_and_retry(#[case] failure_point: CodecFailurePoint) {
    let codec = Arc::new(FailOnceCodec::new(failure_point));
    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();
    let mut starter = CoreWfStarter::new_with_overrides(
        &format!("codec_errors_fail_and_retry_{failure_point:?}"),
        None,
        Some(client),
    );
    starter.sdk_config.register_activities(TestActivities);
    starter
        .sdk_config
        .register_workflow::<DataConverterTestWorkflow>()
        .unwrap();
    let workflow_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;
    let handle = worker
        .submit_workflow(
            DataConverterTestWorkflow::run,
            TrackedWrapper(TrackedValue::new(CODEC_RETRY_MARKER.to_owned())),
            WorkflowStartOptions::new(starter.get_task_queue(), workflow_id).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();

    let output = handle.get_result(Default::default()).await.unwrap().0;
    assert_eq!(
        output.data,
        format!("activity-processed:{CODEC_RETRY_MARKER}")
    );
    assert!(codec.failed.load(Ordering::SeqCst));
    assert!(codec.matching_calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn codec_encodes_and_decodes_payloads() {
    let wf_name = DataConverterTestWorkflow::name();
    let codec = Arc::new(XorCodec::new(0x42));

    // Create a client with our custom codec
    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter.sdk_config.register_activities(TestActivities);
    starter
        .sdk_config
        .register_workflow::<DataConverterTestWorkflow>()
        .unwrap();
    // Use task queue name as workflow ID to avoid collisions with parallel tests
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let input = TrackedValue::new("codec-test".to_string());
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            DataConverterTestWorkflow::run,
            TrackedWrapper(input),
            WorkflowStartOptions::new(task_queue, wf_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let output = handle.get_result(Default::default()).await.unwrap().0;

    // Verify the workflow processed correctly (activity echoed the data)
    assert!(
        output.data.contains("activity-processed:codec-test"),
        "Activity should have processed the input, got: {}",
        output.data
    );

    // Verify the codec was used for both encoding and decoding
    assert!(
        codec.encode_count() > 0,
        "Codec should have encoded payloads, but encode_count was 0"
    );
    assert!(
        codec.decode_count() > 0,
        "Codec should have decoded payloads, but decode_count was 0"
    );
}

#[tokio::test]
async fn describe_decodes_workflow_payload_fields() {
    let wf_name = DescribeDataConverterWorkflow::name();
    let codec = Arc::new(XorCodec::new(0x42));

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter.sdk_config.register_activities(TestActivities);
    starter
        .sdk_config
        .register_workflow::<DescribeDataConverterWorkflow>()
        .unwrap();
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            DescribeDataConverterWorkflow::run,
            TrackedWrapper(TrackedValue::new("codec-describe".to_string())),
            WorkflowStartOptions::new(starter.get_task_queue(), wf_id)
                .static_summary("codec summary")
                .static_details("codec details")
                .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let decode_count_before = codec.decode_count();
    let desc = handle
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap();

    assert!(
        codec.decode_count() > decode_count_before,
        "Describe should have decoded response payloads"
    );
    assert_eq!(
        desc.memo().get::<String>("tracked").unwrap(),
        Some("codec-describe".to_owned())
    );
    assert_eq!(
        desc.memo()
            .get::<TrackedWrapper>("wrapped")
            .unwrap()
            .map(|value| value.0.data),
        Some("codec-describe".to_owned())
    );
    assert!(!desc.memo().contains_key("removed"));
    let raw_user_metadata = desc
        .raw_description
        .execution_config
        .as_ref()
        .and_then(|cfg| cfg.user_metadata.as_ref())
        .expect("describe response should include user metadata");
    assert_eq!(
        raw_user_metadata.summary,
        Some("codec summary".as_json_payload().unwrap())
    );
    assert_eq!(
        raw_user_metadata.details,
        Some("codec details".as_json_payload().unwrap())
    );
    assert_eq!(desc.static_summary(), Some("codec summary"));
    assert_eq!(desc.static_details(), Some("codec details"));
}

#[tokio::test]
async fn describe_decodes_user_metadata_with_ungated_xor_codec() {
    let wf_name = DescribeDataConverterWorkflow::name();
    let codec = Arc::new(XorCodec::new_with_metadata_gate(0x42, false));

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter.sdk_config.register_activities(TestActivities);
    starter
        .sdk_config
        .register_workflow::<DescribeDataConverterWorkflow>()
        .unwrap();
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            DescribeDataConverterWorkflow::run,
            TrackedWrapper(TrackedValue::new("codec-describe".to_string())),
            WorkflowStartOptions::new(starter.get_task_queue(), wf_id)
                .static_summary("codec summary")
                .static_details("codec details")
                .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let decode_count_before = codec.decode_count();
    let desc = handle
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap();

    assert!(
        codec.decode_count() > decode_count_before,
        "Describe should have decoded response payloads"
    );
    assert_eq!(
        desc.memo().get::<String>("tracked").unwrap(),
        Some("codec-describe".to_owned())
    );
    assert_eq!(
        desc.memo()
            .get::<TrackedWrapper>("wrapped")
            .unwrap()
            .map(|value| value.0.data),
        Some("codec-describe".to_owned())
    );
    // Making sure codec isn't used when decoding user metadata
    assert_eq!(desc.static_summary(), Some("codec summary"));
    assert_eq!(desc.static_details(), Some("codec details"));
}

#[tokio::test]
async fn codec_roundtrips_activity_cancellation_details() {
    let wf_name = CancellationDetailsWorkflow::name();
    let codec = Arc::new(XorCodec::new(0x42));

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_activities(FailurePayloadActivities);
    starter
        .sdk_config
        .register_workflow::<CancellationDetailsWorkflow>()
        .unwrap();
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            CancellationDetailsWorkflow::run,
            (),
            WorkflowStartOptions::new(starter.get_task_queue(), wf_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let details = handle.get_result(Default::default()).await.unwrap().0;
    assert_eq!(details.data, "codec-cancel-details");
    assert!(
        codec.encode_count() > 0,
        "codec should encode cancellation details"
    );
    assert!(
        codec.decode_count() > 0,
        "codec should decode cancellation details"
    );
}

#[tokio::test]
async fn codec_roundtrips_activity_heartbeat_timeout_details() {
    let wf_name = HeartbeatDetailsWorkflow::name();
    let codec = Arc::new(XorCodec::new(0x42));

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_activities(FailurePayloadActivities);
    starter
        .sdk_config
        .register_workflow::<HeartbeatDetailsWorkflow>()
        .unwrap();
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            HeartbeatDetailsWorkflow::run,
            (),
            WorkflowStartOptions::new(starter.get_task_queue(), wf_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let details = handle.get_result(Default::default()).await.unwrap().0;
    assert_eq!(details.data, "codec-heartbeat-details");
    assert!(
        codec.encode_count() > 0,
        "codec should encode heartbeat details"
    );
    assert!(
        codec.decode_count() > 0,
        "codec should decode heartbeat details"
    );
}
