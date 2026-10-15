use super::*;
use crate::{
    ClientInterceptor, ClientOptions, ConnectionOptions,
    callback_based::{CallbackBasedGrpcService, GrpcRequest, GrpcSuccessResponse},
    interceptors::StartNexusOperationInput,
    test_helpers::XorCodec,
};
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use prost::Message;
use std::sync::Arc;
use temporalio_common::{
    data_converters::{DataConverter, DefaultFailureConverter, PayloadCodec, PayloadConverter},
    protos::temporal::api::{
        common::v1::Payload,
        failure::v1::{CanceledFailureInfo, Failure, failure::FailureInfo},
        nexus::v1::NexusOperationExecutionInfo,
        workflowservice::v1::{
            RequestCancelNexusOperationExecutionResponse, StartNexusOperationExecutionResponse,
            describe_nexus_operation_execution_response,
        },
    },
    search_attributes::SearchAttributeKey,
};

#[nexus_rpc::service]
trait Payments {
    #[operation]
    fn charge(input: String) -> String;
}

async fn client(
    callback: impl Fn(GrpcRequest) -> Vec<u8> + Send + Sync + 'static,
    converter: DataConverter,
    interceptors: Vec<Arc<dyn ClientInterceptor>>,
) -> Client {
    let mut options = ConnectionOptions::new("http://localhost:7233".parse::<url::Url>().unwrap())
        .dns_load_balancing(None)
        .service_override(CallbackBasedGrpcService {
            callback: Arc::new(move |request| {
                let proto = callback(request);
                Box::pin(async move {
                    Ok(GrpcSuccessResponse {
                        headers: Default::default(),
                        proto,
                    })
                })
            }),
        })
        .identity("test-client")
        .build();
    options.skip_get_system_info = true;
    Client::connect(
        options,
        ClientOptions::new("test-namespace")
            .data_converter(converter)
            .client_interceptors(interceptors)
            .build(),
    )
    .await
    .unwrap()
}

struct Headers;

impl ClientInterceptor for Headers {
    fn start_nexus_operation<'a>(
        &'a self,
        mut input: StartNexusOperationInput,
        next: Next<
            'a,
            StartNexusOperationInput,
            BoxFuture<'a, Result<StartNexusOperationExecutionResponse, NexusClientError>>,
        >,
    ) -> BoxFuture<'a, Result<StartNexusOperationExecutionResponse, NexusClientError>> {
        input
            .request
            .nexus_header
            .insert("trace".into(), "span".into());
        next.run(input)
    }
}

struct CheckedCodec(Arc<Mutex<(usize, usize)>>);

impl PayloadCodec for CheckedCodec {
    fn encode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        assert!(matches!(context, SerializationContextData::Nexus(_)));
        self.0.lock().0 += 1;
        XorCodec.encode(context, payloads)
    }

    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        assert!(matches!(context, SerializationContextData::Nexus(_)));
        self.0.lock().1 += 1;
        XorCodec.decode(context, payloads)
    }
}

#[tokio::test]
async fn request_codec_and_run_pinned_result() {
    let converter = PayloadConverter::default();
    let plain = converter
        .to_payload(
            &SerializationContext::new(&context(), &converter),
            &"order".to_owned(),
        )
        .unwrap();
    let encoded = XorCodec
        .encode(&context(), vec![plain.clone()])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let calls = Arc::new(Mutex::new((0, 0)));
    let poll_count = Arc::new(Mutex::new(0));
    let client = client(
        {
            let poll_count = poll_count.clone();
            move |request| match request.rpc.as_str() {
                "StartNexusOperationExecution" => {
                    assert_eq!(request.headers.get("call-meta").unwrap(), "value");
                    let request =
                        StartNexusOperationExecutionRequest::decode(request.proto).unwrap();
                    assert_eq!(
                        (&*request.namespace, &*request.identity),
                        ("test-namespace", "test-client")
                    );
                    assert_eq!(
                        (&*request.endpoint, &*request.service, &*request.operation),
                        ("endpoint", "Payments", "charge")
                    );
                    assert_eq!(request.operation_id, "charge-123");
                    assert_eq!(request.request_id, "stable-request-id");
                    assert_eq!(request.input, Some(encoded.clone()));
                    assert_eq!(request.nexus_header["trace"], "span");
                    assert_eq!(request.schedule_to_close_timeout.unwrap().seconds, 60);
                    assert_eq!(
                        request.id_conflict_policy,
                        NexusOperationIdConflictPolicy::UseExisting as i32
                    );
                    assert_eq!(request.search_attributes.unwrap().indexed_fields.len(), 1);
                    assert_eq!(
                        request.user_metadata.unwrap().summary.unwrap().data,
                        b"\"summary\""
                    );
                    StartNexusOperationExecutionResponse {
                        run_id: "run-1".into(),
                        started: false,
                    }
                    .encode_to_vec()
                }
                "PollNexusOperationExecution" => {
                    let request =
                        PollNexusOperationExecutionRequest::decode(request.proto).unwrap();
                    assert_eq!(
                        (&*request.operation_id, &*request.run_id),
                        ("charge-123", "run-1")
                    );
                    assert_eq!(request.wait_stage, NexusOperationWaitStage::Closed as i32);
                    let mut count = poll_count.lock();
                    *count += 1;
                    PollNexusOperationExecutionResponse {
                        run_id: "run-1".into(),
                        wait_stage: if *count == 1 {
                            NexusOperationWaitStage::Started
                        } else {
                            NexusOperationWaitStage::Closed
                        } as i32,
                        outcome: (*count == 2).then(|| Outcome::Result(encoded.clone())),
                        ..Default::default()
                    }
                    .encode_to_vec()
                }
                other => panic!("unexpected RPC {other}"),
            }
        },
        DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            CheckedCodec(calls.clone()),
        ),
        vec![Arc::new(Headers)],
    )
    .await;
    let mut rpc_options = RpcOptions::default();
    rpc_options.metadata.insert("call-meta", "value").unwrap();
    let payments = client.nexus_client(Payments, "endpoint");
    let handle = payments
        .start(
            Payments::charge,
            "order".into(),
            NexusOperationStartOptions::new("charge-123")
                .request_id("stable-request-id")
                .schedule_to_close_timeout(Duration::from_secs(60))
                .id_conflict_policy(NexusOperationIdConflictPolicy::UseExisting)
                .search_attributes(SearchAttributes::new([SearchAttributeKey::keyword(
                    "order",
                )
                .value_set("123".into())]))
                .static_summary("summary")
                .rpc_options(rpc_options)
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(handle.operation_id(), "charge-123");
    let resumed = payments
        .get_handle(Payments::charge, handle.operation_id(), handle.run_id())
        .unwrap();
    assert_eq!(resumed.result().await.unwrap(), "order");
    assert_eq!(*poll_count.lock(), 2);
    assert_eq!(*calls.lock(), (1, 1));
}

#[tokio::test]
async fn describe_decodes_operation_payloads_but_preserves_ui_metadata() {
    let converter = PayloadConverter::default();
    let plain = converter
        .to_payload(
            &SerializationContext::new(&context(), &converter),
            &"order".to_owned(),
        )
        .unwrap();
    let encoded = XorCodec
        .encode(&context(), vec![plain.clone()])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let metadata = UserMetadata {
        summary: Some(plain.clone()),
        details: Some(plain.clone()),
    };
    let calls = Arc::new(Mutex::new((0, 0)));
    let client = client(
        {
            let metadata = metadata.clone();
            move |request| {
                assert_eq!(request.rpc, "DescribeNexusOperationExecution");
                let request =
                    DescribeNexusOperationExecutionRequest::decode(request.proto).unwrap();
                assert!(request.include_input && request.include_outcome);
                assert_eq!(
                    (&*request.operation_id, &*request.run_id),
                    ("operation", "run")
                );
                DescribeNexusOperationExecutionResponse {
                    run_id: "run".into(),
                    info: Some(NexusOperationExecutionInfo {
                        user_metadata: Some(metadata.clone()),
                        ..Default::default()
                    }),
                    input: Some(encoded.clone()),
                    outcome: Some(
                        describe_nexus_operation_execution_response::Outcome::Result(
                            encoded.clone(),
                        ),
                    ),
                    ..Default::default()
                }
                .encode_to_vec()
            }
        },
        DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            CheckedCodec(calls.clone()),
        ),
        vec![],
    )
    .await;
    let response = client
        .nexus_client(Payments, "endpoint")
        .get_handle(Payments::charge, "operation", "run")
        .unwrap()
        .describe()
        .await
        .unwrap();
    assert_eq!(response.input, Some(plain.clone()));
    assert_eq!(
        response.outcome,
        Some(describe_nexus_operation_execution_response::Outcome::Result(plain)),
    );
    assert_eq!(response.info.unwrap().user_metadata, Some(metadata));
    assert_eq!(*calls.lock(), (0, 2));
}

#[tokio::test]
async fn cancellation_retains_failure_and_targets_exact_run() {
    let failure = Failure {
        message: "cancelled by caller".into(),
        failure_info: Some(FailureInfo::CanceledFailureInfo(
            CanceledFailureInfo::default(),
        )),
        ..Default::default()
    };
    let client = client(
        {
            let failure = failure.clone();
            move |request| match request.rpc.as_str() {
                "RequestCancelNexusOperationExecution" => {
                    let request =
                        RequestCancelNexusOperationExecutionRequest::decode(request.proto).unwrap();
                    assert_eq!(
                        (&*request.operation_id, &*request.run_id, &*request.reason),
                        ("operation", "run", "stop")
                    );
                    assert!(!request.request_id.is_empty());
                    RequestCancelNexusOperationExecutionResponse {}.encode_to_vec()
                }
                "PollNexusOperationExecution" => PollNexusOperationExecutionResponse {
                    run_id: "run".into(),
                    wait_stage: NexusOperationWaitStage::Closed as i32,
                    outcome: Some(Outcome::Failure(failure.clone())),
                    ..Default::default()
                }
                .encode_to_vec(),
                other => panic!("unexpected RPC {other}"),
            }
        },
        DataConverter::default(),
        vec![],
    )
    .await;
    let handle = client
        .nexus_client(Payments, "endpoint")
        .get_handle(Payments::charge, "operation", "run")
        .unwrap();
    handle
        .cancel_with_options("stop", RpcOptions::default())
        .await
        .unwrap();
    let NexusClientError::Operation(error @ NexusOperationError::Cancelled(_)) =
        handle.result().await.unwrap_err()
    else {
        panic!("expected cancelled operation")
    };
    assert_eq!(error.failure(), Some(&failure));
}

#[tokio::test]
async fn rejects_run_drift_and_missing_terminal_outcome() {
    let client = client(
        move |request| {
            let request = PollNexusOperationExecutionRequest::decode(request.proto).unwrap();
            PollNexusOperationExecutionResponse {
                run_id: if request.operation_id == "drift" {
                    "new-run"
                } else {
                    "run"
                }
                .into(),
                wait_stage: NexusOperationWaitStage::Closed as i32,
                ..Default::default()
            }
            .encode_to_vec()
        },
        DataConverter::default(),
        vec![],
    )
    .await;
    let payments = client.nexus_client(Payments, "endpoint");
    for id in ["drift", "missing-outcome"] {
        let handle = payments.get_handle(Payments::charge, id, "run").unwrap();
        assert!(matches!(
            handle.result().await,
            Err(NexusClientError::MalformedResponse(_))
        ));
    }
}
