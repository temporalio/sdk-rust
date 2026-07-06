use crate::common::{
    CoreWfStarter, NAMESPACE,
    fake_grpc_server::{FakeServer, GenericService, fake_server},
    get_integ_server_options,
    http_proxy::HttpProxy,
};
use assert_matches::assert_matches;
use futures_util::{FutureExt, stream};
use http_body_util::{BodyExt, StreamBody, combinators::BoxBody};
use hyper::{
    body::{Bytes, Frame},
    http::{HeaderMap, HeaderValue},
};
use prost::Message;
use std::{
    collections::HashMap,
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    Connection, GrpcCompression, Namespace, RETRYABLE_ERROR_CODES, RetryOptions, UntypedWorkflow,
    errors::ClientConnectError, grpc::WorkflowService, proxy::HttpConnectProxyOptions,
};
use temporalio_common::protos::temporal::api::{
    cloud::cloudservice::v1::GetNamespaceRequest,
    workflowservice::v1::{
        DescribeNamespaceRequest, GetSystemInfoResponse, GetWorkflowExecutionHistoryRequest,
        ListNamespacesRequest, RespondActivityTaskCanceledResponse, SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse, get_system_info_response,
    },
};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::{net::TcpListener, sync::oneshot};
use tonic::{
    Code, IntoRequest, Request, Status, body::Body, codegen::http::Response, transport::Server,
};
use tracing::info;

#[tokio::test]
async fn can_use_retry_client() {
    // Not terribly interesting by itself but can be useful for manually inspecting metrics etc
    let mut core = CoreWfStarter::new("retry_client");
    let retry_client = core.get_client().await;
    for _ in 0..10 {
        WorkflowService::list_namespaces(
            &mut retry_client.clone(),
            ListNamespacesRequest::default().into_request(),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn can_use_retry_raw_client() {
    let opts = get_integ_server_options();
    let mut connection = Connection::connect(opts).await.unwrap();
    connection
        .describe_namespace(
            DescribeNamespaceRequest {
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn calls_get_system_info() {
    let opts = get_integ_server_options();
    let connection = Connection::connect(opts).await.unwrap();
    assert!(connection.capabilities().is_some());
}

#[derive(Clone)]
enum CompressionTestBehavior {
    RejectGzipGetSystemInfo,
    GenericGzipUnimplemented,
    UnknownMethod,
}

async fn compression_test_server(
    behavior: CompressionTestBehavior,
) -> (FakeServer, Arc<Mutex<Vec<(String, String)>>>) {
    let records = Arc::new(Mutex::new(Vec::new()));
    let records_clone = records.clone();
    let fs = fake_server(move |req| {
        let path = req.uri().path().to_string();
        let encoding = req
            .headers()
            .get("grpc-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        records_clone
            .lock()
            .unwrap()
            .push((path.clone(), encoding.clone()));

        let behavior = behavior.clone();
        async move {
            match behavior {
                CompressionTestBehavior::UnknownMethod => Status::unimplemented(
                    "unknown method GetSystemInfo for service \
                     temporal.api.workflowservice.v1.WorkflowService",
                )
                .into_http(),
                CompressionTestBehavior::GenericGzipUnimplemented
                    if path.ends_with("/GetSystemInfo") && encoding == "gzip" =>
                {
                    Status::unimplemented("gzip is unavailable for this backend").into_http()
                }
                CompressionTestBehavior::RejectGzipGetSystemInfo
                    if path.ends_with("/GetSystemInfo") && encoding == "gzip" =>
                {
                    Status::unimplemented(
                        "grpc: Decompressor is not installed for grpc-encoding \"gzip\"",
                    )
                    .into_http()
                }
                _ if path.ends_with("/GetSystemInfo") => make_ok_response(GetSystemInfoResponse {
                    capabilities: Some(get_system_info_response::Capabilities {
                        sdk_metadata: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                _ if path.ends_with("/SignalWorkflowExecution") => {
                    make_ok_response(SignalWorkflowExecutionResponse::default())
                }
                _ => Response::new(Body::empty()),
            }
        }
        .boxed()
    })
    .await;
    (fs, records)
}

fn compression_test_options(
    fs: &FakeServer,
    compression: GrpcCompression,
) -> temporalio_client::ConnectionOptions {
    let mut opts = get_integ_server_options();
    opts.target = format!("http://localhost:{}", fs.addr.port())
        .parse::<url::Url>()
        .unwrap();
    opts.retry_options = RetryOptions::no_retries();
    opts.grpc_compression = compression;
    opts
}

#[tokio::test]
async fn gzip_get_system_info_failure_reconnects_without_compression() {
    let (fs, records) =
        compression_test_server(CompressionTestBehavior::RejectGzipGetSystemInfo).await;

    let mut connection = Connection::connect(compression_test_options(&fs, GrpcCompression::Gzip))
        .await
        .unwrap();
    assert!(connection.capabilities().unwrap().sdk_metadata);

    WorkflowService::signal_workflow_execution(
        &mut connection,
        SignalWorkflowExecutionRequest::default().into_request(),
    )
    .await
    .unwrap();

    let records = records.lock().unwrap().clone();
    assert_eq!(records.len(), 3);
    assert!(records[0].0.ends_with("/GetSystemInfo"));
    assert_eq!(records[0].1, "gzip");
    assert!(records[1].0.ends_with("/GetSystemInfo"));
    assert_eq!(records[1].1, "");
    assert!(records[2].0.ends_with("/SignalWorkflowExecution"));
    assert_eq!(records[2].1, "");

    fs.shutdown().await;
}

#[tokio::test]
async fn compression_none_does_not_retry_compression_fallback() {
    let (fs, records) =
        compression_test_server(CompressionTestBehavior::RejectGzipGetSystemInfo).await;

    let connection = Connection::connect(compression_test_options(&fs, GrpcCompression::None))
        .await
        .unwrap();
    assert!(connection.capabilities().unwrap().sdk_metadata);

    let records = records.lock().unwrap().clone();
    assert_eq!(records.len(), 1);
    assert!(records[0].0.ends_with("/GetSystemInfo"));
    assert_eq!(records[0].1, "");

    fs.shutdown().await;
}

#[tokio::test]
async fn generic_gzip_unimplemented_does_not_reconnect_without_compression() {
    let (fs, records) =
        compression_test_server(CompressionTestBehavior::GenericGzipUnimplemented).await;

    let err = match Connection::connect(compression_test_options(&fs, GrpcCompression::Gzip)).await
    {
        Ok(_) => panic!("connection should fail"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        ClientConnectError::SystemInfoCallError(status)
            if status.code() == Code::Unimplemented
                && status.message() == "gzip is unavailable for this backend"
    ));

    let records = records.lock().unwrap().clone();
    assert_eq!(records.len(), 1);
    assert!(records[0].0.ends_with("/GetSystemInfo"));
    assert_eq!(records[0].1, "gzip");

    fs.shutdown().await;
}

#[tokio::test]
async fn unknown_method_unimplemented_does_not_trigger_compression_reconnect() {
    let (fs, records) = compression_test_server(CompressionTestBehavior::UnknownMethod).await;

    let connection = Connection::connect(compression_test_options(&fs, GrpcCompression::Gzip))
        .await
        .unwrap();

    assert!(connection.capabilities().is_none());

    let records = records.lock().unwrap().clone();
    assert_eq!(records.len(), 1);
    assert!(records[0].0.ends_with("/GetSystemInfo"));
    assert_eq!(records[0].1, "gzip");

    fs.shutdown().await;
}

#[tokio::test]
async fn per_call_timeout_respected_whole_client() {
    let opts = get_integ_server_options();
    let mut connection = Connection::connect(opts).await.unwrap();
    let mut hm = HashMap::new();
    hm.insert("grpc-timeout".to_string(), "0S".to_string());
    connection.set_headers(hm).unwrap();
    let err = connection
        .describe_namespace(
            DescribeNamespaceRequest {
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap_err();
    assert_matches!(err.code(), Code::DeadlineExceeded | Code::Cancelled);
}

#[tokio::test]
async fn per_call_timeout_respected_one_call() {
    let opts = get_integ_server_options();
    let mut connection = Connection::connect(opts).await.unwrap();

    let mut req = Request::new(DescribeNamespaceRequest {
        namespace: NAMESPACE.to_string(),
        ..Default::default()
    });
    req.set_timeout(Duration::from_millis(0));
    let res = connection.describe_namespace(req).await;
    assert_matches!(
        res.unwrap_err().code(),
        Code::DeadlineExceeded | Code::Cancelled
    );
}

#[tokio::test]
async fn timeouts_respected_one_call_fake_server() {
    let mut fs = fake_server(|_| async { Response::new(Body::empty()) }.boxed()).await;
    let header_rx = &mut fs.header_rx;

    let mut opts = get_integ_server_options();
    opts.target = format!("http://localhost:{}", fs.addr.port())
        .parse::<url::Url>()
        .unwrap();
    opts.set_skip_get_system_info(true);
    opts.retry_options = RetryOptions::no_retries();

    let mut connection = Connection::connect(opts).await.unwrap();

    macro_rules! call_client {
        ($client:ident, $trx:ident, $client_fn:ident, $msg:expr) => {
            let mut req = Request::new($msg);
            req.set_timeout(Duration::from_millis(100));
            // Response is always error b/c empty body
            let _ = $client.$client_fn(req).await;
            let timeout = $trx.recv().await.unwrap();
            assert_eq!("100000u", timeout);
        };
    }

    call_client!(
        connection,
        header_rx,
        get_workflow_execution_history,
        Default::default()
    );
    call_client!(
        connection,
        header_rx,
        get_workflow_execution_history,
        GetWorkflowExecutionHistoryRequest {
            // Ensure these calls when done long-poll style still respect timeout
            wait_new_event: true,
            ..Default::default()
        }
    );
    call_client!(
        connection,
        header_rx,
        update_workflow_execution,
        Default::default()
    );
    call_client!(
        connection,
        header_rx,
        poll_workflow_execution_update,
        Default::default()
    );

    fs.shutdown().await;
}

#[tokio::test]
async fn non_retryable_errors() {
    for code in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::FailedPrecondition,
        Code::Cancelled,
        Code::DeadlineExceeded,
        Code::Unauthenticated,
        Code::Unimplemented,
    ] {
        let mut fs = fake_server(move |_| {
            let s = Status::new(code, "bla").into_http();
            async { s }.boxed()
        })
        .await;

        let mut opts = get_integ_server_options();
        opts.target = format!("http://localhost:{}", fs.addr.port())
            .parse::<url::Url>()
            .unwrap();
        opts.set_skip_get_system_info(true);
        let connection = Connection::connect(opts).await.unwrap();
        let client_opts = temporalio_client::ClientOptions::new("ns").build();
        let client = temporalio_client::Client::new(connection, client_opts).unwrap();

        let result = client.count_workflows("whatever", Default::default()).await;

        // Expecting an error after a single attempt, since there was a non-retryable error.
        assert!(result.is_err());
        let mut all_calls = vec![];
        fs.header_rx.recv_many(&mut all_calls, 9999).await;
        assert_eq!(all_calls.len(), 1);

        fs.shutdown().await;
    }
}

#[tokio::test]
async fn retryable_errors() {
    // Take out retry exhausted since it gets a special policy which would make this take ages
    for code in RETRYABLE_ERROR_CODES
        .iter()
        .copied()
        .filter(|p| p != &Code::ResourceExhausted)
    {
        let count = Arc::new(AtomicUsize::new(0));
        let mut fs = fake_server(move |_| {
            let prev = count.fetch_add(1, Ordering::Relaxed);
            let r = if prev < 3 {
                Status::new(code, "bla").into_http()
            } else {
                make_ok_response(RespondActivityTaskCanceledResponse::default())
            };
            async { r }.boxed()
        })
        .await;

        let mut opts = get_integ_server_options();
        opts.target = format!("http://localhost:{}", fs.addr.port())
            .parse::<url::Url>()
            .unwrap();
        opts.set_skip_get_system_info(true);
        let connection = Connection::connect(opts).await.unwrap();
        let client_opts = temporalio_client::ClientOptions::new("ns").build();
        let client = temporalio_client::Client::new(connection, client_opts).unwrap();

        let result = client.count_workflows("whatever", Default::default()).await;

        // Expecting successful response after retries
        assert!(result.is_ok(), "{:?}", result);
        let mut all_calls = vec![];
        fs.header_rx.recv_many(&mut all_calls, 9999).await;
        // Should be 4 attempts
        assert_eq!(all_calls.len(), 4);
        fs.shutdown().await;
    }
}

#[tokio::test]
async fn namespace_header_attached_to_relevant_calls() {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (header_tx, mut header_rx) = tokio::sync::mpsc::unbounded_channel();

    let listener = TcpListener::bind("[::]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(GenericService {
                header_to_parse: "Temporal-Namespace",
                header_tx,
                response_maker: |_| async { Response::new(Body::empty()) }.boxed(),
            })
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    shutdown_rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    let namespace = "namespace";
    let mut opts = get_integ_server_options();
    opts.target = format!("http://localhost:{}", addr.port())
        .parse::<url::Url>()
        .unwrap();
    opts.set_skip_get_system_info(true);
    opts.retry_options = RetryOptions::no_retries();
    let connection = Connection::connect(opts).await.unwrap();
    let client_opts = temporalio_client::ClientOptions::new(namespace).build();
    let client = temporalio_client::Client::new(connection, client_opts).unwrap();

    let _ = client
        .get_workflow_handle::<UntypedWorkflow>("hi")
        .fetch_history(Default::default())
        .await;
    let val = header_rx.recv().await.unwrap();
    assert_eq!(namespace, val);
    let _ = WorkflowService::list_namespaces(
        &mut client.clone(),
        ListNamespacesRequest::default().into_request(),
    )
    .await;
    let val = header_rx.recv().await.unwrap();
    // List namespaces is not namespace-specific
    assert_eq!("", val);
    let _ = WorkflowService::describe_namespace(
        &mut client.clone(),
        Namespace::Name("Other".to_string())
            .into_describe_namespace_request()
            .into_request(),
    )
    .await;
    let val = header_rx.recv().await.unwrap();
    assert_eq!("Other", val);

    // Shutdown the server
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn grpc_compression() {
    crate::shared_tests::grpc_compression().await
}

#[tokio::test]
async fn cloud_ops_test() {
    let api_key = match env::var("TEMPORAL_CLIENT_CLOUD_API_KEY") {
        Ok(k) => k,
        Err(_) => {
            // skip test
            info!("Skipped cloud operations client test");
            return;
        }
    };
    let api_version =
        env::var("TEMPORAL_CLIENT_CLOUD_API_VERSION").expect("version env var must exist");
    let namespace =
        env::var("TEMPORAL_CLIENT_CLOUD_NAMESPACE").expect("namespace env var must exist");
    let mut opts = get_integ_server_options();
    opts.target = "https://saas-api.tmprl.cloud:443"
        .parse::<url::Url>()
        .unwrap();
    opts.api_key = Some(api_key);
    opts.headers = Some({
        let mut hm = HashMap::new();
        hm.insert("temporal-cloud-api-version".to_string(), api_version);
        hm
    });
    let connection = Connection::connect(opts).await.unwrap();
    let mut cloud_client = connection.cloud_service();
    let res = cloud_client
        .get_namespace(
            GetNamespaceRequest {
                namespace: namespace.clone(),
            }
            .into_request(),
        )
        .await
        .unwrap();
    assert_eq!(res.into_inner().namespace.unwrap().namespace, namespace);
}

#[tokio::test]
async fn http_proxy() {
    // Create server
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_cloned = call_count.clone();
    let server = fake_server(move |_| {
        call_count_cloned.fetch_add(1, Ordering::SeqCst);
        async { Response::new(Body::empty()) }.boxed()
    })
    .await;

    // Create HTTP TCP proxy
    let tcp_proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_proxy_addr = tcp_proxy_listener.local_addr().unwrap();
    let tcp_proxy = HttpProxy::spawn_tcp(tcp_proxy_listener);

    // General client options
    let mut opts = get_integ_server_options();
    opts.retry_options = RetryOptions::no_retries();
    opts.set_skip_get_system_info(true);

    // Connect client with no proxy and make call and confirm reached
    opts.target = format!("http://[::1]:{}", server.addr.port())
        .parse()
        .unwrap();
    let connection = Connection::connect(opts.clone()).await.unwrap();
    let client_opts = temporalio_client::ClientOptions::new("my-namespace").build();
    let client = temporalio_client::Client::new(connection, client_opts).unwrap();
    let _ = WorkflowService::list_namespaces(
        &mut client.clone(),
        ListNamespacesRequest::default().into_request(),
    )
    .await;
    assert!(call_count.load(Ordering::SeqCst) == 1);
    assert!(tcp_proxy.hit_count() == 0);

    // Connect client to proxy and make call and confirm reached
    opts.http_connect_proxy = Some(HttpConnectProxyOptions {
        target_addr: tcp_proxy_addr.to_string(),
        basic_auth: None,
    });
    opts.dns_load_balancing = None;
    let connection = Connection::connect(opts.clone()).await.unwrap();
    let client_opts = temporalio_client::ClientOptions::new("my-namespace").build();
    let proxied_client = temporalio_client::Client::new(connection, client_opts).unwrap();
    let _ = WorkflowService::list_namespaces(
        &mut proxied_client.clone(),
        ListNamespacesRequest::default().into_request(),
    )
    .await;
    assert!(call_count.load(Ordering::SeqCst) == 2);
    assert!(tcp_proxy.hit_count() == 1);

    // Test Unix socket too only in Unix environments
    #[cfg(unix)]
    {
        // Create temp socket path
        let mut sock_path = std::env::temp_dir();
        sock_path.push(format!("http-proxy-test-{}.sock", std::process::id()));
        // Remove if there just in case
        let _ = std::fs::remove_file(&sock_path);

        // Create unix-socket-based proxy
        let unix_proxy = HttpProxy::spawn_unix(UnixListener::bind(&sock_path).unwrap());

        // Connect client to proxy and make call and confirm reached
        opts.http_connect_proxy = Some(HttpConnectProxyOptions {
            target_addr: format!("unix:{}", sock_path.to_str().unwrap()),
            basic_auth: None,
        });
        opts.dns_load_balancing = None;
        let connection = Connection::connect(opts.clone()).await.unwrap();
        let client_opts = temporalio_client::ClientOptions::new("my-namespace").build();
        let proxied_client = temporalio_client::Client::new(connection, client_opts).unwrap();
        let _ = WorkflowService::list_namespaces(
            &mut proxied_client.clone(),
            ListNamespacesRequest::default().into_request(),
        )
        .await;
        assert!(call_count.load(Ordering::SeqCst) == 3);
        assert!(unix_proxy.hit_count() == 1);

        // Shutdown unix proxy
        unix_proxy.shutdown();
    }

    // Shutdown server and proxy
    server.shutdown().await;
    tcp_proxy.shutdown();
}

#[tokio::test]
async fn update_get_result_retries_on_empty_outcome() {
    use temporalio_common::protos::temporal::api::{
        common::v1::{Payloads, WorkflowExecution as ProtoWorkflowExecution},
        update::v1::{self, Outcome, UpdateRef},
        workflowservice::v1::{
            PollWorkflowExecutionUpdateResponse, UpdateWorkflowExecutionResponse,
        },
    };

    let poll_count = Arc::new(AtomicUsize::new(0));
    let poll_count_clone = poll_count.clone();

    let fs = fake_server(move |req| {
        let poll_count = poll_count_clone.clone();
        async move {
            let path = req.uri().path();
            if path.contains("UpdateWorkflowExecution") {
                // start_update: return a handle reference with no outcome
                make_ok_response(UpdateWorkflowExecutionResponse {
                    update_ref: Some(UpdateRef {
                        workflow_execution: Some(ProtoWorkflowExecution {
                            workflow_id: "wf-id".into(),
                            run_id: "run-id".into(),
                        }),
                        update_id: "update-id".into(),
                    }),
                    outcome: None,
                    ..Default::default()
                })
            } else if path.contains("PollWorkflowExecutionUpdate") {
                let n = poll_count.fetch_add(1, Ordering::SeqCst);
                let response = if n == 0 {
                    // First poll: simulate server long-poll timeout (no outcome)
                    PollWorkflowExecutionUpdateResponse::default()
                } else {
                    // Second poll: return a successful outcome
                    PollWorkflowExecutionUpdateResponse {
                        outcome: Some(Outcome {
                            value: Some(v1::outcome::Value::Success(Payloads { payloads: vec![] })),
                        }),
                        ..Default::default()
                    }
                };
                make_ok_response(response)
            } else {
                Response::new(Body::empty())
            }
        }
        .boxed()
    })
    .await;

    let mut opts = get_integ_server_options();
    opts.target = format!("http://localhost:{}", fs.addr.port())
        .parse::<url::Url>()
        .unwrap();
    opts.set_skip_get_system_info(true);
    opts.retry_options = RetryOptions::no_retries();
    let connection = Connection::connect(opts).await.unwrap();
    let client_opts = temporalio_client::ClientOptions::new("default").build();
    let client = temporalio_client::Client::new(connection, client_opts).unwrap();

    let wf_handle = client.get_workflow_handle::<UntypedWorkflow>("wf-id");
    let result = wf_handle
        .execute_update(
            temporalio_client::UntypedUpdate::new("my-update"),
            temporalio_common::data_converters::RawValue::default(),
            Default::default(),
        )
        .await;
    assert!(
        result.is_ok(),
        "execute_update should retry polling and succeed, got: {result:?}"
    );
    assert_eq!(
        poll_count.load(Ordering::SeqCst),
        2,
        "should have polled twice"
    );

    fs.shutdown().await;
}

fn make_ok_response<T>(message: T) -> Response<Body>
where
    T: Message,
{
    // Encode the message into a byte buffer.
    let mut buf = Vec::new();
    message
        .encode(&mut buf)
        .expect("failed to encode response message");

    let mut frame = Vec::with_capacity(5 + buf.len());
    frame.push(0);
    let len = buf.len() as u32;
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&buf);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let stream = stream::iter(vec![
        Ok::<_, Status>(Frame::data(Bytes::from(frame))),
        Ok::<_, Status>(Frame::trailers(trailers)),
    ]);
    let body = Body::new(BoxBody::new(StreamBody::new(stream)).boxed());

    // Build the HTTP response with the required gRPC headers.
    Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(body)
        .expect("failed to build response")
}
