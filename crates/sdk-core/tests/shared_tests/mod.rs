//! Shared tests that are meant to be run against both local dev server and cloud

use crate::common::{
    CoreWfStarter, NAMESPACE, activity_functions::StdActivities, fake_grpc_server::GenericService,
    get_cloud_or_local_client,
};
use futures_util::FutureExt;
use http_body_util::BodyExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, GrpcCompression, NamespacedClient,
    RetryOptions, WorkflowFetchHistoryOptions, WorkflowSignalOptions, WorkflowStartOptions,
    WorkflowTerminateOptions, grpc::WorkflowService,
};
use temporalio_common::{
    ActivityError, UntypedWorkflow,
    protos::{
        coresdk::workflow_commands::ActivityCancellationType,
        temporal::api::{
            common::v1::RetryPolicy,
            enums::v1::{
                EventType,
                WorkflowTaskFailedCause::{self, GrpcMessageTooLarge},
            },
            history::v1::history_event::{
                self,
                Attributes::{
                    WorkflowExecutionTerminatedEventAttributes, WorkflowTaskFailedEventAttributes,
                },
            },
            workflowservice::v1::{DescribeNamespaceRequest, ListWorkflowExecutionsRequest},
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, SyncWorkflowContext, WorkflowContext, WorkflowResult,
    WorkflowTermination, activities::ActivityContext,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tonic::{
    IntoRequest,
    body::Body,
    codegen::http::{Request, Response},
    transport::Server,
};
use tracing::warn;

pub(crate) mod priority;

/// Verifies transport-level gRPC compression end-to-end, with and without it enabled.
///
/// Part 1 runs against the real server (cloud or local dev server) and confirms both settings work
/// *and* that the server actually engages compression rather than silently ignoring it: the
/// Temporal frontend only gzip-compresses its response when the client advertises
/// `grpc-accept-encoding: gzip` (which our toggle controls), so `grpc-encoding: gzip` on the
/// response iff enabled proves the negotiation is live.
///
/// Part 2 proves the *outbound request* bytes on the wire are genuinely gzip-compressed. We cannot
/// inspect the bytes of the real (TLS) connection, so this routes through an in-process tonic
/// server that lets us read the raw gRPC frame and confirm the compression flag, gzip magic bytes,
/// size reduction, and that the payload decompresses to the exact same protobuf as the
/// uncompressed request.
pub(crate) async fn grpc_compression() {
    // Part 1: real-server negotiation.
    for compression in [GrpcCompression::None, GrpcCompression::Gzip] {
        let mut client = get_cloud_or_local_client(compression).await;
        let namespace = client.namespace();
        let resp = client
            .list_workflow_executions(
                ListWorkflowExecutionsRequest {
                    namespace,
                    page_size: 1,
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap_or_else(|e| {
                panic!("list_workflow_executions failed with {compression:?}: {e}")
            });
        let server_encoding = resp
            .metadata()
            .get("grpc-encoding")
            .map(|v| v.to_str().unwrap().to_owned());
        match compression {
            GrpcCompression::Gzip => assert_eq!(
                server_encoding.as_deref(),
                Some("gzip"),
                "server must gzip-compress the response when compression is enabled, proving \
                 compression was actually negotiated and not ignored"
            ),
            GrpcCompression::None => assert_eq!(
                server_encoding, None,
                "server must not compress the response when compression is disabled"
            ),
            _ => unreachable!("only None and Gzip are iterated"),
        }
    }

    // Part 2: wire-level proof the request body is really gzip.
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (header_tx, mut header_rx) = mpsc::unbounded_channel::<String>();
    let (body_tx, mut body_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(GenericService {
                header_to_parse: "grpc-encoding",
                header_tx,
                response_maker: move |req: Request<Body>| {
                    let body_tx = body_tx.clone();
                    async move {
                        let body = req.into_body().collect().await.unwrap().to_bytes().to_vec();
                        let _ = body_tx.send(body);
                        Response::new(Body::empty())
                    }
                    .boxed()
                },
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

    // A sizable, highly compressible field so compression is unambiguous and clearly shrinks the
    // payload.
    let big_namespace = "compressible-namespace-".repeat(512);

    let mut frames = Vec::new();
    for (compression, expected_encoding) in
        [(GrpcCompression::None, ""), (GrpcCompression::Gzip, "gzip")]
    {
        let mut opts =
            ConnectionOptions::new(format!("http://{addr}").parse::<url::Url>().unwrap()).build();
        opts.set_skip_get_system_info(true);
        opts.retry_options = RetryOptions::no_retries();
        opts.grpc_compression = compression;
        let connection = Connection::connect(opts).await.unwrap();
        let mut client = Client::new(connection, ClientOptions::new(NAMESPACE).build()).unwrap();

        let _ = client
            .describe_namespace(
                DescribeNamespaceRequest {
                    namespace: big_namespace.clone(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await;

        assert_eq!(
            header_rx.recv().await.unwrap(),
            expected_encoding,
            "unexpected grpc-encoding header for {compression:?}"
        );
        frames.push(body_rx.recv().await.unwrap());
    }

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();

    // gRPC message frame: [compressed-flag: u8][length: u32 BE][message].
    let none_frame = &frames[0];
    let gzip_frame = &frames[1];
    assert_eq!(
        none_frame[0], 0,
        "uncompressed request frame must have compression flag 0"
    );
    assert_eq!(
        gzip_frame[0], 1,
        "gzip request frame must have compression flag 1"
    );

    let none_msg = &none_frame[5..];
    let gzip_msg = &gzip_frame[5..];
    assert_eq!(
        &gzip_msg[..2],
        &[0x1f, 0x8b],
        "compressed payload must begin with gzip magic bytes"
    );
    assert!(
        gzip_msg.len() < none_msg.len(),
        "gzip payload ({} bytes) should be smaller than uncompressed ({} bytes)",
        gzip_msg.len(),
        none_msg.len()
    );

    let mut decompressed = Vec::new();
    std::io::Read::read_to_end(
        &mut flate2::read::GzDecoder::new(gzip_msg),
        &mut decompressed,
    )
    .unwrap();
    assert_eq!(
        decompressed, none_msg,
        "gzip payload must decompress to the exact protobuf bytes of the uncompressed request"
    );
}

#[workflow]
struct OversizeGrpcMessageWf {
    run_flag: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl OversizeGrpcMessageWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<Vec<u8>> {
        if ctx.state(|wf| wf.run_flag.load(Relaxed)) {
            Ok(vec![])
        } else {
            ctx.state(|wf| wf.run_flag.store(true, Relaxed));
            let result: Vec<u8> = vec![0; 5000000];
            Ok(result)
        }
    }
}

pub(crate) async fn grpc_message_too_large() {
    let run_flag = Arc::new(AtomicBool::new(false));
    let run_flag_clone = run_flag.clone();

    let wf_name = "oversize_grpc_message";
    let mut starter = CoreWfStarter::new_cloud_or_local(wf_name, "")
        .await
        .unwrap();
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter
        .sdk_config
        .register_workflow_with_factory(move || OversizeGrpcMessageWf {
            run_flag: run_flag_clone.clone(),
        })
        .unwrap();

    let mut sdk = starter.worker().await;
    sdk.submit_workflow(
        OversizeGrpcMessageWf::run,
        (),
        starter.workflow_options.clone(),
    )
    .await
    .unwrap();
    sdk.run_until_done().await.unwrap();

    let events = starter.get_history().await.events;
    // Depending on the version of server, it may terminate the workflow, or simply be a task
    // failure
    assert!(
        events.iter().any(is_oversize_grpc_event),
        "Expected workflow task failure or termination b/c grpc message too large: {events:?}",
    );
}

pub(crate) fn is_oversize_grpc_event(
    e: &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
) -> bool {
    // Task failure
    e.event_type == EventType::WorkflowTaskFailed as i32
        && if let WorkflowTaskFailedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
            attr.cause == GrpcMessageTooLarge as i32
                && attr.failure.as_ref().unwrap().message == "GRPC Message too large"
        } else {
            false
        }
    // Workflow terminated
    ||
    e.event_type == EventType::WorkflowExecutionTerminated as i32
        && if let WorkflowExecutionTerminatedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
            attr.reason == "GrpcMessageTooLarge"
        } else {
            false
        }
}

#[workflow]
#[derive(Default)]
struct ShutdownTimerActivityLoopWf;

#[workflow_methods]
impl ShutdownTimerActivityLoopWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        loop {
            ctx.timer(Duration::from_millis(10)).await;
            ctx.start_activity(
                StdActivities::no_op,
                (),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await
            .map_err(|e| WorkflowTermination::from(anyhow::Error::from(e)))?;
        }
    }
}

/// Starts 10 workflows that each run a tight timer+activity loop, then shuts down the worker
/// and verifies:
///   1. Shutdown completes rapidly (< 5s)
///   2. No workflow task failures or timeouts appear in any workflow's history
pub(crate) async fn shutdown_during_active_timer_activity_workflows() {
    let wf_name = "shutdown_during_active_timer_activity_workflows";
    let num_workflows = 10;

    let mut starter =
        if let Some(wfs) = CoreWfStarter::new_cloud_or_local(wf_name, ">=1.6.3-serverless").await {
            wfs
        } else {
            return;
        };
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<ShutdownTimerActivityLoopWf>()
        .unwrap();

    let core = worker.core_worker();
    core.validate().await.unwrap();
    assert!(
        core.get_namespace_capabilities().graceful_poll_shutdown(),
        "Server must support graceful poll shutdown for this test"
    );

    let task_queue = starter.get_task_queue().to_owned();
    let mut wf_ids = Vec::with_capacity(num_workflows);
    for i in 0..num_workflows {
        let wf_id = format!("{task_queue}-{i}");
        worker
            .submit_workflow(
                ShutdownTimerActivityLoopWf::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), wf_id.clone()).build(),
            )
            .await
            .unwrap();
        wf_ids.push(wf_id);
    }
    // Don't wait for workflow completion — these loop forever
    worker.fetch_results = false;

    let shutdown_handle = worker.inner_mut().shutdown_handle();
    let run_fut = async { worker.run_until_done().await.unwrap() };

    let shutdown_fut = async {
        // Let workflows run a few iterations
        tokio::time::sleep(Duration::from_secs(2)).await;
        shutdown_handle();
    };

    let shutdown_start = std::time::Instant::now();
    tokio::join!(run_fut, shutdown_fut);
    let shutdown_elapsed = shutdown_start.elapsed();

    assert!(
        shutdown_elapsed < Duration::from_secs(5),
        "Worker shutdown took {shutdown_elapsed:?}, expected < 5s"
    );

    let client = starter.get_client().await;
    for wf_id in &wf_ids {
        client
            .get_workflow_handle::<UntypedWorkflow>(wf_id)
            .terminate(WorkflowTerminateOptions::default())
            .await
            .unwrap();

        let history = client
            .get_workflow_handle::<UntypedWorkflow>(wf_id)
            .fetch_history(WorkflowFetchHistoryOptions::default())
            .await
            .unwrap();
        let bad_events: Vec<_> = history
            .events()
            .iter()
            .filter(|e| match &e.attributes {
                Some(history_event::Attributes::WorkflowTaskFailedEventAttributes(f))
                    if f.cause() != WorkflowTaskFailedCause::ForceCloseCommand =>
                {
                    true
                }
                Some(history_event::Attributes::WorkflowTaskTimedOutEventAttributes(_)) => true,
                _ => false,
            })
            .collect();
        assert!(
            bad_events.is_empty(),
            "Workflow {wf_id} had unexpected WFT failures/timeouts: {bad_events:?}"
        );
    }
}

/// Verifies that activity cancellation is delivered via the nexus worker command channel
/// even when the activity does not heartbeat.
pub(crate) async fn activity_cancel_delivered_without_heartbeat() {
    let wf_name = "activity_cancel_delivered_without_heartbeat";
    let mut starter = CoreWfStarter::new_cloud_or_local(wf_name, "")
        .await
        .unwrap();

    struct WaitForCancelActivities;
    #[activities]
    impl WaitForCancelActivities {
        #[activity]
        async fn wait_for_cancel(
            self: Arc<Self>,
            ctx: ActivityContext,
            _: String,
        ) -> Result<String, ActivityError> {
            ctx.workflow_handle::<CancelWithoutHeartbeatWorkflow>()
                .unwrap()
                .signal(
                    CancelWithoutHeartbeatWorkflow::act_started,
                    (),
                    WorkflowSignalOptions::default(),
                )
                .await
                .unwrap();
            ctx.cancelled().await;
            Ok("done".to_string())
        }
    }

    starter
        .sdk_config
        .register_activities(WaitForCancelActivities);
    let mut worker = starter.worker().await;
    if !worker
        .core_worker()
        .get_namespace_capabilities()
        .worker_commands()
    {
        warn!("Skipping test: worker_commands not supported in this namespace");
        return;
    }

    #[workflow]
    #[derive(Default)]
    struct CancelWithoutHeartbeatWorkflow {
        act_started: bool,
    }

    #[workflow_methods]
    impl CancelWithoutHeartbeatWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let act_fut = ctx.start_activity(
                WaitForCancelActivities::wait_for_cancel,
                "hi".to_string(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                    .retry_policy(RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    })
                    .cancellation_type(ActivityCancellationType::WaitCancellationCompleted)
                    // TODO: enable eager dispatch once server supports it for worker commands.
                    .do_not_eagerly_execute(true)
                    .build(),
            );
            // ensure the activity is started on a worker before cancelling, so the cancel goes
            // through the worker commands path.
            ctx.wait_condition(|s| s.act_started).await;
            act_fut.cancel();
            act_fut
                .await
                .map_err(|e| WorkflowTermination::from(anyhow::Error::from(e)))?;
            Ok(())
        }

        #[signal]
        fn act_started(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
            self.act_started = true;
        }
    }

    worker
        .register_workflow::<CancelWithoutHeartbeatWorkflow>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            CancelWithoutHeartbeatWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned())
                .run_timeout(Duration::from_secs(10))
                .build(),
        )
        .await
        .unwrap();
    // Fails with workflow timeout if cancel via worker commands doesn't work
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}
