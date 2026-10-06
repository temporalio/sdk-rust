use crate::common::{get_integ_server_options, integ_namespace};
use lambda_runtime::{Context, LambdaEvent};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_proto::tonic::{
    collector::{
        metrics::v1::{
            ExportMetricsServiceRequest, ExportMetricsServiceResponse,
            metrics_service_server::{MetricsService, MetricsServiceServer},
        },
        trace::v1::{
            ExportTraceServiceRequest, ExportTraceServiceResponse,
            trace_service_server::{TraceService, TraceServiceServer},
        },
    },
    trace::v1::Span,
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SimpleSpanProcessor};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_client::{Client, ClientOptions, WorkflowStartOptions};
use temporalio_common::worker::WorkerDeploymentVersion;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, VersioningOverride, WorkerOptions, WorkflowContext, WorkflowContextView,
    WorkflowResult,
    activities::{ActivityContext, ActivityError},
    opentelemetry::{OpenTelemetryPlugin, WorkflowIdGenerator, WorkflowSpanProcessor},
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};
use temporalio_sdk_aws_lambda::{LambdaWorker, LambdaWorkerDefaults, otel::OpenTelemetryOptions};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Default)]
struct Collector {
    spans: Arc<Mutex<Vec<Span>>>,
    metrics: Arc<AtomicUsize>,
    unavailable: Arc<AtomicBool>,
}

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        if self.unavailable.load(Ordering::Relaxed) {
            return Err(Status::unavailable("collector unavailable"));
        }
        self.spans.lock().unwrap().extend(
            request
                .into_inner()
                .resource_spans
                .into_iter()
                .flat_map(|resource| resource.scope_spans)
                .flat_map(|scope| scope.spans),
        );
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for Collector {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        if self.unavailable.load(Ordering::Relaxed) {
            return Err(Status::unavailable("collector unavailable"));
        }
        self.metrics.fetch_add(
            request.into_inner().resource_metrics.len(),
            Ordering::Relaxed,
        );
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[derive(Default)]
struct LambdaActivities {
    cancellation_complete: Arc<AtomicBool>,
}

#[activities]
impl LambdaActivities {
    #[activity]
    async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }

    #[activity]
    async fn wait_for_shutdown(
        self: Arc<Self>,
        ctx: ActivityContext,
        _: (),
    ) -> Result<(), ActivityError> {
        ctx.cancelled().await;
        self.cancellation_complete.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct LambdaWorkflow;

#[workflow_methods]
impl LambdaWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView) -> Self {
        Self
    }

    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        Ok(ctx
            .execute_activity(
                LambdaActivities::greet,
                name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?)
    }
}

fn event(request_id: &str) -> LambdaEvent<()> {
    let mut context = Context::default();
    context.request_id = request_id.to_owned();
    context.invoked_function_arn =
        "arn:aws:lambda:us-east-1:123456789012:function:worker".to_owned();
    context.deadline = (SystemTime::now() + Duration::from_secs(6))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    LambdaEvent::new((), context)
}

#[tokio::test]
async fn lambda_warm_invocations_flush_propagate_and_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let collector = Collector::default();
    let (stop_tx, stop_rx) = oneshot::channel();
    let collector_task = tokio::spawn(
        Server::builder()
            .add_service(TraceServiceServer::new(collector.clone()))
            .add_service(MetricsServiceServer::new(collector.clone()))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stop_rx.await;
            }),
    );
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_id_generator(WorkflowIdGenerator::default())
        .with_span_processor(WorkflowSpanProcessor::new(SimpleSpanProcessor::new(
            exporter.clone(),
        )))
        .build();
    let plugin = OpenTelemetryPlugin::builder()
        .tracer(provider.tracer("lambda-test-client"))
        .build();
    let client = Client::connect(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace())
            .plugin(plugin.clone())
            .build(),
    )
    .await
    .unwrap();
    let queue = format!("lambda-{}", Uuid::new_v4());
    let version = WorkerDeploymentVersion::builder()
        .deployment_name(queue.clone())
        .build_id("test")
        .build();
    let hooks = Arc::new(AtomicUsize::new(0));
    let hook_calls = hooks.clone();
    let mut telemetry = OpenTelemetryOptions::default();
    telemetry.endpoint = Some(endpoint);
    telemetry.metric_export_interval = Duration::from_secs(3600);
    telemetry.export_timeout = Duration::from_millis(200);
    let mut defaults = LambdaWorkerDefaults::default();
    defaults.graceful_shutdown_period = Duration::from_millis(100);
    let worker = LambdaWorker::builder(
        version.clone(),
        WorkerOptions::new(queue.clone())
            .register_workflow::<LambdaWorkflow>()
            .unwrap()
            .register_activities(LambdaActivities::default())
            .build(),
    )
    .client_options(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace()).build(),
    )
    .lambda_defaults(defaults)
    .open_telemetry(telemetry)
    .shutdown_hook(move |_| {
        hook_calls.fetch_add(1, Ordering::Relaxed);
        async { Ok(()) }
    })
    .build()
    .unwrap();

    worker.handle(event("register-version")).await.unwrap();
    for invocation in 0..4 {
        collector
            .unavailable
            .store(invocation == 2, Ordering::Relaxed);
        let previous_metrics = collector.metrics.load(Ordering::Relaxed);
        let handle = client
            .start_workflow(
                LambdaWorkflow::run,
                "Temporal".to_owned(),
                WorkflowStartOptions::new(queue.clone(), format!("lambda-{}", Uuid::new_v4()))
                    .versioning_override(VersioningOverride::Pinned(version.clone()))
                    .build(),
            )
            .await
            .unwrap();
        let (invocation_result, workflow_result) = tokio::join!(
            worker.handle(event(&format!("request-{invocation}"))),
            handle.get_result(Default::default()),
        );
        invocation_result.unwrap();
        assert_eq!(workflow_result.unwrap(), "Hello, Temporal!");
        assert_eq!(hooks.load(Ordering::Relaxed), invocation + 2);
        if invocation == 2 {
            continue;
        }
        assert!(collector.metrics.load(Ordering::Relaxed) > previous_metrics);
        let start = exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .rev()
            .find(|span| span.name.starts_with("StartWorkflow:"))
            .unwrap();
        let spans = collector.spans.lock().unwrap().clone();
        let run = spans
            .iter()
            .find(|span| {
                span.name.starts_with("RunWorkflow:")
                    && span.parent_span_id == start.span_context.span_id().to_bytes()
            })
            .unwrap();
        assert_eq!(run.trace_id, start.span_context.trace_id().to_bytes());
        let activity_start = spans
            .iter()
            .find(|span| {
                span.name.starts_with("StartActivity:") && span.parent_span_id == run.span_id
            })
            .unwrap();
        assert!(
            spans
                .iter()
                .any(|span| span.name.starts_with("RunActivity:")
                    && span.parent_span_id == activity_start.span_id
                    && span.trace_id == run.trace_id)
        );
        let before_replay = exporter.get_finished_spans().unwrap().len();
        let replayer = WorkflowReplayer::new(
            WorkflowReplayerOptions::new()
                .worker_plugin(plugin.clone())
                .register_workflow::<LambdaWorkflow>()
                .unwrap()
                .build(),
        )
        .unwrap();
        replayer
            .replay_workflow(handle.fetch_history(Default::default()))
            .await
            .unwrap();
        assert_eq!(exporter.get_finished_spans().unwrap().len(), before_replay);
    }
    stop_tx.send(()).unwrap();
    collector_task.await.unwrap().unwrap();
}

#[workflow]
struct LambdaCancellationWorkflow;

#[workflow_methods]
impl LambdaCancellationWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView) -> Self {
        Self
    }

    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, _: ()) -> WorkflowResult<()> {
        ctx.execute_activity(
            LambdaActivities::wait_for_shutdown,
            (),
            ActivityOptions::start_to_close_timeout(Duration::from_secs(60)),
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn lambda_drains_cancelled_activities_before_hooks() {
    let client = Client::connect(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace()).build(),
    )
    .await
    .unwrap();
    let queue = format!("lambda-cancellation-{}", Uuid::new_v4());
    let version = WorkerDeploymentVersion::builder()
        .deployment_name(queue.clone())
        .build_id("test")
        .build();
    let cancellation_complete = Arc::new(AtomicBool::new(false));
    let observed_in_hook = Arc::new(AtomicBool::new(false));
    let finished = cancellation_complete.clone();
    let observed = observed_in_hook.clone();
    let mut defaults = LambdaWorkerDefaults::default();
    defaults.graceful_shutdown_period = Duration::from_millis(100);
    let worker = LambdaWorker::builder(
        version.clone(),
        WorkerOptions::new(queue.clone())
            .register_workflow::<LambdaCancellationWorkflow>()
            .unwrap()
            .register_activities(LambdaActivities {
                cancellation_complete: cancellation_complete.clone(),
            })
            .build(),
    )
    .client_options(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace()).build(),
    )
    .lambda_defaults(defaults)
    .shutdown_hook(move |_| {
        observed.store(finished.load(Ordering::Relaxed), Ordering::Relaxed);
        async { Ok(()) }
    })
    .build()
    .unwrap();
    worker.handle(event("register-version")).await.unwrap();
    let _handle = client
        .start_workflow(
            LambdaCancellationWorkflow::run,
            (),
            WorkflowStartOptions::new(queue, format!("lambda-cancellation-{}", Uuid::new_v4()))
                .versioning_override(VersioningOverride::Pinned(version))
                .build(),
        )
        .await
        .unwrap();
    worker.handle(event("cancel-activity")).await.unwrap();
    assert!(cancellation_complete.load(Ordering::Relaxed));
    assert!(observed_in_hook.load(Ordering::Relaxed));
}
