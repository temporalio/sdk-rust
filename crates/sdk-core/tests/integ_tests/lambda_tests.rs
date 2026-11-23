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
use temporalio_client::{
    Client, ClientOptions, NamespacedClient, PluginError, WorkflowStartOptions,
    grpc::WorkflowService, worker::ClientWorkerSet,
};
use temporalio_common::{
    protos::temporal::api::{
        enums::v1::VersioningBehavior as ProtoVersioningBehavior,
        history::v1::history_event::Attributes,
        workflowservice::v1::{
            DescribeWorkerDeploymentRequest, SetWorkerDeploymentCurrentVersionRequest,
        },
    },
    worker::{VersioningBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion},
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ClientAndWorkerPlugin, VersioningOverride, Worker, WorkerOptions,
    WorkerPlugin, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::WorkerInterceptor,
    opentelemetry::{OpenTelemetryPlugin, WorkflowIdGenerator, WorkflowSpanProcessor},
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};
use temporalio_sdk_aws_lambda::{LambdaWorker, LambdaWorkerOptions, otel::OpenTelemetryOptions};
use tokio::{
    net::TcpListener,
    sync::{Notify, oneshot},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{IntoRequest, Request, Response, Status, transport::Server};
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
    cancelled: Arc<AtomicBool>,
    completion: Arc<Notify>,
    gate: Option<Arc<Notify>>,
    registry: Arc<Mutex<Option<Arc<ClientWorkerSet>>>>,
}

#[activities]
impl LambdaActivities {
    #[activity]
    async fn greet(
        self: Arc<Self>,
        ctx: ActivityContext,
        name: String,
    ) -> Result<String, ActivityError> {
        *self.registry.lock().unwrap() = Some(ctx.client().connection().workers());
        Ok(format!("Hello, {name}!"))
    }

    #[activity]
    async fn wait_for_shutdown(
        self: Arc<Self>,
        ctx: ActivityContext,
        _: (),
    ) -> Result<(), ActivityError> {
        *self.registry.lock().unwrap() = Some(ctx.client().connection().workers());
        ctx.cancelled().await;
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        self.cancelled.store(true, Ordering::Relaxed);
        self.completion.notify_one();
        Ok(())
    }
}

struct ShutdownObserver(Arc<Mutex<Option<Uuid>>>);

impl WorkerInterceptor for ShutdownObserver {
    fn on_shutdown(&self, worker: &Worker) {
        *self.0.lock().unwrap() = Some(worker.worker_instance_key());
    }
}

#[workflow]
#[derive(Default)]
struct LambdaWorkflow;

#[workflow_methods]
impl LambdaWorkflow {
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

#[derive(Clone)]
struct ConflictingPlugin(Arc<AtomicUsize>);

impl temporalio_client::ClientPlugin for ConflictingPlugin {
    fn name(&self) -> &str {
        "lambda-conflict"
    }
}

impl WorkerPlugin for ConflictingPlugin {
    fn name(&self) -> &str {
        "lambda-conflict"
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        options.deployment_options = WorkerDeploymentOptions::new(
            WorkerDeploymentVersion::builder()
                .deployment_name("wrong")
                .build_id("wrong")
                .build(),
        )
        .build();
        options.graceful_shutdown_period = Some(Duration::from_secs(60));
        options.max_eager_activity_reservations_per_workflow_task = 3;
        Ok(())
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
async fn lambda_validation_failure_runs_hooks_and_prevents_reuse() {
    let queue = format!("lambda-validation-{}", Uuid::new_v4());
    let called = Arc::new(AtomicBool::new(false));
    let hook_called = called.clone();
    let mut limits = LambdaWorkerOptions::default();
    limits.graceful_shutdown_period = Duration::from_millis(100);
    let worker = LambdaWorker::builder(
        WorkerDeploymentVersion::builder()
            .deployment_name(queue.clone())
            .build_id("test")
            .build(),
        WorkerOptions::new(queue)
            .register_workflow::<LambdaWorkflow>()
            .unwrap()
            .build(),
    )
    .client_options(
        get_integ_server_options(),
        ClientOptions::new(format!("missing-{}", Uuid::new_v4())).build(),
    )
    .lambda_options(limits)
    .shutdown_hook(move |_| {
        hook_called.store(true, Ordering::Relaxed);
        async { Ok(()) }
    })
    .build()
    .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(6),
        worker.handle(event("invalid-namespace")),
    )
    .await
    .expect("cleanup exceeded the invocation deadline")
    .unwrap_err();
    assert!(error.requires_restart());
    assert!(called.load(Ordering::Relaxed));
    assert!(
        worker
            .handle(event("after-failure"))
            .await
            .unwrap_err()
            .requires_restart()
    );
}

#[rstest::rstest]
#[case::pinned_default(false)]
#[case::auto_upgrade_override(true)]
#[tokio::test]
async fn lambda_warm_invocations_finalize_flush_propagate_and_replay(#[case] auto_upgrade: bool) {
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
    let registry = Arc::new(Mutex::new(None::<Arc<ClientWorkerSet>>));
    let hook_registry = registry.clone();
    let key = Arc::new(Mutex::new(None::<Uuid>));
    let hook_key = key.clone();
    let mut telemetry = OpenTelemetryOptions::default();
    telemetry.endpoint = Some(endpoint);
    telemetry.metric_export_interval = Duration::from_secs(3600);
    telemetry.export_timeout = Duration::from_millis(200);
    let mut limits = LambdaWorkerOptions::default();
    limits.graceful_shutdown_period = Duration::from_millis(100);
    let conflicts = Arc::new(AtomicUsize::new(0));
    let callback_conflicts = conflicts.clone();
    let builder = LambdaWorker::builder(
        version.clone(),
        WorkerOptions::new(queue.clone())
            .register_workflow::<LambdaWorkflow>()
            .unwrap()
            .register_activities(LambdaActivities {
                registry,
                ..Default::default()
            })
            .worker_interceptor(ShutdownObserver(key))
            .worker_plugin(ConflictingPlugin(conflicts.clone()))
            .build(),
    )
    .client_options(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace())
            .plugin(ClientAndWorkerPlugin::new(ConflictingPlugin(
                conflicts.clone(),
            )))
            .build(),
    )
    .configure(move |mut config| {
        config
            .worker_options
            .worker_plugin(ConflictingPlugin(callback_conflicts.clone()));
        async { Ok(config) }
    })
    .lambda_options(limits)
    .open_telemetry(telemetry)
    .shutdown_hook(move |_| {
        if let Some(registry) = hook_registry.lock().unwrap().take() {
            // Keeping the registry alive prevents client drop from hiding a missing finalizer.
            assert!(
                registry
                    .unregister_slot_provider(hook_key.lock().unwrap().unwrap())
                    .is_err(),
                "worker registration remains when hooks start"
            );
        }
        hook_calls.fetch_add(1, Ordering::Relaxed);
        async { Ok(()) }
    });
    let worker = if auto_upgrade {
        builder.default_versioning_behavior(VersioningBehavior::AutoUpgrade)
    } else {
        builder
    }
    .build()
    .unwrap();
    worker.handle(event("register-version")).await.unwrap();
    let description = client
        .connection()
        .clone()
        .describe_worker_deployment(
            DescribeWorkerDeploymentRequest {
                namespace: client.namespace(),
                deployment_name: queue.clone(),
            }
            .into_request(),
        )
        .await
        .unwrap()
        .into_inner();
    #[allow(deprecated)]
    client
        .connection()
        .clone()
        .set_worker_deployment_current_version(
            SetWorkerDeploymentCurrentVersionRequest {
                namespace: client.namespace(),
                deployment_name: queue.clone(),
                version: format!("{queue}.test"),
                conflict_token: description.conflict_token,
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap();
    for invocation in 0..3 {
        collector
            .unavailable
            .store(invocation == 1, Ordering::Relaxed);
        let previous_metrics = collector.metrics.load(Ordering::Relaxed);
        let handle = client
            .start_workflow(
                LambdaWorkflow::run,
                "Temporal".to_owned(),
                WorkflowStartOptions::new(queue.clone(), format!("lambda-{}", Uuid::new_v4()))
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
        assert_eq!(conflicts.load(Ordering::Relaxed), 3 * (invocation + 2));
        let events = handle
            .fetch_history(Default::default())
            .into_events()
            .await
            .unwrap();
        assert!(events.iter().any(|event| matches!(&event.attributes,
        Some(Attributes::WorkflowTaskCompletedEventAttributes(attrs))
        if attrs.versioning_behavior == if auto_upgrade {
            ProtoVersioningBehavior::AutoUpgrade as i32
        } else {
            ProtoVersioningBehavior::Pinned as i32
        })));
        if invocation == 1 {
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
        let activity = spans
            .iter()
            .find(|span| {
                span.name.starts_with("StartActivity:") && span.parent_span_id == run.span_id
            })
            .unwrap();
        assert!(
            spans
                .iter()
                .any(|span| span.name.starts_with("RunActivity:")
                    && span.parent_span_id == activity.span_id
                    && span.trace_id == run.trace_id)
        );
        let before_replay = exporter.get_finished_spans().unwrap().len();
        WorkflowReplayer::new(
            WorkflowReplayerOptions::new()
                .worker_plugin(plugin.clone())
                .register_workflow::<LambdaWorkflow>()
                .unwrap()
                .build(),
        )
        .unwrap()
        .replay_workflow(handle.fetch_history(Default::default()))
        .await
        .unwrap();
        assert_eq!(exporter.get_finished_spans().unwrap().len(), before_replay);
    }
    stop_tx.send(()).unwrap();
    collector_task.await.unwrap().unwrap();
}

#[workflow]
#[derive(Default)]
struct LambdaCancellationWorkflow;

#[workflow_methods]
impl LambdaCancellationWorkflow {
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

#[rstest::rstest]
#[case::drained(false, false)]
#[case::timed_out(true, false)]
#[case::drains_during_hooks(true, true)]
#[tokio::test]
async fn lambda_drains_cancelled_activities_before_hooks(
    #[case] block_completion: bool,
    #[case] release_during_cleanup: bool,
) {
    let client = Client::connect(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace()).build(),
    )
    .await
    .unwrap();
    let queue = format!("lambda-cancel-{}", Uuid::new_v4());
    let version = WorkerDeploymentVersion::builder()
        .deployment_name(queue.clone())
        .build_id("test")
        .build();
    let cancelled = Arc::new(AtomicBool::new(false));
    let completion = Arc::new(Notify::new());
    let gate = block_completion.then(|| Arc::new(Notify::new()));
    let hook_gate = gate.clone();
    let registry = Arc::new(Mutex::new(None::<Arc<ClientWorkerSet>>));
    let key = Arc::new(Mutex::new(None::<Uuid>));
    let hook_called = Arc::new(AtomicBool::new(false));
    let called = hook_called.clone();
    let observed = Arc::new(AtomicBool::new(false));
    let observed_in_hook = observed.clone();
    let finished = cancelled.clone();
    let release = Arc::new(AtomicBool::new(false));
    let release_from_hook = release.clone();
    let mut limits = LambdaWorkerOptions::default();
    limits.graceful_shutdown_period = Duration::from_millis(100);
    let worker = LambdaWorker::builder(
        version.clone(),
        WorkerOptions::new(queue.clone())
            .register_workflow::<LambdaCancellationWorkflow>()
            .unwrap()
            .register_activities(LambdaActivities {
                cancelled: cancelled.clone(),
                completion: completion.clone(),
                gate: gate.clone(),
                registry: registry.clone(),
            })
            .worker_interceptor(ShutdownObserver(key.clone()))
            .build(),
    )
    .client_options(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace()).build(),
    )
    .lambda_options(limits)
    .shutdown_hook(move |remaining| {
        assert!(!remaining.is_zero(), "drain consumed the hook reserve");
        called.store(true, Ordering::Relaxed);
        observed_in_hook.store(finished.load(Ordering::Relaxed), Ordering::Relaxed);
        if release_from_hook.load(Ordering::Relaxed) {
            hook_gate.as_ref().unwrap().notify_one();
        }
        async { Ok(()) }
    })
    .build()
    .unwrap();
    worker.handle(event("register-version")).await.unwrap();
    hook_called.store(false, Ordering::Relaxed);
    release.store(release_during_cleanup, Ordering::Relaxed);
    let _handle = client
        .start_workflow(
            LambdaCancellationWorkflow::run,
            (),
            WorkflowStartOptions::new(queue, format!("lambda-cancel-{}", Uuid::new_v4()))
                .versioning_override(VersioningOverride::Pinned(version))
                .build(),
        )
        .await
        .unwrap();
    let result = worker.handle(event("cancel-activity")).await;
    assert!(hook_called.load(Ordering::Relaxed));
    if block_completion {
        assert!(result.unwrap_err().requires_restart());
        assert!(!observed.load(Ordering::Relaxed));
        assert!(
            worker
                .handle(event("after-timeout"))
                .await
                .unwrap_err()
                .requires_restart()
        );
        if release_during_cleanup {
            assert!(cancelled.load(Ordering::Relaxed));
        } else {
            // Release the background Activity so the test runtime can close without retained work.
            gate.unwrap().notify_one();
            tokio::time::timeout(Duration::from_secs(6), completion.notified())
                .await
                .unwrap();
        }
    } else {
        result.unwrap();
        assert!(observed.load(Ordering::Relaxed));
    }
    if !block_completion || release_during_cleanup {
        let registry = registry.lock().unwrap().take().unwrap();
        assert!(
            registry
                .unregister_slot_provider(key.lock().unwrap().unwrap())
                .is_err(),
            "shutdown finalization was abandoned while hooks ran"
        );
    }
}
