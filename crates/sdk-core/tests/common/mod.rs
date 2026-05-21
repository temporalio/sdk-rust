//! Common integration testing utilities
//! These utilities are specific to integration tests and depend on the full temporal-client stack.

pub(crate) mod activity_functions;
pub(crate) mod fake_grpc_server;
pub(crate) mod http_proxy;
pub(crate) mod workflows;

use anyhow::bail;
use futures_util::{
    Future, StreamExt, future, stream,
    stream::{Stream, TryStreamExt},
};
use parking_lot::Mutex;
use prost::Message;
use rand::RngExt;
use std::{
    cell::Cell,
    collections::VecDeque,
    convert::TryFrom,
    env,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use temporalio_client::{
    Client, ClientTlsOptions, Connection, ConnectionOptions, NamespacedClient, TlsOptions,
    UntypedWorkflow, UntypedWorkflowHandle, WorkflowExecutionInfo, WorkflowGetResultOptions,
    WorkflowHandle, WorkflowStartOptions,
    errors::{WorkflowGetResultError, WorkflowStartError},
    grpc::WorkflowService,
};
use temporalio_common::{
    HasWorkflowDefinition, WorkflowDefinition,
    data_converters::{DataConverter, RawValue},
    protos::{
        coresdk::{
            workflow_activation::WorkflowActivation,
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::Payload, history::v1::History, workflowservice::v1::GetClusterInfoRequest,
        },
    },
    telemetry::{
        Logger, OtelCollectorOptions, PrometheusExporterOptions, TelemetryOptions,
        build_otlp_metric_exporter, metrics::CoreMeter, start_prometheus_metric_exporter,
    },
    worker::{WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes},
};
use temporalio_sdk::{
    Worker, WorkerOptions, WorkflowRegistrationError,
    activities::ActivityImplementer,
    interceptors::{
        FailOnNondeterminismInterceptor, InterceptorWithNext, ReturnWorkflowExitValueInterceptor,
        WorkerInterceptor,
    },
    workflows::WorkflowImplementation,
};
#[cfg(any(feature = "test-utilities", test))]
pub(crate) use temporalio_sdk_core::test_help::NAMESPACE;
use temporalio_sdk_core::{
    CoreRuntime, RuntimeOptions, Worker as CoreWorker, WorkerConfig, WorkerVersioningStrategy,
    init_replay_worker, init_worker,
    replay::{HistoryForReplay, ReplayWorkerInput},
    test_help::{MockPollCfg, build_mock_pollers, mock_worker},
};
use tokio::{sync::OnceCell, task::AbortHandle};
use tonic::IntoRequest;
use tracing::{debug, warn};
use url::Url;
use uuid::Uuid;
/// The env var used to specify where the integ tests should point
pub(crate) const INTEG_SERVER_TARGET_ENV_VAR: &str = "TEMPORAL_SERVICE_ADDRESS";
pub(crate) const INTEG_NAMESPACE_ENV_VAR: &str = "TEMPORAL_NAMESPACE";
pub(crate) const INTEG_USE_TLS_ENV_VAR: &str = "TEMPORAL_USE_TLS";
pub(crate) const INTEG_API_KEY: &str = "TEMPORAL_API_KEY_PATH";
pub(crate) static SEARCH_ATTR_TXT: &str = "CustomTextField";
pub(crate) static SEARCH_ATTR_INT: &str = "CustomIntField";
/// If set, turn export traces and metrics to the OTel collector at the given URL
pub(crate) const OTEL_URL_ENV_VAR: &str = "TEMPORAL_INTEG_OTEL_URL";
/// If set, enable direct scraping of prom metrics on the specified port
pub(crate) const PROM_ENABLE_ENV_VAR: &str = "TEMPORAL_INTEG_PROM_PORT";
/// This should match the prometheus port exposed in docker-compose-ci.yaml
pub(crate) const PROMETHEUS_QUERY_API: &str = "http://localhost:9090/api/v1/query";
/// If set, integ tests will use this specific version of CLI for starting dev server
pub(crate) const CLI_VERSION_OVERRIDE_ENV_VAR: &str = "CLI_VERSION_OVERRIDE";
pub(crate) const INTEG_CLIENT_IDENTITY: &str = "integ_tester";
pub(crate) const INTEG_CLIENT_NAME: &str = "temporal-core";
pub(crate) const INTEG_CLIENT_VERSION: &str = "0.1.0";

/// Create a worker instance which will use the provided test name to base the task queue and wf id
/// upon. Returns the instance.
pub(crate) async fn init_core_and_create_wf(test_name: &str) -> CoreWfStarter {
    let mut starter = CoreWfStarter::new(test_name);
    let _ = starter.get_worker().await;
    starter.start_wf().await;
    starter
}

pub(crate) fn integ_namespace() -> String {
    env::var(INTEG_NAMESPACE_ENV_VAR).unwrap_or(NAMESPACE.to_string())
}

pub(crate) fn integ_worker_config(tq: &str) -> WorkerConfig {
    WorkerConfig::builder()
        .namespace(integ_namespace())
        .task_queue(tq)
        .max_outstanding_activities(100_usize)
        .max_outstanding_local_activities(100_usize)
        .max_outstanding_workflow_tasks(100_usize)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "test_build_id".to_owned(),
        })
        .task_types(WorkerTaskTypes::all())
        .skip_client_worker_set_check(true)
        .build()
        .expect("Configuration options construct properly")
}

pub(crate) fn integ_sdk_config(tq: &str) -> WorkerOptions {
    WorkerOptions::new(tq)
        .deployment_options(WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "".to_owned(),
                build_id: "test_build_id".to_owned(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        })
        .build()
}

/// Create a worker replay instance preloaded with provided histories. Returns the worker impl.
pub(crate) fn init_core_replay_preloaded<I>(test_name: &str, histories: I) -> CoreWorker
where
    I: IntoIterator<Item = HistoryForReplay> + 'static,
    <I as IntoIterator>::IntoIter: Send,
{
    init_core_replay_stream(test_name, stream::iter(histories))
}
pub(crate) fn init_core_replay_stream<I>(test_name: &str, histories: I) -> CoreWorker
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    init_integ_telem();
    let worker_cfg = integ_worker_config(test_name);
    init_replay_worker(ReplayWorkerInput::new(worker_cfg, histories))
        .expect("Replay worker must init properly")
}
pub(crate) fn replay_sdk_worker<I>(histories: I) -> Worker
where
    I: IntoIterator<Item = HistoryForReplay> + 'static,
    <I as IntoIterator>::IntoIter: Send,
{
    replay_sdk_worker_stream(stream::iter(histories))
}
pub(crate) fn replay_sdk_worker_stream<I>(histories: I) -> Worker
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    let core = init_core_replay_stream("replay_worker_test", histories);
    // TODO [rust-sdk-branch]: Needs DC passed in
    let mut worker = Worker::new_from_core(Arc::new(core), DataConverter::default());
    worker.set_worker_interceptor(FailOnNondeterminismInterceptor {});
    worker
}

/// Load history from a file containing the protobuf serialization of it.
/// File is expected to be a path within `crates/core/tests/histories`
pub(crate) async fn history_from_proto_binary(
    path_from_root: &str,
) -> Result<History, anyhow::Error> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("histories");
    path.push(path_from_root);
    let bytes = tokio::fs::read(path).await?;
    Ok(History::decode(&*bytes)?)
}

thread_local! {
    /// Can be set true to disable auto-initialization of integ-test telemetry.
    pub static DONT_AUTO_INIT_INTEG_TELEM: Cell<bool> = const { Cell::new(false) };
}
static INTEG_TESTS_RT: std::sync::OnceLock<CoreRuntime> = std::sync::OnceLock::new();
pub(crate) fn init_integ_telem() -> Option<&'static CoreRuntime> {
    if DONT_AUTO_INIT_INTEG_TELEM.get() {
        return None;
    }
    Some(INTEG_TESTS_RT.get_or_init(|| {
        let telemetry_options = get_integ_telem_options();
        let runtime_options = RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .expect("Runtime options build cleanly");
        let rt =
            CoreRuntime::new_assume_tokio(runtime_options).expect("Core runtime inits cleanly");
        if let Some(sub) = rt.telemetry().trace_subscriber() {
            let _ = tracing::subscriber::set_global_default(sub);
        }
        rt
    }))
}

pub(crate) async fn get_cloud_client() -> Client {
    let cloud_addr = env::var("TEMPORAL_CLOUD_ADDRESS").unwrap();
    let cloud_key = env::var("TEMPORAL_CLIENT_KEY").unwrap();

    let client_cert = env::var("TEMPORAL_CLIENT_CERT")
        .expect("TEMPORAL_CLIENT_CERT must be set")
        .replace("\\n", "\n")
        .into_bytes();
    let client_private_key = cloud_key.replace("\\n", "\n").into_bytes();
    let connection_opts = ConnectionOptions::new(Url::from_str(&cloud_addr).unwrap())
        .client_name("sdk-core-integ-tests")
        .client_version("clientver")
        .identity("sdk-test-client")
        .tls_options(TlsOptions {
            client_tls_options: Some(ClientTlsOptions {
                client_cert,
                client_private_key,
            }),
            ..Default::default()
        })
        .build();
    let connection = Connection::connect(connection_opts).await.unwrap();
    let namespace = env::var("TEMPORAL_NAMESPACE").expect("TEMPORAL_NAMESPACE must be set");
    let client_opts = temporalio_client::ClientOptions::new(namespace).build();
    Client::new(connection, client_opts).unwrap()
}

/// Implements a builder pattern to help integ tests initialize core and create workflows
pub(crate) struct CoreWfStarter {
    /// Used for both the task queue and workflow id
    task_queue_name: String,
    pub sdk_config: WorkerOptions,
    /// Options to use when starting workflow(s). Is initialized with task_queue & workflow_id
    /// to be the same, derived from test name given to `new`.
    pub workflow_options: WorkflowStartOptions,
    initted_worker: OnceCell<InitializedWorker>,
    runtime_override: Option<Arc<CoreRuntime>>,
    client_override: Option<Client>,
    min_local_server_version: Option<String>,
    /// Run when initializing, allows for altering the config used to init the core worker
    #[allow(clippy::type_complexity)] // It's not tho
    core_config_mutator: Option<Arc<dyn Fn(&mut WorkerConfig)>>,
}
struct InitializedWorker {
    worker: Arc<CoreWorker>,
    client: Client,
}

impl CoreWfStarter {
    pub(crate) fn new(test_name: &str) -> Self {
        init_integ_telem();
        Self::new_with_overrides(test_name, None, None)
    }

    pub(crate) fn new_with_runtime(test_name: &str, runtime: CoreRuntime) -> Self {
        Self::new_with_overrides(test_name, Some(runtime), None)
    }

    /// Targets cloud if the required env vars are present. Otherwise, local server (but only if
    /// the minimum version requirement is met). Returns None if the local server is not new enough.
    ///
    /// An empty string means to skip the version check.
    pub(crate) async fn new_cloud_or_local(test_name: &str, version_req: &str) -> Option<Self> {
        init_integ_telem();
        let mut check_mlsv = false;
        let client = if env::var("TEMPORAL_CLOUD_ADDRESS").is_ok() {
            Some(get_cloud_client().await)
        } else {
            check_mlsv = true;
            None
        };
        let mut s = Self::new_with_overrides(test_name, None, client);

        if check_mlsv && !version_req.is_empty() {
            let clustinfo = s
                .get_client()
                .await
                .get_cluster_info(GetClusterInfoRequest::default().into_request())
                .await;
            let srv_ver = semver::Version::parse(
                &clustinfo
                    .expect("must be able to get cluster info")
                    .into_inner()
                    .server_version,
            )
            .expect("must be able to parse server version");
            let req = semver::VersionReq::parse(version_req)
                .expect("must be able to parse server version requirement");

            if !req.matches(&srv_ver) {
                warn!(
                    "Server version {} does not meet requirement {} for test {}",
                    srv_ver, req, test_name
                );
                return None;
            }
        }

        Some(s)
    }

    pub(crate) fn new_with_overrides(
        test_name: &str,
        runtime_override: Option<CoreRuntime>,
        client_override: Option<Client>,
    ) -> Self {
        let task_q_salt = rand_6_chars();
        let task_queue = format!("{test_name}_{task_q_salt}");
        let sdk_config = integ_sdk_config(&task_queue);
        Self {
            task_queue_name: task_queue.clone(),
            sdk_config,
            initted_worker: OnceCell::new(),
            workflow_options: WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
            runtime_override: runtime_override.map(Arc::new),
            client_override,
            min_local_server_version: None,
            core_config_mutator: None,
        }
    }

    /// Create a new starter with no initialized worker or runtime override. Useful for starting a
    /// new worker on the same queue.
    pub(crate) fn clone_no_worker(&self) -> Self {
        Self {
            task_queue_name: self.task_queue_name.clone(),
            sdk_config: self.sdk_config.clone(),
            workflow_options: self.workflow_options.clone(),
            runtime_override: self.runtime_override.clone(),
            client_override: self.client_override.clone(),
            min_local_server_version: self.min_local_server_version.clone(),
            initted_worker: Default::default(),
            core_config_mutator: self.core_config_mutator.clone(),
        }
    }

    pub(crate) async fn worker(&mut self) -> TestWorker {
        let worker = self.get_worker().await;
        let client = self.get_client().await;
        let sdk = Worker::new_from_core_options(
            worker,
            client.data_converter().clone(),
            self.sdk_config.clone(),
        )
        .expect("SDK worker should initialize from core worker and options");
        let mut w = TestWorker::new(sdk);
        w.client = Some(client);

        w
    }

    pub(crate) fn set_core_cfg_mutator(&mut self, mutator: impl Fn(&mut WorkerConfig) + 'static) {
        self.core_config_mutator = Some(Arc::new(mutator))
    }

    pub(crate) async fn shutdown(&mut self) {
        self.get_worker().await.shutdown().await;
    }

    pub(crate) async fn get_worker(&mut self) -> Arc<CoreWorker> {
        self.get_or_init().await.worker.clone()
    }

    pub(crate) async fn get_client(&mut self) -> Client {
        self.get_or_init().await.client.clone()
    }

    /// Start the workflow defined by the builder and return run id
    pub(crate) async fn start_wf(&mut self) -> String {
        self.start_wf_with_id(self.get_wf_id().to_owned()).await
    }

    /// Starts the workflow using the worker
    pub(crate) async fn start_with_worker(
        &self,
        wf_name: impl Into<String>,
        worker: &mut TestWorker,
    ) -> UntypedWorkflowHandle<Client> {
        let wf_name = wf_name.into();
        worker
            .submit_wf(&wf_name, vec![], self.workflow_options.clone())
            .await
            .unwrap();
        self.initted_worker
            .get()
            .unwrap()
            .client
            .get_workflow_handle::<UntypedWorkflow>(&self.task_queue_name)
    }

    pub(crate) async fn start_wf_with_id(&self, workflow_id: String) -> String {
        let iw = self.initted_worker.get().expect(
            "Worker must be initted before starting a workflow.\
                             Tests must call `get_worker` first.",
        );
        let mut options = self.workflow_options.clone();
        options.workflow_id = workflow_id;
        iw.client
            .start_workflow(
                UntypedWorkflow::new(&self.task_queue_name),
                RawValue::empty(),
                options,
            )
            .await
            .unwrap()
            .run_id()
            .unwrap()
            .to_string()
    }

    pub(crate) fn get_task_queue(&self) -> &str {
        &self.task_queue_name
    }

    pub(crate) fn get_wf_id(&self) -> &str {
        &self.task_queue_name
    }

    /// Fetch the history of the default workflow for this starter. IE: The one that would
    /// be started by [CoreWfStarter::start_wf].
    pub(crate) async fn get_history(&self) -> History {
        let client = &self
            .initted_worker
            .get()
            .expect("Starter must be initialized")
            .client;
        let events = client
            .get_workflow_handle::<UntypedWorkflow>(self.get_wf_id())
            .fetch_history(Default::default())
            .await
            .unwrap()
            .into_events();
        History { events }
    }

    pub(crate) async fn wait_for_default_wf_finish(
        &self,
    ) -> Result<RawValue, WorkflowGetResultError> {
        self.initted_worker
            .get()
            .unwrap()
            .client
            .get_workflow_handle::<UntypedWorkflow>(&self.task_queue_name)
            .get_result(
                WorkflowGetResultOptions::builder()
                    .follow_runs(false)
                    .build(),
            )
            .await
    }

    async fn get_or_init(&mut self) -> &InitializedWorker {
        self.initted_worker
            .get_or_init(|| async {
                let rt = if let Some(ref rto) = self.runtime_override {
                    rto
                } else {
                    init_integ_telem().unwrap()
                };
                let (connection, client) = if let Some(client) = self.client_override.take() {
                    // Extract the connection from the client to pass to init_worker
                    let connection = client.connection().clone();
                    (connection, client)
                } else {
                    // Create connection and client
                    let mut opts = get_integ_server_options();
                    opts.metrics_meter = rt.telemetry().get_temporal_metric_meter();
                    let connection = Connection::connect(opts).await.expect("Must connect");
                    let client_opts =
                        temporalio_client::ClientOptions::new(integ_namespace()).build();
                    let client = Client::new(connection.clone(), client_opts).unwrap();
                    (connection, client)
                };
                let mut core_config = self
                    .sdk_config
                    .to_core_options(client.namespace(), client.identity())
                    .expect("sdk config converts to core config");
                if let Some(ref ccm) = self.core_config_mutator {
                    ccm(&mut core_config);
                }
                let worker =
                    init_worker(rt, core_config, connection).expect("Worker inits cleanly");
                InitializedWorker {
                    worker: Arc::new(worker),
                    client,
                }
            })
            .await
    }
}

/// Provides conveniences for running integ tests with the SDK (against real server or mocks)
pub(crate) struct TestWorker {
    inner: Worker,
    client: Option<Client>,
    pub started_workflows: Arc<Mutex<Vec<WorkflowExecutionInfo>>>,
    /// If set true (default), and a client is available, we will fetch workflow results to
    /// determine when they have all completed.
    pub fetch_results: bool,
}
impl TestWorker {
    /// Create a new test worker
    pub(crate) fn new(sdk: Worker) -> Self {
        Self {
            inner: sdk,
            client: None,
            started_workflows: Arc::new(Mutex::new(vec![])),
            fetch_results: true,
        }
    }

    pub(crate) fn inner_mut(&mut self) -> &mut Worker {
        &mut self.inner
    }

    pub(crate) fn worker_instance_key(&self) -> Uuid {
        self.inner.worker_instance_key()
    }

    pub(crate) fn register_activities<AI: ActivityImplementer>(
        &mut self,
        instance: AI,
    ) -> &mut Self {
        self.inner.register_activities::<AI>(instance);
        self
    }

    #[allow(unused)]
    pub(crate) fn register_workflow<W>(&mut self) -> Result<&mut Self, WorkflowRegistrationError>
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
    {
        self.inner.register_workflow::<W>()?;
        Ok(self)
    }

    pub(crate) fn register_workflow_with_factory<W, F>(
        &mut self,
        factory: F,
    ) -> Result<&mut Self, WorkflowRegistrationError>
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
        F: Fn() -> W + Send + Sync + 'static,
    {
        self.inner.register_workflow_with_factory::<W, F>(factory)?;
        Ok(self)
    }

    /// Create a handle that can be used to submit workflows. Useful when workflows need to be
    /// started concurrently with running the worker.
    pub(crate) fn get_submitter_handle(&self) -> TestWorkerSubmitterHandle {
        TestWorkerSubmitterHandle {
            client: self.client.clone().expect("client must be set"),
            started_workflows: self.started_workflows.clone(),
        }
    }

    /// Create a workflow, asking the server to start it with the provided workflow ID and using the
    /// provided workflow function.
    ///
    /// Increments the expected Workflow run count.
    ///
    /// Returns the run id of the started workflow (if no client has initialized returns a fake id)
    pub(crate) async fn submit_wf(
        &self,
        workflow_type: impl Into<String>,
        input: Vec<Payload>,
        mut options: WorkflowStartOptions,
    ) -> Result<String, anyhow::Error> {
        if self.client.is_none() {
            return Ok("fake_run_id".to_string());
        }
        // Fallback overall execution timeout to avoid leaving open workflows when testing against
        // cloud
        if options.execution_timeout.is_none() {
            options.execution_timeout = Some(Duration::from_secs(60 * 5));
        }
        self.get_submitter_handle()
            .submit_wf(workflow_type, input, options)
            .await
    }

    /// Start a workflow returning a handle to the started workflow.
    pub(crate) async fn submit_workflow<W>(
        &self,
        workflow: W,
        input: W::Input,
        mut options: WorkflowStartOptions,
    ) -> Result<WorkflowHandle<Client, W>, WorkflowStartError>
    where
        W: HasWorkflowDefinition,
        W::Input: Send,
    {
        let c = self.client.as_ref().expect("client must be set");
        if options.execution_timeout.is_none() {
            options.execution_timeout = Some(Duration::from_secs(60 * 5));
        }
        let wfid = options.workflow_id.clone();
        let handle = c.start_workflow(workflow, input, options).await?;
        self.started_workflows.lock().push(WorkflowExecutionInfo {
            namespace: c.namespace(),
            workflow_id: wfid,
            run_id: handle.info().run_id.clone(),
            first_execution_run_id: None,
        });
        Ok(handle)
    }

    pub(crate) fn expect_workflow_completion(
        &self,
        wf_id: impl Into<String>,
        run_id: Option<String>,
    ) {
        self.started_workflows.lock().push(WorkflowExecutionInfo {
            namespace: self
                .client
                .as_ref()
                .map(|c| c.namespace())
                .unwrap_or(NAMESPACE.to_owned()),
            workflow_id: wf_id.into(),
            run_id,
            first_execution_run_id: None,
        });
    }

    /// Runs until all expected workflows have completed and then shuts down the worker
    pub(crate) async fn run_until_done(&mut self) -> Result<(), anyhow::Error> {
        self.run_until_done_intercepted(Option::<TestWorkerCompletionIceptor>::None)
            .await
    }

    /// See [Self::run_until_done], but allows configuration of some low-level interception.
    pub(crate) async fn run_until_done_intercepted(
        &mut self,
        next_interceptor: Option<impl WorkerInterceptor + 'static>,
    ) -> Result<(), anyhow::Error> {
        let mut iceptor = TestWorkerCompletionIceptor::new(
            TestWorkerShutdownCond::NoAutoShutdown,
            Arc::new(self.inner.shutdown_handle()),
        );
        // Automatically use results-based complete detection if we have a client
        if self.fetch_results
            && let Some(c) = self.client.clone()
        {
            iceptor.condition = TestWorkerShutdownCond::GetResults(
                std::mem::take(&mut self.started_workflows.lock()),
                c,
            );
        }
        iceptor.next = next_interceptor.map(|i| Box::new(i) as Box<dyn WorkerInterceptor>);
        let get_results_waiter = iceptor.wait_all_wfs();
        self.inner.set_worker_interceptor(iceptor);
        tokio::try_join!(self.inner.run(), get_results_waiter)?;
        Ok(())
    }

    pub(crate) fn core_worker(&self) -> Arc<temporalio_sdk_core::Worker> {
        self.inner.core_worker()
    }
}

pub(crate) struct TestWorkerSubmitterHandle {
    client: Client,
    started_workflows: Arc<Mutex<Vec<WorkflowExecutionInfo>>>,
}
impl TestWorkerSubmitterHandle {
    /// Create a workflow, asking the server to start it with the provided workflow ID and using the
    /// provided workflow function.
    ///
    /// Increments the expected Workflow run count.
    ///
    /// Returns the run id of the started workflow
    pub(crate) async fn submit_wf(
        &self,
        workflow_type: impl Into<String>,
        input: Vec<Payload>,
        options: WorkflowStartOptions,
    ) -> Result<String, anyhow::Error> {
        let wfid = options.workflow_id.clone();
        let handle = self
            .client
            .start_workflow(
                UntypedWorkflow::new(workflow_type.into()),
                RawValue::new(input),
                options,
            )
            .await?;
        let run_id = handle.run_id().unwrap().to_string();
        self.started_workflows.lock().push(WorkflowExecutionInfo {
            namespace: self.client.namespace(),
            workflow_id: wfid,
            run_id: Some(run_id.clone()),
            first_execution_run_id: None,
        });
        Ok(run_id)
    }
}

pub(crate) enum TestWorkerShutdownCond {
    GetResults(Vec<WorkflowExecutionInfo>, Client),
    NoAutoShutdown,
}
/// Implements calling the shutdown handle when the expected number of test workflows has completed
pub(crate) struct TestWorkerCompletionIceptor {
    condition: TestWorkerShutdownCond,
    shutdown_handle: Arc<dyn Fn()>,
    next: Option<Box<dyn WorkerInterceptor>>,
}
impl TestWorkerCompletionIceptor {
    pub(crate) fn new(condition: TestWorkerShutdownCond, shutdown_handle: Arc<dyn Fn()>) -> Self {
        Self {
            condition,
            shutdown_handle,
            next: None,
        }
    }

    fn wait_all_wfs(&mut self) -> impl Future<Output = Result<(), anyhow::Error>> + 'static {
        if let TestWorkerShutdownCond::GetResults(ref mut wfs, ref client) = self.condition {
            let wfs = std::mem::take(wfs);
            let shutdown_h = self.shutdown_handle.clone();
            let client = client.clone();
            let stream = stream::iter(
                wfs.into_iter()
                    .map(move |info| info.bind_untyped(client.clone())),
            )
            .map(Ok);
            future::Either::Left(async move {
                stream
                    .try_for_each_concurrent(None, |wh| async move {
                        if let Err(e) = wh.get_result(Default::default()).await
                            && !e.is_workflow_outcome()
                        {
                            return Err(e.into());
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .await?;
                shutdown_h();
                Ok(())
            })
        } else {
            future::Either::Right(future::ready(Ok(())))
        }
    }
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for TestWorkerCompletionIceptor {
    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        if completion.has_execution_ending() {
            debug!("Workflow {} says it's finishing", &completion.run_id);
        }
        if let Some(n) = self.next.as_ref() {
            n.on_workflow_activation_completion(completion).await;
        }
    }

    fn on_shutdown(&self, sdk_worker: &Worker) {
        if let Some(n) = self.next.as_ref() {
            n.on_shutdown(sdk_worker);
        }
    }
    async fn on_workflow_activation(&self, a: &WorkflowActivation) -> Result<(), anyhow::Error> {
        if let Some(n) = self.next.as_ref() {
            n.on_workflow_activation(a).await?;
        }
        Ok(())
    }
}

/// Returns the connection options used to connect to the server used for integration tests.
pub(crate) fn get_integ_server_options() -> ConnectionOptions {
    let temporal_server_address = env::var(INTEG_SERVER_TARGET_ENV_VAR)
        .unwrap_or_else(|_| "http://localhost:7233".to_owned());
    let url = Url::try_from(&*temporal_server_address).unwrap();
    let api_key = env::var(INTEG_API_KEY)
        .ok()
        .map(|key_file| std::fs::read_to_string(key_file).unwrap());
    let tls_cfg = get_integ_tls_config();

    ConnectionOptions::new(url)
        .identity(INTEG_CLIENT_IDENTITY.to_string())
        .client_name(INTEG_CLIENT_NAME.to_string())
        .client_version(INTEG_CLIENT_VERSION.to_string())
        .maybe_api_key(api_key)
        .maybe_tls_options(tls_cfg)
        .build()
}

/// Helper to create a connection using the default integ test options
pub(crate) async fn get_integ_connection(
    meter: Option<temporalio_common::telemetry::metrics::TemporalMeter>,
) -> Connection {
    let mut opts = get_integ_server_options();
    opts.metrics_meter = meter;
    Connection::connect(opts).await.expect("Must connect")
}

/// Helper to create a namespaced client using the default integ test options
pub(crate) async fn get_integ_client(
    namespace: String,
    meter: Option<temporalio_common::telemetry::metrics::TemporalMeter>,
) -> Client {
    let connection = get_integ_connection(meter).await;
    let client_opts = temporalio_client::ClientOptions::new(namespace).build();
    Client::new(connection, client_opts).unwrap()
}

pub(crate) fn get_integ_tls_config() -> Option<TlsOptions> {
    if env::var(INTEG_USE_TLS_ENV_VAR).is_ok() {
        let root = std::fs::read("../.cloud_certs/ca.pem").unwrap();
        let client_cert = std::fs::read("../.cloud_certs/client.pem").unwrap();
        let client_private_key = std::fs::read("../.cloud_certs/client.key").unwrap();
        Some(TlsOptions {
            server_root_ca_cert: Some(root),
            domain: None,
            client_tls_options: Some(ClientTlsOptions {
                client_cert,
                client_private_key,
            }),
        })
    } else {
        None
    }
}

pub(crate) fn get_integ_telem_options() -> TelemetryOptions {
    let filter_string =
        env::var("RUST_LOG").unwrap_or_else(|_| "INFO,temporalio_sdk_core=INFO".to_string());

    if let Some(url) = env::var(OTEL_URL_ENV_VAR)
        .ok()
        .map(|x| x.parse::<Url>().unwrap())
    {
        let opts = OtelCollectorOptions::builder().url(url).build();
        TelemetryOptions::builder()
            .metrics(Arc::new(build_otlp_metric_exporter(opts).unwrap()) as Arc<dyn CoreMeter>)
            .logging(Logger::Console {
                filter: filter_string,
            })
            .build()
    } else if let Some(addr) = env::var(PROM_ENABLE_ENV_VAR)
        .ok()
        .map(|x| SocketAddr::new([127, 0, 0, 1].into(), x.parse().unwrap()))
    {
        let prom_info = start_prometheus_metric_exporter(
            PrometheusExporterOptions::builder()
                .socket_addr(addr)
                .build(),
        )
        .unwrap();
        TelemetryOptions::builder()
            .metrics(prom_info.meter as Arc<dyn CoreMeter>)
            .logging(Logger::Console {
                filter: filter_string,
            })
            .build()
    } else {
        TelemetryOptions::builder()
            .logging(Logger::Console {
                filter: filter_string,
            })
            .build()
    }
}

pub(crate) fn get_integ_runtime_options(telemopts: TelemetryOptions) -> RuntimeOptions {
    RuntimeOptions::builder()
        .telemetry_options(telemopts)
        .build()
        .unwrap()
}

#[async_trait::async_trait(?Send)]
pub(crate) trait WorkflowHandleExt {
    async fn fetch_history_and_replay(
        &self,
        worker: &mut Worker,
    ) -> Result<Option<Payload>, anyhow::Error>;
}

#[async_trait::async_trait(?Send)]
impl<W> WorkflowHandleExt for WorkflowHandle<Client, W>
where
    W: HasWorkflowDefinition,
{
    async fn fetch_history_and_replay(
        &self,
        worker: &mut Worker,
    ) -> Result<Option<Payload>, anyhow::Error> {
        let wf_id = self.info().workflow_id.clone();
        let events = self.fetch_history(Default::default()).await?.into_events();
        let with_id = HistoryForReplay::new(events, wf_id);
        let replay_worker = init_core_replay_preloaded(worker.task_queue(), [with_id]);
        worker.with_new_core_worker(Arc::new(replay_worker));
        let retval_icept = ReturnWorkflowExitValueInterceptor::default();
        let retval_handle = retval_icept.result_handle();
        let mut top_icept = InterceptorWithNext::new(Box::new(FailOnNondeterminismInterceptor {}));
        top_icept.set_next(Box::new(retval_icept));
        worker.set_worker_interceptor(top_icept);
        worker.run().await?;
        Ok(retval_handle.get().cloned())
    }
}

pub(crate) fn rand_6_chars() -> String {
    rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(6)
        .map(char::from)
        .collect()
}

pub(crate) static ANY_PORT: &str = "127.0.0.1:0";

pub(crate) fn prom_metrics(
    options_override: Option<PrometheusExporterOptions>,
) -> (TelemetryOptions, SocketAddr, AbortOnDrop) {
    let prom_exp_opts = options_override.unwrap_or_else(|| {
        PrometheusExporterOptions::builder()
            .socket_addr(ANY_PORT.parse().unwrap())
            .build()
    });
    let mut telemopts = get_integ_telem_options();
    let prom_info = start_prometheus_metric_exporter(prom_exp_opts).unwrap();
    telemopts.metrics = Some(prom_info.meter as Arc<dyn CoreMeter>);
    (
        telemopts,
        prom_info.bound_addr,
        AbortOnDrop {
            ah: prom_info.abort_handle,
        },
    )
}

pub(crate) struct AbortOnDrop {
    ah: AbortHandle,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.ah.abort();
    }
}

pub(crate) async fn eventually<F, Fut, T, E>(
    mut func: F,
    timeout: Duration,
) -> Result<T, anyhow::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let start = Instant::now();
    let mut last_err = None;
    loop {
        if start.elapsed() > timeout {
            bail!("Eventually hit timeout after {timeout:?}. Last error: {last_err:?}");
        }
        match func().await {
            Ok(v) => return Ok(v),
            Err(e) => last_err = Some(format!("{e:?}")),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(crate) fn build_fake_sdk(mock_cfg: MockPollCfg) -> temporalio_sdk::Worker {
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|c| {
        c.max_cached_workflows = 1;
        c.ignore_evicts_on_shutdown = false;
    });
    let core = mock_worker(mock);
    let mut worker =
        temporalio_sdk::Worker::new_from_core(Arc::new(core), DataConverter::default());
    worker.set_worker_interceptor(FailOnNondeterminismInterceptor {});
    worker
}

pub(crate) fn mock_sdk(poll_cfg: MockPollCfg) -> TestWorker {
    mock_sdk_cfg(poll_cfg, |_| {})
}

pub(crate) fn mock_sdk_cfg(
    mut poll_cfg: MockPollCfg,
    mutator: impl FnOnce(&mut WorkerConfig),
) -> TestWorker {
    poll_cfg.using_rust_sdk = true;
    let mut mock = build_mock_pollers(poll_cfg);
    mock.worker_cfg(mutator);
    let core = mock_worker(mock);
    TestWorker::new(temporalio_sdk::Worker::new_from_core(
        Arc::new(core),
        DataConverter::default(),
    ))
}

#[derive(Default)]
pub(crate) struct ActivationAssertionsInterceptor {
    #[allow(clippy::type_complexity)]
    assertions: Mutex<VecDeque<Box<dyn FnOnce(&WorkflowActivation)>>>,
    used: AtomicBool,
}

impl ActivationAssertionsInterceptor {
    pub(crate) fn skip_one(&mut self) -> &mut Self {
        self.assertions.lock().push_back(Box::new(|_| {}));
        self
    }

    pub(crate) fn then(&mut self, assert: impl FnOnce(&WorkflowActivation) + 'static) -> &mut Self {
        self.assertions.lock().push_back(Box::new(assert));
        self
    }
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for ActivationAssertionsInterceptor {
    async fn on_workflow_activation(&self, act: &WorkflowActivation) -> Result<(), anyhow::Error> {
        self.used.store(true, Ordering::Relaxed);
        if let Some(fun) = self.assertions.lock().pop_front() {
            fun(act);
        }
        Ok(())
    }
}

#[cfg(test)]
impl Drop for ActivationAssertionsInterceptor {
    fn drop(&mut self) {
        if !self.used.load(Ordering::Relaxed) {
            panic!("Activation assertions interceptor was never used!")
        }
    }
}

#[cfg(feature = "ephemeral-server")]
use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, TemporalDevServerConfig, default_cached_download,
};

#[cfg(feature = "ephemeral-server")]
pub(crate) fn integ_dev_server_config(
    mut extra_args: Vec<String>,
    ui: bool,
) -> TemporalDevServerConfig {
    let cli_version = if let Ok(ver_override) = env::var(CLI_VERSION_OVERRIDE_ENV_VAR) {
        EphemeralExe::CachedDownload {
            version: EphemeralExeVersion::Fixed(ver_override.to_owned()),
            dest_dir: None,
            ttl: None,
        }
    } else {
        default_cached_download()
    };
    extra_args.extend(
        [
            // TODO: Delete when temporalCLI enables it by default.
            "--dynamic-config-value".to_string(),
            "system.enableEagerWorkflowStart=true".to_string(),
            "--dynamic-config-value".to_owned(),
            "frontend.workerVersioningWorkflowAPIs=true".to_owned(),
            "--dynamic-config-value".to_owned(),
            "frontend.workerVersioningDataAPIs=true".to_owned(),
            "--dynamic-config-value".to_owned(),
            "system.enableDeploymentVersions=true".to_owned(),
            "--dynamic-config-value".to_owned(),
            "component.nexusoperations.recordCancelRequestCompletionEvents=true".to_owned(),
            "--dynamic-config-value".to_owned(),
            "frontend.WorkerHeartbeatsEnabled=true".to_owned(),
            "--dynamic-config-value".to_owned(),
            "frontend.ListWorkersEnabled=true".to_owned(),
            "--dynamic-config-value".to_owned(),
            "frontend.enableCancelWorkerPollsOnShutdown=true".to_owned(),
            "--dynamic-config-value".to_owned(),
            "matching.rps=12000".to_owned(),
            "--search-attribute".to_string(),
            format!("{SEARCH_ATTR_TXT}=Text"),
            "--search-attribute".to_string(),
            format!("{SEARCH_ATTR_INT}=Int"),
        ]
        .map(Into::into),
    );

    TemporalDevServerConfig::builder()
        .exe(cli_version)
        .extra_args(extra_args)
        .ui(ui)
        .build()
}
