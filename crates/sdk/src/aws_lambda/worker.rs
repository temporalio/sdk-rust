//! Helpers for running a Temporal worker in AWS Lambda.
//!
//! This module is available with the `aws-lambda` crate feature. [`run_worker`] owns the Lambda
//! runtime loop. It creates one SDK runtime at cold start, then creates a fresh Temporal client and
//! worker for every Lambda invocation.

use crate::{
    Runtime, Worker, WorkerOptions,
    runtime::{PollerBehavior, RuntimeOptions, TunerHolder},
};
use lambda_runtime::{Context, LambdaEvent, service_fn};
use serde_json::Value;
use std::{
    env,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_client::{
    Client, ClientOptions, ConnectionOptions,
    envconfig::{ConfigError, DataSource, LoadClientConfigProfileOptions},
    errors::ClientConnectError,
};
pub use temporalio_common::worker::WorkerDeploymentVersion;

const DEFAULT_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_SHUTDOWN_HOOK_BUFFER: Duration = Duration::from_secs(2);
const MINIMUM_WORK_TIME: Duration = Duration::from_secs(1);
const LOW_WORK_TIME_WARNING: Duration = Duration::from_secs(5);
const DEFAULT_CONFIG_FILE: &str = "temporal.toml";
const ENV_CONFIG_FILE: &str = "TEMPORAL_CONFIG_FILE";
const ENV_LAMBDA_TASK_ROOT: &str = "LAMBDA_TASK_ROOT";
const ENV_TASK_QUEUE: &str = "TEMPORAL_TASK_QUEUE";

type ShutdownHookFuture = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send + 'static>>;
type ShutdownHook = Arc<dyn Fn(Context) -> ShutdownHookFuture + Send + Sync + 'static>;

/// Configuration for [`run_worker`].
///
/// The initial values are loaded from Temporal environment configuration and a Lambda deployment
/// package's `temporal.toml`, then tuned for Lambda's constrained invocation model. The configure
/// callback may replace any public option. Deployment version and `use_worker_versioning` are
/// enforced after the callback returns.
pub struct LambdaWorkerOptions {
    /// Options used to initialize the SDK runtime once per Lambda execution environment.
    pub runtime_options: RuntimeOptions,
    /// Options used to create a fresh Temporal connection for each invocation.
    pub connection_options: ConnectionOptions,
    /// Options used to create a fresh namespace client for each invocation.
    pub client_options: ClientOptions,
    /// Worker registrations and options cloned for each invocation.
    pub worker_options: WorkerOptions,
    /// How long before the Lambda deadline to begin worker shutdown.
    ///
    /// This must be at least [`LambdaWorkerOptions::worker_shutdown_timeout`]. The default is seven
    /// seconds, reserving five seconds for the worker and two seconds for shutdown hooks.
    pub shutdown_deadline_buffer: Duration,
    /// Maximum time to wait for graceful worker shutdown after polling stops.
    ///
    /// When this expires, in-flight Rust activity tasks are aborted before shutdown hooks run. The
    /// default is five seconds.
    pub worker_shutdown_timeout: Duration,
    shutdown_hooks: Vec<ShutdownHook>,
}

impl LambdaWorkerOptions {
    /// Load Temporal client settings and construct Lambda-tuned worker defaults.
    ///
    /// Client configuration is resolved from `TEMPORAL_CONFIG_FILE`, then
    /// `$LAMBDA_TASK_ROOT/temporal.toml`, then `./temporal.toml`. The file is optional and Temporal
    /// environment variables override file values. `TEMPORAL_TASK_QUEUE` supplies the initial task
    /// queue when present.
    pub fn from_environment() -> Result<Self, LambdaWorkerError> {
        let config_source = DataSource::Path(
            lambda_config_file_path(|key| env::var_os(key))
                .to_string_lossy()
                .into(),
        );
        let (connection_options, client_options) = ClientOptions::load_from_config(
            LoadClientConfigProfileOptions::builder()
                .config_source(config_source)
                .build(),
        )
        .map_err(LambdaWorkerError::LoadConfiguration)?;

        let task_queue = env::var(ENV_TASK_QUEUE).unwrap_or_default();
        Ok(lambda_worker_options(
            connection_options,
            client_options,
            task_queue,
        ))
    }

    /// Add a best-effort asynchronous hook that runs after each invocation's worker stops.
    ///
    /// Hooks run in registration order and receive the invocation context. A hook error is logged
    /// and does not prevent later hooks from running. Hooks are useful for flushing telemetry
    /// providers that must remain alive across warm Lambda invocations.
    pub fn on_shutdown<F, Fut>(&mut self, hook: F) -> &mut Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
    {
        self.shutdown_hooks
            .push(Arc::new(move |context| Box::pin(hook(context))));
        self
    }

    fn prepare(&mut self, version: WorkerDeploymentVersion) -> Result<(), LambdaWorkerError> {
        if version.deployment_name.trim().is_empty() || version.build_id.trim().is_empty() {
            return Err(LambdaWorkerError::InvalidConfiguration(
                "worker deployment name and build ID must both be non-empty".to_owned(),
            ));
        }
        if self.worker_options.task_queue.trim().is_empty() {
            return Err(LambdaWorkerError::InvalidConfiguration(format!(
                "task queue is required: set worker_options.task_queue or {ENV_TASK_QUEUE}",
            )));
        }
        if self.worker_shutdown_timeout.is_zero() {
            return Err(LambdaWorkerError::InvalidConfiguration(
                "worker_shutdown_timeout must be greater than zero".to_owned(),
            ));
        }
        if self.shutdown_deadline_buffer < self.worker_shutdown_timeout {
            return Err(LambdaWorkerError::InvalidConfiguration(
                "shutdown_deadline_buffer must be greater than or equal to worker_shutdown_timeout"
                    .to_owned(),
            ));
        }

        self.worker_options.deployment_options.version = version;
        self.worker_options.deployment_options.use_worker_versioning = true;
        Ok(())
    }
}

fn lambda_worker_options(
    connection_options: ConnectionOptions,
    client_options: ClientOptions,
    task_queue: String,
) -> LambdaWorkerOptions {
    let worker_options = WorkerOptions::new(task_queue)
        .max_cached_workflows(30)
        .tuner(Arc::new(TunerHolder::fixed_size(10, 2, 2, 5)))
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1))
        .nexus_task_poller_behavior(PollerBehavior::SimpleMaximum(1))
        .max_eager_activity_reservations_per_workflow_task(0)
        .graceful_shutdown_period(DEFAULT_WORKER_SHUTDOWN_TIMEOUT)
        .build();

    LambdaWorkerOptions {
        runtime_options: RuntimeOptions::default(),
        connection_options,
        client_options,
        worker_options,
        shutdown_deadline_buffer: DEFAULT_WORKER_SHUTDOWN_TIMEOUT + DEFAULT_SHUTDOWN_HOOK_BUFFER,
        worker_shutdown_timeout: DEFAULT_WORKER_SHUTDOWN_TIMEOUT,
        shutdown_hooks: Vec::new(),
    }
}

/// Errors produced while configuring or running an AWS Lambda worker.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LambdaWorkerError {
    /// Temporal environment or file configuration could not be loaded.
    #[error("failed to load Temporal client configuration: {0}")]
    LoadConfiguration(#[source] ConfigError),
    /// The configure callback failed.
    #[error("Lambda worker configure callback failed: {0}")]
    Configure(#[source] anyhow::Error),
    /// Lambda worker options are not internally consistent.
    #[error("invalid Lambda worker configuration: {0}")]
    InvalidConfiguration(String),
    /// The SDK runtime could not be initialized.
    #[error("failed to initialize Temporal SDK runtime: {0}")]
    RuntimeInitialization(#[source] anyhow::Error),
    /// The AWS Lambda runtime loop failed.
    #[error("AWS Lambda runtime failed: {0}")]
    LambdaRuntime(#[source] lambda_runtime::Error),
    /// A per-invocation Temporal client connection failed.
    #[error("failed to connect Lambda worker to Temporal: {0}")]
    Connect(#[source] ClientConnectError),
    /// The Temporal client did not connect before the reserved shutdown window began.
    #[error(
        "Lambda worker did not connect before its reserved {shutdown_buffer:?} shutdown window"
    )]
    StartupDeadline {
        /// Time reserved for worker and hook shutdown.
        shutdown_buffer: Duration,
    },
    /// A per-invocation worker could not be created.
    #[error("failed to create Lambda worker: {0}")]
    CreateWorker(#[source] crate::WorkerCreateError),
    /// A per-invocation worker stopped with an error.
    #[error("Lambda worker failed while running: {0}")]
    RunWorker(#[source] anyhow::Error),
    /// Too little time remains in the invocation to start polling safely.
    #[error(
        "Lambda invocation has {remaining:?} remaining; reserving {shutdown_buffer:?} for shutdown leaves {work_time:?} for work"
    )]
    InsufficientWorkTime {
        /// Time remaining in the Lambda invocation.
        remaining: Duration,
        /// Time reserved for worker and hook shutdown.
        shutdown_buffer: Duration,
        /// Time that would remain for polling.
        work_time: Duration,
    },
    /// Graceful worker shutdown exceeded its configured bound.
    #[error("Lambda worker did not shut down within {timeout:?}")]
    WorkerShutdownTimeout {
        /// Configured graceful worker shutdown bound.
        timeout: Duration,
    },
}

/// Start the AWS Lambda runtime and serve Temporal worker invocations.
///
/// `configure` runs once at cold start after environment/file settings and Lambda defaults have
/// been applied. Register workflows and activities on `options.worker_options`, install plugins on
/// the client or worker options, and use [`LambdaWorkerOptions::on_shutdown`] for per-invocation
/// telemetry flushing. This function does not return during normal Lambda operation.
pub async fn run_worker<F>(
    version: WorkerDeploymentVersion,
    configure: F,
) -> Result<(), LambdaWorkerError>
where
    F: FnOnce(&mut LambdaWorkerOptions) -> Result<(), anyhow::Error>,
{
    let mut options = LambdaWorkerOptions::from_environment()?;
    configure(&mut options).map_err(LambdaWorkerError::Configure)?;
    options.prepare(version)?;

    let runtime_options = std::mem::take(&mut options.runtime_options);
    let runtime = Runtime::new_assume_tokio(runtime_options)
        .map_err(LambdaWorkerError::RuntimeInitialization)?;
    let state = Arc::new(LambdaWorkerState {
        runtime,
        connection_options: options.connection_options,
        client_options: options.client_options,
        worker_options: options.worker_options,
        shutdown_deadline_buffer: options.shutdown_deadline_buffer,
        worker_shutdown_timeout: options.worker_shutdown_timeout,
        shutdown_hooks: options.shutdown_hooks,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
        let state = state.clone();
        async move {
            state.invoke(event.context).await.map_err(|error| {
                lambda_runtime::Error::from(std::io::Error::other(error.to_string()))
            })
        }
    }))
    .await
    .map_err(LambdaWorkerError::LambdaRuntime)
}

struct LambdaWorkerState {
    runtime: Runtime,
    connection_options: ConnectionOptions,
    client_options: ClientOptions,
    worker_options: WorkerOptions,
    shutdown_deadline_buffer: Duration,
    worker_shutdown_timeout: Duration,
    shutdown_hooks: Vec<ShutdownHook>,
}

impl LambdaWorkerState {
    async fn invoke(&self, context: Context) -> Result<(), LambdaWorkerError> {
        let work_time = work_time(
            context.deadline(),
            SystemTime::now(),
            self.shutdown_deadline_buffer,
        )?;
        if work_time < LOW_WORK_TIME_WARNING {
            warn!(
                ?work_time,
                shutdown_buffer = ?self.shutdown_deadline_buffer,
                "Lambda timeout leaves less than five seconds for Temporal worker polling"
            );
        }

        let soft_shutdown_at = tokio::time::Instant::now() + work_time;
        let hard_shutdown_at = soft_shutdown_at + self.worker_shutdown_timeout;
        let result = self
            .run_invocation(&context, soft_shutdown_at, hard_shutdown_at)
            .await;
        self.run_shutdown_hooks(&context).await;
        result
    }

    async fn run_invocation(
        &self,
        context: &Context,
        soft_shutdown_at: tokio::time::Instant,
        hard_shutdown_at: tokio::time::Instant,
    ) -> Result<(), LambdaWorkerError> {
        let mut connection_options = self.connection_options.clone();
        if connection_options.identity.is_empty() {
            connection_options.identity = lambda_identity(context);
        }
        let client = tokio::time::timeout_at(
            soft_shutdown_at,
            Client::connect(connection_options, self.client_options.clone()),
        )
        .await
        .map_err(|_| LambdaWorkerError::StartupDeadline {
            shutdown_buffer: self.shutdown_deadline_buffer,
        })?
        .map_err(LambdaWorkerError::Connect)?;
        let mut worker = Worker::new(&self.runtime, client, self.worker_options.clone())
            .map_err(LambdaWorkerError::CreateWorker)?;
        let shutdown = worker.shutdown_handle();

        let outcome = run_with_bounded_shutdown(
            worker.run(),
            tokio::time::sleep_until(soft_shutdown_at),
            || tokio::time::sleep_until(hard_shutdown_at),
            shutdown,
        )
        .await;

        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                worker.core_worker().initiate_shutdown();
                worker.abort_active_activities();
                Err(LambdaWorkerError::RunWorker(error))
            }
            Err(()) => {
                worker.abort_active_activities();
                Err(LambdaWorkerError::WorkerShutdownTimeout {
                    timeout: self.worker_shutdown_timeout,
                })
            }
        }
    }

    async fn run_shutdown_hooks(&self, context: &Context) {
        run_shutdown_hooks(&self.shutdown_hooks, context).await;
    }
}

async fn run_shutdown_hooks(hooks: &[ShutdownHook], context: &Context) {
    for hook in hooks {
        let remaining = context
            .deadline()
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            warn!("Lambda deadline reached before all worker shutdown hooks could run");
            break;
        }
        match tokio::time::timeout(remaining, hook(context.clone())).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => error!(%error, "Lambda worker shutdown hook failed"),
            Err(_) => {
                warn!("Lambda worker shutdown hook exceeded the invocation deadline");
                break;
            }
        }
    }
}

async fn run_with_bounded_shutdown<F, S, H, HF, T>(
    run: F,
    shutdown_signal: S,
    hard_shutdown_signal: H,
    shutdown: impl FnOnce(),
) -> Result<T, ()>
where
    F: Future<Output = T>,
    S: Future<Output = ()>,
    H: FnOnce() -> HF,
    HF: Future<Output = ()>,
{
    tokio::pin!(run);
    tokio::pin!(shutdown_signal);
    tokio::select! {
        output = &mut run => Ok(output),
        () = &mut shutdown_signal => {
            shutdown();
            let hard_shutdown_signal = hard_shutdown_signal();
            tokio::pin!(hard_shutdown_signal);
            tokio::select! {
                output = &mut run => Ok(output),
                () = &mut hard_shutdown_signal => Err(()),
            }
        }
    }
}

fn work_time(
    deadline: SystemTime,
    now: SystemTime,
    shutdown_buffer: Duration,
) -> Result<Duration, LambdaWorkerError> {
    let remaining = deadline.duration_since(now).unwrap_or_default();
    let work_time = remaining.saturating_sub(shutdown_buffer);
    if work_time <= MINIMUM_WORK_TIME {
        return Err(LambdaWorkerError::InsufficientWorkTime {
            remaining,
            shutdown_buffer,
            work_time,
        });
    }
    Ok(work_time)
}

fn lambda_identity(context: &Context) -> String {
    format!(
        "{}@{}",
        nonempty_or_unknown(&context.request_id),
        nonempty_or_unknown(&context.invoked_function_arn),
    )
}

fn nonempty_or_unknown(value: &str) -> &str {
    if value.is_empty() { "unknown" } else { value }
}

fn lambda_config_file_path(getenv: impl Fn(&str) -> Option<std::ffi::OsString>) -> PathBuf {
    if let Some(path) = getenv(ENV_CONFIG_FILE).filter(|path| !path.is_empty()) {
        return PathBuf::from(path);
    }
    let root = getenv(ENV_LAMBDA_TASK_ROOT)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    root.join(DEFAULT_CONFIG_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::sync::oneshot;

    #[test]
    fn lambda_config_path_uses_expected_precedence() {
        let explicit = lambda_config_file_path(|key| match key {
            ENV_CONFIG_FILE => Some("/explicit/config.toml".into()),
            ENV_LAMBDA_TASK_ROOT => Some("/var/task".into()),
            _ => None,
        });
        assert_eq!(explicit, PathBuf::from("/explicit/config.toml"));

        let task_root = lambda_config_file_path(|key| {
            (key == ENV_LAMBDA_TASK_ROOT).then(|| "/var/task".into())
        });
        assert_eq!(task_root, PathBuf::from("/var/task/temporal.toml"));
        assert_eq!(
            lambda_config_file_path(|_| None),
            PathBuf::from("./temporal.toml")
        );
    }

    #[test]
    fn deadline_budget_rejects_one_second_or_less_of_work() {
        let now = SystemTime::UNIX_EPOCH;
        let buffer = Duration::from_secs(7);
        assert!(matches!(
            work_time(now + buffer + Duration::from_secs(1), now, buffer),
            Err(LambdaWorkerError::InsufficientWorkTime { .. })
        ));
        assert_eq!(
            work_time(now + buffer + Duration::from_secs(2), now, buffer).unwrap(),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn lambda_defaults_and_forced_versioning_are_applied() {
        use temporalio_common::worker::VersioningBehavior;

        let mut options = test_options("queue");
        assert_eq!(options.worker_options.max_cached_workflows, 30);
        assert_eq!(
            options.worker_options.workflow_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(2))
        );
        assert_eq!(
            options.worker_options.activity_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(1))
        );
        assert_eq!(
            options.worker_options.nexus_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(1))
        );
        assert_eq!(
            options.worker_options.graceful_shutdown_period,
            Some(DEFAULT_WORKER_SHUTDOWN_TIMEOUT)
        );
        assert_eq!(
            options
                .worker_options
                .max_eager_activity_reservations_per_workflow_task,
            0
        );
        assert_eq!(
            options
                .worker_options
                .tuner
                .workflow_task_slot_supplier()
                .available_slots(),
            Some(10)
        );
        assert_eq!(
            options
                .worker_options
                .tuner
                .activity_task_slot_supplier()
                .available_slots(),
            Some(2)
        );
        assert_eq!(
            options
                .worker_options
                .tuner
                .local_activity_slot_supplier()
                .available_slots(),
            Some(2)
        );
        assert_eq!(
            options
                .worker_options
                .tuner
                .nexus_task_slot_supplier()
                .available_slots(),
            Some(5)
        );

        options
            .worker_options
            .deployment_options
            .default_versioning_behavior = Some(VersioningBehavior::AutoUpgrade);
        let version = WorkerDeploymentVersion {
            deployment_name: "deployment".to_owned(),
            build_id: "build".to_owned(),
        };
        options.prepare(version.clone()).unwrap();
        assert_eq!(options.worker_options.deployment_options.version, version);
        assert!(
            options
                .worker_options
                .deployment_options
                .use_worker_versioning
        );
        assert_eq!(
            options
                .worker_options
                .deployment_options
                .default_versioning_behavior,
            Some(VersioningBehavior::AutoUpgrade)
        );
    }

    #[test]
    fn prepare_rejects_missing_task_queue_and_invalid_shutdown_window() {
        let version = WorkerDeploymentVersion {
            deployment_name: "deployment".to_owned(),
            build_id: "build".to_owned(),
        };
        let mut options = test_options("");
        assert!(matches!(
            options.prepare(version.clone()),
            Err(LambdaWorkerError::InvalidConfiguration(_))
        ));

        options.worker_options.task_queue = "queue".to_owned();
        options.shutdown_deadline_buffer = Duration::from_secs(4);
        assert!(matches!(
            options.prepare(version),
            Err(LambdaWorkerError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn identity_uses_invocation_request_and_function() {
        assert_eq!(
            lambda_identity(&test_context()),
            "request-id@arn:aws:lambda:region:account:function:test"
        );
    }

    #[tokio::test]
    async fn soft_deadline_signals_shutdown_and_continues_driving_worker() {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (finished_tx, finished_rx) = oneshot::channel();
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let shutdown_called_clone = shutdown_called.clone();

        let result = run_with_bounded_shutdown(
            async move {
                let _ = shutdown_rx.await;
                let _ = finished_tx.send(());
                42
            },
            std::future::ready(()),
            std::future::pending,
            move || {
                shutdown_called_clone.store(true, Ordering::SeqCst);
                let _ = shutdown_tx.send(());
            },
        )
        .await;

        assert_eq!(result, Ok(42));
        assert!(shutdown_called.load(Ordering::SeqCst));
        assert!(finished_rx.await.is_ok());
    }

    #[tokio::test]
    async fn hard_deadline_bounds_uncooperative_shutdown() {
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let shutdown_called_clone = shutdown_called.clone();
        let result = run_with_bounded_shutdown(
            std::future::pending::<()>(),
            std::future::ready(()),
            || std::future::ready(()),
            move || shutdown_called_clone.store(true, Ordering::SeqCst),
        )
        .await;

        assert_eq!(result, Err(()));
        assert!(shutdown_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_hooks_run_in_order_after_errors() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut hooks: Vec<ShutdownHook> = Vec::new();
        for (name, fail) in [("first", true), ("second", false)] {
            let order = order.clone();
            hooks.push(Arc::new(move |_| {
                let order = order.clone();
                Box::pin(async move {
                    order.lock().unwrap().push(name);
                    if fail {
                        anyhow::bail!("expected test failure");
                    }
                    Ok(())
                })
            }));
        }

        run_shutdown_hooks(&hooks, &test_context()).await;
        assert_eq!(*order.lock().unwrap(), ["first", "second"]);
    }

    fn test_context() -> Context {
        use http::{HeaderMap, HeaderValue};
        use lambda_runtime::Config;

        let deadline = SystemTime::now()
            .checked_add(Duration::from_secs(60))
            .unwrap()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        let mut headers = HeaderMap::new();
        headers.insert(
            "lambda-runtime-deadline-ms",
            HeaderValue::from_str(&deadline).unwrap(),
        );
        headers.insert(
            "lambda-runtime-invoked-function-arn",
            HeaderValue::from_static("arn:aws:lambda:region:account:function:test"),
        );
        Context::new("request-id", Arc::new(Config::default()), &headers).unwrap()
    }

    fn test_options(task_queue: &str) -> LambdaWorkerOptions {
        use temporalio_client::Url;

        lambda_worker_options(
            ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap()).build(),
            ClientOptions::new("default").build(),
            task_queue.to_owned(),
        )
    }
}
