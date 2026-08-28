#![warn(missing_docs)]

//! AWS Lambda support for the Temporal Rust SDK.
//!
//! A [`LambdaWorker`] creates a fresh Temporal client and Worker for every Lambda invocation. The
//! Worker polls until the invocation enters its reserved shutdown window, drains gracefully, runs
//! shutdown hooks, and then returns control to the Lambda runtime.

use std::{
    env,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};

use lambda_runtime::{LambdaEvent, service_fn};
use temporalio_client::{
    Client, ClientOptions, ConnectionOptions,
    envconfig::{ConfigError, DataSource, LoadClientConfigProfileOptions},
    errors::ClientConnectError,
};
use temporalio_common::worker::{
    VersioningBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion,
};
use temporalio_sdk::{
    Runtime, Worker, WorkerCreateError, WorkerOptions, WorkerRunError,
    runtime::{PollerBehavior, TunerHolder, WorkerTuner},
};
use tokio::time::{Instant, sleep, sleep_until, timeout};

const DEFAULT_CONFIG_FILE: &str = "temporal.toml";
const ENV_CONFIG_FILE: &str = "TEMPORAL_CONFIG_FILE";
const ENV_LAMBDA_TASK_ROOT: &str = "LAMBDA_TASK_ROOT";
const ENV_TASK_QUEUE: &str = "TEMPORAL_TASK_QUEUE";
const MINIMUM_WORK_TIME: Duration = Duration::from_secs(1);
const LOW_WORK_TIME_WARNING: Duration = Duration::from_secs(5);

type HookFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
type ShutdownHook = Arc<dyn Fn(Duration) -> HookFuture + Send + Sync>;

/// Lambda-oriented Worker limits applied by [`LambdaWorkerBuilder`].
///
/// These settings are distinct from [`WorkerOptions`] so callers can explicitly choose Lambda
/// defaults or replace them without the integration guessing whether an SDK default was intentional.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LambdaWorkerDefaults {
    /// Maximum concurrent Workflow Tasks.
    pub workflow_slots: usize,
    /// Maximum concurrent Activities.
    pub activity_slots: usize,
    /// Maximum concurrent Local Activities.
    pub local_activity_slots: usize,
    /// Maximum concurrent Nexus Tasks. Rust SDK Nexus registration is not yet exposed, but the
    /// slot supplier is configured for forward compatibility.
    pub nexus_slots: usize,
    /// Maximum concurrent Workflow Task polls.
    pub workflow_task_pollers: usize,
    /// Maximum concurrent Activity Task polls.
    pub activity_task_pollers: usize,
    /// Maximum concurrent Nexus Task polls.
    pub nexus_task_pollers: usize,
    /// Maximum number of cached Workflows.
    pub max_cached_workflows: usize,
    /// Time allowed for graceful Worker shutdown.
    pub graceful_shutdown_period: Duration,
    /// Time reserved after Worker shutdown for hooks and final cleanup.
    pub shutdown_hook_buffer: Duration,
}

impl Default for LambdaWorkerDefaults {
    fn default() -> Self {
        Self {
            workflow_slots: 10,
            activity_slots: 2,
            local_activity_slots: 2,
            nexus_slots: 5,
            workflow_task_pollers: 2,
            activity_task_pollers: 1,
            nexus_task_pollers: 1,
            max_cached_workflows: 30,
            graceful_shutdown_period: Duration::from_secs(5),
            shutdown_hook_buffer: Duration::from_secs(2),
        }
    }
}

/// Errors produced while configuring or running a Lambda Worker.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LambdaWorkerError {
    /// The Worker Deployment Version is incomplete.
    #[error("worker deployment name and build ID must both be non-empty")]
    InvalidDeploymentVersion,
    /// No task queue was configured.
    #[error("task queue is required: set WorkerOptions.task_queue or {ENV_TASK_QUEUE}")]
    MissingTaskQueue,
    /// A Lambda Worker limit is invalid.
    #[error("invalid Lambda Worker configuration: {0}")]
    InvalidConfiguration(String),
    /// Client environment or file configuration could not be loaded.
    #[error("failed to load Temporal client configuration: {0}")]
    ClientConfiguration(#[from] ConfigError),
    /// The Temporal SDK runtime could not be created.
    #[error("failed to create Temporal runtime: {0}")]
    Runtime(#[source] anyhow::Error),
    /// The Temporal client could not connect.
    #[error("failed to connect Temporal client: {0}")]
    ClientConnect(#[from] ClientConnectError),
    /// The Temporal client did not connect before the Worker shutdown window began.
    #[error("Temporal client did not connect within the {0:?} work budget")]
    ClientConnectTimedOut(Duration),
    /// The Temporal Worker could not be created.
    #[error("failed to create Temporal Worker: {0}")]
    WorkerCreate(#[from] WorkerCreateError),
    /// The Temporal Worker stopped with an error.
    #[error("Temporal Worker failed: {0}")]
    WorkerRun(#[from] WorkerRunError),
    /// Too little invocation time remains to start a Worker.
    #[error(
        "insufficient Lambda invocation time: {remaining:?} remaining with a {shutdown_buffer:?} shutdown buffer"
    )]
    InsufficientTime {
        /// Time remaining in the invocation.
        remaining: Duration,
        /// Time reserved for shutdown.
        shutdown_buffer: Duration,
    },
    /// The Worker did not finish before its graceful shutdown allowance elapsed.
    #[error("Temporal Worker did not stop within {0:?}")]
    ShutdownTimedOut(Duration),
}

/// Builder for [`LambdaWorker`].
pub struct LambdaWorkerBuilder {
    version: WorkerDeploymentVersion,
    worker_options: WorkerOptions,
    connection_options: Option<ConnectionOptions>,
    client_options: Option<ClientOptions>,
    runtime: Option<Arc<Runtime>>,
    defaults: LambdaWorkerDefaults,
    custom_tuner: Option<Arc<dyn WorkerTuner + Send + Sync>>,
    default_versioning_behavior: VersioningBehavior,
    shutdown_hooks: Vec<ShutdownHook>,
}

impl LambdaWorkerBuilder {
    /// Use explicit connection and namespace-bound client options instead of environment loading.
    pub fn client_options(
        mut self,
        connection_options: ConnectionOptions,
        client_options: ClientOptions,
    ) -> Self {
        self.connection_options = Some(connection_options);
        self.client_options = Some(client_options);
        self
    }

    /// Use an already-created Temporal SDK runtime.
    pub fn runtime(mut self, runtime: Arc<Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Replace the Lambda-oriented limits and shutdown timings.
    pub fn lambda_defaults(mut self, defaults: LambdaWorkerDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// Explicitly use a custom Worker tuner instead of the Lambda fixed-size tuner.
    pub fn worker_tuner(mut self, tuner: Arc<dyn WorkerTuner + Send + Sync>) -> Self {
        self.custom_tuner = Some(tuner);
        self
    }

    /// Set the default versioning behavior for Workflows without a registration-time behavior.
    ///
    /// The default is [`VersioningBehavior::Pinned`]. `Unspecified` is rejected.
    pub fn default_versioning_behavior(mut self, behavior: VersioningBehavior) -> Self {
        self.default_versioning_behavior = behavior;
        self
    }

    /// Add a hook that runs after the invocation's Worker has stopped.
    ///
    /// Hooks run in registration order. Each receives the invocation time remaining when it starts.
    /// Hook failures are logged and do not prevent later hooks from running.
    pub fn shutdown_hook<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(Duration) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.shutdown_hooks
            .push(Arc::new(move |remaining| Box::pin(hook(remaining))));
        self
    }

    /// Validate options and build a reusable Lambda handler.
    ///
    /// When no client options were supplied, configuration is loaded from `temporal.toml` and
    /// environment variables. This method must run inside a Tokio runtime unless [`Self::runtime`]
    /// was used.
    pub fn build(mut self) -> Result<LambdaWorker, LambdaWorkerError> {
        validate_version(&self.version)?;
        validate_defaults(&self.defaults)?;
        if self.default_versioning_behavior == VersioningBehavior::Unspecified {
            return Err(LambdaWorkerError::InvalidConfiguration(
                "default versioning behavior cannot be Unspecified".to_owned(),
            ));
        }

        self.worker_options.task_queue =
            resolve_task_queue(&self.worker_options.task_queue, |name| env::var(name).ok())
                .ok_or(LambdaWorkerError::MissingTaskQueue)?;

        apply_worker_configuration(
            &mut self.worker_options,
            &self.version,
            &self.defaults,
            self.custom_tuner,
            self.default_versioning_behavior,
        );

        let (connection_options, client_options) =
            match (self.connection_options, self.client_options) {
                (Some(connection), Some(client)) => (connection, client),
                (None, None) => load_client_options()?,
                _ => unreachable!("client_options sets both option types"),
            };
        let runtime = match self.runtime {
            Some(runtime) => runtime,
            None => Arc::new(
                Runtime::new_assume_tokio(Default::default())
                    .map_err(LambdaWorkerError::Runtime)?,
            ),
        };
        let shutdown_buffer = self
            .defaults
            .graceful_shutdown_period
            .saturating_add(self.defaults.shutdown_hook_buffer);

        Ok(LambdaWorker {
            inner: Arc::new(LambdaWorkerInner {
                connection_options,
                client_options,
                worker_options: self.worker_options,
                runtime,
                shutdown_buffer,
                shutdown_hooks: self.shutdown_hooks,
            }),
        })
    }
}

/// A reusable AWS Lambda handler that runs one Temporal Worker per invocation.
#[derive(Clone)]
pub struct LambdaWorker {
    inner: Arc<LambdaWorkerInner>,
}

struct LambdaWorkerInner {
    connection_options: ConnectionOptions,
    client_options: ClientOptions,
    worker_options: WorkerOptions,
    runtime: Arc<Runtime>,
    shutdown_buffer: Duration,
    shutdown_hooks: Vec<ShutdownHook>,
}

impl LambdaWorker {
    /// Start building a Lambda Worker around existing SDK Worker registrations.
    pub fn builder(
        version: WorkerDeploymentVersion,
        worker_options: WorkerOptions,
    ) -> LambdaWorkerBuilder {
        LambdaWorkerBuilder {
            version,
            worker_options,
            connection_options: None,
            client_options: None,
            runtime: None,
            defaults: LambdaWorkerDefaults::default(),
            custom_tuner: None,
            default_versioning_behavior: VersioningBehavior::Pinned,
            shutdown_hooks: Vec::new(),
        }
    }

    /// Handle one Lambda invocation.
    ///
    /// The event payload is ignored; the invocation exists to give the Worker a bounded polling
    /// window and an invocation-specific identity.
    pub async fn handle<T>(&self, event: LambdaEvent<T>) -> Result<(), LambdaWorkerError> {
        let deadline = event.context.deadline();
        let initial_remaining = remaining_until(deadline);
        let work_time = initial_remaining
            .checked_sub(self.inner.shutdown_buffer)
            .ok_or(LambdaWorkerError::InsufficientTime {
                remaining: initial_remaining,
                shutdown_buffer: self.inner.shutdown_buffer,
            })?;
        if work_time <= MINIMUM_WORK_TIME {
            return Err(LambdaWorkerError::InsufficientTime {
                remaining: initial_remaining,
                shutdown_buffer: self.inner.shutdown_buffer,
            });
        }
        if work_time < LOW_WORK_TIME_WARNING {
            tracing::warn!(
                ?work_time,
                shutdown_buffer = ?self.inner.shutdown_buffer,
                "Lambda invocation has little time available for Temporal Worker polling"
            );
        }

        let shutdown_at = deadline
            .checked_sub(self.inner.shutdown_buffer)
            .expect("the checked work-time calculation already succeeded");
        let result = self.run_worker(event.context, shutdown_at).await;
        self.run_shutdown_hooks(deadline).await;
        result
    }

    /// Run this handler using the standard AWS Lambda Rust runtime.
    pub async fn run(self) -> Result<(), lambda_runtime::Error> {
        lambda_runtime::run(service_fn(move |event: LambdaEvent<serde_json::Value>| {
            let worker = self.clone();
            async move {
                worker.handle(event).await.map_err(|error| {
                    Box::new(std::io::Error::other(error.to_string())) as lambda_runtime::Error
                })
            }
        }))
        .await
    }

    async fn run_worker(
        &self,
        context: lambda_runtime::Context,
        shutdown_at: SystemTime,
    ) -> Result<(), LambdaWorkerError> {
        let mut connection_options = self.inner.connection_options.clone();
        if connection_options.identity.is_empty()
            && self.inner.worker_options.client_identity_override.is_none()
        {
            connection_options.identity = invocation_identity(&context);
        }

        let connect_budget = remaining_until(shutdown_at);
        let client = timeout(
            connect_budget,
            Client::connect(connection_options, self.inner.client_options.clone()),
        )
        .await
        .map_err(|_| LambdaWorkerError::ClientConnectTimedOut(connect_budget))??;
        let mut worker = Worker::new(
            &self.inner.runtime,
            client,
            self.inner.worker_options.clone(),
        )?;
        let initiate_shutdown = worker.shutdown_handle();
        let work_time = remaining_until(shutdown_at);
        let worker_result = run_until_shutdown(
            worker.run(),
            sleep(work_time),
            initiate_shutdown,
            self.inner.shutdown_buffer,
        )
        .await?;
        worker_result.map_err(LambdaWorkerError::WorkerRun)
    }

    async fn run_shutdown_hooks(&self, deadline: SystemTime) {
        for hook in &self.inner.shutdown_hooks {
            let remaining = remaining_until(deadline);
            if remaining.is_zero() {
                tracing::error!("Lambda deadline reached before all shutdown hooks ran");
                break;
            }
            match timeout(remaining, hook(remaining)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::error!(%error, "Lambda Worker shutdown hook failed");
                }
                Err(_) => {
                    tracing::error!("Lambda Worker shutdown hook exceeded the invocation deadline");
                    break;
                }
            }
        }
    }
}

fn apply_worker_configuration(
    options: &mut WorkerOptions,
    version: &WorkerDeploymentVersion,
    defaults: &LambdaWorkerDefaults,
    custom_tuner: Option<Arc<dyn WorkerTuner + Send + Sync>>,
    default_versioning_behavior: VersioningBehavior,
) {
    options.tuner = custom_tuner.unwrap_or_else(|| {
        Arc::new(TunerHolder::fixed_size(
            defaults.workflow_slots,
            defaults.activity_slots,
            defaults.local_activity_slots,
            defaults.nexus_slots,
        ))
    });
    options.workflow_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(
        defaults.workflow_task_pollers,
    ));
    options.activity_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(
        defaults.activity_task_pollers,
    ));
    options.nexus_task_poller_behavior =
        Some(PollerBehavior::SimpleMaximum(defaults.nexus_task_pollers));
    options.max_cached_workflows = defaults.max_cached_workflows;
    options.graceful_shutdown_period = Some(defaults.graceful_shutdown_period);
    options.max_eager_activity_reservations_per_workflow_task = 0;
    options.deployment_options = WorkerDeploymentOptions::new(version.clone())
        .use_worker_versioning(true)
        .default_versioning_behavior(default_versioning_behavior)
        .build();
}

fn validate_version(version: &WorkerDeploymentVersion) -> Result<(), LambdaWorkerError> {
    if version.deployment_name.trim().is_empty() || version.build_id.trim().is_empty() {
        Err(LambdaWorkerError::InvalidDeploymentVersion)
    } else {
        Ok(())
    }
}

fn validate_defaults(defaults: &LambdaWorkerDefaults) -> Result<(), LambdaWorkerError> {
    if defaults.workflow_slots == 0
        || defaults.activity_slots == 0
        || defaults.local_activity_slots == 0
        || defaults.nexus_slots == 0
    {
        return Err(LambdaWorkerError::InvalidConfiguration(
            "all fixed-size tuner slot counts must be greater than zero".to_owned(),
        ));
    }
    if defaults.workflow_task_pollers < 2 {
        return Err(LambdaWorkerError::InvalidConfiguration(
            "workflow_task_pollers must be at least 2 when sticky caching is enabled".to_owned(),
        ));
    }
    if defaults.activity_task_pollers == 0 || defaults.nexus_task_pollers == 0 {
        return Err(LambdaWorkerError::InvalidConfiguration(
            "activity_task_pollers and nexus_task_pollers must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn load_client_options() -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
    let load_options =
        match resolve_config_file(|name| env::var_os(name), env::current_dir().ok().as_deref()) {
            Some(path) => LoadClientConfigProfileOptions::builder()
                .config_source(DataSource::Path(path.to_string_lossy().into_owned()))
                .build(),
            None => LoadClientConfigProfileOptions::builder()
                .disable_file(true)
                .build(),
        };
    ClientOptions::load_from_config(load_options)
}

fn resolve_config_file(
    getenv: impl Fn(&str) -> Option<std::ffi::OsString>,
    current_dir: Option<&Path>,
) -> Option<PathBuf> {
    if let Some(path) = getenv(ENV_CONFIG_FILE).filter(|path| !path.is_empty()) {
        return Some(PathBuf::from(path));
    }
    if let Some(task_root) = getenv(ENV_LAMBDA_TASK_ROOT).filter(|path| !path.is_empty()) {
        let candidate = PathBuf::from(task_root).join(DEFAULT_CONFIG_FILE);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    current_dir
        .map(|dir| dir.join(DEFAULT_CONFIG_FILE))
        .filter(|path| path.is_file())
}

fn resolve_task_queue(configured: &str, getenv: impl Fn(&str) -> Option<String>) -> Option<String> {
    if !configured.trim().is_empty() {
        return Some(configured.to_owned());
    }
    getenv(ENV_TASK_QUEUE).filter(|task_queue| !task_queue.trim().is_empty())
}

fn invocation_identity(context: &lambda_runtime::Context) -> String {
    let request_id = if context.request_id.is_empty() {
        "unknown"
    } else {
        &context.request_id
    };
    let function_arn = if context.invoked_function_arn.is_empty() {
        "unknown"
    } else {
        &context.invoked_function_arn
    };
    format!("{request_id}@{function_arn}")
}

fn remaining_until(deadline: SystemTime) -> Duration {
    deadline
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO)
}

async fn run_until_shutdown<R, D, S, E>(
    run: R,
    shutdown_delay: D,
    initiate_shutdown: S,
    graceful_shutdown_period: Duration,
) -> Result<Result<(), E>, LambdaWorkerError>
where
    R: Future<Output = Result<(), E>>,
    D: Future<Output = ()>,
    S: FnOnce(),
{
    tokio::pin!(run);
    tokio::pin!(shutdown_delay);
    tokio::select! {
        result = &mut run => Ok(result),
        () = &mut shutdown_delay => {
            initiate_shutdown();
            let shutdown_deadline = Instant::now() + graceful_shutdown_period;
            tokio::select! {
                result = &mut run => Ok(result),
                () = sleep_until(shutdown_deadline) => {
                    Err(LambdaWorkerError::ShutdownTimedOut(graceful_shutdown_period))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        ffi::OsString,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };
    use tokio::sync::{Notify, oneshot};

    fn version() -> WorkerDeploymentVersion {
        WorkerDeploymentVersion::builder()
            .deployment_name("deployment")
            .build_id("build")
            .build()
    }

    #[test]
    fn applies_lambda_worker_configuration() {
        let mut options = WorkerOptions::new("queue").build();
        let defaults = LambdaWorkerDefaults::default();
        apply_worker_configuration(
            &mut options,
            &version(),
            &defaults,
            None,
            VersioningBehavior::Pinned,
        );

        assert_eq!(options.max_cached_workflows, 30);
        assert_eq!(
            options.workflow_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(2))
        );
        assert_eq!(
            options.activity_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(1))
        );
        assert_eq!(
            options.nexus_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(1))
        );
        assert_eq!(options.max_eager_activity_reservations_per_workflow_task, 0);
        assert_eq!(
            options.graceful_shutdown_period,
            Some(Duration::from_secs(5))
        );
        assert!(options.deployment_options.use_worker_versioning);
        assert_eq!(options.deployment_options.version, version());
        assert_eq!(
            options.deployment_options.default_versioning_behavior,
            Some(VersioningBehavior::Pinned)
        );
    }

    #[test]
    fn custom_tuner_is_preserved_explicitly() {
        let custom: Arc<dyn WorkerTuner + Send + Sync> =
            Arc::new(TunerHolder::fixed_size(3, 4, 5, 6));
        let mut options = WorkerOptions::new("queue").build();
        apply_worker_configuration(
            &mut options,
            &version(),
            &LambdaWorkerDefaults::default(),
            Some(custom.clone()),
            VersioningBehavior::AutoUpgrade,
        );

        assert!(Arc::ptr_eq(&options.tuner, &custom));
        assert_eq!(
            options.deployment_options.default_versioning_behavior,
            Some(VersioningBehavior::AutoUpgrade)
        );
    }

    #[test]
    fn validates_version_and_limits() {
        let invalid_version = WorkerDeploymentVersion::builder()
            .deployment_name("")
            .build_id("build")
            .build();
        assert!(matches!(
            validate_version(&invalid_version),
            Err(LambdaWorkerError::InvalidDeploymentVersion)
        ));

        let defaults = LambdaWorkerDefaults {
            workflow_task_pollers: 1,
            ..Default::default()
        };
        assert!(matches!(
            validate_defaults(&defaults),
            Err(LambdaWorkerError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn resolves_config_file_in_lambda_order() {
        let temp = tempfile::tempdir().unwrap();
        let task_root = temp.path().join("task");
        let cwd = temp.path().join("cwd");
        std::fs::create_dir_all(&task_root).unwrap();
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::write(task_root.join(DEFAULT_CONFIG_FILE), "").unwrap();
        std::fs::write(cwd.join(DEFAULT_CONFIG_FILE), "").unwrap();

        let explicit = temp.path().join("explicit.toml");
        let resolved = resolve_config_file(
            |name| match name {
                ENV_CONFIG_FILE => Some(explicit.clone().into_os_string()),
                ENV_LAMBDA_TASK_ROOT => Some(task_root.clone().into_os_string()),
                _ => None,
            },
            Some(&cwd),
        );
        assert_eq!(resolved, Some(explicit));

        let resolved = resolve_config_file(
            |name| (name == ENV_LAMBDA_TASK_ROOT).then(|| OsString::from(task_root.as_os_str())),
            Some(&cwd),
        );
        assert_eq!(resolved, Some(task_root.join(DEFAULT_CONFIG_FILE)));

        std::fs::remove_file(task_root.join(DEFAULT_CONFIG_FILE)).unwrap();
        let resolved = resolve_config_file(
            |name| (name == ENV_LAMBDA_TASK_ROOT).then(|| OsString::from(task_root.as_os_str())),
            Some(&cwd),
        );
        assert_eq!(resolved, Some(cwd.join(DEFAULT_CONFIG_FILE)));
    }

    #[test]
    fn explicit_task_queue_wins_and_environment_is_fallback() {
        assert_eq!(
            resolve_task_queue("configured", |_| Some("environment".to_owned())),
            Some("configured".to_owned())
        );
        assert_eq!(
            resolve_task_queue("  ", |_| Some("environment".to_owned())),
            Some("environment".to_owned())
        );
        assert_eq!(resolve_task_queue("", |_| None), None);
    }

    #[test]
    fn builds_invocation_identity_from_lambda_context() {
        let mut context = lambda_runtime::Context::default();
        context.request_id = "request-123".to_owned();
        context.invoked_function_arn = "arn:aws:lambda:region:account:function:worker".to_owned();
        assert_eq!(
            invocation_identity(&context),
            "request-123@arn:aws:lambda:region:account:function:worker"
        );

        let context = lambda_runtime::Context::default();
        assert_eq!(invocation_identity(&context), "unknown@unknown");
    }

    #[tokio::test]
    async fn lifecycle_initiates_shutdown_and_waits_for_run() {
        let (trigger_tx, trigger_rx) = oneshot::channel();
        let stopped = Arc::new(Notify::new());
        let stopped_for_run = stopped.clone();
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let shutdown_called_for_closure = shutdown_called.clone();

        let run = async move {
            stopped_for_run.notified().await;
            Result::<(), ()>::Ok(())
        };
        let result = run_until_shutdown(
            run,
            async move {
                trigger_rx.await.unwrap();
            },
            move || {
                shutdown_called_for_closure.store(true, Ordering::SeqCst);
                stopped.notify_one();
            },
            Duration::from_secs(5),
        );
        trigger_tx.send(()).unwrap();

        assert_eq!(result.await.unwrap(), Ok(()));
        assert!(shutdown_called.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_times_out_when_run_does_not_stop() {
        let (trigger_tx, trigger_rx) = oneshot::channel();
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let shutdown_called_for_closure = shutdown_called.clone();
        let never = Arc::new(Notify::new());
        let run = async move {
            never.notified().await;
            Result::<(), ()>::Ok(())
        };

        let result = run_until_shutdown(
            run,
            async move {
                trigger_rx.await.unwrap();
            },
            move || shutdown_called_for_closure.store(true, Ordering::SeqCst),
            Duration::from_secs(5),
        );
        trigger_tx.send(()).unwrap();

        assert!(matches!(
            result.await,
            Err(LambdaWorkerError::ShutdownTimedOut(duration))
                if duration == Duration::from_secs(5)
        ));
        assert!(shutdown_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_hooks_run_in_order_and_continue_after_errors() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let first_order = order.clone();
        let second_order = order.clone();
        let worker = LambdaWorker {
            inner: Arc::new(LambdaWorkerInner {
                connection_options: ConnectionOptions::new(
                    temporalio_client::Url::parse("http://localhost:7233").unwrap(),
                )
                .build(),
                client_options: ClientOptions::new("default").build(),
                worker_options: WorkerOptions::new("queue").build(),
                runtime: Arc::new(Runtime::new_assume_tokio(Default::default()).unwrap()),
                shutdown_buffer: Duration::from_secs(7),
                shutdown_hooks: vec![
                    Arc::new(move |_| {
                        first_order.lock().unwrap().push("first");
                        Box::pin(async { anyhow::bail!("expected") })
                    }),
                    Arc::new(move |_| {
                        second_order.lock().unwrap().push("second");
                        Box::pin(async { Ok(()) })
                    }),
                ],
            }),
        };

        worker
            .run_shutdown_hooks(SystemTime::now() + Duration::from_secs(1))
            .await;
        assert_eq!(*order.lock().unwrap(), vec!["first", "second"]);
    }
}
