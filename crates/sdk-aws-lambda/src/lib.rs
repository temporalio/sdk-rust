#![warn(missing_docs)]

//! AWS Lambda support for the Temporal Rust SDK.
//!
//! A [`LambdaWorker`] creates a fresh Temporal client and Worker for every Lambda invocation. The
//! Worker polls until the invocation enters its reserved shutdown window, drains gracefully, runs
//! shutdown hooks, and then returns control to the Lambda runtime.

#[cfg(feature = "otel")]
pub mod otel;

use std::{
    env,
    error::Error as StdError,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use lambda_runtime::{LambdaEvent, service_fn};
use temporalio_client::{
    Client, ClientOptions, ConnectionOptions,
    envconfig::{ConfigError, DataSource, LoadClientConfigProfileOptions},
};
use temporalio_common::worker::{
    VersioningBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion,
};
use temporalio_sdk::{
    Runtime, Worker, WorkerOptions,
    runtime::{
        PollerBehavior,
        worker_tuner::{FixedSizeSlotSupplier, TunerHolder, WorkerTuner},
    },
};
use tokio::{
    sync::{Notify, Semaphore},
    time::{Instant, sleep_until, timeout_at},
};

const DEFAULT_CONFIG_FILE: &str = "temporal.toml";
const ENV_CONFIG_FILE: &str = "TEMPORAL_CONFIG_FILE";
const ENV_LAMBDA_TASK_ROOT: &str = "LAMBDA_TASK_ROOT";
const ENV_TASK_QUEUE: &str = "TEMPORAL_TASK_QUEUE";
const MINIMUM_WORK_TIME: Duration = Duration::from_secs(1);
const LOW_WORK_TIME_WARNING: Duration = Duration::from_secs(5);
const RESPONSE_BUFFER: Duration = Duration::from_millis(100);

type HookFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
type ShutdownHook = Arc<dyn Fn(Duration) -> HookFuture + Send + Sync>;

/// Lambda-oriented Worker limits applied by [`LambdaWorkerBuilder`].
///
/// These settings are distinct from [`WorkerOptions`] so callers can explicitly choose Lambda
/// defaults or replace them without the integration guessing whether an SDK default was intentional.
/// Pass customized limits and shutdown timings to [`LambdaWorkerBuilder::lambda_options`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LambdaWorkerOptions {
    /// Maximum concurrent Workflow Tasks.
    pub workflow_slots: usize,
    /// Maximum concurrent Activities.
    pub activity_slots: usize,
    /// Maximum concurrent Local Activities.
    pub local_activity_slots: usize,
    /// Maximum concurrent Workflow Task polls.
    pub workflow_task_pollers: usize,
    /// Maximum concurrent Activity Task polls.
    pub activity_task_pollers: usize,
    /// Maximum number of cached Workflows.
    pub max_cached_workflows: usize,
    /// Time allowed for graceful Worker shutdown.
    pub graceful_shutdown_period: Duration,
    /// Additional time for cancelled Activities and Worker finalization after the graceful period.
    pub worker_shutdown_buffer: Duration,
    /// Time reserved after Worker shutdown for hooks and final cleanup.
    pub shutdown_hook_buffer: Duration,
}

impl Default for LambdaWorkerOptions {
    fn default() -> Self {
        Self {
            workflow_slots: 10,
            activity_slots: 2,
            local_activity_slots: 2,
            workflow_task_pollers: 2,
            activity_task_pollers: 1,
            max_cached_workflows: 30,
            graceful_shutdown_period: Duration::from_secs(5),
            worker_shutdown_buffer: Duration::from_secs(1),
            shutdown_hook_buffer: Duration::from_secs(2),
        }
    }
}

/// An error configuring a Lambda Worker. The underlying cause is available through
/// [`std::error::Error::source`].
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct LambdaWorkerBuildError(#[source] Box<dyn StdError>);

impl LambdaWorkerBuildError {
    fn new(source: impl Into<Box<dyn StdError>>) -> Self {
        Self(source.into())
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            message.into(),
        ))
    }
}

/// An error handling a Lambda invocation. The underlying cause is available through
/// [`std::error::Error::source`].
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct LambdaInvocationError {
    #[source]
    source: Box<dyn StdError + Send + Sync>,
    requires_restart: bool,
}

impl LambdaInvocationError {
    fn new(source: impl Into<Box<dyn StdError + Send + Sync>>, requires_restart: bool) -> Self {
        Self {
            source: source.into(),
            requires_restart,
        }
    }

    fn interrupted() -> Self {
        Self::new(
            anyhow::anyhow!(
                "this Lambda Worker cannot be reused after an interrupted or failed Worker run"
            ),
            true,
        )
    }

    /// Whether this failure requires recycling the Lambda execution environment.
    ///
    /// When true, do not reuse this handler or construct another in the same process, since
    /// background Worker resources or Activity work may still be running. When false, this
    /// failure alone does not prevent a subsequent invocation.
    pub fn requires_restart(&self) -> bool {
        self.requires_restart
    }
}

/// Builder for [`LambdaWorker`].
pub struct LambdaWorkerBuilder {
    version: WorkerDeploymentVersion,
    worker_options: WorkerOptions,
    connection_options: Option<ConnectionOptions>,
    client_options: Option<ClientOptions>,
    runtime: Option<Arc<Runtime>>,
    lambda_options: LambdaWorkerOptions,
    custom_tuner: Option<WorkerTuner>,
    default_versioning_behavior: VersioningBehavior,
    shutdown_hooks: Vec<ShutdownHook>,
    #[cfg(feature = "otel")]
    open_telemetry: Option<otel::OpenTelemetryOptions>,
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

    /// Configure OTLP metrics and tracing for a Lambda OpenTelemetry Collector extension.
    ///
    /// This creates the SDK runtime, so it cannot be combined with [`Self::runtime`]. Pending
    /// telemetry is force-flushed after every invocation without shutting down the providers,
    /// allowing them to remain available for warm starts. Defaults target a local OTLP gRPC
    /// receiver; the collector's configuration controls export to CloudWatch and AWS X-Ray.
    #[cfg(feature = "otel")]
    pub fn open_telemetry(mut self, options: otel::OpenTelemetryOptions) -> Self {
        self.open_telemetry = Some(options);
        self
    }

    /// Set the Lambda-oriented limits and shutdown timings.
    pub fn lambda_options(mut self, options: LambdaWorkerOptions) -> Self {
        self.lambda_options = options;
        self
    }

    /// Explicitly use a custom Worker tuner instead of the Lambda fixed-size tuner.
    pub fn worker_tuner(mut self, tuner: impl Into<WorkerTuner>) -> Self {
        self.custom_tuner = Some(tuner.into());
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
    /// Hooks run in registration order. Each receives the shared hook budget remaining when it starts.
    /// Hook failures are logged and do not prevent later hooks from running. The automatic
    /// telemetry flush runs first. All hooks share [`LambdaWorkerOptions::shutdown_hook_buffer`].
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
    pub fn build(mut self) -> Result<LambdaWorker, LambdaWorkerBuildError> {
        validate_version(&self.version)?;
        validate_lambda_options(&self.lambda_options)?;
        if self.default_versioning_behavior == VersioningBehavior::Unspecified {
            return Err(LambdaWorkerBuildError::invalid(
                "default versioning behavior cannot be Unspecified",
            ));
        }

        self.worker_options.task_queue =
            resolve_task_queue(&self.worker_options.task_queue, |name| env::var(name).ok())
                .ok_or_else(|| {
                    LambdaWorkerBuildError::invalid(format!(
                        "task queue is required: set WorkerOptions.task_queue or {ENV_TASK_QUEUE}"
                    ))
                })?;

        apply_worker_configuration(
            &mut self.worker_options,
            &self.version,
            &self.lambda_options,
            self.custom_tuner,
            self.default_versioning_behavior,
        );

        #[allow(unused_mut)]
        let (connection_options, mut client_options) =
            match (self.connection_options, self.client_options) {
                (Some(connection), Some(client)) => (connection, client),
                (None, None) => load_client_options().map_err(LambdaWorkerBuildError::new)?,
                _ => unreachable!("client_options sets both option types"),
            };
        #[cfg(feature = "otel")]
        if let Some(options) = self.open_telemetry {
            if self.runtime.is_some() {
                return Err(LambdaWorkerBuildError::invalid(
                    "runtime and OpenTelemetry options cannot both be supplied",
                ));
            }
            otel::configure(
                options,
                &mut client_options,
                &mut self.worker_options,
                &mut self.shutdown_hooks,
                &mut self.runtime,
            )
            .map_err(LambdaWorkerBuildError::new)?;
        }
        let runtime = match self.runtime {
            Some(runtime) => runtime,
            None => Arc::new(
                Runtime::from_current_tokio(Default::default())
                    .map_err(LambdaWorkerBuildError::new)?,
            ),
        };
        let drain_budget = self
            .lambda_options
            .graceful_shutdown_period
            .saturating_add(self.lambda_options.worker_shutdown_buffer);
        let shutdown_buffer = drain_budget
            .saturating_add(self.lambda_options.shutdown_hook_buffer)
            .saturating_add(RESPONSE_BUFFER);

        Ok(LambdaWorker {
            inner: Arc::new(LambdaWorkerInner {
                connection_options,
                client_options,
                worker_options: self.worker_options,
                runtime,
                shutdown_buffer,
                drain_budget,
                hook_budget: self.lambda_options.shutdown_hook_buffer,
                shutdown_hooks: self.shutdown_hooks,
                invocation_gate: Semaphore::new(1),
                healthy: AtomicBool::new(true),
                interrupted: Notify::new(),
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
    drain_budget: Duration,
    hook_budget: Duration,
    shutdown_hooks: Vec<ShutdownHook>,
    invocation_gate: Semaphore,
    healthy: AtomicBool,
    interrupted: Notify,
}

struct InvocationGuard<'a> {
    healthy: &'a AtomicBool,
    interrupted: &'a Notify,
    completed: bool,
}

impl Drop for InvocationGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.healthy.store(false, Ordering::Release);
            self.interrupted.notify_one();
        }
    }
}

struct ShutdownGuard<S: FnOnce()>(Option<S>);

impl<S: FnOnce()> Drop for ShutdownGuard<S> {
    fn drop(&mut self) {
        if let Some(shutdown) = self.0.take() {
            shutdown();
        }
    }
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
            lambda_options: LambdaWorkerOptions::default(),
            custom_tuner: None,
            default_versioning_behavior: VersioningBehavior::Pinned,
            shutdown_hooks: Vec::new(),
            #[cfg(feature = "otel")]
            open_telemetry: None,
        }
    }

    /// Handle one Lambda invocation.
    ///
    /// Typically, use [`Self::run`] to let the AWS Lambda runtime drive invocations and exit the
    /// runtime loop if this handler can no longer be reused safely.
    ///
    /// The event payload is ignored; the invocation exists to give the Worker a bounded polling
    /// window and an invocation-specific identity. Calls must be sequential. Cancelling this
    /// future or exceeding the Worker drain budget makes this handler unusable for later calls,
    /// since background Activity work may still be running. Recycle the Lambda environment in
    /// that case rather than constructing another handler in the same process. For returned
    /// errors, use [`LambdaInvocationError::requires_restart`] to determine whether to recycle.
    pub async fn handle<T>(&self, event: LambdaEvent<T>) -> Result<(), LambdaInvocationError> {
        if !self.inner.healthy.load(Ordering::Acquire) {
            return Err(LambdaInvocationError::interrupted());
        }
        let now = Instant::now();
        let deadline = event.context.deadline();
        let initial_remaining = deadline
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        let work_time = initial_remaining.saturating_sub(self.inner.shutdown_buffer);
        if work_time <= MINIMUM_WORK_TIME {
            return Err(LambdaInvocationError::new(
                anyhow::anyhow!(
                    "insufficient Lambda invocation time: {initial_remaining:?} remaining with a {:?} shutdown buffer",
                    self.inner.shutdown_buffer,
                ),
                false,
            ));
        }
        if work_time < LOW_WORK_TIME_WARNING {
            tracing::warn!(
                ?work_time,
                shutdown_buffer = ?self.inner.shutdown_buffer,
                "Lambda invocation has little time available for Temporal Worker polling"
            );
        }
        let _permit = self.inner.invocation_gate.try_acquire().map_err(|_| {
            LambdaInvocationError::new(
                anyhow::anyhow!("a Lambda invocation is already running on this handler"),
                false,
            )
        })?;
        if !self.inner.healthy.load(Ordering::Acquire) {
            return Err(LambdaInvocationError::interrupted());
        }
        let mut guard = InvocationGuard {
            healthy: &self.inner.healthy,
            interrupted: &self.inner.interrupted,
            completed: false,
        };
        // Use one monotonic deadline so wall-clock adjustments cannot extend polling into cleanup.
        let invocation_deadline = now + initial_remaining;
        let shutdown_at = invocation_deadline - self.inner.shutdown_buffer;
        let drain_deadline = shutdown_at + self.inner.drain_budget;
        let result = self
            .run_worker(event.context, shutdown_at, drain_deadline)
            .await;
        self.run_shutdown_hooks(
            (invocation_deadline - RESPONSE_BUFFER).min(Instant::now() + self.inner.hook_budget),
        )
        .await;
        guard.completed = result
            .as_ref()
            .err()
            .is_none_or(|error| !error.requires_restart());
        result
    }

    /// Run this handler using the standard AWS Lambda Rust runtime.
    pub async fn run(self) -> Result<(), lambda_runtime::Error> {
        let handler = self.clone();
        let runtime =
            lambda_runtime::run(service_fn(move |event: LambdaEvent<serde_json::Value>| {
                let worker = handler.clone();
                async move {
                    worker
                        .handle(event)
                        .await
                        .map_err(lambda_runtime::Error::from)
                }
            }));
        // An interrupted Worker can retain background Activities. Ending the runtime loop lets
        // the process exit instead of letting AWS reuse an unsafe execution environment.
        tokio::select! {
            result = runtime => result,
            () = self.inner.interrupted.notified() => Err(LambdaInvocationError::interrupted().into()),
        }
    }

    async fn run_worker(
        &self,
        context: lambda_runtime::Context,
        shutdown_at: Instant,
        drain_deadline: Instant,
    ) -> Result<(), LambdaInvocationError> {
        let mut connection_options = self.inner.connection_options.clone();
        if connection_options.identity.is_empty()
            && self.inner.worker_options.client_identity_override.is_none()
        {
            connection_options.identity = invocation_identity(&context);
        }

        let connect_budget = shutdown_at.saturating_duration_since(Instant::now());
        let client = timeout_at(
            shutdown_at,
            Client::connect(connection_options, self.inner.client_options.clone()),
        )
        .await
        .map_err(|error| {
            LambdaInvocationError::new(
                anyhow::Error::new(error).context(format!(
                    "Temporal client did not connect within the {connect_budget:?} work budget"
                )),
                false,
            )
        })?
        .map_err(|error| LambdaInvocationError::new(error, false))?;
        let mut worker = Worker::new(
            &self.inner.runtime,
            client,
            self.inner.worker_options.clone(),
        )
        .map_err(|error| LambdaInvocationError::new(error, false))?;
        let mut shutdown = ShutdownGuard(Some(worker.shutdown_handle()));
        let worker_result = {
            let run = worker.run();
            tokio::pin!(run);
            tokio::select! {
                result = &mut run => result,
                () = sleep_until(shutdown_at) => {
                    shutdown.0.take().expect("shutdown has not been initiated")();
                    timeout_at(drain_deadline, &mut run)
                        .await
                        .map_err(|error| {
                            LambdaInvocationError::new(
                                anyhow::Error::new(error).context(format!(
                                    "Temporal Worker did not drain and finalize within {:?}",
                                    self.inner.drain_budget,
                                )),
                                true,
                            )
                        })?
                }
            }
        };
        if let Some(initiate_shutdown) = shutdown.0.take() {
            initiate_shutdown();
        }
        if let Err(error) = &worker_result {
            tracing::error!(
                ?error,
                "Temporal Worker failed; attempting shutdown finalization"
            );
        }
        // Core retains the heartbeat registration after polling stops; finalize before hooks or
        // a warm invocation, without letting cleanup consume the hooks' reserved time.
        match timeout_at(drain_deadline, worker.finalize_shutdown()).await {
            Ok(result) => result.map_err(|error| LambdaInvocationError::new(error, true))?,
            Err(error) => {
                let source = worker_result
                    .err()
                    .map_or_else(|| anyhow::Error::new(error), anyhow::Error::new);
                return Err(LambdaInvocationError::new(
                    source.context(format!(
                        "Temporal Worker did not drain and finalize within {:?}",
                        self.inner.drain_budget,
                    )),
                    true,
                ));
            }
        }
        worker_result.map_err(|error| LambdaInvocationError::new(error, true))
    }

    async fn run_shutdown_hooks(&self, deadline: Instant) {
        for hook in &self.inner.shutdown_hooks {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                tracing::error!(
                    "Lambda Worker shutdown hook budget exhausted before all hooks ran"
                );
                break;
            }
            match timeout_at(deadline, hook(remaining)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::error!(%error, "Lambda Worker shutdown hook failed");
                }
                Err(_) => {
                    tracing::error!("Lambda Worker shutdown hook exceeded the shared hook budget");
                    break;
                }
            }
        }
    }
}

fn apply_worker_configuration(
    options: &mut WorkerOptions,
    version: &WorkerDeploymentVersion,
    lambda_options: &LambdaWorkerOptions,
    custom_tuner: Option<WorkerTuner>,
    default_versioning_behavior: VersioningBehavior,
) {
    options.tuner = custom_tuner.unwrap_or_else(|| {
        TunerHolder::builder()
            .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(lambda_options.workflow_slots))
            .activity_task_slot_supplier(FixedSizeSlotSupplier::new(lambda_options.activity_slots))
            .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(
                lambda_options.local_activity_slots,
            ))
            // The tuner requires every supplier even though Rust workers do not poll Nexus tasks.
            .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(1))
            .build()
            .into()
    });
    options.workflow_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(
        lambda_options.workflow_task_pollers,
    ));
    options.activity_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(
        lambda_options.activity_task_pollers,
    ));
    options.max_cached_workflows = lambda_options.max_cached_workflows;
    options.graceful_shutdown_period = Some(lambda_options.graceful_shutdown_period);
    options.max_eager_activity_reservations_per_workflow_task = 0;
    options.deployment_options = WorkerDeploymentOptions::new(version.clone())
        .use_worker_versioning(true)
        .default_versioning_behavior(default_versioning_behavior)
        .build();
}

fn validate_version(version: &WorkerDeploymentVersion) -> Result<(), LambdaWorkerBuildError> {
    if version.deployment_name.trim().is_empty() || version.build_id.trim().is_empty() {
        Err(LambdaWorkerBuildError::invalid(
            "worker deployment name and build ID must both be non-empty",
        ))
    } else {
        Ok(())
    }
}

fn validate_lambda_options(options: &LambdaWorkerOptions) -> Result<(), LambdaWorkerBuildError> {
    if options.workflow_slots == 0
        || options.activity_slots == 0
        || options.local_activity_slots == 0
    {
        return Err(LambdaWorkerBuildError::invalid(
            "all fixed-size tuner slot counts must be greater than zero",
        ));
    }
    if options.workflow_task_pollers < 2 {
        return Err(LambdaWorkerBuildError::invalid(
            "workflow_task_pollers must be at least 2 when sticky caching is enabled",
        ));
    }
    if options.activity_task_pollers == 0 {
        return Err(LambdaWorkerBuildError::invalid(
            "activity_task_pollers must be greater than zero",
        ));
    }
    if options.worker_shutdown_buffer.is_zero() || options.shutdown_hook_buffer.is_zero() {
        return Err(LambdaWorkerBuildError::invalid(
            "worker_shutdown_buffer and shutdown_hook_buffer must be greater than zero",
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        ffi::OsString,
        sync::{Mutex, atomic::AtomicBool},
    };
    use temporalio_client::errors::ClientConnectError;
    use temporalio_sdk::{RuntimeError, runtime::worker_tuner::SlotSupplier};
    use tokio::sync::Notify;

    fn version() -> WorkerDeploymentVersion {
        WorkerDeploymentVersion::builder()
            .deployment_name("deployment")
            .build_id("build")
            .build()
    }

    #[test]
    fn applies_lambda_worker_configuration() {
        let mut options = WorkerOptions::new("queue").build();
        let lambda_options = LambdaWorkerOptions::default();
        apply_worker_configuration(
            &mut options,
            &version(),
            &lambda_options,
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
        let custom: WorkerTuner = TunerHolder::builder()
            .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(3))
            .activity_task_slot_supplier(FixedSizeSlotSupplier::new(4))
            .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(5))
            .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(1))
            .build()
            .into();
        let mut options = WorkerOptions::new("queue").build();
        apply_worker_configuration(
            &mut options,
            &version(),
            &LambdaWorkerOptions::default(),
            Some(custom.clone()),
            VersioningBehavior::AutoUpgrade,
        );

        let WorkerTuner::TunerHolder(tuner) = options.tuner else {
            panic!("custom tuner was replaced");
        };
        assert!(
            matches!(tuner.workflow_task_slot_supplier, SlotSupplier::FixedSize(supplier) if supplier.num_slots == 3)
        );
        assert!(
            matches!(tuner.activity_task_slot_supplier, SlotSupplier::FixedSize(supplier) if supplier.num_slots == 4)
        );
        assert!(
            matches!(tuner.local_activity_task_slot_supplier, SlotSupplier::FixedSize(supplier) if supplier.num_slots == 5)
        );
        assert_eq!(
            options.deployment_options.default_versioning_behavior,
            Some(VersioningBehavior::AutoUpgrade)
        );
    }

    #[cfg(feature = "otel")]
    #[tokio::test]
    async fn rejects_open_telemetry_with_caller_owned_runtime() {
        let runtime = Arc::new(Runtime::from_current_tokio(Default::default()).unwrap());
        let result = LambdaWorker::builder(version(), WorkerOptions::new("queue").build())
            .client_options(
                ConnectionOptions::new(
                    temporalio_client::Url::parse("http://localhost:7233").unwrap(),
                )
                .build(),
                ClientOptions::new("default").build(),
            )
            .runtime(runtime)
            .open_telemetry(otel::OpenTelemetryOptions::default())
            .build();

        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("runtime and OpenTelemetry")
        );
    }

    #[test]
    fn validates_version_and_limits() {
        let invalid_version = WorkerDeploymentVersion::builder()
            .deployment_name("")
            .build_id("build")
            .build();
        assert!(
            validate_version(&invalid_version)
                .unwrap_err()
                .to_string()
                .contains("deployment name and build ID")
        );

        let lambda_options = LambdaWorkerOptions {
            workflow_task_pollers: 1,
            ..Default::default()
        };
        assert!(
            validate_lambda_options(&lambda_options)
                .unwrap_err()
                .to_string()
                .contains("workflow_task_pollers must be at least 2")
        );
    }

    #[test]
    fn build_error_preserves_runtime_cause() {
        let error = LambdaWorker::builder(version(), WorkerOptions::new("queue").build())
            .client_options(
                ConnectionOptions::new(
                    temporalio_client::Url::parse("http://localhost:7233").unwrap(),
                )
                .build(),
                ClientOptions::new("default").build(),
            )
            .build()
            .err()
            .unwrap();
        assert!(matches!(
            error.source().unwrap().downcast_ref::<RuntimeError>(),
            Some(RuntimeError::NoCurrentTokioRuntime)
        ));
    }

    #[tokio::test]
    async fn startup_failure_preserves_cause_and_allows_reuse() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let worker = LambdaWorker::builder(version(), WorkerOptions::new("queue").build())
            .client_options(
                ConnectionOptions::new(
                    temporalio_client::Url::parse(&format!("http://{address}")).unwrap(),
                )
                .build(),
                ClientOptions::new("default").build(),
            )
            .build()
            .unwrap();
        for _ in 0..2 {
            let mut context = lambda_runtime::Context::default();
            context.deadline = (SystemTime::now() + Duration::from_secs(12))
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let error = worker
                .handle(LambdaEvent::new((), context))
                .await
                .unwrap_err();
            assert!(!error.requires_restart());
            assert!(error.source().unwrap().is::<ClientConnectError>());
        }
        let error = worker
            .handle(LambdaEvent::new((), lambda_runtime::Context::default()))
            .await
            .unwrap_err();
        assert!(!error.requires_restart());
        assert!(
            error
                .to_string()
                .contains("insufficient Lambda invocation time")
        );
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
    async fn interrupted_invocation_cannot_be_reused() {
        let started = Arc::new(Notify::new());
        let hook_started = started.clone();
        let worker = LambdaWorker::builder(version(), WorkerOptions::new("queue").build())
            .client_options(
                ConnectionOptions::new(
                    temporalio_client::Url::parse("http://127.0.0.1:9").unwrap(),
                )
                .build(),
                ClientOptions::new("default").build(),
            )
            .shutdown_hook(move |_| {
                hook_started.notify_one();
                std::future::pending::<anyhow::Result<()>>()
            })
            .build()
            .unwrap();
        let mut context = lambda_runtime::Context::default();
        context.deadline = (SystemTime::now() + Duration::from_secs(12))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut invocation = Box::pin(worker.handle(LambdaEvent::new((), context.clone())));
        tokio::select! {
            result = &mut invocation => panic!("invocation ended before cancellation: {result:?}"),
            () = started.notified() => {}
        }
        let error = worker
            .handle(LambdaEvent::new((), context.clone()))
            .await
            .unwrap_err();
        assert!(!error.requires_restart());
        assert!(error.to_string().contains("invocation is already running"));
        drop(invocation);
        context.deadline = 0;
        let error = worker
            .handle(LambdaEvent::new((), context))
            .await
            .unwrap_err();
        assert!(error.requires_restart());
        assert!(error.to_string().contains("cannot be reused"));
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
                runtime: Arc::new(Runtime::from_current_tokio(Default::default()).unwrap()),
                shutdown_buffer: Duration::from_secs(7),
                drain_budget: Duration::from_secs(5),
                hook_budget: Duration::from_secs(2),
                invocation_gate: Semaphore::new(1),
                healthy: AtomicBool::new(true),
                interrupted: Notify::new(),
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
            .run_shutdown_hooks(Instant::now() + Duration::from_secs(1))
            .await;
        assert_eq!(*order.lock().unwrap(), vec!["first", "second"]);
    }
}
