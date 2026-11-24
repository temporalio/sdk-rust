#![warn(missing_docs)]

//! Experimental AWS Lambda support for the Temporal Rust SDK.
//!
//! A [`LambdaWorker`] creates a fresh client and Worker for every invocation, polls until the
//! reserved shutdown window, drains and finalizes the Worker, and runs cleanup hooks.
//! Invocation payloads are ignored: Temporal tasks are obtained by polling the configured queue.
//!
//! # Example
//!
//! An Activity-only Lambda Worker using the normal SDK Activity registration API:
//!
//! ```no_run
//! use temporalio_common::worker::WorkerDeploymentVersion;
//! use temporalio_macros::activities;
//! use temporalio_sdk::activities::{ActivityContext, ActivityError};
//! use temporalio_sdk_aws_lambda::LambdaWorker;
//!
//! struct GreetingActivities;
//!
//! #[activities]
//! impl GreetingActivities {
//!     #[activity]
//!     async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
//!         Ok(format!("Hello, {name}!"))
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), lambda_runtime::Error> {
//!     lambda_runtime::tracing::init_default_subscriber();
//!     let version = WorkerDeploymentVersion::builder()
//!         .deployment_name("greeting-worker")
//!         .build_id("v1")
//!         .build();
//!     let mut options = LambdaWorker::worker_options("greeting-task-queue");
//!     options.register_activities(GreetingActivities);
//!
//!     LambdaWorker::builder(version, options).build()?.run().await
//! }
//! ```
//!
//! Connection settings are loaded from environment variables and an optional `temporal.toml`.
//! The deployment name and build ID must match the serverless deployment configured in Temporal.
//! Register Workflows on the same [`WorkerOptions`] to run both Workflows and Activities.

#[cfg(feature = "otel")]
pub mod otel;

use std::{
    env,
    error::Error as StdError,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, SystemTime},
};

use lambda_runtime::{LambdaEvent, LambdaInvocation, Service, service_fn, tower::layer::layer_fn};
use temporalio_client::{
    Client, ClientOptions, ConnectionOptions, PluginError,
    envconfig::{ConfigError, DataSource, LoadClientConfigProfileOptions},
};
use temporalio_common::worker::{
    VersioningBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion,
};
use temporalio_sdk::{
    Runtime, Worker, WorkerOptions, WorkerPlugin,
    runtime::{
        PollerBehavior,
        worker_tuner::{FixedSizeSlotSupplier, TunerHolder},
    },
};
use tokio::{
    sync::Semaphore,
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
type ConfigureFuture = Pin<Box<dyn Future<Output = anyhow::Result<LambdaInvocationConfig>> + Send>>;
type Configure = Arc<dyn Fn(LambdaInvocationConfig) -> ConfigureFuture + Send + Sync>;

/// Invocation cleanup budgets that are not part of SDK [`WorkerOptions`].
///
/// Worker tuning and graceful shutdown are configured on [`WorkerOptions`]. Use
/// [`LambdaWorker::worker_options`] for Lambda-oriented defaults before customizing those options.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LambdaWorkerOptions {
    /// Additional drain and finalization time after the graceful period.
    pub worker_shutdown_buffer: Duration,
    /// Shared budget for telemetry flush and application hooks.
    pub shutdown_hook_buffer: Duration,
}

impl Default for LambdaWorkerOptions {
    fn default() -> Self {
        Self {
            worker_shutdown_buffer: Duration::from_secs(1),
            shutdown_hook_buffer: Duration::from_secs(2),
        }
    }
}

/// Fresh configuration passed to [`LambdaWorkerBuilder::configure`] for each invocation.
///
/// Use the callback to create invocation-scoped resources and customize registrations or connection
/// options. Cleanup hooks belong on the builder; resources captured by Activities must also handle
/// cancellation. The callback and connection setup share the invocation's work budget.
#[non_exhaustive]
pub struct LambdaInvocationConfig {
    /// AWS invocation context.
    pub context: lambda_runtime::Context,
    /// Options used to connect to Temporal.
    pub connection_options: ConnectionOptions,
    /// Namespace-bound client options.
    pub client_options: ClientOptions,
    /// Worker registrations and options, cloned from the options supplied to the builder.
    pub worker_options: WorkerOptions,
}

/// An error configuring a Lambda Worker, preserving its underlying cause.
#[derive(Debug, thiserror::Error)]
#[error("failed to configure Lambda Worker")]
pub struct LambdaWorkerBuildError(#[source] Box<dyn StdError + Send + Sync>);

impl LambdaWorkerBuildError {
    fn new(source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        Self(source.into())
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            message.into(),
        ))
    }
}

/// An invocation failure, preserving its underlying cause.
#[derive(Debug, thiserror::Error)]
#[error("Lambda Worker invocation failed")]
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
    /// When true, do not reuse this handler or construct another in the same process: background
    /// Worker resources or Activity work may still be running.
    pub fn requires_restart(&self) -> bool {
        self.requires_restart
    }
}

impl From<LambdaInvocationError> for lambda_runtime::Diagnostic {
    fn from(error: LambdaInvocationError) -> Self {
        Self {
            error_type: std::any::type_name::<LambdaInvocationError>().to_owned(),
            error_message: format!("{:#}", anyhow::Error::new(error)),
        }
    }
}

/// Builder for [`LambdaWorker`].
pub struct LambdaWorkerBuilder {
    version: WorkerDeploymentVersion,
    worker_options: WorkerOptions,
    client_options: Option<(ConnectionOptions, ClientOptions)>,
    runtime: Option<Arc<Runtime>>,
    lambda_options: LambdaWorkerOptions,
    default_versioning_behavior: VersioningBehavior,
    configure: Option<Configure>,
    shutdown_hooks: Vec<ShutdownHook>,
    #[cfg(feature = "otel")]
    open_telemetry: Option<otel::LambdaOpenTelemetryOptions>,
}

impl LambdaWorkerBuilder {
    /// Use explicit connection and client options instead of environment loading.
    pub fn client_options(
        mut self,
        connection_options: ConnectionOptions,
        client_options: ClientOptions,
    ) -> Self {
        self.client_options = Some((connection_options, client_options));
        self
    }

    /// Use an already-created SDK runtime.
    pub fn runtime(mut self, runtime: Arc<Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Configure OTLP metrics and tracing, flushed after each invocation.
    ///
    /// Creates the SDK runtime; cannot be combined with [`Self::runtime`].
    #[cfg(feature = "otel")]
    pub fn open_telemetry(mut self, options: otel::LambdaOpenTelemetryOptions) -> Self {
        self.open_telemetry = Some(options);
        self
    }

    /// Set the additional Worker drain and application cleanup budgets.
    pub fn lambda_options(mut self, options: LambdaWorkerOptions) -> Self {
        self.lambda_options = options;
        self
    }

    /// Set the default behavior for Workflows without a registration-time behavior.
    ///
    /// Defaults to [`VersioningBehavior::Pinned`]. `Unspecified` is rejected.
    pub fn default_versioning_behavior(mut self, behavior: VersioningBehavior) -> Self {
        self.default_versioning_behavior = behavior;
        self
    }

    /// Customize a fresh configuration asynchronously on every invocation.
    ///
    /// Return the modified configuration. Setup runs before connection and is bounded by the work
    /// deadline. It must not block the async executor. Builder shutdown hooks also run on setup
    /// failure, and captured process-level resources must remain usable on subsequent invocations.
    pub fn configure<F, Fut>(mut self, configure: F) -> Self
    where
        F: Fn(LambdaInvocationConfig) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<LambdaInvocationConfig>> + Send + 'static,
    {
        self.configure = Some(Arc::new(move |config| Box::pin(configure(config))));
        self
    }

    /// Add an async cleanup hook, normally run after Worker finalization.
    ///
    /// Hooks run in registration order and share the hook budget; the automatic telemetry flush
    /// runs first. Each receives the remaining budget. Errors are logged, and timed-out hooks are
    /// dropped. Hooks must not block the executor. If drain overruns, hooks run alongside shutdown.
    pub fn shutdown_hook<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(Duration) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.shutdown_hooks
            .push(Arc::new(move |remaining| Box::pin(hook(remaining))));
        self
    }

    /// Validate options and build a reusable handler.
    ///
    /// Loads environment/TOML connection configuration once when no explicit options were supplied.
    /// Must run inside Tokio unless [`Self::runtime`] was supplied.
    pub fn build(mut self) -> Result<LambdaWorker, LambdaWorkerBuildError> {
        validate_version(&self.version)?;
        validate_lambda_options(&self.lambda_options)?;
        if self.default_versioning_behavior == VersioningBehavior::Unspecified {
            return Err(LambdaWorkerBuildError::invalid(
                "default versioning behavior cannot be Unspecified",
            ));
        }
        if let Some(queue) =
            resolve_task_queue(&self.worker_options.task_queue, |name| env::var(name).ok())
        {
            self.worker_options.task_queue = queue;
        } else if self.configure.is_none() {
            return Err(LambdaWorkerBuildError::invalid(
                "task queue is required: set WorkerOptions.task_queue or TEMPORAL_TASK_QUEUE",
            ));
        }
        #[allow(unused_mut)]
        let (connection_options, mut client_options) = match self.client_options {
            Some(options) => options,
            None => load_client_options().map_err(LambdaWorkerBuildError::new)?,
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
        Ok(LambdaWorker {
            inner: Arc::new(LambdaWorkerInner {
                connection_options,
                client_options,
                worker_options: self.worker_options,
                version: self.version,
                default_versioning_behavior: self.default_versioning_behavior,
                configure: self.configure,
                runtime,
                lambda_options: self.lambda_options,
                shutdown_hooks: self.shutdown_hooks,
                invocation_gate: Semaphore::new(1),
                healthy: AtomicBool::new(true),
            }),
        })
    }
}

#[derive(Clone)]
struct LambdaWorkerPlugin {
    version: WorkerDeploymentVersion,
    default_versioning_behavior: VersioningBehavior,
    lambda_options: LambdaWorkerOptions,
    invocation_deadline: Instant,
    shutdown_at: Arc<OnceLock<Instant>>,
}

impl WorkerPlugin for LambdaWorkerPlugin {
    fn name(&self) -> &str {
        "temporalio-sdk-aws-lambda"
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        let graceful_period = options.graceful_shutdown_period.ok_or_else(|| {
            PluginError::new(LambdaWorkerBuildError::invalid(
                "Lambda workers require a finite WorkerOptions::graceful_shutdown_period",
            ))
        })?;
        let reserve = graceful_period
            .checked_add(self.lambda_options.worker_shutdown_buffer)
            .and_then(|budget| budget.checked_add(self.lambda_options.shutdown_hook_buffer))
            .and_then(|budget| budget.checked_add(RESPONSE_BUFFER))
            .ok_or_else(|| {
                PluginError::new(LambdaWorkerBuildError::invalid(
                    "worker shutdown period and Lambda cleanup buffers exceed the supported duration",
                ))
            })?;
        let shutdown_at = self.invocation_deadline.checked_sub(reserve).filter(|deadline| {
            deadline.saturating_duration_since(Instant::now()) > MINIMUM_WORK_TIME
        }).ok_or_else(|| {
            PluginError::new(LambdaWorkerBuildError::invalid(
                "insufficient Lambda invocation time for the configured worker shutdown period and cleanup buffers",
            ))
        })?;
        let work_time = shutdown_at.saturating_duration_since(Instant::now());
        if work_time < LOW_WORK_TIME_WARNING {
            tracing::warn!(
                ?work_time,
                "Lambda invocation has little time available for Worker polling"
            );
        }
        // Only this last plugin sees the effective shutdown period. Publish its cutoff before
        // Core is created so configuration and connection time cannot extend polling.
        self.shutdown_at.set(shutdown_at).map_err(|_| {
            PluginError::new(LambdaWorkerBuildError::invalid(
                "Lambda shutdown deadline was already configured",
            ))
        })?;
        options.deployment_options = WorkerDeploymentOptions::new(self.version.clone())
            .use_worker_versioning(true)
            .default_versioning_behavior(self.default_versioning_behavior)
            .build();
        options.max_eager_activity_reservations_per_workflow_task = 0;
        Ok(())
    }
}

/// A reusable handler running one fresh Temporal Worker per Lambda invocation.
#[derive(Clone)]
pub struct LambdaWorker {
    inner: Arc<LambdaWorkerInner>,
}

struct LambdaWorkerInner {
    connection_options: ConnectionOptions,
    client_options: ClientOptions,
    worker_options: WorkerOptions,
    version: WorkerDeploymentVersion,
    default_versioning_behavior: VersioningBehavior,
    configure: Option<Configure>,
    runtime: Arc<Runtime>,
    lambda_options: LambdaWorkerOptions,
    shutdown_hooks: Vec<ShutdownHook>,
    invocation_gate: Semaphore,
    healthy: AtomicBool,
}

struct InvocationGuard<'a> {
    healthy: &'a AtomicBool,
    completed: bool,
}

impl Drop for InvocationGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.healthy.store(false, Ordering::Release);
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

struct RestartAfterResponse<S> {
    inner: S,
    worker: LambdaWorker,
}

impl<S> Service<LambdaInvocation> for RestartAfterResponse<S>
where
    S: Service<LambdaInvocation, Response = (), Error = lambda_runtime::Error>,
    S::Future: 'static,
{
    type Response = ();
    type Error = lambda_runtime::Error;
    type Future = Pin<Box<dyn Future<Output = Result<(), Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, invocation: LambdaInvocation) -> Self::Future {
        let response = self.inner.call(invocation);
        let worker = self.worker.clone();
        Box::pin(async move {
            // This service includes posting the response; retire only after that I/O completes.
            response.await?;
            if worker.inner.healthy.load(Ordering::Acquire) {
                Ok(())
            } else {
                Err(LambdaInvocationError::interrupted().into())
            }
        })
    }
}

impl LambdaWorker {
    /// Create ordinary SDK options with Lambda-oriented defaults, before caller customization.
    ///
    /// Defaults are ten Workflow Task slots, two Activity slots, two Local Activity slots, two
    /// Workflow pollers, one Activity poller, thirty cached Workflows, and five seconds of graceful
    /// shutdown. Customize these fields and registrations directly on the returned options.
    pub fn worker_options(task_queue: impl Into<String>) -> WorkerOptions {
        WorkerOptions::new(task_queue)
            .tuner(
                TunerHolder::builder()
                    .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(10))
                    .activity_task_slot_supplier(FixedSizeSlotSupplier::new(2))
                    .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(2))
                    .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(1))
                    .build(),
            )
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1))
            .max_cached_workflows(30)
            .graceful_shutdown_period(Duration::from_secs(5))
            .build()
    }

    /// Start building a handler around existing SDK options without replacing their tuning.
    ///
    /// Use [`Self::worker_options`] for Lambda defaults. The effective options after per-invocation
    /// configuration and plugins must specify a finite graceful shutdown period that fits the
    /// invocation. Deployment versioning and disabled eager Activities are managed by the adapter.
    pub fn builder(
        version: WorkerDeploymentVersion,
        worker_options: WorkerOptions,
    ) -> LambdaWorkerBuilder {
        LambdaWorkerBuilder {
            version,
            worker_options,
            client_options: None,
            runtime: None,
            lambda_options: LambdaWorkerOptions::default(),
            default_versioning_behavior: VersioningBehavior::Pinned,
            configure: None,
            shutdown_hooks: Vec::new(),
            #[cfg(feature = "otel")]
            open_telemetry: None,
        }
    }

    /// Handle one invocation, ignoring its payload and polling Temporal for tasks.
    ///
    /// Calls must be sequential. Recycle the process if the returned error requires restart or if
    /// this future is cancelled. Prefer [`Self::run`], which retires unhealthy handlers after
    /// reporting the invocation error to AWS.
    pub async fn handle<T>(&self, event: LambdaEvent<T>) -> Result<(), LambdaInvocationError> {
        if !self.inner.healthy.load(Ordering::Acquire) {
            return Err(LambdaInvocationError::interrupted());
        }
        let now = Instant::now();
        let initial_remaining = event
            .context
            .deadline()
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        let minimum_shutdown_buffer = self.inner.lambda_options.worker_shutdown_buffer
            + self.inner.lambda_options.shutdown_hook_buffer
            + RESPONSE_BUFFER;
        let work_time = initial_remaining.saturating_sub(minimum_shutdown_buffer);
        if work_time <= MINIMUM_WORK_TIME {
            return Err(LambdaInvocationError::new(
                anyhow::anyhow!(
                    "insufficient Lambda invocation time: {initial_remaining:?} remaining with a {:?} shutdown buffer",
                    minimum_shutdown_buffer
                ),
                false,
            ));
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
            completed: false,
        };
        // One monotonic deadline prevents wall-clock changes and setup time from extending polling.
        let invocation_deadline = now + initial_remaining;
        let response_deadline = invocation_deadline - RESPONSE_BUFFER;
        let drain_deadline = response_deadline - self.inner.lambda_options.shutdown_hook_buffer;
        let setup_deadline = drain_deadline - self.inner.lambda_options.worker_shutdown_buffer;
        let worker_run = self.run_worker(event.context, setup_deadline, invocation_deadline);
        tokio::pin!(worker_run);
        let result = match timeout_at(drain_deadline, &mut worker_run).await {
            Ok(result) => {
                self.run_shutdown_hooks(
                    response_deadline
                        .min(Instant::now() + self.inner.lambda_options.shutdown_hook_buffer),
                )
                .await;
                result
            }
            Err(error) => {
                tracing::error!(
                    "Worker drain budget elapsed; continuing shutdown alongside cleanup hooks"
                );
                // Continue finalizing during cleanup, but retire even if it succeeds: telemetry
                // may already have flushed before the last Worker spans were emitted.
                let (drain_result, ()) = tokio::join!(
                    timeout_at(response_deadline, &mut worker_run),
                    self.run_shutdown_hooks(
                        response_deadline
                            .min(Instant::now() + self.inner.lambda_options.shutdown_hook_buffer)
                    ),
                );
                if !matches!(drain_result, Ok(Ok(()))) {
                    tracing::error!(?drain_result, "Worker shutdown failed during cleanup");
                }
                Err(LambdaInvocationError::new(
                    anyhow::Error::new(error).context(
                        "Temporal Worker did not drain and finalize before the invocation cleanup deadline",
                    ),
                    true,
                ))
            }
        };
        guard.completed = result
            .as_ref()
            .err()
            .is_none_or(|error| !error.requires_restart());
        result
    }

    /// Run using the AWS Lambda Rust runtime, retiring unsafe execution environments.
    pub async fn run(self) -> Result<(), lambda_runtime::Error> {
        if !self.inner.healthy.load(Ordering::Acquire) {
            return Err(LambdaInvocationError::interrupted().into());
        }
        let handler = self.clone();
        lambda_runtime::Runtime::new(service_fn(move |event: LambdaEvent<serde_json::Value>| {
            let worker = handler.clone();
            async move { worker.handle(event).await }
        }))
        .layer(lambda_runtime::layers::TracingLayer::new())
        .layer(layer_fn(move |inner| RestartAfterResponse {
            inner,
            worker: self.clone(),
        }))
        .run()
        .await
    }

    async fn run_worker(
        &self,
        context: lambda_runtime::Context,
        setup_deadline: Instant,
        invocation_deadline: Instant,
    ) -> Result<(), LambdaInvocationError> {
        let shutdown_at = Arc::new(OnceLock::new());
        let setup = async {
            let mut config = LambdaInvocationConfig {
                context,
                connection_options: self.inner.connection_options.clone(),
                client_options: self.inner.client_options.clone(),
                worker_options: self.inner.worker_options.clone(),
            };
            if let Some(configure) = &self.inner.configure {
                config = configure(config).await?;
            }
            if config.worker_options.task_queue.trim().is_empty() {
                anyhow::bail!("task queue is required for this invocation");
            }
            if config.connection_options.identity.is_empty()
                && config.worker_options.client_identity_override.is_none()
            {
                config.connection_options.identity = invocation_identity(&config.context);
            }
            config.worker_options.worker_plugin(LambdaWorkerPlugin {
                version: self.inner.version.clone(),
                default_versioning_behavior: self.inner.default_versioning_behavior,
                lambda_options: self.inner.lambda_options.clone(),
                invocation_deadline,
                shutdown_at: shutdown_at.clone(),
            });
            let client = Client::connect(config.connection_options, config.client_options).await?;
            Ok::<_, anyhow::Error>((client, config.worker_options))
        };
        let (client, options) = timeout_at(setup_deadline, setup)
            .await
            .map_err(|error| {
                LambdaInvocationError::new(
                    anyhow::Error::new(error)
                        .context("configuration or Temporal connection exceeded the work deadline"),
                    false,
                )
            })?
            .map_err(|error| LambdaInvocationError::new(error, false))?;
        let worker = Worker::new(&self.inner.runtime, client, options)
            .map_err(|error| LambdaInvocationError::new(error, false))?;
        let shutdown_at = *shutdown_at
            .get()
            .expect("Worker creation ran the final Lambda plugin");
        // Dropping run_until alone can leave Activities holding Core alive.
        let _shutdown = ShutdownGuard(Some(worker.shutdown_handle()));
        worker
            .run_until(sleep_until(shutdown_at))
            .await
            .map_err(|error| LambdaInvocationError::new(error, true))
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
                Ok(Err(error)) => tracing::error!(%error, "Lambda Worker shutdown hook failed"),
                Err(_) => {
                    tracing::error!("Lambda Worker shutdown hook exceeded the shared hook budget");
                    break;
                }
            }
        }
    }
}

fn validate_version(version: &WorkerDeploymentVersion) -> Result<(), LambdaWorkerBuildError> {
    if version.deployment_name.trim().is_empty() || version.build_id.trim().is_empty() {
        return Err(LambdaWorkerBuildError::invalid(
            "worker deployment name and build ID must both be non-empty",
        ));
    }
    Ok(())
}

fn validate_lambda_options(options: &LambdaWorkerOptions) -> Result<(), LambdaWorkerBuildError> {
    if options.worker_shutdown_buffer.is_zero() || options.shutdown_hook_buffer.is_zero() {
        return Err(LambdaWorkerBuildError::invalid(
            "worker and hook shutdown buffers must be greater than zero",
        ));
    }
    let total = options
        .worker_shutdown_buffer
        .checked_add(options.shutdown_hook_buffer)
        .and_then(|budget| budget.checked_add(RESPONSE_BUFFER));
    if total.is_none_or(|budget| Instant::now().checked_add(budget).is_none()) {
        return Err(LambdaWorkerBuildError::invalid(
            "shutdown budgets exceed the supported duration",
        ));
    }
    Ok(())
}

fn load_client_options() -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
    let options =
        match resolve_config_file(|name| env::var_os(name), env::current_dir().ok().as_deref()) {
            Some(path) => LoadClientConfigProfileOptions::builder()
                .config_source(DataSource::Path(path.to_string_lossy().into_owned()))
                .build(),
            None => LoadClientConfigProfileOptions::builder()
                .disable_file(true)
                .build(),
        };
    ClientOptions::load_from_config(options)
}

fn resolve_config_file(
    getenv: impl Fn(&str) -> Option<std::ffi::OsString>,
    current_dir: Option<&Path>,
) -> Option<PathBuf> {
    if let Some(path) = getenv(ENV_CONFIG_FILE).filter(|path| !path.is_empty()) {
        return Some(PathBuf::from(path));
    }
    if let Some(root) = getenv(ENV_LAMBDA_TASK_ROOT).filter(|path| !path.is_empty()) {
        let candidate = PathBuf::from(root).join(DEFAULT_CONFIG_FILE);
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
    getenv(ENV_TASK_QUEUE).filter(|queue| !queue.trim().is_empty())
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
    use futures::FutureExt;
    use std::sync::{Mutex, atomic::AtomicUsize};
    use temporalio_sdk::{
        RuntimeError,
        runtime::worker_tuner::{SlotSupplier, WorkerTuner},
    };
    use tokio::sync::Notify;

    fn builder() -> LambdaWorkerBuilder {
        LambdaWorker::builder(
            WorkerDeploymentVersion::builder()
                .deployment_name("deployment")
                .build_id("build")
                .build(),
            LambdaWorker::worker_options("queue"),
        )
        .client_options(
            ConnectionOptions::new(temporalio_client::Url::parse("http://127.0.0.1:9").unwrap())
                .build(),
            ClientOptions::new("default").build(),
        )
    }

    fn event() -> LambdaEvent<()> {
        let mut context = lambda_runtime::Context::default();
        context.deadline = (SystemTime::now() + Duration::from_secs(30))
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        LambdaEvent::new((), context)
    }

    #[test]
    fn build_error_preserves_runtime_cause() {
        let error = builder().build().err().unwrap();
        assert!(matches!(
            error.source().unwrap().downcast_ref::<RuntimeError>(),
            Some(RuntimeError::NoCurrentTokioRuntime)
        ));
    }

    #[test]
    fn rejects_invalid_budgets_before_runtime_creation() {
        for options in [
            LambdaWorkerOptions {
                worker_shutdown_buffer: Duration::ZERO,
                ..Default::default()
            },
            LambdaWorkerOptions {
                worker_shutdown_buffer: Duration::MAX,
                ..Default::default()
            },
            LambdaWorkerOptions {
                shutdown_hook_buffer: Duration::ZERO,
                ..Default::default()
            },
        ] {
            assert!(
                builder()
                    .lambda_options(options)
                    .build()
                    .err()
                    .unwrap()
                    .source()
                    .unwrap()
                    .is::<std::io::Error>()
            );
        }
    }

    #[tokio::test]
    async fn building_does_not_replace_caller_worker_tuning() {
        let mut builder = builder();
        builder.worker_options.max_cached_workflows = 7;
        builder.worker_options.workflow_task_poller_behavior =
            Some(PollerBehavior::SimpleMaximum(4));
        builder.worker_options.activity_task_poller_behavior =
            Some(PollerBehavior::SimpleMaximum(3));
        builder.worker_options.graceful_shutdown_period = Some(Duration::from_secs(9));
        builder.worker_options.tuner = TunerHolder::builder()
            .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(6))
            .activity_task_slot_supplier(FixedSizeSlotSupplier::new(8))
            .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(4))
            .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(1))
            .build()
            .into();
        let worker = builder.build().unwrap();
        let options = &worker.inner.worker_options;
        assert_eq!(options.max_cached_workflows, 7);
        assert_eq!(
            options.workflow_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(4))
        );
        assert_eq!(
            options.activity_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(3))
        );
        assert_eq!(
            options.graceful_shutdown_period,
            Some(Duration::from_secs(9))
        );
        let WorkerTuner::TunerHolder(tuner) = &options.tuner else {
            panic!("caller tuner was replaced");
        };
        assert!(
            matches!(&tuner.workflow_task_slot_supplier, SlotSupplier::FixedSize(s) if s.num_slots == 6)
        );
        assert!(
            matches!(&tuner.activity_task_slot_supplier, SlotSupplier::FixedSize(s) if s.num_slots == 8)
        );
        assert!(
            matches!(&tuner.local_activity_task_slot_supplier, SlotSupplier::FixedSize(s) if s.num_slots == 4)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cutoff_uses_effective_grace_and_remaining_invocation_time() {
        let now = Instant::now();
        let cutoff = Arc::new(OnceLock::new());
        let plugin = LambdaWorkerPlugin {
            version: builder().version,
            default_versioning_behavior: VersioningBehavior::Pinned,
            lambda_options: LambdaWorkerOptions::default(),
            invocation_deadline: now + Duration::from_secs(30),
            shutdown_at: cutoff.clone(),
        };
        let mut options = LambdaWorker::worker_options("queue");
        options.graceful_shutdown_period = Some(Duration::from_secs(2));
        plugin.configure_worker_options(&mut options).unwrap();
        assert_eq!(
            options.graceful_shutdown_period,
            Some(Duration::from_secs(2))
        );
        assert_eq!(*cutoff.get().unwrap(), now + Duration::from_millis(24_900));

        tokio::time::advance(Duration::from_secs(25)).await;
        let plugin = LambdaWorkerPlugin {
            shutdown_at: Arc::new(OnceLock::new()),
            ..plugin
        };
        assert!(plugin.configure_worker_options(&mut options).is_err());
        assert!(plugin.shutdown_at.get().is_none());
        assert_eq!(
            options.graceful_shutdown_period,
            Some(Duration::from_secs(2))
        );
    }

    #[tokio::test]
    async fn unusable_worker_shutdown_periods_are_rejected_without_replacement() {
        for period in [None, Some(Duration::MAX), Some(Duration::from_secs(60))] {
            let plugin = LambdaWorkerPlugin {
                version: builder().version,
                default_versioning_behavior: VersioningBehavior::Pinned,
                lambda_options: LambdaWorkerOptions::default(),
                invocation_deadline: Instant::now() + Duration::from_secs(30),
                shutdown_at: Arc::new(OnceLock::new()),
            };
            let mut options = LambdaWorker::worker_options("queue");
            options.graceful_shutdown_period = period;
            assert!(plugin.configure_worker_options(&mut options).is_err());
            assert_eq!(options.graceful_shutdown_period, period);
            assert!(plugin.shutdown_at.get().is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn configuration_timeout_runs_hooks_and_allows_warm_reuse() {
        let configured = Arc::new(AtomicUsize::new(0));
        let called = configured.clone();
        let hooks = Arc::new(AtomicUsize::new(0));
        let hook_calls = hooks.clone();
        let worker = builder()
            .configure(move |mut config| {
                let invocation = called.fetch_add(1, Ordering::Relaxed);
                async move {
                    if invocation == 0 {
                        config.worker_options.task_queue = "must-not-leak".to_owned();
                        std::future::pending::<()>().await;
                    }
                    anyhow::bail!(
                        "second invocation has fresh options: {}",
                        config.worker_options.task_queue
                    );
                }
            })
            .shutdown_hook(move |_| {
                hook_calls.fetch_add(1, Ordering::Relaxed);
                async { Ok(()) }
            })
            .build()
            .unwrap();
        let error = worker.handle(event()).await.unwrap_err();
        assert!(!error.requires_restart());
        assert!(format!("{:#}", anyhow::Error::new(error)).contains("exceeded the work deadline"));
        let error = worker.handle(event()).await.unwrap_err();
        assert!(!error.requires_restart());
        assert!(format!("{:#}", anyhow::Error::new(error)).contains("fresh options: queue"));
        assert_eq!(hooks.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn cancellation_and_concurrent_calls_cannot_leak_into_warm_reuse() {
        let started = Arc::new(Notify::new());
        let setup_started = started.clone();
        let worker = builder()
            .configure(move |_| {
                setup_started.notify_one();
                std::future::pending()
            })
            .build()
            .unwrap();
        let mut invocation = Box::pin(worker.handle(event()));
        tokio::select! {
            result = &mut invocation => panic!("unexpected completion: {result:?}"),
            () = started.notified() => {}
        }
        let error = worker.handle(event()).await.unwrap_err();
        assert!(!error.requires_restart());
        assert!(
            error
                .source()
                .unwrap()
                .to_string()
                .contains("already running")
        );
        drop(invocation);
        assert!(worker.handle(event()).await.unwrap_err().requires_restart());
    }

    #[tokio::test(start_paused = true)]
    async fn hooks_share_a_budget_and_continue_after_errors() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let first = order.clone();
        let second = order.clone();
        let third = order.clone();
        let worker = builder()
            .shutdown_hook(move |_| {
                first.lock().unwrap().push("first");
                async { anyhow::bail!("expected hook failure") }
            })
            .shutdown_hook(move |_| {
                second.lock().unwrap().push("second");
                std::future::pending()
            })
            .shutdown_hook(move |_| {
                third.lock().unwrap().push("third");
                async { Ok(()) }
            })
            .build()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        worker.run_shutdown_hooks(deadline).await;
        assert_eq!(*order.lock().unwrap(), vec!["first", "second"]);
        assert_eq!(Instant::now(), deadline);
    }

    #[tokio::test]
    async fn restart_waits_for_the_runtime_response_to_finish() {
        let worker = builder().build().unwrap();
        let posted = Arc::new(Notify::new());
        let acknowledge = Arc::new(Notify::new());
        let response_posted = posted.clone();
        let response_acknowledge = acknowledge.clone();
        let response_worker = worker.clone();
        let transport = service_fn(move |_: LambdaInvocation| {
            let worker = response_worker.clone();
            let posted = response_posted.clone();
            let acknowledge = response_acknowledge.clone();
            async move {
                drop(InvocationGuard {
                    healthy: &worker.inner.healthy,
                    completed: false,
                });
                posted.notify_one();
                acknowledge.notified().await;
                Ok::<(), lambda_runtime::Error>(())
            }
        });
        let mut service = RestartAfterResponse {
            inner: transport,
            worker: worker.clone(),
        };
        let (parts, ()) = http::Response::new(()).into_parts();
        let response = service.call(LambdaInvocation {
            parts,
            body: b"{}".to_vec().into(),
            context: lambda_runtime::Context::default(),
        });
        tokio::pin!(response);
        tokio::select! {
            result = &mut response => panic!("exited before acknowledgment: {result:?}"),
            () = posted.notified() => {}
        }
        assert!(response.as_mut().now_or_never().is_none());
        acknowledge.notify_one();
        assert!(
            response
                .await
                .unwrap_err()
                .downcast_ref::<LambdaInvocationError>()
                .unwrap()
                .requires_restart()
        );
    }

    #[test]
    fn config_file_resolution_preserves_precedence_and_explicit_missing_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("task");
        std::fs::create_dir(&root).unwrap();
        let cwd_file = temp.path().join(DEFAULT_CONFIG_FILE);
        std::fs::write(&cwd_file, "").unwrap();
        let resolve = || {
            resolve_config_file(
                |name| (name == ENV_LAMBDA_TASK_ROOT).then(|| root.clone().into_os_string()),
                Some(temp.path()),
            )
        };
        assert_eq!(resolve(), Some(cwd_file));
        let root_file = root.join(DEFAULT_CONFIG_FILE);
        std::fs::write(&root_file, "").unwrap();
        assert_eq!(resolve(), Some(root_file));
        let missing = temp.path().join("missing.toml");
        assert_eq!(
            resolve_config_file(
                |_| Some(missing.clone().into_os_string()),
                Some(temp.path()),
            ),
            Some(missing)
        );
        assert_eq!(
            resolve_task_queue("explicit", |_| Some("env".to_owned())),
            Some("explicit".to_owned())
        );
        assert_eq!(
            resolve_task_queue(" ", |_| Some("env".to_owned())),
            Some("env".to_owned())
        );
        assert_eq!(resolve_task_queue("", |_| Some(" ".to_owned())), None);
    }
}
