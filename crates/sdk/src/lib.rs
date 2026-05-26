#![warn(missing_docs)] // error if there are missing docs

//! This crate defines a Public Preview Temporal Rust SDK.
//!
//! Currently defining activities and running an activity-only worker is the most stable code.
//! Workflow definitions exist and running a workflow worker works, but the API is still very
//! unstable.
//!
//! An example of running an activity worker:
//! ```no_run
//! use std::str::FromStr;
//! use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
//! use temporalio_common::{
//!     telemetry::TelemetryOptions,
//!     worker::{WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes},
//! };
//! use temporalio_macros::activities;
//! use temporalio_sdk::{
//!     Worker, WorkerOptions,
//!     activities::{ActivityContext, ActivityError},
//! };
//! use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
//!
//! struct MyActivities;
//!
//! #[activities]
//! impl MyActivities {
//!     #[activity]
//!     pub(crate) async fn echo(
//!         _ctx: ActivityContext,
//!         e: String,
//!     ) -> Result<String, ActivityError> {
//!         Ok(e)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let connection_options =
//!         ConnectionOptions::new(Url::from_str("http://localhost:7233")?).build();
//!     let telemetry_options = TelemetryOptions::builder().build();
//!     let runtime_options = RuntimeOptions::builder()
//!         .telemetry_options(telemetry_options)
//!         .build()?;
//!     let runtime = CoreRuntime::new_assume_tokio(runtime_options)?;
//!
//!     let connection = Connection::connect(connection_options).await?;
//!     let client = Client::new(connection, ClientOptions::new("my_namespace").build())?;
//!
//!     let worker_options = WorkerOptions::new("task_queue")
//!         .task_types(WorkerTaskTypes::activity_only())
//!         .deployment_options(WorkerDeploymentOptions {
//!             version: WorkerDeploymentVersion {
//!                 deployment_name: "my_deployment".to_owned(),
//!                 build_id: "my_build_id".to_owned(),
//!             },
//!             use_worker_versioning: false,
//!             default_versioning_behavior: None,
//!         })
//!         .register_activities(MyActivities)
//!         .build();
//!
//!     let mut worker = Worker::new(&runtime, client, worker_options)?;
//!     worker.run().await?;
//!
//!     Ok(())
//! }
//! ```

#[macro_use]
extern crate tracing;
extern crate self as temporalio_sdk;

pub mod activities;
pub mod error;
pub mod interceptors;
mod workflow_context;
mod workflow_executor;
mod workflow_future;
pub mod workflows;

#[macro_export]
#[doc(hidden)]
macro_rules! __temporal_select {
    ($($tokens:tt)*) => {
        ::futures_util::select_biased! { $($tokens)* }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __temporal_join {
    ($($tokens:tt)*) => {
        ::futures_util::join!($($tokens)*)
    };
}

use workflow_future::WorkflowFunction;

pub use error::{
    ActivityExecutionError, ApplicationFailure, ChildWorkflowExecutionError,
    ChildWorkflowSignalError, ChildWorkflowStartError, OutgoingActivityError, OutgoingError,
    OutgoingWorkflowError,
};
pub use temporalio_client::Namespace;
pub use workflow_context::{
    ActivityCloseTimeouts, ActivityOptions, BaseWorkflowContext, CancellableFuture,
    ChildWorkflowOptions, ContinueAsNewOptions, ExternalWorkflowHandle, LocalActivityOptions,
    NexusOperationOptions, ParentWorkflowInfo, RootWorkflowInfo, Signal, SignalData,
    StartChildWorkflowExecutionFailedCause, StartedChildWorkflow, SyncWorkflowContext,
    TimerOptions, WorkflowContext, WorkflowContextView,
};

use crate::{
    activities::{
        ActivityContext, ActivityDefinitions, ActivityImplementer, ExecutableActivity,
        activity_error_to_core_result,
    },
    interceptors::{ActivityInboundInterceptor, WorkerInterceptor},
    workflow_context::{
        ChildWfCommon, NexusUnblockData, PendingChildWorkflow, StartedNexusOperation,
    },
    workflow_executor::WorkflowExecutor,
    workflows::{WorkflowDefinitions, WorkflowImplementation, WorkflowImplementer},
};
use anyhow::{Context, anyhow, bail};
use futures_util::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display, Formatter},
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};
use temporalio_client::{Client, NamespacedClient};
use temporalio_common::{
    ActivityDefinition, WorkflowDefinition,
    data_converters::{DataConverter, SerializationContext, SerializationContextData},
    payload_visitor::{decode_payloads, encode_payloads},
    protos::{
        TaskToken,
        coresdk::{
            ActivityTaskCompletion, AsJsonPayloadExt,
            activity_result::{ActivityExecutionResult, ActivityResolution},
            activity_task::{ActivityTask, activity_task},
            child_workflow::ChildWorkflowResult,
            nexus::NexusOperationResult,
            workflow_activation::{
                WorkflowActivation,
                resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
                resolve_nexus_operation_start, workflow_activation_job::Variant,
            },
            workflow_commands::{
                ContinueAsNewWorkflowExecution, WorkflowCommand, workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::Payload, enums::v1::WorkflowTaskFailedCause, failure::v1::Failure,
        },
    },
    worker::{WorkerDeploymentOptions, WorkerTaskTypes, build_id_from_current_exe},
};
use temporalio_sdk_core::{
    CoreRuntime, PollError, PollerBehavior, TunerBuilder, Worker as CoreWorker, WorkerConfig,
    WorkerTuner, WorkerVersioningStrategy, WorkflowErrorType, init_worker,
};
use tokio::sync::{
    Notify,
    mpsc::{UnboundedSender, unbounded_channel},
    oneshot,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, field};
use uuid::Uuid;

/// Contains options for configuring a worker.
#[derive(bon::Builder, Clone)]
#[builder(start_fn = new, on(String, into), state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct WorkerOptions {
    /// What task queue will this worker poll from? This task queue name will be used for both
    /// workflow and activity polling.
    #[builder(start_fn)]
    pub task_queue: String,

    #[builder(field)]
    activities: ActivityDefinitions,

    #[builder(field)]
    workflows: WorkflowDefinitions,

    /// Set the deployment options for this worker. Defaults to a hash of the currently running
    /// executable.
    #[builder(default = def_build_id())]
    pub deployment_options: WorkerDeploymentOptions,
    /// A human-readable string that can identify this worker. If set, overrides the identity on
    /// the client used by this worker. If unset and the client has no identity, defaults to
    /// `{pid}@{hostname}`.
    pub client_identity_override: Option<String>,
    /// If set nonzero, workflows will be cached and sticky task queues will be used, meaning that
    /// history updates are applied incrementally to suspended instances of workflow execution.
    /// Workflows are evicted according to a least-recently-used policy once the cache maximum is
    /// reached. Workflows may also be explicitly evicted at any time, or as a result of errors
    /// or failures.
    #[builder(default = 1000)]
    pub max_cached_workflows: usize,
    /// Set a [crate::WorkerTuner] for this worker, which controls how many slots are available for
    /// the different kinds of tasks.
    #[builder(default = Arc::new(TunerBuilder::default().build()))]
    pub tuner: Arc<dyn WorkerTuner + Send + Sync>,
    /// Controls how polling for Workflow tasks will happen on this worker's task queue. See also
    /// [WorkerConfig::nonsticky_to_sticky_poll_ratio]. If using SimpleMaximum, Must be at least 2
    /// when `max_cached_workflows` > 0, or is an error.
    #[builder(default = PollerBehavior::SimpleMaximum(5))]
    pub workflow_task_poller_behavior: PollerBehavior,
    /// Only applies when using [PollerBehavior::SimpleMaximum]
    ///
    /// (max workflow task polls * this number) = the number of max pollers that will be allowed for
    /// the nonsticky queue when sticky tasks are enabled. If both defaults are used, the sticky
    /// queue will allow 4 max pollers while the nonsticky queue will allow one. The minimum for
    /// either poller is 1, so if the maximum allowed is 1 and sticky queues are enabled, there will
    /// be 2 concurrent polls.
    #[builder(default = 0.2)]
    pub nonsticky_to_sticky_poll_ratio: f32,
    /// Controls how polling for Activity tasks will happen on this worker's task queue.
    #[builder(default = PollerBehavior::SimpleMaximum(5))]
    pub activity_task_poller_behavior: PollerBehavior,
    /// Controls how polling for Nexus tasks will happen on this worker's task queue.
    #[builder(default = PollerBehavior::SimpleMaximum(5))]
    pub nexus_task_poller_behavior: PollerBehavior,
    // TODO [rust-sdk-branch]: Will go away once workflow registration can only happen in here.
    //   Then it can be auto-determined.
    /// Specifies which task types this worker will poll for.
    ///
    /// Note: At least one task type must be specified or the worker will fail validation.
    #[builder(default = WorkerTaskTypes::all())]
    pub task_types: WorkerTaskTypes,
    /// How long a workflow task is allowed to sit on the sticky queue before it is timed out
    /// and moved to the non-sticky queue where it may be picked up by any worker.
    #[builder(default = Duration::from_secs(10))]
    pub sticky_queue_schedule_to_start_timeout: Duration,
    /// Longest interval for throttling activity heartbeats
    #[builder(default = Duration::from_secs(60))]
    pub max_heartbeat_throttle_interval: Duration,
    /// Default interval for throttling activity heartbeats in case
    /// `ActivityOptions.heartbeat_timeout` is unset.
    /// When the timeout *is* set in the `ActivityOptions`, throttling is set to
    /// `heartbeat_timeout * 0.8`.
    #[builder(default = Duration::from_secs(30))]
    pub default_heartbeat_throttle_interval: Duration,
    /// Sets the maximum number of activities per second the task queue will dispatch, controlled
    /// server-side. Note that this only takes effect upon an activity poll request. If multiple
    /// workers on the same queue have different values set, they will thrash with the last poller
    /// winning.
    ///
    /// Setting this to a nonzero value will also disable eager activity execution.
    pub max_task_queue_activities_per_second: Option<f64>,
    /// Limits the number of activities per second that this worker will process. The worker will
    /// not poll for new activities if by doing so it might receive and execute an activity which
    /// would cause it to exceed this limit. Negative, zero, or NaN values will cause building
    /// the options to fail.
    pub max_worker_activities_per_second: Option<f64>,
    /// Any error types listed here will cause any workflow being processed by this worker to fail,
    /// rather than simply failing the workflow task.
    #[builder(default)]
    pub workflow_failure_errors: HashSet<WorkflowErrorType>,
    /// Like [WorkerConfig::workflow_failure_errors], but specific to certain workflow types (the
    /// map key).
    #[builder(default)]
    pub workflow_types_to_failure_errors: HashMap<String, HashSet<WorkflowErrorType>>,
    /// If set, the worker will issue cancels for all outstanding activities and nexus operations after
    /// shutdown has been initiated and this amount of time has elapsed.
    pub graceful_shutdown_period: Option<Duration>,
    /// Detect nondeterministic async usage in workflow code. When enabled (the default), workflows
    /// that use external async operations (tokio timers, IO, spawned threads, raw tokio::sync
    /// channels, etc.) will have their tasks failed with a descriptive error.
    #[builder(default = true)]
    pub detect_nondeterministic_futures: bool,
}

impl<S: worker_options_builder::State> WorkerOptionsBuilder<S> {
    /// Registers all activities on an activity implementer.
    pub fn register_activities<AI: ActivityImplementer>(mut self, instance: AI) -> Self {
        self.activities.register_activities::<AI>(instance);
        self
    }
    /// Registers a specific activitiy.
    pub fn register_activity<AD>(mut self, instance: Arc<AD::Implementer>) -> Self
    where
        AD: ActivityDefinition + ExecutableActivity,
        AD::Input: Send + Sync,
        AD::Output: Send + Sync,
    {
        self.activities.register_activity::<AD>(instance);
        self
    }

    /// Registers all workflows on a workflow implementer.
    pub fn register_workflow<WI: WorkflowImplementer>(mut self) -> Self {
        self.workflows.register_workflow::<WI>();
        self
    }

    /// Register a workflow with a custom factory for instance creation.
    ///
    /// # Warning: Advanced Usage
    ///
    /// This method is intended for scenarios requiring injection of un-serializable
    /// state into workflows.
    ///
    /// **This can easily cause nondeterminism**
    ///
    /// Only use when you understand the implications and have a specific need that cannot be met
    /// otherwise.
    ///
    /// # Panics
    ///
    /// Panics if the workflow type defines an `#[init]` method. Workflows using
    /// factory registration must not have `#[init]` to avoid ambiguity about
    /// instance creation.
    pub fn register_workflow_with_factory<W, F>(mut self, factory: F) -> Self
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
        F: Fn() -> W + Send + Sync + 'static,
    {
        self.workflows
            .register_workflow_run_with_factory::<W, F>(factory);
        self
    }
}

// Needs to exist to avoid https://github.com/elastio/bon/issues/359
fn def_build_id() -> WorkerDeploymentOptions {
    WorkerDeploymentOptions::from_build_id(build_id_from_current_exe().to_owned())
}

impl WorkerOptions {
    /// Registers all activities on an activity implementer.
    pub fn register_activities<AI: ActivityImplementer>(&mut self, instance: AI) -> &mut Self {
        self.activities.register_activities::<AI>(instance);
        self
    }
    /// Registers a specific activitiy.
    pub fn register_activity<AD>(&mut self, instance: Arc<AD::Implementer>) -> &mut Self
    where
        AD: ActivityDefinition + ExecutableActivity,
        AD::Input: Send + Sync,
        AD::Output: Send + Sync,
    {
        self.activities.register_activity::<AD>(instance);
        self
    }
    /// Returns all the registered activities by cloning the current set.
    pub fn activities(&self) -> ActivityDefinitions {
        self.activities.clone()
    }

    /// Registers all workflows on a workflow implementer.
    pub fn register_workflow<WI: WorkflowImplementer>(&mut self) -> &mut Self {
        self.workflows.register_workflow::<WI>();
        self
    }

    /// Register a workflow with a custom factory for instance creation.
    ///
    /// # Warning: Advanced Usage
    /// See [WorkerOptionsBuilder::register_workflow_with_factory] for more.
    pub fn register_workflow_with_factory<W, F>(&mut self, factory: F) -> &mut Self
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
        F: Fn() -> W + Send + Sync + 'static,
    {
        self.workflows
            .register_workflow_run_with_factory::<W, F>(factory);
        self
    }

    /// Returns all the registered workflows by cloning the current set.
    pub fn workflows(&self) -> WorkflowDefinitions {
        self.workflows.clone()
    }

    #[doc(hidden)]
    pub fn to_core_options(
        &self,
        namespace: String,
        connection_identity: String,
    ) -> Result<WorkerConfig, String> {
        WorkerConfig::builder()
            .namespace(namespace)
            .task_queue(self.task_queue.clone())
            .maybe_client_identity_override(self.client_identity_override.clone().or_else(|| {
                connection_identity.is_empty().then(|| {
                    format!(
                        "{}@{}",
                        std::process::id(),
                        gethostname::gethostname().to_string_lossy()
                    )
                })
            }))
            .max_cached_workflows(self.max_cached_workflows)
            .tuner(self.tuner.clone())
            .workflow_task_poller_behavior(self.workflow_task_poller_behavior)
            .activity_task_poller_behavior(self.activity_task_poller_behavior)
            .nexus_task_poller_behavior(self.nexus_task_poller_behavior)
            .task_types(self.task_types)
            .sticky_queue_schedule_to_start_timeout(self.sticky_queue_schedule_to_start_timeout)
            .max_heartbeat_throttle_interval(self.max_heartbeat_throttle_interval)
            .default_heartbeat_throttle_interval(self.default_heartbeat_throttle_interval)
            .maybe_max_task_queue_activities_per_second(self.max_task_queue_activities_per_second)
            .maybe_max_worker_activities_per_second(self.max_worker_activities_per_second)
            .maybe_graceful_shutdown_period(self.graceful_shutdown_period)
            .versioning_strategy(WorkerVersioningStrategy::WorkerDeploymentBased(
                self.deployment_options.clone(),
            ))
            .workflow_failure_errors(self.workflow_failure_errors.clone())
            .workflow_types_to_failure_errors(self.workflow_types_to_failure_errors.clone())
            .build()
    }
}

/// A worker that can poll for and respond to workflow tasks by using
/// [temporalio_macros::workflow], and activity tasks by using activities defined with
/// [temporalio_macros::activities].
pub struct Worker {
    common: CommonWorker,
    workflow_half: WorkflowHalf,
    activity_half: ActivityHalf,
}

struct CommonWorker {
    worker: Arc<CoreWorker>,
    task_queue: String,
    worker_interceptor: Option<Box<dyn WorkerInterceptor>>,
    activity_inbound_interceptors: Vec<Arc<dyn ActivityInboundInterceptor>>,
    data_converter: DataConverter,
}

struct WorkflowHalf {
    /// Maps run id to cached workflow state
    workflows: RefCell<HashMap<String, WorkflowData>>,
    workflow_definitions: WorkflowDefinitions,
    workflow_removed_from_map: Notify,
    detect_nondeterministic_futures: bool,
}
struct WorkflowData {
    /// Channel used to send the workflow activations
    activation_chan: UnboundedSender<WorkflowActivation>,
}

struct WorkflowFutureHandle<F: Future> {
    join_handle: F,
    run_id: String,
}

#[derive(Default)]
struct ActivityHalf {
    /// Maps activity type to the function for executing activities of that type
    activities: ActivityDefinitions,
    task_tokens_to_cancels: HashMap<TaskToken, CancellationToken>,
}

impl Worker {
    /// Create a new worker from an existing connection, and options.
    pub fn new(
        runtime: &CoreRuntime,
        client: Client,
        mut options: WorkerOptions,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let acts = std::mem::take(&mut options.activities);
        let wfs = std::mem::take(&mut options.workflows);
        let wc = options
            .to_core_options(client.namespace(), client.identity())
            .map_err(|s| anyhow::anyhow!("{s}"))?;
        let core = init_worker(runtime, wc, client.connection().clone())?;
        let mut me = Self::new_from_core_definitions(
            Arc::new(core),
            client.data_converter().clone(),
            Default::default(),
            Default::default(),
        );
        me.set_detect_nondeterministic_futures(options.detect_nondeterministic_futures);
        me.activity_half.activities = acts;
        me.workflow_half.workflow_definitions = wfs;
        Ok(me)
    }

    // TODO [rust-sdk-branch]: Eliminate this constructor in favor of passing in fake connection
    #[doc(hidden)]
    pub fn new_from_core(worker: Arc<CoreWorker>, data_converter: DataConverter) -> Self {
        Self::new_from_core_definitions(
            worker,
            data_converter,
            Default::default(),
            Default::default(),
        )
    }

    // TODO [rust-sdk-branch]: Eliminate this constructor in favor of passing in fake connection
    #[doc(hidden)]
    pub fn new_from_core_definitions(
        worker: Arc<CoreWorker>,
        data_converter: DataConverter,
        activities: ActivityDefinitions,
        workflows: WorkflowDefinitions,
    ) -> Self {
        Self {
            common: CommonWorker {
                task_queue: worker.get_config().task_queue.clone(),
                worker,
                worker_interceptor: None,
                activity_inbound_interceptors: Vec::new(),
                data_converter,
            },
            workflow_half: WorkflowHalf {
                workflows: Default::default(),
                workflow_definitions: workflows,
                workflow_removed_from_map: Default::default(),
                detect_nondeterministic_futures: false,
            },
            activity_half: ActivityHalf {
                activities,
                ..Default::default()
            },
        }
    }

    /// Returns the task queue name this worker polls on
    pub fn task_queue(&self) -> &str {
        &self.common.task_queue
    }

    #[doc(hidden)]
    /// Set whether nondeterministic future detection is enabled for workflows on this worker. Users
    /// should use [WorkerOptions] to set this. TODO: Only needs to exist due to test setup.
    pub fn set_detect_nondeterministic_futures(&mut self, enabled: bool) {
        self.workflow_half.detect_nondeterministic_futures = enabled;
    }

    /// Return a handle that can be used to initiate shutdown. This is useful because [Worker::run]
    /// takes self mutably, so you may want to obtain a handle for shutting down before running.
    pub fn shutdown_handle(&self) -> impl Fn() + use<> {
        let w = self.common.worker.clone();
        move || w.initiate_shutdown()
    }

    /// Registers all activities on an activity implementer.
    pub fn register_activities<AI: ActivityImplementer>(&mut self, instance: AI) -> &mut Self {
        self.activity_half
            .activities
            .register_activities::<AI>(instance);
        self
    }
    /// Registers a specific activitiy.
    pub fn register_activity<AD>(&mut self, instance: Arc<AD::Implementer>) -> &mut Self
    where
        AD: ActivityDefinition + ExecutableActivity,
        AD::Input: Send + Sync,
        AD::Output: Send + Sync,
    {
        self.activity_half
            .activities
            .register_activity::<AD>(instance);
        self
    }

    /// Registers all workflows on a workflow implementer.
    pub fn register_workflow<WI: WorkflowImplementer>(&mut self) -> &mut Self {
        self.workflow_half
            .workflow_definitions
            .register_workflow::<WI>();
        self
    }

    /// Register a workflow with a custom factory for instance creation.
    ///
    /// See [WorkerOptionsBuilder::register_workflow_with_factory] for more.
    pub fn register_workflow_with_factory<W, F>(&mut self, factory: F) -> &mut Self
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
        F: Fn() -> W + Send + Sync + 'static,
    {
        self.workflow_half
            .workflow_definitions
            .register_workflow_run_with_factory::<W, F>(factory);
        self
    }

    /// Runs the worker. Eventually resolves after the worker has been explicitly shut down,
    /// or may return early with an error in the event of some unresolvable problem.
    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let shutdown_token = CancellationToken::new();
        let (common, wf_half, act_half) = self.split_apart();
        let (wf_future_tx, wf_future_rx) = unbounded_channel::<
            WorkflowFutureHandle<workflow_executor::TaskHandle<WorkflowResult<Payload>>>,
        >();
        let (completions_tx, completions_rx) = unbounded_channel();

        // Workflows run in a LocalSet because they use Rc<RefCell> for state management.
        // This allows them to not require Send/Sync bounds. The WorkflowExecutor replaces
        // tokio::task::spawn_local for workflow tasks and provides custom wakers for
        // nondeterminism detection.
        let workflow_local_set = tokio::task::LocalSet::new();
        let executor = WorkflowExecutor::new();

        let wf_future_joiner = async {
            UnboundedReceiverStream::new(wf_future_rx)
                .map(Result::<_, anyhow::Error>::Ok)
                .try_for_each_concurrent(
                    None,
                    |WorkflowFutureHandle {
                         join_handle,
                         run_id,
                     }| {
                        let wf_half = &*wf_half;
                        async move {
                            let result = join_handle.await.map_err(anyhow::Error::new)?;
                            // Eviction is normal workflow lifecycle - workflows loop waiting for
                            // eviction after completion to manage cache cleanup
                            if let Err(e) = result
                                && !matches!(e, WorkflowTermination::Evicted)
                            {
                                return Err(anyhow::Error::new(e));
                            }
                            debug!(run_id=%run_id, "Removing workflow from cache");
                            wf_half.workflows.borrow_mut().remove(&run_id);
                            wf_half.workflow_removed_from_map.notify_one();
                            Ok(())
                        }
                    },
                )
                .await
                .context("Workflow futures encountered an error")
        };
        let wf_completion_processor = async {
            UnboundedReceiverStream::new(completions_rx)
                .map(Ok)
                .try_for_each_concurrent(None, |mut completion| async {
                    encode_payloads(
                        &mut completion,
                        common.data_converter.codec(),
                        &SerializationContextData::Workflow,
                    )
                    .await;
                    if let Some(ref i) = common.worker_interceptor {
                        i.on_workflow_activation_completion(&completion).await;
                    }
                    common.worker.complete_workflow_activation(completion).await
                })
                .map_err(anyhow::Error::from)
                .await
                .context("Workflow completions processor encountered an error")
        };
        tokio::try_join!(
            // Workflow-related tasks run inside LocalSet (allows !Send futures)
            async {
                workflow_local_set.run_until(async {
                    tokio::try_join!(
                        // Workflow polling loop
                        async {
                            loop {
                            let mut activation =
                                match common.worker.poll_workflow_activation().await {
                                    Err(PollError::ShutDown) => {
                                        break;
                                    }
                                    o => o?,
                                };
                            decode_payloads(
                                &mut activation,
                                common.data_converter.codec(),
                                &SerializationContextData::Workflow,
                            )
                            .await;
                            if let Some(ref i) = common.worker_interceptor {
                                i.on_workflow_activation(&activation).await?;
                            }
                            if let Some(wf_fut) = wf_half
                                .workflow_activation_handler(
                                    common,
                                    shutdown_token.clone(),
                                    activation,
                                    &completions_tx,
                                    &executor,
                                )
                                .await?
                                && wf_future_tx.send(wf_fut).is_err()
                            {
                                panic!(
                                    "Receive half of completion processor channel cannot be dropped"
                                );
                            }
                            // Drive the executor so spawned tasks and sent activations make
                            // progress.
                            executor.process_tasks();
                        }
                        // Tell still-alive workflows to evict themselves
                        shutdown_token.cancel();
                        // It's important to drop these so the future and completion processors will
                        // terminate.
                        drop(wf_future_tx);
                        drop(completions_tx);
                        executor.shutdown().await;
                        Result::<_, anyhow::Error>::Ok(())
                    },
                    wf_future_joiner,
                )
                }).await
            },
            // Only poll on the activity queue if activity functions have been registered. This
            // makes tests which use mocks dramatically more manageable.
            async {
                if !act_half.activities.is_empty() {
                    loop {
                        let activity = common.worker.poll_activity_task().await;
                        if matches!(activity, Err(PollError::ShutDown)) {
                            break;
                        }
                        let mut activity = activity?;
                        decode_payloads(
                            &mut activity,
                            common.data_converter.codec(),
                            &SerializationContextData::Activity,
                        )
                        .await;
                        act_half.activity_task_handler(
                            common.worker.clone(),
                            common.task_queue.clone(),
                            common.data_converter.clone(),
                            common.activity_inbound_interceptors.clone(),
                            activity,
                        )?;
                    }
                };
                Result::<_, anyhow::Error>::Ok(())
            },
            wf_completion_processor,
        )?;

        if let Some(i) = self.common.worker_interceptor.as_ref() {
            i.on_shutdown(self);
        }
        self.common.worker.shutdown().await;
        Ok(())
    }

    /// Set a [WorkerInterceptor]
    pub fn set_worker_interceptor(&mut self, interceptor: impl WorkerInterceptor + 'static) {
        self.common.worker_interceptor = Some(Box::new(interceptor));
    }

    /// Append an [ActivityInboundInterceptor] to the chain. Interceptors run in the order they
    /// are added, outer-most first.
    pub fn add_activity_inbound_interceptor(
        &mut self,
        interceptor: impl ActivityInboundInterceptor,
    ) {
        self.common
            .activity_inbound_interceptors
            .push(Arc::new(interceptor));
    }

    /// Turns this rust worker into a new worker with all the same workflows and activities
    /// registered, but with a new underlying core worker. Can be used to swap the worker for
    /// a replay worker, change task queues, etc.
    pub fn with_new_core_worker(&mut self, new_core_worker: Arc<CoreWorker>) {
        self.common.worker = new_core_worker;
    }

    /// Returns number of currently cached workflows as understood by the SDK. Importantly, this
    /// is not the same as understood by core, though they *should* always be in sync.
    pub fn cached_workflows(&self) -> usize {
        self.workflow_half.workflows.borrow().len()
    }

    /// Returns the instance key for this worker, used for worker heartbeating.
    pub fn worker_instance_key(&self) -> Uuid {
        self.common.worker.worker_instance_key()
    }

    #[doc(hidden)]
    pub fn core_worker(&self) -> Arc<CoreWorker> {
        self.common.worker.clone()
    }

    fn split_apart(&mut self) -> (&mut CommonWorker, &mut WorkflowHalf, &mut ActivityHalf) {
        (
            &mut self.common,
            &mut self.workflow_half,
            &mut self.activity_half,
        )
    }
}

impl WorkflowHalf {
    #[allow(clippy::type_complexity)]
    async fn workflow_activation_handler(
        &self,
        common: &CommonWorker,
        shutdown_token: CancellationToken,
        mut activation: WorkflowActivation,
        completions_tx: &UnboundedSender<WorkflowActivationCompletion>,
        executor: &WorkflowExecutor,
    ) -> Result<
        Option<WorkflowFutureHandle<workflow_executor::TaskHandle<WorkflowResult<Payload>>>>,
        anyhow::Error,
    > {
        let mut res = None;
        let run_id = activation.run_id.clone();

        // If the activation is to init a workflow, create a new workflow driver for it,
        // using the function associated with that workflow id
        if let Some(sw) = activation.jobs.iter_mut().find_map(|j| match j.variant {
            Some(Variant::InitializeWorkflow(ref mut sw)) => Some(sw),
            _ => None,
        }) {
            let workflow_type = sw.workflow_type.clone();
            let (wff, activations) = {
                if let Some(factory) = self.workflow_definitions.get_workflow(&workflow_type) {
                    match WorkflowFunction::from_invocation(factory).start_workflow(
                        common.worker.get_config().namespace.clone(),
                        common.task_queue.clone(),
                        run_id.clone(),
                        std::mem::take(sw),
                        completions_tx.clone(),
                        common.data_converter.clone(),
                        self.detect_nondeterministic_futures,
                    ) {
                        Ok(result) => result,
                        Err(e) => {
                            warn!("Failed to create workflow {workflow_type}: {e}");
                            completions_tx
                                .send(WorkflowActivationCompletion::fail(
                                    run_id,
                                    format!("Failed to create workflow: {e}").into(),
                                    Some(WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure),
                                ))
                                .expect("Completion channel intact");
                            return Ok(None);
                        }
                    }
                } else {
                    warn!("Workflow type {workflow_type} not found");
                    completions_tx
                        .send(WorkflowActivationCompletion::fail(
                            run_id,
                            format!("Workflow type {workflow_type} not found").into(),
                            Some(WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure),
                        ))
                        .expect("Completion channel intact");
                    return Ok(None);
                }
            };
            // TODO [rust-sdk-branch]: Deadlock detection
            let jh = executor.spawn(async move {
                tokio::select! {
                    r = wff.fuse() => r,
                    // TODO: This probably shouldn't abort early, as it could cause an in-progress
                    //  complete to abort. Send synthetic remove activation
                    _ = shutdown_token.cancelled() => {
                        Err(WorkflowTermination::Evicted)
                    }
                }
            });
            res = Some(WorkflowFutureHandle {
                join_handle: jh,
                run_id: run_id.clone(),
            });
            loop {
                // It's possible that we've got a new initialize workflow action before the last
                // future for this run finished evicting, as a result of how futures might be
                // interleaved. In that case, just wait until it's not in the map, which should be
                // a matter of only a few `poll` calls.
                if self.workflows.borrow_mut().contains_key(&run_id) {
                    self.workflow_removed_from_map.notified().await;
                } else {
                    break;
                }
            }
            self.workflows.borrow_mut().insert(
                run_id.clone(),
                WorkflowData {
                    activation_chan: activations,
                },
            );
        }

        // The activation is expected to apply to some workflow we know about. Use it to
        // unblock things and advance the workflow.
        if let Some(dat) = self.workflows.borrow_mut().get_mut(&run_id) {
            dat.activation_chan
                .send(activation)
                .expect("Workflow should exist if we're sending it an activation");
        } else {
            // When we failed to start a workflow, we never inserted it into the cache. But core
            // sends us a `RemoveFromCache` job when we mark the StartWorkflow workflow activation
            // as a failure, which we need to complete. Other SDKs add the workflow to the cache
            // even when the workflow type is unknown/not found. To circumvent this, we simply mark
            // any RemoveFromCache job for workflows that are not in the cache as complete.
            if activation.jobs.len() == 1
                && matches!(
                    activation.jobs.first().map(|j| &j.variant),
                    Some(Some(Variant::RemoveFromCache(_)))
                )
            {
                completions_tx
                    .send(WorkflowActivationCompletion::from_cmds(run_id, vec![]))
                    .expect("Completion channel intact");
                return Ok(None);
            }

            // In all other cases, we want to error as the runtime could be in an inconsistent state
            // at this point.
            bail!("Got activation {activation:?} for unknown workflow {run_id}");
        };

        Ok(res)
    }
}

impl ActivityHalf {
    /// Spawns off a task to handle the provided activity task
    fn activity_task_handler(
        &mut self,
        worker: Arc<CoreWorker>,
        task_queue: String,
        data_converter: DataConverter,
        activity_inbound_interceptors: Vec<Arc<dyn ActivityInboundInterceptor>>,
        activity: ActivityTask,
    ) -> Result<(), anyhow::Error> {
        match activity.variant {
            Some(activity_task::Variant::Start(start)) => {
                let act_fn = self.activities.get(&start.activity_type).ok_or_else(|| {
                    anyhow!(
                        "No function registered for activity type {}",
                        start.activity_type
                    )
                })?;
                let span = info_span!(
                    "RunActivity",
                    "otel.name" = format!("RunActivity:{}", start.activity_type),
                    "otel.kind" = "server",
                    "temporalActivityID" = start.activity_id,
                    "temporalWorkflowID" = field::Empty,
                    "temporalRunID" = field::Empty,
                );
                let ct = CancellationToken::new();
                let task_token = activity.task_token;
                self.task_tokens_to_cancels
                    .insert(task_token.clone().into(), ct.clone());

                let (ctx, args) =
                    ActivityContext::new(worker.clone(), ct, task_queue, task_token.clone(), start);
                let codec_data_converter = data_converter.clone();

                tokio::spawn(async move {
                    let act_fut = async move {
                        if let Some(info) = &ctx.info().workflow_execution {
                            Span::current()
                                .record("temporalWorkflowID", &info.workflow_id)
                                .record("temporalRunID", &info.run_id);
                        }
                        (act_fn)(args, data_converter, ctx, activity_inbound_interceptors).await
                    }
                    .instrument(span);
                    let result = act_fut.await;
                    let result = match result {
                        Ok(output) => {
                            // Codec application happens at the SDK/Core boundary, so activity
                            // implementations work with the payload converter directly.
                            let pc = codec_data_converter.payload_converter();
                            let ctx = SerializationContext {
                                data: &SerializationContextData::Activity,
                                converter: pc,
                            };
                            match output.serialize_payload(&ctx) {
                                Ok(payload) => ActivityExecutionResult::ok(payload),
                                Err(err) => {
                                    activity_error_to_core_result(&codec_data_converter, err.into())
                                }
                            }
                        }
                        Err(err) => activity_error_to_core_result(&codec_data_converter, err),
                    };
                    let mut completion = ActivityTaskCompletion {
                        task_token,
                        result: Some(result),
                    };
                    encode_payloads(
                        &mut completion,
                        codec_data_converter.codec(),
                        &SerializationContextData::Activity,
                    )
                    .await;
                    worker.complete_activity_task(completion).await?;
                    Ok::<_, anyhow::Error>(())
                });
            }
            Some(activity_task::Variant::Cancel(_)) => {
                if let Some(ct) = self
                    .task_tokens_to_cancels
                    .get(activity.task_token.as_slice())
                {
                    ct.cancel();
                }
            }
            None => bail!("Undefined activity task variant"),
        }
        Ok(())
    }
}

#[derive(Debug)]
enum UnblockEvent {
    Timer(u32, TimerResult),
    Activity(u32, Box<ActivityResolution>),
    WorkflowStart(u32, Box<ChildWorkflowStartStatus>),
    WorkflowComplete(u32, Box<ChildWorkflowResult>),
    SignalExternal(u32, Option<Failure>),
    CancelExternal(u32, Option<Failure>),
    NexusOperationStart(u32, Box<resolve_nexus_operation_start::Status>),
    NexusOperationComplete(u32, Box<NexusOperationResult>),
}

/// Result of awaiting on a timer
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TimerResult {
    /// The timer was cancelled
    Cancelled,
    /// The timer elapsed and fired
    Fired,
}

/// Successful result of sending a signal to an external workflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalExternalOk;
/// Result of awaiting on sending a signal to an external workflow
pub type SignalExternalWfResult = Result<SignalExternalOk, Failure>;

/// Successful result of sending a cancel request to an external workflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelExternalOk;
/// Result of awaiting on sending a cancel request to an external workflow
pub type CancelExternalWfResult = Result<CancelExternalOk, Failure>;

trait Unblockable {
    type OtherDat;

    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self;
}

impl Unblockable for TimerResult {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::Timer(_, result) => result,
            _ => panic!("Invalid unblock event for timer"),
        }
    }
}

impl Unblockable for ActivityResolution {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::Activity(_, result) => *result,
            _ => panic!("Invalid unblock event for activity"),
        }
    }
}

impl<WD: WorkflowDefinition> Unblockable for PendingChildWorkflow<WD> {
    type OtherDat = ChildWfCommon;
    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::WorkflowStart(_, result) => Self {
                status: *result,
                common: od,
                _phantom: PhantomData,
            },
            _ => panic!("Invalid unblock event for child workflow start"),
        }
    }
}

impl Unblockable for ChildWorkflowResult {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::WorkflowComplete(_, result) => *result,
            _ => panic!("Invalid unblock event for child workflow complete"),
        }
    }
}

impl Unblockable for SignalExternalWfResult {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::SignalExternal(_, maybefail) => {
                maybefail.map_or(Ok(SignalExternalOk), Err)
            }
            _ => panic!("Invalid unblock event for signal external workflow result"),
        }
    }
}

impl Unblockable for CancelExternalWfResult {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::CancelExternal(_, maybefail) => {
                maybefail.map_or(Ok(CancelExternalOk), Err)
            }
            _ => panic!("Invalid unblock event for signal external workflow result"),
        }
    }
}

type NexusStartResult = Result<StartedNexusOperation, Failure>;
impl Unblockable for NexusStartResult {
    type OtherDat = NexusUnblockData;
    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::NexusOperationStart(_, result) => match *result {
                resolve_nexus_operation_start::Status::OperationToken(op_token) => {
                    Ok(StartedNexusOperation {
                        operation_token: Some(op_token),
                        unblock_dat: od,
                    })
                }
                resolve_nexus_operation_start::Status::StartedSync(_) => {
                    Ok(StartedNexusOperation {
                        operation_token: None,
                        unblock_dat: od,
                    })
                }
                resolve_nexus_operation_start::Status::Failed(f) => Err(f),
            },
            _ => panic!("Invalid unblock event for nexus operation"),
        }
    }
}

impl Unblockable for NexusOperationResult {
    type OtherDat = ();

    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::NexusOperationComplete(_, result) => *result,
            _ => panic!("Invalid unblock event for nexus operation complete"),
        }
    }
}

/// Identifier for cancellable operations
#[derive(Debug, Clone)]
pub(crate) enum CancellableID {
    Timer(u32),
    Activity(u32),
    LocalActivity(u32),
    ChildWorkflow {
        seqnum: u32,
        reason: String,
    },
    SignalExternalWorkflow(u32),
    /// A nexus operation (waiting for start)
    NexusOp(u32),
}

/// Cancellation IDs that support a reason.
pub(crate) trait SupportsCancelReason {
    /// Returns a new version of this ID with the provided cancellation reason.
    fn with_reason(self, reason: String) -> CancellableID;
}
#[derive(Debug, Clone)]
pub(crate) enum CancellableIDWithReason {
    ChildWorkflow { seqnum: u32 },
}
impl SupportsCancelReason for CancellableIDWithReason {
    fn with_reason(self, reason: String) -> CancellableID {
        match self {
            CancellableIDWithReason::ChildWorkflow { seqnum } => {
                CancellableID::ChildWorkflow { seqnum, reason }
            }
        }
    }
}
impl From<CancellableIDWithReason> for CancellableID {
    fn from(v: CancellableIDWithReason) -> Self {
        v.with_reason("".to_string())
    }
}

#[derive(derive_more::From)]
#[allow(clippy::large_enum_variant)]
enum RustWfCmd {
    #[from(ignore)]
    Cancel(CancellableID),
    ForceWFTFailure(anyhow::Error),
    NewCmd(CommandCreateRequest),
    NewNonblockingCmd(workflow_command::Variant),
    SubscribeChildWorkflowCompletion(CommandSubscribeChildWorkflowCompletion),
    SubscribeNexusOperationCompletion {
        seq: u32,
        unblocker: oneshot::Sender<UnblockEvent>,
    },
}

struct CommandCreateRequest {
    cmd: WorkflowCommand,
    unblocker: oneshot::Sender<UnblockEvent>,
}

struct CommandSubscribeChildWorkflowCompletion {
    seq: u32,
    unblocker: oneshot::Sender<UnblockEvent>,
}

/// The result of running a workflow.
///
/// Successful completion returns `Ok(T)` where `T` is the workflow's return type.
/// Non-error terminations (cancel, eviction, continue-as-new) return `Err(WorkflowTermination)`.
pub type WorkflowResult<T> = Result<T, WorkflowTermination>;

/// Represents ways a workflow can terminate without producing a normal result.
///
/// This is used as the error type in [`WorkflowResult<T>`] for non-error termination conditions
/// like cancellation, eviction, continue-as-new, or actual failures.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowTermination {
    /// The workflow was cancelled.
    #[error("Workflow cancelled")]
    Cancelled,

    /// The workflow was evicted from the cache.
    #[error("Workflow evicted from cache")]
    Evicted,

    /// The workflow should continue as a new execution.
    #[error("Continue as new")]
    ContinueAsNew(Box<ContinueAsNewWorkflowExecution>),

    /// The workflow failed with an error.
    #[error("Workflow failed: {0}")]
    Failed(#[source] OutgoingWorkflowError),
}

impl WorkflowTermination {
    /// Construct a [WorkflowTermination::ContinueAsNew]
    pub fn continue_as_new(can: ContinueAsNewWorkflowExecution) -> Self {
        Self::ContinueAsNew(Box::new(can))
    }

    /// Construct a [WorkflowTermination::Failed] variant from an application failure.
    pub fn failed_application(err: ApplicationFailure) -> Self {
        Self::Failed(err.into())
    }
}

impl From<anyhow::Error> for WorkflowTermination {
    fn from(err: anyhow::Error) -> Self {
        Self::Failed(err.into())
    }
}

impl From<temporalio_common::data_converters::PayloadConversionError> for WorkflowTermination {
    fn from(err: temporalio_common::data_converters::PayloadConversionError) -> Self {
        Self::Failed(err.into())
    }
}

impl From<workflows::WorkflowError> for WorkflowTermination {
    fn from(err: workflows::WorkflowError) -> Self {
        match err {
            workflows::WorkflowError::PayloadConversion(e) => Self::from(e),
            workflows::WorkflowError::Execution(e) => Self::from(e),
        }
    }
}

impl From<ActivityExecutionError> for WorkflowTermination {
    fn from(value: ActivityExecutionError) -> Self {
        Self::Failed(value.into())
    }
}

impl From<ChildWorkflowExecutionError> for WorkflowTermination {
    fn from(value: ChildWorkflowExecutionError) -> Self {
        Self::Failed(value.into())
    }
}

impl From<ChildWorkflowStartError> for WorkflowTermination {
    fn from(value: ChildWorkflowStartError) -> Self {
        Self::Failed(value.into())
    }
}

impl From<ChildWorkflowSignalError> for WorkflowTermination {
    fn from(value: ChildWorkflowSignalError) -> Self {
        Self::Failed(value.into())
    }
}

/// Activity functions may return these values when exiting
#[derive(Debug)]
pub enum ActExitValue<T> {
    /// Completion requires an asynchronous callback
    WillCompleteAsync,
    /// Finish with a result
    Normal(T),
}

impl<T: AsJsonPayloadExt> From<T> for ActExitValue<T> {
    fn from(t: T) -> Self {
        Self::Normal(t)
    }
}

/// Attempts to turn caught panics into something printable
fn panic_formatter(panic: Box<dyn Any>) -> Box<dyn Display> {
    _panic_formatter::<&str>(panic)
}
fn _panic_formatter<T: 'static + PrintablePanicType>(panic: Box<dyn Any>) -> Box<dyn Display> {
    match panic.downcast::<T>() {
        Ok(d) => d,
        Err(orig) => {
            if TypeId::of::<<T as PrintablePanicType>::NextType>()
                == TypeId::of::<EndPrintingAttempts>()
            {
                return Box::new("Couldn't turn panic into a string");
            }
            _panic_formatter::<T::NextType>(orig)
        }
    }
}
trait PrintablePanicType: Display {
    type NextType: PrintablePanicType;
}

impl PrintablePanicType for &str {
    type NextType = String;
}
impl PrintablePanicType for String {
    type NextType = EndPrintingAttempts;
}
struct EndPrintingAttempts {}
impl Display for EndPrintingAttempts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Will never be printed")
    }
}
impl PrintablePanicType for EndPrintingAttempts {
    type NextType = EndPrintingAttempts;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activities::ActivityError;
    use temporalio_macros::{activities, workflow, workflow_methods};

    struct MyActivities {}

    #[activities]
    impl MyActivities {
        #[activity]
        async fn my_activity(_ctx: ActivityContext) -> Result<(), ActivityError> {
            Ok(())
        }

        #[activity]
        async fn takes_self(
            self: Arc<Self>,
            _ctx: ActivityContext,
            _: String,
        ) -> Result<(), ActivityError> {
            Ok(())
        }
    }

    #[test]
    fn test_activity_registration() {
        let act_instance = MyActivities {};
        let _ = WorkerOptions::new("task_q").register_activities(act_instance);
    }

    // Compile-only test for workflow context invocation
    #[allow(unused, clippy::diverging_sub_expression)]
    fn test_activity_via_workflow_context() {
        let wf_ctx: WorkflowContext<MyWorkflow> = unimplemented!();
        wf_ctx.start_activity(
            MyActivities::my_activity,
            (),
            ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
        );
        wf_ctx.start_activity(
            MyActivities::takes_self,
            "Hi".to_owned(),
            ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
        );
    }

    // Compile-only test for direct invocation via .run()
    #[allow(dead_code, unreachable_code, unused, clippy::diverging_sub_expression)]
    async fn test_activity_direct_invocation() {
        let ctx: ActivityContext = unimplemented!();
        let _result = MyActivities::my_activity.run(ctx).await;
    }

    #[workflow]
    struct MyWorkflow {
        counter: u32,
    }

    #[allow(dead_code)]
    #[workflow_methods]
    impl MyWorkflow {
        #[init]
        fn new(_ctx: &WorkflowContextView, _input: String) -> Self {
            Self { counter: 0 }
        }

        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
            Ok(format!("Counter: {}", ctx.state(|s| s.counter)))
        }

        #[signal(name = "increment")]
        fn increment_counter(&mut self, _ctx: &mut SyncWorkflowContext<Self>, amount: u32) {
            self.counter += amount;
        }

        #[signal]
        async fn async_signal(_ctx: &mut WorkflowContext<Self>) {}

        #[query]
        fn get_counter(&self, _ctx: &WorkflowContextView) -> u32 {
            self.counter
        }

        #[update(name = "double")]
        fn double_counter(&mut self, _ctx: &mut SyncWorkflowContext<Self>) -> u32 {
            self.counter *= 2;
            self.counter
        }

        #[update]
        async fn async_update(_ctx: &mut WorkflowContext<Self>, val: i32) -> i32 {
            val * 2
        }
    }

    #[test]
    fn test_workflow_registration() {
        let _ = WorkerOptions::new("task_q").register_workflow::<MyWorkflow>();
    }

    fn default_identity() -> String {
        format!(
            "{}@{}",
            std::process::id(),
            gethostname::gethostname().to_string_lossy()
        )
    }

    #[rstest::rstest]
    #[case::default_when_none_provided(None, "", Some(default_identity()))]
    #[case::connection_identity_preserved(None, "conn-identity", None)]
    #[case::worker_override_takes_precedence(
        Some("worker-identity"),
        "conn-identity",
        Some("worker-identity".into())
    )]
    #[case::worker_override_with_empty_connection(
        Some("worker-identity"),
        "",
        Some("worker-identity".into())
    )]
    #[test]
    fn client_identity_resolution(
        #[case] worker_override: Option<&str>,
        #[case] connection_identity: &str,
        #[case] expected: Option<String>,
    ) {
        let opts = WorkerOptions::new("task_q")
            .task_types(WorkerTaskTypes::activity_only())
            .maybe_client_identity_override(worker_override.map(|s| s.to_owned()))
            .build();
        let config = opts
            .to_core_options("ns".into(), connection_identity.into())
            .unwrap();
        assert_eq!(config.client_identity_override, expected);
    }
}
