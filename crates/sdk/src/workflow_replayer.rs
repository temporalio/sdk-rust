use crate::{
    Worker, WorkerOptions,
    interceptors::WorkerInterceptor,
    plugins::WorkerPlugin,
    runtime::WorkflowErrorType,
    workflow_interceptors::WorkflowInterceptorConstructor,
    workflow_registry::{WorkflowDefinitions, WorkflowRegistrationError},
};
use anyhow::anyhow;
use futures_util::stream;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use temporalio_client::{ClientOptions, PluginApplyError, WorkflowHistory};
use temporalio_common::{
    WorkflowDefinition,
    data_converters::DataConverter,
    protos::{
        coresdk::workflow_activation::{
            WorkflowActivation, remove_from_cache::EvictionReason,
            workflow_activation_job::Variant as ActivationVariant,
        },
        temporal::api::history::v1::{History, history_event::Attributes},
    },
};
use temporalio_sdk_core::{
    init_replay_worker,
    replay::{HistoryForReplay, HistoryInfo, ReplayWorkerInput},
};
use temporalio_workflow::{PatchActivationCallback, runtime::entry::WorkflowImplementation};

#[cfg(feature = "wasm-workflows")]
use crate::WasmWorkflowComponent;

const DEFAULT_REPLAY_NAMESPACE: &str = "ReplayNamespace";
const DEFAULT_REPLAY_TASK_QUEUE: &str = "ReplayTaskQueue";
const DEFAULT_REPLAY_WORKFLOW_ID: &str = "replay-workflow";

/// Options for constructing a workflow replayer.
#[derive(bon::Builder, Clone)]
#[builder(start_fn = new, on(String, into), state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct WorkflowReplayerOptions {
    #[builder(field)]
    pub(super) workflows: WorkflowDefinitions,

    #[builder(field)]
    pub(super) worker_interceptors: Vec<Arc<dyn WorkerInterceptor>>,

    #[builder(field)]
    pub(super) workflow_interceptor_constructors: Vec<WorkflowInterceptorConstructor>,

    #[builder(field)]
    pub(super) worker_plugins: Vec<Arc<dyn WorkerPlugin>>,

    #[cfg(feature = "wasm-workflows")]
    #[builder(field)]
    pub(super) wasm_workflow_components: Vec<WasmWorkflowComponent>,

    /// Namespace exposed to workflow code during replay.
    #[builder(default = DEFAULT_REPLAY_NAMESPACE.to_owned())]
    pub namespace: String,

    /// Task queue exposed to workflow code during replay.
    #[builder(default = DEFAULT_REPLAY_TASK_QUEUE.to_owned())]
    pub task_queue: String,

    /// Data converter used for workflow payloads.
    #[builder(default)]
    pub data_converter: DataConverter,

    /// Worker-level workflow errors that should fail workflow executions.
    #[builder(default)]
    pub workflow_failure_errors: HashSet<WorkflowErrorType>,

    /// Per-workflow-type errors that should fail workflow executions.
    #[builder(default)]
    pub workflow_types_to_failure_errors: HashMap<String, HashSet<WorkflowErrorType>>,

    /// Whether to detect nondeterministic future usage in workflow code.
    #[builder(default = true)]
    pub detect_nondeterministic_futures: bool,

    /// Callback controlling first non-replay patch decisions.
    pub patch_activation_callback: Option<PatchActivationCallback>,
}

impl<S: workflow_replayer_options_builder::State> WorkflowReplayerOptionsBuilder<S> {
    /// Register a worker plugin with this replayer.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn worker_plugin<P: WorkerPlugin>(mut self, plugin: P) -> Self {
        self.worker_plugins.push(Arc::new(plugin));
        self
    }

    /// Append a worker interceptor used during replay.
    pub fn worker_interceptor<I: WorkerInterceptor + 'static>(mut self, interceptor: I) -> Self {
        self.worker_interceptors.push(Arc::new(interceptor));
        self
    }

    /// Append a workflow interceptor constructor used during replay.
    pub fn workflow_interceptor(mut self, constructor: WorkflowInterceptorConstructor) -> Self {
        self.workflow_interceptor_constructors.push(constructor);
        self
    }

    /// Register a workflow implementation for replay.
    pub fn register_workflow<W>(mut self) -> Result<Self, WorkflowRegistrationError>
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
    {
        self.workflows.register_workflow::<W>()?;
        Ok(self)
    }

    /// Register a workflow using a custom instance factory.
    pub fn register_workflow_with_factory<W, F>(
        mut self,
        factory: F,
    ) -> Result<Self, WorkflowRegistrationError>
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
        F: Fn() -> W + Send + Sync + 'static,
    {
        self.workflows
            .register_workflow_run_with_factory::<W, F>(factory)?;
        Ok(self)
    }

    /// Set the ordered constructors used to create workflow interceptors for each workflow instance.
    ///
    /// This replaces any previously configured workflow interceptor constructors.
    pub fn register_workflow_interceptors(
        mut self,
        constructors: Vec<WorkflowInterceptorConstructor>,
    ) -> Self {
        self.workflow_interceptor_constructors = constructors;
        self
    }

    /// Get a mutable reference to the workflow interceptor constructors list.
    pub fn workflow_interceptor_constructors_mut(
        &mut self,
    ) -> &mut Vec<WorkflowInterceptorConstructor> {
        &mut self.workflow_interceptor_constructors
    }

    /// Register a prebuilt WASM workflow component for replay.
    #[cfg(feature = "wasm-workflows")]
    pub fn register_wasm_workflow(mut self, component: WasmWorkflowComponent) -> Self {
        self.wasm_workflow_components.push(component);
        self
    }
}

impl WorkflowReplayerOptions {
    /// Append a worker interceptor used during replay.
    pub fn worker_interceptor<I: WorkerInterceptor + 'static>(
        &mut self,
        interceptor: I,
    ) -> &mut Self {
        self.worker_interceptors.push(Arc::new(interceptor));
        self
    }

    /// Append a workflow interceptor constructor used during replay.
    pub fn workflow_interceptor(
        &mut self,
        constructor: WorkflowInterceptorConstructor,
    ) -> &mut Self {
        self.workflow_interceptor_constructors.push(constructor);
        self
    }

    /// Register a workflow implementation for replay.
    pub fn register_workflow<W>(&mut self) -> Result<&mut Self, WorkflowRegistrationError>
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
    {
        self.workflows.register_workflow::<W>()?;
        Ok(self)
    }

    /// Register a workflow using a custom instance factory.
    pub fn register_workflow_with_factory<W, F>(
        &mut self,
        factory: F,
    ) -> Result<&mut Self, WorkflowRegistrationError>
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
        F: Fn() -> W + Send + Sync + 'static,
    {
        self.workflows
            .register_workflow_run_with_factory::<W, F>(factory)?;
        Ok(self)
    }

    /// Set the ordered constructors used to create workflow interceptors for each workflow instance.
    ///
    /// This replaces any previously configured workflow interceptor constructors.
    pub fn register_workflow_interceptors(
        &mut self,
        constructors: Vec<WorkflowInterceptorConstructor>,
    ) -> &mut Self {
        self.workflow_interceptor_constructors = constructors;
        self
    }

    /// Register a prebuilt WASM workflow component for replay.
    #[cfg(feature = "wasm-workflows")]
    pub fn register_wasm_workflow(&mut self, component: WasmWorkflowComponent) -> &mut Self {
        self.wasm_workflow_components.push(component);
        self
    }

    /// Returns all the registered workflows by cloning the current set.
    pub fn workflows(&self) -> WorkflowDefinitions {
        self.workflows.clone()
    }
}

/// A failure attributable to one workflow history during replay.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowReplayFailure {
    /// The history could not be replayed because it was malformed or incomplete.
    #[error("invalid workflow history: {message}")]
    InvalidHistory {
        /// Validation failure details.
        message: String,
    },
    /// Workflow code was incompatible with recorded history.
    #[error("workflow replay was nondeterministic: {message}")]
    Nondeterminism {
        /// Nondeterminism details reported by Core.
        message: String,
    },
    /// Language workflow execution failed while processing a task.
    #[error("workflow task failed during replay: {message}")]
    WorkflowTaskFailure {
        /// Workflow task failure details.
        message: String,
    },
    /// Replay ended for an unexpected internal reason.
    #[error("workflow replay failed internally ({reason}): {message}")]
    Internal {
        /// Core eviction reason.
        reason: String,
        /// Eviction details reported by Core.
        message: String,
    },
}

/// Outcome of replaying one workflow history.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WorkflowReplayResult {
    /// History supplied to the replayer.
    pub history: WorkflowHistory,
    /// Replay failure, or `None` when the workflow code is compatible with the history.
    pub replay_failure: Option<WorkflowReplayFailure>,
}

/// Error creating or running a workflow replayer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowReplayError {
    /// A plugin failed while configuring replay options.
    #[error(transparent)]
    Plugin(#[from] PluginApplyError),
    /// No workflow definitions were registered after plugin configuration.
    #[error("at least one workflow must be registered for replay")]
    NoWorkflowsRegistered,
    /// The replay worker could not be initialized.
    #[error("workflow replay initialization failed: {0}")]
    Initialization(#[source] anyhow::Error),
    /// The replay worker stopped before producing trustworthy results.
    #[error("workflow replay worker failed: {0}")]
    Worker(#[source] anyhow::Error),
    /// A single-history replay failed.
    #[error(transparent)]
    Replay(#[from] WorkflowReplayFailure),
}

/// Replays workflow histories against registered workflow implementations.
pub struct WorkflowReplayer {
    options: WorkflowReplayerOptions,
}

impl WorkflowReplayer {
    /// Construct a replayer and apply its worker plugins.
    pub fn new(mut options: WorkflowReplayerOptions) -> Result<Self, WorkflowReplayError> {
        crate::plugins::apply_workflow_replayer_plugins(&mut options)?;
        if options.workflows.is_empty() {
            return Err(WorkflowReplayError::NoWorkflowsRegistered);
        }
        Ok(Self { options })
    }

    /// Return the configured replay options.
    pub fn options(&self) -> &WorkflowReplayerOptions {
        &self.options
    }

    /// Replay one history and return an error if it is incompatible with workflow code.
    pub async fn replay_workflow(
        &self,
        history: WorkflowHistory,
    ) -> Result<(), WorkflowReplayError> {
        let mut results = self.replay_workflows([history]).await?;
        let result = results.pop().ok_or_else(|| {
            WorkflowReplayError::Worker(anyhow!("replay produced no result for its history"))
        })?;
        match result.replay_failure {
            Some(failure) => Err(failure.into()),
            None => Ok(()),
        }
    }

    /// Replay histories using one replay worker and return outcomes in input order.
    pub async fn replay_workflows(
        &self,
        histories: impl IntoIterator<Item = WorkflowHistory>,
    ) -> Result<Vec<WorkflowReplayResult>, WorkflowReplayError> {
        self.replay_workflows_internal(histories.into_iter().collect())
            .await
    }

    async fn replay_workflows_internal(
        &self,
        histories: Vec<WorkflowHistory>,
    ) -> Result<Vec<WorkflowReplayResult>, WorkflowReplayError> {
        let mut results = histories
            .into_iter()
            .map(|history| WorkflowReplayResult {
                history,
                replay_failure: None,
            })
            .collect::<Vec<_>>();
        let mut valid_indexes = Vec::new();
        let mut core_histories = Vec::new();

        for (index, result) in results.iter_mut().enumerate() {
            match validate_history(&result.history) {
                Ok(history) => {
                    valid_indexes.push(index);
                    core_histories.push(history);
                }
                Err(failure) => result.replay_failure = Some(failure),
            }
        }

        if core_histories.is_empty() {
            return Ok(results);
        }

        let recorded_outcomes = Arc::new(Mutex::new(Vec::new()));
        let observer = ReplayOutcomeInterceptor {
            outcomes: recorded_outcomes.clone(),
        };
        let mut worker_options = WorkerOptions::new(self.options.task_queue.clone()).build();
        worker_options.workflows = self.options.workflows.clone();
        worker_options.worker_interceptors = self.options.worker_interceptors.clone();
        worker_options
            .worker_interceptors
            .insert(0, Arc::new(observer));
        worker_options.workflow_interceptor_constructors =
            self.options.workflow_interceptor_constructors.clone();
        worker_options.worker_plugins = self.options.worker_plugins.clone();
        worker_options.workflow_failure_errors = self.options.workflow_failure_errors.clone();
        worker_options.workflow_types_to_failure_errors =
            self.options.workflow_types_to_failure_errors.clone();
        worker_options.detect_nondeterministic_futures =
            self.options.detect_nondeterministic_futures;
        worker_options.patch_activation_callback = self.options.patch_activation_callback.clone();
        #[cfg(feature = "wasm-workflows")]
        {
            worker_options.wasm_workflow_components = self.options.wasm_workflow_components.clone();
        }

        let core_options = worker_options
            .to_core_options(self.options.namespace.clone(), String::new())
            .map_err(|error| WorkflowReplayError::Initialization(anyhow!(error)))?;
        let core_worker = init_replay_worker(ReplayWorkerInput::new(
            core_options,
            stream::iter(core_histories),
        ))
        .map_err(WorkflowReplayError::Initialization)?;
        let client_options = ClientOptions::new(self.options.namespace.clone())
            .data_converter(self.options.data_converter.clone())
            .build();
        let mut worker = Worker::new_from_core_options_prepared(
            Arc::new(core_worker),
            client_options,
            worker_options,
        )
        .map_err(|error| WorkflowReplayError::Initialization(anyhow!(error)))?;

        if let Err(source) = worker.run().await {
            let core_worker = worker.core_worker();
            core_worker.initiate_shutdown();
            core_worker.shutdown().await;
            return Err(WorkflowReplayError::Worker(source));
        }

        let outcomes = std::mem::take(&mut *recorded_outcomes.lock());
        if outcomes.len() != valid_indexes.len() {
            return Err(WorkflowReplayError::Worker(anyhow!(
                "replay produced {} outcomes for {} valid histories",
                outcomes.len(),
                valid_indexes.len()
            )));
        }
        for (index, replay_failure) in valid_indexes.into_iter().zip(outcomes) {
            results[index].replay_failure = replay_failure;
        }
        Ok(results)
    }
}

fn validate_history(history: &WorkflowHistory) -> Result<HistoryForReplay, WorkflowReplayFailure> {
    let proto = History {
        events: history.events().to_vec(),
    };
    HistoryInfo::new_from_history(&proto, None).map_err(|error| {
        WorkflowReplayFailure::InvalidHistory {
            message: error.to_string(),
        }
    })?;
    let attributes = match proto
        .events
        .first()
        .and_then(|event| event.attributes.as_ref())
    {
        Some(Attributes::WorkflowExecutionStartedEventAttributes(attributes)) => attributes,
        _ => {
            return Err(WorkflowReplayFailure::InvalidHistory {
                message: "first event is not WorkflowExecutionStarted".to_owned(),
            });
        }
    };
    if attributes.original_execution_run_id.is_empty() {
        return Err(WorkflowReplayFailure::InvalidHistory {
            message: "workflow start event has no original execution run ID".to_owned(),
        });
    }
    if attributes
        .task_queue
        .as_ref()
        .is_none_or(|task_queue| task_queue.name.is_empty())
    {
        return Err(WorkflowReplayFailure::InvalidHistory {
            message: "workflow start event has no task queue".to_owned(),
        });
    }
    Ok(HistoryForReplay::new(
        proto,
        history.workflow_id().unwrap_or(DEFAULT_REPLAY_WORKFLOW_ID),
    ))
}

struct ReplayOutcomeInterceptor {
    outcomes: Arc<Mutex<Vec<Option<WorkflowReplayFailure>>>>,
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for ReplayOutcomeInterceptor {
    async fn on_workflow_activation(
        &self,
        activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        let Some(remove) = activation.jobs.iter().find_map(|job| match &job.variant {
            Some(ActivationVariant::RemoveFromCache(remove)) => Some(remove),
            _ => None,
        }) else {
            return Ok(());
        };
        let reason = remove.reason();
        let failure = match reason {
            EvictionReason::CacheFull | EvictionReason::LangRequested => None,
            EvictionReason::Nondeterminism => Some(WorkflowReplayFailure::Nondeterminism {
                message: remove.message.clone(),
            }),
            EvictionReason::LangFail => Some(WorkflowReplayFailure::WorkflowTaskFailure {
                message: remove.message.clone(),
            }),
            reason => Some(WorkflowReplayFailure::Internal {
                reason: format!("{reason:?}"),
                message: remove.message.clone(),
            }),
        };
        self.outcomes.lock().push(failure);
        Ok(())
    }
}
