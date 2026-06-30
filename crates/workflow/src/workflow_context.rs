mod options;

pub use options::{
    ActivityCloseTimeouts, ActivityOptions, ChildWorkflowOptions, ContinueAsNewOptions,
    ContinueAsNewVersioningBehavior, LocalActivityOptions, NexusOperationOptions, Signal,
    SignalData, TimerOptions,
};
pub use temporalio_common_wasm::protos::coresdk::child_workflow::StartChildWorkflowExecutionFailedCause;

use crate::runtime::{
    SdkGuardedFuture, SdkWakeGuard,
    entry::WorkflowImplementation,
    host::WorkflowHost,
    model::{
        CancelExternalWfResult, CancellableID, NexusStartResult, SignalExternalWfResult,
        TimerResult, UnblockEvent, Unblockable, WorkflowTermination,
    },
};
use futures_channel::oneshot;
use futures_util::{
    FutureExt,
    future::{FusedFuture, Shared},
    task::Context,
};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::{self, Future},
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    sync::atomic::{AtomicBool, Ordering},
    task::{Poll, Waker},
    time::{Duration, SystemTime},
};
use temporalio_common_wasm::{
    ActivityDefinition, SignalDefinition, WorkflowDefinition,
    data_converters::{
        ActivityExecutionDecodeHint, ChildWorkflowExecutionDecodeHint,
        ChildWorkflowStartDecodeHint, DataConverter, GenericPayloadConverter, PayloadConverter,
        SerializationContext, SerializationContextData, TemporalDeserializable,
        WorkflowSignalDecodeHint,
    },
    error::{
        ActivityExecutionError, ChildWorkflowExecutionError, ChildWorkflowStartError,
        WorkflowSignalError,
    },
    protos::{
        coresdk::{
            activity_result::{ActivityResolution, Cancellation, activity_resolution},
            child_workflow::{ChildWorkflowResult, child_workflow_result},
            common::NamespacedWorkflowExecution,
            nexus::NexusOperationResult,
            workflow_activation::{
                InitializeWorkflow, WorkflowActivation as CoreWorkflowActivation,
                resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
                workflow_activation_job::Variant as ActivationVariant,
            },
            workflow_commands::{
                CancelChildWorkflowExecution, CancelSignalWorkflow, CancelTimer,
                ModifyWorkflowProperties, RequestCancelActivity,
                RequestCancelExternalWorkflowExecution, RequestCancelLocalActivity,
                RequestCancelNexusOperation, SetPatchMarker, SignalExternalWorkflowExecution,
                UpsertWorkflowSearchAttributes, signal_external_workflow_execution,
                workflow_command,
            },
        },
        temporal::api::{
            common::v1::{Memo, Payload, SearchAttributes as ProtoSearchAttributes},
            failure::v1::{CanceledFailureInfo, Failure, failure::FailureInfo},
        },
        utilities::TryIntoOrNone,
    },
    search_attributes::{SearchAttributeUpdate, SearchAttributes},
    worker::WorkerDeploymentVersion,
};

/// Non-generic base context containing all workflow execution infrastructure.
///
/// This is used internally by futures and commands that don't need typed workflow state.
#[derive(Clone)]
pub struct BaseWorkflowContext {
    inner: Rc<WorkflowContextInner>,
}
impl BaseWorkflowContext {
    pub(crate) fn apply_activation_context(&self, activation: &CoreWorkflowActivation) {
        let mut shared = self.inner.shared.borrow_mut();
        shared.activation = activation.clone();
        if let Some(seed) = activation.jobs.iter().find_map(|job| match &job.variant {
            Some(ActivationVariant::UpdateRandomSeed(attrs)) => Some(attrs.randomness_seed),
            _ => None,
        }) {
            shared.random_seed = seed;
        }
    }

    /// Returns the [`DataConverter`] associated with this workflow's worker.
    pub fn data_converter(&self) -> &DataConverter {
        &self.inner.data_converter
    }

    pub(crate) fn record_patch(&self, patch_id: String, present: bool) {
        self.inner
            .shared
            .borrow_mut()
            .changes
            .insert(patch_id, present);
    }

    /// Create a read-only view of this context.
    pub(crate) fn view(&self) -> WorkflowContextView {
        WorkflowContextView::new(
            self.inner.namespace.clone(),
            self.inner.task_queue.clone(),
            self.inner.run_id.clone(),
            &self.inner.inital_information,
        )
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum PendingCommandId {
    Timer(u32),
    Activity(u32),
    ChildWorkflowStart(u32),
    ChildWorkflowComplete(u32),
    SignalExternal(u32),
    CancelExternal(u32),
    NexusOpStart(u32),
    NexusOpComplete(u32),
}

impl PendingCommandId {
    fn from_unblock_event(event: &UnblockEvent) -> Self {
        match event {
            UnblockEvent::Timer(seq, _) => Self::Timer(*seq),
            UnblockEvent::Activity(seq, _) => Self::Activity(*seq),
            UnblockEvent::WorkflowStart(seq, _) => Self::ChildWorkflowStart(*seq),
            UnblockEvent::WorkflowComplete(seq, _) => Self::ChildWorkflowComplete(*seq),
            UnblockEvent::SignalExternal(seq, _) => Self::SignalExternal(*seq),
            UnblockEvent::CancelExternal(seq, _) => Self::CancelExternal(*seq),
            UnblockEvent::NexusOperationStart(seq, _) => Self::NexusOpStart(*seq),
            UnblockEvent::NexusOperationComplete(seq, _) => Self::NexusOpComplete(*seq),
        }
    }
}

struct WorkflowRuntimeState {
    host: Rc<dyn WorkflowHost>,
    pending_unblocks: RefCell<HashMap<PendingCommandId, oneshot::Sender<UnblockEvent>>>,
    forced_wft_failure: RefCell<Option<Box<dyn std::error::Error + Send + Sync>>>,
    progress_made: Cell<bool>,
}

impl WorkflowRuntimeState {
    fn new(host: Rc<dyn WorkflowHost>) -> Self {
        Self {
            host,
            pending_unblocks: RefCell::new(HashMap::new()),
            forced_wft_failure: RefCell::new(None),
            progress_made: Cell::new(false),
        }
    }

    fn register_unblocker(&self, id: PendingCommandId, unblocker: oneshot::Sender<UnblockEvent>) {
        self.pending_unblocks.borrow_mut().insert(id, unblocker);
    }

    fn unblock(&self, event: UnblockEvent) -> Result<(), anyhow::Error> {
        let id = PendingCommandId::from_unblock_event(&event);
        let unblocker = self
            .pending_unblocks
            .borrow_mut()
            .remove(&id)
            .ok_or_else(|| anyhow::anyhow!("Command {id:?} not found to unblock"))?;
        self.progress_made.set(true);
        let _guard = SdkWakeGuard::new();
        let _ = unblocker.send(event);
        Ok(())
    }

    fn maybe_unblock(&self, event: UnblockEvent) -> bool {
        let id = PendingCommandId::from_unblock_event(&event);
        let Some(unblocker) = self.pending_unblocks.borrow_mut().remove(&id) else {
            return false;
        };
        self.progress_made.set(true);
        let _guard = SdkWakeGuard::new();
        let _ = unblocker.send(event);
        true
    }

    fn set_forced_wft_failure(&self, err: Box<dyn std::error::Error + Send + Sync>) {
        *self.forced_wft_failure.borrow_mut() = Some(err);
        self.progress_made.set(true);
    }

    fn take_forced_wft_failure(&self) -> Option<Box<dyn std::error::Error + Send + Sync>> {
        self.forced_wft_failure.borrow_mut().take()
    }

    fn mark_progress(&self) {
        self.progress_made.set(true);
    }

    fn take_progress(&self) -> bool {
        self.progress_made.replace(false)
    }
}

struct WorkflowContextInner {
    namespace: String,
    task_queue: String,
    run_id: String,
    inital_information: InitializeWorkflow,
    runtime: WorkflowRuntimeState,
    cancelled_reason: RefCell<Option<String>>,
    cancel_wakers: RefCell<Vec<Waker>>,
    shared: RefCell<WorkflowContextSharedData>,
    seq_nums: RefCell<WfCtxProtectedDat>,
    data_converter: DataConverter,
    state_mutated: Cell<bool>,
}

/// Context provided to synchronous signal and update handlers.
///
/// This type provides all workflow context capabilities except `state()`, `state_mut()`,
/// and `wait_condition()`. Those methods are not applicable in sync handler contexts.
///
/// Sync handlers receive `&mut self` directly, so they can reference and mutate workflow state without
/// needing `state()`/`state_mut()`.
pub struct SyncWorkflowContext<W> {
    base: BaseWorkflowContext,
    /// Headers from the current handler invocation (signal, update, etc.)
    headers: Rc<HashMap<String, Payload>>,
    _phantom: PhantomData<W>,
}

impl<W> Clone for SyncWorkflowContext<W> {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            headers: self.headers.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Used within workflows to issue commands, get info, etc.
///
/// The type parameter `W` represents the workflow type. This enables type-safe
/// access to workflow state via `state_mut()` for mutations.
pub struct WorkflowContext<W> {
    sync: SyncWorkflowContext<W>,
    /// The workflow instance
    workflow_state: Rc<RefCell<W>>,
    /// Wakers registered by `wait_condition` futures. Drained and woken on
    /// every `state_mut` call so that waker-based combinators (e.g.
    /// `FuturesOrdered`) re-poll the condition after state changes.
    condition_wakers: Rc<RefCell<Vec<Waker>>>,
}

impl<W> Clone for WorkflowContext<W> {
    fn clone(&self) -> Self {
        Self {
            sync: self.sync.clone(),
            workflow_state: self.workflow_state.clone(),
            condition_wakers: self.condition_wakers.clone(),
        }
    }
}

/// Read-only view of workflow context for use in init and query handlers.
///
/// This provides access to workflow information but cannot issue commands.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WorkflowContextView {
    /// The workflow's unique identifier
    pub workflow_id: String,
    /// The run id of this workflow execution
    pub run_id: String,
    /// The workflow type name
    pub workflow_type: String,
    /// The task queue this workflow is executing on
    pub task_queue: String,
    /// The namespace this workflow is executing in
    pub namespace: String,

    /// The current attempt number (starting from 1)
    pub attempt: u32,
    /// The run id of the very first execution in the chain
    pub first_execution_run_id: String,
    /// The run id of the previous execution if this is a continuation
    pub continued_from_run_id: Option<String>,

    /// When the workflow execution started
    pub start_time: Option<SystemTime>,
    /// Total workflow execution timeout including retries and continue as new
    pub execution_timeout: Option<Duration>,
    /// Timeout of a single workflow run
    pub run_timeout: Option<Duration>,
    /// Timeout of a single workflow task
    pub task_timeout: Option<Duration>,

    /// Information about the parent workflow, if this is a child workflow
    pub parent: Option<ParentWorkflowInfo>,
    /// Information about the root workflow in the execution chain
    pub root: Option<RootWorkflowInfo>,

    /// The workflow's retry policy
    pub retry_policy:
        Option<temporalio_common_wasm::protos::temporal::api::common::v1::RetryPolicy>,
    /// If this workflow runs on a cron schedule
    pub cron_schedule: Option<String>,
    /// User-defined memo
    pub memo: Option<Memo>,
    /// Initial search attributes as a typed collection.
    pub search_attributes: Option<SearchAttributes>,
}

/// Information about a parent workflow.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ParentWorkflowInfo {
    /// The parent workflow's unique identifier
    pub workflow_id: String,
    /// The parent workflow's run id
    pub run_id: String,
    /// The parent workflow's namespace
    pub namespace: String,
}

/// Information about the root workflow in an execution chain.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RootWorkflowInfo {
    /// The root workflow's unique identifier
    pub workflow_id: String,
    /// The root workflow's run id
    pub run_id: String,
}

impl WorkflowContextView {
    /// Create a new view from workflow initialization data.
    pub(crate) fn new(
        namespace: String,
        task_queue: String,
        run_id: String,
        init: &InitializeWorkflow,
    ) -> Self {
        let parent = init
            .parent_workflow_info
            .as_ref()
            .map(|p| ParentWorkflowInfo {
                workflow_id: p.workflow_id.clone(),
                run_id: p.run_id.clone(),
                namespace: p.namespace.clone(),
            });

        let root = init.root_workflow.as_ref().map(|r| RootWorkflowInfo {
            workflow_id: r.workflow_id.clone(),
            run_id: r.run_id.clone(),
        });

        let continued_from_run_id = if init.continued_from_execution_run_id.is_empty() {
            None
        } else {
            Some(init.continued_from_execution_run_id.clone())
        };

        let cron_schedule = if init.cron_schedule.is_empty() {
            None
        } else {
            Some(init.cron_schedule.clone())
        };

        Self {
            workflow_id: init.workflow_id.clone(),
            run_id,
            workflow_type: init.workflow_type.clone(),
            task_queue,
            namespace,
            attempt: init.attempt as u32,
            first_execution_run_id: init.first_execution_run_id.clone(),
            continued_from_run_id,
            start_time: init.start_time.and_then(|t| t.try_into().ok()),
            execution_timeout: init
                .workflow_execution_timeout
                .and_then(|d| d.try_into().ok()),
            run_timeout: init.workflow_run_timeout.and_then(|d| d.try_into().ok()),
            task_timeout: init.workflow_task_timeout.and_then(|d| d.try_into().ok()),
            parent,
            root,
            retry_policy: init.retry_policy.clone(),
            cron_schedule,
            memo: init.memo.clone(),
            search_attributes: init
                .search_attributes
                .as_ref()
                .map(SearchAttributes::from_proto),
        }
    }
}

impl BaseWorkflowContext {
    /// Create a new base context backed by the provided runtime host.
    #[doc(hidden)]
    pub fn new(
        namespace: String,
        task_queue: String,
        run_id: String,
        init_workflow_job: InitializeWorkflow,
        data_converter: DataConverter,
        host: Rc<dyn WorkflowHost>,
    ) -> Self {
        Self {
            inner: Rc::new(WorkflowContextInner {
                namespace,
                task_queue,
                run_id,
                shared: RefCell::new(WorkflowContextSharedData {
                    random_seed: init_workflow_job.randomness_seed,
                    search_attributes: init_workflow_job
                        .search_attributes
                        .clone()
                        .unwrap_or_default(),
                    ..Default::default()
                }),
                inital_information: init_workflow_job,
                runtime: WorkflowRuntimeState::new(host),
                cancelled_reason: RefCell::new(None),
                cancel_wakers: RefCell::new(Vec::new()),
                seq_nums: RefCell::new(WfCtxProtectedDat {
                    next_timer_sequence_number: 1,
                    next_activity_sequence_number: 1,
                    next_child_workflow_sequence_number: 1,
                    next_cancel_external_wf_sequence_number: 1,
                    next_signal_external_wf_sequence_number: 1,
                    next_nexus_op_sequence_number: 1,
                }),
                data_converter,
                state_mutated: Cell::new(false),
            }),
        }
    }

    /// Check and clear the state_mutated flag. Returns `true` if `state_mut`
    /// was called since the last time this method was invoked.
    pub(crate) fn take_state_mutated(&self) -> bool {
        self.inner.state_mutated.replace(false)
    }

    /// Mark that workflow state has been mutated.
    pub(crate) fn set_state_mutated(&self) {
        self.inner.state_mutated.set(true);
    }

    pub(crate) fn take_runtime_progress(&self) -> bool {
        self.inner.runtime.take_progress()
    }

    pub(crate) fn take_forced_wft_failure(
        &self,
    ) -> Option<Box<dyn std::error::Error + Send + Sync>> {
        self.inner.runtime.take_forced_wft_failure()
    }

    pub(crate) fn notify_cancel(&self, reason: String) {
        let _guard = SdkWakeGuard::new();
        *self.inner.cancelled_reason.borrow_mut() = Some(reason);
        for waker in self.inner.cancel_wakers.borrow_mut().drain(..) {
            waker.wake();
        }
        self.inner.runtime.mark_progress();
    }

    pub(crate) fn unblock(&self, event: UnblockEvent) -> Result<(), anyhow::Error> {
        self.inner.runtime.unblock(event)
    }

    /// Cancel any cancellable operation by ID
    fn cancel(&self, cancellable_id: CancellableID) {
        match cancellable_id {
            CancellableID::Timer(seq) => {
                if self
                    .inner
                    .runtime
                    .maybe_unblock(UnblockEvent::Timer(seq, TimerResult::Cancelled))
                {
                    self.inner.runtime.host.push_command(
                        workflow_command::Variant::CancelTimer(CancelTimer { seq }).into(),
                    );
                }
            }
            CancellableID::Activity(seq) => {
                self.inner.runtime.host.push_command(
                    workflow_command::Variant::RequestCancelActivity(RequestCancelActivity { seq })
                        .into(),
                );
            }
            CancellableID::LocalActivity(seq) => {
                self.inner.runtime.host.push_command(
                    workflow_command::Variant::RequestCancelLocalActivity(
                        RequestCancelLocalActivity { seq },
                    )
                    .into(),
                );
            }
            CancellableID::ChildWorkflow { seqnum, reason } => {
                self.inner.runtime.host.push_command(
                    workflow_command::Variant::CancelChildWorkflowExecution(
                        CancelChildWorkflowExecution {
                            child_workflow_seq: seqnum,
                            reason,
                        },
                    )
                    .into(),
                );
            }
            CancellableID::SignalExternalWorkflow(seq) => {
                self.inner.runtime.host.push_command(
                    workflow_command::Variant::CancelSignalWorkflow(CancelSignalWorkflow { seq })
                        .into(),
                );
            }
            CancellableID::NexusOp(seq) => {
                self.inner.runtime.host.push_command(
                    workflow_command::Variant::RequestCancelNexusOperation(
                        RequestCancelNexusOperation { seq },
                    )
                    .into(),
                );
            }
        }
    }

    /// Return the current value of current_details.
    pub fn current_details(&self) -> String {
        self.inner.shared.borrow().current_details.clone()
    }

    /// Request to create a timer
    pub fn timer<T: Into<TimerOptions>>(
        &self,
        opts: T,
    ) -> impl CancellableFuture<TimerResult> + use<T> {
        let opts: TimerOptions = opts.into();
        let seq = self.inner.seq_nums.borrow_mut().next_timer_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::Timer(seq), self.clone());
        self.inner
            .runtime
            .register_unblocker(PendingCommandId::Timer(seq), unblocker);
        self.inner.runtime.host.push_command(opts.into_command(seq));
        cmd
    }

    /// Request to run an activity
    pub fn start_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: impl Into<AD::Input>,
        mut opts: ActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        let input = input.into();
        let payload_converter = self.inner.data_converter.payload_converter();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: payload_converter,
        };
        let payloads = match payload_converter.to_payloads(&ctx, &input) {
            Ok(p) => p,
            Err(e) => {
                return ActivityFut::eager(e.into());
            }
        };
        let seq = self.inner.seq_nums.borrow_mut().next_activity_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::Activity(seq), self.clone());
        self.inner
            .runtime
            .register_unblocker(PendingCommandId::Activity(seq), unblocker);
        if opts.task_queue.is_none() {
            opts.task_queue = Some(self.inner.task_queue.clone());
        }
        self.inner.runtime.host.push_command(opts.into_command(
            seq,
            activity.name().to_string(),
            payloads,
        ));
        ActivityFut::running(cmd, self.inner.data_converter.clone())
    }

    /// Request to run a local activity
    pub fn start_local_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: impl Into<AD::Input>,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        let input = input.into();
        let payload_converter = self.inner.data_converter.payload_converter();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: payload_converter,
        };
        let payloads = match payload_converter.to_payloads(&ctx, &input) {
            Ok(p) => p,
            Err(e) => {
                return ActivityFut::eager(e.into());
            }
        };
        ActivityFut::running(
            LATimerBackoffFut::new(activity.name().to_string(), payloads, opts, self.clone()),
            self.inner.data_converter.clone(),
        )
    }

    /// Start a child workflow with typed input/output.
    fn start_child_workflow<WD: WorkflowDefinition>(
        &self,
        workflow: WD,
        input: impl Into<WD::Input>,
        opts: ChildWorkflowOptions,
    ) -> impl CancellableFutureWithReason<Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>>
    where
        WD::Output: TemporalDeserializable,
    {
        let input = input.into();
        let payload_converter = self.inner.data_converter.payload_converter();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: payload_converter,
        };
        let payloads = match payload_converter.to_payloads(&ctx, &input) {
            Ok(p) => p,
            Err(e) => {
                return ChildWorkflowStartFut::eager(e.into());
            }
        };
        let workflow_type = workflow.name().to_string();

        let child_seq = self.inner.seq_nums.borrow_mut().next_child_workflow_seq();
        // Immediately create the command/future for the result, otherwise if the user does
        // not await the result until *after* we receive an activation for it, there will be nothing
        // to match when unblocking.
        let (result_cmd, unblocker) = CancellableWFCommandFut::new(
            CancellableID::ChildWorkflow {
                seqnum: child_seq,
                reason: String::new(),
            },
            self.clone(),
        );
        self.inner.runtime.register_unblocker(
            PendingCommandId::ChildWorkflowComplete(child_seq),
            unblocker,
        );

        let common = ChildWfCommon {
            workflow_id: opts.workflow_id.clone(),
            child_seq,
            result_future: result_cmd,
            base_ctx: self.clone(),
            data_converter: self.inner.data_converter.clone(),
        };

        let (cmd, unblocker) = CancellableWFCommandFut::new_with_dat(
            CancellableID::ChildWorkflow {
                seqnum: child_seq,
                reason: String::new(),
            },
            common,
            self.clone(),
        );
        self.inner
            .runtime
            .register_unblocker(PendingCommandId::ChildWorkflowStart(child_seq), unblocker);
        self.inner
            .runtime
            .host
            .push_command(opts.into_command(child_seq, workflow_type, payloads));

        ChildWorkflowStartFut::Running(cmd)
    }

    /// Request to run a local activity with no implementation of timer-backoff based retrying.
    fn local_activity_no_timer_retry(
        self,
        activity_type: String,
        arguments: Vec<Payload>,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<ActivityResolution> {
        let seq = self.inner.seq_nums.borrow_mut().next_activity_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::LocalActivity(seq), self.clone());
        self.inner
            .runtime
            .register_unblocker(PendingCommandId::Activity(seq), unblocker);
        self.inner
            .runtime
            .host
            .push_command(opts.into_command(seq, activity_type, arguments));
        cmd
    }

    fn send_signal_wf(
        self,
        target: signal_external_workflow_execution::Target,
        signal: Signal,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let seq = self
            .inner
            .seq_nums
            .borrow_mut()
            .next_signal_external_wf_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::SignalExternalWorkflow(seq), self.clone());
        self.inner
            .runtime
            .register_unblocker(PendingCommandId::SignalExternal(seq), unblocker);
        let signal = signal.into_invocation();
        self.inner.runtime.host.push_command(
            workflow_command::Variant::SignalExternalWorkflowExecution(
                SignalExternalWorkflowExecution {
                    seq,
                    signal_name: signal.signal_name,
                    args: signal.input,
                    target: Some(target),
                    headers: signal.headers,
                },
            )
            .into(),
        );
        cmd
    }
}

impl<W> SyncWorkflowContext<W> {
    /// Return the workflow's unique identifier
    pub fn workflow_id(&self) -> &str {
        &self.base.inner.inital_information.workflow_id
    }

    /// Return the run id of this workflow execution
    pub fn run_id(&self) -> &str {
        &self.base.inner.run_id
    }

    /// Return the namespace the workflow is executing in
    pub fn namespace(&self) -> &str {
        &self.base.inner.namespace
    }

    /// Return the task queue the workflow is executing in
    pub fn task_queue(&self) -> &str {
        &self.base.inner.task_queue
    }

    /// Return the current time according to the workflow (which is not wall-clock time).
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.base
            .inner
            .shared
            .borrow()
            .activation
            .timestamp
            .try_into_or_none()
    }

    /// Return the length of history so far at this point in the workflow
    pub fn history_length(&self) -> u32 {
        self.base.inner.shared.borrow().activation.history_length
    }

    /// Return the deployment version, if any,  as it was when this point in the workflow was first
    /// reached. If this code is being executed for the first time, return this Worker's deployment
    /// version if it has one.
    pub fn current_deployment_version(&self) -> Option<WorkerDeploymentVersion> {
        self.base
            .inner
            .shared
            .borrow()
            .activation
            .clone()
            .deployment_version_for_current_task
            .map(Into::into)
    }

    /// Return current values for workflow search attributes.
    pub fn search_attributes(&self) -> SearchAttributes {
        SearchAttributes::from_proto(&self.base.inner.shared.borrow().search_attributes)
    }

    /// Return the workflow's randomness seed
    pub fn random_seed(&self) -> u64 {
        self.base.inner.shared.borrow().random_seed
    }

    /// Returns true if the current workflow task is happening under replay
    pub fn is_replaying(&self) -> bool {
        self.base.inner.shared.borrow().activation.is_replaying
    }

    /// Returns true if the server suggests this workflow should continue-as-new
    pub fn continue_as_new_suggested(&self) -> bool {
        self.base
            .inner
            .shared
            .borrow()
            .activation
            .continue_as_new_suggested
    }

    /// Returns true if the workflow's target worker deployment version changed.
    ///
    /// This experimental signal is intended for workers using worker deployment versioning.
    pub fn target_worker_deployment_version_changed(&self) -> bool {
        self.base
            .inner
            .shared
            .borrow()
            .activation
            .target_worker_deployment_version_changed
    }

    /// Returns the headers for the current handler invocation (signal, update, query, etc.).
    ///
    /// When called from within a signal handler, returns the headers that were sent with that
    /// signal. When called from the main workflow run method, returns an empty map.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        &self.headers
    }

    /// Returns the [PayloadConverter] currently used by the worker running this workflow.
    pub fn payload_converter(&self) -> &PayloadConverter {
        self.base.inner.data_converter.payload_converter()
    }

    /// Return various information that the workflow was initialized with. Will eventually become
    /// a proper non-proto workflow info struct.
    pub fn workflow_initial_info(&self) -> &InitializeWorkflow {
        &self.base.inner.inital_information
    }

    /// A future that resolves if/when the workflow is cancelled, with the user provided cause
    pub fn cancelled(&self) -> impl FusedFuture<Output = String> + '_ {
        let inner = self.base.inner.clone();
        future::poll_fn(move |cx| {
            if let Some(reason) = inner.cancelled_reason.borrow().as_ref() {
                Poll::Ready(reason.clone())
            } else {
                inner.cancel_wakers.borrow_mut().push(cx.waker().clone());
                Poll::Pending
            }
        })
        .fuse()
    }

    /// Signal that this workflow should continue as a new workflow execution with the given input and
    /// options.
    ///
    /// This always returns an `Err` which should be propigated.
    pub fn continue_as_new(
        &self,
        input: &<W::Run as WorkflowDefinition>::Input,
        opts: ContinueAsNewOptions,
    ) -> Result<std::convert::Infallible, WorkflowTermination>
    where
        W: WorkflowImplementation,
    {
        let pc = self.base.inner.data_converter.payload_converter();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: pc,
        };
        let arguments = pc
            .to_payloads(&ctx, input)
            .map_err(WorkflowTermination::from)?;
        let workflow_type = self.workflow_initial_info().workflow_type.clone();
        let request = opts.into_request(workflow_type, arguments);
        Err(WorkflowTermination::continue_as_new(request))
    }

    /// Request to create a timer
    pub fn timer<T: Into<TimerOptions>>(&self, opts: T) -> impl CancellableFuture<TimerResult> {
        self.base.timer(opts)
    }

    /// Request to run an activity
    pub fn start_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: impl Into<AD::Input>,
        opts: ActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        self.base.start_activity(activity, input, opts)
    }

    /// Request to run a local activity
    pub fn start_local_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: impl Into<AD::Input>,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        self.base.start_local_activity(activity, input, opts)
    }

    /// Start a child workflow. Returns a future that resolves to a [StartedChildWorkflow]
    /// which can be used to await the result, send signals, or cancel the child.
    pub fn start_child_workflow<WD: WorkflowDefinition>(
        &self,
        workflow: WD,
        input: impl Into<WD::Input>,
        opts: ChildWorkflowOptions,
    ) -> impl CancellableFutureWithReason<Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>>
    where
        WD::Output: TemporalDeserializable,
    {
        self.base.start_child_workflow(workflow, input, opts)
    }

    /// Deprecated alias for [`SyncWorkflowContext::start_child_workflow`].
    #[deprecated(note = "use `start_child_workflow` instead")]
    pub fn child_workflow<WD: WorkflowDefinition>(
        &self,
        workflow: WD,
        input: impl Into<WD::Input>,
        opts: ChildWorkflowOptions,
    ) -> impl CancellableFutureWithReason<Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>>
    where
        WD::Output: TemporalDeserializable,
    {
        self.start_child_workflow(workflow, input, opts)
    }

    /// Check (or record) that this workflow history was created with the provided patch
    pub fn patched(&self, patch_id: &str) -> bool {
        self.patch_impl(patch_id, false)
    }

    /// Record that this workflow history was created with the provided patch, and it is being
    /// phased out.
    pub fn deprecate_patch(&self, patch_id: &str) -> bool {
        self.patch_impl(patch_id, true)
    }

    fn patch_impl(&self, patch_id: &str, deprecated: bool) -> bool {
        self.base.inner.runtime.host.push_command(
            workflow_command::Variant::SetPatchMarker(SetPatchMarker {
                patch_id: patch_id.to_string(),
                deprecated,
            })
            .into(),
        );
        // See if we already know about the status of this change
        if let Some(present) = self.base.inner.shared.borrow().changes.get(patch_id) {
            return *present;
        }

        // If we don't already know about the change, that means there is no marker in history,
        // and we should return false if we are replaying
        let res = !self.base.inner.shared.borrow().activation.is_replaying;

        self.base
            .inner
            .shared
            .borrow_mut()
            .changes
            .insert(patch_id.to_string(), res);

        res
    }

    /// Get a handle to an external workflow for sending signals or requesting cancellation.
    pub fn external_workflow(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
    ) -> ExternalWorkflowHandle {
        ExternalWorkflowHandle {
            workflow_id: workflow_id.into(),
            run_id,
            namespace: self.base.inner.namespace.clone(),
            base_ctx: self.base.clone(),
        }
    }

    /// Add, update, or remove search attributes using typed keys.
    ///
    /// Updates are applied to the local in-memory view immediately so that
    /// subsequent calls to [`search_attributes()`](Self::search_attributes)
    /// reflect the changes. The command is also sent to the server.
    pub fn upsert_search_attributes(
        &self,
        updates: impl IntoIterator<Item = SearchAttributeUpdate>,
    ) {
        // Collect so we can iterate twice: once for local state, once for the
        // wire proto (which uses a different encoding for "unset").
        let updates: Vec<SearchAttributeUpdate> = updates.into_iter().collect();

        // Update local state using the typed API, which correctly removes keys
        // on unset (rather than inserting empty payloads like the wire format).
        {
            let mut shared = self.base.inner.shared.borrow_mut();
            let mut attrs = SearchAttributes::from_proto(&shared.search_attributes);
            for update in updates.iter().cloned() {
                attrs.apply(update);
            }
            shared.search_attributes = attrs.into_proto();
        }

        let proto = SearchAttributes::updates_to_proto(updates);
        self.base.inner.runtime.host.push_command(
            workflow_command::Variant::UpsertWorkflowSearchAttributes(
                UpsertWorkflowSearchAttributes {
                    search_attributes: Some(proto),
                },
            )
            .into(),
        );
    }

    /// Add or create a set of search attributes
    pub fn upsert_memo(&self, attr_iter: impl IntoIterator<Item = (String, Payload)>) {
        self.base.inner.runtime.host.push_command(
            workflow_command::Variant::ModifyWorkflowProperties(ModifyWorkflowProperties {
                upserted_memo: Some(Memo {
                    fields: attr_iter.into_iter().collect(),
                }),
            })
            .into(),
        );
    }

    /// Set the current details string for this workflow execution.
    ///
    /// The value is surfaced to the Temporal server UI in real time via the
    /// the workflow metadata query.
    pub fn set_current_details(&self, details: impl Into<String>) {
        let details = details.into();
        self.base.inner.shared.borrow_mut().current_details = details.clone();
        self.base.inner.runtime.host.set_current_details(details);
    }

    /// Force a workflow task failure (EX: in order to retry on non-sticky queue)
    pub fn force_task_fail(&self, with: impl Into<Box<dyn std::error::Error + Send + Sync>>) {
        self.base.inner.runtime.set_forced_wft_failure(with.into());
    }

    /// Start a nexus operation
    pub fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<NexusStartResult> {
        let seq = self.base.inner.seq_nums.borrow_mut().next_nexus_op_seq();
        let (result_future, unblocker) = WFCommandFut::new();
        self.base
            .inner
            .runtime
            .register_unblocker(PendingCommandId::NexusOpComplete(seq), unblocker);
        let (cmd, unblocker) = CancellableWFCommandFut::new_with_dat(
            CancellableID::NexusOp(seq),
            NexusUnblockData {
                result_future: result_future.shared(),
                schedule_seq: seq,
                base_ctx: self.base.clone(),
            },
            self.base.clone(),
        );
        self.base
            .inner
            .runtime
            .register_unblocker(PendingCommandId::NexusOpStart(seq), unblocker);
        self.base
            .inner
            .runtime
            .host
            .push_command(opts.into_command(seq));
        cmd
    }

    /// Create a read-only view of this context.
    pub(crate) fn view(&self) -> WorkflowContextView {
        self.base.view()
    }
}

impl<W> WorkflowContext<W> {
    /// Create a new wf context from a base context and workflow state.
    pub(crate) fn from_base(base: BaseWorkflowContext, workflow_state: Rc<RefCell<W>>) -> Self {
        Self {
            sync: SyncWorkflowContext {
                base,
                headers: Rc::new(HashMap::new()),
                _phantom: PhantomData,
            },
            workflow_state,
            condition_wakers: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Returns a new context with the specified headers set.
    pub(crate) fn with_headers(&self, headers: HashMap<String, Payload>) -> Self {
        Self {
            sync: SyncWorkflowContext {
                base: self.sync.base.clone(),
                headers: Rc::new(headers),
                _phantom: PhantomData,
            },
            workflow_state: self.workflow_state.clone(),
            condition_wakers: self.condition_wakers.clone(),
        }
    }

    /// Returns a [`SyncWorkflowContext`] extracted from this context.
    pub(crate) fn sync_context(&self) -> SyncWorkflowContext<W> {
        self.sync.clone()
    }

    /// Create a read-only view of this context.
    pub(crate) fn view(&self) -> WorkflowContextView {
        self.sync.view()
    }

    // --- Delegated methods from SyncWorkflowContext ---

    /// Return the workflow's unique identifier
    pub fn workflow_id(&self) -> &str {
        self.sync.workflow_id()
    }

    /// Return the run id of this workflow execution
    pub fn run_id(&self) -> &str {
        self.sync.run_id()
    }

    /// Return the namespace the workflow is executing in
    pub fn namespace(&self) -> &str {
        self.sync.namespace()
    }

    /// Return the task queue the workflow is executing in
    pub fn task_queue(&self) -> &str {
        self.sync.task_queue()
    }

    /// Return the current time according to the workflow (which is not wall-clock time).
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.sync.workflow_time()
    }

    /// Return the length of history so far at this point in the workflow
    pub fn history_length(&self) -> u32 {
        self.sync.history_length()
    }

    /// Return the deployment version, if any, as it was when this point in the workflow was first
    /// reached. If this code is being executed for the first time, return this Worker's deployment
    /// version if it has one.
    pub fn current_deployment_version(&self) -> Option<WorkerDeploymentVersion> {
        self.sync.current_deployment_version()
    }

    /// Return current values for workflow search attributes.
    pub fn search_attributes(&self) -> SearchAttributes {
        self.sync.search_attributes()
    }

    /// Return the workflow's randomness seed
    pub fn random_seed(&self) -> u64 {
        self.sync.random_seed()
    }

    /// Returns true if the current workflow task is happening under replay
    pub fn is_replaying(&self) -> bool {
        self.sync.is_replaying()
    }

    /// Returns true if the server suggests this workflow should continue-as-new
    pub fn continue_as_new_suggested(&self) -> bool {
        self.sync.continue_as_new_suggested()
    }

    /// Returns true if the workflow's target worker deployment version changed.
    ///
    /// This experimental signal is intended for workers using worker deployment versioning.
    pub fn target_worker_deployment_version_changed(&self) -> bool {
        self.sync.target_worker_deployment_version_changed()
    }

    /// Returns the headers for the current handler invocation (signal, update, query, etc.).
    pub fn headers(&self) -> &HashMap<String, Payload> {
        self.sync.headers()
    }

    /// Returns the [PayloadConverter] currently used by the worker running this workflow.
    pub fn payload_converter(&self) -> &PayloadConverter {
        self.sync.payload_converter()
    }

    /// Return various information that the workflow was initialized with.
    pub fn workflow_initial_info(&self) -> &InitializeWorkflow {
        self.sync.workflow_initial_info()
    }

    /// A future that resolves if/when the workflow is cancelled, with the user provided cause
    pub fn cancelled(&self) -> impl FusedFuture<Output = String> + '_ {
        self.sync.cancelled()
    }

    /// Request to create a timer
    pub fn timer<T: Into<TimerOptions>>(&self, opts: T) -> impl CancellableFuture<TimerResult> {
        self.sync.timer(opts)
    }

    /// Request to run an activity
    pub fn start_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: impl Into<AD::Input>,
        opts: ActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        self.sync.start_activity(activity, input, opts)
    }

    /// Request to run a local activity
    pub fn start_local_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: impl Into<AD::Input>,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        self.sync.start_local_activity(activity, input, opts)
    }

    /// Start a child workflow. See [SyncWorkflowContext::start_child_workflow] for details.
    pub fn start_child_workflow<WD: WorkflowDefinition>(
        &self,
        workflow: WD,
        input: impl Into<WD::Input>,
        opts: ChildWorkflowOptions,
    ) -> impl CancellableFutureWithReason<Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>>
    where
        WD::Output: TemporalDeserializable,
    {
        self.sync.start_child_workflow(workflow, input, opts)
    }

    /// Deprecated alias for [`WorkflowContext::start_child_workflow`].
    #[deprecated(note = "use `start_child_workflow` instead")]
    pub fn child_workflow<WD: WorkflowDefinition>(
        &self,
        workflow: WD,
        input: impl Into<WD::Input>,
        opts: ChildWorkflowOptions,
    ) -> impl CancellableFutureWithReason<Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>>
    where
        WD::Output: TemporalDeserializable,
    {
        self.start_child_workflow(workflow, input, opts)
    }

    /// Check (or record) that this workflow history was created with the provided patch
    pub fn patched(&self, patch_id: &str) -> bool {
        self.sync.patched(patch_id)
    }

    /// Record that this workflow history was created with the provided patch, and it is being
    /// phased out.
    pub fn deprecate_patch(&self, patch_id: &str) -> bool {
        self.sync.deprecate_patch(patch_id)
    }

    /// Get a handle to an external workflow. See [SyncWorkflowContext::external_workflow].
    pub fn external_workflow(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
    ) -> ExternalWorkflowHandle {
        self.sync.external_workflow(workflow_id, run_id)
    }

    /// Add, update, or remove search attributes using typed keys.
    pub fn upsert_search_attributes(
        &self,
        updates: impl IntoIterator<Item = SearchAttributeUpdate>,
    ) {
        self.sync.upsert_search_attributes(updates)
    }

    /// Add or create a set of memo fields
    pub fn upsert_memo(&self, attr_iter: impl IntoIterator<Item = (String, Payload)>) {
        self.sync.upsert_memo(attr_iter)
    }

    /// Set the current details string for this workflow execution.
    ///
    /// See [`SyncWorkflowContext::set_current_details`].
    pub fn set_current_details(&self, details: impl Into<String>) {
        self.sync.set_current_details(details)
    }

    /// Force a workflow task failure (EX: in order to retry on non-sticky queue)
    pub fn force_task_fail(&self, with: impl Into<Box<dyn std::error::Error + Send + Sync>>) {
        self.sync.force_task_fail(with)
    }

    /// Start a nexus operation
    pub fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<NexusStartResult> {
        self.sync.start_nexus_operation(opts)
    }

    /// Access workflow state immutably via closure.
    ///
    /// The borrow is scoped to the closure and cannot escape, preventing
    /// borrows from being held across await points.
    pub fn state<R>(&self, f: impl FnOnce(&W) -> R) -> R {
        f(&*self.workflow_state.borrow())
    }

    /// Access workflow state mutably via closure.
    ///
    /// The borrow is scoped to the closure and cannot escape, preventing
    /// borrows from being held across await points.
    ///
    /// After the mutation, all wakers registered by pending `wait_condition`
    /// futures are woken so that waker-based combinators (e.g.
    /// `FuturesOrdered`) re-poll them on the next pass.
    pub fn state_mut<R>(&self, f: impl FnOnce(&mut W) -> R) -> R {
        let result = f(&mut *self.workflow_state.borrow_mut());
        let _guard = SdkWakeGuard::new();
        for waker in self.condition_wakers.borrow_mut().drain(..) {
            waker.wake();
        }
        self.sync.base.set_state_mutated();
        result
    }

    /// Signal that this workflow should continue as a new workflow execution with the given input and
    /// options.
    ///
    /// This always returns an `Err` which should be propigated
    pub fn continue_as_new(
        &self,
        input: &<W::Run as WorkflowDefinition>::Input,
        opts: ContinueAsNewOptions,
    ) -> Result<std::convert::Infallible, WorkflowTermination>
    where
        W: WorkflowImplementation,
    {
        self.sync.continue_as_new(input, opts)
    }

    /// Wait for some condition on workflow state to become true, yielding the workflow if not.
    ///
    /// The condition closure receives an immutable reference to the workflow state,
    /// which is borrowed only for the duration of each poll (not across await points).
    pub fn wait_condition<'a>(
        &'a self,
        mut condition: impl FnMut(&W) -> bool + 'a,
    ) -> impl FusedFuture<Output = ()> + 'a {
        future::poll_fn(move |cx: &mut Context<'_>| {
            if condition(&*self.workflow_state.borrow()) {
                Poll::Ready(())
            } else {
                self.condition_wakers.borrow_mut().push(cx.waker().clone());
                Poll::Pending
            }
        })
        .fuse()
    }
}

struct WfCtxProtectedDat {
    next_timer_sequence_number: u32,
    next_activity_sequence_number: u32,
    next_child_workflow_sequence_number: u32,
    next_cancel_external_wf_sequence_number: u32,
    next_signal_external_wf_sequence_number: u32,
    next_nexus_op_sequence_number: u32,
}

impl WfCtxProtectedDat {
    fn next_timer_seq(&mut self) -> u32 {
        let seq = self.next_timer_sequence_number;
        self.next_timer_sequence_number += 1;
        seq
    }
    fn next_activity_seq(&mut self) -> u32 {
        let seq = self.next_activity_sequence_number;
        self.next_activity_sequence_number += 1;
        seq
    }
    fn next_child_workflow_seq(&mut self) -> u32 {
        let seq = self.next_child_workflow_sequence_number;
        self.next_child_workflow_sequence_number += 1;
        seq
    }
    fn next_cancel_external_wf_seq(&mut self) -> u32 {
        let seq = self.next_cancel_external_wf_sequence_number;
        self.next_cancel_external_wf_sequence_number += 1;
        seq
    }
    fn next_signal_external_wf_seq(&mut self) -> u32 {
        let seq = self.next_signal_external_wf_sequence_number;
        self.next_signal_external_wf_sequence_number += 1;
        seq
    }
    fn next_nexus_op_seq(&mut self) -> u32 {
        let seq = self.next_nexus_op_sequence_number;
        self.next_nexus_op_sequence_number += 1;
        seq
    }
}

#[derive(Clone, Debug, Default)]
struct WorkflowContextSharedData {
    /// Maps change ids -> resolved status
    changes: HashMap<String, bool>,
    activation: CoreWorkflowActivation,
    search_attributes: ProtoSearchAttributes,
    random_seed: u64,
    /// Current details string, surfaced via the workflow metadata query.
    current_details: String,
}

/// A Future that can be cancelled.
/// Used in the prototype SDK for cancelling operations like timers and activities.
pub trait CancellableFuture<T>: Future<Output = T> + FusedFuture {
    /// Cancel this Future
    fn cancel(&self);
}

/// A Future that can be cancelled with a reason
pub trait CancellableFutureWithReason<T>: CancellableFuture<T> {
    /// Cancel this Future with a reason
    fn cancel_with_reason(&self, reason: String);
}

struct WFCommandFut<T, D> {
    _unused: PhantomData<T>,
    result_rx: oneshot::Receiver<UnblockEvent>,
    other_dat: Option<D>,
}
impl<T> WFCommandFut<T, ()> {
    fn new() -> (Self, oneshot::Sender<UnblockEvent>) {
        Self::new_with_dat(())
    }
}

impl<T, D> WFCommandFut<T, D> {
    fn new_with_dat(other_dat: D) -> (Self, oneshot::Sender<UnblockEvent>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                _unused: PhantomData,
                result_rx: rx,
                other_dat: Some(other_dat),
            },
            tx,
        )
    }
}

impl<T, D> Unpin for WFCommandFut<T, D> where T: Unblockable<OtherDat = D> {}
impl<T, D> Future for WFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.result_rx.poll_unpin(cx).map(|x| {
            let od = self
                .other_dat
                .take()
                .expect("Other data must exist when resolving command future");
            Unblockable::unblock(x.unwrap(), od)
        })
    }
}
impl<T, D> FusedFuture for WFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    fn is_terminated(&self) -> bool {
        self.other_dat.is_none()
    }
}

struct CancellableWFCommandFut<T, D> {
    cmd_fut: WFCommandFut<T, D>,
    cancellable_id: CancellableID,
    base_ctx: BaseWorkflowContext,
}
impl<T> CancellableWFCommandFut<T, ()> {
    fn new(
        cancellable_id: CancellableID,
        base_ctx: BaseWorkflowContext,
    ) -> (Self, oneshot::Sender<UnblockEvent>) {
        Self::new_with_dat(cancellable_id, (), base_ctx)
    }
}
impl<T, D> CancellableWFCommandFut<T, D> {
    fn new_with_dat(
        cancellable_id: CancellableID,
        other_dat: D,
        base_ctx: BaseWorkflowContext,
    ) -> (Self, oneshot::Sender<UnblockEvent>) {
        let (cmd_fut, sender) = WFCommandFut::new_with_dat(other_dat);
        (
            Self {
                cmd_fut,
                cancellable_id,
                base_ctx,
            },
            sender,
        )
    }
}
impl<T, D> Unpin for CancellableWFCommandFut<T, D> where T: Unblockable<OtherDat = D> {}
impl<T, D> Future for CancellableWFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.cmd_fut.poll_unpin(cx)
    }
}
impl<T, D> FusedFuture for CancellableWFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    fn is_terminated(&self) -> bool {
        self.cmd_fut.is_terminated()
    }
}

impl<T, D> CancellableFuture<T> for CancellableWFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    fn cancel(&self) {
        self.base_ctx.cancel(self.cancellable_id.clone());
    }
}
impl<T, D> CancellableFutureWithReason<T> for CancellableWFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    fn cancel_with_reason(&self, reason: String) {
        self.base_ctx
            .cancel(self.cancellable_id.clone().with_reason(reason));
    }
}

struct LATimerBackoffFut {
    la_opts: LocalActivityOptions,
    activity_type: String,
    arguments: Vec<Payload>,
    current_fut: Pin<Box<dyn CancellableFuture<ActivityResolution> + Unpin>>,
    timer_fut: Option<Pin<Box<dyn CancellableFuture<TimerResult> + Unpin>>>,
    base_ctx: BaseWorkflowContext,
    next_attempt: u32,
    next_sched_time: Option<prost_types::Timestamp>,
    did_cancel: AtomicBool,
    terminated: bool,
}
impl LATimerBackoffFut {
    fn new(
        activity_type: String,
        arguments: Vec<Payload>,
        opts: LocalActivityOptions,
        base_ctx: BaseWorkflowContext,
    ) -> Self {
        let current_fut = Box::pin(base_ctx.clone().local_activity_no_timer_retry(
            activity_type.clone(),
            arguments.clone(),
            opts.clone(),
        ));
        Self {
            la_opts: opts,
            activity_type,
            arguments,
            current_fut,
            timer_fut: None,
            base_ctx,
            next_attempt: 1,
            next_sched_time: None,
            did_cancel: AtomicBool::new(false),
            terminated: false,
        }
    }
}
impl Unpin for LATimerBackoffFut {}
impl Future for LATimerBackoffFut {
    type Output = ActivityResolution;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // If the timer exists, wait for it first
        if let Some(tf) = self.timer_fut.as_mut() {
            return match tf.poll_unpin(cx) {
                Poll::Ready(tr) => {
                    self.timer_fut = None;
                    // Schedule next LA if this timer wasn't cancelled
                    if let TimerResult::Fired = tr {
                        let mut opts = self.la_opts.clone();
                        opts.attempt = Some(self.next_attempt);
                        opts.original_schedule_time
                            .clone_from(&self.next_sched_time);
                        self.current_fut =
                            Box::pin(self.base_ctx.clone().local_activity_no_timer_retry(
                                self.activity_type.clone(),
                                self.arguments.clone(),
                                opts,
                            ));
                        Poll::Pending
                    } else {
                        self.terminated = true;
                        Poll::Ready(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(Cancellation {
                                failure: Some(Failure {
                                    message: "Activity cancelled".to_owned(),
                                    failure_info: Some(FailureInfo::CanceledFailureInfo(
                                        CanceledFailureInfo::default(),
                                    )),
                                    ..Default::default()
                                }),
                            })),
                        })
                    }
                }
                Poll::Pending => Poll::Pending,
            };
        }
        let poll_res = self.current_fut.poll_unpin(cx);
        if let Poll::Ready(ref r) = poll_res
            && let Some(activity_resolution::Status::Backoff(b)) = r.status.as_ref()
        {
            // If we've already said we want to cancel, don't schedule the backoff timer. Just
            // return cancel status. This can happen if cancel comes after the LA says it wants
            // to back off but before we have scheduled the timer.
            if self.did_cancel.load(Ordering::Acquire) {
                self.terminated = true;
                return Poll::Ready(ActivityResolution {
                    status: Some(activity_resolution::Status::Cancelled(Cancellation {
                        failure: Some(Failure {
                            message: "Activity cancelled".to_owned(),
                            failure_info: Some(FailureInfo::CanceledFailureInfo(
                                CanceledFailureInfo::default(),
                            )),
                            ..Default::default()
                        }),
                    })),
                });
            }

            let timer_f = self.base_ctx.timer::<Duration>(
                b.backoff_duration
                    .expect("Duration is set")
                    .try_into()
                    .expect("duration converts ok"),
            );
            self.timer_fut = Some(Box::pin(timer_f));
            self.next_attempt = b.attempt;
            self.next_sched_time.clone_from(&b.original_schedule_time);
            return Poll::Pending;
        }
        if poll_res.is_ready() {
            self.terminated = true;
        }
        poll_res
    }
}
impl FusedFuture for LATimerBackoffFut {
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}
impl CancellableFuture<ActivityResolution> for LATimerBackoffFut {
    fn cancel(&self) {
        self.did_cancel.store(true, Ordering::Release);
        if let Some(tf) = self.timer_fut.as_ref() {
            tf.cancel();
        }
        self.current_fut.cancel();
    }
}

/// Future for activity results. Either an immediate error or a running activity.
enum ActivityFut<F, Output> {
    /// Immediate error (e.g., input serialization failure). Resolves on first poll.
    Errored {
        error: Option<Box<ActivityExecutionError>>,
        _phantom: PhantomData<Output>,
    },
    /// Running activity that will deserialize output on completion.
    Running {
        inner: F,
        data_converter: DataConverter,
        _phantom: PhantomData<Output>,
    },
    Terminated,
}

impl<F, Output> ActivityFut<F, Output> {
    fn eager(err: ActivityExecutionError) -> Self {
        Self::Errored {
            error: Some(Box::new(err)),
            _phantom: PhantomData,
        }
    }

    fn running(inner: F, data_converter: DataConverter) -> Self {
        Self::Running {
            inner,
            data_converter,
            _phantom: PhantomData,
        }
    }
}

impl<F, Output> Unpin for ActivityFut<F, Output> where F: Unpin {}

impl<F, Output> Future for ActivityFut<F, Output>
where
    F: Future<Output = ActivityResolution> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    type Output = Result<Output, ActivityExecutionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poll = match this {
            ActivityFut::Errored { error, .. } => {
                Poll::Ready(Err(*error.take().expect("polled after completion")))
            }
            ActivityFut::Running {
                inner,
                data_converter,
                ..
            } => match Pin::new(inner).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(resolution) => Poll::Ready({
                    let status = resolution.status.ok_or_else(|| {
                        data_converter
                            .to_error(
                                &SerializationContextData::Workflow,
                                Failure {
                                    message: "Activity completed without a status".to_string(),
                                    ..Default::default()
                                },
                                ActivityExecutionDecodeHint { cancelled: false },
                            )
                            .expect("synthetic activity failure should decode")
                    })?;

                    match status {
                        activity_resolution::Status::Completed(success) => {
                            let payload = success.result.unwrap_or_default();
                            let ctx = SerializationContext {
                                data: &SerializationContextData::Workflow,
                                converter: data_converter.payload_converter(),
                            };
                            data_converter
                                .payload_converter()
                                .from_payload::<Output>(&ctx, payload)
                                .map_err(ActivityExecutionError::Serialization)
                        }
                        activity_resolution::Status::Failed(f) => Err(data_converter.to_error(
                            &SerializationContextData::Workflow,
                            f.failure.unwrap_or_default(),
                            ActivityExecutionDecodeHint { cancelled: false },
                        )?),
                        activity_resolution::Status::Cancelled(c) => Err(data_converter.to_error(
                            &SerializationContextData::Workflow,
                            c.failure.unwrap_or_default(),
                            ActivityExecutionDecodeHint { cancelled: true },
                        )?),
                        activity_resolution::Status::Backoff(_) => {
                            panic!("DoBackoff should be handled by LATimerBackoffFut")
                        }
                    }
                }),
            },
            ActivityFut::Terminated => panic!("polled after termination"),
        };
        if poll.is_ready() {
            *this = ActivityFut::Terminated;
        }
        poll
    }
}

impl<F, Output> FusedFuture for ActivityFut<F, Output>
where
    F: Future<Output = ActivityResolution> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    fn is_terminated(&self) -> bool {
        matches!(self, ActivityFut::Terminated)
    }
}

impl<F, Output> CancellableFuture<Result<Output, ActivityExecutionError>> for ActivityFut<F, Output>
where
    F: CancellableFuture<ActivityResolution> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    fn cancel(&self) {
        if let ActivityFut::Running { inner, .. } = self {
            inner.cancel()
        }
    }
}

pub(crate) struct ChildWfCommon {
    workflow_id: String,
    child_seq: u32,
    result_future: CancellableWFCommandFut<ChildWorkflowResult, ()>,
    base_ctx: BaseWorkflowContext,
    data_converter: DataConverter,
}

/// Child workflow in pending state. Internal type used during the start handshake;
/// `ChildWorkflowStartFut` converts this into `Result<StartedChildWorkflow, _>` before
/// the caller sees it.
#[derive(derive_more::Debug)]
pub(crate) struct PendingChildWorkflow<WD: WorkflowDefinition> {
    pub(crate) status: ChildWorkflowStartStatus,
    #[debug(skip)]
    pub(crate) common: ChildWfCommon,
    pub(crate) _phantom: PhantomData<WD>,
}

/// Child workflow in started state.
#[derive(derive_more::Debug)]
pub struct StartedChildWorkflow<WD: WorkflowDefinition> {
    /// Run ID of the child workflow
    pub run_id: String,
    #[debug(skip)]
    common: ChildWfCommon,
    _phantom: PhantomData<WD>,
}

/// Future for child workflow results. Wraps the raw result future and deserializes
/// the output on completion.
enum ChildWorkflowFut<F, Output> {
    Running {
        inner: F,
        data_converter: DataConverter,
        _phantom: PhantomData<Output>,
    },
    Terminated,
}

impl<F, Output> Unpin for ChildWorkflowFut<F, Output> where F: Unpin {}

impl<F, Output> Future for ChildWorkflowFut<F, Output>
where
    F: Future<Output = ChildWorkflowResult> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    type Output = Result<Output, ChildWorkflowExecutionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poll = match this {
            ChildWorkflowFut::Running {
                inner,
                data_converter,
                ..
            } => match Pin::new(inner).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => Poll::Ready({
                    let status = result.status.ok_or_else(|| {
                        data_converter
                            .to_error(
                                &SerializationContextData::Workflow,
                                Failure {
                                    message: "Child workflow completed without a status"
                                        .to_string(),
                                    ..Default::default()
                                },
                                ChildWorkflowExecutionDecodeHint,
                            )
                            .expect("synthetic child workflow failure should decode")
                    })?;
                    match status {
                        child_workflow_result::Status::Completed(success) => {
                            let payloads = success.result.into_iter().collect();
                            let ctx = SerializationContext {
                                data: &SerializationContextData::Workflow,
                                converter: data_converter.payload_converter(),
                            };
                            data_converter
                                .payload_converter()
                                .from_payloads::<Output>(&ctx, payloads)
                                .map_err(ChildWorkflowExecutionError::Serialization)
                        }
                        child_workflow_result::Status::Failed(f) => Err(data_converter.to_error(
                            &SerializationContextData::Workflow,
                            f.failure.unwrap_or_default(),
                            ChildWorkflowExecutionDecodeHint,
                        )?),
                        child_workflow_result::Status::Cancelled(c) => Err(data_converter
                            .to_error(
                                &SerializationContextData::Workflow,
                                c.failure.unwrap_or_default(),
                                ChildWorkflowExecutionDecodeHint,
                            )?),
                    }
                }),
            },
            ChildWorkflowFut::Terminated => panic!("polled after termination"),
        };
        if poll.is_ready() {
            *this = ChildWorkflowFut::Terminated;
        }
        poll
    }
}

impl<F, Output> FusedFuture for ChildWorkflowFut<F, Output>
where
    F: Future<Output = ChildWorkflowResult> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    fn is_terminated(&self) -> bool {
        matches!(self, ChildWorkflowFut::Terminated)
    }
}

impl<F, Output> CancellableFutureWithReason<Result<Output, ChildWorkflowExecutionError>>
    for ChildWorkflowFut<F, Output>
where
    F: CancellableFutureWithReason<ChildWorkflowResult> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    fn cancel_with_reason(&self, reason: String) {
        if let ChildWorkflowFut::Running { inner, .. } = self {
            inner.cancel_with_reason(reason)
        }
    }
}

impl<F, Output> CancellableFuture<Result<Output, ChildWorkflowExecutionError>>
    for ChildWorkflowFut<F, Output>
where
    F: CancellableFutureWithReason<ChildWorkflowResult> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    fn cancel(&self) {
        if let ChildWorkflowFut::Running { inner, .. } = self {
            inner.cancel()
        }
    }
}

/// Wrapper future for starting a child workflow. Mirrors `ActivityFut` to allow returning
/// serialization errors eagerly.
enum ChildWorkflowStartFut<F, WD: WorkflowDefinition> {
    /// Immediate error (e.g., input serialization failure). Resolves on first poll.
    Errored {
        error: Option<Box<ChildWorkflowStartError>>,
        _phantom: PhantomData<WD>,
    },
    Running(F),
    Terminated,
}

impl<F, WD: WorkflowDefinition> ChildWorkflowStartFut<F, WD> {
    fn eager(err: ChildWorkflowStartError) -> Self {
        Self::Errored {
            error: Some(Box::new(err)),
            _phantom: PhantomData,
        }
    }
}

impl<F, WD: WorkflowDefinition> Unpin for ChildWorkflowStartFut<F, WD> where F: Unpin {}

impl<F, WD> Future for ChildWorkflowStartFut<F, WD>
where
    F: Future<Output = PendingChildWorkflow<WD>> + Unpin,
    WD: WorkflowDefinition,
{
    type Output = Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poll = match this {
            ChildWorkflowStartFut::Errored { error, .. } => {
                Poll::Ready(Err(*error.take().expect("polled after completion")))
            }
            ChildWorkflowStartFut::Running(inner) => match Pin::new(inner).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(pending) => Poll::Ready(match pending.status {
                    ChildWorkflowStartStatus::Succeeded(s) => Ok(StartedChildWorkflow {
                        run_id: s.run_id,
                        common: pending.common,
                        _phantom: PhantomData,
                    }),
                    ChildWorkflowStartStatus::Failed(f) => {
                        Err(ChildWorkflowStartError::StartFailed {
                            workflow_id: f.workflow_id,
                            workflow_type: f.workflow_type,
                            cause: StartChildWorkflowExecutionFailedCause::try_from(f.cause)
                                .unwrap_or(StartChildWorkflowExecutionFailedCause::Unspecified),
                        })
                    }
                    ChildWorkflowStartStatus::Cancelled(c) => {
                        Err(pending.common.data_converter.to_error(
                            &SerializationContextData::Workflow,
                            c.failure.unwrap_or_default(),
                            ChildWorkflowStartDecodeHint,
                        )?)
                    }
                }),
            },
            ChildWorkflowStartFut::Terminated => panic!("polled after termination"),
        };
        if poll.is_ready() {
            *this = ChildWorkflowStartFut::Terminated;
        }
        poll
    }
}

impl<F, WD> FusedFuture for ChildWorkflowStartFut<F, WD>
where
    F: Future<Output = PendingChildWorkflow<WD>> + Unpin,
    WD: WorkflowDefinition,
{
    fn is_terminated(&self) -> bool {
        matches!(self, ChildWorkflowStartFut::Terminated)
    }
}

impl<F, WD> CancellableFuture<Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>>
    for ChildWorkflowStartFut<F, WD>
where
    F: CancellableFutureWithReason<PendingChildWorkflow<WD>> + Unpin,
    WD: WorkflowDefinition,
{
    fn cancel(&self) {
        if let ChildWorkflowStartFut::Running(inner) = self {
            inner.cancel()
        }
    }
}

impl<F, WD> CancellableFutureWithReason<Result<StartedChildWorkflow<WD>, ChildWorkflowStartError>>
    for ChildWorkflowStartFut<F, WD>
where
    F: CancellableFutureWithReason<PendingChildWorkflow<WD>> + Unpin,
    WD: WorkflowDefinition,
{
    fn cancel_with_reason(&self, reason: String) {
        if let ChildWorkflowStartFut::Running(inner) = self {
            inner.cancel_with_reason(reason)
        }
    }
}

/// Wrapper future for signaling a child workflow. Allows returning serialization errors
/// eagerly instead of panicking.
enum SignalChildFut<F> {
    /// Immediate error (e.g., signal input serialization failure). Resolves on first poll.
    Errored {
        error: Option<WorkflowSignalError>,
    },
    Running {
        inner: F,
        data_converter: DataConverter,
    },
    Terminated,
}

impl<F> SignalChildFut<F> {
    fn eager(err: WorkflowSignalError) -> Self {
        Self::Errored { error: Some(err) }
    }
}

impl<F> Unpin for SignalChildFut<F> where F: Unpin {}

impl<F> Future for SignalChildFut<F>
where
    F: Future<Output = SignalExternalWfResult> + Unpin,
{
    type Output = Result<(), WorkflowSignalError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poll = match this {
            SignalChildFut::Errored { error } => {
                Poll::Ready(Err(error.take().expect("polled after completion")))
            }
            SignalChildFut::Running {
                inner,
                data_converter,
            } => match Pin::new(inner).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
                Poll::Ready(Err(failure)) => Poll::Ready(Err(data_converter.to_error(
                    &SerializationContextData::Workflow,
                    failure,
                    WorkflowSignalDecodeHint,
                )?)),
            },
            SignalChildFut::Terminated => panic!("polled after termination"),
        };
        if poll.is_ready() {
            *this = SignalChildFut::Terminated;
        }
        poll
    }
}

impl<F> FusedFuture for SignalChildFut<F>
where
    F: Future<Output = SignalExternalWfResult> + Unpin,
{
    fn is_terminated(&self) -> bool {
        matches!(self, SignalChildFut::Terminated)
    }
}

impl<F> CancellableFuture<Result<(), WorkflowSignalError>> for SignalChildFut<F>
where
    F: CancellableFuture<SignalExternalWfResult> + Unpin,
{
    fn cancel(&self) {
        if let SignalChildFut::Running { inner, .. } = self {
            inner.cancel()
        }
    }
}

impl<WD: WorkflowDefinition> StartedChildWorkflow<WD>
where
    WD::Output: TemporalDeserializable + 'static,
{
    /// Consumes self and returns a future that deserializes the child workflow result
    /// into `WD::Output`.
    pub fn result(
        self,
    ) -> impl CancellableFutureWithReason<Result<WD::Output, ChildWorkflowExecutionError>> {
        ChildWorkflowFut::Running {
            inner: self.common.result_future,
            data_converter: self.common.data_converter,
            _phantom: PhantomData,
        }
    }

    /// Cancel the child workflow
    pub fn cancel(&self, reason: String) {
        self.common.base_ctx.inner.runtime.host.push_command(
            workflow_command::Variant::CancelChildWorkflowExecution(CancelChildWorkflowExecution {
                child_workflow_seq: self.common.child_seq,
                reason,
            })
            .into(),
        );
    }

    /// Send a typed signal to the child workflow.
    pub fn signal<S: SignalDefinition<Workflow = WD>>(
        &self,
        signal: S,
        input: S::Input,
    ) -> impl CancellableFuture<Result<(), WorkflowSignalError>> + 'static {
        let payload_converter = self.common.data_converter.payload_converter();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: payload_converter,
        };
        let payloads = match payload_converter.to_payloads(&ctx, &input) {
            Ok(p) => p,
            Err(e) => {
                return SignalChildFut::eager(e.into());
            }
        };
        let signal = Signal::new(S::name(&signal), payloads);
        let target = signal_external_workflow_execution::Target::ChildWorkflowId(
            self.common.workflow_id.clone(),
        );
        SignalChildFut::Running {
            inner: self.common.base_ctx.clone().send_signal_wf(target, signal),
            data_converter: self.common.data_converter.clone(),
        }
    }
}

/// Handle to an external workflow for sending signals or requesting cancellation.
///
/// Obtained via [`SyncWorkflowContext::external_workflow`] or
/// [`WorkflowContext::external_workflow`].
#[derive(derive_more::Debug)]
pub struct ExternalWorkflowHandle {
    workflow_id: String,
    run_id: Option<String>,
    namespace: String,
    #[debug(skip)]
    base_ctx: BaseWorkflowContext,
}

impl ExternalWorkflowHandle {
    /// The workflow ID of the external workflow.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// The run ID of the external workflow, or `None` if targeting the latest run.
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// Send a signal to the external workflow.
    pub fn signal<S: SignalDefinition>(
        &self,
        signal: S,
        input: S::Input,
    ) -> impl CancellableFuture<Result<(), WorkflowSignalError>> + 'static {
        let payload_converter = self.base_ctx.data_converter().payload_converter();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: payload_converter,
        };
        let payloads = match payload_converter.to_payloads(&ctx, &input) {
            Ok(p) => p,
            Err(e) => {
                return SignalChildFut::eager(e.into());
            }
        };
        let signal = Signal::new(S::name(&signal), payloads);
        let target = signal_external_workflow_execution::Target::WorkflowExecution(
            NamespacedWorkflowExecution {
                namespace: self.namespace.clone(),
                workflow_id: self.workflow_id.clone(),
                run_id: self.run_id.clone().unwrap_or_default(),
            },
        );
        SignalChildFut::Running {
            inner: self.base_ctx.clone().send_signal_wf(target, signal),
            data_converter: self.base_ctx.data_converter().clone(),
        }
    }

    /// Request cancellation of the external workflow.
    pub fn cancel(
        &self,
        reason: Option<String>,
    ) -> impl FusedFuture<Output = CancelExternalWfResult> {
        let seq = self
            .base_ctx
            .inner
            .seq_nums
            .borrow_mut()
            .next_cancel_external_wf_seq();
        let (cmd, unblocker) = WFCommandFut::new();
        self.base_ctx
            .inner
            .runtime
            .register_unblocker(PendingCommandId::CancelExternal(seq), unblocker);
        self.base_ctx.inner.runtime.host.push_command(
            workflow_command::Variant::RequestCancelExternalWorkflowExecution(
                RequestCancelExternalWorkflowExecution {
                    seq,
                    workflow_execution: Some(NamespacedWorkflowExecution {
                        namespace: self.namespace.clone(),
                        workflow_id: self.workflow_id.clone(),
                        run_id: self.run_id.clone().unwrap_or_default(),
                    }),
                    reason: reason.unwrap_or_default(),
                },
            )
            .into(),
        );
        cmd
    }
}

#[derive(derive_more::Debug)]
#[debug("StartedNexusOperation{{ operation_token: {operation_token:?} }}")]
pub struct StartedNexusOperation {
    /// The operation token, if the operation started asynchronously
    pub operation_token: Option<String>,
    pub(crate) unblock_dat: NexusUnblockData,
}

pub(crate) struct NexusUnblockData {
    result_future: Shared<WFCommandFut<NexusOperationResult, ()>>,
    schedule_seq: u32,
    base_ctx: BaseWorkflowContext,
}

impl StartedNexusOperation {
    pub async fn result(&self) -> NexusOperationResult {
        // The result future is a `Shared`; poll it inside an `SdkWakeGuard` (via
        // `SdkGuardedFuture`) so its internal waker machinery isn't mistaken for a non-SDK wake on
        // replay (which would fail the workflow task with TMPRL1100).
        SdkGuardedFuture(self.unblock_dat.result_future.clone()).await
    }

    pub fn cancel(&self) {
        self.unblock_dat
            .base_ctx
            .cancel(CancellableID::NexusOp(self.unblock_dat.schedule_seq));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use temporalio_common_wasm::{
        data_converters::{TemporalDeserializable, TemporalSerializable},
        protos::{
            coresdk::{
                AsJsonPayloadExt, common::VersioningIntent as ProtoVersioningIntent,
                workflow_commands::WorkflowCommand,
            },
            temporal::api::{
                common::v1::{Payload, RetryPolicy},
                enums::v1::ContinueAsNewVersioningBehavior as ProtoContinueAsNewVersioningBehavior,
            },
        },
    };
    use temporalio_macros::{workflow, workflow_methods};

    #[derive(Default)]
    struct NoopHost;

    impl WorkflowHost for NoopHost {
        fn set_current_details(&self, _details: String) {}
        fn push_command(&self, _command: WorkflowCommand) {}
    }

    #[workflow]
    #[derive(Default)]
    struct TestWorkflow;

    #[workflow_methods]
    impl TestWorkflow {
        #[run]
        async fn run(_ctx: &mut WorkflowContext<Self>, _input: u8) -> crate::WorkflowResult<()> {
            unreachable!("test workflow run should not be polled")
        }
    }

    fn test_context() -> WorkflowContext<TestWorkflow> {
        let init = InitializeWorkflow {
            workflow_type: TestWorkflow.name().to_string(),
            ..Default::default()
        };
        let base = BaseWorkflowContext::new(
            "default".to_string(),
            "orig-task-queue".to_string(),
            "run-id".to_string(),
            init,
            DataConverter::default(),
            Rc::new(NoopHost),
        );
        WorkflowContext::from_base(base, Rc::new(RefCell::new(TestWorkflow)))
    }

    #[test]
    fn workflow_context_continue_as_new_serializes_input_and_defaults() {
        let ctx = test_context();

        let termination = ctx
            .continue_as_new(&7, ContinueAsNewOptions::default())
            .expect_err("continue_as_new should terminate the workflow");
        assert!(
            matches!(termination, WorkflowTermination::ContinueAsNew(_)),
            "expected continue-as-new termination, got {termination:?}"
        );
        let WorkflowTermination::ContinueAsNew(cmd) = termination else {
            unreachable!()
        };

        assert_eq!(
            *cmd,
            crate::runtime::types::ContinueAsNewRequest {
                workflow_type: TestWorkflow.name().to_string(),
                task_queue: String::new(),
                arguments: vec![7u8.as_json_payload().unwrap()],
                workflow_run_timeout: None,
                workflow_task_timeout: None,
                backoff_start_interval: None,
                memo: HashMap::new(),
                headers: HashMap::new(),
                search_attributes: None,
                retry_policy: None,
                versioning_intent: ProtoVersioningIntent::Unspecified.into(),
                initial_versioning_behavior: ProtoContinueAsNewVersioningBehavior::Unspecified
                    .into(),
            }
        );
    }

    #[test]
    fn sync_workflow_context_continue_as_new_applies_options() {
        let ctx = test_context();
        let sync = ctx.sync_context();
        let mut memo = HashMap::new();
        memo.insert(
            "memo-key".to_string(),
            Payload::from(b"memo-value".as_slice()),
        );
        let mut headers = HashMap::new();
        headers.insert(
            "header-key".to_string(),
            Payload::from(b"header-value".as_slice()),
        );
        let mut proto_search_attributes = ProtoSearchAttributes::default();
        proto_search_attributes.indexed_fields.insert(
            "CustomKeywordField".to_string(),
            Payload::from(b"value".as_slice()),
        );
        let search_attributes = SearchAttributes::from_proto(&proto_search_attributes);

        let termination = sync
            .continue_as_new(
                &11,
                ContinueAsNewOptions {
                    workflow_type: Some("next-workflow".to_string()),
                    task_queue: Some("next-task-queue".to_string()),
                    run_timeout: Some(Duration::from_secs(10)),
                    task_timeout: Some(Duration::from_secs(3)),
                    backoff_start_interval: Some(Duration::from_secs(4)),
                    memo: Some(memo.clone()),
                    headers: Some(headers.clone()),
                    search_attributes: Some(search_attributes.clone()),
                    retry_policy: Some(RetryPolicy {
                        maximum_attempts: 5,
                        ..Default::default()
                    }),
                    versioning_intent: Some(ProtoVersioningIntent::Compatible),
                    initial_versioning_behavior: Some(
                        ContinueAsNewVersioningBehavior::UseRampingVersion,
                    ),
                },
            )
            .expect_err("continue_as_new should terminate the workflow");
        assert!(
            matches!(termination, WorkflowTermination::ContinueAsNew(_)),
            "expected continue-as-new termination, got {termination:?}"
        );
        let WorkflowTermination::ContinueAsNew(cmd) = termination else {
            unreachable!()
        };

        assert_eq!(
            *cmd,
            crate::runtime::types::ContinueAsNewRequest {
                workflow_type: "next-workflow".to_string(),
                task_queue: "next-task-queue".to_string(),
                arguments: vec![11u8.as_json_payload().unwrap()],
                workflow_run_timeout: Some(Duration::from_secs(10).try_into().unwrap()),
                workflow_task_timeout: Some(Duration::from_secs(3).try_into().unwrap()),
                backoff_start_interval: Some(Duration::from_secs(4).try_into().unwrap()),
                memo,
                headers,
                search_attributes: Some(proto_search_attributes),
                retry_policy: Some(RetryPolicy {
                    maximum_attempts: 5,
                    ..Default::default()
                }),
                versioning_intent: ProtoVersioningIntent::Compatible.into(),
                initial_versioning_behavior: ProtoContinueAsNewVersioningBehavior::UseRampingVersion
                    as i32,
            }
        );
    }

    #[test]
    fn continue_as_new_preserves_explicit_empty_search_attributes() {
        let ctx = test_context();
        let sync = ctx.sync_context();

        let termination = sync
            .continue_as_new(
                &11,
                ContinueAsNewOptions {
                    search_attributes: Some(SearchAttributes::default()),
                    ..Default::default()
                },
            )
            .expect_err("continue_as_new should terminate the workflow");
        let WorkflowTermination::ContinueAsNew(cmd) = termination else {
            unreachable!()
        };

        assert_eq!(
            cmd.search_attributes,
            Some(ProtoSearchAttributes::default())
        );
    }

    #[test]
    fn workflow_context_continue_as_new_applies_auto_upgrade_versioning_behavior() {
        let ctx = test_context();

        let termination = ctx
            .continue_as_new(
                &13,
                ContinueAsNewOptions {
                    initial_versioning_behavior: Some(ContinueAsNewVersioningBehavior::AutoUpgrade),
                    ..Default::default()
                },
            )
            .expect_err("continue_as_new should terminate the workflow");
        let WorkflowTermination::ContinueAsNew(cmd) = termination else {
            unreachable!()
        };

        assert_eq!(
            cmd.initial_versioning_behavior,
            ProtoContinueAsNewVersioningBehavior::AutoUpgrade as i32
        );
    }

    #[test]
    fn continue_as_new_reports_serialization_errors() {
        #[derive(Debug)]
        struct FailingInput;

        impl TemporalSerializable for FailingInput {
            fn to_payload(
                &self,
                _ctx: &temporalio_common_wasm::data_converters::SerializationContext<'_>,
            ) -> Result<Payload, temporalio_common_wasm::data_converters::PayloadConversionError>
            {
                Err(
                    temporalio_common_wasm::data_converters::PayloadConversionError::EncodingError(
                        std::io::Error::other("serialization failure").into(),
                    ),
                )
            }
        }

        impl TemporalDeserializable for FailingInput {
            fn from_payload(
                _ctx: &temporalio_common_wasm::data_converters::SerializationContext<'_>,
                _payload: Payload,
            ) -> Result<Self, temporalio_common_wasm::data_converters::PayloadConversionError>
            {
                unreachable!("test input is only serialized")
            }
        }

        #[workflow]
        #[derive(Default)]
        struct FailingWorkflow;

        #[workflow_methods]
        impl FailingWorkflow {
            #[run]
            async fn run(
                _ctx: &mut WorkflowContext<Self>,
                _input: FailingInput,
            ) -> crate::WorkflowResult<()> {
                unreachable!("test workflow run should not be polled")
            }
        }

        let init = InitializeWorkflow {
            workflow_type: "failing-workflow".to_string(),
            ..Default::default()
        };
        let base = BaseWorkflowContext::new(
            "default".to_string(),
            "orig-task-queue".to_string(),
            "run-id".to_string(),
            init,
            DataConverter::default(),
            Rc::new(NoopHost),
        );
        let ctx = WorkflowContext::from_base(base, Rc::new(RefCell::new(FailingWorkflow)));

        let err = ctx
            .continue_as_new(&FailingInput, ContinueAsNewOptions::default())
            .expect_err("serialization errors should be surfaced");

        let WorkflowTermination::Failed(err) = err else {
            panic!("expected failed termination, got {err:?}");
        };
        assert_eq!(err.to_string(), "Encoding error: serialization failure");
    }

    #[test]
    fn upsert_search_attributes_updates_local_state() {
        use temporalio_common_wasm::search_attributes::SearchAttributeKey;

        const K: SearchAttributeKey<i64> = SearchAttributeKey::int("my_int");

        let ctx = test_context();
        assert!(ctx.search_attributes().is_empty());

        ctx.upsert_search_attributes([K.value_set(42)]);
        let attrs = ctx.search_attributes();
        assert_eq!(attrs.get(&K), Some(42));
    }

    #[test]
    fn upsert_search_attributes_unset_removes_from_local_state() {
        use temporalio_common_wasm::search_attributes::SearchAttributeKey;

        const K: SearchAttributeKey<String> = SearchAttributeKey::keyword("my_kw");

        let ctx = test_context();
        // Set, then unset.
        ctx.upsert_search_attributes([K.value_set("hello".into())]);
        assert_eq!(ctx.search_attributes().get(&K), Some("hello".into()));

        ctx.upsert_search_attributes([K.value_unset()]);
        assert!(!ctx.search_attributes().contains_key(&K));
        assert!(ctx.search_attributes().is_empty());
    }

    #[test]
    fn upsert_search_attributes_multiple_updates_last_wins() {
        use temporalio_common_wasm::search_attributes::SearchAttributeKey;

        const K: SearchAttributeKey<i64> = SearchAttributeKey::int("counter");

        let ctx = test_context();
        ctx.upsert_search_attributes([K.value_set(1), K.value_set(2)]);
        assert_eq!(ctx.search_attributes().get(&K), Some(2));
    }

    #[test]
    fn upsert_search_attributes_merges_with_initial() {
        use temporalio_common_wasm::search_attributes::SearchAttributeKey;

        const A: SearchAttributeKey<i64> = SearchAttributeKey::int("attr_a");
        const B: SearchAttributeKey<String> = SearchAttributeKey::keyword("attr_b");

        // Start with initial search attribute A.
        let init_sa = SearchAttributes::new([A.value_set(1)]).into_proto();
        let init = InitializeWorkflow {
            workflow_type: TestWorkflow.name().to_string(),
            search_attributes: Some(init_sa),
            ..Default::default()
        };
        let base = BaseWorkflowContext::new(
            "default".to_string(),
            "tq".to_string(),
            "run-id".to_string(),
            init,
            DataConverter::default(),
            Rc::new(NoopHost),
        );
        let ctx = WorkflowContext::from_base(base, Rc::new(RefCell::new(TestWorkflow)));

        assert_eq!(ctx.search_attributes().get(&A), Some(1));

        // Upsert B — A should still be present.
        ctx.upsert_search_attributes([B.value_set("hello".into())]);
        assert_eq!(ctx.search_attributes().get(&A), Some(1));
        assert_eq!(ctx.search_attributes().get(&B), Some("hello".into()));
    }

    #[test]
    fn view_search_attributes_returns_typed() {
        use temporalio_common_wasm::search_attributes::SearchAttributeKey;

        const K: SearchAttributeKey<bool> = SearchAttributeKey::bool("active");

        let init_sa = SearchAttributes::new([K.value_set(true)]).into_proto();
        let init = InitializeWorkflow {
            workflow_type: TestWorkflow.name().to_string(),
            search_attributes: Some(init_sa),
            ..Default::default()
        };
        let base = BaseWorkflowContext::new(
            "default".to_string(),
            "tq".to_string(),
            "run-id".to_string(),
            init,
            DataConverter::default(),
            Rc::new(NoopHost),
        );
        let ctx = WorkflowContext::from_base(base, Rc::new(RefCell::new(TestWorkflow)));

        let view = ctx.view();
        let sa = view
            .search_attributes
            .expect("should have search attributes");
        assert_eq!(sa.get(&K), Some(true));
    }
}
