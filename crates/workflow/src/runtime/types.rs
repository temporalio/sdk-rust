//! Shared runtime model types mirroring the checked-in WIT interface.
//!
//! All items here are SDK/runtime glue.

use temporalio_common_wasm::protos::{
    coresdk::{
        workflow_activation::{InitializeWorkflow, WorkflowActivation as CoreWorkflowActivation},
        workflow_commands::ContinueAsNewWorkflowExecution,
    },
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::Failure,
    },
};

/// Host-provided state required to construct one workflow execution.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowInit {
    /// Namespace used when workflow code constructs namespaced commands.
    pub namespace: String,
    /// Task queue exposed through workflow information.
    pub task_queue: String,
    /// Run ID used to seed deterministic workflow state.
    pub run_id: String,
    /// Initialization activation job containing workflow metadata and input.
    pub initialize_workflow: InitializeWorkflow,
}

/// Static metadata a host needs before choosing and instantiating a workflow implementation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowDefinitionDescriptor {
    /// Workflow type registered with the worker.
    pub workflow_type: String,
    /// Whether initialization must invoke a user-defined `#[init]` method.
    pub has_init: bool,
    /// Whether workflow input is consumed by `#[init]` instead of `#[run]`.
    pub init_takes_input: bool,
    /// Signal names accepted by the workflow implementation.
    pub signals: Vec<String>,
    /// Query names accepted by the workflow implementation.
    pub queries: Vec<String>,
    /// Update definitions accepted by the workflow implementation.
    pub updates: Vec<UpdateDefinitionDescriptor>,
}

/// Static metadata needed to route an update before constructing its handler future.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateDefinitionDescriptor {
    /// Update name registered by the workflow implementation.
    pub name: String,
    /// Whether the update has a validator that must run before its handler.
    pub has_validator: bool,
}

/// Encoded query result returned directly while applying an activation.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryResponse {
    /// Successful payload or failure produced by the query handler.
    pub result: Result<Payload, Failure>,
}

/// Identifier assigned by the workflow runtime to a pollable routine.
pub type RoutineId = u64;
/// Reserved routine identifier for the workflow's main run method.
pub const MAIN_ROUTINE_ID: RoutineId = 0;

/// Activation representation shared by native and component workflow backends.
pub type WorkflowActivation = CoreWorkflowActivation;

/// Identifies which workflow handler owns a runtime routine.
#[derive(Clone, Debug, PartialEq)]
pub enum RoutineKind {
    /// The workflow's main run method.
    Main,
    /// A signal handler, identified by signal name.
    Signal(String),
    /// An update handler and its protocol routing metadata.
    Update(UpdateRoutineKind),
}

/// Routing metadata required to complete an update routine through the update protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateRoutineKind {
    /// Registered update name.
    pub name: String,
    /// User-visible update ID.
    pub update_id: String,
    /// Protocol instance receiving the update response.
    pub protocol_instance_id: String,
}

/// Describes a handler routine created while applying an activation.
#[derive(Clone, Debug, PartialEq)]
pub struct StartedRoutine {
    /// Runtime-assigned identifier used for subsequent polls.
    pub routine_id: RoutineId,
    /// Handler category and routing metadata for the new routine.
    pub kind: RoutineKind,
}

/// Result produced synchronously while applying one activation job.
#[derive(Clone, Debug, PartialEq)]
pub enum ActivationJobResult {
    /// The job produced no host-visible result.
    None,
    /// The job started a routine that the host must poll.
    StartedRoutine(StartedRoutine),
    /// A query completed without creating a persistent routine.
    QueryResponse(Box<QueryResponse>),
    /// An update validator rejected the update before its handler started.
    UpdateRejected(WorkflowFailure),
}

/// Results produced while applying all jobs in one activation.
#[derive(Clone, Debug, PartialEq)]
pub struct ActivationResult {
    /// One result for each activation job, preserving activation order.
    pub job_results: Vec<ActivationJobResult>,
}

/// Command attributes used when a workflow continues as a new run.
pub(crate) type ContinueAsNewRequest = ContinueAsNewWorkflowExecution;

/// Workflow Task failure requested by the main workflow routine.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskFailure {
    /// Failure returned to Core for the current Workflow Task.
    pub failure: WorkflowFailure,
    /// Optional server failure cause override used for failures such as nondeterminism.
    pub force_cause: Option<u32>,
}

/// Terminal command requested when the main workflow routine finishes.
#[derive(Clone, Debug, PartialEq)]
pub enum TerminalOutcome {
    /// Complete the Workflow Execution with the encoded result.
    Completed(Payload),
    /// Fail the Workflow Execution with the encoded failure.
    Failed(WorkflowFailure),
    /// Cancel the Workflow Execution with optional encoded details.
    Cancelled(Option<Payloads>),
    /// Continue the Workflow Execution as a new run.
    ContinueAsNew(Box<ContinueAsNewRequest>),
}

/// Completion state returned when polling the main workflow routine.
#[derive(Clone, Debug, PartialEq)]
pub enum MainRoutineCompletion {
    /// The main routine is intentionally blocked until a later activation.
    Blocked,
    /// The current Workflow Task must fail without terminating the Workflow Execution.
    TaskFailed(TaskFailure),
    /// The Workflow Execution reached a terminal or continue-as-new outcome.
    Terminal(Box<TerminalOutcome>),
}

/// Completion state returned when polling an update handler routine.
#[derive(Clone, Debug, PartialEq)]
pub enum UpdateRoutineCompletion {
    /// The update handler completed successfully.
    Completed {
        /// Protocol instance receiving the successful response.
        protocol_instance_id: String,
        /// Encoded update result.
        result: Payload,
    },
    /// The update handler failed after being accepted.
    Rejected {
        /// Protocol instance receiving the failure response.
        protocol_instance_id: String,
        /// Encoded handler failure.
        failure: WorkflowFailure,
    },
}

/// Completion state for any pollable workflow routine.
#[derive(Clone, Debug, PartialEq)]
pub enum RoutineCompletion {
    /// Completion from the main workflow routine.
    Main(MainRoutineCompletion),
    /// Completion from a signal handler.
    Signal(Result<(), WorkflowFailure>),
    /// Completion from an update handler.
    Update(UpdateRoutineCompletion),
}

/// Describes why the outer inbound interceptor future remained pending after its latest poll.
///
/// A plain [`std::task::Poll::Pending`] cannot tell the SDK whether completing the current
/// activation will provide another opportunity to poll the chain, so the runtime records that
/// distinction here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutinePendingState {
    /// The underlying handler future was polled, after which normal workflow blocking semantics
    /// determine when another activation is needed.
    Handler,
    /// No handler boundary or command-backed SDK future was polled, so Core cannot produce the
    /// activation needed to make progress.
    Interceptor,
    /// A command-backed SDK future was polled, allowing the current activation to complete because
    /// its resolution will produce another activation.
    InterceptorWithActivation,
}

/// Outcome of polling one workflow routine.
#[derive(Clone, Debug, PartialEq)]
pub struct RoutinePollResult {
    /// Completion emitted when the routine finished during this poll.
    pub completion: Option<RoutineCompletion>,
    /// Whether polling advanced runtime state even if the routine remains pending.
    pub made_progress: bool,
    /// Why an intercepted routine remains pending, when interceptor tracking applies.
    pub pending_state: Option<RoutinePendingState>,
}

/// Failure representation shared across native and component workflow runtime boundaries.
pub type WorkflowFailure = Box<Failure>;
