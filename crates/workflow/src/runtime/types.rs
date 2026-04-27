//! Shared runtime model types mirroring the checked-in WIT interface.
//!
//! All items here are SDK/runtime glue.

use temporalio_common_wasm::protos::{
    coresdk::{
        workflow_activation::{InitializeWorkflow, WorkflowActivation as CoreWorkflowActivation},
        workflow_commands::ContinueAsNewWorkflowExecution,
    },
    temporal::api::{common::v1::Payload, failure::v1::Failure},
};

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowInit {
    pub namespace: String,
    pub task_queue: String,
    pub run_id: String,
    pub initialize_workflow: InitializeWorkflow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowDefinitionDescriptor {
    pub workflow_type: String,
    pub has_init: bool,
    pub init_takes_input: bool,
    pub signals: Vec<String>,
    pub queries: Vec<String>,
    pub updates: Vec<UpdateDefinitionDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateDefinitionDescriptor {
    pub name: String,
    pub has_validator: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryResponse {
    pub result: Result<Payload, Failure>,
}

pub type RoutineId = u64;
pub const MAIN_ROUTINE_ID: RoutineId = 0;

pub type WorkflowActivation = CoreWorkflowActivation;

#[derive(Clone, Debug, PartialEq)]
pub enum RoutineKind {
    Main,
    Signal(String),
    Update(UpdateRoutineKind),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateRoutineKind {
    pub name: String,
    pub update_id: String,
    pub protocol_instance_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartedRoutine {
    pub routine_id: RoutineId,
    pub kind: RoutineKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ActivationJobResult {
    None,
    StartedRoutine(StartedRoutine),
    QueryResponse(Box<QueryResponse>),
    UpdateRejected(WorkflowFailure),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivationResult {
    pub job_results: Vec<ActivationJobResult>,
}

pub type ContinueAsNewRequest = ContinueAsNewWorkflowExecution;

#[derive(Clone, Debug, PartialEq)]
pub struct TaskFailure {
    pub failure: WorkflowFailure,
    pub force_cause: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TerminalOutcome {
    Completed(Payload),
    Failed(WorkflowFailure),
    Cancelled,
    ContinueAsNew(Box<ContinueAsNewRequest>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum MainRoutineCompletion {
    Blocked,
    TaskFailed(TaskFailure),
    Terminal(Box<TerminalOutcome>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateRoutineCompletion {
    Completed {
        protocol_instance_id: String,
        result: Payload,
    },
    Rejected {
        protocol_instance_id: String,
        failure: WorkflowFailure,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum RoutineCompletion {
    Main(MainRoutineCompletion),
    Signal(Result<(), WorkflowFailure>),
    Update(UpdateRoutineCompletion),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutinePollResult {
    pub completion: Option<RoutineCompletion>,
    pub made_progress: bool,
}

pub type WorkflowFailure = Box<Failure>;
