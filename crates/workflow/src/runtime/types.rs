//! Shared runtime model types mirroring the checked-in WIT interface.

#![allow(missing_docs)]

use crate::runtime::model::{
    CancelExternalOk, CancelExternalWfResult, CancellableID, CancellableIDWithReason,
    SignalExternalOk, SignalExternalWfResult, TimerResult, UnblockEvent, WorkflowResult,
    WorkflowTermination,
};
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
use temporalio_common_wasm::{
    Priority, WorkerDeploymentVersion,
    protos::{
        coresdk::{
            activity_result::ActivityResolution,
            child_workflow::{
                ChildWorkflowCancellationType, ChildWorkflowResult,
                StartChildWorkflowExecutionFailedCause,
            },
            nexus::{NexusOperationCancellationType, NexusOperationResult},
            workflow_activation::{
                resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
                resolve_nexus_operation_start::Status as NexusOperationStartStatus,
            },
            workflow_commands::ActivityCancellationType,
        },
        temporal::api::{
            common::v1::{Payload, RetryPolicy},
            enums::v1::{
                ContinueAsNewVersioningBehavior, ParentClosePolicy, SuggestContinueAsNewReason,
                WorkflowIdReusePolicy,
            },
            failure::v1::Failure,
        },
    },
};

pub use temporalio_common_wasm::protos::coresdk::common::VersioningIntent;

#[derive(Clone, Debug, PartialEq)]
pub struct NamedPayload {
    pub key: String,
    pub value: Payload,
}

pub trait IntoPayloadMap {
    fn into_payload_map(self) -> HashMap<String, Payload>;
}

impl<I> IntoPayloadMap for I
where
    I: IntoIterator<Item = NamedPayload>,
{
    fn into_payload_map(self) -> HashMap<String, Payload> {
        self.into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect()
    }
}

pub trait IntoNamedPayloads {
    fn into_named_payloads(self) -> Vec<NamedPayload>;
}

impl<I> IntoNamedPayloads for I
where
    I: IntoIterator<Item = (String, Payload)>,
{
    fn into_named_payloads(self) -> Vec<NamedPayload> {
        self.into_iter()
            .map(|(key, value)| NamedPayload { key, value })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StringHeader {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowExecutionRef {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowInit {
    pub namespace: String,
    pub task_queue: String,
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub attempt: u32,
    pub first_execution_run_id: String,
    pub continued_from_run_id: Option<String>,
    pub start_time: Option<SystemTime>,
    pub execution_timeout: Option<Duration>,
    pub run_timeout: Option<Duration>,
    pub task_timeout: Option<Duration>,
    pub parent: Option<WorkflowExecutionRef>,
    pub root: Option<WorkflowExecutionRef>,
    pub retry_policy: Option<RetryPolicy>,
    pub cron_schedule: Option<String>,
    pub memo: Vec<NamedPayload>,
    pub search_attributes: Vec<NamedPayload>,
    pub headers: Vec<NamedPayload>,
    pub identity: Option<String>,
    pub priority: Option<Priority>,
    pub randomness_seed: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActivationContext {
    pub workflow_time: Option<SystemTime>,
    pub is_replaying: bool,
    pub history_length: u32,
    pub history_size_bytes: u64,
    pub continue_as_new_suggested: bool,
    pub current_deployment_version: Option<WorkerDeploymentVersion>,
    pub last_sdk_version: Option<String>,
    pub available_internal_flags: Vec<u32>,
    pub updated_randomness_seed: Option<u64>,
    pub target_worker_deployment_version_changed: bool,
    pub suggest_continue_as_new_reasons: Vec<SuggestContinueAsNewReason>,
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
pub struct SignalInvocation {
    pub name: String,
    pub args: Vec<Payload>,
    pub headers: Vec<NamedPayload>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateInvocation {
    pub update_id: String,
    pub protocol_instance_id: String,
    pub name: String,
    pub args: Vec<Payload>,
    pub headers: Vec<NamedPayload>,
    pub run_validator: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryInvocation {
    pub name: String,
    pub args: Vec<Payload>,
    pub headers: Vec<NamedPayload>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryResponse {
    pub result: Result<Payload, Failure>,
}

pub type RoutineId = u64;
pub const MAIN_ROUTINE_ID: RoutineId = 0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerFiredEvent {
    pub seq: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivityResolutionEvent {
    pub seq: u32,
    pub result: ActivityResolution,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChildWorkflowStartResolutionEvent {
    pub seq: u32,
    pub status: ChildWorkflowStartStatus,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChildWorkflowResolutionEvent {
    pub seq: u32,
    pub result: ChildWorkflowResult,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExternalSignalResolutionEvent {
    pub seq: u32,
    pub failure: Option<Failure>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExternalCancelResolutionEvent {
    pub seq: u32,
    pub failure: Option<Failure>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusStartResolutionEvent {
    pub seq: u32,
    pub status: NexusOperationStartStatus,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusResolutionEvent {
    pub seq: u32,
    pub result: NexusOperationResult,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowResolution {
    TimerFired(TimerFiredEvent),
    Activity(ActivityResolutionEvent),
    ChildWorkflowStart(ChildWorkflowStartResolutionEvent),
    ChildWorkflow(ChildWorkflowResolutionEvent),
    ExternalSignal(ExternalSignalResolutionEvent),
    ExternalCancel(ExternalCancelResolutionEvent),
    NexusStart(NexusStartResolutionEvent),
    Nexus(NexusResolutionEvent),
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowActivationJob {
    NotifyPatch { patch_id: String },
    Cancel { reason: String },
    Signal(SignalInvocation),
    Update(UpdateInvocation),
    Query(QueryInvocation),
    Resolution(WorkflowResolution),
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowActivation {
    pub context: ActivationContext,
    pub jobs: Vec<WorkflowActivationJob>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RoutineKind {
    Main,
    Signal {
        name: String,
    },
    Update {
        name: String,
        update_id: String,
        protocol_instance_id: String,
    },
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

#[derive(Clone, Debug, PartialEq)]
pub struct StartTimerRequest {
    pub seq: u32,
    pub timeout: Duration,
    pub summary: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduleActivityRequest {
    pub seq: u32,
    pub activity_type: String,
    pub activity_id: Option<String>,
    pub task_queue: Option<String>,
    pub args: Vec<Payload>,
    pub schedule_to_start_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
    pub schedule_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub cancellation_type: ActivityCancellationType,
    pub retry_policy: Option<RetryPolicy>,
    pub priority: Option<Priority>,
    pub summary: Option<String>,
    pub do_not_eagerly_execute: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduleLocalActivityRequest {
    pub seq: u32,
    pub activity_type: String,
    pub activity_id: Option<String>,
    pub args: Vec<Payload>,
    pub retry_policy: RetryPolicy,
    pub attempt: Option<u32>,
    pub original_schedule_time: Option<SystemTime>,
    pub timer_backoff_threshold: Option<Duration>,
    pub cancellation_type: ActivityCancellationType,
    pub schedule_to_close_timeout: Option<Duration>,
    pub schedule_to_start_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
    pub summary: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartChildWorkflowRequest {
    pub seq: u32,
    pub workflow_type: String,
    pub workflow_id: String,
    pub task_queue: Option<String>,
    pub args: Vec<Payload>,
    pub cancellation_type: ChildWorkflowCancellationType,
    pub parent_close_policy: ParentClosePolicy,
    pub static_summary: Option<String>,
    pub static_details: Option<String>,
    pub id_reuse_policy: WorkflowIdReusePolicy,
    pub execution_timeout: Option<Duration>,
    pub run_timeout: Option<Duration>,
    pub task_timeout: Option<Duration>,
    pub cron_schedule: Option<String>,
    pub search_attributes: Vec<NamedPayload>,
    pub priority: Option<Priority>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelChildWorkflowRequest {
    pub seq: u32,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestCancelExternalWorkflowRequest {
    pub seq: u32,
    pub namespace: Option<String>,
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SignalWorkflowTarget {
    WorkflowExecution(WorkflowExecutionRef),
    ChildWorkflowId(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalExternalWorkflowRequest {
    pub seq: u32,
    pub target: SignalWorkflowTarget,
    pub signal: SignalInvocation,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduleNexusOperationRequest {
    pub seq: u32,
    pub endpoint: String,
    pub service: String,
    pub operation: String,
    pub input: Option<Payload>,
    pub schedule_to_close_timeout: Option<Duration>,
    pub schedule_to_start_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
    pub headers: Vec<StringHeader>,
    pub cancellation_type: NexusOperationCancellationType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestCancelNexusOperationRequest {
    pub seq: u32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ContinueAsNewRequest {
    pub workflow_type: Option<String>,
    pub task_queue: Option<String>,
    pub args: Vec<Payload>,
    pub run_timeout: Option<Duration>,
    pub task_timeout: Option<Duration>,
    pub memo: Vec<NamedPayload>,
    pub headers: Vec<NamedPayload>,
    pub search_attributes: Option<Vec<NamedPayload>>,
    pub retry_policy: Option<RetryPolicy>,
    pub versioning_intent: Option<VersioningIntent>,
    pub initial_versioning_behavior: Option<ContinueAsNewVersioningBehavior>,
}

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
    ContinueAsNew(ContinueAsNewRequest),
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
    pub completion: Option<Box<RoutineCompletion>>,
    pub made_progress: bool,
}

pub type SearchAttributeMap = HashMap<String, Payload>;
pub type StartChildWorkflowFailedCause = StartChildWorkflowExecutionFailedCause;
pub type WorkflowFailure = Box<Failure>;
pub type WorkflowSignalResult = SignalExternalWfResult;
pub type WorkflowCancelResult = CancelExternalWfResult;
pub type WorkflowSignalOk = SignalExternalOk;
pub type WorkflowCancelOk = CancelExternalOk;
pub type RuntimeUnblockEvent = UnblockEvent;
pub type RuntimeTimerResult = TimerResult;
pub type RuntimeWorkflowResult<T> = WorkflowResult<T>;
pub type RuntimeWorkflowTermination = WorkflowTermination;
pub type RuntimeCancellableId = CancellableID;
pub type RuntimeCancellableIdWithReason = CancellableIDWithReason;
