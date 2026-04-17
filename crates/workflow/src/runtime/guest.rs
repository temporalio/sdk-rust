//! High-level guest-side runtime traits mirroring the checked-in WIT interface.

#![allow(missing_docs)]

use crate::runtime::types::{
    ActivationResult, RoutineId, RoutinePollResult, WorkflowActivation,
    WorkflowDefinitionDescriptor, WorkflowFailure, WorkflowInit,
};
use std::task::Waker;
use temporalio_common_wasm::protos::temporal::api::common::v1::Payload;

/// Runtime-facing workflow module interface for native and WASM backends.
pub trait WorkflowModule {
    /// List workflow definitions exported by this module.
    fn list_workflows(&self) -> Vec<WorkflowDefinitionDescriptor>;

    /// Instantiate a workflow run by workflow type.
    fn instantiate_workflow(
        &self,
        workflow_type: &str,
        init: WorkflowInit,
        args: Vec<Payload>,
    ) -> Result<Box<dyn WorkflowInstance>, WorkflowFailure>;
}

/// Runtime-facing single-workflow execution interface for native and WASM backends.
pub trait WorkflowInstance {
    /// Apply one ordered workflow activation without polling any routines.
    fn activate(
        &mut self,
        activation: WorkflowActivation,
    ) -> Result<ActivationResult, WorkflowFailure>;
    /// Poll exactly one guest routine, using the provided waker for native integrations that need
    /// wake tracking.
    fn poll_routine(
        &mut self,
        routine_id: RoutineId,
        waker: &Waker,
    ) -> Result<RoutinePollResult, WorkflowFailure>;
}
