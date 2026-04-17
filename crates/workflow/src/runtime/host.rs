//! Host-side command sink trait mirroring the checked-in WIT interface.

#![allow(missing_docs)]

use crate::runtime::types::{
    CancelChildWorkflowRequest, ContinueAsNewRequest, NamedPayload,
    RequestCancelExternalWorkflowRequest, RequestCancelNexusOperationRequest,
    ScheduleActivityRequest, ScheduleLocalActivityRequest, ScheduleNexusOperationRequest,
    SignalExternalWorkflowRequest, StartChildWorkflowRequest, StartTimerRequest,
};

/// Runtime-facing workflow host interface for native and WASM backends.
pub trait WorkflowHost {
    /// Update the details string surfaced through the workflow metadata query.
    fn set_current_details(&self, details: String);
    /// Start a timer.
    fn start_timer(&self, req: StartTimerRequest);
    /// Cancel a timer by sequence number.
    fn cancel_timer(&self, seq: u32);
    /// Schedule an activity.
    fn schedule_activity(&self, req: ScheduleActivityRequest);
    /// Cancel a scheduled activity.
    fn cancel_activity(&self, seq: u32);
    /// Schedule a local activity.
    fn schedule_local_activity(&self, req: ScheduleLocalActivityRequest);
    /// Cancel a scheduled local activity.
    fn cancel_local_activity(&self, seq: u32);
    /// Start a child workflow.
    fn start_child_workflow(&self, req: StartChildWorkflowRequest);
    /// Cancel a child workflow.
    fn cancel_child_workflow(&self, req: CancelChildWorkflowRequest);
    /// Request cancellation of an external workflow.
    fn request_cancel_external_workflow(&self, req: RequestCancelExternalWorkflowRequest);
    /// Send a signal to an external workflow.
    fn signal_external_workflow(&self, req: SignalExternalWorkflowRequest);
    /// Cancel an in-flight signal-external request.
    fn cancel_signal_external_workflow(&self, seq: u32);
    /// Schedule a nexus operation.
    fn schedule_nexus_operation(&self, req: ScheduleNexusOperationRequest);
    /// Cancel a nexus operation.
    fn cancel_nexus_operation(&self, req: RequestCancelNexusOperationRequest);
    /// Upsert search attributes on the running workflow.
    fn upsert_search_attributes(&self, entries: Vec<NamedPayload>);
    /// Upsert memo entries on the running workflow.
    fn upsert_memo(&self, entries: Vec<NamedPayload>);
    /// Record a patch marker.
    fn set_patch_marker(&self, patch_id: String, deprecated: bool);
    /// Continue the workflow as new.
    fn continue_as_new(&self, req: ContinueAsNewRequest);
}
