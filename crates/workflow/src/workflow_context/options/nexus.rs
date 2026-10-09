use std::{collections::HashMap, time::Duration};

use crate::WorkflowCancellationToken;
use temporalio_common_wasm::protos::{
    coresdk::{
        nexus::NexusOperationCancellationType as ProtoNexusOperationCancellationType,
        workflow_commands::{ScheduleNexusOperation, WorkflowCommand, workflow_command},
    },
    temporal::api::common::v1::Payload,
};

/// Controls when Nexus operation cancellation is reported to a workflow.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum NexusOperationCancellationType {
    /// Wait until cancellation has completed.
    #[default]
    WaitCancellationCompleted,
    /// Do not request cancellation.
    Abandon,
    /// Request cancellation and report it immediately.
    TryCancel,
    /// Wait until the cancellation request is acknowledged.
    WaitCancellationRequested,
}

impl From<NexusOperationCancellationType> for ProtoNexusOperationCancellationType {
    fn from(value: NexusOperationCancellationType) -> Self {
        match value {
            NexusOperationCancellationType::WaitCancellationCompleted => {
                Self::WaitCancellationCompleted
            }
            NexusOperationCancellationType::Abandon => Self::Abandon,
            NexusOperationCancellationType::TryCancel => Self::TryCancel,
            NexusOperationCancellationType::WaitCancellationRequested => {
                Self::WaitCancellationRequested
            }
        }
    }
}

impl From<ProtoNexusOperationCancellationType> for NexusOperationCancellationType {
    fn from(value: ProtoNexusOperationCancellationType) -> Self {
        match value {
            ProtoNexusOperationCancellationType::WaitCancellationCompleted => {
                Self::WaitCancellationCompleted
            }
            ProtoNexusOperationCancellationType::Abandon => Self::Abandon,
            ProtoNexusOperationCancellationType::TryCancel => Self::TryCancel,
            ProtoNexusOperationCancellationType::WaitCancellationRequested => {
                Self::WaitCancellationRequested
            }
        }
    }
}

/// Options for Nexus Operations
#[derive(Debug, Clone, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct NexusOperationOptions {
    /// Endpoint name, must exist in the endpoint registry or this command will fail.
    pub endpoint: String,
    /// Service name.
    pub service: String,
    /// Operation name.
    pub operation: String,
    /// Input for the operation. The server converts this into Nexus request content and the
    /// appropriate content headers internally when sending the StartOperation request. On the
    /// handler side, if it is also backed by Temporal, the content is transformed back to the
    /// original Payload sent in this command.
    pub input: Option<Payload>,
    /// Schedule-to-close timeout for this operation.
    /// Indicates how long the caller is willing to wait for operation completion.
    /// Calls are retried internally by the server.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Header to attach to the Nexus request.
    /// Users are responsible for encrypting sensitive data in this header as it is stored in
    /// workflow history and transmitted to external services as-is. This is useful for propagating
    /// tracing information. Note these headers are not the same as Temporal headers on internal
    /// activities and child workflows, these are transmitted to Nexus operations that may be
    /// external and are not traditional payloads.
    #[builder(default)]
    pub nexus_header: HashMap<String, String>,
    /// Cancellation type for the operation
    pub cancellation_type: Option<NexusOperationCancellationType>,
    /// Cancellation token for this operation. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
    /// Schedule-to-start timeout for this operation.
    /// Indicates how long the caller is willing to wait for the operation to be started (or completed if synchronous)
    /// by the handler. If the operation is not started within this timeout, it will fail with
    /// TIMEOUT_TYPE_SCHEDULE_TO_START.
    /// If not set or zero, no schedule-to-start timeout is enforced.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Start-to-close timeout for this operation.
    /// Indicates how long the caller is willing to wait for an asynchronous operation to complete after it has been
    /// started. If the operation does not complete within this timeout after starting, it will fail with
    /// TIMEOUT_TYPE_START_TO_CLOSE.
    /// Only applies to asynchronous operations. Synchronous operations ignore this timeout.
    /// If not set or zero, no start-to-close timeout is enforced.
    pub start_to_close_timeout: Option<Duration>,
}

impl NexusOperationOptions {
    pub(crate) fn into_command(self, seq: u32) -> WorkflowCommand {
        workflow_command::Variant::ScheduleNexusOperation(ScheduleNexusOperation {
            seq,
            endpoint: self.endpoint,
            service: self.service,
            operation: self.operation,
            input: self.input,
            schedule_to_close_timeout: self
                .schedule_to_close_timeout
                .and_then(|duration| duration.try_into().ok()),
            schedule_to_start_timeout: self
                .schedule_to_start_timeout
                .and_then(|duration| duration.try_into().ok()),
            start_to_close_timeout: self
                .start_to_close_timeout
                .and_then(|duration| duration.try_into().ok()),
            nexus_header: self.nexus_header,
            cancellation_type: ProtoNexusOperationCancellationType::from(
                self.cancellation_type
                    .unwrap_or(NexusOperationCancellationType::WaitCancellationCompleted),
            )
            .into(),
        })
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_defaults_to_wait_for_completion() {
        assert_eq!(
            NexusOperationCancellationType::default(),
            NexusOperationCancellationType::WaitCancellationCompleted
        );
    }
}
