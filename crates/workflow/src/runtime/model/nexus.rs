use super::*;
use crate::{
    nexus::NexusOperationError,
    workflow_context::{NexusUnblockData, StartedNexusOperation},
};
use temporalio_common_wasm::data_converters::{
    NexusOperationDecodeHint, SerializationContextData, WorkflowSerializationContext,
};

pub(crate) type NexusStartResult = Result<StartedNexusOperation, NexusOperationError>;

impl From<NexusOperationError> for WorkflowTermination {
    fn from(value: NexusOperationError) -> Self {
        Self::Failed(value.into())
    }
}

impl Unblockable for NexusStartResult {
    type OtherDat = NexusUnblockData;

    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self {
        let NexusUnblockData {
            result_future,
            schedule_seq,
            base_ctx,
        } = od;
        match ue {
            UnblockEvent::NexusOperationStart(_, result) => match *result {
                resolve_nexus_operation_start::Status::OperationToken(op_token) => {
                    Ok(StartedNexusOperation {
                        operation_token: Some(op_token),
                        result_future,
                        schedule_seq,
                        base_ctx,
                    })
                }
                resolve_nexus_operation_start::Status::StartedSync(_) => {
                    Ok(StartedNexusOperation {
                        operation_token: None,
                        result_future,
                        schedule_seq,
                        base_ctx,
                    })
                }
                resolve_nexus_operation_start::Status::Failed(failure) => Err(base_ctx
                    .data_converter()
                    .to_error(
                        &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                        failure,
                        NexusOperationDecodeHint::InferFromFailure,
                    )
                    .unwrap_or_else(NexusOperationError::Serialization)),
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
