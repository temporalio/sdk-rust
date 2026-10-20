use super::*;
use crate::{
    CancellableFuture,
    runtime::{host::WorkflowHost, model::UnblockEvent, types::WorkflowInit},
    workflow_interceptors::{
        CancellableWorkflowOutboundFuture, StartNexusOperationInput, StartNexusOperationResult,
        WorkflowInterceptor, WorkflowInterceptorConstructor, WorkflowInterceptorContext,
        WorkflowNext,
    },
};
use futures_util::FutureExt;
use nexus_rpc::service;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, rc::Rc};
use temporalio_common_wasm::{
    data_converters::DataConverter,
    protos::{
        coresdk::{
            AsJsonPayloadExt, FromJsonPayloadExt,
            nexus::NexusOperationResult,
            workflow_activation::resolve_nexus_operation_start::Status as StartStatus,
            workflow_commands::{WorkflowCommand, workflow_command::Variant},
        },
        temporal::api::{
            common::v1::Payload,
            failure::v1::{
                ApplicationFailureInfo, CanceledFailureInfo, Failure, NexusOperationFailureInfo,
                TimeoutFailureInfo, failure::FailureInfo,
            },
        },
    },
};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(super) struct Request {
    account: String,
    amount: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(super) struct Receipt {
    confirmation: String,
    accepted: bool,
}

pub(super) struct Unserializable;

impl TemporalSerializable for Unserializable {
    fn to_payload(
        &self,
        _ctx: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        Err(PayloadConversionError::EncodingError(
            std::io::Error::other("cannot encode request").into(),
        ))
    }
}

#[service(name = "wire-payments")]
pub(super) trait Payments {
    #[operation(name = "wire-charge")]
    fn charge(input: Request) -> Receipt;

    #[operation]
    fn unencodable(input: Unserializable) -> Receipt;
}

#[derive(Default)]
struct RecordingHost {
    commands: RefCell<Vec<WorkflowCommand>>,
}

impl WorkflowHost for RecordingHost {
    fn set_current_details(&self, _details: String) {}

    fn push_command(&self, command: WorkflowCommand) {
        self.commands.borrow_mut().push(command);
    }
}

struct AddTraceHeader;

impl WorkflowInterceptor for AddTraceHeader {
    fn start_nexus_operation(
        &self,
        _ctx: WorkflowInterceptorContext,
        mut input: StartNexusOperationInput,
        next: WorkflowNext<
            'static,
            StartNexusOperationInput,
            CancellableWorkflowOutboundFuture<StartNexusOperationResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<StartNexusOperationResult> {
        input
            .options_mut()
            .nexus_header
            .insert("trace-id".to_string(), "trace-42".to_string());
        next.run(input)
    }
}

fn client() -> (
    BaseWorkflowContext,
    NexusWorkflowClient<Payments>,
    Rc<RecordingHost>,
) {
    let host = Rc::new(RecordingHost::default());
    let base = BaseWorkflowContext::from_raw(
        WorkflowInit {
            namespace: "test-namespace".to_string(),
            task_queue: "test-task-queue".to_string(),
            run_id: "test-run".to_string(),
            initialize_workflow: Default::default(),
        },
        DataConverter::default(),
        host.clone(),
        None,
        vec![WorkflowInterceptorConstructor::new(|_| AddTraceHeader)],
    );
    let client = NexusWorkflowClient::new(
        base.clone(),
        Payments,
        NexusWorkflowClientOptions::new("payments-endpoint").build(),
    );
    (base, client, host)
}

fn request() -> Request {
    Request {
        account: "customer-42".to_string(),
        amount: 123,
    }
}

fn receipt() -> Receipt {
    Receipt {
        confirmation: "receipt-42".to_string(),
        accepted: true,
    }
}

fn resolve_start(base: &BaseWorkflowContext, seq: u32, status: StartStatus) {
    base.unblock(UnblockEvent::NexusOperationStart(seq, Box::new(status)))
        .unwrap();
}

fn resolve_result(
    base: &BaseWorkflowContext,
    seq: u32,
    status: Option<nexus_operation_result::Status>,
) {
    base.unblock(UnblockEvent::NexusOperationComplete(
        seq,
        Box::new(NexusOperationResult { status }),
    ))
    .unwrap();
}

#[test]
fn execute_uses_renamed_wire_names_and_converts_distinct_input_and_output_types() {
    let (base, client, host) = client();
    let execute = client.execute(
        Payments::charge,
        request(),
        NexusWorkflowOperationOptions::builder()
            .schedule_to_close_timeout(Duration::from_secs(60))
            .schedule_to_start_timeout(Duration::from_secs(10))
            .start_to_close_timeout(Duration::from_secs(30))
            .build(),
    );
    let mut execute = std::pin::pin!(execute);
    assert!(execute.as_mut().now_or_never().is_none());
    {
        let commands = host.commands.borrow();
        assert_eq!(commands.len(), 1);
        let Some(Variant::ScheduleNexusOperation(command)) = &commands[0].variant else {
            panic!("expected Nexus schedule command");
        };
        assert_eq!(command.endpoint, "payments-endpoint");
        assert_eq!(command.service, "wire-payments");
        assert_eq!(command.operation, "wire-charge");
        assert_eq!(
            Request::from_json_payload(command.input.as_ref().unwrap()).unwrap(),
            request()
        );
        assert_eq!(
            command.schedule_to_close_timeout.as_ref().unwrap().seconds,
            60
        );
        assert_eq!(
            command.schedule_to_start_timeout.as_ref().unwrap().seconds,
            10
        );
        assert_eq!(command.start_to_close_timeout.as_ref().unwrap().seconds, 30);
        assert_eq!(command.nexus_header["trace-id"], "trace-42");
    }
    resolve_start(&base, 1, StartStatus::StartedSync(true));
    assert!(execute.as_mut().now_or_never().is_none());
    resolve_result(
        &base,
        1,
        Some(nexus_operation_result::Status::Completed(
            receipt().as_json_payload().unwrap(),
        )),
    );
    assert_eq!(
        execute
            .now_or_never()
            .expect("execute should resolve")
            .unwrap(),
        receipt()
    );
}

#[test]
fn asynchronous_handle_exposes_token_and_repeats_typed_result_without_rescheduling() {
    let (base, client, host) = client();
    let start = client.start(
        Payments::charge,
        request(),
        NexusWorkflowOperationOptions::default(),
    );
    let mut start = std::pin::pin!(start);
    assert!(start.as_mut().now_or_never().is_none());
    resolve_start(
        &base,
        1,
        StartStatus::OperationToken("opaque-operation-token".to_string()),
    );
    let handle = start.now_or_never().unwrap().unwrap();
    assert_eq!(handle.operation_token(), Some("opaque-operation-token"));
    assert!(handle.result().now_or_never().is_none());
    resolve_result(
        &base,
        1,
        Some(nexus_operation_result::Status::Completed(
            receipt().as_json_payload().unwrap(),
        )),
    );
    for _ in 0..2 {
        assert_eq!(handle.result().now_or_never().unwrap().unwrap(), receipt());
    }
    assert_eq!(host.commands.borrow().len(), 1);
}

#[test]
fn synchronous_handle_has_no_operation_token() {
    let (base, client, _) = client();
    let start = client.start(
        Payments::charge,
        request(),
        NexusWorkflowOperationOptions::default(),
    );
    let mut start = std::pin::pin!(start);
    assert!(start.as_mut().now_or_never().is_none());
    resolve_start(&base, 1, StartStatus::StartedSync(true));
    let handle = start.now_or_never().unwrap().unwrap();
    assert_eq!(handle.operation_token(), None);
    resolve_result(
        &base,
        1,
        Some(nexus_operation_result::Status::Completed(
            receipt().as_json_payload().unwrap(),
        )),
    );
    assert_eq!(handle.result().now_or_never().unwrap().unwrap(), receipt());
}

#[test]
fn calls_schedule_eagerly_and_can_cancel_before_start_acknowledgment() {
    let (_base, client, host) = client();
    let start = client.start(
        Payments::charge,
        request(),
        NexusWorkflowOperationOptions::default(),
    );
    let execute = client.execute(
        Payments::charge,
        request(),
        NexusWorkflowOperationOptions::default(),
    );
    assert_eq!(host.commands.borrow().len(), 2);
    start.cancel();
    execute.cancel();
    start.cancel();
    execute.cancel();
    let commands = host.commands.borrow();
    assert_eq!(commands.len(), 4);
    assert!(matches!(
        &commands[2].variant,
        Some(Variant::RequestCancelNexusOperation(command)) if command.seq == 1
    ));
    assert!(matches!(
        &commands[3].variant,
        Some(Variant::RequestCancelNexusOperation(command)) if command.seq == 2
    ));
}

#[test]
fn handle_cancellation_targets_its_schedule_and_is_idempotent() {
    let (base, client, host) = client();
    for seq in 1..=2 {
        let start = client.start(
            Payments::charge,
            request(),
            NexusWorkflowOperationOptions::default(),
        );
        let mut start = std::pin::pin!(start);
        assert!(start.as_mut().now_or_never().is_none());
        resolve_start(
            &base,
            seq,
            StartStatus::OperationToken(format!("token-{seq}")),
        );
        let handle = start.now_or_never().unwrap().unwrap();
        if seq == 2 {
            handle.cancel();
            handle.cancel();
            assert!(handle.result().now_or_never().is_none());
        }
    }
    let commands = host.commands.borrow();
    assert_eq!(commands.len(), 3);
    assert!(matches!(
        &commands[2].variant,
        Some(Variant::RequestCancelNexusOperation(command)) if command.seq == 2
    ));
}

#[test]
fn workflow_cancellation_is_inherited_but_custom_token_is_independent() {
    for custom_token in [false, true] {
        let (base, client, host) = client();
        let token = WorkflowCancellationToken::new();
        let start = client.start(
            Payments::charge,
            request(),
            NexusWorkflowOperationOptions {
                cancellation_token: custom_token.then(|| token.clone()),
                ..Default::default()
            },
        );
        let mut start = std::pin::pin!(start);
        assert!(start.as_mut().now_or_never().is_none());
        resolve_start(&base, 1, StartStatus::OperationToken("token".to_string()));
        let handle = start.now_or_never().unwrap().unwrap();
        base.notify_cancel("workflow cancelled".to_string());
        if custom_token {
            assert_eq!(host.commands.borrow().len(), 1);
            token.cancel();
        }
        assert_eq!(host.commands.borrow().len(), 2);
        handle.cancel();
        token.cancel();
        let commands = host.commands.borrow();
        assert_eq!(commands.len(), 2);
        assert!(matches!(
            &commands[1].variant,
            Some(Variant::RequestCancelNexusOperation(command)) if command.seq == 1
        ));
    }
}

#[test]
fn start_and_result_failures_preserve_server_failure_details() {
    let failure = Failure {
        message: "charge rejected".to_string(),
        source: "remote-service".to_string(),
        stack_trace: "remote stack".to_string(),
        cause: Some(Box::new(Failure {
            message: "insufficient funds".to_string(),
            ..Default::default()
        })),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "PaymentRejected".to_string(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    for phase in 0..2 {
        let (base, client, _) = client();
        let start = client.start(
            Payments::charge,
            request(),
            NexusWorkflowOperationOptions::default(),
        );
        let mut start = std::pin::pin!(start);
        assert!(start.as_mut().now_or_never().is_none());
        if phase == 0 {
            resolve_start(&base, 1, StartStatus::Failed(failure.clone()));
            let Err(error) = start.now_or_never().unwrap() else {
                panic!("start should fail");
            };
            assert_eq!(error.operation_failure().unwrap().failure(), &failure);
        } else {
            resolve_start(&base, 1, StartStatus::OperationToken("token".to_string()));
            let handle = start.now_or_never().unwrap().unwrap();
            let status = nexus_operation_result::Status::Failed(failure.clone());
            resolve_result(&base, 1, Some(status));
            for _ in 0..2 {
                let error = handle.result().now_or_never().unwrap().unwrap_err();
                assert_eq!(error.operation_failure().unwrap().failure(), &failure);
            }
        }
    }
}

#[test]
fn start_preserves_typed_failure_causes() {
    for (failure_info, cancelled, timed_out) in [
        (None, false, false),
        (
            Some(FailureInfo::CanceledFailureInfo(
                CanceledFailureInfo::default(),
            )),
            true,
            false,
        ),
        (
            Some(FailureInfo::TimeoutFailureInfo(
                TimeoutFailureInfo::default(),
            )),
            false,
            true,
        ),
    ] {
        for wrapped in [false, true] {
            let reason = Failure {
                message: "start resolution".to_owned(),
                failure_info: failure_info.clone(),
                ..Default::default()
            };
            let failure = if wrapped {
                Failure {
                    message: "operation failed to start".to_owned(),
                    cause: Some(Box::new(reason)),
                    failure_info: Some(FailureInfo::NexusOperationExecutionFailureInfo(
                        NexusOperationFailureInfo::default(),
                    )),
                    ..Default::default()
                }
            } else {
                reason
            };
            let (base, client, _) = client();
            let execute = client.execute(
                Payments::charge,
                request(),
                NexusWorkflowOperationOptions::default(),
            );
            let mut execute = std::pin::pin!(execute);
            assert!(execute.as_mut().now_or_never().is_none());
            resolve_start(&base, 1, StartStatus::Failed(failure.clone()));
            let error = execute.now_or_never().unwrap().unwrap_err();
            let operation_failure = error.operation_failure().unwrap();
            assert_eq!(error.as_cancelled().is_some(), cancelled);
            assert_eq!(error.as_timeout().is_some(), timed_out);
            assert_eq!(operation_failure.failure(), &failure);
        }
    }
}

#[test]
fn result_preserves_typed_failure_causes() {
    for timed_out in [false, true] {
        let cause = Failure {
            message: "operation cause".to_owned(),
            failure_info: Some(if timed_out {
                FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo::default())
            } else {
                FailureInfo::CanceledFailureInfo(CanceledFailureInfo::default())
            }),
            ..Default::default()
        };
        let failure = Failure {
            message: "operation failed".to_owned(),
            cause: Some(Box::new(cause.clone())),
            failure_info: Some(FailureInfo::NexusOperationExecutionFailureInfo(
                NexusOperationFailureInfo::default(),
            )),
            ..Default::default()
        };
        let status = if timed_out {
            nexus_operation_result::Status::TimedOut(failure.clone())
        } else {
            nexus_operation_result::Status::Cancelled(failure.clone())
        };
        let (base, client, _) = client();
        let execute = client.execute(
            Payments::charge,
            request(),
            NexusWorkflowOperationOptions::default(),
        );
        let mut execute = std::pin::pin!(execute);
        assert!(execute.as_mut().now_or_never().is_none());
        resolve_start(&base, 1, StartStatus::OperationToken("token".to_owned()));
        resolve_result(&base, 1, Some(status));
        let error = execute.now_or_never().unwrap().unwrap_err();
        let operation_failure = error.operation_failure().unwrap();
        assert_eq!(error.as_cancelled().is_some(), !timed_out);
        assert_eq!(error.as_timeout().is_some(), timed_out);
        assert_eq!(operation_failure.failure(), &failure);
        assert_eq!(
            operation_failure.cause().map(|cause| cause.failure()),
            Some(&cause)
        );
    }
}

#[test]
fn serialization_failure_does_not_schedule_or_consume_operation_sequence() {
    let (base, client, host) = client();
    let result = client
        .start(
            Payments::unencodable,
            Unserializable,
            NexusWorkflowOperationOptions::default(),
        )
        .now_or_never()
        .unwrap();
    let Err(error) = result else {
        panic!("input serialization should fail");
    };
    assert!(matches!(error, NexusOperationError::Serialization(_)));
    assert!(host.commands.borrow().is_empty());

    let start = client.start(
        Payments::charge,
        request(),
        NexusWorkflowOperationOptions::default(),
    );
    let mut start = std::pin::pin!(start);
    assert!(start.as_mut().now_or_never().is_none());
    resolve_start(&base, 1, StartStatus::StartedSync(true));
    assert!(start.now_or_never().unwrap().is_ok());
    assert!(matches!(
        &host.commands.borrow()[0].variant,
        Some(Variant::ScheduleNexusOperation(command)) if command.seq == 1
    ));
}

#[test]
fn malformed_output_and_missing_status_return_serialization_errors() {
    for status in [
        Some(nexus_operation_result::Status::Completed(
            "not a receipt".as_json_payload().unwrap(),
        )),
        None,
    ] {
        let (base, client, _) = client();
        let start = client.start(
            Payments::charge,
            request(),
            NexusWorkflowOperationOptions::default(),
        );
        let mut start = std::pin::pin!(start);
        assert!(start.as_mut().now_or_never().is_none());
        resolve_start(&base, 1, StartStatus::StartedSync(true));
        let handle = start.now_or_never().unwrap().unwrap();
        resolve_result(&base, 1, status);
        let error = handle.result().now_or_never().unwrap().unwrap_err();
        assert!(matches!(error, NexusOperationError::Serialization(_)));
    }
}
