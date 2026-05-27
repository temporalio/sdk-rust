use crate::{
    common::{CoreWfStarter, WorkflowHandleExt, rand_6_chars},
    integ_tests::mk_nexus_endpoint,
};
use anyhow::anyhow;
use assert_matches::assert_matches;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    UntypedSignal, UntypedWorkflow, WorkflowCancelOptions, WorkflowSignalOptions,
    WorkflowStartOptions,
};
use temporalio_common::{
    data_converters::{
        GenericPayloadConverter, PayloadConverter, RawValue, SerializationContext,
        SerializationContextData,
    },
    protos::{
        coresdk::{
            FromJsonPayloadExt,
            nexus::{
                NexusOperationCancellationType, NexusOperationResult, NexusTaskCancelReason,
                NexusTaskCompletion, nexus_operation_result, nexus_task, nexus_task_completion,
            },
        },
        temporal::api::{
            common::v1::{Callback, callback},
            enums::v1::NexusHandlerErrorRetryBehavior,
            failure::v1::{Failure, failure::FailureInfo},
            nexus,
            nexus::v1::{
                CancelOperationResponse, HandlerError, StartOperationResponse, request,
                start_operation_response, workflow_event_link_from_nexus,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, NexusOperationOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult, WorkflowTermination,
};
use temporalio_sdk_core::PollError;
use tokio::{
    join,
    sync::{mpsc, watch},
};

#[derive(Debug, PartialEq, Eq, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub(crate) enum Outcome {
    Succeed,
    Fail,
    Cancel,
    CancelAfterRecordedBeforeStarted,
    Timeout,
}

#[workflow]
#[derive(Default)]
struct NexusBasicWf {
    endpoint: String,
}

#[workflow_methods]
impl NexusBasicWf {
    #[init]
    fn new(_ctx: &WorkflowContextView, endpoint: String) -> Self {
        Self { endpoint }
    }

    #[run]
    pub(crate) async fn run(
        ctx: &mut WorkflowContext<Self>,
    ) -> WorkflowResult<Result<NexusOperationResult, Failure>> {
        match ctx
            .start_nexus_operation(NexusOperationOptions {
                endpoint: ctx.state(|wf| wf.endpoint.clone()),
                service: "svc".to_string(),
                operation: "op".to_string(),
                schedule_to_close_timeout: Some(Duration::from_secs(3)),
                ..Default::default()
            })
            .await
        {
            Ok(started) => {
                assert_eq!(started.operation_token, None);
                let res = started.result().await;
                Ok(Ok(res))
            }
            Err(failure) => Ok(Err(failure)),
        }
    }
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_basic(
    #[values(Outcome::Succeed, Outcome::Fail, Outcome::Timeout)] outcome: Outcome,
) {
    let wf_name = "nexus_basic";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    };
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;

    worker.register_workflow::<NexusBasicWf>().unwrap();
    let wf_handle = worker
        .submit_workflow(
            NexusBasicWf::run,
            endpoint.clone(),
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();

    let client = starter.get_client().await;
    let nexus_task_handle = async {
        let nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        match outcome {
            Outcome::Succeed => {
                core_worker
                    .complete_nexus_task(NexusTaskCompletion {
                        task_token: nt.task_token,
                        status: Some(nexus_task_completion::Status::Completed(
                            nexus::v1::Response {
                                variant: Some(nexus::v1::response::Variant::StartOperation(
                                    StartOperationResponse {
                                        variant: Some(
                                            start_operation_response::Variant::SyncSuccess(
                                                start_operation_response::Sync {
                                                    payload: Some("yay".into()),
                                                    links: vec![],
                                                },
                                            ),
                                        ),
                                    },
                                )),
                            },
                        )),
                    })
                    .await
                    .unwrap();
            }
            Outcome::Fail | Outcome::Timeout => {
                if outcome == Outcome::Timeout {
                    // Wait for the timeout task cancel to get sent
                    let timeout_t = core_worker.poll_nexus_task().await.unwrap();
                    let cancel = assert_matches!(timeout_t.variant,
                        Some(nexus_task::Variant::CancelTask(ct)) => ct);
                    assert_eq!(cancel.reason, NexusTaskCancelReason::TimedOut as i32);
                }
                core_worker
                    .complete_nexus_task(NexusTaskCompletion {
                        task_token: nt.task_token,
                        #[allow(deprecated)]
                        status: Some(nexus_task_completion::Status::Error(HandlerError {
                            error_type: "BAD_REQUEST".to_string(), // bad req is non-retryable
                            failure: Some(nexus::v1::Failure {
                                message: "busted".to_string(),
                                ..Default::default()
                            }),
                            retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable.into(),
                        })),
                    })
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    join!(nexus_task_handle, async {
        worker.run_until_done().await.unwrap();
    });

    let res = client
        .get_workflow_handle::<UntypedWorkflow>(starter.get_task_queue().to_owned())
        .get_result(Default::default())
        .await
        .unwrap();
    let res =
        Result::<NexusOperationResult, Failure>::from_json_payload(res.payloads.first().unwrap())
            .unwrap();
    match outcome {
        Outcome::Succeed => {
            let p = assert_matches!(
                res.unwrap().status,
                Some(nexus_operation_result::Status::Completed(p)) => p
            );
            assert_eq!(p.data, b"yay");
        }
        Outcome::Fail => {
            let f = res.unwrap_err();
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
        }
        Outcome::Timeout => {
            let f = res.unwrap_err();
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            let cause = f.cause.unwrap();
            assert_eq!(cause.message, "operation timed out");
            assert_matches!(
                f.failure_info,
                Some(FailureInfo::NexusOperationExecutionFailureInfo(_))
            );
            assert_matches!(cause.failure_info, Some(FailureInfo::TimeoutFailureInfo(_)));
        }
        _ => unreachable!(),
    }
    wf_handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[workflow]
struct NexusAsyncWf {
    endpoint: String,
    schedule_to_close_timeout: Option<Duration>,
    outcome: Outcome,
}

#[workflow_methods]
impl NexusAsyncWf {
    #[init]
    fn new(
        _ctx: &WorkflowContextView,
        (endpoint, schedule_to_close_timeout, outcome): (String, Option<Duration>, Outcome),
    ) -> Self {
        Self {
            endpoint,
            schedule_to_close_timeout,
            outcome,
        }
    }

    #[run]
    pub(crate) async fn run(
        ctx: &mut WorkflowContext<Self>,
    ) -> WorkflowResult<NexusOperationResult> {
        let started = ctx.start_nexus_operation(NexusOperationOptions {
            endpoint: ctx.state(|wf| wf.endpoint.clone()),
            service: "svc".to_string(),
            operation: "op".to_string(),
            schedule_to_close_timeout: ctx.state(|wf| wf.schedule_to_close_timeout),
            ..Default::default()
        });
        if ctx.state(|wf| wf.outcome) == Outcome::CancelAfterRecordedBeforeStarted {
            ctx.timer(Duration::from_millis(1)).await;
            started.cancel();
        }
        let started = started.await.unwrap();
        let result = started.result();
        if matches!(ctx.state(|wf| wf.outcome), Outcome::Cancel) {
            started.cancel();
            started.cancel();
        }
        let res = result.await;
        started.cancel();
        Ok(res)
    }
}

#[workflow]
struct AsyncCompleter {
    outcome: Outcome,
}

#[workflow_methods]
impl AsyncCompleter {
    #[init]
    fn new(_ctx: &WorkflowContextView, outcome: Outcome) -> Self {
        Self { outcome }
    }

    #[allow(dead_code)] // Started via untyped submitter handle
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        match ctx.state(|wf| wf.outcome) {
            Outcome::Succeed => Ok("completed async".to_string()),
            Outcome::Cancel | Outcome::CancelAfterRecordedBeforeStarted => {
                ctx.cancelled().await;
                Err(WorkflowTermination::Cancelled)
            }
            _ => Err(anyhow!("broken").into()),
        }
    }
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_async(
    #[values(
        Outcome::Succeed,
        Outcome::Fail,
        Outcome::Cancel,
        Outcome::CancelAfterRecordedBeforeStarted,
        Outcome::Timeout
    )]
    outcome: Outcome,
) {
    let wf_name = "nexus_async";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    };
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;
    let schedule_to_close_timeout = if outcome == Outcome::CancelAfterRecordedBeforeStarted {
        None
    } else {
        Some(Duration::from_secs(5))
    };

    worker.register_workflow::<NexusAsyncWf>().unwrap();
    worker.register_workflow::<AsyncCompleter>().unwrap();
    let submitter = worker.get_submitter_handle();
    let converter = PayloadConverter::default();
    let ser_ctx = SerializationContext {
        data: &SerializationContextData::Workflow,
        converter: &converter,
    };
    let wf_handle = worker
        .submit_workflow(
            NexusAsyncWf::run,
            (endpoint.clone(), schedule_to_close_timeout, outcome),
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();

    let client = starter.get_client().await;
    let nexus_task_handle = async {
        let mut nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        // Verify request header key for timeout exists and is lowercase
        if outcome == Outcome::Timeout {
            assert!(
                nt.request
                    .as_ref()
                    .unwrap()
                    .header
                    .contains_key("request-timeout")
            );
        }
        let start_req = assert_matches!(
            nt.request.unwrap().variant.unwrap(),
            request::Variant::StartOperation(sr) => sr
        );
        let completer_id = format!("completer-{}", rand_6_chars());
        if !matches!(outcome, Outcome::Timeout) {
            if outcome == Outcome::CancelAfterRecordedBeforeStarted {
                // Server does not permit cancels to happen in this state. So, we wait for one timeout
                // to happen, then say the operation started, after which it will be cancelled.
                let ntt = core_worker.poll_nexus_task().await.unwrap();
                assert_matches!(ntt.variant, Some(nexus_task::Variant::CancelTask(_)));
                core_worker
                    .complete_nexus_task(NexusTaskCompletion {
                        task_token: ntt.task_token().to_vec(),
                        status: Some(nexus_task_completion::Status::AckCancel(true)),
                    })
                    .await
                    .unwrap();
                // Get the next start request
                nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
            }
            let links = start_req
                .links
                .iter()
                .map(workflow_event_link_from_nexus)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            // Start the workflow which will act like the nexus handler and complete the async
            // operation
            let payloads = vec![converter.to_payload(&ser_ctx, &outcome).unwrap()];
            let task_queue = starter.get_task_queue().to_owned();
            submitter
                .submit_wf(
                    AsyncCompleter::name(),
                    payloads,
                    WorkflowStartOptions::new(task_queue, completer_id.clone())
                        .completion_callbacks(vec![Callback {
                            variant: Some(callback::Variant::Nexus(callback::Nexus {
                                url: start_req.callback,
                                header: start_req.callback_header,
                            })),
                            links: links.clone(),
                        }])
                        .links(links)
                        .build(),
                )
                .await
                .unwrap();
        }
        core_worker
            .complete_nexus_task(NexusTaskCompletion {
                task_token: nt.task_token,
                status: Some(nexus_task_completion::Status::Completed(
                    nexus::v1::Response {
                        variant: Some(nexus::v1::response::Variant::StartOperation(
                            StartOperationResponse {
                                variant: Some(start_operation_response::Variant::AsyncSuccess(
                                    #[allow(deprecated)]
                                    start_operation_response::Async {
                                        operation_id: "op-1".to_string(),
                                        links: vec![],
                                        operation_token: "op-1".to_string(),
                                    },
                                )),
                            },
                        )),
                    },
                )),
            })
            .await
            .unwrap();
        if matches!(
            outcome,
            Outcome::Cancel | Outcome::CancelAfterRecordedBeforeStarted
        ) {
            let nt = core_worker.poll_nexus_task().await.unwrap();
            let nt = nt.unwrap_task();
            assert_matches!(
                nt.request.unwrap().variant.unwrap(),
                request::Variant::CancelOperation(_)
            );
            let handle = client.get_workflow_handle::<UntypedWorkflow>(completer_id);
            handle
                .cancel(
                    WorkflowCancelOptions::builder()
                        .reason("nexus cancel")
                        .build(),
                )
                .await
                .unwrap();
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: nt.task_token,
                    status: Some(nexus_task_completion::Status::Completed(
                        nexus::v1::Response {
                            variant: Some(nexus::v1::response::Variant::CancelOperation(
                                CancelOperationResponse {},
                            )),
                        },
                    )),
                })
                .await
                .unwrap();
        }
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    join!(nexus_task_handle, async {
        worker.run_until_done().await.unwrap();
    });

    let res = client
        .get_workflow_handle::<UntypedWorkflow>(starter.get_task_queue().to_owned())
        .get_result(Default::default())
        .await
        .unwrap();
    let res = NexusOperationResult::from_json_payload(res.payloads.first().unwrap()).unwrap();
    match outcome {
        Outcome::Succeed => {
            let p = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Completed(p)) => p
            );
            assert_eq!(p.data, b"\"completed async\"");
        }
        Outcome::Fail => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Failed(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "broken");
        }
        Outcome::Cancel | Outcome::CancelAfterRecordedBeforeStarted => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Cancelled(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "operation canceled");
        }
        Outcome::Timeout => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::TimedOut(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "operation timed out");
        }
    }
    wf_handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}
#[workflow]
#[derive(Default)]
struct NexusCancelBeforeStartWf {
    endpoint: String,
}

#[workflow_methods]
impl NexusCancelBeforeStartWf {
    #[init]
    fn new(_ctx: &WorkflowContextView, endpoint: String) -> Self {
        Self { endpoint }
    }

    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx.start_nexus_operation(NexusOperationOptions {
            endpoint: ctx.state(|wf| wf.endpoint.clone()),
            service: "svc".to_string(),
            operation: "op".to_string(),
            ..Default::default()
        });
        started.cancel();
        let res = started.await.unwrap_err();
        assert_eq!(res.message, "Nexus Operation cancelled before scheduled");
        if let FailureInfo::NexusOperationExecutionFailureInfo(fi) = res.failure_info.unwrap() {
            assert_eq!(fi.endpoint, ctx.state(|wf| wf.endpoint.clone()));
            assert_eq!(fi.service, "svc");
            assert_eq!(fi.operation, "op");
        } else {
            panic!("unexpected failure info");
        }
        Ok(())
    }
}

#[tokio::test]
async fn nexus_cancel_before_start() {
    let wf_name = "nexus_cancel_before_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    };
    let mut worker = starter.worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;

    worker
        .register_workflow::<NexusCancelBeforeStartWf>()
        .unwrap();
    let handle = worker
        .submit_workflow(
            NexusCancelBeforeStartWf::run,
            endpoint,
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();

    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[workflow]
#[derive(Default)]
struct NexusMustCompleteTaskWf {
    endpoint: String,
}

#[workflow_methods]
impl NexusMustCompleteTaskWf {
    #[init]
    fn new(_ctx: &WorkflowContextView, endpoint: String) -> Self {
        Self { endpoint }
    }

    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        // We just need to create the command, not await it.
        drop(ctx.start_nexus_operation(NexusOperationOptions {
            endpoint: ctx.state(|wf| wf.endpoint.clone()),
            service: "svc".to_string(),
            operation: "op".to_string(),
            ..Default::default()
        }));
        // Workflow completes right away, only having scheduled the operation. We need a timer
        // to make sure the nexus task actually gets scheduled.
        ctx.timer(Duration::from_millis(1)).await;
        // started.await.unwrap();
        Ok(())
    }
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_must_complete_task_to_shutdown(#[values(true, false)] use_grace_period: bool) {
    let wf_name = "nexus_must_complete_task_to_shutdown";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    };
    if use_grace_period {
        starter.sdk_config.graceful_shutdown_period = Some(Duration::from_millis(500));
    }
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;

    worker
        .register_workflow::<NexusMustCompleteTaskWf>()
        .unwrap();
    let handle = worker
        .submit_workflow(
            NexusMustCompleteTaskWf::run,
            endpoint,
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();
    let (complete_order_tx, mut complete_order_rx) = mpsc::unbounded_channel();

    let task_handle = async {
        // Should get the nexus task first
        let nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        // The workflow will complete
        handle.get_result(Default::default()).await.unwrap();
        if use_grace_period {
            // Wait for cancel to be sent
            let nt = core_worker.poll_nexus_task().await.unwrap();
            assert_matches!(nt.variant, Some(nexus_task::Variant::CancelTask(_)));
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: nt.task_token().to_vec(),
                    status: Some(nexus_task_completion::Status::AckCancel(true)),
                })
                .await
                .unwrap();
        } else {
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: nt.task_token,
                    #[allow(deprecated)]
                    status: Some(nexus_task_completion::Status::Error(HandlerError {
                        error_type: "BAD_REQUEST".to_string(), // bad req is non-retryable
                        failure: Some(nexus::v1::Failure {
                            message: "busted".to_string(),
                            ..Default::default()
                        }),
                        retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable.into(),
                    })),
                })
                .await
                .unwrap();
        }
        complete_order_tx.send("t").unwrap();
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    join!(task_handle, async {
        worker.run_until_done().await.unwrap();
        complete_order_tx.send("w").unwrap();
    });

    // The first thing to finish needs to have been the nexus task completion
    assert_eq!(complete_order_rx.recv().await.unwrap(), "t");

    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[workflow]
struct NexusCancellationCallerWf {
    endpoint: String,
    schedule_to_close_timeout: Option<Duration>,
    cancellation_type: NexusOperationCancellationType,
    caller_op_future_tx: watch::Sender<bool>,
}

#[workflow_methods(factory_only)]
impl NexusCancellationCallerWf {
    #[run(name = "nexus_cancellation_types")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<NexusOperationResult> {
        let options = NexusOperationOptions {
            endpoint: ctx.state(|wf| wf.endpoint.clone()),
            service: "svc".to_string(),
            operation: "op".to_string(),
            schedule_to_close_timeout: ctx.state(|wf| wf.schedule_to_close_timeout),
            cancellation_type: Some(ctx.state(|wf| wf.cancellation_type)),
            ..Default::default()
        };
        let started = ctx.start_nexus_operation(options);
        let started = started.await.unwrap();
        let result = started.result();
        started.cancel();
        started.cancel();

        let res = result.await;
        ctx.state(|wf| wf.caller_op_future_tx.send(true).unwrap());

        // Make sure cancel after op completion doesn't cause problems
        started.cancel();

        // We need to wait slightly so that the workflow is not complete at the same time
        // cancellation is invoked. If it does, the caller workflow will close and the server
        // won't attempt to send the cancellation to the handler
        ctx.timer(Duration::from_millis(1)).await;
        Ok(res)
    }
}

#[workflow]
struct AsyncCompleterWf {
    cancellation_type: NexusOperationCancellationType,
    cancellation_wait_happened: Arc<AtomicBool>,
    cancellation_tx: watch::Sender<bool>,
    handler_exited_tx: watch::Sender<bool>,
    proceed_signal_received: bool,
}

#[workflow_methods(factory_only)]
impl AsyncCompleterWf {
    #[run(name = "async_completer")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        // Wait for cancellation
        ctx.cancelled().await;
        ctx.state(|wf| wf.cancellation_tx.send(true).unwrap());

        if ctx.state(|wf| wf.cancellation_type)
            == NexusOperationCancellationType::WaitCancellationCompleted
        {
            ctx.wait_condition(|wf| wf.cancellation_wait_happened.load(Ordering::Relaxed))
                .await;
        } else if ctx.state(|wf| wf.cancellation_type)
            == NexusOperationCancellationType::WaitCancellationRequested
        {
            // For WAIT_REQUESTED, wait until the caller nexus op future has been resolved. This
            // allows the test to verify that it resolved due to
            // NexusOperationCancelRequestCompleted (written after cancel handler responds)
            // rather than NexusOperationCanceled (written after handler workflow completes as
            // cancelled).
            ctx.wait_condition(|wf| wf.proceed_signal_received).await;
        }

        ctx.state(|wf| wf.handler_exited_tx.send(true).unwrap());
        Err(WorkflowTermination::Cancelled)
    }

    #[signal(name = "proceed-to-exit")]
    fn handle_proceed_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.proceed_signal_received = true;
    }
}

#[rstest::rstest]
#[tokio::test]
async fn nexus_cancellation_types(
    #[values(
        NexusOperationCancellationType::Abandon,
        NexusOperationCancellationType::TryCancel,
        NexusOperationCancellationType::WaitCancellationRequested,
        NexusOperationCancellationType::WaitCancellationCompleted
    )]
    cancellation_type: NexusOperationCancellationType,
) {
    let wf_name = "nexus_cancellation_types";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    };
    // This test uses a tokio watch channel directly from workflow code to
    // coordinate with the test harness, which triggers nondeterminism detection.
    starter.sdk_config.detect_nondeterministic_futures = false;
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;

    let endpoint = mk_nexus_endpoint(&mut starter).await;
    let schedule_to_close_timeout = Some(Duration::from_secs(5));

    let (caller_op_future_tx, caller_op_future_rx) = watch::channel(false);
    worker
        .register_workflow_with_factory({
            let endpoint = endpoint.clone();
            let caller_op_future_tx = caller_op_future_tx.clone();
            move || NexusCancellationCallerWf {
                endpoint: endpoint.clone(),
                schedule_to_close_timeout,
                cancellation_type,
                caller_op_future_tx: caller_op_future_tx.clone(),
            }
        })
        .unwrap();

    let cancellation_wait_happened = Arc::new(AtomicBool::new(false));
    let (cancellation_tx, mut cancellation_rx) = watch::channel(false);
    let (handler_exited_tx, mut handler_exited_rx) = watch::channel(false);
    worker
        .register_workflow_with_factory({
            let cancellation_wait_happened = cancellation_wait_happened.clone();
            let cancellation_tx = cancellation_tx.clone();
            let handler_exited_tx = handler_exited_tx.clone();
            move || AsyncCompleterWf {
                cancellation_type,
                cancellation_wait_happened: cancellation_wait_happened.clone(),
                cancellation_tx: cancellation_tx.clone(),
                handler_exited_tx: handler_exited_tx.clone(),
                proceed_signal_received: false,
            }
        })
        .unwrap();
    let submitter = worker.get_submitter_handle();
    let wf_handle = starter.start_with_worker(wf_name, &mut worker).await;
    let client = starter.get_client().await;
    let (handler_wf_id_tx, mut handler_wf_id_rx) = tokio::sync::oneshot::channel();
    let completer_id = &format!("completer-{}", rand_6_chars());
    let nexus_task_handle = async {
        let nt = core_worker.poll_nexus_task().await.unwrap().unwrap_task();
        let start_req = assert_matches!(
            nt.request.unwrap().variant.unwrap(),
            request::Variant::StartOperation(sr) => sr
        );
        let _ = handler_wf_id_tx.send(completer_id.clone());
        let links = start_req
            .links
            .iter()
            .map(workflow_event_link_from_nexus)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Start the workflow which will act like the nexus handler and complete the async
        // operation
        let task_queue = starter.get_task_queue().to_owned();
        submitter
            .submit_wf(
                "async_completer",
                vec![],
                WorkflowStartOptions::new(task_queue, completer_id.clone())
                    .completion_callbacks(vec![Callback {
                        variant: Some(callback::Variant::Nexus(callback::Nexus {
                            url: start_req.callback,
                            header: start_req.callback_header,
                        })),
                        links: links.clone(),
                    }])
                    .links(links)
                    .build(),
            )
            .await
            .unwrap();
        core_worker
            .complete_nexus_task(NexusTaskCompletion {
                task_token: nt.task_token,
                status: Some(nexus_task_completion::Status::Completed(
                    nexus::v1::Response {
                        variant: Some(nexus::v1::response::Variant::StartOperation(
                            StartOperationResponse {
                                variant: Some(start_operation_response::Variant::AsyncSuccess(
                                    #[allow(deprecated)]
                                    start_operation_response::Async {
                                        operation_id: "op-1".to_string(),
                                        links: vec![],
                                        operation_token: "op-1".to_string(),
                                    },
                                )),
                            },
                        )),
                    },
                )),
            })
            .await
            .unwrap();

        match cancellation_type {
            NexusOperationCancellationType::WaitCancellationCompleted
            | NexusOperationCancellationType::WaitCancellationRequested => {
                // The nexus op future should not have been resolved
                assert!(!*caller_op_future_rx.borrow());
            }
            NexusOperationCancellationType::Abandon | NexusOperationCancellationType::TryCancel => {
                wf_handle.get_result(Default::default()).await.unwrap();
                // The nexus op future should have been resolved
                assert!(*caller_op_future_rx.borrow())
            }
        }
        let (cancel_handler_responded_tx, _cancel_handler_responded_rx) = watch::channel(false);
        if cancellation_type != NexusOperationCancellationType::Abandon {
            let nt = core_worker.poll_nexus_task().await.unwrap();
            let nt = nt.unwrap_task();
            assert_matches!(
                nt.request.unwrap().variant.unwrap(),
                request::Variant::CancelOperation(_)
            );
            let handle = client.get_workflow_handle::<UntypedWorkflow>(completer_id.to_string());
            handle
                .cancel(
                    WorkflowCancelOptions::builder()
                        .reason("nexus cancel")
                        .build(),
                )
                .await
                .unwrap();
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token: nt.task_token,
                    status: Some(nexus_task_completion::Status::Completed(
                        nexus::v1::Response {
                            variant: Some(nexus::v1::response::Variant::CancelOperation(
                                CancelOperationResponse {},
                            )),
                        },
                    )),
                })
                .await
                .unwrap();
            // Mark that the cancel handler has responded
            cancel_handler_responded_tx.send(true).unwrap();
        }

        // Check that the nexus op future resolves only _after_ the handler WF completes
        if cancellation_type == NexusOperationCancellationType::WaitCancellationCompleted {
            assert!(!*caller_op_future_rx.borrow());

            cancellation_wait_happened.store(true, Ordering::Relaxed);
            // Send a signal just to wake up the workflow so it'll check the condition
            // (it may already have completed, so ignore the result)
            let _ = client
                .get_workflow_handle::<UntypedWorkflow>(completer_id.to_string())
                .signal(
                    UntypedSignal::new("wakeupdude"),
                    RawValue::empty(),
                    WorkflowSignalOptions::default(),
                )
                .await;
            wf_handle.get_result(Default::default()).await.unwrap();
            assert!(*caller_op_future_rx.borrow());
        }

        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    let shutdown_handle = worker.inner_mut().shutdown_handle();

    let check_caller_op_future_resolved_then_allow_handler_to_complete = async {
        // The caller nexus op future has been resolved
        assert!(*caller_op_future_rx.borrow());

        // Verify the handler workflow has not exited yet. This proves that the caller op future
        // was resolved as a result of NexusOperationCancelRequestCompleted (written after cancel
        // handler responds), as opposed to NexusOperationCanceled (written after handler workflow
        // exits).
        assert!(
            !*handler_exited_rx.borrow(),
            "Handler should not have exited yet"
        );

        let handler_wf_id = handler_wf_id_rx
            .try_recv()
            .expect("Should have received handler workflow ID");
        client
            .get_workflow_handle::<async_completer_wf::Run>(handler_wf_id)
            .signal(
                AsyncCompleterWf::handle_proceed_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        handler_exited_rx.changed().await.unwrap();
        assert!(*handler_exited_rx.borrow());
    };

    join!(
        nexus_task_handle,
        async { worker.inner_mut().run().await.unwrap() },
        async {
            wf_handle.get_result(Default::default()).await.unwrap();
            if cancellation_type == NexusOperationCancellationType::TryCancel {
                cancellation_rx.changed().await.unwrap();
            }
            if cancellation_type == NexusOperationCancellationType::WaitCancellationRequested {
                check_caller_op_future_resolved_then_allow_handler_to_complete.await;
            }
            shutdown_handle();
        }
    );

    match cancellation_type {
        NexusOperationCancellationType::Abandon => {
            assert!(!*cancellation_rx.borrow());
        }
        NexusOperationCancellationType::TryCancel
        | NexusOperationCancellationType::WaitCancellationRequested
        | NexusOperationCancellationType::WaitCancellationCompleted => {
            assert!(*cancellation_rx.borrow())
        }
    }

    let res = client
        .get_workflow_handle::<UntypedWorkflow>(starter.get_task_queue().to_owned())
        .get_result(Default::default())
        .await
        .unwrap();
    let res = NexusOperationResult::from_json_payload(res.payloads.first().unwrap()).unwrap();

    match cancellation_type {
        NexusOperationCancellationType::Abandon | NexusOperationCancellationType::TryCancel => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Cancelled(f)) => f
            );
            assert_eq!(f.message, "Nexus operation cancelled after starting");
        }
        NexusOperationCancellationType::WaitCancellationRequested => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Cancelled(f)) => f
            );
            assert_eq!(f.message, "Nexus operation cancellation request completed");
        }
        NexusOperationCancellationType::WaitCancellationCompleted => {
            let f = assert_matches!(
                res.status,
                Some(nexus_operation_result::Status::Cancelled(f)) => f
            );
            assert_eq!(f.message, "nexus operation completed unsuccessfully");
            assert_eq!(f.cause.unwrap().message, "operation canceled");
        }
    }

    wf_handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}
