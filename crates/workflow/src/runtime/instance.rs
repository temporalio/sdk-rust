//! Guest-side workflow execution implementation used by native and future WASM hosts.

use crate::{
    BaseWorkflowContext, WorkflowContext,
    runtime::{
        entry::{WorkflowError, WorkflowImplementation},
        guest::WorkflowInstance,
        model::{TimerResult, UnblockEvent, WorkflowResult, WorkflowTermination},
        types::{
            ActivationJobResult, ActivationResult, MAIN_ROUTINE_ID, MainRoutineCompletion,
            QueryResponse, RoutineCompletion, RoutineId, RoutineKind, RoutinePollResult,
            StartedRoutine, UpdateRoutineCompletion, UpdateRoutineKind, WorkflowActivation,
            WorkflowFailure,
        },
    },
};
use futures_util::{
    FutureExt,
    future::{Fuse, LocalBoxFuture},
};
use std::{
    cell::RefCell,
    collections::HashMap,
    future::ready,
    rc::Rc,
    task::{Context, Poll, Waker},
};
use temporalio_common_wasm::{
    WorkflowDefinition,
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    error::ApplicationFailure,
    protos::{
        coresdk::workflow_activation::{
            DoUpdate, QueryWorkflow, SignalWorkflow,
            workflow_activation_job::Variant as ActivationVariant,
        },
        temporal::api::{
            common::v1::{Payload, Payloads},
            failure::v1::Failure,
        },
    },
};

pub struct GuestWorkflowInstance<W: WorkflowImplementation> {
    base_ctx: BaseWorkflowContext,
    ctx: WorkflowContext<W>,
    run_future: Fuse<LocalBoxFuture<'static, Result<Payload, WorkflowTermination>>>,
    next_routine_id: RoutineId,
    routines: HashMap<RoutineId, GuestRoutine>,
}

enum GuestRoutine {
    Signal {
        future: LocalBoxFuture<'static, Result<(), WorkflowError>>,
    },
    Update {
        protocol_instance_id: String,
        future: LocalBoxFuture<'static, Result<Payload, WorkflowError>>,
    },
}

enum RoutinePollState<T> {
    Ready {
        result: T,
        made_progress: bool,
    },
    ForcedFailure {
        failure: WorkflowFailure,
        made_progress: bool,
    },
    Stalled {
        made_progress: bool,
    },
}

fn expect_resolution<T>(value: Option<T>) -> T {
    value.expect("resolution expected payload")
}

impl<W: WorkflowImplementation> GuestWorkflowInstance<W>
where
    <W::Run as WorkflowDefinition>::Input: Send,
{
    pub fn instantiate(
        payloads: Vec<Payload>,
        converter: PayloadConverter,
        base_ctx: BaseWorkflowContext,
    ) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError> {
        let ser_ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };
        let input = converter.from_payloads(&ser_ctx, payloads)?;
        let (init_input, run_input) = if W::INIT_TAKES_INPUT {
            (Some(input), None)
        } else {
            (None, Some(input))
        };
        Ok(Box::new({
            let view = base_ctx.view();
            let workflow = W::init(view, init_input);
            Self::new_with_workflow(workflow, base_ctx, run_input)
        }))
    }

    pub fn new_with_workflow(
        workflow: W,
        base_ctx: BaseWorkflowContext,
        run_input: Option<<W::Run as WorkflowDefinition>::Input>,
    ) -> Self {
        let workflow = Rc::new(RefCell::new(workflow));
        let ctx = WorkflowContext::from_base(base_ctx.clone(), workflow);
        let run_future = W::run(ctx.clone(), run_input).fuse();
        Self {
            base_ctx,
            ctx,
            run_future,
            next_routine_id: MAIN_ROUTINE_ID + 1,
            routines: HashMap::new(),
        }
    }

    fn query_metadata(&self) -> QueryResponse {
        #[derive(serde::Serialize)]
        struct WorkflowMetadataJson {
            #[serde(rename = "currentDetails", skip_serializing_if = "String::is_empty")]
            current_details: String,
        }

        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };
        QueryResponse {
            result: converter
                .to_payload(
                    &ctx,
                    &WorkflowMetadataJson {
                        current_details: self.base_ctx.current_details(),
                    },
                )
                .map_err(|err| Failure {
                    message: err.to_string(),
                    ..Default::default()
                }),
        }
    }

    fn rejection_for_missing_update_handler(&self, name: String) -> ActivationJobResult {
        ActivationJobResult::UpdateRejected(Box::new(self.message_to_failure(format!(
            "No update handler registered for update name {name}"
        ))))
    }

    fn workflow_error_to_failure(&self, err: WorkflowError) -> Failure {
        use temporalio_common_wasm::error::{OutgoingError, OutgoingWorkflowError};
        let outgoing: OutgoingWorkflowError = match err {
            WorkflowError::PayloadConversion(err) => OutgoingWorkflowError::from(err),
            WorkflowError::Execution(err) => {
                OutgoingWorkflowError::Application(Box::new(ApplicationFailure::new(err)))
            }
        };
        self.base_ctx.data_converter().to_failure(
            &SerializationContextData::Workflow,
            OutgoingError::Workflow(outgoing),
        )
    }

    fn message_to_failure(&self, message: String) -> Failure {
        use temporalio_common_wasm::error::{OutgoingError, OutgoingWorkflowError};
        self.base_ctx.data_converter().to_failure(
            &SerializationContextData::Workflow,
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::new(message),
            ))),
        )
    }

    fn next_routine_id(&mut self) -> RoutineId {
        let id = self.next_routine_id;
        self.next_routine_id += 1;
        id
    }

    fn start_signal_routine(&mut self, signal: SignalWorkflow) -> ActivationJobResult {
        let name = signal.signal_name;
        let payloads = Payloads {
            payloads: signal.input,
        };
        let converter = self.ctx.payload_converter();
        let future = match W::decode_signal_input(&name, payloads, converter) {
            Ok(Some(input)) => {
                let ctx = self.ctx.with_headers(signal.headers);
                W::dispatch_signal(ctx, &name, input)
            }
            Err(err) => ready(Err(err)).boxed_local(),
            Ok(None) => return ActivationJobResult::None,
        };
        let routine_id = self.next_routine_id();
        self.routines
            .insert(routine_id, GuestRoutine::Signal { future });
        ActivationJobResult::StartedRoutine(StartedRoutine {
            routine_id,
            kind: RoutineKind::Signal(name),
        })
    }

    fn start_update_routine(&mut self, update: DoUpdate) -> ActivationJobResult {
        let DoUpdate {
            id,
            protocol_instance_id,
            name,
            input,
            headers,
            run_validator,
            ..
        } = update;
        let has_validator = match W::definition()
            .updates
            .into_iter()
            .find(|update| update.name.as_str() == name)
            .map(|update| update.has_validator)
        {
            Some(has_validator) => has_validator,
            None => return self.rejection_for_missing_update_handler(name),
        };

        if run_validator && has_validator {
            let payloads = Payloads {
                payloads: input.clone(),
            };
            let converter = self.ctx.payload_converter();
            let decoded_input = match W::decode_update_input(&name, payloads, converter) {
                Ok(Some(input)) => input,
                Err(err) => {
                    return ActivationJobResult::UpdateRejected(Box::new(
                        self.workflow_error_to_failure(err),
                    ));
                }
                Ok(None) => {
                    return self.rejection_for_missing_update_handler(name);
                }
            };
            let view = self.ctx.view();
            let validation = self
                .ctx
                .state(|wf| wf.validate_update(view, &name, decoded_input));
            match validation {
                Ok(()) => {}
                Err(e) => {
                    return ActivationJobResult::UpdateRejected(Box::new(
                        self.workflow_error_to_failure(e),
                    ));
                }
            }
        }

        let payloads = Payloads { payloads: input };
        let converter = self.ctx.payload_converter();
        let future = match W::decode_update_input(&name, payloads, converter) {
            Ok(Some(input)) => {
                let ctx = self.ctx.with_headers(headers);
                W::dispatch_update(ctx, &name, input, converter)
            }
            Err(err) => ready(Err(err)).boxed_local(),
            Ok(None) => {
                return self.rejection_for_missing_update_handler(name);
            }
        };
        let routine_id = self.next_routine_id();
        self.routines.insert(
            routine_id,
            GuestRoutine::Update {
                protocol_instance_id: protocol_instance_id.clone(),
                future,
            },
        );
        ActivationJobResult::StartedRoutine(StartedRoutine {
            routine_id,
            kind: RoutineKind::Update(UpdateRoutineKind {
                name,
                update_id: id,
                protocol_instance_id,
            }),
        })
    }

    fn query(&self, query: QueryWorkflow) -> QueryResponse {
        if query.query_type == "__temporal_workflow_metadata" {
            return self.query_metadata();
        }

        let payloads = Payloads {
            payloads: query.arguments,
        };
        let converter = self.ctx.payload_converter();
        let view = self.ctx.view();
        let decoded_input = match W::decode_query_input(&query.query_type, &payloads, converter) {
            Ok(Some(input)) => input,
            Err(err) => {
                return QueryResponse {
                    result: Err(self.workflow_error_to_failure(err)),
                };
            }
            Ok(None) => {
                return QueryResponse {
                    result: Err(self.message_to_failure(format!(
                        "No query handler for '{}'",
                        query.query_type
                    ))),
                };
            }
        };
        QueryResponse {
            result: self
                .ctx
                .state(|wf| wf.dispatch_query(view, &query.query_type, decoded_input, converter))
                .map_err(|err| self.workflow_error_to_failure(err)),
        }
    }

    fn apply_resolution(&mut self, resolution: ActivationVariant) {
        let event = match resolution {
            ActivationVariant::FireTimer(event) => {
                UnblockEvent::Timer(event.seq, TimerResult::Fired)
            }
            ActivationVariant::ResolveActivity(event) => {
                UnblockEvent::Activity(event.seq, Box::new(expect_resolution(event.result)))
            }
            ActivationVariant::ResolveChildWorkflowExecutionStart(event) => {
                UnblockEvent::WorkflowStart(event.seq, Box::new(expect_resolution(event.status)))
            }
            ActivationVariant::ResolveChildWorkflowExecution(event) => {
                UnblockEvent::WorkflowComplete(event.seq, Box::new(expect_resolution(event.result)))
            }
            ActivationVariant::ResolveSignalExternalWorkflow(event) => {
                UnblockEvent::SignalExternal(event.seq, event.failure)
            }
            ActivationVariant::ResolveRequestCancelExternalWorkflow(event) => {
                UnblockEvent::CancelExternal(event.seq, event.failure)
            }
            ActivationVariant::ResolveNexusOperationStart(event) => {
                UnblockEvent::NexusOperationStart(
                    event.seq,
                    Box::new(expect_resolution(event.status)),
                )
            }
            ActivationVariant::ResolveNexusOperation(event) => {
                UnblockEvent::NexusOperationComplete(
                    event.seq,
                    Box::new(expect_resolution(event.result)),
                )
            }
            _ => unreachable!("only resolution jobs can be applied as resolutions"),
        };
        self.base_ctx
            .unblock(event)
            .expect("resolution must have a registered unblocker");
    }

    fn terminal_outcome_from_result(
        &self,
        result: WorkflowResult<Payload>,
    ) -> crate::runtime::types::TerminalOutcome {
        match result {
            Ok(result) => crate::runtime::types::TerminalOutcome::Completed(result),
            Err(WorkflowTermination::ContinueAsNew(req)) => {
                crate::runtime::types::TerminalOutcome::ContinueAsNew(req)
            }
            Err(WorkflowTermination::Cancelled) => {
                crate::runtime::types::TerminalOutcome::Cancelled
            }
            Err(WorkflowTermination::Evicted) => {
                panic!("workflow instances must not explicitly return eviction")
            }
            Err(WorkflowTermination::Failed(err)) => {
                let failure = self.base_ctx.data_converter().to_failure(
                    &SerializationContextData::Workflow,
                    temporalio_common_wasm::error::OutgoingError::Workflow(err),
                );
                crate::runtime::types::TerminalOutcome::Failed(Box::new(failure))
            }
        }
    }

    fn poll_routine_loop<F: Future + Unpin>(
        base_ctx: &BaseWorkflowContext,
        cx: &mut Context<'_>,
        future: &mut F,
    ) -> RoutinePollState<F::Output> {
        base_ctx.take_state_mutated();
        base_ctx.take_runtime_progress();
        let mut made_progress = false;

        loop {
            if let Some(failure) = base_ctx.take_forced_wft_failure().map(|err| {
                Box::new(Failure {
                    message: err.to_string(),
                    ..Default::default()
                })
            }) {
                return RoutinePollState::ForcedFailure {
                    failure,
                    made_progress,
                };
            }

            match future.poll_unpin(cx) {
                Poll::Ready(result) => {
                    let state_mutated = base_ctx.take_state_mutated();
                    let runtime_progress = base_ctx.take_runtime_progress();
                    made_progress |= state_mutated || runtime_progress;
                    return RoutinePollState::Ready {
                        result,
                        made_progress,
                    };
                }
                Poll::Pending => {
                    let state_mutated = base_ctx.take_state_mutated();
                    let runtime_progress = base_ctx.take_runtime_progress();
                    made_progress |= state_mutated || runtime_progress;
                    if !(state_mutated || runtime_progress) {
                        return RoutinePollState::Stalled { made_progress };
                    }
                }
            }
        }
    }

    fn poll_main_routine(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        Ok(
            match Self::poll_routine_loop(&self.base_ctx, cx, &mut self.run_future) {
                RoutinePollState::Ready {
                    result,
                    made_progress,
                } => RoutinePollResult {
                    completion: Some(RoutineCompletion::Main(MainRoutineCompletion::Terminal(
                        Box::new(self.terminal_outcome_from_result(result)),
                    ))),
                    made_progress,
                },
                RoutinePollState::ForcedFailure {
                    failure,
                    made_progress,
                } => RoutinePollResult {
                    completion: Some(RoutineCompletion::Main(MainRoutineCompletion::TaskFailed(
                        crate::runtime::types::TaskFailure {
                            failure,
                            force_cause: None,
                        },
                    ))),
                    made_progress,
                },
                RoutinePollState::Stalled { made_progress } => RoutinePollResult {
                    completion: Some(RoutineCompletion::Main(MainRoutineCompletion::Blocked)),
                    made_progress,
                },
            },
        )
    }

    fn poll_signal_routine(
        &mut self,
        routine_id: RoutineId,
        mut future: LocalBoxFuture<'static, Result<(), WorkflowError>>,
        cx: &mut Context<'_>,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        match Self::poll_routine_loop(&self.base_ctx, cx, &mut future) {
            RoutinePollState::Ready {
                result,
                made_progress,
            } => {
                let result = result.map_err(|err| Box::new(self.workflow_error_to_failure(err)));
                Ok(RoutinePollResult {
                    completion: Some(RoutineCompletion::Signal(result)),
                    made_progress,
                })
            }
            RoutinePollState::ForcedFailure { failure, .. } => Err(failure),
            RoutinePollState::Stalled { made_progress } => {
                self.routines
                    .insert(routine_id, GuestRoutine::Signal { future });
                Ok(RoutinePollResult {
                    completion: None,
                    made_progress,
                })
            }
        }
    }

    fn poll_update_routine(
        &mut self,
        routine_id: RoutineId,
        protocol_instance_id: String,
        mut future: LocalBoxFuture<'static, Result<Payload, WorkflowError>>,
        cx: &mut Context<'_>,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        match Self::poll_routine_loop(&self.base_ctx, cx, &mut future) {
            RoutinePollState::Ready {
                result,
                made_progress,
            } => {
                let completion = match result {
                    Ok(result) => UpdateRoutineCompletion::Completed {
                        protocol_instance_id,
                        result,
                    },
                    Err(err) => UpdateRoutineCompletion::Rejected {
                        protocol_instance_id,
                        failure: Box::new(self.workflow_error_to_failure(err)),
                    },
                };
                Ok(RoutinePollResult {
                    completion: Some(RoutineCompletion::Update(completion)),
                    made_progress,
                })
            }
            RoutinePollState::ForcedFailure { failure, .. } => Err(failure),
            RoutinePollState::Stalled { made_progress } => {
                self.routines.insert(
                    routine_id,
                    GuestRoutine::Update {
                        protocol_instance_id,
                        future,
                    },
                );
                Ok(RoutinePollResult {
                    completion: None,
                    made_progress,
                })
            }
        }
    }
}

impl<W: WorkflowImplementation> WorkflowInstance for GuestWorkflowInstance<W>
where
    <W::Run as WorkflowDefinition>::Input: Send,
{
    fn activate(
        &mut self,
        activation: WorkflowActivation,
    ) -> Result<ActivationResult, WorkflowFailure> {
        self.base_ctx.apply_activation_context(&activation);
        let mut job_results = Vec::with_capacity(activation.jobs.len());
        for job in activation.jobs {
            let result = match job.variant {
                Some(ActivationVariant::InitializeWorkflow(_))
                | Some(ActivationVariant::UpdateRandomSeed(_)) => ActivationJobResult::None,
                Some(ActivationVariant::NotifyHasPatch(patch)) => {
                    self.base_ctx.record_patch(patch.patch_id, true);
                    ActivationJobResult::None
                }
                Some(ActivationVariant::CancelWorkflow(cancel)) => {
                    self.base_ctx.notify_cancel(cancel.reason);
                    ActivationJobResult::None
                }
                Some(ActivationVariant::SignalWorkflow(signal)) => {
                    self.start_signal_routine(signal)
                }
                Some(ActivationVariant::DoUpdate(update)) => self.start_update_routine(update),
                Some(ActivationVariant::QueryWorkflow(query)) => {
                    ActivationJobResult::QueryResponse(Box::new(self.query(query)))
                }
                Some(
                    resolution @ (ActivationVariant::FireTimer(_)
                    | ActivationVariant::ResolveActivity(_)
                    | ActivationVariant::ResolveChildWorkflowExecutionStart(_)
                    | ActivationVariant::ResolveChildWorkflowExecution(_)
                    | ActivationVariant::ResolveSignalExternalWorkflow(_)
                    | ActivationVariant::ResolveRequestCancelExternalWorkflow(_)
                    | ActivationVariant::ResolveNexusOperationStart(_)
                    | ActivationVariant::ResolveNexusOperation(_)),
                ) => {
                    self.apply_resolution(resolution);
                    ActivationJobResult::None
                }
                Some(ActivationVariant::RemoveFromCache(_)) => ActivationJobResult::None,
                None => {
                    return Err(Box::new(Failure {
                        message: "Activation job missing variant".to_string(),
                        ..Default::default()
                    }));
                }
            };
            job_results.push(result);
        }
        Ok(ActivationResult { job_results })
    }

    fn poll_routine(
        &mut self,
        routine_id: RoutineId,
        waker: &Waker,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        let mut cx = Context::from_waker(waker);
        if routine_id == MAIN_ROUTINE_ID {
            return self.poll_main_routine(&mut cx);
        }

        let routine = self.routines.remove(&routine_id).ok_or_else(|| {
            Box::new(Failure {
                message: format!("No routine registered for id {routine_id}"),
                ..Default::default()
            })
        })?;

        match routine {
            GuestRoutine::Signal { future } => {
                self.poll_signal_routine(routine_id, future, &mut cx)
            }
            GuestRoutine::Update {
                protocol_instance_id,
                future,
            } => self.poll_update_routine(routine_id, protocol_instance_id, future, &mut cx),
        }
    }
}

pub fn instantiate_workflow<W: WorkflowImplementation>(
    payloads: Vec<Payload>,
    converter: PayloadConverter,
    base_ctx: BaseWorkflowContext,
) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError>
where
    <W::Run as WorkflowDefinition>::Input: Send,
{
    GuestWorkflowInstance::<W>::instantiate(payloads, converter, base_ctx)
}
