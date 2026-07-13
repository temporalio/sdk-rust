//! Guest-side workflow execution implementation used by native and future WASM hosts.

use crate::{
    BaseWorkflowContext, WorkflowContext, WorkflowContextView,
    runtime::{
        ConstructionBlockedFuture,
        entry::{WorkflowError, WorkflowImplementation},
        guest::WorkflowInstance,
        model::{TimerResult, UnblockEvent, WorkflowTermination},
        types::{
            ActivationJobResult, ActivationResult, MAIN_ROUTINE_ID, MainRoutineCompletion,
            QueryResponse, RoutineCompletion, RoutineId, RoutineKind, RoutinePollResult,
            StartedRoutine, UpdateRoutineCompletion, UpdateRoutineKind, WorkflowActivation,
            WorkflowFailure,
        },
    },
    workflow_interceptors::{
        ExecuteWorkflowInput, ExecuteWorkflowResult, HandleQueryInput, HandleQueryResult,
        HandleSignalInput, HandleSignalResult, HandleUpdateInput, HandleUpdateResult,
        InitializeWorkflowInput, InitializeWorkflowOutput, SyncWorkflowInterceptorContext,
        ValidateUpdateInput, ValidateUpdateResult, WorkflowInterceptor, WorkflowInterceptorContext,
        WorkflowInterceptorFactory, WorkflowInterceptorFuture, WorkflowNext,
        serialize_workflow_output, wrong_workflow_input_type,
    },
};
use futures_util::{
    FutureExt,
    future::{Fuse, LocalBoxFuture},
};
use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    future::ready,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use temporalio_common_wasm::{
    WorkflowDefinition,
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    error::{ApplicationFailure, OutgoingError, OutgoingWorkflowError},
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
    run_future: Fuse<LocalBoxFuture<'static, ExecuteWorkflowResult>>,
    interceptors: Vec<Arc<dyn WorkflowInterceptor>>,
    main_construction_polled: bool,
    next_routine_id: RoutineId,
    routines: HashMap<RoutineId, GuestRoutine>,
}

enum GuestRoutine {
    Signal {
        future: LocalBoxFuture<'static, HandleSignalResult>,
    },
    Update {
        protocol_instance_id: String,
        future: LocalBoxFuture<'static, HandleUpdateResult>,
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

fn call_initialize_workflow<'a>(
    interceptors: &'a [Arc<dyn WorkflowInterceptor>],
    ctx: WorkflowContextView,
    input: InitializeWorkflowInput,
    next: WorkflowNext<'a, InitializeWorkflowInput, InitializeWorkflowOutput>,
) -> InitializeWorkflowOutput {
    if let Some((first, rest)) = interceptors.split_first() {
        let next_ctx = ctx.clone();
        first.initialize_workflow(
            ctx,
            input,
            WorkflowNext::new(move |input| call_initialize_workflow(rest, next_ctx, input, next)),
        )
    } else {
        next.run(input)
    }
}

fn call_execute_workflow<'a>(
    interceptors: &'a [Arc<dyn WorkflowInterceptor>],
    ctx: WorkflowInterceptorContext,
    input: ExecuteWorkflowInput,
    next: WorkflowNext<
        'a,
        ExecuteWorkflowInput,
        WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
    >,
) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
    if let Some((first, rest)) = interceptors.split_first() {
        let next_ctx = ctx.clone();
        first.execute(
            ctx,
            input,
            WorkflowNext::new(move |input| call_execute_workflow(rest, next_ctx, input, next)),
        )
    } else {
        next.run(input)
    }
}

fn call_handle_signal<'a>(
    interceptors: &'a [Arc<dyn WorkflowInterceptor>],
    ctx: WorkflowInterceptorContext,
    input: HandleSignalInput,
    next: WorkflowNext<'a, HandleSignalInput, WorkflowInterceptorFuture<'a, HandleSignalResult>>,
) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
    if let Some((first, rest)) = interceptors.split_first() {
        let next_ctx = ctx.clone();
        first.handle_signal(
            ctx,
            input,
            WorkflowNext::new(move |input| call_handle_signal(rest, next_ctx, input, next)),
        )
    } else {
        next.run(input)
    }
}

fn call_handle_update<'a>(
    interceptors: &'a [Arc<dyn WorkflowInterceptor>],
    ctx: WorkflowInterceptorContext,
    input: HandleUpdateInput,
    next: WorkflowNext<'a, HandleUpdateInput, WorkflowInterceptorFuture<'a, HandleUpdateResult>>,
) -> WorkflowInterceptorFuture<'a, HandleUpdateResult> {
    if let Some((first, rest)) = interceptors.split_first() {
        let next_ctx = ctx.clone();
        first.handle_update(
            ctx,
            input,
            WorkflowNext::new(move |input| call_handle_update(rest, next_ctx, input, next)),
        )
    } else {
        next.run(input)
    }
}

fn call_handle_query<'a>(
    interceptors: &'a [Arc<dyn WorkflowInterceptor>],
    ctx: SyncWorkflowInterceptorContext,
    input: HandleQueryInput,
    next: WorkflowNext<'a, HandleQueryInput, HandleQueryResult>,
) -> HandleQueryResult {
    if let Some((first, rest)) = interceptors.split_first() {
        let next_ctx = ctx.clone();
        first.handle_query(
            ctx,
            input,
            WorkflowNext::new(move |input| call_handle_query(rest, next_ctx, input, next)),
        )
    } else {
        next.run(input)
    }
}

fn call_validate_update<'a>(
    interceptors: &'a [Arc<dyn WorkflowInterceptor>],
    ctx: SyncWorkflowInterceptorContext,
    input: ValidateUpdateInput,
    next: WorkflowNext<'a, ValidateUpdateInput, ValidateUpdateResult>,
) -> ValidateUpdateResult {
    if let Some((first, rest)) = interceptors.split_first() {
        let next_ctx = ctx.clone();
        first.validate_update(
            ctx,
            input,
            WorkflowNext::new(move |input| call_validate_update(rest, next_ctx, input, next)),
        )
    } else {
        next.run(input)
    }
}

fn intercepted_execute_future<W>(
    ctx: WorkflowContext<W>,
    base_ctx: BaseWorkflowContext,
    run_input: Option<<W::Run as WorkflowDefinition>::Input>,
    headers: HashMap<String, Payload>,
    interceptors: Vec<Arc<dyn WorkflowInterceptor>>,
) -> LocalBoxFuture<'static, ExecuteWorkflowResult>
where
    W: WorkflowImplementation,
    <W::Run as WorkflowDefinition>::Input: Send,
{
    async move {
        let input = ExecuteWorkflowInput::new(
            run_input.map(|input| Box::new(input) as Box<dyn Any>),
            headers,
        );
        let handler_base_ctx = base_ctx.clone();
        let interceptor_ctx = WorkflowInterceptorContext::new(base_ctx);
        let next = WorkflowNext::new(move |input: ExecuteWorkflowInput| {
            let (input, headers) = input.into_parts();
            let run_input = match input {
                Some(input) => match input.downcast::<<W::Run as WorkflowDefinition>::Input>() {
                    Ok(input) => Some(*input),
                    Err(_) => {
                        return WorkflowInterceptorFuture::new(ready(Err(
                            wrong_workflow_input_type(W::name()),
                        )));
                    }
                },
                None => None,
            };
            WorkflowInterceptorFuture::new(ConstructionBlockedFuture::new(
                handler_base_ctx,
                W::run(ctx.with_headers(headers), run_input),
            ))
        });
        call_execute_workflow(&interceptors, interceptor_ctx, input, next).await
    }
    .boxed_local()
}

fn intercepted_signal_future<W>(
    ctx: WorkflowContext<W>,
    base_ctx: BaseWorkflowContext,
    interceptors: Vec<Arc<dyn WorkflowInterceptor>>,
    input: HandleSignalInput,
) -> LocalBoxFuture<'static, HandleSignalResult>
where
    W: WorkflowImplementation,
{
    async move {
        let interceptor_ctx = WorkflowInterceptorContext::new(base_ctx);
        let next = WorkflowNext::new(move |input: HandleSignalInput| {
            let (name, input, headers) = input.into_parts();
            WorkflowInterceptorFuture::new(W::dispatch_signal(
                ctx.with_headers(headers),
                &name,
                input,
            ))
        });
        call_handle_signal(&interceptors, interceptor_ctx, input, next).await
    }
    .boxed_local()
}

fn intercepted_update_future<W>(
    ctx: WorkflowContext<W>,
    base_ctx: BaseWorkflowContext,
    interceptors: Vec<Arc<dyn WorkflowInterceptor>>,
    input: HandleUpdateInput,
) -> LocalBoxFuture<'static, HandleUpdateResult>
where
    W: WorkflowImplementation,
{
    async move {
        let interceptor_ctx = WorkflowInterceptorContext::new(base_ctx);
        let next = WorkflowNext::new(move |input: HandleUpdateInput| {
            let (name, input, headers) = input.into_parts();
            WorkflowInterceptorFuture::new(W::dispatch_update(
                ctx.with_headers(headers),
                &name,
                input,
            ))
        });
        call_handle_update(&interceptors, interceptor_ctx, input, next).await
    }
    .boxed_local()
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
        Self::instantiate_with_interceptors(payloads, converter, base_ctx, Vec::new())
    }

    pub fn instantiate_with_interceptors(
        payloads: Vec<Payload>,
        converter: PayloadConverter,
        base_ctx: BaseWorkflowContext,
        interceptors: Vec<Arc<dyn WorkflowInterceptor>>,
    ) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError> {
        Self::instantiate_with_interceptor_provider(payloads, converter, base_ctx, move || {
            interceptors
        })
    }

    pub fn instantiate_with_interceptor_factories(
        payloads: Vec<Payload>,
        converter: PayloadConverter,
        base_ctx: BaseWorkflowContext,
        interceptor_factories: Vec<Arc<dyn WorkflowInterceptorFactory>>,
    ) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError> {
        Self::instantiate_with_interceptor_provider(payloads, converter, base_ctx, move || {
            interceptor_factories
                .into_iter()
                .flat_map(|factory| factory.create().into_inner())
                .collect()
        })
    }

    fn instantiate_with_interceptor_provider(
        payloads: Vec<Payload>,
        converter: PayloadConverter,
        base_ctx: BaseWorkflowContext,
        create_interceptors: impl FnOnce() -> Vec<Arc<dyn WorkflowInterceptor>>,
    ) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError> {
        let interceptors = create_interceptors();
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
        let view = base_ctx.view();
        let (workflow, headers) = if W::HAS_INIT {
            let input = InitializeWorkflowInput::new(
                init_input.map(|input| Box::new(input) as Box<dyn Any>),
                base_ctx.initial_headers(),
            );
            let initialized = RefCell::new(None);
            let initialized_ref = &initialized;
            let init_view = view.clone();
            let next = WorkflowNext::new(move |input: InitializeWorkflowInput| {
                let (input, headers) = input.into_parts();
                let input = input.map(|input| {
                    *input
                        .downcast::<<W::Run as WorkflowDefinition>::Input>()
                        .unwrap_or_else(|_| {
                            panic!("workflow initialization received the wrong concrete input type")
                        })
                });
                initialized_ref.replace(Some((W::init(init_view, input), headers)));
                InitializeWorkflowOutput::new()
            });
            let _ = call_initialize_workflow(&interceptors, view, input, next);
            initialized
                .into_inner()
                .expect("workflow initialization interceptor must call next")
        } else {
            (W::init(view, init_input), base_ctx.initial_headers())
        };
        Ok(Box::new(Self::new_with_workflow_interceptors_and_headers(
            workflow,
            base_ctx,
            run_input,
            headers,
            interceptors,
        )))
    }

    pub fn new_with_workflow(
        workflow: W,
        base_ctx: BaseWorkflowContext,
        run_input: Option<<W::Run as WorkflowDefinition>::Input>,
    ) -> Self {
        Self::new_with_workflow_and_interceptors(workflow, base_ctx, run_input, Vec::new())
    }

    pub fn new_with_workflow_and_interceptors(
        workflow: W,
        base_ctx: BaseWorkflowContext,
        run_input: Option<<W::Run as WorkflowDefinition>::Input>,
        interceptors: Vec<Arc<dyn WorkflowInterceptor>>,
    ) -> Self {
        let headers = base_ctx.initial_headers();
        Self::new_with_workflow_interceptors_and_headers(
            workflow,
            base_ctx,
            run_input,
            headers,
            interceptors,
        )
    }

    fn new_with_workflow_interceptors_and_headers(
        workflow: W,
        base_ctx: BaseWorkflowContext,
        run_input: Option<<W::Run as WorkflowDefinition>::Input>,
        headers: HashMap<String, Payload>,
        interceptors: Vec<Arc<dyn WorkflowInterceptor>>,
    ) -> Self {
        base_ctx.set_workflow_interceptors(interceptors.clone());
        let workflow = Rc::new(RefCell::new(workflow));
        let ctx = WorkflowContext::from_base(base_ctx.clone(), workflow);
        let run_future = intercepted_execute_future::<W>(
            ctx.clone(),
            base_ctx.clone(),
            run_input,
            headers,
            interceptors.clone(),
        )
        .fuse();
        Self {
            base_ctx,
            ctx,
            run_future,
            interceptors,
            main_construction_polled: false,
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

    fn poll_for_construction<F: Future + Unpin>(
        base_ctx: &BaseWorkflowContext,
        future: &mut F,
    ) -> Result<Option<F::Output>, WorkflowFailure> {
        if let Some(failure) = Self::take_forced_wft_failure(base_ctx) {
            return Err(failure);
        }

        let waker = base_ctx.construction_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = {
            let _guard = base_ctx.enter_construction_poll();
            future.poll_unpin(&mut cx)
        };

        if let Some(failure) = Self::take_forced_wft_failure(base_ctx) {
            return Err(failure);
        }

        match poll {
            Poll::Ready(result) => Ok(Some(result)),
            Poll::Pending => Ok(None),
        }
    }

    fn take_forced_wft_failure(base_ctx: &BaseWorkflowContext) -> Option<WorkflowFailure> {
        base_ctx.take_forced_wft_failure().map(|err| {
            Box::new(Failure {
                message: err.to_string(),
                ..Default::default()
            })
        })
    }

    fn start_signal_routine(
        &mut self,
        signal: SignalWorkflow,
    ) -> Result<ActivationJobResult, WorkflowFailure> {
        let name = signal.signal_name;
        let payloads = Payloads {
            payloads: signal.input,
        };
        let converter = self.ctx.payload_converter();
        let future = match W::decode_signal_input(&name, payloads, converter) {
            Ok(Some(input)) => {
                let input = HandleSignalInput::new(name.clone(), input, signal.headers);
                let mut future = intercepted_signal_future::<W>(
                    self.ctx.clone(),
                    self.base_ctx.clone(),
                    self.interceptors.clone(),
                    input,
                );
                if let Some(result) = Self::poll_for_construction(&self.base_ctx, &mut future)? {
                    future = ready(result).boxed_local();
                }
                future
            }
            Err(err) => ready(Err(err)).boxed_local(),
            Ok(None) => return Ok(ActivationJobResult::None),
        };
        let routine_id = self.next_routine_id();
        self.routines
            .insert(routine_id, GuestRoutine::Signal { future });
        Ok(ActivationJobResult::StartedRoutine(StartedRoutine {
            routine_id,
            kind: RoutineKind::Signal(name),
        }))
    }

    fn start_update_routine(
        &mut self,
        update: DoUpdate,
    ) -> Result<ActivationJobResult, WorkflowFailure> {
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
            None => return Ok(self.rejection_for_missing_update_handler(name)),
        };

        if run_validator && has_validator {
            let payloads = Payloads {
                payloads: input.clone(),
            };
            let converter = self.ctx.payload_converter();
            let decoded_input = match W::decode_update_input(&name, payloads, converter) {
                Ok(Some(input)) => input,
                Err(err) => {
                    return Ok(ActivationJobResult::UpdateRejected(Box::new(
                        self.workflow_error_to_failure(err),
                    )));
                }
                Ok(None) => {
                    return Ok(self.rejection_for_missing_update_handler(name));
                }
            };
            let validation_input =
                ValidateUpdateInput::new(id.clone(), name.clone(), decoded_input, headers.clone());
            let validation_ctx = SyncWorkflowInterceptorContext::new(self.base_ctx.clone());
            let workflow_ctx = self.ctx.clone();
            let validation_next = WorkflowNext::new(move |input: ValidateUpdateInput| {
                let (name, input, _headers) = input.into_parts();
                let view = workflow_ctx.view();
                workflow_ctx.state(|wf| wf.validate_update(view, &name, input))
            });
            let validation = call_validate_update(
                &self.interceptors,
                validation_ctx,
                validation_input,
                validation_next,
            );
            match validation {
                Ok(()) => {}
                Err(e) => {
                    return Ok(ActivationJobResult::UpdateRejected(Box::new(
                        self.workflow_error_to_failure(e),
                    )));
                }
            }
        }

        let payloads = Payloads { payloads: input };
        let converter = self.ctx.payload_converter();
        let future = match W::decode_update_input(&name, payloads, converter) {
            Ok(Some(input)) => {
                let input = HandleUpdateInput::new(id.clone(), name.clone(), input, headers);
                let mut future = intercepted_update_future::<W>(
                    self.ctx.clone(),
                    self.base_ctx.clone(),
                    self.interceptors.clone(),
                    input,
                );
                if let Some(result) = Self::poll_for_construction(&self.base_ctx, &mut future)? {
                    future = ready(result).boxed_local();
                }
                future
            }
            Err(err) => ready(Err(err)).boxed_local(),
            Ok(None) => {
                return Ok(self.rejection_for_missing_update_handler(name));
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
        Ok(ActivationJobResult::StartedRoutine(StartedRoutine {
            routine_id,
            kind: RoutineKind::Update(UpdateRoutineKind {
                name,
                update_id: id,
                protocol_instance_id,
            }),
        }))
    }

    fn query(&self, query: QueryWorkflow) -> QueryResponse {
        if query.query_type == "__temporal_workflow_metadata" {
            return self.query_metadata();
        }

        let payloads = Payloads {
            payloads: query.arguments,
        };
        let converter = self.ctx.payload_converter();
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
        let query_input = HandleQueryInput::new(
            query.query_id,
            query.query_type.clone(),
            decoded_input,
            query.headers,
        );
        let interceptor_ctx = SyncWorkflowInterceptorContext::new(self.base_ctx.clone());
        let workflow_ctx = self.ctx.clone();
        let query_next = WorkflowNext::new(move |input: HandleQueryInput| {
            let (name, input, _headers) = input.into_parts();
            let view = workflow_ctx.view();
            workflow_ctx.state(|wf| wf.dispatch_query(view, &name, input))
        });
        QueryResponse {
            result: call_handle_query(&self.interceptors, interceptor_ctx, query_input, query_next)
                .and_then(|output| {
                    serialize_workflow_output(output.as_ref(), converter)
                        .map_err(WorkflowError::from)
                })
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
        result: ExecuteWorkflowResult,
    ) -> crate::runtime::types::TerminalOutcome {
        let result = result.and_then(|result| {
            serialize_workflow_output(result.as_ref(), self.ctx.payload_converter())
                .map_err(WorkflowTermination::from)
        });
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
            if let Some(err) = base_ctx.take_forced_wft_failure() {
                return RoutinePollState::ForcedFailure {
                    failure: Box::new(Failure {
                        message: err.to_string(),
                        ..Default::default()
                    }),
                    made_progress,
                };
            }

            let poll = future.poll_unpin(cx);
            if let Some(err) = base_ctx.take_forced_wft_failure() {
                return RoutinePollState::ForcedFailure {
                    failure: Box::new(Failure {
                        message: err.to_string(),
                        ..Default::default()
                    }),
                    made_progress,
                };
            }

            match poll {
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
        mut future: LocalBoxFuture<'static, HandleSignalResult>,
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
        mut future: LocalBoxFuture<'static, HandleUpdateResult>,
        cx: &mut Context<'_>,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        match Self::poll_routine_loop(&self.base_ctx, cx, &mut future) {
            RoutinePollState::Ready {
                result,
                made_progress,
            } => {
                let completion = match result {
                    Ok(result) => match serialize_workflow_output(
                        result.as_ref(),
                        self.ctx.payload_converter(),
                    )
                    .map_err(WorkflowError::from)
                    {
                        Ok(result) => UpdateRoutineCompletion::Completed {
                            protocol_instance_id,
                            result,
                        },
                        Err(err) => UpdateRoutineCompletion::Rejected {
                            protocol_instance_id,
                            failure: Box::new(self.workflow_error_to_failure(err)),
                        },
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
        let is_replaying_history_events = activation.is_replaying
            && activation
                .jobs
                .iter()
                .any(|job| !matches!(job.variant, Some(ActivationVariant::QueryWorkflow(_))));
        self.base_ctx
            .apply_activation_context(&activation, is_replaying_history_events);
        let mut job_results = Vec::with_capacity(activation.jobs.len());
        for job in activation.jobs {
            let result = match job.variant {
                Some(ActivationVariant::InitializeWorkflow(_)) => {
                    if !self.main_construction_polled {
                        if let Some(result) =
                            Self::poll_for_construction(&self.base_ctx, &mut self.run_future)?
                        {
                            self.run_future = ready(result).boxed_local().fuse();
                        }
                        self.main_construction_polled = true;
                    }
                    ActivationJobResult::None
                }
                Some(ActivationVariant::UpdateRandomSeed(_)) => ActivationJobResult::None,
                Some(ActivationVariant::NotifyHasPatch(patch)) => {
                    self.base_ctx.notify_patch(patch.patch_id);
                    ActivationJobResult::None
                }
                Some(ActivationVariant::CancelWorkflow(cancel)) => {
                    self.base_ctx.notify_cancel(cancel.reason);
                    ActivationJobResult::None
                }
                Some(ActivationVariant::SignalWorkflow(signal)) => {
                    self.start_signal_routine(signal)?
                }
                Some(ActivationVariant::DoUpdate(update)) => self.start_update_routine(update)?,
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

pub fn instantiate_workflow_with_interceptors<W: WorkflowImplementation>(
    payloads: Vec<Payload>,
    converter: PayloadConverter,
    base_ctx: BaseWorkflowContext,
    interceptors: Vec<Arc<dyn WorkflowInterceptor>>,
) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError>
where
    <W::Run as WorkflowDefinition>::Input: Send,
{
    GuestWorkflowInstance::<W>::instantiate_with_interceptors(
        payloads,
        converter,
        base_ctx,
        interceptors,
    )
}

pub fn instantiate_workflow_with_interceptor_factories<W: WorkflowImplementation>(
    payloads: Vec<Payload>,
    converter: PayloadConverter,
    base_ctx: BaseWorkflowContext,
    interceptor_factories: Vec<Arc<dyn WorkflowInterceptorFactory>>,
) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError>
where
    <W::Run as WorkflowDefinition>::Input: Send,
{
    GuestWorkflowInstance::<W>::instantiate_with_interceptor_factories(
        payloads,
        converter,
        base_ctx,
        interceptor_factories,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{runtime::host::WorkflowHost, workflow_interceptors::WorkflowInterceptors};
    use std::{
        rc::Rc,
        sync::atomic::{AtomicUsize, Ordering},
        task::Waker,
    };
    use temporalio_common_wasm::{
        data_converters::{DataConverter, TemporalDeserializable, TemporalSerializable},
        protos::{
            coresdk::{
                workflow_activation::InitializeWorkflow, workflow_commands::WorkflowCommand,
            },
            temporal::api::common::v1::Payload,
        },
    };
    use temporalio_macros::{workflow, workflow_methods};

    struct NoopHost;

    impl WorkflowHost for NoopHost {
        fn set_current_details(&self, _details: String) {}

        fn push_command(&self, _command: WorkflowCommand) {}
    }

    #[workflow]
    #[derive(Default)]
    struct ForcedFailureWorkflow;

    #[workflow_methods]
    impl ForcedFailureWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> crate::WorkflowResult<()> {
            ctx.force_task_fail(std::io::Error::other("forced failure"));
            Ok(())
        }
    }

    struct FailingInput;

    impl TemporalSerializable for FailingInput {}
    impl TemporalDeserializable for FailingInput {}

    #[workflow]
    #[derive(Default)]
    struct DecodeFailureWorkflow;

    #[workflow_methods]
    impl DecodeFailureWorkflow {
        #[run]
        async fn run(
            _ctx: &mut WorkflowContext<Self>,
            _input: FailingInput,
        ) -> crate::WorkflowResult<()> {
            unreachable!("workflow execution must not start when its input cannot be decoded")
        }
    }

    struct CountingExecuteInterceptor {
        calls: Arc<AtomicUsize>,
    }

    impl WorkflowInterceptor for CountingExecuteInterceptor {
        fn execute<'a>(
            &'a self,
            _ctx: WorkflowInterceptorContext,
            input: ExecuteWorkflowInput,
            next: WorkflowNext<
                'a,
                ExecuteWorkflowInput,
                WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
            >,
        ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            next.run(input)
        }
    }

    #[test]
    fn forced_failure_set_during_ready_poll_wins_over_completion() {
        let base_ctx = BaseWorkflowContext::from_raw(
            "default".to_string(),
            "task-queue".to_string(),
            "run-id".to_string(),
            InitializeWorkflow {
                workflow_type: ForcedFailureWorkflow::name().to_string(),
                ..Default::default()
            },
            DataConverter::default(),
            Rc::new(NoopHost),
            None,
        );

        let mut instance = GuestWorkflowInstance::<ForcedFailureWorkflow>::new_with_workflow(
            ForcedFailureWorkflow,
            base_ctx,
            None,
        );

        let result = instance
            .poll_routine(MAIN_ROUTINE_ID, Waker::noop())
            .unwrap();

        let Some(RoutineCompletion::Main(MainRoutineCompletion::TaskFailed(failure))) =
            result.completion
        else {
            panic!("expected a workflow task failure, got {result:?}");
        };
        assert_eq!(failure.failure.message, "forced failure");
    }

    #[test]
    fn interceptor_factories_run_before_workflow_input_decoding() {
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let factory_calls_ref = factory_calls.clone();
        let execute_calls_ref = execute_calls.clone();
        let factory: Arc<dyn WorkflowInterceptorFactory> = Arc::new(move || {
            factory_calls_ref.fetch_add(1, Ordering::Relaxed);
            WorkflowInterceptors::new().with_interceptor(CountingExecuteInterceptor {
                calls: execute_calls_ref.clone(),
            })
        });
        let base_ctx = BaseWorkflowContext::from_raw(
            "default".to_string(),
            "task-queue".to_string(),
            "run-id".to_string(),
            InitializeWorkflow {
                workflow_type: DecodeFailureWorkflow::name().to_string(),
                ..Default::default()
            },
            DataConverter::default(),
            Rc::new(NoopHost),
            None,
        );

        let result =
            GuestWorkflowInstance::<DecodeFailureWorkflow>::instantiate_with_interceptor_factories(
                vec![Payload::default()],
                PayloadConverter::default(),
                base_ctx,
                vec![factory],
            );

        assert!(result.is_err());
        assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
        assert_eq!(execute_calls.load(Ordering::Relaxed), 0);
    }
}
