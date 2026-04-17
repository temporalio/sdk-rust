use anyhow::{Context as AnyhowContext, Error, anyhow, bail};
use std::{
    cell::RefCell,
    future::Future,
    panic,
    panic::AssertUnwindSafe,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
use temporalio_common::{
    data_converters::{
        DataConverter, GenericPayloadConverter, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    protos::{
        coresdk::{
            workflow_activation::{
                FireTimer, InitializeWorkflow, NotifyHasPatch, ResolveActivity,
                ResolveChildWorkflowExecution, ResolveChildWorkflowExecutionStart,
                WorkflowActivation as CoreWorkflowActivation,
                WorkflowActivationJob as CoreWorkflowActivationJob,
                workflow_activation_job::Variant,
            },
            workflow_commands::{
                CancelChildWorkflowExecution, CancelSignalWorkflow, CancelTimer,
                CancelWorkflowExecution, CompleteWorkflowExecution, ContinueAsNewWorkflowExecution,
                FailWorkflowExecution, ModifyWorkflowProperties, QueryResult, QuerySuccess,
                RequestCancelActivity, RequestCancelExternalWorkflowExecution,
                RequestCancelLocalActivity, RequestCancelNexusOperation, ScheduleActivity,
                ScheduleLocalActivity, ScheduleNexusOperation, SetPatchMarker,
                SignalExternalWorkflowExecution, StartChildWorkflowExecution, StartTimer,
                UpdateResponse, UpsertWorkflowSearchAttributes, WorkflowCommand, query_result,
                update_response, workflow_command,
            },
            workflow_completion::{
                self, WorkflowActivationCompletion, workflow_activation_completion,
            },
        },
        temporal::api::{
            common::v1::{Memo, Payload, SearchAttributes},
            enums::v1::{SuggestContinueAsNewReason, VersioningBehavior, WorkflowTaskFailedCause},
            sdk::v1::UserMetadata,
        },
        utilities::TryIntoOrNone,
    },
};
use temporalio_workflow::runtime::{
    BaseWorkflowContext,
    guest::WorkflowInstance,
    host::WorkflowHost,
    model::{WorkflowResult, WorkflowTermination},
    types::{
        ActivationContext, ActivationJobResult, ActivationResult, CancelChildWorkflowRequest,
        IntoNamedPayloads, IntoPayloadMap, MainRoutineCompletion, NamedPayload,
        RequestCancelExternalWorkflowRequest, RequestCancelNexusOperationRequest,
        RoutineCompletion, RoutineId, RoutineKind, RoutinePollResult, ScheduleActivityRequest,
        ScheduleLocalActivityRequest, ScheduleNexusOperationRequest, SignalExternalWorkflowRequest,
        SignalWorkflowTarget, StartChildWorkflowRequest, StartTimerRequest, TerminalOutcome,
        UpdateRoutineCompletion, WorkflowActivation, WorkflowActivationJob, WorkflowResolution,
    },
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::{
    panic_formatter, workflow_executor::WakeTracker, workflow_registry::WorkflowExecutionFactory,
};

pub(crate) struct WorkflowFunction {
    factory: WorkflowExecutionFactory,
}

impl WorkflowFunction {
    /// Create a workflow driver from a registered workflow factory.
    pub(crate) fn from_invocation(factory: WorkflowExecutionFactory) -> Self {
        WorkflowFunction { factory }
    }

    /// Start a workflow function, returning a future that will resolve when the workflow does,
    /// and a channel that can be used to send it activations.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_workflow(
        &self,
        namespace: String,
        task_queue: String,
        run_id: String,
        init_workflow_job: InitializeWorkflow,
        outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
        data_converter: DataConverter,
        detect_nondeterministic: bool,
    ) -> Result<
        (
            impl Future<Output = WorkflowResult<Payload>> + use<>,
            UnboundedSender<CoreWorkflowActivation>,
        ),
        anyhow::Error,
    > {
        let span = info_span!(
            "RunWorkflow",
            "otel.name" = format!("RunWorkflow:{}", &init_workflow_job.workflow_type),
            "otel.kind" = "server"
        );

        let payload_converter = data_converter.payload_converter().clone();
        let input = init_workflow_job.arguments.clone();
        let host = Rc::new(NativeWorkflowHost::default());
        let base_ctx = BaseWorkflowContext::new(
            namespace,
            task_queue,
            run_id,
            init_workflow_job,
            data_converter.clone(),
            host.clone(),
        );

        // Create the workflow execution using the factory
        let execution = (self.factory)(input, payload_converter, base_ctx.clone())
            .context("Failed to create workflow execution")?;

        let wake_tracking = if detect_nondeterministic {
            Some(WakeTracker::new())
        } else {
            None
        };

        let (tx, incoming_activations) = unbounded_channel();
        Ok((
            WorkflowFuture {
                execution,
                host,
                span,
                outgoing_completions,
                incoming_activations,
                wake_tracking,
                active_routines: Vec::new(),
            },
            tx,
        ))
    }
}

#[derive(Default)]
struct NativeWorkflowHost {
    activation_cmds: RefCell<Vec<WorkflowCommand>>,
}

impl NativeWorkflowHost {
    fn push_command(
        &self,
        variant: workflow_command::Variant,
        user_metadata: Option<UserMetadata>,
    ) {
        self.activation_cmds.borrow_mut().push(WorkflowCommand {
            variant: Some(variant),
            user_metadata,
        });
    }

    fn take_commands(&self) -> Vec<WorkflowCommand> {
        std::mem::take(&mut *self.activation_cmds.borrow_mut())
    }
}

impl WorkflowHost for NativeWorkflowHost {
    fn set_current_details(&self, _details: String) {}

    fn start_timer(&self, req: StartTimerRequest) {
        self.push_command(
            workflow_command::Variant::StartTimer(StartTimer {
                seq: req.seq,
                start_to_fire_timeout: Some(
                    req.timeout
                        .try_into()
                        .expect("workflow timer timeout must fit into protobuf duration"),
                ),
            }),
            string_user_metadata(req.summary, None),
        );
    }

    fn cancel_timer(&self, seq: u32) {
        self.push_command(
            workflow_command::Variant::CancelTimer(CancelTimer { seq }),
            None,
        );
    }

    fn schedule_activity(&self, req: ScheduleActivityRequest) {
        self.push_command(
            workflow_command::Variant::ScheduleActivity(ScheduleActivity {
                seq: req.seq,
                activity_type: req.activity_type,
                activity_id: req.activity_id.unwrap_or_else(|| req.seq.to_string()),
                task_queue: req.task_queue.unwrap_or_default(),
                schedule_to_start_timeout: req
                    .schedule_to_start_timeout
                    .and_then(|v| v.try_into().ok()),
                start_to_close_timeout: req.start_to_close_timeout.and_then(|v| v.try_into().ok()),
                schedule_to_close_timeout: req
                    .schedule_to_close_timeout
                    .and_then(|v| v.try_into().ok()),
                heartbeat_timeout: req.heartbeat_timeout.and_then(|v| v.try_into().ok()),
                cancellation_type: req.cancellation_type.into(),
                arguments: req.args,
                retry_policy: req.retry_policy,
                priority: req.priority.map(Into::into),
                do_not_eagerly_execute: req.do_not_eagerly_execute,
                ..Default::default()
            }),
            string_user_metadata(req.summary, None),
        );
    }

    fn cancel_activity(&self, seq: u32) {
        self.push_command(
            workflow_command::Variant::RequestCancelActivity(RequestCancelActivity { seq }),
            None,
        );
    }

    fn schedule_local_activity(&self, req: ScheduleLocalActivityRequest) {
        self.push_command(
            workflow_command::Variant::ScheduleLocalActivity(ScheduleLocalActivity {
                seq: req.seq,
                activity_type: req.activity_type,
                activity_id: req.activity_id.unwrap_or_else(|| req.seq.to_string()),
                arguments: req.args,
                retry_policy: Some(req.retry_policy),
                attempt: req.attempt.unwrap_or(1),
                original_schedule_time: req.original_schedule_time.map(Into::into),
                local_retry_threshold: req.timer_backoff_threshold.and_then(|v| v.try_into().ok()),
                cancellation_type: req.cancellation_type.into(),
                schedule_to_close_timeout: req
                    .schedule_to_close_timeout
                    .and_then(|v| v.try_into().ok()),
                schedule_to_start_timeout: req
                    .schedule_to_start_timeout
                    .and_then(|v| v.try_into().ok()),
                start_to_close_timeout: req.start_to_close_timeout.and_then(|v| v.try_into().ok()),
                ..Default::default()
            }),
            string_user_metadata(req.summary, None),
        );
    }

    fn cancel_local_activity(&self, seq: u32) {
        self.push_command(
            workflow_command::Variant::RequestCancelLocalActivity(RequestCancelLocalActivity {
                seq,
            }),
            None,
        );
    }

    fn start_child_workflow(&self, req: StartChildWorkflowRequest) {
        self.push_command(
            workflow_command::Variant::StartChildWorkflowExecution(StartChildWorkflowExecution {
                seq: req.seq,
                workflow_type: req.workflow_type,
                workflow_id: req.workflow_id,
                task_queue: req.task_queue.unwrap_or_default(),
                input: req.args,
                cancellation_type: req.cancellation_type.into(),
                workflow_id_reuse_policy: req.id_reuse_policy.into(),
                workflow_execution_timeout: req.execution_timeout.and_then(|v| v.try_into().ok()),
                workflow_run_timeout: req.run_timeout.and_then(|v| v.try_into().ok()),
                workflow_task_timeout: req.task_timeout.and_then(|v| v.try_into().ok()),
                search_attributes: (!req.search_attributes.is_empty()).then_some(
                    SearchAttributes {
                        indexed_fields: req.search_attributes.into_payload_map(),
                    },
                ),
                cron_schedule: req.cron_schedule.unwrap_or_default(),
                parent_close_policy: req.parent_close_policy.into(),
                priority: req.priority.map(Into::into),
                ..Default::default()
            }),
            string_user_metadata(req.static_summary, req.static_details),
        );
    }

    fn cancel_child_workflow(&self, req: CancelChildWorkflowRequest) {
        self.push_command(
            workflow_command::Variant::CancelChildWorkflowExecution(CancelChildWorkflowExecution {
                child_workflow_seq: req.seq,
                reason: req.reason.unwrap_or_default(),
            }),
            None,
        );
    }

    fn request_cancel_external_workflow(&self, req: RequestCancelExternalWorkflowRequest) {
        let workflow_execution =
            temporalio_common::protos::coresdk::common::NamespacedWorkflowExecution {
                namespace: req.namespace.unwrap_or_default(),
                workflow_id: req.workflow_id,
                run_id: req.run_id.unwrap_or_default(),
            };
        self.push_command(
            workflow_command::Variant::RequestCancelExternalWorkflowExecution(
                RequestCancelExternalWorkflowExecution {
                    seq: req.seq,
                    workflow_execution: Some(workflow_execution),
                    reason: req.reason.unwrap_or_default(),
                },
            ),
            None,
        );
    }

    fn signal_external_workflow(&self, req: SignalExternalWorkflowRequest) {
        let target = match req.target {
            SignalWorkflowTarget::WorkflowExecution(target) => {
                temporalio_common::protos::coresdk::workflow_commands::signal_external_workflow_execution::Target::WorkflowExecution(
                    temporalio_common::protos::coresdk::common::NamespacedWorkflowExecution {
                        namespace: target.namespace,
                        workflow_id: target.workflow_id,
                        run_id: target.run_id.unwrap_or_default(),
                    },
                )
            }
            SignalWorkflowTarget::ChildWorkflowId(child_workflow_id) => {
                temporalio_common::protos::coresdk::workflow_commands::signal_external_workflow_execution::Target::ChildWorkflowId(
                    child_workflow_id,
                )
            }
        };
        self.push_command(
            workflow_command::Variant::SignalExternalWorkflowExecution(
                SignalExternalWorkflowExecution {
                    seq: req.seq,
                    signal_name: req.signal.name,
                    args: req.signal.args,
                    target: Some(target),
                    headers: req.signal.headers.into_payload_map(),
                },
            ),
            None,
        );
    }

    fn cancel_signal_external_workflow(&self, seq: u32) {
        self.push_command(
            workflow_command::Variant::CancelSignalWorkflow(CancelSignalWorkflow { seq }),
            None,
        );
    }

    fn schedule_nexus_operation(&self, req: ScheduleNexusOperationRequest) {
        self.push_command(
            workflow_command::Variant::ScheduleNexusOperation(ScheduleNexusOperation {
                seq: req.seq,
                endpoint: req.endpoint,
                service: req.service,
                operation: req.operation,
                input: req.input,
                schedule_to_close_timeout: req
                    .schedule_to_close_timeout
                    .and_then(|v| v.try_into().ok()),
                schedule_to_start_timeout: req
                    .schedule_to_start_timeout
                    .and_then(|v| v.try_into().ok()),
                start_to_close_timeout: req.start_to_close_timeout.and_then(|v| v.try_into().ok()),
                nexus_header: req
                    .headers
                    .into_iter()
                    .map(|header| (header.key, header.value))
                    .collect(),
                cancellation_type: req.cancellation_type.into(),
            }),
            None,
        );
    }

    fn cancel_nexus_operation(&self, req: RequestCancelNexusOperationRequest) {
        self.push_command(
            workflow_command::Variant::RequestCancelNexusOperation(RequestCancelNexusOperation {
                seq: req.seq,
            }),
            None,
        );
    }

    fn upsert_search_attributes(&self, entries: Vec<NamedPayload>) {
        self.push_command(
            workflow_command::Variant::UpsertWorkflowSearchAttributes(
                UpsertWorkflowSearchAttributes {
                    search_attributes: Some(SearchAttributes {
                        indexed_fields: entries.into_payload_map(),
                    }),
                },
            ),
            None,
        );
    }

    fn upsert_memo(&self, entries: Vec<NamedPayload>) {
        self.push_command(
            workflow_command::Variant::ModifyWorkflowProperties(ModifyWorkflowProperties {
                upserted_memo: Some(Memo {
                    fields: entries.into_payload_map(),
                }),
            }),
            None,
        );
    }

    fn set_patch_marker(&self, patch_id: String, deprecated: bool) {
        self.push_command(
            workflow_command::Variant::SetPatchMarker(SetPatchMarker {
                patch_id,
                deprecated,
            }),
            None,
        );
    }

    fn continue_as_new(&self, req: temporalio_workflow::runtime::types::ContinueAsNewRequest) {
        self.push_command(
            workflow_command::Variant::ContinueAsNewWorkflowExecution(
                ContinueAsNewWorkflowExecution {
                    workflow_type: req.workflow_type.unwrap_or_default(),
                    task_queue: req.task_queue.unwrap_or_default(),
                    arguments: req.args,
                    workflow_run_timeout: req.run_timeout.and_then(|v| v.try_into().ok()),
                    workflow_task_timeout: req.task_timeout.and_then(|v| v.try_into().ok()),
                    memo: req.memo.into_payload_map(),
                    headers: req.headers.into_payload_map(),
                    search_attributes: req.search_attributes.map(|entries| SearchAttributes {
                        indexed_fields: entries.into_payload_map(),
                    }),
                    retry_policy: req.retry_policy,
                    versioning_intent: req.versioning_intent.unwrap_or_default().into(),
                    initial_versioning_behavior: req
                        .initial_versioning_behavior
                        .unwrap_or_default()
                        .into(),
                },
            ),
            None,
        );
    }
}

pub(crate) struct WorkflowFuture {
    /// The workflow execution instance
    execution: Box<dyn WorkflowInstance>,
    /// Native host adapter collecting commands emitted by guest workflow code.
    host: Rc<NativeWorkflowHost>,
    /// The tracing span for this workflow
    span: tracing::Span,
    /// Once blocked or the workflow has finished or errored out, the result is sent here
    outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
    /// Activations from core
    incoming_activations: UnboundedReceiver<CoreWorkflowActivation>,
    /// Nondeterminism detection tracker. When present, a tracking waker is used
    /// for polling user workflow code, and any non-SDK wake is flagged.
    wake_tracking: Option<WakeTracker>,
    /// Signal and update routines that are still live across activations.
    active_routines: Vec<RoutineId>,
}

#[derive(Debug)]
enum ActivationJobContext {
    Passive,
    Signal,
    Update { protocol_instance_id: String },
    Query { query_id: String },
}

impl WorkflowFuture {
    fn fail_wft(&self, run_id: String, fail: Error, cause: Option<WorkflowTaskFailedCause>) {
        warn!("Workflow task failed for {}: {}", run_id, fail);
        self.outgoing_completions
            .send(WorkflowActivationCompletion::fail(
                run_id,
                fail.into(),
                cause,
            ))
            .expect("Completion channel intact");
    }

    fn send_completion(&self, run_id: String, activation_cmds: Vec<WorkflowCommand>) {
        self.outgoing_completions
            .send(WorkflowActivationCompletion {
                run_id,
                status: Some(workflow_activation_completion::Status::Successful(
                    workflow_completion::Success {
                        commands: activation_cmds,
                        used_internal_flags: vec![],
                        versioning_behavior: VersioningBehavior::Unspecified.into(),
                    },
                )),
            })
            .expect("Completion channel intact");
    }

    fn translate_activation(
        &self,
        activation: CoreWorkflowActivation,
    ) -> Result<(WorkflowActivation, Vec<ActivationJobContext>, bool), Error> {
        let context = activation_context_from_activation(&activation);
        let mut should_poll_routines = false;
        let mut jobs = Vec::with_capacity(activation.jobs.len());
        let mut job_contexts = Vec::with_capacity(activation.jobs.len());
        macro_rules! push_job {
            ($job:expr, $context:expr $(,)?) => {{
                jobs.push($job);
                job_contexts.push($context);
            }};
        }
        macro_rules! push_polled_job {
            ($job:expr, $context:expr $(,)?) => {{
                should_poll_routines = true;
                push_job!($job, $context);
            }};
        }
        macro_rules! push_resolution {
            ($resolution:expr $(,)?) => {
                push_polled_job!(
                    WorkflowActivationJob::Resolution($resolution),
                    ActivationJobContext::Passive
                )
            };
        }
        for CoreWorkflowActivationJob { variant } in activation.jobs {
            match variant.context("Empty activation job variant")? {
                Variant::InitializeWorkflow(_) => {
                    should_poll_routines = true;
                }
                Variant::FireTimer(FireTimer { seq }) => {
                    push_resolution!(WorkflowResolution::TimerFired(
                        temporalio_workflow::runtime::types::TimerFiredEvent { seq },
                    ));
                }
                Variant::ResolveActivity(ResolveActivity { seq, result, .. }) => {
                    push_resolution!(WorkflowResolution::Activity(
                        temporalio_workflow::runtime::types::ActivityResolutionEvent {
                            seq,
                            result: result.context("Activity must have result")?,
                        },
                    ));
                }
                Variant::ResolveChildWorkflowExecutionStart(
                    ResolveChildWorkflowExecutionStart { seq, status },
                ) => {
                    push_resolution!(WorkflowResolution::ChildWorkflowStart(
                        temporalio_workflow::runtime::types::ChildWorkflowStartResolutionEvent {
                            seq,
                            status: status.context("Workflow start must have status")?,
                        },
                    ));
                }
                Variant::ResolveChildWorkflowExecution(ResolveChildWorkflowExecution {
                    seq,
                    result,
                }) => {
                    push_resolution!(WorkflowResolution::ChildWorkflow(
                        temporalio_workflow::runtime::types::ChildWorkflowResolutionEvent {
                            seq,
                            result: result
                                .context("Child Workflow execution must have a result")?,
                        },
                    ));
                }
                Variant::UpdateRandomSeed(_) => {
                    should_poll_routines = true;
                }
                Variant::QueryWorkflow(q) => {
                    debug!(query_type = %q.query_type, "Query received");
                    push_job!(
                        WorkflowActivationJob::Query(
                            temporalio_workflow::runtime::types::QueryInvocation {
                                name: q.query_type,
                                args: q.arguments,
                                headers: q.headers.into_named_payloads(),
                            },
                        ),
                        ActivationJobContext::Query {
                            query_id: q.query_id,
                        },
                    );
                }
                Variant::CancelWorkflow(c) => {
                    push_polled_job!(
                        WorkflowActivationJob::Cancel { reason: c.reason },
                        ActivationJobContext::Passive,
                    );
                }
                Variant::SignalWorkflow(sig) => {
                    debug!(signal_name = %sig.signal_name, "Signal received");
                    push_polled_job!(
                        WorkflowActivationJob::Signal(
                            temporalio_workflow::runtime::types::SignalInvocation {
                                name: sig.signal_name,
                                args: sig.input,
                                headers: sig.headers.into_named_payloads(),
                            },
                        ),
                        ActivationJobContext::Signal,
                    );
                }
                Variant::NotifyHasPatch(NotifyHasPatch { patch_id }) => {
                    push_polled_job!(
                        WorkflowActivationJob::NotifyPatch { patch_id },
                        ActivationJobContext::Passive,
                    );
                }
                Variant::ResolveSignalExternalWorkflow(attrs) => {
                    push_resolution!(WorkflowResolution::ExternalSignal(
                        temporalio_workflow::runtime::types::ExternalSignalResolutionEvent {
                            seq: attrs.seq,
                            failure: attrs.failure,
                        },
                    ));
                }
                Variant::ResolveRequestCancelExternalWorkflow(attrs) => {
                    push_resolution!(WorkflowResolution::ExternalCancel(
                        temporalio_workflow::runtime::types::ExternalCancelResolutionEvent {
                            seq: attrs.seq,
                            failure: attrs.failure,
                        },
                    ));
                }
                Variant::DoUpdate(u) => {
                    let protocol_instance_id = u.protocol_instance_id;
                    push_polled_job!(
                        WorkflowActivationJob::Update(
                            temporalio_workflow::runtime::types::UpdateInvocation {
                                update_id: u.id,
                                protocol_instance_id: protocol_instance_id.clone(),
                                name: u.name,
                                args: u.input,
                                headers: u.headers.into_named_payloads(),
                                run_validator: u.run_validator,
                            },
                        ),
                        ActivationJobContext::Update {
                            protocol_instance_id,
                        },
                    );
                }
                Variant::ResolveNexusOperationStart(attrs) => {
                    push_resolution!(WorkflowResolution::NexusStart(
                        temporalio_workflow::runtime::types::NexusStartResolutionEvent {
                            seq: attrs.seq,
                            status: attrs
                                .status
                                .context("Nexus operation start must have status")?,
                        },
                    ));
                }
                Variant::ResolveNexusOperation(attrs) => {
                    push_resolution!(WorkflowResolution::Nexus(
                        temporalio_workflow::runtime::types::NexusResolutionEvent {
                            seq: attrs.seq,
                            result: attrs.result.context("Nexus operation must have result")?,
                        },
                    ));
                }
                Variant::RemoveFromCache(_) => {
                    unreachable!("Cache removal should happen higher up");
                }
            }
        }

        Ok((
            WorkflowActivation { context, jobs },
            job_contexts,
            should_poll_routines,
        ))
    }

    fn process_activation_results(
        &mut self,
        job_contexts: Vec<ActivationJobContext>,
        activation_result: ActivationResult,
        outgoing_cmds: &mut Vec<WorkflowCommand>,
    ) -> Result<(), Error> {
        if job_contexts.len() != activation_result.job_results.len() {
            bail!(
                "Activation result count {} did not match job count {}",
                activation_result.job_results.len(),
                job_contexts.len()
            );
        }

        for (job_context, job_result) in job_contexts.into_iter().zip(activation_result.job_results)
        {
            match (job_context, job_result) {
                (ActivationJobContext::Passive, ActivationJobResult::None)
                | (ActivationJobContext::Signal, ActivationJobResult::None) => {}
                (
                    ActivationJobContext::Signal,
                    ActivationJobResult::StartedRoutine(started_routine),
                ) => match started_routine.kind {
                    RoutineKind::Signal { .. } => {
                        self.active_routines.push(started_routine.routine_id)
                    }
                    other => bail!("Signal job started unexpected routine kind {other:?}"),
                },
                (
                    ActivationJobContext::Update {
                        protocol_instance_id,
                    },
                    ActivationJobResult::StartedRoutine(started_routine),
                ) => match started_routine.kind {
                    RoutineKind::Update {
                        protocol_instance_id: started_id,
                        ..
                    } => {
                        if started_id != protocol_instance_id {
                            bail!(
                                "Update routine protocol instance id {} did not match {}",
                                started_id,
                                protocol_instance_id
                            );
                        }
                        outgoing_cmds.push(
                            update_response(
                                protocol_instance_id,
                                update_response::Response::Accepted(()),
                            )
                            .into(),
                        );
                        self.active_routines.push(started_routine.routine_id);
                    }
                    other => bail!("Update job started unexpected routine kind {other:?}"),
                },
                (
                    ActivationJobContext::Update {
                        protocol_instance_id,
                    },
                    ActivationJobResult::UpdateRejected(failure),
                ) => {
                    outgoing_cmds.push(
                        update_response(
                            protocol_instance_id,
                            update_response::Response::Rejected(*failure),
                        )
                        .into(),
                    );
                }
                (
                    ActivationJobContext::Query { query_id },
                    ActivationJobResult::QueryResponse(query_response),
                ) => outgoing_cmds.push(query_response_command(query_id, *query_response)),
                (job_context, job_result) => {
                    bail!("Unexpected activation result {job_result:?} for job {job_context:?}");
                }
            }
        }

        Ok(())
    }

    fn poll_guest_routine(
        &mut self,
        routine_id: RoutineId,
        cx: &Context<'_>,
    ) -> Result<RoutinePollResult, Error> {
        let span = self.span.clone();
        let _guard = span.enter();
        if let Some(ref tracker) = self.wake_tracking {
            let waker = tracker.new_per_poll_waker(cx.waker());
            match panic::catch_unwind(AssertUnwindSafe(|| {
                self.execution.poll_routine(routine_id, &waker)
            })) {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(err)) => Err(anyhow!(err.message)),
                Err(e) => bail!("Workflow function panicked: {}", panic_formatter(e)),
            }
        } else {
            match panic::catch_unwind(AssertUnwindSafe(|| {
                self.execution.poll_routine(routine_id, cx.waker())
            })) {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(err)) => Err(anyhow!(err.message)),
                Err(e) => bail!("Workflow function panicked: {}", panic_formatter(e)),
            }
        }
    }
}

impl Future for WorkflowFuture {
    type Output = WorkflowResult<Payload>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        'activations: loop {
            let activation = match self.incoming_activations.poll_recv(cx) {
                Poll::Ready(a) => match a {
                    Some(act) => act,
                    None => {
                        return Poll::Ready(Err(anyhow!(
                            "Workflow future's activation channel was lost!"
                        )
                        .into()));
                    }
                },
                Poll::Pending => return Poll::Pending,
            };

            let is_only_eviction = activation.is_only_eviction();
            let run_id = activation.run_id.clone();

            let mut activation_cmds = vec![];

            if is_only_eviction {
                self.outgoing_completions
                    .send(WorkflowActivationCompletion::from_cmds(run_id, vec![]))
                    .expect("Completion channel intact");
                return Err(WorkflowTermination::Evicted).into();
            }

            if self
                .wake_tracking
                .as_mut()
                .is_some_and(|t| t.take_non_sdk_wake())
            {
                self.fail_wft(
                    run_id,
                    anyhow!(
                        "[TMPRL1100] Nondeterministic future detected: a waker was invoked by a \
                         non-SDK source. This usually means workflow code is using nondeterministic \
                         operations like tokio async functions or channels, other async libraries, \
                         or std::thread. Use SDK-provided alternatives \
                         (ctx.timer(), ctx.state_mut() + ctx.wait_condition(), etc.) instead."
                    ),
                    Some(WorkflowTaskFailedCause::NonDeterministicError),
                );
                continue 'activations;
            }

            let (guest_activation, job_contexts, should_poll_routines) =
                match self.translate_activation(activation) {
                    Ok(translated) => translated,
                    Err(e) => {
                        self.fail_wft(run_id, e, None);
                        continue 'activations;
                    }
                };

            let activation_result = match panic::catch_unwind(AssertUnwindSafe(|| {
                self.execution.activate(guest_activation)
            })) {
                Ok(Ok(result)) => result,
                Ok(Err(err)) => {
                    self.fail_wft(run_id.clone(), anyhow!(err.message), None);
                    continue 'activations;
                }
                Err(e) => {
                    self.fail_wft(
                        run_id.clone(),
                        anyhow!("Workflow function panicked: {}", panic_formatter(e)),
                        None,
                    );
                    continue 'activations;
                }
            };

            if let Err(e) = self.process_activation_results(
                job_contexts,
                activation_result,
                &mut activation_cmds,
            ) {
                self.fail_wft(run_id.clone(), e, None);
                continue 'activations;
            }

            if should_poll_routines {
                loop {
                    let mut pass_made_progress = false;
                    let mut should_stop_polling = false;
                    let mut still_active = Vec::with_capacity(self.active_routines.len());
                    for routine_id in std::mem::take(&mut self.active_routines) {
                        let poll_result = match self.poll_guest_routine(routine_id, cx) {
                            Ok(result) => result,
                            Err(e) => {
                                self.fail_wft(run_id.clone(), e, None);
                                continue 'activations;
                            }
                        };
                        pass_made_progress |= poll_result.made_progress;
                        match poll_result.completion {
                            None => still_active.push(routine_id),
                            Some(result) => match *result {
                                RoutineCompletion::Signal(Ok(())) => {}
                                RoutineCompletion::Signal(Err(failure)) => {
                                    self.fail_wft(run_id.clone(), anyhow!(failure.message), None);
                                    continue 'activations;
                                }
                                RoutineCompletion::Update(UpdateRoutineCompletion::Completed {
                                    protocol_instance_id,
                                    result,
                                }) => activation_cmds.push(
                                    update_response(
                                        protocol_instance_id,
                                        update_response::Response::Completed(result),
                                    )
                                    .into(),
                                ),
                                RoutineCompletion::Update(UpdateRoutineCompletion::Rejected {
                                    protocol_instance_id,
                                    failure,
                                }) => activation_cmds.push(
                                    update_response(
                                        protocol_instance_id,
                                        update_response::Response::Rejected(*failure),
                                    )
                                    .into(),
                                ),
                                RoutineCompletion::Main(_) => {
                                    self.fail_wft(
                                        run_id.clone(),
                                        anyhow!("non-main routine returned a main completion"),
                                        None,
                                    );
                                    continue 'activations;
                                }
                            },
                        }
                    }
                    self.active_routines = still_active;

                    let main_poll_result = match self.poll_guest_routine(
                        temporalio_workflow::runtime::types::MAIN_ROUTINE_ID,
                        cx,
                    ) {
                        Ok(result) => result,
                        Err(e) => {
                            self.fail_wft(run_id.clone(), e, None);
                            continue 'activations;
                        }
                    };
                    pass_made_progress |= main_poll_result.made_progress;

                    match main_poll_result.completion {
                        None => {
                            self.fail_wft(
                                run_id.clone(),
                                anyhow!("main routine returned no completion"),
                                None,
                            );
                            continue 'activations;
                        }
                        Some(result) => match *result {
                            RoutineCompletion::Main(MainRoutineCompletion::Blocked) => {}
                            RoutineCompletion::Main(MainRoutineCompletion::TaskFailed(
                                task_failure,
                            )) => {
                                self.fail_wft(
                                    run_id.clone(),
                                    anyhow!(task_failure.failure.message),
                                    None,
                                );
                                continue 'activations;
                            }
                            RoutineCompletion::Main(MainRoutineCompletion::Terminal(outcome)) => {
                                emit_terminal_outcome(&self.host, *outcome);
                                should_stop_polling = true;
                            }
                            other => {
                                self.fail_wft(
                                    run_id.clone(),
                                    anyhow!(
                                        "main routine returned unexpected completion {other:?}"
                                    ),
                                    None,
                                );
                                continue 'activations;
                            }
                        },
                    }

                    if should_stop_polling || !pass_made_progress {
                        break;
                    }
                }
            }

            activation_cmds.extend(self.host.take_commands());
            self.send_completion(run_id, activation_cmds);
        }
    }
}

fn update_response(
    instance_id: String,
    resp: update_response::Response,
) -> workflow_command::Variant {
    UpdateResponse {
        protocol_instance_id: instance_id,
        response: Some(resp),
    }
    .into()
}

fn query_response_command(
    query_id: String,
    response: temporalio_workflow::runtime::types::QueryResponse,
) -> WorkflowCommand {
    workflow_command::Variant::RespondToQuery(QueryResult {
        query_id,
        variant: Some(match response.result {
            Ok(payload) => query_result::Variant::Succeeded(QuerySuccess {
                response: Some(payload),
            }),
            Err(failure) => query_result::Variant::Failed(failure),
        }),
    })
    .into()
}

fn string_user_metadata(summary: Option<String>, details: Option<String>) -> Option<UserMetadata> {
    if summary.is_none() && details.is_none() {
        return None;
    }
    let converter = PayloadConverter::default();
    let context = SerializationContext {
        data: &SerializationContextData::Workflow,
        converter: &converter,
    };
    Some(UserMetadata {
        summary: summary.map(|value| {
            converter
                .to_payload(&context, &value)
                .expect("String-to-JSON payload serialization is infallible")
        }),
        details: details.map(|value| {
            converter
                .to_payload(&context, &value)
                .expect("String-to-JSON payload serialization is infallible")
        }),
    })
}

fn emit_terminal_outcome(host: &NativeWorkflowHost, outcome: TerminalOutcome) {
    match outcome {
        TerminalOutcome::Completed(result) => {
            host.push_command(
                workflow_command::Variant::CompleteWorkflowExecution(CompleteWorkflowExecution {
                    result: Some(result),
                }),
                None,
            );
        }
        TerminalOutcome::Failed(failure) => {
            host.push_command(
                workflow_command::Variant::FailWorkflowExecution(FailWorkflowExecution {
                    failure: Some(*failure),
                }),
                None,
            );
        }
        TerminalOutcome::Cancelled => {
            host.push_command(
                workflow_command::Variant::CancelWorkflowExecution(CancelWorkflowExecution {}),
                None,
            );
        }
        TerminalOutcome::ContinueAsNew(req) => host.continue_as_new(req),
    }
}

fn activation_context_from_activation(activation: &CoreWorkflowActivation) -> ActivationContext {
    let updated_randomness_seed = activation.jobs.iter().find_map(|job| match &job.variant {
        Some(Variant::UpdateRandomSeed(attrs)) => Some(attrs.randomness_seed),
        _ => None,
    });
    ActivationContext {
        workflow_time: activation.timestamp.try_into_or_none(),
        is_replaying: activation.is_replaying,
        history_length: activation.history_length,
        history_size_bytes: activation.history_size_bytes,
        continue_as_new_suggested: activation.continue_as_new_suggested,
        current_deployment_version: activation
            .deployment_version_for_current_task
            .clone()
            .map(Into::into),
        last_sdk_version: (!activation.last_sdk_version.is_empty())
            .then_some(activation.last_sdk_version.clone()),
        available_internal_flags: activation.available_internal_flags.clone(),
        updated_randomness_seed,
        target_worker_deployment_version_changed: activation
            .target_worker_deployment_version_changed,
        suggest_continue_as_new_reasons: activation
            .suggest_continue_as_new_reasons
            .iter()
            .filter_map(|v| SuggestContinueAsNewReason::try_from(*v).ok())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::coresdk::workflow_activation::{
        UpdateRandomSeed, WorkflowActivationJob,
    };

    #[test]
    fn activation_context_preserves_updated_random_seed() {
        let activation = CoreWorkflowActivation {
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::UpdateRandomSeed(UpdateRandomSeed {
                    randomness_seed: 1234,
                })),
            }],
            ..Default::default()
        };

        let context = activation_context_from_activation(&activation);

        assert_eq!(context.updated_randomness_seed, Some(1234));
    }
}
