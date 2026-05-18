use crate::{
    BaseWorkflowContext, CancellableID, OutgoingError, OutgoingWorkflowError, RustWfCmd,
    TimerResult, UnblockEvent, WorkflowResult, WorkflowTermination, panic_formatter,
    workflow_executor::{SdkWakeGuard, WakeTracker},
    workflows::{DispatchData, DynWorkflowExecution, WorkflowExecutionFactory},
};
use anyhow::{Context as AnyhowContext, Error, anyhow, bail};
use futures_util::{FutureExt, future::LocalBoxFuture};
use std::{
    collections::HashMap,
    future::Future,
    panic,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::mpsc::Receiver,
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
                WorkflowActivation, WorkflowActivationJob, workflow_activation_job::Variant,
            },
            workflow_commands::{
                CancelChildWorkflowExecution, CancelSignalWorkflow, CancelTimer,
                CancelWorkflowExecution, CompleteWorkflowExecution, FailWorkflowExecution,
                QueryResult, QuerySuccess, RequestCancelActivity, RequestCancelLocalActivity,
                RequestCancelNexusOperation, ScheduleActivity, ScheduleLocalActivity, StartTimer,
                UpdateResponse, WorkflowCommand, query_result, update_response, workflow_command,
            },
            workflow_completion::{
                self, WorkflowActivationCompletion, workflow_activation_completion,
            },
        },
        temporal::api::{
            common::v1::{Payload, Payloads},
            enums::v1::{VersioningBehavior, WorkflowTaskFailedCause},
            failure::v1::Failure,
        },
        utilities::TryIntoOrNone,
    },
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot, watch,
};

pub(crate) struct WorkflowFunction {
    factory: WorkflowExecutionFactory,
}

impl WorkflowFunction {
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
            UnboundedSender<WorkflowActivation>,
        ),
        anyhow::Error,
    > {
        let (cancel_tx, cancel_rx) = watch::channel(None);
        let span = info_span!(
            "RunWorkflow",
            "otel.name" = format!("RunWorkflow:{}", &init_workflow_job.workflow_type),
            "otel.kind" = "server"
        );

        let payload_converter = data_converter.payload_converter().clone();
        let input = init_workflow_job.arguments.clone();
        let (base_ctx, cmd_receiver) = BaseWorkflowContext::new(
            namespace,
            task_queue,
            run_id,
            init_workflow_job,
            cancel_rx,
            data_converter.clone(),
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
                base_ctx,
                execution,
                span,
                incoming_commands: cmd_receiver,
                outgoing_completions,
                incoming_activations,
                command_status: Default::default(),
                cancel_sender: cancel_tx,
                data_converter,
                update_futures: Default::default(),
                signal_futures: Default::default(),
                wake_tracking,
            },
            tx,
        ))
    }
}

struct WFCommandFutInfo {
    unblocker: oneshot::Sender<UnblockEvent>,
}

pub(crate) struct WorkflowFuture {
    /// The workflow execution instance
    execution: Box<dyn DynWorkflowExecution>,
    /// The tracing span for this workflow
    span: tracing::Span,
    /// Commands produced inside user's wf code
    incoming_commands: Receiver<RustWfCmd>,
    /// Once blocked or the workflow has finished or errored out, the result is sent here
    outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
    /// Activations from core
    incoming_activations: UnboundedReceiver<WorkflowActivation>,
    /// Commands by ID -> blocked status
    command_status: HashMap<CommandID, WFCommandFutInfo>,
    /// Use to notify workflow code of cancellation
    cancel_sender: watch::Sender<Option<String>>,
    /// Base workflow context for sending commands
    base_ctx: BaseWorkflowContext,
    /// Data converter for workflow failure conversion and payload serialization.
    data_converter: DataConverter,
    /// Stores in-progress update futures
    update_futures: Vec<(
        String,
        LocalBoxFuture<'static, Result<Payload, crate::workflows::WorkflowError>>,
    )>,
    /// Stores in-progress signal futures
    signal_futures: Vec<LocalBoxFuture<'static, Result<(), crate::workflows::WorkflowError>>>,
    /// Nondeterminism detection tracker. When present, a tracking waker is used
    /// for polling user workflow code, and any non-SDK wake is flagged.
    wake_tracking: Option<WakeTracker>,
}

impl WorkflowFuture {
    fn workflow_message_to_failure(&self, message: String) -> Failure {
        self.workflow_error_to_failure(anyhow!(message).into())
    }

    fn workflow_error_to_failure(&self, error: crate::workflows::WorkflowError) -> Failure {
        let outgoing = match error {
            crate::workflows::WorkflowError::PayloadConversion(err) => {
                OutgoingWorkflowError::from(err)
            }
            crate::workflows::WorkflowError::Execution(err) => err.into(),
        };
        self.data_converter.to_failure(
            &SerializationContextData::Workflow,
            OutgoingError::Workflow(outgoing),
        )
    }

    fn unblock(&mut self, event: UnblockEvent) -> Result<(), Error> {
        let cmd_id = CommandID::from(&event);
        self.maybe_unblock(event)
            .then_some(())
            .ok_or_else(|| anyhow!("Command {cmd_id:?} not found to unblock!"))
    }

    fn maybe_unblock(&mut self, event: UnblockEvent) -> bool {
        let cmd_id = CommandID::from(&event);
        let Some(unblocker) = self.command_status.remove(&cmd_id) else {
            return false;
        };
        let _guard = SdkWakeGuard::new();
        let _ = unblocker.unblocker.send(event);
        true
    }

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

    /// Handle a particular workflow activation job.
    ///
    /// Returns `Ok(should_poll_wf)` where `should_poll_wf` indicates whether the workflow future
    /// should be polled after this job. Query jobs return false since an activation containing
    /// _only_ queries must not advance the workflow.
    ///
    /// Returns an error in the event that the workflow task should be failed.
    ///
    /// Panics if internal assumptions are violated
    fn handle_job(
        &mut self,
        variant: Option<Variant>,
        outgoing_cmds: &mut Vec<WorkflowCommand>,
    ) -> Result<bool, Error> {
        if let Some(v) = variant {
            match v {
                Variant::InitializeWorkflow(_) => {
                    // Don't do anything in here. Init workflow is looked at earlier, before
                    // jobs are handled, and may have information taken out of it to avoid clones.
                }
                Variant::FireTimer(FireTimer { seq }) => {
                    self.unblock(UnblockEvent::Timer(seq, TimerResult::Fired))?
                }
                Variant::ResolveActivity(ResolveActivity { seq, result, .. }) => {
                    self.unblock(UnblockEvent::Activity(
                        seq,
                        Box::new(result.context("Activity must have result")?),
                    ))?;
                }
                Variant::ResolveChildWorkflowExecutionStart(
                    ResolveChildWorkflowExecutionStart { seq, status },
                ) => self.unblock(UnblockEvent::WorkflowStart(
                    seq,
                    Box::new(status.context("Workflow start must have status")?),
                ))?,
                Variant::ResolveChildWorkflowExecution(ResolveChildWorkflowExecution {
                    seq,
                    result,
                }) => self.unblock(UnblockEvent::WorkflowComplete(
                    seq,
                    Box::new(result.context("Child Workflow execution must have a result")?),
                ))?,
                Variant::UpdateRandomSeed(rs) => {
                    self.base_ctx.shared_mut().random_seed = rs.randomness_seed;
                }
                Variant::QueryWorkflow(q) => {
                    debug!(query_type = %q.query_type, "Query received");
                    let query_type = q.query_type;
                    let query_id = q.query_id;
                    let data = DispatchData {
                        payloads: Payloads {
                            payloads: q.arguments,
                        },
                        headers: q.headers,
                        converter: self.data_converter.payload_converter(),
                    };

                    let dispatch_result = if query_type == "__temporal_workflow_metadata" {
                        // Mirror the proto JSON shape of temporal.api.sdk.v1.WorkflowMetadata.
                        // TODO [rust-sdk-branch]: support normal JSON and proto JSON serialization, and this will no longer be necessary.
                        #[derive(serde::Serialize)]
                        struct WorkflowMetadataJson {
                            #[serde(
                                rename = "currentDetails",
                                skip_serializing_if = "String::is_empty"
                            )]
                            current_details: String,
                        }
                        let converter = PayloadConverter::default();
                        let ctx = SerializationContext {
                            data: &SerializationContextData::Workflow,
                            converter: &converter,
                        };
                        let payload = converter.to_payload(
                            &ctx,
                            &WorkflowMetadataJson {
                                current_details: self.base_ctx.current_details(),
                            },
                        );
                        Some(Ok(payload?))
                    } else {
                        match panic::catch_unwind(AssertUnwindSafe(|| {
                            self.execution.dispatch_query(&query_type, data)
                        })) {
                            Ok(r) => r,
                            Err(e) => Some(Err(anyhow!(
                                "Panic in query handler: {}",
                                panic_formatter(e)
                            )
                            .into())),
                        }
                    };

                    let response = match dispatch_result {
                        Some(Ok(payload)) => query_result::Variant::Succeeded(QuerySuccess {
                            response: Some(payload),
                        }),
                        // TODO [rust-sdk-branch]: Return list of known queries in error
                        None => query_result::Variant::Failed(self.workflow_message_to_failure(
                            format!("No query handler for '{}'", query_type),
                        )),
                        Some(Err(e)) => {
                            query_result::Variant::Failed(self.workflow_error_to_failure(e))
                        }
                    };

                    outgoing_cmds.push(
                        workflow_command::Variant::RespondToQuery(QueryResult {
                            query_id,
                            variant: Some(response),
                        })
                        .into(),
                    );
                    return Ok(false);
                }
                Variant::CancelWorkflow(c) => {
                    // TODO: Cancel pending futures, etc
                    let _guard = SdkWakeGuard::new();
                    self.cancel_sender
                        .send(Some(c.reason))
                        .expect("Cancel rx not dropped");
                }
                Variant::SignalWorkflow(sig) => {
                    debug!(signal_name = %sig.signal_name, "Signal received");
                    let data = DispatchData {
                        payloads: Payloads {
                            payloads: sig.input,
                        },
                        headers: sig.headers,
                        converter: self.data_converter.payload_converter(),
                    };

                    let dispatch_result = match panic::catch_unwind(AssertUnwindSafe(|| {
                        self.execution.dispatch_signal(&sig.signal_name, data)
                    })) {
                        Ok(r) => r,
                        Err(e) => {
                            bail!("Panic in signal handler: {}", panic_formatter(e));
                        }
                    };

                    if let Some(fut) = dispatch_result {
                        self.signal_futures.push(fut);
                    }
                    // TODO [rust-sdk-branch]: Buffer signals w/ no handler
                }
                Variant::NotifyHasPatch(NotifyHasPatch { patch_id }) => {
                    self.base_ctx.shared_mut().changes.insert(patch_id, true);
                }
                Variant::ResolveSignalExternalWorkflow(attrs) => {
                    self.unblock(UnblockEvent::SignalExternal(attrs.seq, attrs.failure))?;
                }
                Variant::ResolveRequestCancelExternalWorkflow(attrs) => {
                    self.unblock(UnblockEvent::CancelExternal(attrs.seq, attrs.failure))?;
                }
                Variant::DoUpdate(u) => {
                    let protocol_instance_id = u.protocol_instance_id;
                    let name = u.name;
                    let data = DispatchData {
                        payloads: Payloads { payloads: u.input },
                        headers: u.headers,
                        converter: self.data_converter.payload_converter(),
                    };

                    let trait_val_result = if u.run_validator {
                        match panic::catch_unwind(AssertUnwindSafe(|| {
                            self.execution.validate_update(&name, &data)
                        })) {
                            Ok(r) => r,
                            Err(e) => {
                                bail!("Panic in update validator {}", panic_formatter(e));
                            }
                        }
                    } else {
                        Some(Ok(()))
                    };

                    let mut not_found = false;
                    match trait_val_result {
                        Some(Ok(())) => {
                            if let Some(fut) = self.execution.start_update(&name, data) {
                                outgoing_cmds.push(
                                    update_response(
                                        protocol_instance_id.clone(),
                                        update_response::Response::Accepted(()),
                                    )
                                    .into(),
                                );
                                self.update_futures
                                    .push((protocol_instance_id.clone(), fut));
                            } else {
                                not_found = true;
                            }
                        }
                        Some(Err(e)) => {
                            outgoing_cmds.push(
                                update_response(
                                    protocol_instance_id.clone(),
                                    update_response::Response::Rejected(
                                        self.workflow_error_to_failure(e),
                                    ),
                                )
                                .into(),
                            );
                        }
                        None => {
                            not_found = true;
                        }
                    }
                    if not_found {
                        outgoing_cmds.push(
                            update_response(
                                protocol_instance_id,
                                update_response::Response::Rejected(
                                    format!(
                                        "No update handler registered for update name {}",
                                        name
                                    )
                                    .into(),
                                ),
                            )
                            .into(),
                        );
                    }
                }
                Variant::ResolveNexusOperationStart(attrs) => {
                    self.unblock(UnblockEvent::NexusOperationStart(
                        attrs.seq,
                        Box::new(
                            attrs
                                .status
                                .context("Nexus operation start must have status")?,
                        ),
                    ))?
                }
                Variant::ResolveNexusOperation(attrs) => {
                    self.unblock(UnblockEvent::NexusOperationComplete(
                        attrs.seq,
                        Box::new(attrs.result.context("Nexus operation must have result")?),
                    ))?
                }
                Variant::RemoveFromCache(_) => {
                    unreachable!("Cache removal should happen higher up");
                }
            }
        } else {
            bail!("Empty activation job variant");
        }

        Ok(true)
    }
}

impl Future for WorkflowFuture {
    type Output = WorkflowResult<Payload>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        'activations: loop {
            // WF must always receive an activation first before responding with commands.
            // Use the executor waker (cx) for the activation channel -- these wakes are
            // always legitimate and should not be tracked.
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
            let run_id = activation.run_id;
            {
                let mut wlock = self.base_ctx.shared_mut();
                wlock.is_replaying = activation.is_replaying;
                wlock.wf_time = activation.timestamp.try_into_or_none();
                wlock.history_length = activation.history_length;
                wlock.continue_as_new_suggested = activation.continue_as_new_suggested;
                wlock.current_deployment_version = activation
                    .deployment_version_for_current_task
                    .map(Into::into);
            }

            let mut activation_cmds = vec![];

            if is_only_eviction {
                // No need to do anything with the workflow code in this case
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
                    Some(WorkflowTaskFailedCause::NonDeterministicError)
                );
                continue 'activations;
            }

            let mut should_poll_wf = false;
            for WorkflowActivationJob { variant } in activation.jobs {
                match self.handle_job(variant, &mut activation_cmds) {
                    Ok(should_poll) => should_poll_wf |= should_poll,
                    Err(e) => {
                        self.fail_wft(run_id, e, None);
                        continue 'activations;
                    }
                }
            }

            // Poll sub-futures using the tracked context if available,
            // otherwise use the executor context.
            let repoll = if let Some(ref tracker) = self.wake_tracking {
                let waker = tracker.new_per_poll_waker(cx.waker());
                let mut tcx = Context::from_waker(&waker);
                self.poll_sub_futures(&mut tcx, should_poll_wf, &run_id, &mut activation_cmds)?
            } else {
                self.poll_sub_futures(cx, should_poll_wf, &run_id, &mut activation_cmds)?
            };
            if repoll {
                continue 'activations;
            }

            // TODO: deadlock detector
            // Check if there's nothing to unblock and workflow has not completed.
            // This is different from the assertion that was here before that checked that WF did
            // not produce any commands which is completely viable in the case WF is waiting on
            // multiple completions.

            self.send_completion(run_id, activation_cmds);

            // We don't actually return here, since we could be queried after finishing executing,
            // and it allows us to rely on evictions for death and cache management
        }
    }
}

// Separate impl block down here just to keep it close to the future poll implementation which
// it is specific to.
impl WorkflowFuture {
    /// Runs the inner re-poll loop for signal, update, and workflow futures.
    /// Returns `Ok(true)` if the activation loop should continue (restart from
    /// the top), `Ok(false)` if the activation is fully processed.
    fn poll_sub_futures(
        &mut self,
        cx: &mut Context<'_>,
        should_poll_wf: bool,
        run_id: &str,
        activation_cmds: &mut Vec<WorkflowCommand>,
    ) -> Result<bool, Error> {
        self.base_ctx.take_state_mutated();
        loop {
            // Poll signals
            let signal_result: Result<Vec<_>, _> = std::mem::take(&mut self.signal_futures)
                .into_iter()
                .filter_map(|mut sig_fut| match sig_fut.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => None,
                    Poll::Ready(Err(e)) => Some(Err(e)),
                    Poll::Pending => Some(Ok(sig_fut)),
                })
                .collect();
            match signal_result {
                Ok(remaining) => self.signal_futures = remaining,
                Err(e) => {
                    self.fail_wft(
                        run_id.to_owned(),
                        anyhow!("Signal handler error: {}", e),
                        None,
                    );
                    return Ok(true);
                }
            }

            // Poll updates
            self.update_futures = std::mem::take(&mut self.update_futures)
                .into_iter()
                .filter_map(
                    |(instance_id, mut update_fut)| match update_fut.poll_unpin(cx) {
                        Poll::Ready(v) => {
                            self.base_ctx.send(
                                update_response(
                                    instance_id,
                                    match v {
                                        Ok(v) => update_response::Response::Completed(v),
                                        Err(e) => update_response::Response::Rejected(
                                            self.workflow_error_to_failure(e),
                                        ),
                                    },
                                )
                                .into(),
                            );
                            None
                        }
                        Poll::Pending => Some((instance_id, update_fut)),
                    },
                )
                .collect();

            if should_poll_wf && self.poll_wf_future(cx, run_id, activation_cmds)? {
                return Ok(true);
            }

            if !self.base_ctx.take_state_mutated() {
                break;
            }
        }
        Ok(false)
    }

    /// Returns true if the workflow future polling loop should be continued
    fn poll_wf_future(
        &mut self,
        cx: &mut Context,
        run_id: &str,
        activation_cmds: &mut Vec<WorkflowCommand>,
    ) -> Result<bool, Error> {
        // SAFETY: AssertUnwindSafe is safe here because:
        // 1. Workflows run in a single-threaded LocalSet - no data races possible
        // 2. Workflow state uses closure-scoped borrows (RefCell guards released on unwind)
        // 3. Core sends eviction after WFT failure, discarding all workflow state
        let mut res = {
            let _guard = self.span.enter();
            match panic::catch_unwind(AssertUnwindSafe(|| self.execution.poll_run(cx))) {
                Ok(r) => r,
                Err(e) => {
                    let errmsg = format!("Workflow function panicked: {}", panic_formatter(e));
                    warn!("{}", errmsg);
                    self.outgoing_completions
                        .send(WorkflowActivationCompletion::fail(
                            run_id,
                            self.data_converter.to_failure(
                                &SerializationContextData::Workflow,
                                OutgoingError::Workflow(OutgoingWorkflowError::Application(
                                    Box::new(crate::ApplicationFailure::non_retryable(anyhow!(
                                        "{errmsg}"
                                    ))),
                                )),
                            ),
                            None,
                        ))
                        .expect("Completion channel intact");
                    // Loop back up because we're about to get evicted
                    return Ok(true);
                }
            }
        };

        while let Ok(cmd) = self.incoming_commands.try_recv() {
            match cmd {
                RustWfCmd::Cancel(cancellable_id) => {
                    let cmd_variant = match cancellable_id {
                        CancellableID::Timer(seq) => {
                            if !self.maybe_unblock(UnblockEvent::Timer(seq, TimerResult::Cancelled))
                            {
                                // If timer is no longer present, do not emit command for cancellation.
                                continue;
                            }
                            // Re-poll wf future since a timer is now unblocked
                            res = self.execution.poll_run(cx);
                            workflow_command::Variant::CancelTimer(CancelTimer { seq })
                        }
                        CancellableID::Activity(seq) => {
                            workflow_command::Variant::RequestCancelActivity(
                                RequestCancelActivity { seq },
                            )
                        }
                        CancellableID::LocalActivity(seq) => {
                            workflow_command::Variant::RequestCancelLocalActivity(
                                RequestCancelLocalActivity { seq },
                            )
                        }
                        CancellableID::ChildWorkflow { seqnum, reason } => {
                            workflow_command::Variant::CancelChildWorkflowExecution(
                                CancelChildWorkflowExecution {
                                    child_workflow_seq: seqnum,
                                    reason,
                                },
                            )
                        }
                        CancellableID::SignalExternalWorkflow(seq) => {
                            workflow_command::Variant::CancelSignalWorkflow(CancelSignalWorkflow {
                                seq,
                            })
                        }
                        CancellableID::NexusOp(seq) => {
                            workflow_command::Variant::RequestCancelNexusOperation(
                                RequestCancelNexusOperation { seq },
                            )
                        }
                    };
                    activation_cmds.push(cmd_variant.into());
                }

                RustWfCmd::NewCmd(cmd) => {
                    let command_id = match cmd.cmd.variant.as_ref().expect("command variant is set")
                    {
                        workflow_command::Variant::StartTimer(StartTimer { seq, .. }) => {
                            CommandID::Timer(*seq)
                        }
                        workflow_command::Variant::ScheduleActivity(ScheduleActivity {
                            seq,
                            ..
                        })
                        | workflow_command::Variant::ScheduleLocalActivity(
                            ScheduleLocalActivity { seq, .. },
                        ) => CommandID::Activity(*seq),
                        workflow_command::Variant::SetPatchMarker(_) => {
                            panic!("Set patch marker should be a nonblocking command")
                        }
                        workflow_command::Variant::StartChildWorkflowExecution(req) => {
                            let seq = req.seq;
                            CommandID::ChildWorkflowStart(seq)
                        }
                        workflow_command::Variant::SignalExternalWorkflowExecution(req) => {
                            CommandID::SignalExternal(req.seq)
                        }
                        workflow_command::Variant::RequestCancelExternalWorkflowExecution(req) => {
                            CommandID::CancelExternal(req.seq)
                        }
                        workflow_command::Variant::ScheduleNexusOperation(req) => {
                            CommandID::NexusOpStart(req.seq)
                        }
                        _ => unimplemented!("Command type not implemented"),
                    };
                    activation_cmds.push(cmd.cmd);

                    self.command_status.insert(
                        command_id,
                        WFCommandFutInfo {
                            unblocker: cmd.unblocker,
                        },
                    );
                }
                RustWfCmd::NewNonblockingCmd(cmd) => activation_cmds.push(cmd.into()),
                RustWfCmd::SubscribeChildWorkflowCompletion(sub) => {
                    self.command_status.insert(
                        CommandID::ChildWorkflowComplete(sub.seq),
                        WFCommandFutInfo {
                            unblocker: sub.unblocker,
                        },
                    );
                }
                RustWfCmd::ForceWFTFailure(err) => {
                    self.fail_wft(run_id.to_string(), err, None);
                    return Ok(true);
                }
                RustWfCmd::SubscribeNexusOperationCompletion { seq, unblocker } => {
                    self.command_status.insert(
                        CommandID::NexusOpComplete(seq),
                        WFCommandFutInfo { unblocker },
                    );
                }
            }
        }

        if let Poll::Ready(res) = res {
            let cmd = match res {
                Ok(result) => workflow_command::Variant::CompleteWorkflowExecution(
                    CompleteWorkflowExecution {
                        result: Some(result),
                    },
                ),
                Err(termination) => match termination {
                    WorkflowTermination::ContinueAsNew(cmd) => {
                        workflow_command::Variant::ContinueAsNewWorkflowExecution(*cmd)
                    }
                    WorkflowTermination::Cancelled => {
                        workflow_command::Variant::CancelWorkflowExecution(
                            CancelWorkflowExecution {},
                        )
                    }
                    WorkflowTermination::Evicted => {
                        panic!("Don't explicitly return WorkflowTermination::Evicted")
                    }
                    WorkflowTermination::Failed(e) => {
                        workflow_command::Variant::FailWorkflowExecution(FailWorkflowExecution {
                            failure: Some(self.data_converter.to_failure(
                                &SerializationContextData::Workflow,
                                OutgoingError::Workflow(e),
                            )),
                        })
                    }
                },
            };
            activation_cmds.push(cmd.into())
        }
        Ok(false)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum CommandID {
    Timer(u32),
    Activity(u32),
    ChildWorkflowStart(u32),
    ChildWorkflowComplete(u32),
    SignalExternal(u32),
    CancelExternal(u32),
    NexusOpStart(u32),
    NexusOpComplete(u32),
}

impl From<&UnblockEvent> for CommandID {
    fn from(event: &UnblockEvent) -> Self {
        match event {
            UnblockEvent::Timer(seq, _) => CommandID::Timer(*seq),
            UnblockEvent::Activity(seq, _) => CommandID::Activity(*seq),
            UnblockEvent::WorkflowStart(seq, _) => CommandID::ChildWorkflowStart(*seq),
            UnblockEvent::WorkflowComplete(seq, _) => CommandID::ChildWorkflowComplete(*seq),
            UnblockEvent::SignalExternal(seq, _) => CommandID::SignalExternal(*seq),
            UnblockEvent::CancelExternal(seq, _) => CommandID::CancelExternal(*seq),
            UnblockEvent::NexusOperationStart(seq, _) => CommandID::NexusOpStart(*seq),
            UnblockEvent::NexusOperationComplete(seq, _) => CommandID::NexusOpComplete(*seq),
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
