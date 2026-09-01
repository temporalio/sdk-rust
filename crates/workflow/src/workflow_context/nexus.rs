use super::*;
use crate::{
    runtime::{SdkGuardedFuture, model::NexusStartResult},
    workflow_interceptors::{StartNexusOperationInput, call_start_nexus_operation},
};
use futures_util::{FutureExt, future::Shared};
use temporalio_common_wasm::protos::coresdk::nexus::NexusOperationResult;

impl BaseWorkflowContext {
    pub(crate) fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<Output = NexusStartResult> {
        let input = StartNexusOperationInput::new(opts);
        let base_ctx = self.clone();
        let next = WorkflowNext::new(move |input: StartNexusOperationInput| {
            let mut opts = input.into_options();
            let cancellation_token = opts
                .cancellation_token
                .take()
                .unwrap_or_else(|| base_ctx.cancellation_token());
            let seq = base_ctx.inner.seq_nums.borrow_mut().next_nexus_op_seq();
            let (result_future, unblocker) =
                CancellableWFCommandFut::new(CancellableID::NexusOp(seq), base_ctx.clone());
            base_ctx
                .inner
                .runtime
                .register_unblocker(PendingCommandId::NexusOpComplete(seq), unblocker);
            base_ctx
                .inner
                .runtime
                .host
                .push_command(opts.into_command(seq));
            let result_future = CancellableWorkflowOutboundFuture::new(
                result_future,
                base_ctx.cancellation_handle(CancellableID::NexusOp(seq)),
            )
            .with_cancellation_token(cancellation_token)
            .shared();
            let (cmd, unblocker) = CancellableWFCommandFut::new_with_dat(
                CancellableID::NexusOp(seq),
                NexusUnblockData {
                    result_future: result_future.clone(),
                    schedule_seq: seq,
                    base_ctx: base_ctx.clone(),
                },
                base_ctx.clone(),
            );
            base_ctx
                .inner
                .runtime
                .register_unblocker(PendingCommandId::NexusOpStart(seq), unblocker);
            cancellable_outbound(cmd)
        });
        let interceptors = self.inner.workflow_interceptors.clone();
        let future = call_start_nexus_operation(
            interceptors,
            WorkflowInterceptorContext::new(self.clone()),
            input,
            next,
        );
        self.prepare_cancellable_outbound_future(future)
    }
}

impl<W> SyncWorkflowContext<W> {
    /// Start a Nexus operation.
    pub fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<Output = NexusStartResult> {
        self.base.start_nexus_operation(opts)
    }
}

impl<W> WorkflowContext<W> {
    /// Start a Nexus operation.
    pub fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<Output = NexusStartResult> {
        self.sync.start_nexus_operation(opts)
    }
}

impl WfCtxProtectedDat {
    fn next_nexus_op_seq(&mut self) -> u32 {
        let seq = self.next_nexus_op_sequence_number;
        self.next_nexus_op_sequence_number += 1;
        seq
    }
}

#[derive(derive_more::Debug)]
#[debug("StartedNexusOperation{{ operation_token: {operation_token:?} }}")]
/// Handle to a started Nexus operation.
pub struct StartedNexusOperation {
    /// The operation token, if the operation started asynchronously
    pub operation_token: Option<String>,
    #[debug(skip)]
    pub(crate) result_future: Shared<CancellableWorkflowOutboundFuture<NexusOperationResult>>,
    pub(crate) schedule_seq: u32,
    #[debug(skip)]
    pub(crate) base_ctx: BaseWorkflowContext,
}

pub(crate) struct NexusUnblockData {
    pub(crate) result_future: Shared<CancellableWorkflowOutboundFuture<NexusOperationResult>>,
    pub(crate) schedule_seq: u32,
    pub(crate) base_ctx: BaseWorkflowContext,
}

impl StartedNexusOperation {
    /// Wait for the operation result.
    pub async fn result(&self) -> NexusOperationResult {
        // The result future is a `Shared`; poll it inside an `SdkWakeGuard` (via
        // `SdkGuardedFuture`) so its internal waker machinery isn't mistaken for a non-SDK wake on
        // replay (which would fail the workflow task with TMPRL1100).
        SdkGuardedFuture(self.result_future.clone()).await
    }

    /// Request cancellation of the operation.
    pub fn cancel(&self) {
        self.base_ctx
            .cancel(CancellableID::NexusOp(self.schedule_seq));
    }
}
