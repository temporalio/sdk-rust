use super::*;
use crate::{
    NexusOperationOptions, StartedNexusOperation, nexus::NexusOperationError,
    runtime::model::NexusStartResult,
};

impl WorkflowInterceptorContext {
    /// Start a Nexus operation through the workflow outbound interceptor chain.
    pub fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<Output = NexusStartResult> {
        self.base.start_nexus_operation(opts)
    }
}

/// Input passed to [`WorkflowInterceptor::start_nexus_operation`].
#[non_exhaustive]
pub struct StartNexusOperationInput {
    options: NexusOperationOptions,
}

impl StartNexusOperationInput {
    pub(crate) fn new(options: NexusOperationOptions) -> Self {
        Self { options }
    }

    pub(crate) fn into_options(self) -> NexusOperationOptions {
        self.options
    }

    /// Nexus operation options.
    pub fn options(&self) -> &NexusOperationOptions {
        &self.options
    }

    /// Mutably access Nexus operation options.
    pub fn options_mut(&mut self) -> &mut NexusOperationOptions {
        &mut self.options
    }
}

/// Result of an intercepted Nexus operation start.
pub type StartNexusOperationResult = Result<StartedNexusOperation, NexusOperationError>;

outbound_chain!(
    call_start_nexus_operation,
    start_nexus_operation,
    WorkflowInterceptorContext,
    StartNexusOperationInput,
    CancellableWorkflowOutboundFuture<StartNexusOperationResult>
);
