use std::{collections::HashMap, fmt::Debug, sync::Arc};

use temporalio_common::{
    WorkflowDefinition,
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    protos::temporal::api::common::v1::Payload,
};
use temporalio_workflow::runtime::{
    BaseWorkflowContext,
    entry::WorkflowImplementation,
    guest::WorkflowInstance,
    instance::{GuestWorkflowInstance, instantiate_workflow},
};

/// Creates workflow execution instances from activation input payloads and context.
pub(crate) type WorkflowExecutionFactory = Arc<
    dyn Fn(
            Vec<Payload>,
            PayloadConverter,
            BaseWorkflowContext,
        ) -> Result<Box<dyn WorkflowInstance>, PayloadConversionError>
        + Send
        + Sync,
>;

/// Contains workflow registrations in a form ready for execution by workers.
#[derive(Default, Clone)]
pub struct WorkflowDefinitions {
    workflows: HashMap<&'static str, WorkflowExecutionFactory>,
}

impl WorkflowDefinitions {
    /// Creates a new empty `WorkflowDefinitions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a workflow implementation.
    pub fn register_workflow<W: WorkflowImplementation>(&mut self) -> &mut Self
    where
        <W::Run as WorkflowDefinition>::Input: Send,
    {
        let workflow_name = W::name();
        let factory: WorkflowExecutionFactory =
            Arc::new(move |payloads, converter: PayloadConverter, base_ctx| {
                instantiate_workflow::<W>(payloads, converter, base_ctx)
            });
        self.workflows.insert(workflow_name, factory);
        self
    }

    /// Register a workflow with a custom factory for instance creation.
    pub fn register_workflow_run_with_factory<W, F>(&mut self, user_factory: F) -> &mut Self
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
        F: Fn() -> W + Send + Sync + 'static,
    {
        assert!(
            !W::HAS_INIT,
            "Workflows registered with a factory must not define an #[init] method. \
             The factory replaces init for instance creation."
        );

        let workflow_name = W::name();
        let user_factory = Arc::new(user_factory);
        let factory: WorkflowExecutionFactory =
            Arc::new(move |payloads, converter: PayloadConverter, base_ctx| {
                let ser_ctx = SerializationContext {
                    data: &SerializationContextData::Workflow,
                    converter: &converter,
                };
                let input: <W::Run as WorkflowDefinition>::Input =
                    converter.from_payloads(&ser_ctx, payloads)?;

                let workflow = user_factory();
                Ok(Box::new(GuestWorkflowInstance::<W>::new_with_workflow(
                    workflow,
                    base_ctx,
                    Some(input),
                )) as Box<dyn WorkflowInstance>)
            });

        self.workflows.insert(workflow_name, factory);
        self
    }

    /// Check if any workflows are registered.
    pub fn is_empty(&self) -> bool {
        self.workflows.is_empty()
    }

    pub(crate) fn get_workflow(&self, workflow_type: &str) -> Option<WorkflowExecutionFactory> {
        self.workflows.get(workflow_type).cloned()
    }

    /// Returns an iterator over registered workflow type names.
    pub fn workflow_types(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.workflows.keys().copied()
    }
}

impl Debug for WorkflowDefinitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowDefinitions")
            .field("workflows", &self.workflows.keys().collect::<Vec<_>>())
            .finish()
    }
}
