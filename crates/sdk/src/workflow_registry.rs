use std::{collections::HashMap, fmt::Debug, rc::Rc, sync::Arc};

use anyhow::Context;
use temporalio_common::{
    WorkflowDefinition,
    data_converters::{
        GenericPayloadConverter, PayloadConverter, SerializationContext, SerializationContextData,
    },
    protos::{
        coresdk::workflow_activation::InitializeWorkflow, temporal::api::common::v1::Payload,
    },
};
use temporalio_workflow::{
    BaseWorkflowContext,
    runtime::{
        entry::WorkflowImplementation,
        guest::WorkflowInstance,
        host::WorkflowHost,
        instance::{GuestWorkflowInstance, instantiate_workflow},
        types::WorkflowDefinitionDescriptor,
    },
};

/// Host-owned execution inputs used to instantiate a single workflow run.
pub(crate) struct WorkflowExecutionInput {
    pub namespace: String,
    pub task_queue: String,
    pub run_id: String,
    pub init_workflow_job: InitializeWorkflow,
    pub payload_converter: PayloadConverter,
    pub host: Rc<dyn WorkflowHost>,
}

/// Creates workflow execution instances from activation input payloads and context.
pub(crate) type WorkflowExecutionFactory = Arc<
    dyn Fn(WorkflowExecutionInput) -> Result<Box<dyn WorkflowInstance>, anyhow::Error>
        + Send
        + Sync,
>;

#[derive(Clone)]
struct RegisteredWorkflow {
    definition: WorkflowDefinitionDescriptor,
    factory: WorkflowExecutionFactory,
}

/// Contains workflow registrations in a form ready for execution by workers.
#[derive(Default, Clone)]
pub struct WorkflowDefinitions {
    workflows: HashMap<String, RegisteredWorkflow>,
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
        let factory = Arc::new(move |input| {
            let (payloads, payload_converter, base_ctx) = workflow_input_parts(input);
            instantiate_workflow::<W>(payloads, payload_converter, base_ctx)
                .context("Failed to instantiate native workflow")
        });
        self.insert_workflow(W::definition(), factory)
            .unwrap_or_else(|err| panic!("{err}"));
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

        let factory = Arc::new(move |input| {
            let (payloads, payload_converter, base_ctx) = workflow_input_parts(input);
            let ser_ctx = SerializationContext {
                data: &SerializationContextData::Workflow,
                converter: &payload_converter,
            };
            let input: <W::Run as WorkflowDefinition>::Input =
                payload_converter.from_payloads(&ser_ctx, payloads)?;

            let workflow = user_factory();
            Ok(Box::new(GuestWorkflowInstance::<W>::new_with_workflow(
                workflow,
                base_ctx,
                Some(input),
            )) as Box<dyn WorkflowInstance>)
        });

        self.insert_workflow(W::definition(), factory)
            .unwrap_or_else(|err| panic!("{err}"));
        self
    }

    /// Check if any workflows are registered.
    pub fn is_empty(&self) -> bool {
        self.workflows.is_empty()
    }

    pub(crate) fn insert_workflow(
        &mut self,
        definition: WorkflowDefinitionDescriptor,
        factory: WorkflowExecutionFactory,
    ) -> Result<(), anyhow::Error> {
        let workflow_type = definition.workflow_type.clone();
        if self.workflows.contains_key(&workflow_type) {
            anyhow::bail!("Workflow type {workflow_type} is already registered");
        }
        self.workflows.insert(
            workflow_type,
            RegisteredWorkflow {
                definition,
                factory,
            },
        );
        Ok(())
    }

    pub(crate) fn get_workflow(&self, workflow_type: &str) -> Option<WorkflowExecutionFactory> {
        self.workflows
            .get(workflow_type)
            .map(|wf| wf.factory.clone())
    }

    /// Returns an iterator over registered workflow definitions.
    pub fn workflow_definitions(&self) -> impl Iterator<Item = &WorkflowDefinitionDescriptor> + '_ {
        self.workflows.values().map(|wf| &wf.definition)
    }
}

fn workflow_input_parts(
    input: WorkflowExecutionInput,
) -> (Vec<Payload>, PayloadConverter, BaseWorkflowContext) {
    let WorkflowExecutionInput {
        namespace,
        task_queue,
        run_id,
        init_workflow_job,
        payload_converter,
        host,
    } = input;
    let payloads = init_workflow_job.arguments.clone();
    let base_ctx = BaseWorkflowContext::new(
        namespace,
        task_queue,
        run_id,
        init_workflow_job,
        payload_converter.clone(),
        host,
    );
    (payloads, payload_converter, base_ctx)
}

impl Debug for WorkflowDefinitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowDefinitions")
            .field("workflows", &self.workflows.keys().collect::<Vec<_>>())
            .finish()
    }
}
