use std::time::Duration;
use temporalio_workflow::{
    WorkflowContext, WorkflowResult,
    component::{StaticWorkflowComponent, instantiate_component_workflow},
    runtime::{
        guest::WorkflowInstance,
        host::WorkflowHost,
        types::{WorkflowDefinitionDescriptor, WorkflowFailure, WorkflowInit},
    },
    workflow, workflow_methods,
};

#[workflow]
#[derive(Default)]
struct PatchActivationWorkflow;

#[workflow_methods]
impl PatchActivationWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, patch_id: String) -> WorkflowResult<Vec<bool>> {
        let first = ctx.patched(&patch_id);
        ctx.timer(Duration::from_millis(1)).await;
        Ok(vec![first, ctx.patched(&patch_id)])
    }
}

struct WasmPatchActivationWorkflowModule;

impl StaticWorkflowComponent for WasmPatchActivationWorkflowModule {
    fn list_workflows() -> Vec<WorkflowDefinitionDescriptor> {
        vec![
            <PatchActivationWorkflow as temporalio_workflow::runtime::entry::WorkflowImplementation>::definition(),
        ]
    }

    fn instantiate_workflow(
        workflow_type: &str,
        init: WorkflowInit,
        host: std::rc::Rc<dyn WorkflowHost>,
    ) -> Result<Box<dyn WorkflowInstance>, WorkflowFailure> {
        match workflow_type {
            name if name
                == <PatchActivationWorkflow as temporalio_workflow::runtime::entry::WorkflowImplementation>::name() =>
            {
                instantiate_component_workflow::<PatchActivationWorkflow>(init, host)
            }
            _ => unreachable!("unexpected workflow type '{workflow_type}'"),
        }
    }
}

type WasmPatchActivationWorkflowComponentExport =
    temporalio_workflow::component::ExportedComponent<WasmPatchActivationWorkflowModule>;

temporalio_workflow::__temporalio_export_workflow_component!(
    WasmPatchActivationWorkflowComponentExport
);
