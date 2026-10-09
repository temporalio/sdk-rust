use std::time::Duration;
use temporalio_workflow::{
    WorkflowContext, WorkflowResult,
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

temporalio_workflow::export_workflow_module!([PatchActivationWorkflow]);
