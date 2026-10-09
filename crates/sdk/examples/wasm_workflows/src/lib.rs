use temporalio_workflow::{WorkflowContext, WorkflowResult, workflow, workflow_methods};

#[workflow]
#[derive(Default)]
pub struct HelloWorkflow;

#[workflow_methods]
impl HelloWorkflow {
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        Ok(format!("Hello, {name}!"))
    }
}

temporalio_workflow::export_workflow_module!([HelloWorkflow]);
