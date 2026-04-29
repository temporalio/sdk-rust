use temporalio_workflow::{
    WorkflowContext, WorkflowResult, export_workflow_module, workflow, workflow_methods,
};

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

export_workflow_module!([HelloWorkflow]);
