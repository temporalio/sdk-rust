use temporalio_macros::{workflow, workflow_methods};
use temporalio_workflow::{
    WorkflowContext, WorkflowResult, export_workflow_module,
    workflow_interceptors::WorkflowInboundInterceptor,
};

#[workflow]
#[derive(Default)]
struct FirstWorkflow;

#[workflow_methods]
impl FirstWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct SecondWorkflow;

#[workflow_methods]
impl SecondWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

struct FirstInterceptor;
impl WorkflowInboundInterceptor for FirstInterceptor {}

struct SecondInterceptor;
impl WorkflowInboundInterceptor for SecondInterceptor {}

export_workflow_module!(
    [FirstWorkflow, SecondWorkflow],
    interceptors = [FirstInterceptor, SecondInterceptor],
);

fn main() {}
