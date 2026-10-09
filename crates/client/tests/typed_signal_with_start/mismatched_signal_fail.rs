use temporalio_client::{Client, WorkflowStartOptions};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_workflow::{SyncWorkflowContext, WorkflowContext, WorkflowResult};

#[workflow]
#[derive(Default)]
struct FirstWorkflow;

#[workflow_methods]
impl FirstWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }

    #[signal]
    fn first_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: String) {}
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

    #[signal]
    fn second_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: String) {}
}

fn mismatched_signal(client: &Client) {
    let _ = client.signal_with_start_workflow(
        FirstWorkflow::run,
        (),
        SecondWorkflow::second_signal,
        "signal".to_owned(),
        WorkflowStartOptions::new("task-queue", "workflow-id").build(),
    );
}

fn main() {}
