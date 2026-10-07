mod workflows;

use std::env;
use temporalio_client::{
    Client, ClientOptions, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use uuid::Uuid;
use workflows::GreetingWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let name = env::args().nth(1).unwrap_or_else(|| "Temporal".to_owned());
    let task_queue = env::var("TEMPORAL_TASK_QUEUE")?;
    let prefix =
        env::var("TEMPORAL_LAMBDA_WORKFLOW_ID_PREFIX").unwrap_or_else(|_| "rust-lambda".to_owned());
    let workflow_id = format!("{prefix}-{}", Uuid::new_v4());
    let (connection_options, client_options) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let client = Client::connect(connection_options, client_options).await?;
    let handle = client
        .start_workflow(
            GreetingWorkflow::run,
            name,
            WorkflowStartOptions::new(task_queue, workflow_id.clone()).build(),
        )
        .await?;
    println!(
        "Started workflow: {workflow_id}, run ID: {:?}",
        handle.run_id()
    );
    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");
    Ok(())
}
