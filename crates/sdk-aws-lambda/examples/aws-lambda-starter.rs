mod workflows;

use std::env;
use temporalio_client::{
    Client, ClientOptions, WorkflowStartOptions, envconfig::LoadClientConfigProfileOptions,
};
use workflows::GreetingWorkflow;

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    let (connection, options) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let client = Client::connect(connection, options).await?;
    let handle = client
        .start_workflow(
            GreetingWorkflow::run,
            "Temporal".to_owned(),
            WorkflowStartOptions::new(env::var("TEMPORAL_TASK_QUEUE")?, env::var("WORKFLOW_ID")?)
                .build(),
        )
        .await?;
    println!("Started workflow, run_id: {:?}", handle.run_id());
    println!("Result: {}", handle.get_result(Default::default()).await?);
    Ok(())
}
