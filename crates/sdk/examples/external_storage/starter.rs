mod codec;
mod storage;
mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use workflows::{BatchSummary, ProcessOrderBatchWorkflow};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, mut client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    client_opts.data_converter = storage::data_converter()?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let handle = client
        .start_workflow(
            ProcessOrderBatchWorkflow::run,
            200,
            WorkflowStartOptions::new("external-storage", "external-storage-workflow-id").build(),
        )
        .await?;
    println!("Started workflow, run_id: {:?}", handle.run_id());

    let BatchSummary {
        order_count,
        total_weight_kg,
    } = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Processed {order_count} orders weighing {total_weight_kg:.1} kg in total");

    Ok(())
}
