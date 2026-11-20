mod codec;
mod storage;
mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use workflows::{OrderActivities, ProcessOrderBatchWorkflow};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::from_current_tokio(Default::default())?;
    let (conn_opts, mut client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    // The worker uses its client's data converter, so this is all it takes to offload the large
    // activity inputs and results it sends.
    client_opts.data_converter = storage::data_converter().await?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let worker_options = WorkerOptions::new("external-storage")
        .register_workflow::<ProcessOrderBatchWorkflow>()?
        .register_activities(OrderActivities)
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    println!("Worker started on task queue: external-storage");
    worker.run().await?;

    Ok(())
}
