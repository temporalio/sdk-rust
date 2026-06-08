mod workflows;

use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_common::{telemetry::TelemetryOptions, worker::WorkerTaskTypes};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions};
use workflows::UpdatableTimerWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()?,
    )?;
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let worker_options = WorkerOptions::new("updatable-timer")
        .register_workflow::<UpdatableTimerWorkflow>()?
        .task_types(WorkerTaskTypes::workflow_only())
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    worker.run().await?;

    Ok(())
}
