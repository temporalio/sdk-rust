// The worker registers the activity but never calls it by name; the client programs do.
#![allow(dead_code)]

mod activities;

use activities::GreetingActivities;
use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::from_current_tokio(Default::default())?;
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    // A Worker that only runs Standalone Activities needs no registered workflows.
    let worker_options = WorkerOptions::new("standalone-activities")
        .register_activities(GreetingActivities)
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    println!("Worker started on task queue: standalone-activities");
    worker.run().await?;

    Ok(())
}
