mod activities;

use std::time::Duration;

use activities::GreetingActivities;
use temporalio_client::{
    ActivityStartOptions, Client, ClientOptions, Connection,
    envconfig::LoadClientConfigProfileOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let options = ActivityStartOptions::with_start_to_close_timeout(
        "standalone-activities",
        "standalone-activity-id",
        Duration::from_secs(10),
    )
    .build();

    // Returns as soon as the server has durably enqueued the activity.
    let handle = client
        .start_activity(
            GreetingActivities::compose_greeting,
            ("Hello".to_string(), "Temporal".to_string()),
            options,
        )
        .await?;

    println!(
        "Started activity, id: {} run_id: {:?}",
        handle.activity_id(),
        handle.run_id()
    );

    Ok(())
}
