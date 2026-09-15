mod activities;

use activities::GreetingActivities;
use temporalio_client::{
    ActivityDescribeOptions, ActivityExecutionInfoLike, Client, ClientOptions, Connection,
    envconfig::LoadClientConfigProfileOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    // Passing `None` for the run ID targets the latest run with this activity ID.
    let handle = client.get_activity_handle(
        GreetingActivities::compose_greeting,
        "standalone-activity-id",
        None,
    );

    let description = handle
        .describe(
            ActivityDescribeOptions::builder()
                .include_outcome(true)
                .build(),
        )
        .await?;

    println!("Status: {:?}", description.status());
    println!("Type: {}", description.activity_type());
    println!("Attempt: {}", description.attempt());
    println!("Activity result: {}", handle.result().await?);

    Ok(())
}
