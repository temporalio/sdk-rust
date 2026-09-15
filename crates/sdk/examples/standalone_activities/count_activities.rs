use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let count = client
        .count_activities("TaskQueue = 'standalone-activities'", Default::default())
        .await?;

    println!("Total: {}", count.count());
    // Non-empty only when the query has a GROUP BY clause.
    for group in count.groups() {
        println!("  {:?} => {}", group.get::<String>(0), group.count());
    }

    Ok(())
}
