mod workflows;

use futures_util::FutureExt;
use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk::{
    Worker, WorkerOptions,
    interceptors::{ActivityInboundInterceptor, ExecuteActivityInput, ExecuteActivityOutput, Next},
};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions};
use workflows::{
    ActivityInterceptorWorkflow, GreetingActivities, GreetingRequest, GreetingResponse,
};

struct LoggingActivityInterceptor;

impl ActivityInboundInterceptor for LoggingActivityInterceptor {
    fn execute_activity<'a>(
        &'a self,
        input: ExecuteActivityInput,
        next: Next<'a, ExecuteActivityInput, ExecuteActivityOutput<'a>>,
    ) -> ExecuteActivityOutput<'a> {
        async move {
            let activity_type = input.activity_info().activity_type.clone();
            match activity_type.as_str() {
                name if name == GreetingActivities::greet.name() => {
                    if let Some(request) = input.args_ref::<GreetingRequest>() {
                        println!("greet input: {request:?}");
                    }
                }
                other => println!("running activity: {other}"),
            }
            let result = next.run(input).await;
            match activity_type.as_str() {
                name if name == GreetingActivities::greet.name() => {
                    if let Ok(output) = &result
                        && let Some(response) = output.downcast_ref::<GreetingResponse>()
                    {
                        println!("greet output: {response:?}");
                    }
                }
                _ => {}
            }
            if let Err(err) = &result {
                println!("activity {activity_type} failed: {err:?}");
            }
            result
        }
        .boxed()
    }
}

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

    let worker_options = WorkerOptions::new("activity-interceptor")
        .register_workflow::<ActivityInterceptorWorkflow>()
        .register_activities(GreetingActivities)
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    worker.add_activity_inbound_interceptor(LoggingActivityInterceptor);
    println!("Worker started on task queue: activity-interceptor");
    worker.run().await?;

    Ok(())
}
