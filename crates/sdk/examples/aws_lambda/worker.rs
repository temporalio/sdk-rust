mod workflows;

use std::env;
use temporalio_common::worker::WorkerDeploymentVersion;
use temporalio_sdk::WorkerOptions;
use temporalio_sdk_aws_lambda::{LambdaWorker, otel::OpenTelemetryOptions};
use workflows::{GreetingActivities, GreetingWorkflow};

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    lambda_runtime::tracing::init_default_subscriber();
    let version = WorkerDeploymentVersion::builder()
        .deployment_name(env::var("TEMPORAL_DEPLOYMENT_NAME")?)
        .build_id(env::var("TEMPORAL_BUILD_ID")?)
        .build();
    let options = WorkerOptions::new("")
        .register_workflow::<GreetingWorkflow>()?
        .register_activities(GreetingActivities)
        .build();
    LambdaWorker::builder(version, options)
        .open_telemetry(OpenTelemetryOptions::default())
        .build()
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .run()
        .await
}
