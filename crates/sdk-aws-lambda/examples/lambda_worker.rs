use std::{env, time::Duration};
use temporalio_common::worker::WorkerDeploymentVersion;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkerOptions, WorkflowContext, WorkflowContextView, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_aws_lambda::{LambdaWorker, otel::OpenTelemetryOptions};

struct GreetingActivities;

#[activities]
impl GreetingActivities {
    #[activity]
    async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}

#[workflow]
struct GreetingWorkflow;

#[workflow_methods]
impl GreetingWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView) -> Self {
        Self
    }

    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        Ok(ctx
            .execute_activity(
                GreetingActivities::greet,
                name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?)
    }
}

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
