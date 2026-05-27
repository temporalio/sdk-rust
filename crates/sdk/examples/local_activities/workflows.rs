#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, LocalActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub struct GreetingActivities;

#[activities]
impl GreetingActivities {
    #[activity]
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}

#[workflow]
#[derive(Default)]
pub struct LocalActivitiesWorkflow;

#[workflow_methods]
impl LocalActivitiesWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        name: String,
    ) -> WorkflowResult<(String, String)> {
        let remote_result = ctx
            .start_activity(
                GreetingActivities::greet,
                name.clone(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;

        let local_result = ctx
            .start_local_activity(
                GreetingActivities::greet,
                name,
                LocalActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(10)),
                    ..Default::default()
                },
            )
            .await?;

        Ok((remote_result, local_result))
    }
}
