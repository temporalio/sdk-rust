#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub struct CancellationActivities;

#[activities]
impl CancellationActivities {
    #[activity]
    pub async fn long_running_activity(
        ctx: ActivityContext,
        _input: (),
    ) -> Result<String, ActivityError> {
        loop {
            if ctx.is_cancelled() {
                return Err(ActivityError::cancelled());
            }
            ctx.record_heartbeat(vec![]);
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    #[activity]
    pub async fn cleanup(_ctx: ActivityContext, _input: ()) -> Result<String, ActivityError> {
        Ok("cleanup done".to_string())
    }
}

fn activity_opts() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(300))
        .heartbeat_timeout(Duration::from_secs(5))
        .build()
}

#[workflow]
#[derive(Default)]
pub struct CancellationWorkflow;

#[workflow_methods]
impl CancellationWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, _input: ()) -> WorkflowResult<String> {
        let mut activity_fut = ctx.start_activity(
            CancellationActivities::long_running_activity,
            (),
            activity_opts(),
        );

        temporalio_sdk::workflows::select! {
            result = &mut activity_fut => {
                let value = result?;
                Ok(value)
            }
            reason = ctx.cancelled() => {
                activity_fut.cancel();

                let cleanup_result = ctx
                    .start_activity(
                        CancellationActivities::cleanup,
                        (),
                        ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
                    )
                    .await?;

                Ok(format!("Cancelled (reason={reason}), {cleanup_result}"))
            }
        }
    }
}
