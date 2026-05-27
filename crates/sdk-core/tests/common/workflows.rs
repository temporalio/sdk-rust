use crate::common::activity_functions::StdActivities;
use std::time::Duration;
use temporalio_common::protos::temporal::api::common::v1::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, LocalActivityOptions, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::prost_dur;

#[workflow]
#[derive(Default)]
pub(crate) struct LaProblemWorkflow;

#[workflow_methods]
impl LaProblemWorkflow {
    #[run(name = "evict_while_la_running_no_interference")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.start_local_activity(
            StdActivities::delay,
            Duration::from_secs(15),
            LocalActivityOptions {
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_micros(15))),
                    backoff_coefficient: 1_000.,
                    maximum_interval: Some(prost_dur!(from_millis(1500))),
                    maximum_attempts: 4,
                    non_retryable_error_types: vec![],
                },
                timer_backoff_threshold: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        ctx.start_activity(
            StdActivities::delay,
            Duration::from_secs(15),
            ActivityOptions::start_to_close_timeout(Duration::from_secs(20)),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }
}
