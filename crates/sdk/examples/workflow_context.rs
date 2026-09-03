//! Establish application context in workflow code and observe it in an outbound interceptor.

use std::{sync::Arc, time::Duration};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowContextKey, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    workflow_interceptors::{
        CancellableWorkflowOutboundFuture, ScheduleActivityInput, ScheduleActivityResult,
        WorkflowInterceptor, WorkflowInterceptorContext, WorkflowNext,
    },
};

struct CurrentSpan;

impl WorkflowContextKey for CurrentSpan {
    type Value = String;
}

#[workflow]
#[derive(Default)]
struct ContextWorkflow;

#[workflow_methods]
impl ContextWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let scoped_ctx = ctx.clone();
        ctx.with_context_value::<CurrentSpan, _>(format!("greet-{name}"), async move {
            scoped_ctx
                .execute_activity(
                    GreetingActivities::greet,
                    name,
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
                )
                .await
                .map_err(Into::into)
        })
        .await
    }
}

struct GreetingActivities;

#[activities]
impl GreetingActivities {
    #[activity]
    async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}

struct ContextHeaderInterceptor;

impl WorkflowInterceptor for ContextHeaderInterceptor {
    fn schedule_activity(
        &self,
        ctx: WorkflowInterceptorContext,
        mut input: ScheduleActivityInput,
        next: WorkflowNext<
            'static,
            ScheduleActivityInput,
            CancellableWorkflowOutboundFuture<ScheduleActivityResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<ScheduleActivityResult> {
        if let Some(span) = ctx.context_value::<CurrentSpan>() {
            input.headers_mut().insert(
                "example-span".to_owned(),
                Payload {
                    metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
                    data: span.as_bytes().to_vec(),
                    ..Default::default()
                },
            );
        }
        next.run(input)
    }
}

fn main() {
    let _interceptor: Arc<dyn WorkflowInterceptor> = Arc::new(ContextHeaderInterceptor);
}
