use temporalio_workflow::{
    WorkflowContext, WorkflowContextView, WorkflowResult,
    workflow,
    workflow_interceptors::{
        ExecuteWorkflowInput, ExecuteWorkflowResult, WorkflowInterceptor,
        WorkflowInterceptorContext, WorkflowInterceptorFuture, WorkflowNext, WorkflowOutputValue,
    },
    workflow_methods,
};

#[workflow]
#[derive(Default)]
struct InterceptorWorkflow;

#[workflow_methods]
impl InterceptorWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        Ok(format!("Hello, {name}!"))
    }
}

struct WasmWorkflowInterceptor;

impl WorkflowInterceptor for WasmWorkflowInterceptor {
    fn execute<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        mut input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        if let Some(name) = input.input_mut::<String>() {
            name.push_str("-intercepted");
        }
        WorkflowInterceptorFuture::new(async move {
            let result = next.run(input).await?;
            let result = result
                .downcast_ref::<String>()
                .expect("interceptor workflow should return a string");
            Ok(Box::new(format!("{result} [intercepted]")) as Box<dyn WorkflowOutputValue>)
        })
    }
}

temporalio_workflow::export_workflow_module!(
    [InterceptorWorkflow],
    interceptor_constructors = [|ctx: &WorkflowContextView| {
        assert_eq!(ctx.workflow_type(), "InterceptorWorkflow");
        assert!(!ctx.workflow_id().is_empty());
        assert!(!ctx.run_id().is_empty());
        assert!(!ctx.task_queue().is_empty());
        assert!(!ctx.namespace().is_empty());
        WasmWorkflowInterceptor
    }],
);
