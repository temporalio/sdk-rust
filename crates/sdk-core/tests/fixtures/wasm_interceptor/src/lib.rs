use std::sync::Arc;
use temporalio_workflow::{
    WorkflowContext, WorkflowResult,
    component::{StaticWorkflowComponent, instantiate_component_workflow_with_interceptors},
    runtime::{
        guest::WorkflowInstance,
        host::WorkflowHost,
        types::{WorkflowDefinitionDescriptor, WorkflowFailure, WorkflowInit},
    },
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

struct WasmInterceptorWorkflowModule;

impl StaticWorkflowComponent for WasmInterceptorWorkflowModule {
    fn list_workflows() -> Vec<WorkflowDefinitionDescriptor> {
        vec![
            <InterceptorWorkflow as temporalio_workflow::runtime::entry::WorkflowImplementation>::definition(),
        ]
    }

    fn instantiate_workflow(
        workflow_type: &str,
        init: WorkflowInit,
        host: std::rc::Rc<dyn WorkflowHost>,
    ) -> Result<Box<dyn WorkflowInstance>, WorkflowFailure> {
        match workflow_type {
            name if name
                == <InterceptorWorkflow as temporalio_workflow::runtime::entry::WorkflowImplementation>::name() =>
            {
                instantiate_component_workflow_with_interceptors::<InterceptorWorkflow>(
                    init,
                    host,
                    vec![Arc::new(WasmWorkflowInterceptor)],
                )
            }
            _ => unreachable!("unexpected workflow type '{workflow_type}'"),
        }
    }
}

type WasmInterceptorWorkflowComponentExport =
    temporalio_workflow::component::ExportedComponent<WasmInterceptorWorkflowModule>;

temporalio_workflow::__temporalio_export_workflow_component!(
    WasmInterceptorWorkflowComponentExport
);
