use temporalio_workflow::{
    __private::{
        macros::{ExportedComponent, StaticWorkflowComponent},
        sdk::{
            ActivationJobResult, ActivationResult, MAIN_ROUTINE_ID, MainRoutineCompletion,
            RoutineCompletion, RoutinePollResult, TaskFailure, WorkflowActivation,
            WorkflowFailure, WorkflowHost, WorkflowInit, WorkflowInstance,
        },
    },
    common::protos::temporal::api::{
        enums::v1::WorkflowTaskFailedCause,
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    },
    workflows::WorkflowDefinitionDescriptor,
};

struct WasmTaskFailureWorkflow;

impl WorkflowInstance for WasmTaskFailureWorkflow {
    fn activate(
        &mut self,
        activation: WorkflowActivation,
        _waker: &std::task::Waker,
    ) -> Result<ActivationResult, WorkflowFailure> {
        Ok(ActivationResult {
            job_results: activation
                .jobs
                .iter()
                .map(|_| ActivationJobResult::None)
                .collect(),
        })
    }

    fn poll_routine(
        &mut self,
        routine_id: u64,
        _waker: &std::task::Waker,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        if routine_id != MAIN_ROUTINE_ID {
            return Err(Box::new(Failure {
                message: format!("unexpected routine id {routine_id}"),
                ..Default::default()
            }));
        }

        Ok(RoutinePollResult {
            completion: Some(RoutineCompletion::Main(MainRoutineCompletion::TaskFailed(
                TaskFailure {
                    failure: Box::new(Failure {
                        message: "structured wasm workflow task failure".to_string(),
                        failure_info: Some(FailureInfo::ApplicationFailureInfo(
                            ApplicationFailureInfo {
                                r#type: "WasmTaskFailure".to_string(),
                                non_retryable: true,
                                ..Default::default()
                            },
                        )),
                        ..Default::default()
                    }),
                    force_cause: Some(WorkflowTaskFailedCause::NonDeterministicError as u32),
                },
            ))),
            made_progress: true,
            pending_state: None,
        })
    }
}

struct WasmTaskFailureWorkflowModule;

impl StaticWorkflowComponent for WasmTaskFailureWorkflowModule {
    fn list_workflows() -> Vec<WorkflowDefinitionDescriptor> {
        vec![WorkflowDefinitionDescriptor {
            workflow_type: "WasmTaskFailureWorkflow".to_string(),
            has_init: false,
            init_takes_input: false,
            signals: vec![],
            queries: vec![],
            updates: vec![],
        }]
    }

    fn instantiate_workflow(
        workflow_type: &str,
        _init: WorkflowInit,
        _host: std::rc::Rc<dyn WorkflowHost>,
    ) -> Result<Box<dyn WorkflowInstance>, WorkflowFailure> {
        match workflow_type {
            "WasmTaskFailureWorkflow" => Ok(Box::new(WasmTaskFailureWorkflow)),
            _ => Err(Box::new(Failure {
                message: format!("No workflow named '{workflow_type}' exported by this component"),
                ..Default::default()
            })),
        }
    }
}

type WasmTaskFailureWorkflowComponentExport = ExportedComponent<WasmTaskFailureWorkflowModule>;

temporalio_workflow::__temporalio_export_workflow_component!(
    WasmTaskFailureWorkflowComponentExport
);
