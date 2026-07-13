use crate::protos::temporal::api::common::v1::WorkflowExecution as ProtoWorkflowExecution;

/// Identifies a workflow execution by workflow and run ID.
#[derive(Clone, Debug, Default, bon::Builder)]
#[non_exhaustive]
#[builder(on(String, into), state_mod(vis = "pub"))]
pub struct WorkflowExecution {
    /// The workflow ID.
    pub workflow_id: String,
    /// The run ID, or an empty string when targeting the latest run.
    pub run_id: String,
    #[builder(skip = ProtoWorkflowExecution {
        workflow_id: workflow_id.clone(),
        run_id: run_id.clone(),
    })]
    raw: ProtoWorkflowExecution,
}

impl WorkflowExecution {
    /// Access the underlying workflow execution protobuf.
    pub fn raw(&self) -> &ProtoWorkflowExecution {
        &self.raw
    }

    /// Consume this wrapper and return the underlying workflow execution protobuf.
    pub fn into_raw(mut self) -> ProtoWorkflowExecution {
        self.raw.workflow_id = self.workflow_id;
        self.raw.run_id = self.run_id;
        self.raw
    }
}

impl PartialEq for WorkflowExecution {
    fn eq(&self, other: &Self) -> bool {
        self.workflow_id == other.workflow_id && self.run_id == other.run_id
    }
}

impl Eq for WorkflowExecution {}

impl From<ProtoWorkflowExecution> for WorkflowExecution {
    fn from(value: ProtoWorkflowExecution) -> Self {
        Self {
            workflow_id: value.workflow_id.clone(),
            run_id: value.run_id.clone(),
            raw: value,
        }
    }
}

impl From<WorkflowExecution> for ProtoWorkflowExecution {
    fn from(value: WorkflowExecution) -> Self {
        value.into_raw()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_and_proto_round_trip() {
        let execution = WorkflowExecution::builder()
            .workflow_id("workflow-id")
            .run_id("run-id")
            .build();

        assert_eq!(execution.raw().workflow_id, "workflow-id");
        assert_eq!(execution.raw().run_id, "run-id");

        assert_eq!(
            WorkflowExecution::from(ProtoWorkflowExecution::from(execution.clone())),
            execution
        );
    }

    #[test]
    fn retains_source_proto() {
        let raw = ProtoWorkflowExecution {
            workflow_id: "workflow-id".to_owned(),
            run_id: "run-id".to_owned(),
        };
        let execution = WorkflowExecution::from(raw.clone());

        assert_eq!(execution.raw(), &raw);
        assert_eq!(execution.into_raw(), raw);
    }
}
