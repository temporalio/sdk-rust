use crate::worker::workflow::{OutgoingJob, WorkflowStartedInfo};
use prost_types::Timestamp;
use std::collections::HashMap;
use temporalio_common::protos::{
    coresdk::workflow_activation::start_workflow_from_attribs,
    temporal::api::{
        common::v1::{Payload, SearchAttributes},
        history::v1::WorkflowExecutionStartedEventAttributes,
    },
    utilities::TryIntoOrNone,
};

/// Tracks workflow information and queues jobs for the next language SDK activation.
#[derive(Default)]
pub(crate) struct DrivenWorkflow {
    started_attrs: Option<WorkflowStartedInfo>,
    search_attribute_modifications: HashMap<String, Payload>,
    /// Outgoing activation jobs that need to be sent to the lang sdk
    outgoing_wf_activation_jobs: Vec<OutgoingJob>,
}

impl DrivenWorkflow {
    /// Start the workflow
    pub(super) fn start(
        &mut self,
        workflow_id: String,
        randomness_seed: u64,
        start_time: Timestamp,
        attribs: WorkflowExecutionStartedEventAttributes,
    ) {
        debug!(run_id = %attribs.original_execution_run_id, "Driven WF start");
        let started_info = WorkflowStartedInfo {
            workflow_task_timeout: attribs.workflow_task_timeout.try_into_or_none(),
            memo: attribs.memo.clone(),
            search_attrs: attribs.search_attributes.clone(),
            retry_policy: attribs.retry_policy.clone(),
        };
        self.send_job(
            start_workflow_from_attribs(attribs, workflow_id, randomness_seed, start_time).into(),
        );
        self.started_attrs = Some(started_info);
    }

    /// Return the attributes from the workflow execution started event if this workflow has started
    pub(super) fn get_started_info(&self) -> Option<&WorkflowStartedInfo> {
        self.started_attrs.as_ref()
    }

    /// Enqueue a new job to be sent to the driven workflow
    pub(super) fn send_job(&mut self, job: OutgoingJob) {
        self.outgoing_wf_activation_jobs.push(job);
    }

    /// Observe pending jobs
    pub(super) fn peek_pending_jobs(&self) -> &[OutgoingJob] {
        self.outgoing_wf_activation_jobs.as_slice()
    }

    /// Drain all pending jobs, so that they may be sent to the driven workflow
    pub(super) fn drain_jobs(&mut self) -> Vec<OutgoingJob> {
        std::mem::take(&mut self.outgoing_wf_activation_jobs)
    }

    /// Lang sent us an SA upsert command - use it to update our current view of search attributes.
    pub(crate) fn search_attributes_update(&mut self, update: Option<SearchAttributes>) {
        if let Some(sa) = update {
            self.search_attribute_modifications
                .extend(sa.indexed_fields);
        }
    }

    /// Return a view of the "current" state of search attributes. IE: The initial attributes
    /// plus any changes during the lifetime of the workflow.
    pub(crate) fn get_current_search_attributes(&self) -> HashMap<String, Payload> {
        let mut retme = self
            .started_attrs
            .as_ref()
            .and_then(|si| si.search_attrs.as_ref().map(|sa| sa.indexed_fields.clone()))
            .unwrap_or_default();
        retme.extend(
            self.search_attribute_modifications
                .iter()
                .map(|(a, b)| (a.clone(), b.clone())),
        );
        retme
    }
}
