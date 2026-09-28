use super::super::local_activity_state_machine::ResolveDat;
use crate::{
    protosext::{CompleteLocalActivityData, ValidScheduleLA},
    worker::{ExecutingLAId, LocalActRequest, NewLocalAct},
};
use std::{
    collections::{HashSet, VecDeque},
    time::SystemTime,
};
use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution;

struct Preresolution {
    seq: u32,
    /// Older markers have no group, so replay must keep their existing batching behavior.
    activation_group: Option<u64>,
    resolution: ResolveDat,
}

#[derive(Default)]
pub(super) struct LocalActivityData {
    /// Queued local activity requests which need to be executed
    new_requests: Vec<ValidScheduleLA>,
    /// Queued cancels that need to be dispatched
    cancel_requests: Vec<ExecutingLAId>,
    /// Seq #s of local activities which we have sent to be executed but have not yet resolved
    executing: HashSet<u32>,
    /// Local activity resolutions in the order their markers were found while looking ahead at the
    /// next WFT.
    preresolutions: VecDeque<Preresolution>,
    /// Results delivered in one activation need the same group on their markers, even when the
    /// activities completed at different times.
    activation_group: u64,
    /// Hold later groups until lang has responded to the activation for this group.
    replay_activation_group: Option<u64>,
    /// Set true if the workflow is terminating
    am_terminating: bool,
}

impl LocalActivityData {
    pub(super) fn enqueue(&mut self, act: ValidScheduleLA) {
        self.new_requests.push(act);
    }

    pub(super) fn enqueue_cancel(&mut self, cancel: ExecutingLAId) {
        self.cancel_requests.push(cancel);
    }

    pub(super) fn done_executing(&mut self, seq: u32) {
        // This seems nonsense, but can happen during abandonment
        self.new_requests.retain(|req| req.seq != seq);
        self.executing.remove(&seq);
    }

    /// Drain all requests to execute or cancel LAs. Additional info is passed in to be able to
    /// augment the data this struct has to form complete request data.
    pub(super) fn take_all_reqs(
        &mut self,
        wf_type: &str,
        wf_id: &str,
        run_id: &str,
    ) -> Vec<LocalActRequest> {
        if self.am_terminating {
            return vec![LocalActRequest::CancelAllInRun(run_id.to_string())];
        }

        self.cancel_requests
            .drain(..)
            .map(LocalActRequest::Cancel)
            .chain(self.new_requests.drain(..).map(|sa| {
                self.executing.insert(sa.seq);
                LocalActRequest::New(NewLocalAct {
                    schedule_time: SystemTime::now(),
                    schedule_cmd: sa,
                    workflow_type: wf_type.to_string(),
                    workflow_exec_info: WorkflowExecution {
                        workflow_id: wf_id.to_string(),
                        run_id: run_id.to_string(),
                    },
                })
            }))
            .collect()
    }

    /// Returns all outstanding local activities, whether executing or requested and in the queue
    pub(super) fn outstanding_la_count(&self) -> usize {
        if self.am_terminating {
            return 0;
        }
        self.executing.len() + self.new_requests.len()
    }

    pub(super) fn insert_peeked_marker(&mut self, dat: CompleteLocalActivityData) {
        if let Some(group) = dat.marker_dat.activation_group {
            // Once replay reaches live execution, newly recorded markers need group IDs beyond
            // those in history so separate activations cannot appear to belong to one group.
            self.activation_group = self.activation_group.max(group + 1);
        }
        self.preresolutions.push_back(Preresolution {
            seq: dat.marker_dat.seq,
            activation_group: dat.marker_dat.activation_group,
            resolution: dat.into(),
        });
    }

    pub(super) fn take_preresolution(&mut self, seq: u32) -> Option<ResolveDat> {
        let idx = self
            .preresolutions
            .iter()
            .position(|item| item.seq == seq)?;
        let item = self
            .preresolutions
            .remove(idx)
            .expect("This index was just found to contain seq");
        self.replay_activation_group = item.activation_group;
        Some(item.resolution)
    }

    pub(super) fn peek_preresolution_seq(&self) -> Option<u32> {
        let item = self.preresolutions.front()?;
        if let (Some(current), Some(next)) = (self.replay_activation_group, item.activation_group)
            && current != next
        {
            return None;
        }
        Some(item.seq)
    }

    pub(super) fn has_grouped_preresolution(&self, seq: u32) -> bool {
        self.preresolutions
            .iter()
            .any(|item| item.seq == seq && item.activation_group.is_some())
    }

    pub(super) fn activation_dispatched(&mut self) {
        self.activation_group += 1;
        self.replay_activation_group = None;
    }

    pub(super) fn current_activation_group(&self) -> u64 {
        self.activation_group
    }

    pub(super) fn remove_from_queue(&mut self, seq: u32) -> Option<ValidScheduleLA> {
        self.new_requests
            .iter()
            .position(|req| req.seq == seq)
            .map(|i| self.new_requests.remove(i))
    }

    /// Store that this workflow is terminating, and thus no new LA requests need be processed,
    /// and any executing LAs should not prevent us from shutting down.
    pub(super) fn indicate_terminating(&mut self) {
        self.am_terminating = true;
    }
}
