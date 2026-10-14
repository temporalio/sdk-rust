use super::*;
use temporalio_common::protos::temporal::api::{
    history::v1::history_event::Attributes, workflowservice::v1::PollWorkflowTaskQueueResponse,
};

pub(super) struct BranchTasks {
    pub initial: PollWorkflowTaskQueueResponse,
    pub losing: PollWorkflowTaskQueueResponse,
    pub losing_retry: PollWorkflowTaskQueueResponse,
    pub winning: PollWorkflowTaskQueueResponse,
}

pub(super) fn branch_tasks(returned_wins: bool) -> BranchTasks {
    let mut common = TestHistoryBuilder::default();
    common.add_by_type(EventType::WorkflowExecutionStarted);
    common.add_full_wf_task();
    let mut initial = hist_to_poll_resp(&common, "branch-transition", 1.into()).resp;
    if returned_wins {
        // A scheduled task can start on either cluster after failover. Only the winning
        // branch's newly started A can complete successfully on the new active cluster.
        common.add_workflow_task_scheduled();
    }
    let common_end = if returned_wins { 5 } else { 4 };
    let task_number = if returned_wins { 3 } else { 2 };
    let mut losing = common.clone();
    let mut winning = common;
    if returned_wins {
        losing.add_workflow_task_started();
        losing.add_workflow_task_completed();
        winning.add_workflow_task_started();
        initial = hist_to_poll_resp(&winning, "branch-transition", 2.into()).resp;
        winning.add_workflow_task_completed();
    }
    for _ in 0..3 {
        losing.add_we_signaled("losing", vec![]);
    }
    losing.add_workflow_task_scheduled_and_started();
    let mut losing_response =
        hist_to_poll_resp(&losing, "branch-transition", task_number.into()).resp;
    losing.add(WorkflowTaskTimedOutEventAttributes {
        scheduled_event_id: losing_response.started_event_id - 1,
        started_event_id: losing_response.started_event_id,
        timeout_type: TimeoutType::StartToClose as i32,
    });
    losing.add_workflow_task_scheduled_and_started();
    let mut losing_retry = hist_to_poll_resp(&losing, "branch-transition", task_number.into()).resp;
    winning.add_we_signaled("winning", vec![]);
    winning.add_workflow_task_scheduled_and_started();
    let mut winning = hist_to_poll_resp(&winning, "branch-transition", task_number.into()).resp;
    for response in [
        &mut initial,
        &mut losing_response,
        &mut losing_retry,
        &mut winning,
    ] {
        response.attempt = 1;
        for event in &mut response.history.as_mut().unwrap().events {
            event.version = 1000;
            if let Some(Attributes::WorkflowTaskScheduledEventAttributes(attrs)) =
                &mut event.attributes
            {
                attrs.attempt = 1;
            }
        }
    }
    // Branch version, rather than event ID, selects the winning history for the same run.
    for response in [&mut initial, &mut winning] {
        for event in &mut response.history.as_mut().unwrap().events {
            if event.event_id > common_end {
                event.version = 1001;
            }
        }
    }
    assert_eq!(
        losing_response.workflow_execution,
        winning.workflow_execution
    );
    assert!(winning.started_event_id < losing_response.started_event_id);
    initial.task_token = b"initial-task".to_vec();
    losing_response.task_token = b"losing-branch-task".to_vec();
    losing_retry.task_token = b"losing-branch-retry".to_vec();
    winning.task_token = b"winning-branch-task".to_vec();
    BranchTasks {
        initial,
        losing: losing_response,
        losing_retry,
        winning,
    }
}
