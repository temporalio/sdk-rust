use std::{collections::HashMap, time::Duration};

use crate::runtime::types::ContinueAsNewRequest;
use temporalio_common_wasm::{
    Priority,
    data_converters::{
        GenericPayloadConverter, PayloadConverter, SerializationContext, SerializationContextData,
    },
    protos::{
        coresdk::{
            child_workflow::{ChildWorkflowCancellationType, ParentClosePolicy},
            common::VersioningIntent,
            nexus::NexusOperationCancellationType,
            workflow_activation::SignalWorkflow,
            workflow_commands::{
                ActivityCancellationType, ContinueAsNewWorkflowExecution, ScheduleActivity,
                ScheduleLocalActivity, ScheduleNexusOperation, StartChildWorkflowExecution,
                StartTimer, WorkflowCommand, workflow_command,
            },
        },
        temporal::api::{
            common::v1::{Payload, RetryPolicy, SearchAttributes},
            enums::v1::{ContinueAsNewVersioningBehavior, WorkflowIdReusePolicy},
            sdk::v1::UserMetadata,
        },
    },
};
/// Options for scheduling an activity
#[derive(Debug, bon::Builder, Clone)]
#[non_exhaustive]
#[builder(start_fn = with_close_timeouts, on(String, into), state_mod(vis = "pub"))]
pub struct ActivityOptions {
    /// Timeouts for activity completion.
    ///
    /// See [`ActivityCloseTimeouts`] for the meaning of each timeout variant.
    #[builder(start_fn)]
    pub close_timeouts: ActivityCloseTimeouts,
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Task queue to schedule the activity in
    ///
    /// If `None`, use the same task queue as the parent workflow.
    pub task_queue: Option<String>,
    /// Time that the Activity Task can stay in the Task Queue before it is picked up by a Worker.
    /// Do not specify this timeout unless using host specific Task Queues for Activity Tasks are
    /// being used for routing.
    /// `schedule_to_start_timeout` is always non-retryable.
    /// Retrying after this timeout doesn't make sense as it would just put the Activity Task back
    /// into the same Task Queue.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Heartbeat interval. Activity must heartbeat before this interval passes after a last
    /// heartbeat or activity start.
    pub heartbeat_timeout: Option<Duration>,
    /// Determines what the SDK does when the Activity is cancelled.
    #[builder(default)]
    pub cancellation_type: ActivityCancellationType,
    /// Activity retry policy
    pub retry_policy: Option<RetryPolicy>,
    /// Summary of the activity
    pub summary: Option<String>,
    /// Priority for the activity
    pub priority: Option<Priority>,
    /// If true, disable eager execution for this activity
    #[builder(default)]
    pub do_not_eagerly_execute: bool,
}

impl ActivityOptions {
    /// Returns a builder with `close_timeout` set to [`ActivityCloseTimeouts::StartToClose`].
    pub fn with_start_to_close_timeout(duration: Duration) -> ActivityOptionsBuilder {
        Self::with_close_timeouts(ActivityCloseTimeouts::StartToClose(duration))
    }

    /// Returns a builder with `close_timeout` set to [`ActivityCloseTimeouts::ScheduleToClose`].
    pub fn with_schedule_to_close_timeout(duration: Duration) -> ActivityOptionsBuilder {
        Self::with_close_timeouts(ActivityCloseTimeouts::ScheduleToClose(duration))
    }

    /// Creates activity options with only `start_to_close_timeout` set.
    ///
    /// If you need additional fields set, use [`Self::with_start_to_close_timeout`].
    pub fn start_to_close_timeout(duration: Duration) -> Self {
        Self::with_start_to_close_timeout(duration).build()
    }

    /// Creates activity options with only `schedule_to_close_timeout` set.
    ///
    /// If you need additional fields set, use [`Self::with_schedule_to_close_timeout`].
    pub fn schedule_to_close_timeout(duration: Duration) -> Self {
        Self::with_schedule_to_close_timeout(duration).build()
    }
}

/// The timeouts applied to an activity's completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityCloseTimeouts {
    /// Total time that a workflow is willing to wait for Activity to complete.
    /// `ActivityCloseTimeouts::ScheduleToClose` limits the total time of an Activity's execution
    /// including retries (use `ActivityCloseTimeouts::StartToClose` to limit the time of a single
    /// attempt).
    ScheduleToClose(Duration),
    /// Maximum time of a single Activity execution attempt. Note that the Temporal Server doesn't
    /// detect Worker process failures directly. It relies on this timeout to detect that an
    /// Activity that didn't complete on time. So this timeout should be as short as the longest
    /// possible execution of the Activity body. Potentially long running Activities must specify
    /// `ActivityOptions::heartbeat_timeout` and heartbeat from the activity periodically for timely
    /// failure detection.
    StartToClose(Duration),
    /// Applies both execution-attempt and overall-completion bounds.
    Both {
        /// Maximum time of a single Activity execution attempt.
        start_to_close: Duration,
        /// Total time that a workflow is willing to wait for Activity to complete.
        schedule_to_close: Duration,
    },
}

impl ActivityCloseTimeouts {
    fn into_durations(self) -> (Option<Duration>, Option<Duration>) {
        match self {
            Self::ScheduleToClose(schedule_to_close) => (None, Some(schedule_to_close)),
            Self::StartToClose(start_to_close) => (Some(start_to_close), None),
            Self::Both {
                start_to_close,
                schedule_to_close,
            } => (Some(start_to_close), Some(schedule_to_close)),
        }
    }
}

impl ActivityOptions {
    pub(crate) fn into_command(
        self,
        seq: u32,
        activity_type: String,
        args: Vec<Payload>,
    ) -> WorkflowCommand {
        let (start_to_close_timeout, schedule_to_close_timeout) =
            self.close_timeouts.into_durations();
        command_with_metadata(
            workflow_command::Variant::ScheduleActivity(ScheduleActivity {
                seq,
                activity_type,
                activity_id: self.activity_id.unwrap_or_else(|| seq.to_string()),
                task_queue: self.task_queue.unwrap_or_default(),
                arguments: args,
                schedule_to_close_timeout: schedule_to_close_timeout
                    .and_then(|duration| duration.try_into().ok()),
                schedule_to_start_timeout: self
                    .schedule_to_start_timeout
                    .and_then(|duration| duration.try_into().ok()),
                start_to_close_timeout: start_to_close_timeout
                    .and_then(|duration| duration.try_into().ok()),
                heartbeat_timeout: self
                    .heartbeat_timeout
                    .and_then(|duration| duration.try_into().ok()),
                cancellation_type: self.cancellation_type.into(),
                retry_policy: self.retry_policy,
                priority: self.priority.map(Into::into),
                do_not_eagerly_execute: self.do_not_eagerly_execute,
                ..Default::default()
            }),
            self.summary,
            None,
        )
    }
}

/// Options for scheduling a local activity
#[derive(Default, Debug, Clone)]
pub struct LocalActivityOptions {
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Retry policy
    pub retry_policy: RetryPolicy,
    /// Override attempt number rather than using 1.
    /// Ideally we would not expose this in a released Rust SDK, but it's needed for test.
    pub attempt: Option<u32>,
    /// Override schedule time when doing timer backoff.
    /// Ideally we would not expose this in a released Rust SDK, but it's needed for test.
    pub original_schedule_time: Option<prost_types::Timestamp>,
    /// Retry backoffs over this amount will use a timer rather than a local retry
    pub timer_backoff_threshold: Option<Duration>,
    /// How the activity will cancel
    pub cancel_type: ActivityCancellationType,
    /// Indicates how long the caller is willing to wait for local activity completion. Limits how
    /// long retries will be attempted. When not specified defaults to the workflow execution
    /// timeout (which may be unset).
    pub schedule_to_close_timeout: Option<Duration>,
    /// Limits time the local activity can idle internally before being executed. That can happen if
    /// the worker is currently at max concurrent local activity executions. This timeout is always
    /// non retryable as all a retry would achieve is to put it back into the same queue. Defaults
    /// to `schedule_to_close_timeout` if not specified and that is set. Must be <=
    /// `schedule_to_close_timeout` when set, if not, it will be clamped down.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum time the local activity is allowed to execute after the task is dispatched. This
    /// timeout is always retryable. Either or both of `schedule_to_close_timeout` and this must be
    /// specified. If set, this must be <= `schedule_to_close_timeout`, if not, it will be clamped
    /// down.
    pub start_to_close_timeout: Option<Duration>,
    /// Single-line summary for this activity that will appear in UI/CLI.
    pub summary: Option<String>,
}

impl LocalActivityOptions {
    pub(crate) fn into_command(
        mut self,
        seq: u32,
        activity_type: String,
        args: Vec<Payload>,
    ) -> WorkflowCommand {
        // Tests and some workflow code rely on the historical SDK behavior where omitted local
        // activity timeouts are normalized before the command is emitted.
        self.schedule_to_close_timeout
            .get_or_insert(Duration::from_secs(100));
        command_with_metadata(
            workflow_command::Variant::ScheduleLocalActivity(ScheduleLocalActivity {
                seq,
                activity_type,
                activity_id: self.activity_id.unwrap_or_else(|| seq.to_string()),
                arguments: args,
                retry_policy: Some(self.retry_policy),
                attempt: self.attempt.unwrap_or(1),
                original_schedule_time: self.original_schedule_time,
                local_retry_threshold: self
                    .timer_backoff_threshold
                    .and_then(|duration| duration.try_into().ok()),
                cancellation_type: self.cancel_type.into(),
                schedule_to_close_timeout: self
                    .schedule_to_close_timeout
                    .and_then(|duration| duration.try_into().ok()),
                schedule_to_start_timeout: self
                    .schedule_to_start_timeout
                    .and_then(|duration| duration.try_into().ok()),
                start_to_close_timeout: self
                    .start_to_close_timeout
                    .and_then(|duration| duration.try_into().ok()),
                ..Default::default()
            }),
            self.summary,
            None,
        )
    }
}

/// Options for scheduling a child workflow
#[derive(Default, Debug, Clone)]
pub struct ChildWorkflowOptions {
    /// Workflow ID
    pub workflow_id: String,
    /// Task queue to schedule the workflow in
    ///
    /// If `None`, use the same task queue as the parent workflow.
    pub task_queue: Option<String>,
    /// Cancellation strategy for the child workflow
    pub cancel_type: ChildWorkflowCancellationType,
    /// How to respond to parent workflow ending
    pub parent_close_policy: ParentClosePolicy,
    /// Static summary of the child workflow
    pub static_summary: Option<String>,
    /// Static details of the child workflow
    pub static_details: Option<String>,
    /// Set the policy for reusing the workflow id
    pub id_reuse_policy: WorkflowIdReusePolicy,
    /// Optionally set the execution timeout for the workflow
    pub execution_timeout: Option<Duration>,
    /// Optionally indicates the default run timeout for a workflow run
    pub run_timeout: Option<Duration>,
    /// Optionally indicates the default task timeout for a workflow run
    pub task_timeout: Option<Duration>,
    /// Optionally set a cron schedule for the workflow
    pub cron_schedule: Option<String>,
    /// Optionally associate extra search attributes with a workflow
    pub search_attributes: Option<HashMap<String, Payload>>,
    /// Priority for the workflow
    pub priority: Option<Priority>,
}

impl ChildWorkflowOptions {
    pub(crate) fn into_command(
        self,
        seq: u32,
        workflow_type: String,
        args: Vec<Payload>,
    ) -> WorkflowCommand {
        command_with_metadata(
            workflow_command::Variant::StartChildWorkflowExecution(StartChildWorkflowExecution {
                seq,
                workflow_type,
                workflow_id: self.workflow_id,
                task_queue: self.task_queue.unwrap_or_default(),
                input: args,
                cancellation_type: self.cancel_type.into(),
                parent_close_policy: self.parent_close_policy.into(),
                workflow_id_reuse_policy: match self.id_reuse_policy {
                    WorkflowIdReusePolicy::Unspecified => WorkflowIdReusePolicy::AllowDuplicate,
                    policy => policy,
                }
                .into(),
                workflow_execution_timeout: self
                    .execution_timeout
                    .and_then(|duration| duration.try_into().ok()),
                workflow_run_timeout: self
                    .run_timeout
                    .and_then(|duration| duration.try_into().ok()),
                workflow_task_timeout: self
                    .task_timeout
                    .and_then(|duration| duration.try_into().ok()),
                cron_schedule: self.cron_schedule.unwrap_or_default(),
                search_attributes: self.search_attributes.and_then(|attrs| {
                    (!attrs.is_empty()).then_some(SearchAttributes {
                        indexed_fields: attrs,
                    })
                }),
                priority: self.priority.map(Into::into),
                ..Default::default()
            }),
            self.static_summary,
            self.static_details,
        )
    }
}

/// Information needed to send a specific signal
#[derive(Debug)]
pub struct Signal {
    /// The signal name
    pub signal_name: String,
    /// The data the signal carries
    pub data: SignalData,
}

impl Signal {
    /// Create a new signal
    pub fn new(
        name: impl Into<String>,
        input: impl IntoIterator<Item = impl Into<Payload>>,
    ) -> Self {
        Self {
            signal_name: name.into(),
            data: SignalData::new(input),
        }
    }

    pub(crate) fn into_invocation(self) -> SignalWorkflow {
        SignalWorkflow {
            signal_name: self.signal_name,
            input: self.data.input,
            identity: String::new(),
            headers: self.data.headers,
        }
    }
}

/// Data contained within a signal
#[derive(Default, Debug)]
pub struct SignalData {
    /// The arguments the signal will receive
    pub input: Vec<Payload>,
    /// Metadata attached to the signal
    pub headers: HashMap<String, Payload>,
}

impl SignalData {
    /// Create data for a signal
    pub fn new(input: impl IntoIterator<Item = impl Into<Payload>>) -> Self {
        Self {
            input: input.into_iter().map(Into::into).collect(),
            headers: HashMap::new(),
        }
    }

    /// Set a header k/v pair attached to the signal
    pub fn with_header(
        &mut self,
        key: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> &mut Self {
        self.headers.insert(key.into(), payload.into());
        self
    }
}

/// Options for timer
#[derive(Default, Debug, Clone)]
pub struct TimerOptions {
    /// Duration for the timer
    pub duration: Duration,
    /// Summary of the timer
    pub summary: Option<String>,
}

impl From<Duration> for TimerOptions {
    fn from(duration: Duration) -> Self {
        TimerOptions {
            duration,
            ..Default::default()
        }
    }
}

impl TimerOptions {
    pub(crate) fn into_command(self, seq: u32) -> WorkflowCommand {
        command_with_metadata(
            workflow_command::Variant::StartTimer(StartTimer {
                seq,
                start_to_fire_timeout: Some(
                    self.duration
                        .try_into()
                        .expect("workflow timer timeout must fit into protobuf duration"),
                ),
            }),
            self.summary,
            None,
        )
    }
}

/// Options for Nexus Operations
#[derive(Default, Debug, Clone)]
pub struct NexusOperationOptions {
    /// Endpoint name, must exist in the endpoint registry or this command will fail.
    pub endpoint: String,
    /// Service name.
    pub service: String,
    /// Operation name.
    pub operation: String,
    /// Input for the operation. The server converts this into Nexus request content and the
    /// appropriate content headers internally when sending the StartOperation request. On the
    /// handler side, if it is also backed by Temporal, the content is transformed back to the
    /// original Payload sent in this command.
    pub input: Option<Payload>,
    /// Schedule-to-close timeout for this operation.
    /// Indicates how long the caller is willing to wait for operation completion.
    /// Calls are retried internally by the server.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Header to attach to the Nexus request.
    /// Users are responsible for encrypting sensitive data in this header as it is stored in
    /// workflow history and transmitted to external services as-is. This is useful for propagating
    /// tracing information. Note these headers are not the same as Temporal headers on internal
    /// activities and child workflows, these are transmitted to Nexus operations that may be
    /// external and are not traditional payloads.
    pub nexus_header: HashMap<String, String>,
    /// Cancellation type for the operation
    pub cancellation_type: Option<NexusOperationCancellationType>,
    /// Schedule-to-start timeout for this operation.
    /// Indicates how long the caller is willing to wait for the operation to be started (or completed if synchronous)
    /// by the handler. If the operation is not started within this timeout, it will fail with
    /// TIMEOUT_TYPE_SCHEDULE_TO_START.
    /// If not set or zero, no schedule-to-start timeout is enforced.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Start-to-close timeout for this operation.
    /// Indicates how long the caller is willing to wait for an asynchronous operation to complete after it has been
    /// started. If the operation does not complete within this timeout after starting, it will fail with
    /// TIMEOUT_TYPE_START_TO_CLOSE.
    /// Only applies to asynchronous operations. Synchronous operations ignore this timeout.
    /// If not set or zero, no start-to-close timeout is enforced.
    pub start_to_close_timeout: Option<Duration>,
}

impl NexusOperationOptions {
    pub(crate) fn into_command(self, seq: u32) -> WorkflowCommand {
        workflow_command::Variant::ScheduleNexusOperation(ScheduleNexusOperation {
            seq,
            endpoint: self.endpoint,
            service: self.service,
            operation: self.operation,
            input: self.input,
            schedule_to_close_timeout: self
                .schedule_to_close_timeout
                .and_then(|duration| duration.try_into().ok()),
            schedule_to_start_timeout: self
                .schedule_to_start_timeout
                .and_then(|duration| duration.try_into().ok()),
            start_to_close_timeout: self
                .start_to_close_timeout
                .and_then(|duration| duration.try_into().ok()),
            nexus_header: self.nexus_header,
            cancellation_type: self
                .cancellation_type
                .unwrap_or(NexusOperationCancellationType::WaitCancellationCompleted)
                .into(),
        })
        .into()
    }
}

/// Options for continuing a workflow as a new execution.
///
/// Unset fields inherit the current workflow's values where applicable.
#[derive(Default, Debug, bon::Builder)]
#[non_exhaustive]
pub struct ContinueAsNewOptions {
    /// Override the workflow type for the new execution. If `None`, reuses the current type.
    pub workflow_type: Option<String>,
    /// Task queue for the new execution. If `None`, reuses the current task queue.
    pub task_queue: Option<String>,
    /// Timeout for a single run of the new workflow.
    pub run_timeout: Option<Duration>,
    /// Timeout of a single workflow task.
    pub task_timeout: Option<Duration>,
    /// If set, the new workflow will have this memo. If `None`, reuses the current memo.
    pub memo: Option<HashMap<String, Payload>>,
    /// If set, the new workflow will have these headers.
    pub headers: Option<HashMap<String, Payload>>,
    /// If set, the new workflow will have these search attributes. If `None`, reuses the current
    /// search attributes.
    pub search_attributes: Option<SearchAttributes>,
    /// If set, the new workflow will have this retry policy. If `None`, reuses the current policy.
    pub retry_policy: Option<RetryPolicy>,
    /// Whether the new workflow should run on a worker with a compatible build id.
    pub versioning_intent: Option<VersioningIntent>,
}

impl ContinueAsNewOptions {
    pub(crate) fn into_request(
        self,
        workflow_type: String,
        arguments: Vec<Payload>,
    ) -> ContinueAsNewRequest {
        ContinueAsNewWorkflowExecution {
            workflow_type: self.workflow_type.unwrap_or(workflow_type),
            task_queue: self.task_queue.unwrap_or_default(),
            arguments,
            workflow_run_timeout: self
                .run_timeout
                .and_then(|duration| duration.try_into().ok()),
            workflow_task_timeout: self
                .task_timeout
                .and_then(|duration| duration.try_into().ok()),
            memo: self.memo.unwrap_or_default(),
            headers: self.headers.unwrap_or_default(),
            search_attributes: self.search_attributes,
            retry_policy: self.retry_policy,
            versioning_intent: self
                .versioning_intent
                .unwrap_or(VersioningIntent::Unspecified)
                .into(),
            initial_versioning_behavior: ContinueAsNewVersioningBehavior::Unspecified.into(),
        }
    }
}

fn command_with_metadata(
    variant: workflow_command::Variant,
    summary: Option<String>,
    details: Option<String>,
) -> WorkflowCommand {
    WorkflowCommand {
        variant: Some(variant),
        user_metadata: string_user_metadata(summary, details),
    }
}

fn string_user_metadata(summary: Option<String>, details: Option<String>) -> Option<UserMetadata> {
    if summary.is_none() && details.is_none() {
        return None;
    }
    let converter = PayloadConverter::default();
    let context = SerializationContext {
        data: &SerializationContextData::Workflow,
        converter: &converter,
    };
    Some(UserMetadata {
        summary: summary.map(|value| {
            converter
                .to_payload(&context, &value)
                .expect("String-to-JSON payload serialization is infallible")
        }),
        details: details.map(|value| {
            converter
                .to_payload(&context, &value)
                .expect("String-to-JSON payload serialization is infallible")
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_options_with_start_to_close_timeout_wrapper_supports_builder_chaining() {
        let opts = ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
            .heartbeat_timeout(Duration::from_secs(2))
            .build();

        assert_eq!(
            opts.close_timeouts,
            ActivityCloseTimeouts::StartToClose(Duration::from_secs(5))
        );
        assert_eq!(opts.heartbeat_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn activity_options_with_schedule_to_close_timeout_wrapper_supports_builder_chaining() {
        let opts = ActivityOptions::with_schedule_to_close_timeout(Duration::from_secs(5))
            .heartbeat_timeout(Duration::from_secs(2))
            .build();

        assert_eq!(
            opts.close_timeouts,
            ActivityCloseTimeouts::ScheduleToClose(Duration::from_secs(5))
        );
        assert_eq!(opts.heartbeat_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn activity_options_both_close_timeouts_map_to_command() {
        let req = ActivityOptions::with_close_timeouts(ActivityCloseTimeouts::Both {
            start_to_close: Duration::from_secs(3),
            schedule_to_close: Duration::from_secs(8),
        })
        .build()
        .into_command(7, "test".to_string(), vec![]);
        let Some(workflow_command::Variant::ScheduleActivity(req)) = req.variant else {
            panic!("expected ScheduleActivity command");
        };
        assert_eq!(req.start_to_close_timeout.unwrap().seconds, 3);
        assert_eq!(req.schedule_to_close_timeout.unwrap().seconds, 8);
    }

    #[test]
    fn child_workflow_run_timeout_uses_run_timeout_field() {
        let opts = ChildWorkflowOptions {
            workflow_id: "test-wf".to_string(),
            execution_timeout: Some(Duration::from_secs(60)),
            run_timeout: Some(Duration::from_secs(10)),
            ..Default::default()
        };
        let command = opts.into_command(1, "TestWorkflow".to_string(), vec![]);
        let Some(workflow_command::Variant::StartChildWorkflowExecution(req)) = command.variant
        else {
            panic!("expected StartChildWorkflowExecution command");
        };
        let exec_timeout = req.workflow_execution_timeout.unwrap();
        let run_timeout = req.workflow_run_timeout.unwrap();
        assert_eq!(exec_timeout.seconds, 60);
        assert_eq!(run_timeout.seconds, 10);
    }

    #[test]
    fn child_workflow_run_timeout_none_when_unset() {
        let opts = ChildWorkflowOptions {
            workflow_id: "test-wf".to_string(),
            execution_timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        let command = opts.into_command(1, "TestWorkflow".to_string(), vec![]);
        let Some(workflow_command::Variant::StartChildWorkflowExecution(req)) = command.variant
        else {
            panic!("expected StartChildWorkflowExecution command");
        };
        let exec_timeout = req.workflow_execution_timeout.unwrap();
        assert_eq!(exec_timeout.seconds, 60);
        assert!(req.workflow_run_timeout.is_none());
    }
}
