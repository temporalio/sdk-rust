use crate::common::{
    ActivationAssertionsInterceptor, CoreWfStarter, INTEG_CLIENT_IDENTITY,
    activity_functions::StdActivities, build_fake_sdk, eventually, init_core_and_create_wf,
    mock_sdk, mock_sdk_cfg,
};
use anyhow::anyhow;
use assert_matches::assert_matches;
use futures_util::FutureExt;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    ActivityIdentifier, UntypedWorkflow, WorkflowDescribeOptions, WorkflowStartOptions,
    WorkflowTerminateOptions,
};

use temporalio_common::{
    error::{ApplicationFailure, IncomingError},
    protos::{
        coresdk::{
            ActivityHeartbeat, ActivityTaskCompletion, AsJsonPayloadExt, IntoCompletion,
            IntoPayloadsExt,
            activity_result::{
                self, ActivityExecutionResult, ActivityResolution, activity_resolution as act_res,
            },
            activity_task::activity_task as act_task,
            workflow_activation::{
                FireTimer, ResolveActivity, WorkflowActivationJob, workflow_activation_job,
            },
            workflow_commands::{
                ActivityCancellationType, RequestCancelActivity, ScheduleActivity, StartTimer,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::{ActivityType, Payload, Payloads, RetryPolicy},
            enums::v1::{CommandType, EventType, RetryState, TimeoutType},
            failure::v1::{ActivityFailureInfo, Failure, failure::FailureInfo},
            sdk::v1::UserMetadata,
        },
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityExecutionError, ActivityOptions, CancellableFuture, LocalActivityOptions,
    WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::{
        ActivityInboundInterceptor, ExecuteActivityInput, ExecuteActivityOutput,
        ExecuteActivityResult, Next,
    },
};
use temporalio_sdk_core::{
    PollerBehavior, prost_dur,
    replay::{DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, ResponseType, WorkerTestHelpers, drain_pollers_and_shutdown,
        mock_worker_client, schedule_activity_cmd,
    },
};
use tokio::{join, sync::Semaphore, time::sleep};

#[workflow]
#[derive(Default)]
struct OneActivityWorkflow;

#[workflow_methods]
impl OneActivityWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        let r = ctx
            .start_activity(
                StdActivities::echo,
                input,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
        Ok(r)
    }
}

#[workflow]
#[derive(Default)]
struct OneLocalActivityWorkflow;

#[workflow_methods]
impl OneLocalActivityWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        let r = ctx
            .start_local_activity(StdActivities::echo, input, LocalActivityOptions::default())
            .await?;
        Ok(r)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ActivityInterceptorPhase {
    Before,
    After,
}

#[derive(Debug, PartialEq, Eq)]
struct ActivityInterceptorRecord {
    interceptor: &'static str,
    phase: ActivityInterceptorPhase,
    activity_type: String,
    workflow_type: String,
    is_local: bool,
    input: Option<String>,
    output: Option<String>,
    status: Option<&'static str>,
}

#[derive(Default)]
struct ActivityInterceptorRecords {
    records: Mutex<Vec<ActivityInterceptorRecord>>,
}

struct RecordingActivityInboundInterceptor {
    interceptor: &'static str,
    records: Arc<ActivityInterceptorRecords>,
}

impl ActivityInboundInterceptor for RecordingActivityInboundInterceptor {
    fn execute_activity<'a>(
        &'a self,
        input: ExecuteActivityInput,
        next: Next<'a, ExecuteActivityInput, ExecuteActivityOutput<'a>>,
    ) -> ExecuteActivityOutput<'a> {
        async move {
            let info = input.activity_info();
            let activity_type = info.activity_type.clone();
            let workflow_type = info.workflow_type.clone();
            let is_local = info.is_local;
            self.records
                .records
                .lock()
                .unwrap()
                .push(ActivityInterceptorRecord {
                    interceptor: self.interceptor,
                    phase: ActivityInterceptorPhase::Before,
                    activity_type: activity_type.clone(),
                    workflow_type: workflow_type.clone(),
                    is_local,
                    input: input.args_ref::<String>().cloned(),
                    output: None,
                    status: None,
                });
            let result = next.run(input).await;
            self.records
                .records
                .lock()
                .unwrap()
                .push(ActivityInterceptorRecord {
                    interceptor: self.interceptor,
                    phase: ActivityInterceptorPhase::After,
                    activity_type,
                    workflow_type,
                    is_local,
                    input: None,
                    output: result
                        .as_ref()
                        .ok()
                        .and_then(|output| output.downcast_ref::<String>())
                        .cloned(),
                    status: Some(activity_execution_result_status(&result)),
                });
            result
        }
        .boxed()
    }
}

fn activity_execution_result_status(result: &ExecuteActivityResult) -> &'static str {
    match result {
        Ok(_) => "completed",
        Err(ActivityError::Application(_)) => "failed",
        Err(ActivityError::Cancelled { .. }) => "cancelled",
        Err(ActivityError::WillCompleteAsync) => "will_complete_async",
    }
}

#[workflow]
#[derive(Default)]
struct MultiArgActivityWorkflow;

#[workflow_methods]
impl MultiArgActivityWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        let r = ctx
            .start_activity(
                StdActivities::concat,
                (input, " world".to_string()),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
        Ok(r)
    }
}

#[tokio::test]
async fn multi_arg_activity() {
    let wf_name = MultiArgActivityWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<MultiArgActivityWorkflow>();
    let mut worker = starter.worker().await;

    let input = "hello".to_string();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            MultiArgActivityWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let r = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(r, "hello world");
}

#[tokio::test]
async fn one_activity_only() {
    let wf_name = OneActivityWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<OneActivityWorkflow>();
    let mut worker = starter.worker().await;

    let input = "hello from input!".to_string();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            OneActivityWorkflow::run,
            input.clone(),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let r = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(r, input);
}

#[tokio::test]
async fn activity_interceptor_wraps_activity_execution() {
    let wf_name = OneActivityWorkflow::name();
    let mut starter = CoreWfStarter::new("activity_interceptor_wraps_activity_execution");
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<OneActivityWorkflow>();
    let mut worker = starter.worker().await;

    let records = Arc::new(ActivityInterceptorRecords::default());
    worker
        .inner_mut()
        .add_activity_inbound_interceptor(RecordingActivityInboundInterceptor {
            interceptor: "outer",
            records: records.clone(),
        });
    worker
        .inner_mut()
        .add_activity_inbound_interceptor(RecordingActivityInboundInterceptor {
            interceptor: "inner",
            records: records.clone(),
        });

    let input = "hello from input!".to_string();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            OneActivityWorkflow::run,
            input.clone(),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let r = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(r, input);

    assert_eq!(
        records.records.lock().unwrap().as_slice(),
        &[
            ActivityInterceptorRecord {
                interceptor: "outer",
                phase: ActivityInterceptorPhase::Before,
                activity_type: "StdActivities::echo".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: false,
                input: Some(input.clone()),
                output: None,
                status: None,
            },
            ActivityInterceptorRecord {
                interceptor: "inner",
                phase: ActivityInterceptorPhase::Before,
                activity_type: "StdActivities::echo".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: false,
                input: Some(input.clone()),
                output: None,
                status: None,
            },
            ActivityInterceptorRecord {
                interceptor: "inner",
                phase: ActivityInterceptorPhase::After,
                activity_type: "StdActivities::echo".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: false,
                input: None,
                output: Some(input.clone()),
                status: Some("completed"),
            },
            ActivityInterceptorRecord {
                interceptor: "outer",
                phase: ActivityInterceptorPhase::After,
                activity_type: "StdActivities::echo".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: false,
                input: None,
                output: Some(input.clone()),
                status: Some("completed"),
            },
        ]
    );
}

#[tokio::test]
async fn activity_interceptor_wraps_local_activity_execution() {
    let wf_name = OneLocalActivityWorkflow::name();
    let mut starter = CoreWfStarter::new("activity_interceptor_wraps_local_activity_execution");
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<OneLocalActivityWorkflow>();
    let mut worker = starter.worker().await;

    let records = Arc::new(ActivityInterceptorRecords::default());
    worker
        .inner_mut()
        .add_activity_inbound_interceptor(RecordingActivityInboundInterceptor {
            interceptor: "local",
            records: records.clone(),
        });

    let input = "hello from local input!".to_string();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            OneLocalActivityWorkflow::run,
            input.clone(),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let r = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(r, input);

    assert_eq!(
        records.records.lock().unwrap().as_slice(),
        &[
            ActivityInterceptorRecord {
                interceptor: "local",
                phase: ActivityInterceptorPhase::Before,
                activity_type: "StdActivities::echo".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: true,
                input: Some(input.clone()),
                output: None,
                status: None,
            },
            ActivityInterceptorRecord {
                interceptor: "local",
                phase: ActivityInterceptorPhase::After,
                activity_type: "StdActivities::echo".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: true,
                input: None,
                output: Some(input.clone()),
                status: Some("completed"),
            },
        ]
    );
}

struct MutatingActivityInboundInterceptor;

impl ActivityInboundInterceptor for MutatingActivityInboundInterceptor {
    fn execute_activity<'a>(
        &'a self,
        mut input: ExecuteActivityInput,
        next: Next<'a, ExecuteActivityInput, ExecuteActivityOutput<'a>>,
    ) -> ExecuteActivityOutput<'a> {
        if let Some(input) = input.args_mut::<String>() {
            input.push_str(" mutated");
        }
        next.run(input)
    }
}

#[tokio::test]
async fn activity_inbound_interceptor_can_mutate_activity_input() {
    let mut starter = CoreWfStarter::new("activity_inbound_interceptor_can_mutate_activity_input");
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<OneActivityWorkflow>();
    let mut worker = starter.worker().await;

    worker
        .inner_mut()
        .add_activity_inbound_interceptor(MutatingActivityInboundInterceptor);

    let input = "hello from input!".to_string();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            OneActivityWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let r = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(r, "hello from input! mutated");
}

#[tokio::test]
async fn activity_interceptor_observes_activity_error() {
    struct FailingActivities;

    #[activities]
    impl FailingActivities {
        #[activity]
        async fn fail(_ctx: ActivityContext, input: String) -> Result<String, ActivityError> {
            Err(anyhow!("failed input: {input}").into())
        }
    }

    #[workflow]
    #[derive(Default)]
    struct ActivityFailureWorkflow;

    #[workflow_methods]
    impl ActivityFailureWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<()> {
            let result: Result<String, ActivityExecutionError> = ctx
                .start_activity(
                    FailingActivities::fail,
                    input,
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                        .retry_policy(RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        })
                        .build(),
                )
                .await;
            assert!(result.is_err());
            Ok(())
        }
    }

    let wf_name = ActivityFailureWorkflow::name();
    let mut starter = CoreWfStarter::new("activity_interceptor_observes_activity_error");
    starter.sdk_config.register_activities(FailingActivities);
    starter
        .sdk_config
        .register_workflow::<ActivityFailureWorkflow>();
    let mut worker = starter.worker().await;

    let records = Arc::new(ActivityInterceptorRecords::default());
    worker
        .inner_mut()
        .add_activity_inbound_interceptor(RecordingActivityInboundInterceptor {
            interceptor: "failure",
            records: records.clone(),
        });

    let input = "bad input".to_string();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ActivityFailureWorkflow::run,
            input.clone(),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();

    assert_eq!(
        records.records.lock().unwrap().as_slice(),
        &[
            ActivityInterceptorRecord {
                interceptor: "failure",
                phase: ActivityInterceptorPhase::Before,
                activity_type: "FailingActivities::fail".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: false,
                input: Some(input),
                output: None,
                status: None,
            },
            ActivityInterceptorRecord {
                interceptor: "failure",
                phase: ActivityInterceptorPhase::After,
                activity_type: "FailingActivities::fail".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: false,
                input: None,
                output: None,
                status: Some("failed"),
            },
        ]
    );
}

#[tokio::test]
async fn activity_interceptor_observes_activity_panic() {
    struct PanickingActivities;

    #[activities]
    impl PanickingActivities {
        #[activity]
        async fn panic_activity(
            _ctx: ActivityContext,
            input: String,
        ) -> Result<String, ActivityError> {
            panic!("panic input: {input}");
        }
    }

    #[workflow]
    #[derive(Default)]
    struct ActivityPanicWorkflow;

    #[workflow_methods]
    impl ActivityPanicWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<()> {
            let result: Result<String, ActivityExecutionError> = ctx
                .start_activity(
                    PanickingActivities::panic_activity,
                    input,
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                        .retry_policy(RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        })
                        .build(),
                )
                .await;
            assert!(result.is_err());
            Ok(())
        }
    }

    let wf_name = ActivityPanicWorkflow::name();
    let mut starter = CoreWfStarter::new("activity_interceptor_observes_activity_panic");
    starter.sdk_config.register_activities(PanickingActivities);
    starter
        .sdk_config
        .register_workflow::<ActivityPanicWorkflow>();
    let mut worker = starter.worker().await;

    let records = Arc::new(ActivityInterceptorRecords::default());
    worker
        .inner_mut()
        .add_activity_inbound_interceptor(RecordingActivityInboundInterceptor {
            interceptor: "panic",
            records: records.clone(),
        });

    let input = "panic input".to_string();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ActivityPanicWorkflow::run,
            input.clone(),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();

    assert_eq!(
        records.records.lock().unwrap().as_slice(),
        &[
            ActivityInterceptorRecord {
                interceptor: "panic",
                phase: ActivityInterceptorPhase::Before,
                activity_type: "PanickingActivities::panic_activity".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: false,
                input: Some(input),
                output: None,
                status: None,
            },
            ActivityInterceptorRecord {
                interceptor: "panic",
                phase: ActivityInterceptorPhase::After,
                activity_type: "PanickingActivities::panic_activity".to_owned(),
                workflow_type: wf_name.to_owned(),
                is_local: false,
                input: None,
                output: None,
                status: Some("failed"),
            },
        ]
    );
}

#[tokio::test]
async fn activity_panics_are_retryable() {
    struct PanicOnceActivities;

    #[activities]
    impl PanicOnceActivities {
        #[activity]
        async fn panic_once(self: Arc<Self>, ctx: ActivityContext) -> Result<u32, ActivityError> {
            let _ = self;
            if ctx.info().attempt == 1 {
                panic!("panic once");
            }
            Ok(ctx.info().attempt)
        }
    }

    #[workflow]
    #[derive(Default)]
    struct ActivityPanicRetryWorkflow;

    #[workflow_methods]
    impl ActivityPanicRetryWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<u32> {
            let result = ctx
                .start_activity(
                    PanicOnceActivities::panic_once,
                    (),
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                        .retry_policy(RetryPolicy {
                            maximum_attempts: 2,
                            ..Default::default()
                        })
                        .build(),
                )
                .await?;
            Ok(result)
        }
    }

    let wf_name = ActivityPanicRetryWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(PanicOnceActivities);
    starter
        .sdk_config
        .register_workflow::<ActivityPanicRetryWorkflow>();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ActivityPanicRetryWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    assert_eq!(handle.get_result(Default::default()).await.unwrap(), 2);
}

#[tokio::test]
async fn activity_workflow() {
    let mut starter = init_core_and_create_wf("activity_workflow").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(
        schedule_activity_cmd(
            0,
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, DEFAULT_ACTIVITY_TYPE.to_string());
            // Workflow-scheduled activities have no activity run ID
            assert!(start_activity.run_id.is_empty());
        }
    );
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
        external_payloads: Default::default(),
    };
    // Complete activity successfully.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::ok(response_payload.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has succeeded.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution{
                      status: Some(
                        act_res::Status::Completed(activity_result::Success{result: Some(r)})),
                        ..
                    }), ..}
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_non_retryable_failure() {
    let mut starter = init_core_and_create_wf("activity_non_retryable_failure").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(
        schedule_activity_cmd(
            0,
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Fail activity with non-retryable error
    let failure = Failure::application_failure("activity failed".to_string(), true);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::fail(failure.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution{
                    status: Some(act_res::Status::Failed(activity_result::Failure{
                        failure: Some(f),
                    }))}),..}
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
            assert_eq!(f, &Failure{
                message: "Activity task failed".to_owned(),
                cause: Some(Box::new(failure)),
                failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo{
                    activity_id: "act-1".to_owned(),
                    activity_type: Some(ActivityType {
                        name: DEFAULT_ACTIVITY_TYPE.to_owned(),
                    }),
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    identity: INTEG_CLIENT_IDENTITY.to_owned(),
                    retry_state: RetryState::NonRetryableFailure as i32,
                })),
                ..Default::default()
            });
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_non_retryable_failure_with_error() {
    let mut starter = init_core_and_create_wf("activity_non_retryable_failure").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(
        schedule_activity_cmd(
            0,
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Fail activity with non-retryable error
    let failure = Failure::application_failure_from_error(anyhow!("activity failed"), true);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::fail(failure.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution{
                    status: Some(act_res::Status::Failed(activity_result::Failure{
                        failure: Some(f),
                    }))}),..}
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
            assert_eq!(f, &Failure{
                message: "Activity task failed".to_owned(),
                cause: Some(Box::new(failure)),
                failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo{
                    activity_id: "act-1".to_owned(),
                    activity_type: Some(ActivityType {
                        name: DEFAULT_ACTIVITY_TYPE.to_owned(),
                    }),
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    identity: INTEG_CLIENT_IDENTITY.to_owned(),
                    retry_state: RetryState::NonRetryableFailure as i32,
                })),
                ..Default::default()
            });
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn workflow_observes_non_retryable_activity() {
    let mut starter = CoreWfStarter::new("workflow-observes-non-retryable-activity-failure");
    starter
        .sdk_config
        .register_activities(NonRetryableActivityErrorActivities);
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct NonRetryableActivityFailureWorkflow;

    #[workflow_methods]
    impl NonRetryableActivityFailureWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let err = ctx
                .start_activity(
                    NonRetryableActivityErrorActivities::fail_non_retryable,
                    (),
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                        .activity_id("non-retryable-act".to_owned())
                        .retry_policy(RetryPolicy {
                            maximum_attempts: 3,
                            ..Default::default()
                        })
                        .build(),
                )
                .await
                .unwrap_err();

            let ActivityExecutionError::Failed(fail_err) = err else {
                panic!("expected activity failure, got {err:?}");
            };
            assert_eq!(fail_err.activity_id(), "non-retryable-act");
            assert_eq!(
                fail_err
                    .activity_type()
                    .map(|activity_type| activity_type.name.as_str()),
                Some(NonRetryableActivityErrorActivities::fail_non_retryable.name())
            );
            assert_eq!(fail_err.retry_state(), RetryState::NonRetryableFailure);

            let Some(IncomingError::Application(app_err)) = fail_err.cause() else {
                panic!("expected application failure cause, got {fail_err:?}");
            };
            assert!(app_err.is_non_retryable());
            assert_eq!(app_err.to_string(), "non-retryable activity failure");
            assert!(app_err.failure().is_some());
            Ok(())
        }
    }

    struct NonRetryableActivityErrorActivities;

    #[activities]
    impl NonRetryableActivityErrorActivities {
        #[activity]
        async fn fail_non_retryable(_ctx: ActivityContext) -> Result<(), ActivityError> {
            Err(ActivityError::application(
                ApplicationFailure::non_retryable(anyhow!("non-retryable activity failure")),
            ))
        }
    }

    worker.register_workflow::<NonRetryableActivityFailureWorkflow>();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            NonRetryableActivityFailureWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn activity_retry() {
    let mut starter = init_core_and_create_wf("activity_retry").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(
        schedule_activity_cmd(
            0,
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity 1st time
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Fail activity with retryable error
    let failure = Failure::application_failure("activity failed".to_string(), false);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::fail(failure)),
    })
    .await
    .unwrap();
    // Poll 2nd time
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Complete activity successfully
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
        external_payloads: Default::default(),
    };
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::ok(response_payload.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify activity has succeeded.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution{
                    status: Some(act_res::Status::Completed(activity_result::Success{result: Some(r)}))}),..}
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_try_cancel() {
    let mut starter = init_core_and_create_wf("activity_cancellation_try_cancel").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is try-cancelled.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(
                    FireTimer { seq }
                )),
            },
        ] => {
            assert_eq!(*seq, 1);
        }
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_plus_complete_doesnt_double_resolve() {
    let mut starter =
        init_core_and_create_wf("activity_cancellation_plus_complete_doesnt_double_resolve").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    // Should get cancel task
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(
                ResolveActivity {
                    result: Some(ActivityResolution {
                        status: Some(act_res::Status::Cancelled(_))
                    }),
                    ..
                }
            )),
        }]
    );
    // We need to complete the wf task to send the activity cancel command to the server, so start
    // another short timer
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 2,
                start_to_fire_timeout: Some(prost_dur!(from_millis(100))),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    // Now say the activity completes anyways
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
        result: Some(ActivityExecutionResult::ok([1].into())),
    })
    .await
    .unwrap();
    // Ensure we do not get a wakeup with the activity being resolved completed, and instead get
    // the timer fired event (also wait for timer to fire)
    sleep(Duration::from_secs(1)).await;
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn started_activity_timeout() {
    let mut starter = init_core_and_create_wf("started_activity_timeout").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity that times out in 1 second.
    core.complete_workflow_activation(
        schedule_activity_cmd(
            0,
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is timed out after 1 second.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        seq,
                        result: Some(ActivityResolution{
                            status: Some(
                                act_res::Status::Failed(
                                    activity_result::Failure{failure: Some(_)}
                                )
                            ),
                            ..
                        }), ..
                    }
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_wait_cancellation_completed() {
    let mut starter =
        init_core_and_create_wf("activity_cancellation_wait_cancellation_completed").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::WaitCancellationCompleted,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is wait-cancelled.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(
                    FireTimer { seq }
                )),
            },
        ] => {
            assert_eq!(*seq, 1);
        }
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_abandon() {
    let mut starter = init_core_and_create_wf("activity_cancellation_abandon").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::Abandon,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is abandoned.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(
                    FireTimer { seq }
                )),
            },
        ] => {
            assert_eq!(*seq, 1);
        }
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();
    // Poll workflow task expecting that activation has been created by the state machine
    // immediately after the cancellation request.
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn async_activity_completion_workflow() {
    let mut starter = init_core_and_create_wf("async_activity_workflow").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(
        schedule_activity_cmd(
            0,
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
        external_payloads: Default::default(),
    };
    // Complete activity asynchronously.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token.clone(),
        result: Some(ActivityExecutionResult::will_complete_async()),
    })
    .await
    .unwrap();
    starter
        .get_client()
        .await
        .get_async_activity_handle(ActivityIdentifier::TaskToken(task.task_token.into()))
        .complete(Some(Payloads {
            payloads: vec![response_payload.clone()],
        }))
        .await
        .unwrap();

    // Poll workflow task and verify that activity has succeeded.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution {
                    status: Some(act_res::Status::Completed(activity_result::Success{result: Some(r)})),
                     ..}), ..}
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancelled_after_heartbeat_times_out() {
    let mut starter = init_core_and_create_wf("activity_cancelled_after_heartbeat_times_out").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue().to_string();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleActivity {
            seq: 0,
            activity_id: activity_id.to_string(),
            activity_type: "dontcare".to_string(),
            task_queue: task_q.clone(),
            schedule_to_close_timeout: Some(prost_dur!(from_secs(10))),
            heartbeat_timeout: Some(prost_dur!(from_secs(1))),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 2,
                initial_interval: Some(prost_dur!(from_secs(5))),
                ..Default::default()
            }),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Delay the heartbeat
    sleep(Duration::from_secs(2)).await;
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: task.task_token.clone(),
        details: vec![],
    });

    // Verify activity got cancelled
    let cancel_task = core.poll_activity_task().await.unwrap();
    assert_matches!(cancel_task.variant, Some(act_task::Variant::Cancel(_)));
    assert_eq!(cancel_task.task_token, task.task_token.clone());

    // Complete activity with cancelled result
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token.clone(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();

    // Verify shutdown completes
    drain_pollers_and_shutdown(&core).await;
    // Cleanup just in case
    starter
        .get_client()
        .await
        .get_workflow_handle::<UntypedWorkflow>(task_q)
        .terminate(WorkflowTerminateOptions::default())
        .await
        .unwrap();
}

#[ignore] // Currently skipped because of https://github.com/temporalio/temporal/issues/8376
#[tokio::test]
async fn activity_heartbeat_not_flushed_on_success() {
    let mut starter = init_core_and_create_wf("activity_heartbeat_not_flushed_on_success").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue().to_string();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleActivity {
            seq: 0,
            activity_id: activity_id.to_string(),
            activity_type: "dontcare".to_string(),
            task_queue: task_q.clone(),
            schedule_to_close_timeout: Some(prost_dur!(from_secs(60))),
            heartbeat_timeout: Some(prost_dur!(from_secs(10))),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 2,
                initial_interval: Some(prost_dur!(from_secs(5))),
                ..Default::default()
            }),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // heartbeat 1 (will send immediately)
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: task.task_token.clone(),
        details: vec!["one".into()],
    });
    // heartbeat 2 (would be throttled if not flushed)
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: task.task_token.clone(),
        details: vec!["two".into()],
    });
    // Complete activity with fail
    let failure = Failure::application_failure("activity failed".to_string(), false);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::fail(failure)),
    })
    .await
    .unwrap();
    // The activity is still in the pending state since it has retries left
    let client = starter.get_client().await;
    eventually(
        || async {
            // Verify pending details has the flushed heartbeat
            let details = client
                .get_workflow_handle::<UntypedWorkflow>(starter.get_wf_id().to_string())
                .describe(WorkflowDescribeOptions::default())
                .await
                .unwrap();
            let last_deets = details
                .raw_description
                .pending_activities
                .into_iter()
                .find(|i| i.activity_id == activity_id)
                .and_then(|i| i.heartbeat_details);
            if last_deets == ["two".into()].into_payloads() {
                Ok(())
            } else {
                Err("details don't yet match")
            }
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    client
        .get_workflow_handle::<UntypedWorkflow>(task_q)
        .terminate(WorkflowTerminateOptions::default())
        .await
        .unwrap();
    drain_pollers_and_shutdown(&core).await;
}

#[workflow]
#[derive(Default)]
struct OneActivityAbandonCancelledBeforeStarted;

#[workflow_methods]
impl OneActivityAbandonCancelledBeforeStarted {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let act_fut = ctx.start_activity(
            StdActivities::delay,
            Duration::from_secs(2),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .cancellation_type(ActivityCancellationType::Abandon)
                .build(),
        );
        act_fut.cancel();
        let _ = act_fut.await;
        Ok(())
    }
}

#[tokio::test]
async fn one_activity_abandon_cancelled_before_started() {
    let wf_name = "one_activity_abandon_cancelled_before_started";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<OneActivityAbandonCancelledBeforeStarted>();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            OneActivityAbandonCancelledBeforeStarted::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}

#[workflow]
#[derive(Default)]
struct OneActivityAbandonCancelledAfterComplete;

#[workflow_methods]
impl OneActivityAbandonCancelledAfterComplete {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let act_fut = ctx.start_activity(
            StdActivities::delay,
            Duration::from_secs(2),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .cancellation_type(ActivityCancellationType::Abandon)
                .build(),
        );
        ctx.timer(Duration::from_secs(1)).await;
        act_fut.cancel();
        ctx.timer(Duration::from_secs(3)).await;
        let _ = act_fut.await;
        Ok(())
    }
}

#[tokio::test]
async fn one_activity_abandon_cancelled_after_complete() {
    let wf_name = "one_activity_abandon_cancelled_after_complete";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<OneActivityAbandonCancelledAfterComplete>();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            OneActivityAbandonCancelledAfterComplete::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown() {
    let wf_name = "graceful_shutdown";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.graceful_shutdown_period = Some(Duration::from_millis(500));

    let acts_started = Arc::new(Semaphore::const_new(0));
    let acts_done = Arc::new(Semaphore::const_new(0));

    struct SleeperActivities {
        acts_started: Arc<Semaphore>,
        acts_done: Arc<Semaphore>,
    }
    #[activities]
    impl SleeperActivities {
        #[activity]
        async fn sleeper(
            self: Arc<Self>,
            ctx: ActivityContext,
            _: String,
        ) -> Result<(), ActivityError> {
            self.acts_started.add_permits(1);
            // just wait to be cancelled
            ctx.cancelled().await;
            self.acts_done.add_permits(1);
            Err(ActivityError::cancelled())
        }
    }

    starter.sdk_config.register_activities(SleeperActivities {
        acts_started: acts_started.clone(),
        acts_done: acts_done.clone(),
    });
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;

    #[workflow]
    #[derive(Default)]
    struct GracefulShutdownWorkflow;

    #[workflow_methods]
    impl GracefulShutdownWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let act_futs = (1..=10).map(|_| {
                ctx.start_activity(
                    SleeperActivities::sleeper,
                    "hi".to_string(),
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                        .retry_policy(RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        })
                        .cancellation_type(ActivityCancellationType::WaitCancellationCompleted)
                        .build(),
                )
            });
            temporalio_sdk::workflows::join_all(act_futs).await;
            Ok(())
        }
    }

    worker.register_workflow::<GracefulShutdownWorkflow>();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            GracefulShutdownWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();

    let handle = worker.inner_mut().shutdown_handle();
    let shutdowner = async {
        // Wait for all acts to be started before initiating shutdown
        let _ = acts_started.acquire_many(10).await;
        handle();
        // Kill workflow once all acts are cancelled. This also ensures we actually see all the
        // cancels, otherwise run_until_done will hang since the workflow won't complete.
        let _ = acts_done.acquire_many(10).await;
        client
            .get_workflow_handle::<UntypedWorkflow>(wf_name.to_owned())
            .terminate(WorkflowTerminateOptions::default())
            .await
            .unwrap();
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    join!(shutdowner, runner);
}

#[tokio::test]
async fn activity_can_be_cancelled_by_local_timeout() {
    let wf_name = "activity_can_be_cancelled_by_local_timeout";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .set_core_cfg_mutator(|m| m.local_timeout_buffer_for_activities = Duration::from_secs(0));

    let was_cancelled = Arc::new(AtomicBool::new(false));

    struct CancellableEchoActivities {
        was_cancelled: Arc<AtomicBool>,
    }
    #[activities]
    impl CancellableEchoActivities {
        #[activity]
        async fn cancellable_echo(
            self: Arc<Self>,
            ctx: ActivityContext,
            echo_me: String,
        ) -> Result<String, ActivityError> {
            // Doesn't heartbeat
            ctx.cancelled().await;
            self.was_cancelled.store(true, Ordering::Relaxed);
            Ok(echo_me)
        }
    }

    starter
        .sdk_config
        .register_activities(CancellableEchoActivities {
            was_cancelled: was_cancelled.clone(),
        });
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct ActivityLocalTimeoutWorkflow;

    #[workflow_methods]
    impl ActivityLocalTimeoutWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let res = ctx
                .start_activity(
                    CancellableEchoActivities::cancellable_echo,
                    "hi!".to_string(),
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(1))
                        .retry_policy(RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        })
                        .build(),
                )
                .await;
            let err = res.unwrap_err();
            let ActivityExecutionError::Failed(fail_err) = err else {
                panic!("expected activity failure, got {err:?}");
            };
            assert_eq!(fail_err.retry_state(), RetryState::MaximumAttemptsReached);
            let Some(timeout) = fail_err.as_timeout() else {
                panic!("expected timeout cause, got {fail_err:?}");
            };
            assert_eq!(timeout.timeout_type(), TimeoutType::StartToClose);
            assert_eq!(
                fail_err
                    .activity_type()
                    .map(|activity_type| activity_type.name.as_str()),
                Some(CancellableEchoActivities::cancellable_echo.name())
            );
            Ok(())
        }
    }

    worker.register_workflow::<ActivityLocalTimeoutWorkflow>();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            ActivityLocalTimeoutWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert!(was_cancelled.load(Ordering::Relaxed));
}

#[tokio::test]
#[ignore] // Runs forever, used to manually attempt to repro spurious activity completion rpc errs
// Unfortunately there is no way to unit test this as tonic doesn't publicly expose the necessary
// machinery to construct the right kind of error.
async fn long_activity_timeout_repro() {
    let wf_name = "long_activity_timeout_repro";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 10,
        initial: 5,
    };
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 10,
        initial: 5,
    };
    starter
        .set_core_cfg_mutator(|m| m.local_timeout_buffer_for_activities = Duration::from_secs(0));
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct LongActivityTimeoutReproWorkflow;

    #[workflow_methods]
    impl LongActivityTimeoutReproWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let mut iter = 1;
            loop {
                let res = ctx
                    .start_activity(
                        StdActivities::echo,
                        "hi!".to_string(),
                        ActivityOptions::with_start_to_close_timeout(Duration::from_secs(1))
                            .retry_policy(RetryPolicy {
                                maximum_attempts: 1,
                                ..Default::default()
                            })
                            .build(),
                    )
                    .await;
                assert!(res.is_ok());
                ctx.timer(Duration::from_secs(60 * 3)).await;
                iter += 1;
                if iter > 5000 {
                    ctx.continue_as_new(&(), Default::default())?;
                }
            }
        }
    }

    worker.register_workflow::<LongActivityTimeoutReproWorkflow>();

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn pass_activity_summary_to_metadata() {
    let t = canned_histories::single_activity("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let expected_user_metadata = Some(UserMetadata {
        summary: Some("activity summary".as_json_payload().unwrap()),
        details: None,
    });
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::ScheduleActivityTask
                );
                assert_eq!(wft.commands[0].user_metadata, expected_user_metadata)
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = mock_sdk_cfg(mock_cfg, |_| {});

    #[workflow]
    #[derive(Default)]
    struct ActivitySummaryWorkflow;

    #[workflow_methods]
    impl ActivitySummaryWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.start_activity(
                StdActivities::default,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .summary("activity summary".to_string())
                    .build(),
            )
            .await?;
            Ok(())
        }
    }

    worker.register_workflow::<ActivitySummaryWorkflow>();
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
#[tokio::test]
async fn abandoned_activities_ignore_start_and_complete(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let activity_id = "1";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let act_scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    let act_started_event_id = t.add_activity_task_started(act_scheduled_event_id);
    t.add_activity_task_completed(
        act_scheduled_event_id,
        act_started_event_id,
        Default::default(),
    );
    t.add_full_wf_task();
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let mock = mock_worker_client();
    let mut worker = mock_sdk(MockPollCfg::from_resp_batches(wfid, t, hist_batches, mock));

    #[workflow]
    #[derive(Default)]
    struct AbandonedActivitiesWorkflow;

    #[workflow_methods]
    impl AbandonedActivitiesWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let act_fut = ctx.start_activity(
                StdActivities::default,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .cancellation_type(ActivityCancellationType::Abandon)
                    .build(),
            );
            ctx.timer(Duration::from_secs(1)).await;
            act_fut.cancel();
            ctx.timer(Duration::from_secs(3)).await;
            let err = act_fut.await.unwrap_err();
            let ActivityExecutionError::Cancelled(cancel_err) = err else {
                panic!("expected cancelled error, got {err:?}");
            };
            assert!(cancel_err.raw_details().is_none());
            assert!(
                cancel_err.cause().is_none(),
                "expected cancel to be end of cause chain, but found another: {:?}",
                cancel_err.cause()
            );
            Ok(())
        }
    }

    worker.register_workflow::<AbandonedActivitiesWorkflow>();
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type,
            vec![],
            WorkflowStartOptions::new(task_queue, wfid).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct ImmediateActivityCancelationWorkflow;

#[workflow_methods]
impl ImmediateActivityCancelationWorkflow {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let cancel_activity_future = ctx.start_activity(
            StdActivities::default,
            (),
            ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
        );
        cancel_activity_future.cancel();
        let err = cancel_activity_future.await.unwrap_err();
        let ActivityExecutionError::Cancelled(cancel_err) = err else {
            panic!("expected cancelled error, got {err:?}");
        };
        assert!(cancel_err.raw_details().is_none());
        assert!(cancel_err.cause().is_none());
        Ok(())
    }
}

#[tokio::test]
async fn immediate_activity_cancelation() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    worker.register_workflow::<ImmediateActivityCancelationWorkflow>();

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.then(|a| {
        assert_matches!(
            a.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
            }]
        )
    });
    aai.then(|a| {
        assert_matches!(
            a.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(act_res::Status::Cancelled(_))
                        }),
                        ..
                    }
                )),
            },]
        )
    });

    worker.set_worker_interceptor(aai);
    worker.run().await.unwrap();
}
