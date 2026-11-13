use crate::common::{
    ActivationAssertionsInterceptor, CoreWfStarter, FailOnNondeterminismInterceptor,
    WorkflowHandleExt, activity_functions::StdActivities, history_from_proto_binary,
    init_core_replay_preloaded, workflows::LaProblemWorkflow,
};
use anyhow::anyhow;
use crossbeam_queue::SegQueue;
use futures_util::{FutureExt, StreamExt, stream};
use rstest::Context;
use std::{
    collections::HashMap,
    ops::Sub,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use temporalio_client::{
    NamespacedClient, WorkflowExecuteUpdateOptions, WorkflowExecutionInfo, WorkflowStartOptions,
};
use temporalio_common::{
    data_converters::RawValue,
    protos::{
        coresdk::{
            ActivityTaskCompletion, AsJsonPayloadExt, FromJsonPayloadExt,
            activity_result::ActivityExecutionResult,
            activity_task::{ActivityTask, activity_task as act_task},
            common::{extract_local_activity_marker_data, extract_local_activity_marker_details},
            workflow_activation::{
                WorkflowActivation, WorkflowActivationJob, remove_from_cache::EvictionReason,
                workflow_activation_job,
            },
            workflow_commands::{
                ActivityCancellationType as ProtoActivityCancellationType,
                CompleteWorkflowExecution, RequestCancelLocalActivity, ScheduleLocalActivity,
                workflow_command::Variant,
            },
            workflow_completion::{
                self, WorkflowActivationCompletion, workflow_activation_completion,
            },
        },
        temporal::api::{
            command::v1::{RecordMarkerCommandAttributes, command},
            common::v1::{Payload, RetryPolicy},
            enums::v1::{
                CommandType, EventType, TimeoutType as ProtoTimeoutType, WorkflowTaskFailedCause,
            },
            failure::v1::{Failure, failure::FailureInfo},
            history::{
                self,
                v1::{
                    History,
                    history_event::{self, Attributes::MarkerRecordedEventAttributes},
                },
            },
            query::v1::WorkflowQuery,
            workflowservice::v1::{
                PollActivityTaskQueueResponse, PollNexusTaskQueueResponse,
                PollWorkflowTaskQueueResponse,
            },
        },
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityCancellationType, ActivityExecutionError, ActivityOptions, ApplicationFailure,
    CancellableFuture, LocalActivityOptions, TimeoutType, Worker, WorkflowContext,
    WorkflowContextView, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::WorkerInterceptor,
};
use temporalio_sdk_core::{
    PollError, TunerHolder, WorkflowTaskCompletion, prost_dur,
    replay::{
        DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE, HistoryForReplay, TestHistoryBuilder,
        canned_histories, default_wes_attribs,
    },
    test_help::{
        LEGACY_QUERY_ID, MockPollCfg, MocksHolder, QueueResponse, ResponseType, WorkerExt,
        WorkerTestHelpers, build_mock_pollers, drain_pollers_and_shutdown, hist_to_poll_resp,
        mock_worker, mock_worker_client, query_ok, schedule_activity_cmd,
        schedule_local_activity_cmd, single_hist_mock_sg, start_timer_cmd,
    },
};
use tokio::{
    join, select,
    sync::{Barrier, Notify, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

#[workflow]
#[derive(Default)]
pub(crate) struct OneLocalActivityWf;

#[workflow_methods]
impl OneLocalActivityWf {
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let initial_workflow_time = ctx.workflow_time().expect("Workflow time should be set");
        ctx.execute_local_activity(
            StdActivities::echo,
            "hi!".to_string(),
            LocalActivityOptions::default(),
        )
        .await?;
        assert!(initial_workflow_time < ctx.workflow_time().unwrap());
        Ok(())
    }
}

#[tokio::test]
async fn one_local_activity() {
    let wf_name = "one_local_activity";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<OneLocalActivityWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            OneLocalActivityWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}

#[workflow]
#[derive(Default)]
pub(crate) struct LocalActConcurrentWithTimerWf;

#[workflow_methods]
impl LocalActConcurrentWithTimerWf {
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let la = ctx.execute_local_activity(
            StdActivities::echo,
            "hi!".to_string(),
            LocalActivityOptions::default(),
        );
        let timer = ctx.timer(Duration::from_secs(1));
        let _ = temporalio_sdk::workflows::join!(la, timer);
        Ok(())
    }
}

#[tokio::test]
async fn local_act_concurrent_with_timer() {
    let wf_name = "local_act_concurrent_with_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<LocalActConcurrentWithTimerWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            LocalActConcurrentWithTimerWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct LocalActThenTimerThenWaitResult;

#[workflow_methods]
impl LocalActThenTimerThenWaitResult {
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let la = ctx.execute_local_activity(
            StdActivities::echo,
            "hi!".to_string(),
            LocalActivityOptions::default(),
        );
        ctx.timer(Duration::from_secs(1)).await;
        let res = la.await;
        assert!(res.is_ok());
        Ok(())
    }
}

#[tokio::test]
async fn local_act_then_timer_then_wait_result() {
    let wf_name = "local_act_then_timer_then_wait_result";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<LocalActThenTimerThenWaitResult>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            LocalActThenTimerThenWaitResult::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
pub(crate) struct LocalActThenTimerThenWait;

#[workflow_methods]
impl LocalActThenTimerThenWait {
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let la = ctx.execute_local_activity(
            StdActivities::delay,
            Duration::from_secs(4),
            LocalActivityOptions::default(),
        );
        ctx.timer(Duration::from_secs(1)).await;
        let res = la.await;
        assert!(res.is_ok());
        Ok(())
    }
}

#[tokio::test]
async fn long_running_local_act_with_timer() {
    let wf_name = "long_running_local_act_with_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<LocalActThenTimerThenWait>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            LocalActThenTimerThenWait::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
pub(crate) struct LocalActFanoutWf;

#[workflow_methods]
impl LocalActFanoutWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let las: Vec<_> = (1..=50)
            .map(|i| {
                ctx.execute_local_activity(
                    StdActivities::echo,
                    format!("Hi {i}"),
                    Default::default(),
                )
            })
            .collect();
        ctx.timer(Duration::from_secs(1)).await;
        temporalio_sdk::workflows::join_all(las).await;
        Ok(())
    }
}

#[tokio::test]
async fn local_act_fanout() {
    let wf_name = "local_act_fanout";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.set_core_tuner(Arc::new(TunerHolder::fixed_size(5, 1, 1, 1)));
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<LocalActFanoutWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            LocalActFanoutWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct LocalActRetryTimerBackoff;

#[workflow_methods]
impl LocalActRetryTimerBackoff {
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let res = ctx
            .execute_local_activity(
                StdActivities::always_fail,
                (),
                LocalActivityOptions::builder()
                    .retry_policy(
                        RetryPolicy {
                            initial_interval: Some(prost_dur!(from_micros(15))),
                            // We want two local backoffs that are short. Third backoff will use timer
                            backoff_coefficient: 1_000.,
                            maximum_interval: Some(prost_dur!(from_millis(1500))),
                            maximum_attempts: 4,
                            non_retryable_error_types: vec![],
                        }
                        .into(),
                    )
                    .timer_backoff_threshold(Duration::from_secs(1))
                    .build(),
            )
            .await;
        assert!(matches!(res, Err(ActivityExecutionError::Failed(_))));
        Ok(())
    }
}

#[tokio::test]
async fn local_act_retry_timer_backoff() {
    let wf_name = "local_act_retry_timer_backoff";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<LocalActRetryTimerBackoff>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            LocalActRetryTimerBackoff::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}

#[rstest::rstest]
#[case::wait(ActivityCancellationType::WaitCancellationCompleted)]
#[case::try_cancel(ActivityCancellationType::TryCancel)]
#[case::abandon(ActivityCancellationType::Abandon)]
#[tokio::test]
async fn cancel_immediate(#[case] cancel_type: ActivityCancellationType) {
    let wf_name = format!("cancel_immediate_{cancel_type:?}");
    // If we don't use this, we'd hang on shutdown for abandon cancel modes.
    let manual_cancel = CancellationToken::new();
    let mut starter = CoreWfStarter::new(&wf_name);

    struct EchoWithManualCancel {
        manual_cancel: CancellationToken,
    }
    #[activities]
    impl EchoWithManualCancel {
        #[activity]
        async fn echo(
            self: Arc<Self>,
            ctx: ActivityContext,
            _: String,
        ) -> Result<(), ActivityError> {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                _ = ctx.cancelled() => {
                    return Err(ActivityError::cancelled())
                }
                _ = self.manual_cancel.cancelled() => {}
            }
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_activities(EchoWithManualCancel {
            manual_cancel: manual_cancel.clone(),
        });

    #[workflow]
    struct CancelImmediate {
        cancel_type: ActivityCancellationType,
    }

    #[workflow_methods]
    impl CancelImmediate {
        #[init]
        fn new(_ctx: &WorkflowContextView, cancel_type: ActivityCancellationType) -> Self {
            Self { cancel_type }
        }

        #[run]
        pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let cancel_type = ctx.state(|wf| wf.cancel_type);
            let la = ctx.execute_local_activity(
                EchoWithManualCancel::echo,
                "hi".to_string(),
                LocalActivityOptions::builder()
                    .cancel_type(cancel_type)
                    .build(),
            );
            la.cancel();
            let resolution = la.await;
            assert!(matches!(
                resolution,
                Err(ActivityExecutionError::Cancelled(_))
            ));
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_workflow::<CancelImmediate>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            CancelImmediate::run,
            cancel_type,
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker
        .run_until_done_intercepted(Some(LACancellerInterceptor {
            cancel_on_workflow_completed: false,
            token: manual_cancel,
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn ordinary_failure_after_local_activity_cancel_does_not_retry() {
    let wf_name = "ordinary_failure_after_local_activity_cancel_does_not_retry";
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut starter = CoreWfStarter::new(wf_name);

    struct FailAfterCancel {
        attempts: Arc<AtomicUsize>,
    }
    #[activities]
    impl FailAfterCancel {
        #[activity]
        async fn run(self: Arc<Self>, ctx: ActivityContext, _: ()) -> Result<(), ActivityError> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                ctx.cancelled().await;
            }
            Err(anyhow!("ordinary failure on attempt {attempt}").into())
        }
    }

    #[workflow]
    #[derive(Default)]
    struct CancelThenFail;

    #[workflow_methods]
    impl CancelThenFail {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let la = ctx.execute_local_activity(
                FailAfterCancel::run,
                (),
                LocalActivityOptions::builder()
                    .cancel_type(ActivityCancellationType::WaitCancellationCompleted)
                    .retry_policy(
                        RetryPolicy {
                            initial_interval: Some(prost_dur!(from_millis(10))),
                            backoff_coefficient: 1.,
                            maximum_interval: Some(prost_dur!(from_millis(10))),
                            maximum_attempts: 2,
                            ..Default::default()
                        }
                        .into(),
                    )
                    .build(),
            );
            ctx.timer(Duration::from_secs(1)).await;
            la.cancel();
            let Err(ActivityExecutionError::Failed(failure)) = la.await else {
                panic!("Expected ordinary failure after cancellation");
            };
            assert_eq!(failure.failure().message, "ordinary failure on attempt 1");
            assert_matches!(
                &failure.failure().failure_info,
                Some(FailureInfo::ApplicationFailureInfo(_))
            );
            Ok(())
        }
    }

    starter.sdk_config.register_activities(FailAfterCancel {
        attempts: attempts.clone(),
    });
    starter
        .sdk_config
        .register_workflow::<CancelThenFail>()
        .unwrap();
    let mut worker = starter.worker().await;
    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            CancelThenFail::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

struct LACancellerInterceptor {
    token: CancellationToken,
    cancel_on_workflow_completed: bool,
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for LACancellerInterceptor {
    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        if !self.cancel_on_workflow_completed {
            return;
        }
        if let Some(workflow_activation_completion::Status::Successful(
            workflow_completion::Success { commands, .. },
        )) = completion.status.as_ref()
            && let Some(&Variant::CompleteWorkflowExecution(_)) =
                commands.last().and_then(|v| v.variant.as_ref())
        {
            self.token.cancel();
        }
    }
    fn on_shutdown(&self, _: &temporalio_sdk::Worker) {
        if !self.cancel_on_workflow_completed {
            self.token.cancel()
        }
    }
}

#[rstest::rstest]
#[case::while_running(None)]
#[case::while_backing_off(Some(Duration::from_millis(1500)))]
#[case::while_backing_off_locally(Some(Duration::from_millis(150)))]
#[tokio::test]
async fn cancel_after_act_starts(
    #[case] cancel_on_backoff: Option<Duration>,
    #[values(
        ActivityCancellationType::WaitCancellationCompleted,
        ActivityCancellationType::TryCancel,
        ActivityCancellationType::Abandon
    )]
    cancel_type: ActivityCancellationType,
) {
    let wf_name = format!("cancel_after_act_starts_{cancel_on_backoff:?}_{cancel_type:?}");
    // If we don't use this, we'd hang on shutdown for abandon cancel modes.
    let manual_cancel = CancellationToken::new();
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));

    struct EchoWithManualCancelAndBackoff {
        manual_cancel: CancellationToken,
        cancel_on_backoff: Option<CancellationToken>,
    }
    #[activities]
    impl EchoWithManualCancelAndBackoff {
        #[activity]
        async fn echo(
            self: Arc<Self>,
            ctx: ActivityContext,
            _: String,
        ) -> Result<(), ActivityError> {
            if self.cancel_on_backoff.is_some() {
                if ctx.is_cancelled() {
                    return Err(ActivityError::cancelled());
                }
                // Just fail constantly so we get stuck on the backoff timer
                return Err(anyhow!("Oh no I failed!").into());
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(100)) => {}
                    _ = ctx.cancelled() => {
                        return Err(ActivityError::cancelled_with_details(
                            "cancel-after-start".to_string(),
                        ))
                    }
                    _ = self.manual_cancel.cancelled() => {
                        return Ok(())
                    }
                }
            }
            Err(anyhow!("Oh no I failed!").into())
        }
    }

    starter
        .sdk_config
        .register_activities(EchoWithManualCancelAndBackoff {
            manual_cancel: manual_cancel.clone(),
            cancel_on_backoff: if cancel_on_backoff.is_some() {
                Some(CancellationToken::new())
            } else {
                None
            },
        });
    #[workflow]
    #[derive(Default)]
    struct CancelAfterActStartsWf;

    #[workflow_methods]
    impl CancelAfterActStartsWf {
        #[run]
        async fn run(
            ctx: &mut WorkflowContext<Self>,
            (bo_dur, cancel_type): (Duration, ActivityCancellationType),
        ) -> WorkflowResult<()> {
            let la = ctx.execute_local_activity(
                EchoWithManualCancelAndBackoff::echo,
                "hi".to_string(),
                LocalActivityOptions::builder()
                    .retry_policy(
                        RetryPolicy {
                            initial_interval: Some(bo_dur.try_into().unwrap()),
                            backoff_coefficient: 1.,
                            maximum_interval: Some(bo_dur.try_into().unwrap()),
                            // Retry forever until cancelled
                            ..Default::default()
                        }
                        .into(),
                    )
                    .timer_backoff_threshold(Duration::from_secs(1))
                    .cancel_type(cancel_type)
                    .build(),
            );
            ctx.timer(Duration::from_secs(1)).await;
            // Note that this cancel can't go through for *two* WF tasks, because we do a full heartbeat
            // before the timer (LA hasn't resolved), and then the timer fired event won't appear in
            // history until *after* the next WFT because we force generated it when we sent the timer
            // command.
            la.cancel();
            // This extra timer is here to ensure the presence of another WF task doesn't mess up
            // resolving the LA with cancel on replay
            ctx.timer(Duration::from_secs(1)).await;
            let err = la.await.unwrap_err();
            match err {
                ActivityExecutionError::Cancelled(cancel_err) => {
                    let expected_details = if bo_dur == Duration::from_secs(1)
                        && cancel_type == ActivityCancellationType::WaitCancellationCompleted
                    {
                        Some("cancel-after-start".to_string())
                    } else {
                        None
                    };
                    assert_eq!(cancel_err.details::<String>().unwrap(), expected_details);
                }
                ActivityExecutionError::Failed(failure)
                    if bo_dur != Duration::from_secs(1)
                        && cancel_type == ActivityCancellationType::WaitCancellationCompleted =>
                {
                    // Cancellation can land during an attempt rather than its backoff. That
                    // attempt's ordinary failure is terminal, not converted to cancellation.
                    assert_eq!(failure.failure().message, "Oh no I failed!");
                    assert_matches!(
                        &failure.failure().failure_info,
                        Some(FailureInfo::ApplicationFailureInfo(_))
                    );
                }
                other => panic!("Unexpected activity result after cancellation: {other:?}"),
            };
            Ok(())
        }
    }

    let bo_dur = cancel_on_backoff.unwrap_or_else(|| Duration::from_secs(1));
    starter
        .sdk_config
        .register_workflow::<CancelAfterActStartsWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            CancelAfterActStartsWf::run,
            (bo_dur, cancel_type),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker
        .run_until_done_intercepted(Some(LACancellerInterceptor {
            token: manual_cancel,
            // Only needed for this one case since the activity is not drained and prevents worker from shutting down.
            cancel_on_workflow_completed: matches!(cancel_type, ActivityCancellationType::Abandon)
                && cancel_on_backoff.is_none(),
        }))
        .await
        .unwrap();
    starter.shutdown().await;
}

#[rstest::rstest]
#[case::schedule(true)]
#[case::start(false)]
#[tokio::test]
async fn x_to_close_timeout(#[case] is_schedule: bool) {
    let wf_name = format!(
        "{}_to_close_timeout",
        if is_schedule { "schedule" } else { "start" }
    );
    let mut starter = CoreWfStarter::new(&wf_name);

    struct LongRunningWithCancellation;
    #[activities]
    impl LongRunningWithCancellation {
        #[activity]
        async fn go(ctx: ActivityContext) -> Result<(), ActivityError> {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(100)) => {}
                _ = ctx.cancelled() => {
                    return Err(ActivityError::cancelled())
                }
            }
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_activities(LongRunningWithCancellation);
    #[workflow]
    #[derive(Default)]
    struct XToCloseTimeoutWf;

    #[workflow_methods]
    impl XToCloseTimeoutWf {
        #[run]
        async fn run(
            ctx: &mut WorkflowContext<Self>,
            (sched, start, is_schedule): (Option<Duration>, Option<Duration>, bool),
        ) -> WorkflowResult<()> {
            let res = ctx
                .execute_local_activity(
                    LongRunningWithCancellation::go,
                    (),
                    LocalActivityOptions::builder()
                        .retry_policy(
                            RetryPolicy {
                                initial_interval: Some(prost_dur!(from_micros(15))),
                                backoff_coefficient: 1_000.,
                                maximum_interval: Some(prost_dur!(from_millis(1500))),
                                maximum_attempts: 4,
                                non_retryable_error_types: vec![],
                            }
                            .into(),
                        )
                        .timer_backoff_threshold(Duration::from_secs(1))
                        .maybe_schedule_to_close_timeout(sched)
                        .maybe_start_to_close_timeout(start)
                        .build(),
                )
                .await;
            let err = res.unwrap_err();
            let timeout = err.as_timeout().unwrap();
            let expected_timeout = if is_schedule {
                TimeoutType::ScheduleToClose
            } else {
                TimeoutType::StartToClose
            };
            assert_eq!(timeout.timeout_type(), expected_timeout);
            Ok(())
        }
    }

    let (sched, start) = if is_schedule {
        (Some(Duration::from_secs(2)), None)
    } else {
        (None, Some(Duration::from_secs(2)))
    };
    starter
        .sdk_config
        .register_workflow::<XToCloseTimeoutWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            XToCloseTimeoutWf::run,
            (sched, start, is_schedule),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[case::cached(true)]
#[case::not_cached(false)]
#[tokio::test]
async fn schedule_to_close_timeout_across_timer_backoff(#[case] cached: bool) {
    let wf_name = format!(
        "schedule_to_close_timeout_across_timer_backoff_{}",
        if cached { "cached" } else { "not_cached" }
    );
    let mut starter = CoreWfStarter::new(&wf_name);
    if !cached {
        starter.sdk_config.max_cached_workflows = 0_usize;
    }

    #[workflow]
    #[derive(Default)]
    struct ScheduleToCloseTimeoutAcrossTimerBackoff;

    #[workflow_methods]
    impl ScheduleToCloseTimeoutAcrossTimerBackoff {
        #[run]
        pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let res = ctx
                .execute_local_activity(
                    FailWithAtomicCounter::go,
                    "hi".to_string(),
                    LocalActivityOptions::builder()
                        .retry_policy(
                            RetryPolicy {
                                initial_interval: Some(prost_dur!(from_millis(15))),
                                backoff_coefficient: 1_000.,
                                maximum_interval: Some(prost_dur!(from_millis(1000))),
                                maximum_attempts: 40,
                                non_retryable_error_types: vec![],
                            }
                            .into(),
                        )
                        .timer_backoff_threshold(Duration::from_millis(500))
                        .schedule_to_close_timeout(Duration::from_secs(2))
                        .build(),
                )
                .await;
            let err = res.unwrap_err();
            let ActivityExecutionError::Failed(failure) = &err else {
                panic!("expected Failed, got {err:?}");
            };
            let Some(timeout) = failure.as_timeout() else {
                panic!("expected timeout cause, got {failure:?}");
            };
            assert_eq!(timeout.timeout_type(), TimeoutType::ScheduleToClose);
            Ok(())
        }
    }

    let num_attempts = Arc::new(AtomicU8::new(0));

    struct FailWithAtomicCounter {
        counter: Arc<AtomicU8>,
    }
    #[activities]
    impl FailWithAtomicCounter {
        #[activity]
        async fn go(self: Arc<Self>, _: ActivityContext, _: String) -> Result<(), ActivityError> {
            self.counter.fetch_add(1, Ordering::Relaxed);
            Err(anyhow!("Oh no I failed!").into())
        }
    }

    starter
        .sdk_config
        .register_activities(FailWithAtomicCounter {
            counter: num_attempts.clone(),
        });
    starter
        .sdk_config
        .register_workflow::<ScheduleToCloseTimeoutAcrossTimerBackoff>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            ScheduleToCloseTimeoutAcrossTimerBackoff::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // 3 attempts b/c first backoff is very small, then the next 2 attempts take at least 2 seconds
    // b/c of timer backoff.
    assert_eq!(3, num_attempts.load(Ordering::Relaxed));
}

#[tokio::test]
async fn timer_backoff_concurrent_with_non_timer_backoff() {
    let wf_name = "timer_backoff_concurrent_with_non_timer_backoff";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);

    #[workflow]
    #[derive(Default)]
    struct TimerBackoffConcurrentWf;

    #[workflow_methods]
    impl TimerBackoffConcurrentWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let r1 = ctx.execute_local_activity(
                StdActivities::always_fail,
                (),
                LocalActivityOptions::builder()
                    .retry_policy(
                        RetryPolicy {
                            initial_interval: Some(prost_dur!(from_micros(15))),
                            backoff_coefficient: 1_000.,
                            maximum_interval: Some(prost_dur!(from_millis(1500))),
                            maximum_attempts: 4,
                            non_retryable_error_types: vec![],
                        }
                        .into(),
                    )
                    .timer_backoff_threshold(Duration::from_secs(1))
                    .build(),
            );
            let r2 = ctx.execute_local_activity(
                StdActivities::always_fail,
                (),
                LocalActivityOptions::builder()
                    .retry_policy(
                        RetryPolicy {
                            initial_interval: Some(prost_dur!(from_millis(15))),
                            backoff_coefficient: 10.,
                            maximum_interval: Some(prost_dur!(from_millis(1500))),
                            maximum_attempts: 4,
                            non_retryable_error_types: vec![],
                        }
                        .into(),
                    )
                    .timer_backoff_threshold(Duration::from_secs(10))
                    .build(),
            );
            let (r1, r2) = temporalio_sdk::workflows::join!(r1, r2);
            assert!(matches!(r1, Err(ActivityExecutionError::Failed(_))));
            assert!(matches!(r2, Err(ActivityExecutionError::Failed(_))));
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_workflow::<TimerBackoffConcurrentWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            TimerBackoffConcurrentWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn repro_nondeterminism_with_timer_bug() {
    let wf_name = "repro_nondeterminism_with_timer_bug";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);

    #[workflow]
    #[derive(Default)]
    struct ReproNondeterminismWithTimerBugWf;

    #[workflow_methods]
    impl ReproNondeterminismWithTimerBugWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let mut t1 = ctx.timer(Duration::from_secs(30));
            let mut r1 = ctx.execute_local_activity(
                StdActivities::delay,
                Duration::from_secs(2),
                LocalActivityOptions::builder()
                    .retry_policy(
                        RetryPolicy {
                            initial_interval: Some(prost_dur!(from_micros(15))),
                            backoff_coefficient: 1_000.,
                            maximum_interval: Some(prost_dur!(from_millis(1500))),
                            maximum_attempts: 4,
                            non_retryable_error_types: vec![],
                        }
                        .into(),
                    )
                    .timer_backoff_threshold(Duration::from_secs(1))
                    .build(),
            );
            temporalio_sdk::workflows::select! {
                _ = t1 => {},
                _ = r1 => {
                    t1.cancel();
                },
            }
            ctx.timer(Duration::from_secs(1)).await;
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_workflow::<ReproNondeterminismWithTimerBugWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ReproNondeterminismWithTimerBugWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let client = starter.get_core_client().await;
    let handle = WorkflowExecutionInfo::builder()
        .namespace(client.namespace())
        .workflow_id(wf_name)
        .maybe_run_id(Some(handle.run_id().unwrap().to_string()))
        .build()
        .bind_untyped(client.clone());
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[tokio::test]
async fn weird_la_nondeterminism_repro(#[values(true, false)] fix_hist: bool) {
    let mut hist =
        history_from_proto_binary("evict_while_la_running_no_interference-85_history.bin")
            .await
            .unwrap();
    if fix_hist {
        // Replace broken ending with accurate ending
        hist.events.truncate(20);
        let mut thb = TestHistoryBuilder::from_history(hist.events);
        thb.add_workflow_task_completed();
        thb.add_workflow_execution_completed();
        hist = thb.get_full_history_info().unwrap().into();
    }

    let mut worker = crate::common::replay_sdk_worker_with_options(
        [HistoryForReplay::new(hist, "fake".to_owned())],
        |options| {
            options.register_workflow::<LaProblemWorkflow>().unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn second_weird_la_nondeterminism_repro() {
    let mut hist =
        history_from_proto_binary("evict_while_la_running_no_interference-23_history.bin")
            .await
            .unwrap();
    // Chop off uninteresting ending
    hist.events.truncate(24);
    let mut thb = TestHistoryBuilder::from_history(hist.events);
    thb.add_workflow_execution_completed();
    hist = thb.get_full_history_info().unwrap().into();

    let mut worker = crate::common::replay_sdk_worker_with_options(
        [HistoryForReplay::new(hist, "fake".to_owned())],
        |options| {
            options.register_workflow::<LaProblemWorkflow>().unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn third_weird_la_nondeterminism_repro() {
    let mut hist =
        history_from_proto_binary("evict_while_la_running_no_interference-16_history.bin")
            .await
            .unwrap();
    let mut thb = TestHistoryBuilder::from_history(hist.events);
    thb.add_workflow_task_scheduled_and_started();
    hist = thb.get_full_history_info().unwrap().into();

    let mut worker = crate::common::replay_sdk_worker_with_options(
        [HistoryForReplay::new(hist, "fake".to_owned())],
        |options| {
            options.register_workflow::<LaProblemWorkflow>().unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run().await.unwrap();
}

/// This test demonstrates why it's important to send LA resolutions last within a job.
/// If we were to (during replay) scan ahead, see the marker, and resolve the LA before the
/// activity cancellation, that would be wrong because, during execution, the LA resolution is
/// always going to take _longer_ than the instantaneous cancel effect.
///
/// This affect applies regardless of how you choose to interleave cancellations and LAs. Ultimately
/// all cancellations will happen at once (in the order they are submitted) while the LA executions
/// are queued (because this all happens synchronously in the workflow machines). If you were to
/// _wait_ on an LA, and then cancel something else, and then run another LA, such that all commands
/// happened in the same workflow task, it would _still_ be fine to sort LA jobs last _within_ the
/// 2 activations that would necessarily entail (because, one you wait on the LA result, control
/// will be yielded and it will take another activation to unblock that LA).
#[tokio::test]
async fn la_resolve_same_time_as_other_cancel() {
    let wf_name = "la_resolve_same_time_as_other_cancel";
    let mut starter = CoreWfStarter::new(wf_name);

    struct DelayWithCancellation;
    #[activities]
    impl DelayWithCancellation {
        #[activity]
        async fn delay(ctx: ActivityContext, dur: Duration) -> Result<(), ActivityError> {
            tokio::select! {
                _ = tokio::time::sleep(dur) => {}
                _ = ctx.cancelled() => {}
            }
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_activities(DelayWithCancellation);
    // The activity won't get a chance to receive the cancel so make sure we still exit fast
    starter.sdk_config.graceful_shutdown_period = Some(Duration::from_millis(100));

    #[workflow]
    #[derive(Default)]
    struct LaResolveSameTimeAsOtherCancelWf;

    #[workflow_methods]
    impl LaResolveSameTimeAsOtherCancelWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let mut normal_act = ctx.execute_activity(
                DelayWithCancellation::delay,
                Duration::from_secs(9),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(9000))
                    .cancellation_type(ActivityCancellationType::TryCancel)
                    .build(),
            );
            // Make new task
            ctx.timer(Duration::from_millis(1)).await;

            // Start LA and cancel the activity at the same time
            let mut local_act = ctx.execute_local_activity(
                DelayWithCancellation::delay,
                Duration::from_millis(100),
                LocalActivityOptions::builder().build(),
            );
            normal_act.cancel();
            // Race them, starting a timer if LA completes first
            temporalio_sdk::workflows::select! {
                _ = normal_act => {},
                _ = local_act => {
                    ctx.timer(Duration::from_millis(1)).await;
                },
            }
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_workflow::<LaResolveSameTimeAsOtherCancelWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            LaResolveSameTimeAsOtherCancelWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let client = starter.get_core_client().await;
    let handle = WorkflowExecutionInfo::builder()
        .namespace(client.namespace())
        .workflow_id(wf_name)
        .maybe_run_id(Some(handle.run_id().unwrap().to_string()))
        .build()
        .bind_untyped(client.clone());
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}

#[rstest::rstest]
#[case(200, 0)]
#[case(200, 2000)]
#[case(2000, 0)]
#[case(2000, 2000)]
#[tokio::test]
async fn long_local_activity_with_update(
    #[context] ctx: Context,
    #[case] update_interval_ms: u64,
    #[case] update_inner_timer: u64,
) {
    let wf_name = format!("{}-{}", ctx.name, ctx.case.unwrap());
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    starter.sdk_config.register_activities(StdActivities);

    #[workflow]
    #[derive(Default)]
    struct LongLocalActivityWithUpdateWf {
        update_counter: usize,
        update_inner_timer: u64,
    }

    #[workflow_methods]
    impl LongLocalActivityWithUpdateWf {
        #[init]
        fn init(_ctx: &WorkflowContextView, update_inner_timer: u64) -> Self {
            Self {
                update_counter: 1,
                update_inner_timer,
            }
        }

        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<usize> {
            ctx.execute_local_activity(
                StdActivities::delay,
                Duration::from_secs(6),
                LocalActivityOptions::default(),
            )
            .await?;
            Ok(ctx.state(|wf| wf.update_counter))
        }

        #[update]
        async fn do_update(ctx: &mut WorkflowContext<Self>, _: ()) {
            let update_inner_timer = ctx.state(|s| s.update_inner_timer);
            if update_inner_timer != 0 {
                ctx.timer(Duration::from_millis(update_inner_timer)).await;
            }
            ctx.state_mut(|s| s.update_counter += 1);
        }
    }

    starter
        .sdk_config
        .register_workflow::<LongLocalActivityWithUpdateWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            LongLocalActivityWithUpdateWf::run,
            update_inner_timer,
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();

    let update = async {
        loop {
            tokio::time::sleep(Duration::from_millis(update_interval_ms)).await;
            let _ = handle
                .execute_update(
                    LongLocalActivityWithUpdateWf::do_update,
                    (),
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await;
        }
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    tokio::select!(_ = update => {}, _ = runner => {});
    let res = handle.get_result(Default::default()).await.unwrap();
    let replay_res = handle.fetch_history_and_replay(&mut worker).await.unwrap();
    assert_eq!(res, usize::from_json_payload(&replay_res.unwrap()).unwrap());

    // Load histories from pre-fix version and ensure compat
    let replay_worker = init_core_replay_preloaded(
        starter.get_task_queue(),
        [HistoryForReplay::new(
            history_from_proto_binary(&format!("{wf_name}_history.bin"))
                .await
                .unwrap(),
            "fake".to_owned(),
        )],
    );
    worker
        .inner_mut()
        .with_new_core_worker(Arc::new(replay_worker));
    worker.set_worker_interceptor(FailOnNondeterminismInterceptor {});
    worker.inner_mut().run().await.unwrap();
}

#[tokio::test]
async fn local_activity_with_heartbeat_only_causes_one_wakeup() {
    let wf_name = "local_activity_with_heartbeat_only_causes_one_wakeup";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    starter.sdk_config.register_activities(StdActivities);

    #[workflow]
    #[derive(Default)]
    struct LocalActivityWithHeartbeatOnlyCausesOneWakeupWf {
        la_resolved: bool,
    }

    #[workflow_methods]
    impl LocalActivityWithHeartbeatOnlyCausesOneWakeupWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<usize> {
            let mut wakeup_counter = 0;
            // Interestingly LA munst come first because if the condition is polled first, we won't
            // see that resolved is true.
            // TODO [rust-sdk-branch] - See if we can fix this and know that we should re-poll.
            let (_, wait_result) = temporalio_sdk::workflows::join!(
                async {
                    ctx.execute_local_activity(
                        StdActivities::delay,
                        Duration::from_secs(6),
                        LocalActivityOptions::default(),
                    )
                    .await
                    .ok();
                    ctx.state_mut(|s| s.la_resolved = true);
                },
                ctx.wait_condition(|_| {
                    wakeup_counter += 1;
                    ctx.state(|s| s.la_resolved)
                })
            );
            wait_result?;
            Ok(wakeup_counter as usize)
        }
    }

    starter
        .sdk_config
        .register_workflow::<LocalActivityWithHeartbeatOnlyCausesOneWakeupWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            LocalActivityWithHeartbeatOnlyCausesOneWakeupWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let r = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(r, 2);
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}

#[workflow]
#[derive(Default)]
pub(crate) struct LocalActivityWithSummaryWf;

#[workflow_methods]
impl LocalActivityWithSummaryWf {
    #[run(name = "local_activity_with_summary")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.execute_local_activity(
            StdActivities::echo,
            "hi".to_string(),
            LocalActivityOptions::builder()
                .summary("Echo summary".to_string())
                .build(),
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn local_activity_with_summary() {
    let wf_name = "local_activity_with_summary";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<LocalActivityWithSummaryWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let handle = starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    handle.fetch_history_and_replay(&mut worker).await.unwrap();

    let la_events = starter
        .get_history()
        .await
        .events
        .into_iter()
        .filter(|e| match e.attributes {
            Some(MarkerRecordedEventAttributes(ref a)) => a.marker_name == "core_local_activity",
            _ => false,
        })
        .collect::<Vec<_>>();
    assert_eq!(la_events.len(), 1);
    let summary = la_events[0]
        .user_metadata
        .as_ref()
        .expect("metadata missing from local activity marker")
        .summary
        .as_ref()
        .expect("summary missing from local activity marker");
    assert_eq!(
        "Echo summary",
        String::from_json_payload(summary).expect("failed to parse summary")
    );
}

/// This test verifies that when replaying we are able to resolve local activities whose data we
/// don't see until after the workflow issues the command
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[case::replay(true, true)]
#[case::not_replay(false, true)]
#[case::replay_cache_off(true, false)]
#[case::not_replay_cache_off(false, false)]
#[tokio::test]
async fn local_act_two_wfts_before_marker(#[case] replay: bool, #[case] cached: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", b"echo".into());
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let resps = if replay {
        vec![ResponseType::AllHistory]
    } else {
        vec![1.into(), 2.into(), ResponseType::AllHistory]
    };
    let mh = MockPollCfg::from_resp_batches(wf_id, t, resps, mock);
    #[workflow]
    #[derive(Default)]
    struct LocalActTwoWftsBeforeMarkerWf;

    #[workflow_methods]
    impl LocalActTwoWftsBeforeMarkerWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let la = ctx.execute_local_activity(StdActivities::default, (), Default::default());
            ctx.timer(Duration::from_secs(1)).await;
            let _ = la.await;
            Ok(())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |cfg| {
            if cached {
                cfg.max_cached_workflows = 1;
            }
        },
        |options| {
            options
                .register_workflow::<LocalActTwoWftsBeforeMarkerWf>()
                .unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn local_act_many_concurrent() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();
    for i in 1..=50 {
        t.add_local_activity_result_marker(i, &i.to_string(), b"echo".into());
    }
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 3], mock);
    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |_| {},
        |options| {
            options.register_workflow::<LocalActFanoutWf>().unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
}

/// Verifies that local activities which take more than a workflow task timeout will cause
/// us to issue additional (empty) WFT completions with the force flag on, thus preventing timeout
/// of WFT while the local activity continues to execute.
///
/// The test with shutdown verifies if we call shutdown while the local activity is running that
/// shutdown does not complete until it's finished.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[case::with_shutdown(true)]
#[case::normal_complete(false)]
#[tokio::test]
async fn local_act_heartbeat(#[case] shutdown_middle: bool) {
    let mut t = TestHistoryBuilder::default();
    let wft_timeout = Duration::from_millis(200);
    t.add_wfe_started_with_wft_timeout(wft_timeout);
    t.add_full_wf_task();
    // Task created by WFT heartbeat
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 2, 2], mock);
    mh.enforce_correct_number_of_polls = false;
    let shutdown_barr = Arc::new(Barrier::new(2));

    #[workflow]
    #[derive(Default)]
    struct LocalActHeartbeatWf;

    #[workflow_methods]
    impl LocalActHeartbeatWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            dbg!("dafuq");
            ctx.execute_local_activity(
                EchoWithConditionalBarrier::echo,
                "hi".to_string(),
                LocalActivityOptions::default(),
            )
            .await?;
            Ok(())
        }
    }

    struct EchoWithConditionalBarrier {
        shutdown_barr: Option<Arc<Barrier>>,
        wft_timeout: Duration,
    }
    #[activities]
    impl EchoWithConditionalBarrier {
        #[activity]
        async fn echo(
            self: Arc<Self>,
            _: ActivityContext,
            str: String,
        ) -> Result<String, ActivityError> {
            dbg!("Running activity");
            if let Some(barr) = &self.shutdown_barr {
                barr.wait().await;
            }
            // Take slightly more than two workflow tasks
            tokio::time::sleep(self.wft_timeout.mul_f32(2.2)).await;
            Ok(str)
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |wc| {
            wc.max_cached_workflows = 1;
            wc.max_outstanding_workflow_tasks = Some(1);
        },
        |options| {
            options.register_workflow::<LocalActHeartbeatWf>().unwrap();
            options.register_activities(EchoWithConditionalBarrier {
                shutdown_barr: if shutdown_middle {
                    Some(shutdown_barr.clone())
                } else {
                    None
                },
                wft_timeout,
            });
        },
    );
    let core = worker.core_worker();
    let (_, runres) = tokio::join!(
        async {
            if shutdown_middle {
                shutdown_barr.wait().await;
                dbg!("Past barrier");
                core.shutdown().await;
            }
        },
        worker.run_until_done()
    );
    runres.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[case::retry_then_pass(true)]
#[case::retry_until_fail(false)]
#[tokio::test]
async fn local_act_fail_and_retry(#[case] eventually_pass: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.set_wf_input(eventually_pass.as_json_payload().unwrap());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1], mock);
    #[workflow]
    #[derive(Default)]
    struct LocalActFailAndRetryWf;

    #[workflow_methods]
    impl LocalActFailAndRetryWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>, eventually_pass: bool) -> WorkflowResult<()> {
            let la_res = ctx
                .execute_local_activity(
                    EventuallyPassingActivity::echo,
                    "hi".to_string(),
                    LocalActivityOptions::builder()
                        .retry_policy(
                            RetryPolicy {
                                initial_interval: Some(prost_dur!(from_millis(50))),
                                backoff_coefficient: 1.2,
                                maximum_interval: None,
                                maximum_attempts: 5,
                                non_retryable_error_types: vec![],
                            }
                            .into(),
                        )
                        .build(),
                )
                .await;
            if eventually_pass {
                assert!(la_res.is_ok())
            } else {
                assert!(matches!(la_res, Err(ActivityExecutionError::Failed(_))))
            }
            Ok(())
        }
    }

    let attempts = Arc::new(AtomicUsize::new(0));

    struct EventuallyPassingActivity {
        attempts: Arc<AtomicUsize>,
        eventually_pass: bool,
    }
    #[activities]
    impl EventuallyPassingActivity {
        #[activity]
        async fn echo(self: Arc<Self>, _: ActivityContext, _: String) -> Result<(), ActivityError> {
            // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
            if 2 == self.attempts.fetch_add(1, Ordering::Relaxed) && self.eventually_pass {
                Ok(())
            } else {
                Err(anyhow!("Oh no I failed!").into())
            }
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |_| {},
        |options| {
            options
                .register_workflow::<LocalActFailAndRetryWf>()
                .unwrap();
            options.register_activities(EventuallyPassingActivity {
                attempts: attempts.clone(),
                eventually_pass,
            });
        },
    );
    worker.run_until_done().await.unwrap();
    let expected_attempts = if eventually_pass { 3 } else { 5 };
    assert_eq!(expected_attempts, attempts.load(Ordering::Relaxed));
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn local_act_retry_long_backoff_uses_timer() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_marker(
        1,
        "1",
        None,
        Some(Failure::application_failure("la failed".to_string(), false)),
        |m| m.activity_type = StdActivities::always_fail.name().to_owned(),
    );
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_local_activity_marker(
        2,
        "2",
        None,
        Some(Failure::application_failure("la failed".to_string(), false)),
        |m| m.activity_type = StdActivities::always_fail.name().to_owned(),
    );
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [1.into(), 2.into(), ResponseType::AllHistory],
        mock,
    );
    #[workflow]
    #[derive(Default)]
    struct LocalActRetryLongBackoffUsesTimerWf;

    #[workflow_methods]
    impl LocalActRetryLongBackoffUsesTimerWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let la_res = ctx
                .execute_local_activity(
                    StdActivities::always_fail,
                    (),
                    LocalActivityOptions::builder()
                        .retry_policy(
                            RetryPolicy {
                                initial_interval: Some(prost_dur!(from_millis(65))),
                                backoff_coefficient: 1_000.,
                                maximum_interval: Some(prost_dur!(from_secs(600))),
                                maximum_attempts: 3,
                                non_retryable_error_types: vec![],
                            }
                            .into(),
                        )
                        .build(),
                )
                .await;
            assert!(matches!(la_res, Err(ActivityExecutionError::Failed(_))));
            ctx.timer(Duration::from_secs(1)).await;
            Ok(())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |w| w.max_cached_workflows = 1,
        |options| {
            options
                .register_workflow::<LocalActRetryLongBackoffUsesTimerWf>()
                .unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn local_act_null_result() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_marker(1, "1", None, None, |_| {});
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    #[workflow]
    #[derive(Default)]
    struct LocalActNullResultWf;

    #[workflow_methods]
    impl LocalActNullResultWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
                .await?;
            Ok(())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |w| w.max_cached_workflows = 1,
        |options| {
            options.register_workflow::<LocalActNullResultWf>().unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn local_act_command_immediately_follows_la_marker() {
    // This repro only works both when cache is off, and there is at least one heartbeat wft
    // before the marker & next command are recorded.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", "done".into());
    t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [3], mock);
    #[workflow]
    #[derive(Default)]
    struct LocalActCommandImmediatelyFollowsLaMarkerWf;

    #[workflow_methods]
    impl LocalActCommandImmediatelyFollowsLaMarkerWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
                .await?;
            ctx.timer(Duration::from_secs(1)).await;
            Ok(())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |w| w.max_cached_workflows = 0,
        |options| {
            options
                .register_workflow::<LocalActCommandImmediatelyFollowsLaMarkerWf>()
                .unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn query_during_wft_heartbeat_doesnt_accidentally_fail_to_continue_heartbeat() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    // get query here
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", "done".into());
    t.add_workflow_execution_completed();

    let query_with_hist_task = {
        let mut pr = hist_to_poll_resp(&t, wfid, ResponseType::ToTaskNum(1));
        pr.queries = HashMap::new();
        pr.queries.insert(
            "the-query".to_string(),
            WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            },
        );
        pr
    };
    let after_la_resolved = Arc::new(Barrier::new(2));
    let poll_barr = after_la_resolved.clone();
    let tasks = [
        query_with_hist_task,
        hist_to_poll_resp(
            &t,
            wfid,
            ResponseType::UntilResolved(
                async move {
                    poll_barr.wait().await;
                }
                .boxed(),
                3,
            ),
        ),
    ];
    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let barrier = Barrier::new(2);

    let wf_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            schedule_local_activity_cmd(
                1,
                "1",
                ProtoActivityCancellationType::TryCancel,
                Duration::from_secs(60),
            ),
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        // Get query, and complete it
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        // Now complete the LA
        barrier.wait().await;
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            query_ok(&query.query_id, "whatev"),
        ))
        .await
        .unwrap();
        // Activation with it resolving:
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        core.complete_execution(&task.run_id).await;
    };
    let act_fut = async {
        let act_task = core.poll_activity_task().await.unwrap();
        barrier.wait().await;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        after_la_resolved.wait().await;
    };

    tokio::join!(wf_fut, act_fut);
}

/// Starts a timer, or with `command_free_start` waits for a signal without issuing a command, then
/// runs `local_activities` local activities. Once they have all resolved, it completes if a signal
/// arrived after they were scheduled. Otherwise it starts a timer, only so that the two branches
/// issue different commands, and completes on the next signal.
#[derive(Clone, Default)]
struct SignalAfterLocalActivitiesWf {
    local_activities: u32,
    command_free_start: bool,
    signals: usize,
    signals_when_scheduled: Option<usize>,
    resolved: u32,
    waiting_for_signal: bool,
}

impl SignalAfterLocalActivitiesWf {
    fn new(local_activities: u32) -> Self {
        Self {
            local_activities,
            ..Default::default()
        }
    }

    /// The same workflow, started over, as after an eviction
    fn restarted(&self) -> Self {
        Self {
            command_free_start: self.command_free_start,
            ..Self::new(self.local_activities)
        }
    }

    fn schedule_local_activities(&mut self, commands: &mut Vec<Variant>) {
        self.signals_when_scheduled = Some(self.signals);
        commands.extend((1..=self.local_activities).map(|seq| {
            schedule_local_activity_cmd(
                seq,
                &seq.to_string(),
                ProtoActivityCancellationType::TryCancel,
                Duration::from_secs(60),
            )
        }));
    }

    fn activate(&mut self, activation: &WorkflowActivation) -> WorkflowActivationCompletion {
        // Lang SDKs run signal handlers before resuming the code that awaits other jobs.
        self.signals += activation
            .jobs
            .iter()
            .filter(|j| {
                matches!(
                    j.variant,
                    Some(workflow_activation_job::Variant::SignalWorkflow(_))
                )
            })
            .count();
        let mut commands = vec![];
        for job in &activation.jobs {
            match job.variant.as_ref().unwrap() {
                workflow_activation_job::Variant::InitializeWorkflow(_) => {
                    if !self.command_free_start {
                        commands.push(start_timer_cmd(1, Duration::from_secs(1)));
                    }
                }
                workflow_activation_job::Variant::FireTimer(_) => {
                    self.schedule_local_activities(&mut commands);
                }
                workflow_activation_job::Variant::ResolveActivity(_) => {
                    self.resolved += 1;
                    if self.resolved == self.local_activities {
                        if self.signals > self.signals_when_scheduled.unwrap() {
                            commands.push(CompleteWorkflowExecution { result: None }.into());
                        } else {
                            self.waiting_for_signal = true;
                            commands.push(start_timer_cmd(2, Duration::from_secs(1)));
                        }
                    }
                }
                workflow_activation_job::Variant::SignalWorkflow(_) if self.waiting_for_signal => {
                    self.waiting_for_signal = false;
                    commands.push(CompleteWorkflowExecution { result: None }.into());
                }
                workflow_activation_job::Variant::SignalWorkflow(_) => {}
                unexpected => panic!("Unexpected activation job: {unexpected:?}"),
            }
        }
        if self.command_free_start && self.signals_when_scheduled.is_none() && self.signals > 0 {
            self.schedule_local_activities(&mut commands);
        }
        WorkflowActivationCompletion::from_cmds(activation.run_id.clone(), commands)
    }
}

fn job_names(activation: &WorkflowActivation) -> Vec<&'static str> {
    activation
        .jobs
        .iter()
        .map(|j| match j.variant.as_ref().unwrap() {
            workflow_activation_job::Variant::InitializeWorkflow(_) => "init",
            workflow_activation_job::Variant::FireTimer(_) => "timer",
            workflow_activation_job::Variant::ResolveActivity(_) => "local activity",
            workflow_activation_job::Variant::SignalWorkflow(_) => "signal",
            unexpected => panic!("Unexpected activation job: {unexpected:?}"),
        })
        .collect()
}

/// Ids of the queries in the activation, sorted
fn query_ids(activation: &WorkflowActivation) -> Vec<&str> {
    let mut ids: Vec<_> = activation
        .jobs
        .iter()
        .filter_map(|j| match &j.variant {
            Some(workflow_activation_job::Variant::QueryWorkflow(q)) => Some(q.query_id.as_str()),
            _ => None,
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn command_types(completion: &WorkflowTaskCompletion) -> Vec<CommandType> {
    completion
        .commands
        .iter()
        .map(|c| c.command_type())
        .collect()
}

/// Appends the events the server records for a workflow task completion.
fn record_completion(t: &mut TestHistoryBuilder, completion: &WorkflowTaskCompletion) {
    t.add_workflow_task_completed();
    if let Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(attrs)) =
        t.last_event().and_then(|e| e.attributes.as_mut())
    {
        attrs.sdk_metadata = Some(completion.sdk_metadata.clone());
    }
    let wft_completed_id = t.current_event_id();
    for command in &completion.commands {
        match command.attributes.clone().unwrap() {
            command::Attributes::StartTimerCommandAttributes(timer) => {
                t.add_timer_started(timer.timer_id);
            }
            command::Attributes::RecordMarkerCommandAttributes(marker) => {
                t.add(history_event::Attributes::MarkerRecordedEventAttributes(
                    history::v1::MarkerRecordedEventAttributes {
                        marker_name: marker.marker_name,
                        details: marker.details,
                        header: marker.header,
                        failure: marker.failure,
                        workflow_task_completed_event_id: wft_completed_id,
                    },
                ));
            }
            command::Attributes::CompleteWorkflowExecutionCommandAttributes(_) => {
                t.add_workflow_execution_completed();
            }
            unexpected => panic!("Unexpected command: {unexpected:?}"),
        }
    }
}

const LIVE_RUN_WFID: &str = "fake_wf_id";

/// Awaits `future`, failing the test if it takes more than five seconds
async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap_or_else(|_| panic!("Timed out {what}"))
}

fn started_history() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add(default_wes_attribs());
    t.add_workflow_task_scheduled_and_started();
    t
}

/// A workflow task with the whole history so far and `queries` attached
fn task_with_queries(
    history: &TestHistoryBuilder,
    queries: &[&str],
) -> PollWorkflowTaskQueueResponse {
    let mut task = hist_to_poll_resp(history, LIVE_RUN_WFID, ResponseType::AllHistory).resp;
    task.queries = queries
        .iter()
        .map(|id| {
            (
                (*id).to_owned(),
                WorkflowQuery {
                    query_type: "query-type".to_owned(),
                    ..Default::default()
                },
            )
        })
        .collect();
    task
}

/// Drives a [SignalAfterLocalActivitiesWf] run through a mocked worker, building each workflow task
/// from the history recorded from the completions before it, and checks that replaying the final
/// history produces the activations workflow code received live.
struct LiveRun {
    core: temporalio_sdk_core::Worker,
    history: TestHistoryBuilder,
    tasks: mpsc::UnboundedSender<PollWorkflowTaskQueueResponse>,
    completions: mpsc::UnboundedReceiver<WorkflowTaskCompletion>,
    task_failures: mpsc::UnboundedReceiver<String>,
    workflow: SignalAfterLocalActivitiesWf,
    /// Jobs of each activation workflow code received live, leaving out evictions and replays
    live: Vec<Vec<&'static str>>,
}

impl LiveRun {
    fn new(
        history: TestHistoryBuilder,
        workflow: SignalAfterLocalActivitiesWf,
        max_cached_workflows: usize,
    ) -> Self {
        let first_task = task_with_queries(&history, &[]);
        let (tasks, next_tasks) = mpsc::unbounded_channel();
        let task_stream = stream::iter([first_task])
            .chain(stream::unfold(next_tasks, |mut rx| async {
                rx.recv().await.map(|task| (task, rx))
            }));
        let (completion_tx, completions) = mpsc::unbounded_channel();
        let (failure_tx, task_failures) = mpsc::unbounded_channel();
        let mut client = mock_worker_client();
        client
            .expect_complete_workflow_task()
            .returning(move |completion, _| {
                let _ = completion_tx.send(completion);
                Ok(Default::default())
            });
        client
            .expect_fail_workflow_task()
            .returning(move |_, cause, failure| {
                let _ = failure_tx.send(format!("{cause:?}: {failure:?}"));
                Ok(Default::default())
            });
        client
            .expect_respond_legacy_query()
            .returning(|_, _| Ok(Default::default()));
        let mut mock = MocksHolder::from_client_with_custom(
            client,
            Some(task_stream),
            None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
            None::<Vec<QueueResponse<PollNexusTaskQueueResponse>>>,
        );
        mock.worker_cfg(|wc| wc.max_cached_workflows = max_cached_workflows);
        Self {
            core: mock_worker(mock),
            history,
            tasks,
            completions,
            task_failures,
            workflow,
            live: vec![],
        }
    }

    /// Sends the next workflow task, with `queries` attached
    fn send_task(&self, queries: &[&str]) {
        self.tasks
            .send(task_with_queries(&self.history, queries))
            .unwrap();
    }

    /// Sends the task forced by the completion that answered the queries, with a signal that
    /// arrived before it started
    fn send_forced_task_after_signal(&mut self, queries: &[&str]) {
        self.history.add_workflow_task_scheduled();
        self.history.add_we_signaled("signal", vec![]);
        self.history.add_workflow_task_started();
        self.send_task(queries);
    }

    async fn poll(&self) -> WorkflowActivation {
        within(
            "polling for an activation",
            self.core.poll_workflow_activation(),
        )
        .await
        .unwrap()
    }

    /// Returns the next activation that carries queries or that workflow code receives live,
    /// completing evictions and replayed activations before it
    async fn next_activation(&mut self) -> WorkflowActivation {
        loop {
            let activation = self.poll().await;
            if activation.eviction_reason().is_some() {
                self.complete_eviction(activation).await;
            } else if !query_ids(&activation).is_empty() {
                return activation;
            } else if activation.is_replaying {
                self.complete(&activation).await;
            } else {
                self.live.push(job_names(&activation));
                return activation;
            }
        }
    }

    async fn complete(&mut self, activation: &WorkflowActivation) {
        let completion = self.workflow.activate(activation);
        within(
            "completing an activation",
            self.core.complete_workflow_activation(completion),
        )
        .await
        .unwrap();
    }

    async fn complete_eviction(&mut self, eviction: WorkflowActivation) {
        within(
            "completing an eviction",
            self.core
                .complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id)),
        )
        .await
        .unwrap();
        self.workflow = self.workflow.restarted();
    }

    /// Answers the queries in `activation`, sending `commands` along with the responses
    async fn answer_queries(&self, activation: WorkflowActivation, mut commands: Vec<Variant>) {
        commands.extend(
            query_ids(&activation)
                .into_iter()
                .map(|id| query_ok(id, "answer")),
        );
        within(
            "answering queries",
            self.core
                .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                    activation.run_id,
                    commands,
                )),
        )
        .await
        .unwrap();
    }

    async fn poll_local_activity(&self) -> ActivityTask {
        within(
            "polling for a local activity",
            self.core.poll_activity_task(),
        )
        .await
        .unwrap()
    }

    async fn complete_local_activity_with(
        &self,
        task: ActivityTask,
        result: ActivityExecutionResult,
    ) {
        within(
            "completing a local activity",
            self.core.complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(result),
            }),
        )
        .await
        .unwrap();
    }

    async fn complete_local_activity(&self, task: ActivityTask) {
        self.complete_local_activity_with(task, ActivityExecutionResult::ok(vec![1].into()))
            .await;
    }

    /// Waits for the next workflow task completion and records it in the history
    async fn next_completion(&mut self) -> WorkflowTaskCompletion {
        let completion = within("waiting for a workflow task completion", async {
            select! {
                completion = self.completions.recv() => completion.unwrap(),
                failure = self.task_failures.recv() => {
                    panic!("Workflow task failed: {}", failure.unwrap())
                }
            }
        })
        .await;
        record_completion(&mut self.history, &completion);
        completion
    }

    /// Completes the first workflow task, then sends the next one with `queries` attached, in which
    /// workflow code schedules its local activities. Returns the query activation, dispatched while
    /// the local activities run, and their tasks.
    async fn run_local_activities_with_queries(
        &mut self,
        queries: &[&str],
    ) -> (WorkflowActivation, Vec<ActivityTask>) {
        let activation = self.next_activation().await;
        self.complete(&activation).await;
        self.next_completion().await;
        if self.workflow.command_free_start {
            self.history.add_we_signaled("signal", vec![]);
        } else {
            self.history
                .add_timer_fired(self.history.current_event_id(), "1".to_owned());
        }
        self.history.add_workflow_task_scheduled_and_started();
        self.send_task(queries);
        let activation = self.next_activation().await;
        self.complete(&activation).await;
        let query = self.next_activation().await;
        let mut expected_queries = queries.to_vec();
        expected_queries.sort_unstable();
        assert_eq!(query_ids(&query), expected_queries);
        let mut local_activities = vec![];
        for _ in 0..self.workflow.local_activities {
            local_activities.push(self.poll_local_activity().await);
        }
        (query, local_activities)
    }

    /// Completes the eviction that follows the workflow's completion, so that it doesn't race the
    /// worker's shutdown, then shuts the worker down.
    async fn evict_and_shut_down(mut self) -> Self {
        let eviction = self.poll().await;
        assert!(
            eviction.eviction_reason().is_some(),
            "Expected an eviction, got {eviction:?}"
        );
        self.complete_eviction(eviction).await;
        within(
            "shutting the worker down",
            drain_pollers_and_shutdown(&self.core),
        )
        .await;
        self
    }

    /// Shuts the worker down and checks that replaying the recorded history produces the
    /// activations workflow code received live
    async fn assert_replay_matches_live(self) {
        let LiveRun {
            core,
            history,
            workflow,
            live,
            ..
        } = self.evict_and_shut_down().await;
        within("finalizing the worker's shutdown", core.finalize_shutdown()).await;
        let replay = init_core_replay_preloaded(LIVE_RUN_WFID, [HistoryForReplay::from(history)]);
        let mut workflow = workflow.restarted();
        let mut replayed = vec![];
        loop {
            let activation = match within("polling replay", replay.poll_workflow_activation()).await
            {
                Ok(activation) => activation,
                Err(PollError::ShutDown) => break,
                Err(e) => panic!("Replay poll failed: {e:?}"),
            };
            if let Some(reason) = activation.eviction_reason() {
                assert_ne!(
                    reason,
                    EvictionReason::Nondeterminism,
                    "Replay diverged from live execution: {activation:?}"
                );
                let completion = WorkflowActivationCompletion::empty(activation.run_id);
                within(
                    "completing a replay eviction",
                    replay.complete_workflow_activation(completion),
                )
                .await
                .unwrap();
                continue;
            }
            replayed.push(job_names(&activation));
            let completion = workflow.activate(&activation);
            within(
                "completing a replayed activation",
                replay.complete_workflow_activation(completion),
            )
            .await
            .unwrap();
        }
        assert_eq!(replayed, live);
    }
}

/// Core can answer queries attached to a workflow task while a local activity the task started is
/// still running. If the local activity finished while lang was answering them, Core used to report
/// its marker together with the query responses, but delivered its result to lang only in the
/// forced next workflow task, along with a signal that had arrived in between. Replay delivers a
/// result in the workflow task whose completion holds its marker, before that signal, so a workflow
/// checking for the signal after the local activity took a different branch on replay and failed
/// with a nondeterminism error. Results arriving once the query responses went out were already
/// held for the next workflow task.
#[rstest::rstest]
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn la_resolved_while_answering_attached_query_replays_like_live(
    #[values(0, 1)] max_cached_workflows: usize,
    #[values(false, true)] la_finishes_after_answer: bool,
    #[values(false, true)] query_on_forced_task: bool,
) {
    let mut run = LiveRun::new(
        started_history(),
        SignalAfterLocalActivitiesWf::new(1),
        max_cached_workflows,
    );
    let (query, mut local_activities) = run.run_local_activities_with_queries(&["q"]).await;
    let local_activity = local_activities.pop().unwrap();
    if la_finishes_after_answer {
        run.answer_queries(query, vec![]).await;
        run.complete_local_activity(local_activity).await;
    } else {
        run.complete_local_activity(local_activity).await;
        run.answer_queries(query, vec![]).await;
    }
    let query_completion = run.next_completion().await;
    assert_eq!(query_completion.query_responses.len(), 1);
    assert!(query_completion.force_create_new_workflow_task);
    // The marker belongs to the workflow task in which lang receives the result.
    assert!(query_completion.commands.is_empty());

    let forced_task_queries: &[&str] = if query_on_forced_task { &["q2"] } else { &[] };
    run.send_forced_task_after_signal(forced_task_queries);
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    if query_on_forced_task {
        let query = run.next_activation().await;
        assert_eq!(query_ids(&query), ["q2"]);
        run.answer_queries(query, vec![]).await;
    }
    let last_completion = run.next_completion().await;
    assert_eq!(
        last_completion.query_responses.len(),
        usize::from(query_on_forced_task)
    );
    assert_eq!(
        command_types(&last_completion),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert_eq!(
        run.live,
        [
            vec!["init"],
            vec!["timer"],
            vec!["signal", "local activity"]
        ]
    );
    run.assert_replay_matches_live().await;
}

/// Like [la_resolved_while_answering_attached_query_replays_like_live], with the local activity
/// running across a workflow task heartbeat and the query attached to the heartbeat's task.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn la_resolved_while_answering_query_attached_to_heartbeat_task_replays_like_live() {
    let mut history = TestHistoryBuilder::default();
    history.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    history.add_workflow_task_scheduled_and_started();
    let mut run = LiveRun::new(history, SignalAfterLocalActivitiesWf::new(1), 1);
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    run.next_completion().await;
    run.history
        .add_timer_fired(run.history.current_event_id(), "1".to_owned());
    run.history.add_workflow_task_scheduled_and_started();
    run.send_task(&[]);
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    let local_activity = run.poll_local_activity().await;

    // Core reports the heartbeat while lang polls, and the server attaches the query to the task
    // that follows it.
    let LiveRun {
        core,
        history,
        tasks,
        completions,
        ..
    } = &mut run;
    let (query, ()) = join!(
        within("polling for the query", core.poll_workflow_activation()),
        async {
            let heartbeat = within("waiting for the heartbeat", completions.recv())
                .await
                .unwrap();
            assert!(heartbeat.force_create_new_workflow_task);
            assert!(heartbeat.commands.is_empty());
            record_completion(history, &heartbeat);
            history.add_workflow_task_scheduled_and_started();
            tasks.send(task_with_queries(history, &["q"])).unwrap();
        }
    );
    let query = query.unwrap();
    assert_eq!(query_ids(&query), ["q"]);
    run.complete_local_activity(local_activity).await;
    run.answer_queries(query, vec![]).await;
    let query_completion = run.next_completion().await;
    assert!(query_completion.force_create_new_workflow_task);
    assert!(query_completion.commands.is_empty());

    run.send_forced_task_after_signal(&[]);
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    let last_completion = run.next_completion().await;
    assert_eq!(
        command_types(&last_completion),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert_eq!(
        run.live,
        [
            vec!["init"],
            vec!["timer"],
            vec!["signal", "local activity"]
        ]
    );
    run.assert_replay_matches_live().await;
}

/// Two local activities finish while lang answers two queries attached to the same task, with the
/// workflow cache disabled. The run must stay cached until the forced task delivers both results.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn las_resolved_while_answering_attached_queries_without_cache_replay_like_live() {
    let mut run = LiveRun::new(started_history(), SignalAfterLocalActivitiesWf::new(2), 0);
    let (query, local_activities) = run.run_local_activities_with_queries(&["q1", "q2"]).await;
    for local_activity in local_activities {
        run.complete_local_activity(local_activity).await;
    }
    run.answer_queries(query, vec![]).await;
    let query_completion = run.next_completion().await;
    assert_eq!(query_completion.query_responses.len(), 2);
    assert!(query_completion.force_create_new_workflow_task);
    assert!(query_completion.commands.is_empty());

    run.send_forced_task_after_signal(&[]);
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    let last_completion = run.next_completion().await;
    assert_eq!(
        command_types(&last_completion),
        [
            CommandType::RecordMarker,
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert_eq!(
        run.live,
        [
            vec!["init"],
            vec!["timer"],
            vec!["signal", "local activity", "local activity"]
        ]
    );
    run.assert_replay_matches_live().await;
}

/// Of two local activities, one finishes while lang answers the attached query and the other only
/// once lang has completed the forced task's first activation. Replay must deliver them in the same
/// activations as live execution did.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn las_resolved_while_answering_query_and_after_forced_task_activation_replay_like_live() {
    let mut run = LiveRun::new(started_history(), SignalAfterLocalActivitiesWf::new(2), 1);
    let (query, mut local_activities) = run.run_local_activities_with_queries(&["q"]).await;
    let second = local_activities.pop().unwrap();
    run.complete_local_activity(local_activities.pop().unwrap())
        .await;
    run.answer_queries(query, vec![]).await;
    let query_completion = run.next_completion().await;
    assert!(query_completion.force_create_new_workflow_task);
    assert!(query_completion.commands.is_empty());

    run.send_forced_task_after_signal(&[]);
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    run.complete_local_activity(second).await;
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    let last_completion = run.next_completion().await;
    assert_eq!(
        command_types(&last_completion),
        [
            CommandType::RecordMarker,
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert_eq!(
        run.live,
        [
            vec!["init"],
            vec!["timer"],
            vec!["signal", "local activity"],
            vec!["local activity"]
        ]
    );
    run.assert_replay_matches_live().await;
}

/// Commands sent with query responses can still resolve a local activity, here a try-cancel of a
/// running one, which resolves synchronously in Core. The completion must then force a new
/// workflow task on which to deliver the resolution.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn la_cancelled_with_query_responses_forces_new_task() {
    let mut run = LiveRun::new(started_history(), SignalAfterLocalActivitiesWf::new(1), 1);
    let (query, mut local_activities) = run.run_local_activities_with_queries(&["q"]).await;
    let local_activity = local_activities.pop().unwrap();
    run.answer_queries(
        query,
        vec![Variant::RequestCancelLocalActivity(
            RequestCancelLocalActivity { seq: 1 },
        )],
    )
    .await;
    let query_completion = run.next_completion().await;
    assert!(query_completion.force_create_new_workflow_task);
    let cancel = run.poll_local_activity().await;
    assert_eq!(cancel.task_token, local_activity.task_token);
    assert_matches!(cancel.variant, Some(act_task::Variant::Cancel(_)));
    run.complete_local_activity_with(cancel, ActivityExecutionResult::cancel_from_details(None))
        .await;

    run.send_forced_task_after_signal(&[]);
    let activation = run.next_activation().await;
    assert_eq!(job_names(&activation), ["signal", "local activity"]);
    run.complete(&activation).await;
    run.next_completion().await;
    let run = run.evict_and_shut_down().await;
    within(
        "finalizing the worker's shutdown",
        run.core.finalize_shutdown(),
    )
    .await;
}

/// A legacy query task can be processed between the completion that answered attached queries and
/// the task it forced. Local activity results held for that task must not reach lang in the query
/// pass, ahead of the task's own events, nor be held past the task's first activation.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn legacy_query_before_forced_task_leaves_la_result_held() {
    // The run still counts the held local activity as running, so it only dispatches the legacy
    // query once its workflow task heartbeat timer fires. A short timeout keeps that wait short.
    let mut history = TestHistoryBuilder::default();
    history.add_wfe_started_with_wft_timeout(Duration::from_secs(1));
    history.add_workflow_task_scheduled_and_started();
    let mut run = LiveRun::new(history, SignalAfterLocalActivitiesWf::new(1), 1);
    let (query, mut local_activities) = run.run_local_activities_with_queries(&["q"]).await;
    run.complete_local_activity(local_activities.pop().unwrap())
        .await;
    run.answer_queries(query, vec![]).await;
    run.next_completion().await;

    run.history.add_workflow_task_scheduled();
    let mut legacy_query = task_with_queries(&run.history, &[]);
    legacy_query.query = Some(WorkflowQuery {
        query_type: "query-type".to_owned(),
        ..Default::default()
    });
    legacy_query.started_event_id = 0;
    run.tasks.send(legacy_query).unwrap();
    let activation = run.poll().await;
    assert_eq!(activation.jobs.len(), 1);
    assert_eq!(query_ids(&activation), [LEGACY_QUERY_ID]);
    run.answer_queries(activation, vec![]).await;

    run.history.add_we_signaled("signal", vec![]);
    run.history.add_workflow_task_started();
    run.send_task(&[]);
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    let last_completion = run.next_completion().await;
    assert_eq!(
        command_types(&last_completion),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert_eq!(
        run.live,
        [
            vec!["init"],
            vec!["timer"],
            vec!["signal", "local activity"]
        ]
    );
    run.assert_replay_matches_live().await;
}

/// Evicting a run while it holds a local activity result for the next workflow task drops the
/// result, so the local activity executes again, once, when the run is replayed. Local activities
/// run at least once, so this is allowed, and Core logs a warning when it happens.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn eviction_while_la_result_held_executes_la_again() {
    let mut run = LiveRun::new(started_history(), SignalAfterLocalActivitiesWf::new(1), 1);
    let (query, mut local_activities) = run.run_local_activities_with_queries(&["q"]).await;
    let run_id = query.run_id.clone();
    run.complete_local_activity(local_activities.pop().unwrap())
        .await;
    run.answer_queries(query, vec![]).await;
    let query_completion = run.next_completion().await;
    assert!(query_completion.commands.is_empty());

    run.core.request_workflow_eviction(&run_id);
    let eviction = run.poll().await;
    assert_matches!(
        eviction.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    run.complete_eviction(eviction).await;

    run.send_forced_task_after_signal(&[]);
    // Once replay has caught up, the local activity executes again.
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    let local_activity = run.poll_local_activity().await;
    run.complete_local_activity(local_activity).await;
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    let last_completion = run.next_completion().await;
    assert_eq!(
        command_types(&last_completion),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), run.core.poll_activity_task())
            .await
            .is_err(),
        "The local activity executed again more than once"
    );
    assert_eq!(
        run.live,
        [
            vec!["init"],
            vec!["timer"],
            vec!["signal"],
            vec!["local activity"]
        ]
    );
    run.assert_replay_matches_live().await;
}

/// Like [la_resolved_while_answering_attached_query_replays_like_live], but the task before the
/// queried one records no command. Replay then takes the command-free completion that answered the
/// query, directly followed by the forced task, for a workflow task heartbeat and merges the two
/// tasks. This fails on main too: there the early marker splits the activations instead, and a
/// result arriving after the query is answered hits the same merge.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[ignore = "Replay merges a command-free task with the next one: temporalio/sdk-rust#1737"]
#[tokio::test]
async fn la_resolved_while_answering_query_after_command_free_task_replays_like_live() {
    let workflow = SignalAfterLocalActivitiesWf {
        command_free_start: true,
        ..SignalAfterLocalActivitiesWf::new(1)
    };
    let mut run = LiveRun::new(started_history(), workflow, 1);
    let (query, mut local_activities) = run.run_local_activities_with_queries(&["q"]).await;
    run.complete_local_activity(local_activities.pop().unwrap())
        .await;
    run.answer_queries(query, vec![]).await;
    run.next_completion().await;
    run.send_forced_task_after_signal(&[]);
    let activation = run.next_activation().await;
    run.complete(&activation).await;
    run.next_completion().await;
    assert_eq!(
        run.live,
        [
            vec!["init"],
            vec!["signal"],
            vec!["signal", "local activity"]
        ]
    );
    run.assert_replay_matches_live().await;
}

#[rstest::rstest]
#[case::impossible_query_in_task(true)]
#[case::real_history(false)]
#[tokio::test]
async fn la_resolve_during_legacy_query_does_not_combine(#[case] impossible_query_in_task: bool) {
    // Ensures we do not send an activation with a legacy query and any other work, which should
    // never happen, but there was an issue where an LA resolving could trigger that.
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add(default_wes_attribs());
    // Since we don't send queries with start workflow, need one workflow task of something else
    // b/c we want to get an activation with a job and a nonlegacy query
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());

    // nonlegacy query got here & LA started here
    // then next task is incremental w/ legacy query (for impossible query case)
    t.add_full_wf_task();

    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2));
            pr.queries = HashMap::new();
            pr.queries.insert(
                "q1".to_string(),
                WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                },
            );
            pr
        },
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(2));
            // Strip beginning of history so the only events are WFT sched/started, we need to look
            // like we hit the cache
            {
                let h = pr.history.as_mut().unwrap();
                h.events = h.events.split_off(6);
            }
            // In the nonsense server response case, we attach a legacy query, otherwise this
            // response looks like a normal response to a forced WFT heartbeat.
            if impossible_query_in_task {
                pr.query = Some(WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                });
            }
            pr
        },
    ];
    let mut mock = mock_worker_client();
    if impossible_query_in_task {
        mock.expect_respond_legacy_query()
            .times(1)
            .returning(move |_, _| Ok(Default::default()));
    }
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let taskmap = mock.outstanding_task_map.clone().unwrap();
    let core = mock_worker(mock);

    let wf_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
            },]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();

        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(_)),
            },]
        );
        // We want to make sure the weird-looking query gets received while we're working on other
        // stuff, so that we don't see the workflow complete and choose to evict.
        taskmap.release_run(&task.run_id);
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            schedule_local_activity_cmd(
                1,
                "act-id",
                ProtoActivityCancellationType::TryCancel,
                Duration::from_secs(60),
            ),
        ))
        .await
        .unwrap();

        let mut resolved_activity = false;
        let mut answered_query = false;
        for _ in 0..2 {
            let task = core.poll_workflow_activation().await.unwrap();
            assert_eq!(task.jobs.len(), 1);
            let run_id = task.run_id.clone();
            match task.jobs[0].variant.as_ref() {
                Some(workflow_activation_job::Variant::ResolveActivity(_)) => {
                    assert!(!resolved_activity);
                    resolved_activity = true;
                    core.complete_execution(&run_id).await;
                }
                Some(workflow_activation_job::Variant::QueryWorkflow(q)) if q.query_id == "q1" => {
                    assert!(!answered_query);
                    answered_query = true;
                    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                        run_id,
                        query_ok("q1", "whatev"),
                    ))
                    .await
                    .unwrap();
                }
                unexpected => panic!("Unexpected activation job: {unexpected:?}"),
            }
        }
        assert!(resolved_activity && answered_query);

        if impossible_query_in_task {
            // finish last query
            let task = core.poll_workflow_activation().await.unwrap();
            core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                query_ok(LEGACY_QUERY_ID, "whatev"),
            ))
            .await
            .unwrap();
        }
    };
    let act_fut = async {
        let act_task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    };

    join!(wf_fut, act_fut);
    core.drain_pollers_and_shutdown().await;
}

#[rstest::rstest]
#[case::pending_workflow_task(false, false)]
#[case::started_workflow_task(true, false)]
#[case::cached_pending_workflow_task(false, true)]
#[case::cached_started_workflow_task(true, true)]
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn legacy_query_must_not_execute_local_activity(
    #[case] task_started: bool,
    #[case] cached: bool,
) {
    let mut history = TestHistoryBuilder::default();
    history.add(default_wes_attribs());
    history.add_full_wf_task();
    let scheduled = history.add_activity_task_scheduled("remote");
    let started = history.add_activity_task_started(scheduled);
    history.add_activity_task_completed(scheduled, started, vec![1].into());
    history.add_workflow_task_scheduled();
    if task_started {
        history.add_workflow_task_started();
    }
    // A replacement worker can receive a legacy query with the activity completion in its
    // history, before the workflow task that will commit the resulting local activity.
    let mut query = hist_to_poll_resp(
        &history,
        "legacy_query_local_activity",
        ResponseType::AllHistory,
    )
    .resp;
    query.query = Some(WorkflowQuery {
        query_type: "query-type".to_owned(),
        ..Default::default()
    });
    query.started_event_id = 0;
    if !task_started {
        history.add_workflow_task_started();
    }
    let workflow_task = hist_to_poll_resp(
        &history,
        "legacy_query_local_activity",
        ResponseType::AllHistory,
    )
    .resp;
    let (send_workflow_task, workflow_task_ready) = oneshot::channel::<()>();
    let tasks = stream::iter([query])
        .chain(stream::once(async move {
            workflow_task_ready.await.unwrap();
            workflow_task
        }))
        .chain(stream::pending());
    let mut client = mock_worker_client();
    client
        .expect_respond_legacy_query()
        .times(1)
        .returning(|_, _| Ok(Default::default()));
    client
        .expect_complete_workflow_task()
        .times(usize::from(cached))
        .returning(|completion, _| {
            assert_eq!(
                completion
                    .commands
                    .iter()
                    .map(|cmd| cmd.command_type())
                    .collect::<Vec<_>>(),
                [
                    CommandType::RecordMarker,
                    CommandType::CompleteWorkflowExecution
                ]
            );
            Ok(Default::default())
        });
    let mut mock = MocksHolder::from_client_with_custom(
        client,
        Some(tasks),
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        None::<Vec<QueueResponse<PollNexusTaskQueueResponse>>>,
    );
    mock.worker_cfg(|wc| wc.max_cached_workflows = usize::from(cached));
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    assert!(activation.is_replaying);
    assert_matches!(
        activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        schedule_activity_cmd(
            1,
            "task-queue",
            "remote",
            ProtoActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::ZERO,
        ),
    ))
    .await
    .unwrap();
    let activation = core.poll_workflow_activation().await.unwrap();
    assert!(activation.is_replaying);
    assert_matches!(
        activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        schedule_local_activity_cmd(
            2,
            "local",
            ProtoActivityCancellationType::TryCancel,
            Duration::from_secs(60),
        ),
    ))
    .await
    .unwrap();

    // Dispatch must lose to query handling without relying on a timing window.
    let query_and_eviction = async {
        let activation = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            activation.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] if q.query_id == LEGACY_QUERY_ID
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            activation.run_id,
            query_ok(LEGACY_QUERY_ID, "answer"),
        ))
        .await
        .unwrap();
        if cached {
            return;
        }
        let activation = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            activation.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            }]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
            .await
            .unwrap();
    };
    select! {
        biased;
        task = core.poll_activity_task() => {
            panic!("Legacy query dispatched a local activity: {:?}", task.unwrap());
        }
        () = query_and_eviction => {}
    }
    if cached {
        send_workflow_task.send(()).unwrap();
        let execute_activity = async {
            let activity = core.poll_activity_task().await.unwrap();
            assert_matches!(
                activity.variant,
                Some(act_task::Variant::Start(ref start))
                    if start.is_local && start.activity_id == "local"
            );
            core.complete_activity_task(ActivityTaskCompletion {
                task_token: activity.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
        };
        let finish_workflow = async {
            let activation = core.poll_workflow_activation().await.unwrap();
            assert!(!activation.is_replaying);
            assert_matches!(
                activation.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
                }]
            );
            core.complete_execution(&activation.run_id).await;
        };
        join!(execute_activity, finish_workflow);
    }
    core.drain_pollers_and_shutdown().await;
}

/// While a workflow task waits on a local activity, core holds it with nothing outstanding to lang
/// (since #1442). A legacy query task (a query the server sends on its own, when it thinks the
/// workflow is idle) that arrives then must wait until that workflow task is reported. Core used
/// to apply it at once, replacing the workflow task ("Trying to send a new WFT for a run which
/// already has one!"), which was then never reported and timed out on the server. Under load, the
/// retried workflow task can fail with a nondeterminism error and leave the workflow stuck.
#[tokio::test]
async fn legacy_query_while_waiting_on_la_is_buffered() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add(default_wes_attribs());
    t.add_full_wf_task();
    let workflow_task = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)).resp;
    let mut legacy_query = workflow_task.clone();
    legacy_query.history = Some(History { events: vec![] });
    legacy_query.query = Some(WorkflowQuery {
        query_type: "query-type".to_string(),
        query_args: Some(b"hi".into()),
        header: None,
    });

    // The test hands core its tasks: the workflow task, then the query when the test sends it.
    // Core asks for a task only after it has handled the one before (history fetches run one at a
    // time), so its asking after the query means it has handled the query. If core stops instead,
    // the stream is dropped.
    let (send_query, query_sent) = oneshot::channel::<()>();
    let (query_handled_tx, query_handled) = oneshot::channel::<()>();
    let tasks = stream::iter([workflow_task])
        .chain(stream::once(async move {
            query_sent.await.unwrap();
            legacy_query
        }))
        .chain(stream::once(async move {
            query_handled_tx.send(()).unwrap();
            std::future::pending().await
        }));

    let mut client = mock_worker_client();
    client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|_, _| Ok(Default::default()));
    client
        .expect_respond_legacy_query()
        .times(1)
        .returning(|_, _| Ok(Default::default()));
    let mut mock = MocksHolder::from_client_with_custom(
        client,
        Some(tasks),
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        None::<Vec<QueueResponse<PollNexusTaskQueueResponse>>>,
    );
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 10;
        wc.fetching_concurrency = 1;
    });
    let core = mock_worker(mock);

    // The workflow starts a local activity. Core replies at once and keeps the workflow task open.
    let task = core.poll_workflow_activation().await.unwrap();
    let run_id = task.run_id;
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        run_id.clone(),
        schedule_local_activity_cmd(
            1,
            "act-id",
            ProtoActivityCancellationType::TryCancel,
            Duration::from_secs(60),
        ),
    ))
    .await
    .unwrap();
    let local_activity = core.poll_activity_task().await.unwrap();

    // The legacy query arrives while the local activity runs.
    send_query.send(()).unwrap();
    query_handled
        .await
        .expect("core stopped while handling the legacy query");

    // The local activity finishes, and the workflow task is reported.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: local_activity.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        run_id.clone(),
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();

    // Only then is the query answered.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] if q.query_id == LEGACY_QUERY_ID
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        run_id,
        query_ok(LEGACY_QUERY_ID, "whatev"),
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn test_schedule_to_start_timeout() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::ToTaskNum(1)], mock);

    #[workflow]
    #[derive(Default)]
    struct TestScheduleToStartTimeoutWf;

    #[workflow_methods]
    impl TestScheduleToStartTimeoutWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let la_res = ctx
                .execute_local_activity(
                    StdActivities::echo,
                    "hi".to_string(),
                    LocalActivityOptions::builder()
                        .maybe_schedule_to_start_timeout(prost_dur!(from_nanos(1)))
                        .build(),
                )
                .await;
            assert!(la_res.is_err());
            if let Err(ActivityExecutionError::Failed(ref fail)) = la_res {
                let Some(timeout) = fail.as_timeout() else {
                    panic!("expected timeout cause, got {fail:?}");
                };
                assert_eq!(timeout.timeout_type(), TimeoutType::ScheduleToStart);
                assert_eq!(fail.activity_type(), Some(StdActivities::echo.name()));
            }
            Ok(())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |w| w.max_cached_workflows = 1,
        |options| {
            options
                .register_workflow::<TestScheduleToStartTimeoutWf>()
                .unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[case::sched_to_start(true)]
#[case::sched_to_close(false)]
#[tokio::test]
async fn test_schedule_to_start_timeout_not_based_on_original_time(
    #[case] is_sched_to_start: bool,
) {
    // We used to carry over the schedule time of LAs from the "original" schedule time if these LAs
    // created newly after backing off across a timer. That was a mistake, since schedule-to-start
    // timeouts should apply to when the new attempt was scheduled. This test verifies:
    // * we don't time out on s-t-s timeouts because of that, when the param is true.
    // * we do properly time out on s-t-c timeouts when the param is false

    let schedule_to_close_timeout = Some(if is_sched_to_start {
        Duration::from_secs(60 * 60)
    } else {
        Duration::from_secs(10 * 60)
    });

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.set_wf_input(
        (is_sched_to_start, schedule_to_close_timeout)
            .as_json_payload()
            .unwrap(),
    );
    t.add_full_wf_task();
    let orig_sched = SystemTime::now().sub(Duration::from_secs(60 * 20));
    t.add_local_activity_marker(
        1,
        "1",
        None,
        Some(Failure::application_failure("la failed".to_string(), false)),
        |deets| {
            // Really old schedule time, which should _not_ count against schedule_to_start
            deets.original_schedule_time = Some(orig_sched.into());
            // Backoff value must be present since we're simulating timer backoff
            deets.backoff = Some(prost_dur!(from_secs(100)));
        },
    );
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);

    #[workflow]
    #[derive(Default)]
    struct TestScheduleToStartTimeoutNotBasedOnOriginalTimeWf;

    #[workflow_methods]
    impl TestScheduleToStartTimeoutNotBasedOnOriginalTimeWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(
            ctx: &mut WorkflowContext<Self>,
            input: (bool, Option<Duration>),
        ) -> WorkflowResult<()> {
            let (is_sched_to_start, schedule_to_close_timeout) = input;
            let la_res = ctx
                .execute_local_activity(
                    StdActivities::echo,
                    "hi".to_string(),
                    LocalActivityOptions::builder()
                        .retry_policy(
                            RetryPolicy {
                                initial_interval: Some(prost_dur!(from_millis(50))),
                                backoff_coefficient: 1.2,
                                maximum_interval: None,
                                maximum_attempts: 5,
                                non_retryable_error_types: vec![],
                            }
                            .into(),
                        )
                        .schedule_to_start_timeout(Duration::from_secs(60))
                        .maybe_schedule_to_close_timeout(schedule_to_close_timeout)
                        .build(),
                )
                .await;
            if is_sched_to_start {
                assert!(la_res.is_ok());
            } else if let Err(ActivityExecutionError::Failed(ref fail)) = la_res {
                let Some(timeout) = fail.as_timeout() else {
                    panic!("expected timeout cause, got {fail:?}");
                };
                assert_eq!(timeout.timeout_type(), TimeoutType::ScheduleToClose);
            }
            Ok(())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |w| w.max_cached_workflows = 1,
        |options| {
            options
                .register_workflow::<TestScheduleToStartTimeoutNotBasedOnOriginalTimeWf>()
                .unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[tokio::test]
async fn start_to_close_timeout_allows_retries(#[values(true, false)] la_completes: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.set_wf_input(la_completes.as_json_payload().unwrap());
    t.add_full_wf_task();
    if la_completes {
        t.add_local_activity_marker(1, "1", Some("hi".into()), None, |_| {});
    } else {
        t.add_local_activity_marker(
            1,
            "1",
            None,
            Some(Failure::timeout(ProtoTimeoutType::StartToClose)),
            |_| {},
        );
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::ToTaskNum(1), ResponseType::AllHistory],
        mock,
    );

    #[workflow]
    #[derive(Default)]
    struct StartToCloseTimeoutAllowsRetriesWf;

    #[workflow_methods]
    impl StartToCloseTimeoutAllowsRetriesWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>, la_completes: bool) -> WorkflowResult<()> {
            let la_res = ctx
                .execute_local_activity(
                    ActivityWithRetriesAndCancellation::go,
                    (),
                    LocalActivityOptions::builder()
                        .retry_policy(
                            RetryPolicy {
                                initial_interval: Some(prost_dur!(from_millis(20))),
                                backoff_coefficient: 1.0,
                                maximum_interval: None,
                                maximum_attempts: 5,
                                non_retryable_error_types: vec![],
                            }
                            .into(),
                        )
                        .start_to_close_timeout(prost_dur!(from_millis(25)))
                        .build(),
                )
                .await;
            if la_completes {
                assert!(la_res.is_ok(), "Result should be ok was {la_res:?}");
            } else if let Err(ActivityExecutionError::Failed(ref fail)) = la_res {
                let Some(timeout) = fail.as_timeout() else {
                    panic!("expected timeout cause, got {fail:?}");
                };
                assert_eq!(timeout.timeout_type(), TimeoutType::StartToClose);
            }
            Ok(())
        }
    }

    let attempts = Arc::new(AtomicUsize::new(0));
    let cancels = Arc::new(AtomicUsize::new(0));

    struct ActivityWithRetriesAndCancellation {
        attempts: Arc<AtomicUsize>,
        cancels: Arc<AtomicUsize>,
        la_completes: bool,
    }
    #[activities]
    impl ActivityWithRetriesAndCancellation {
        #[activity(name = DEFAULT_ACTIVITY_TYPE)]
        async fn go(self: Arc<Self>, ctx: ActivityContext) -> Result<RawValue, ActivityError> {
            // Timeout the first 4 attempts, or all of them if we intend to fail
            if self.attempts.fetch_add(1, Ordering::AcqRel) < 4 || !self.la_completes {
                select! {
                    _ = tokio::time::sleep(Duration::from_millis(100)) => (),
                    _ = ctx.cancelled() => {
                        self.cancels.fetch_add(1, Ordering::AcqRel);
                        return Err(ActivityError::cancelled());
                    }
                }
            }
            Ok(RawValue::empty())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |w| w.max_cached_workflows = 1,
        |options| {
            options
                .register_workflow::<StartToCloseTimeoutAllowsRetriesWf>()
                .unwrap();
            options.register_activities(ActivityWithRetriesAndCancellation {
                attempts: attempts.clone(),
                cancels: cancels.clone(),
                la_completes,
            });
        },
    );
    worker.run_until_done().await.unwrap();
    assert_eq!(attempts.load(Ordering::Acquire), 5);
    let num_cancels = if la_completes { 4 } else { 5 };
    assert_eq!(cancels.load(Ordering::Acquire), num_cancels);
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn wft_failure_cancels_running_las() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2], mock);
    mh.num_expected_fails = 1;

    #[workflow]
    #[derive(Default)]
    struct WftFailureCancelsRunningLasWf;

    #[workflow_methods]
    impl WftFailureCancelsRunningLasWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let la_handle = ctx.execute_local_activity(
                ActivityThatExpectsCancellation::go,
                (),
                Default::default(),
            );
            temporalio_sdk::workflows::join!(
                async {
                    ctx.timer(Duration::from_secs(1)).await;
                    panic!("ahhh I'm failing wft")
                },
                la_handle
            );
            Ok(())
        }
    }

    struct ActivityThatExpectsCancellation;
    #[activities]
    impl ActivityThatExpectsCancellation {
        #[activity]
        async fn go(ctx: ActivityContext) -> Result<(), ActivityError> {
            let res = tokio::time::timeout(Duration::from_millis(500), ctx.cancelled()).await;
            if res.is_err() {
                panic!("Activity must be cancelled!!!!");
            }
            Err(ActivityError::cancelled())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |w| w.max_cached_workflows = 1,
        |options| {
            options
                .register_workflow::<WftFailureCancelsRunningLasWf>()
                .unwrap();
            options.register_activities(ActivityThatExpectsCancellation);
        },
    );
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn resolved_las_not_recorded_if_wft_fails_many_times() {
    // We shouldn't record any LA results if the workflow activation is repeatedly failing. There
    // was an issue that, because we stop reporting WFT failures after 2 tries, this meant the WFT
    // was not marked as "completed" and the WFT could accidentally be replied to with LA results.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [1.into(), ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    mh.num_expected_fails = 2;
    mh.num_expected_completions = Some(0.into());

    #[workflow]
    #[derive(Default)]
    struct ResolvedLasNotRecordedIfWftFailsManyTimesWf;

    #[workflow_methods]
    impl ResolvedLasNotRecordedIfWftFailsManyTimesWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        #[allow(unreachable_code)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.execute_local_activity(
                StdActivities::echo,
                "hi".to_string(),
                LocalActivityOptions::builder().build(),
            )
            .await?;
            panic!()
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |w| w.max_cached_workflows = 1,
        |options| {
            options
                .register_workflow::<ResolvedLasNotRecordedIfWftFailsManyTimesWf>()
                .unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn local_act_records_nonfirst_attempts_ok() {
    let mut t = TestHistoryBuilder::default();
    let wft_timeout = Duration::from_millis(200);
    t.add_wfe_started_with_wft_timeout(wft_timeout);
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 3], mock);
    let nonfirst_counts = Arc::new(SegQueue::new());
    let nfc_c = nonfirst_counts.clone();
    mh.completion_mock_fn = Some(Box::new(move |c| {
        nfc_c.push(
            c.metering_metadata
                .nonfirst_local_activity_execution_attempts,
        );
        Ok(Default::default())
    }));

    #[workflow]
    #[derive(Default)]
    struct LocalActRecordsNonfirstAttemptsOkWf;

    #[workflow_methods]
    impl LocalActRecordsNonfirstAttemptsOkWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.execute_local_activity(
                StdActivities::always_fail,
                (),
                LocalActivityOptions::builder()
                    .retry_policy(
                        RetryPolicy {
                            initial_interval: Some(prost_dur!(from_millis(10))),
                            backoff_coefficient: 1.0,
                            maximum_interval: None,
                            maximum_attempts: 0,
                            non_retryable_error_types: vec![],
                        }
                        .into(),
                    )
                    .build(),
            )
            .await
            .ok();
            Ok(())
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |wc| {
            wc.max_cached_workflows = 1;
            wc.max_outstanding_workflow_tasks = Some(1);
        },
        |options| {
            options
                .register_workflow::<LocalActRecordsNonfirstAttemptsOkWf>()
                .unwrap();
            options.register_activities(StdActivities);
        },
    );
    worker.run_until_done().await.unwrap();
    assert_eq!(nonfirst_counts.len(), 3);
    // First task's non-first count should, of course, be 0
    assert_eq!(nonfirst_counts.pop().unwrap(), 0);
    // Next two, some nonzero amount which could vary based on test load
    assert!(nonfirst_counts.pop().unwrap() > 0);
    assert!(nonfirst_counts.pop().unwrap() > 0);
}

#[tokio::test]
async fn local_activities_can_be_delivered_during_shutdown() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg(
        wfid,
        t,
        [ResponseType::ToTaskNum(1), ResponseType::AllHistory],
        mock,
        true,
    );
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    // Initiate shutdown once we have the WF activation, but before replying that we want to do an
    // LA
    core.initiate_shutdown();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleLocalActivity {
            seq: 1,
            activity_id: "1".to_string(),
            activity_type: "test_act".to_string(),
            start_to_close_timeout: Some(prost_dur!(from_secs(30))),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollError::ShutDown
    );

    let act_task = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act_task.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    assert_matches!(
        core.poll_activity_task().await.unwrap_err(),
        PollError::ShutDown
    );
}

#[tokio::test]
async fn queries_can_be_received_while_heartbeating() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_full_wf_task();

    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2));
            pr.queries = HashMap::new();
            pr.queries.insert(
                "q1".to_string(),
                WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                },
            );
            pr
        },
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(3));
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            });
            pr
        },
    ];
    let mut mock = mock_worker_client();
    mock.expect_respond_legacy_query()
        .times(1)
        .returning(move |_, _| Ok(Default::default()));
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        schedule_local_activity_cmd(
            1,
            "act-id",
            ProtoActivityCancellationType::TryCancel,
            Duration::from_secs(60),
        ),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
        }]
        if q.query_id == "q1"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok("q1", "whatev"),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
        }]
        if q.query_id == LEGACY_QUERY_ID
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID, "whatev"),
    ))
    .await
    .unwrap();

    // Handle the activity so we can shut down cleanly
    let act_task = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act_task.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();

    core.drain_pollers_and_shutdown().await;
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[case::current_history(false)]
#[case::old_heartbeat_history_replay(true)]
#[tokio::test]
async fn local_activity_after_wf_complete_is_discarded(#[case] old_heartbeat_history: bool) {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    if old_heartbeat_history {
        t.add_full_wf_task();
    }
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let response = if old_heartbeat_history {
        ResponseType::AllHistory
    } else {
        ResponseType::ToTaskNum(1)
    };
    let mut mock_cfg = MockPollCfg::from_resp_batches(wfid, t, [response], mock);
    mock_cfg.make_poll_stream_interminable = true;
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 2);
            assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
            assert_eq!(
                wft.commands[1].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.ignore_evicts_on_shutdown = false;
    });
    let core = mock_worker(mock);

    let barr = Barrier::new(2);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            ScheduleLocalActivity {
                seq: 1,
                activity_id: "1".to_string(),
                activity_type: "test_act".to_string(),
                start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                ..Default::default()
            }
            .into(),
            ScheduleLocalActivity {
                seq: 2,
                activity_id: "2".to_string(),
                activity_type: "test_act".to_string(),
                start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                ..Default::default()
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    let wf_poller = async {
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        barr.wait().await;
        core.complete_execution(&task.run_id).await;
    };

    let at_poller = async {
        let act_task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        let act_task = core.poll_activity_task().await.unwrap();
        barr.wait().await;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![2].into())),
        })
        .await
        .unwrap();
    };

    join!(wf_poller, at_poller);
    core.drain_pollers_and_shutdown().await;
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn local_act_retry_explicit_delay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1], mock);

    #[workflow]
    #[derive(Default)]
    struct LocalActRetryExplicitDelayWf;

    #[workflow_methods]
    impl LocalActRetryExplicitDelayWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let la_res = ctx
                .execute_local_activity(
                    ActivityWithExplicitBackoff::go,
                    (),
                    LocalActivityOptions::builder()
                        .retry_policy(
                            RetryPolicy {
                                initial_interval: Some(prost_dur!(from_millis(50))),
                                backoff_coefficient: 1.0,
                                maximum_attempts: 5,
                                ..Default::default()
                            }
                            .into(),
                        )
                        .build(),
                )
                .await;
            assert!(la_res.is_ok());
            Ok(())
        }
    }

    let attempts = Arc::new(AtomicUsize::new(0));

    struct ActivityWithExplicitBackoff {
        attempts: Arc<AtomicUsize>,
    }
    #[activities]
    impl ActivityWithExplicitBackoff {
        #[activity]
        async fn go(self: Arc<Self>, _: ActivityContext) -> Result<(), ActivityError> {
            // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
            let last_attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
            if 0 == last_attempt {
                Err(ActivityError::application(
                    ApplicationFailure::builder(anyhow!("Explicit backoff error"))
                        .next_retry_delay(Duration::from_millis(300))
                        .build(),
                ))
            } else if 2 == last_attempt {
                Ok(())
            } else {
                Err(anyhow!("Oh no I failed!").into())
            }
        }
    }

    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |_| {},
        |options| {
            options
                .register_workflow::<LocalActRetryExplicitDelayWf>()
                .unwrap();
            options.register_activities(ActivityWithExplicitBackoff {
                attempts: attempts.clone(),
            });
        },
    );
    let start = Instant::now();
    worker.run_until_done().await.unwrap();
    let expected_attempts = 3;
    assert_eq!(expected_attempts, attempts.load(Ordering::Relaxed));
    assert!(start.elapsed() > Duration::from_millis(350));
}

#[workflow]
#[derive(Default)]
struct LaWf;

#[workflow_methods]
impl LaWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let _ = ctx
            .execute_local_activity(
                StdActivities::default,
                (),
                LocalActivityOptions::builder()
                    .retry_policy(
                        RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        }
                        .into(),
                    )
                    .build(),
            )
            .await;
        Ok(())
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[case::incremental(false, true)]
#[case::replay(true, true)]
#[case::incremental_fail(false, false)]
#[case::replay_fail(true, false)]
#[tokio::test]
async fn one_la_success(#[case] replay: bool, #[case] completes_ok: bool) {
    let activity_id = "1";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    if completes_ok {
        t.add_local_activity_result_marker(1, activity_id, b"hi".into());
    } else {
        t.add_local_activity_fail_marker(
            1,
            activity_id,
            Failure::application_failure("I failed".to_string(), false),
        );
    }
    t.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            let commands = &wft.commands;
            if !replay {
                assert_eq!(commands.len(), 2);
                assert_eq!(commands[0].command_type(), CommandType::RecordMarker);
                if completes_ok {
                    assert_matches!(
                        commands[0].attributes.as_ref().unwrap(),
                        command::Attributes::RecordMarkerCommandAttributes(
                            RecordMarkerCommandAttributes { failure: None, .. }
                        )
                    );
                } else {
                    assert_matches!(
                        commands[0].attributes.as_ref().unwrap(),
                        command::Attributes::RecordMarkerCommandAttributes(
                            RecordMarkerCommandAttributes {
                                failure: Some(_),
                                ..
                            }
                        )
                    );
                }
                assert_eq!(
                    commands[1].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            } else {
                assert_eq!(commands.len(), 1);
                assert_matches!(
                    commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            }
        });
    });

    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options.register_workflow::<LaWf>().unwrap();
        options.register_activities(ActivityWithReplayCheck {
            replay,
            completes_ok,
        });
    });

    struct ActivityWithReplayCheck {
        replay: bool,
        completes_ok: bool,
    }
    #[activities]
    impl ActivityWithReplayCheck {
        #[activity(name = DEFAULT_ACTIVITY_TYPE)]
        #[allow(unused)]
        async fn echo(
            self: Arc<Self>,
            _: ActivityContext,
            _: RawValue,
        ) -> Result<String, ActivityError> {
            if self.replay {
                panic!("Should not be invoked on replay");
            }
            if self.completes_ok {
                Ok("hi".to_string())
            } else {
                Err(anyhow!("Oh no I failed!").into())
            }
        }
    }

    worker.run().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[case::excluded(false)]
#[case::included(true)]
#[tokio::test]
async fn local_activity_marker_optionally_includes_arguments(#[case] include_arguments: bool) {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_workflow_task_scheduled_and_started();

    let arguments: Vec<Payload> = vec![b"first".into(), b"second".into()];
    let expected_arguments = arguments.clone();
    let mut mock_cfg = MockPollCfg::from_hist_builder(history);
    mock_cfg.make_poll_stream_interminable = true;
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 2);
            let marker = assert_matches!(
                wft.commands[0].attributes.as_ref(),
                Some(command::Attributes::RecordMarkerCommandAttributes(marker)) => marker
            );
            let marker_input = marker.details.get("input");
            if include_arguments {
                assert_eq!(marker_input.unwrap().payloads, expected_arguments);
            } else {
                assert!(marker_input.is_none());
            }
            assert_eq!(
                wft.commands[1].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });
    let core = mock_worker(build_mock_pollers(mock_cfg));

    let activation = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        ScheduleLocalActivity {
            seq: 1,
            activity_id: "1".to_string(),
            activity_type: "test_act".to_string(),
            arguments,
            start_to_close_timeout: Some(prost_dur!(from_secs(30))),
            include_arguments_in_marker: include_arguments,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    let activity_task = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
        result: Some(ActivityExecutionResult::ok(b"result".into())),
    })
    .await
    .unwrap();

    let resolution = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        resolution.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
        }]
    );
    core.complete_execution(&resolution.run_id).await;
    core.drain_pollers_and_shutdown().await;
}

#[workflow]
#[derive(Default)]
struct TwoLaWf;

#[workflow_methods]
impl TwoLaWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
            .await?;
        ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
            .await?;
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct TwoLaWfParallel;

#[workflow_methods]
impl TwoLaWfParallel {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let _ = temporalio_sdk::workflows::join!(
            ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default()),
            ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
        );
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct IncrementalLocalActivityResolutionWf;

#[workflow_methods]
impl IncrementalLocalActivityResolutionWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let long = ctx.execute_local_activity(
            IncrementalLocalActivityResolutionActivities::run,
            true,
            LocalActivityOptions::default(),
        );
        let short_sequence = async {
            for _ in 0..3 {
                ctx.execute_local_activity(
                    IncrementalLocalActivityResolutionActivities::run,
                    false,
                    LocalActivityOptions::default(),
                )
                .await?;
            }
            Ok::<_, ActivityExecutionError>(())
        };
        let (long_result, short_result) = temporalio_sdk::workflows::join!(long, short_sequence);
        long_result?;
        short_result?;
        Ok(())
    }
}

struct IncrementalLocalActivityResolutionActivities {
    long_started: Arc<Notify>,
    release_long: Arc<Notify>,
    short_completed: Arc<AtomicUsize>,
    short_sequence_completed: Arc<Notify>,
}

#[activities]
impl IncrementalLocalActivityResolutionActivities {
    #[activity]
    async fn run(self: Arc<Self>, _: ActivityContext, is_long: bool) -> Result<(), ActivityError> {
        if is_long {
            self.long_started.notify_one();
            self.release_long.notified().await;
        } else if self.short_completed.fetch_add(1, Ordering::Relaxed) == 2 {
            self.short_sequence_completed.notify_one();
        }
        Ok(())
    }
}

#[tokio::test]
async fn local_activity_resolutions_are_delivered_incrementally() {
    let wf_name = "local_activity_resolutions_are_delivered_incrementally";
    let long_started = Arc::new(Notify::new());
    let release_long = Arc::new(Notify::new());
    let short_sequence_completed = Arc::new(Notify::new());
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_activities(IncrementalLocalActivityResolutionActivities {
            long_started: long_started.clone(),
            release_long: release_long.clone(),
            short_completed: Arc::new(AtomicUsize::new(0)),
            short_sequence_completed: short_sequence_completed.clone(),
        });
    starter
        .sdk_config
        .register_workflow::<IncrementalLocalActivityResolutionWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            IncrementalLocalActivityResolutionWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();

    let verify_short_sequence = async {
        long_started.notified().await;
        let completed_while_long_was_running =
            tokio::time::timeout(Duration::from_secs(2), short_sequence_completed.notified())
                .await
                .is_ok();
        release_long.notify_one();
        completed_while_long_was_running
    };
    let (completed_while_long_was_running, worker_result) =
        join!(verify_short_sequence, worker.run_until_done());

    worker_result.unwrap();
    assert!(
        completed_while_long_was_running,
        "the short LA sequence did not complete while the long LA was still running"
    );
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}

/// With a zero-sized cache, a run keeps its workflow task until every local activity resolution has
/// been delivered and the task answered. Resolutions queued while an earlier one is outstanding with
/// lang may schedule further activities, and the markers for all of them belong on the completion
/// that finally answers the task.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn zero_cache_doesnt_evict_before_wft_is_answered() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let reported_commands = Arc::new(SegQueue::new());
    let recorder = reported_commands.clone();
    let mut mock_cfg =
        MockPollCfg::from_resp_batches(wfid, t, [ResponseType::AllHistory], mock_worker_client());
    mock_cfg.enforce_correct_number_of_polls = false;
    mock_cfg.completion_mock_fn = Some(Box::new(move |c| {
        recorder.push(
            c.commands
                .iter()
                .map(|cmd| cmd.command_type())
                .collect::<Vec<_>>(),
        );
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 0);
    let core = mock_worker(mock);

    let la_cmd = |seq: u32, id: &str| {
        schedule_local_activity_cmd(
            seq,
            id,
            ProtoActivityCancellationType::TryCancel,
            Duration::from_secs(60),
        )
    };

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![la_cmd(1, "1"), la_cmd(2, "2")],
    ))
    .await
    .unwrap();

    // Which activity each task belongs to is matched on activity id, since the order the two are
    // handed out in is not something this test should depend on.
    let queued_las = [
        core.poll_activity_task().await.unwrap(),
        core.poll_activity_task().await.unwrap(),
    ];
    let token_for = |activity_id: &str| {
        queued_las
            .iter()
            .find(|t| {
                matches!(&t.variant, Some(act_task::Variant::Start(start))
                    if start.activity_id == activity_id)
            })
            .unwrap_or_else(|| panic!("no local activity task for id {activity_id}"))
            .task_token
            .clone()
    };

    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token_for("1"),
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    let resolve_first = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        resolve_first.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution)),
        }] => assert_eq!(resolution.seq, 1)
    );

    // Finishing the second activity while the first resolution is outstanding with lang is what
    // queues it rather than delivering it, leaving no outstanding activities but an undelivered
    // job once the completion below lands.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: token_for("2"),
        result: Some(ActivityExecutionResult::ok(vec![2].into())),
    })
    .await
    .unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(resolve_first.run_id))
        .await
        .unwrap();

    // Scheduling from the queued resolution keeps the task open with the first two markers still
    // buffered, which is the point at which the run must survive to report them.
    let resolve_second = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        resolve_second.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution)),
        }] => assert_eq!(resolution.seq, 2)
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        resolve_second.run_id,
        la_cmd(3, "3"),
    ))
    .await
    .unwrap();

    let third_la = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: third_la.task_token,
        result: Some(ActivityExecutionResult::ok(vec![3].into())),
    })
    .await
    .unwrap();
    let resolve_third = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        resolve_third.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution)),
        }] => assert_eq!(resolution.seq, 3)
    );
    core.complete_execution(&resolve_third.run_id).await;

    core.shutdown().await;

    let mut all_reported = vec![];
    while let Some(cmds) = reported_commands.pop() {
        all_reported.push(cmds);
    }
    let markers_reported: usize = all_reported
        .iter()
        .flatten()
        .filter(|c| **c == CommandType::RecordMarker)
        .count();
    assert_eq!(
        markers_reported, 3,
        "all three local activity markers should reach the server, got {all_reported:?}"
    );
}

#[tokio::test]
async fn eviction_with_buffered_la_marker_replays_only_completed_la() {
    let mut starter =
        CoreWfStarter::new("eviction_with_buffered_la_marker_replays_only_completed_la");
    starter.workflow_options.task_timeout = Some(Duration::from_secs(60));
    let core = starter.get_core_worker().await;
    let run_id = starter.start_wf().await;
    let la_commands = || {
        (1..=2)
            .map(|seq| {
                schedule_local_activity_cmd(
                    seq,
                    &seq.to_string(),
                    ProtoActivityCancellationType::WaitCancellationCompleted,
                    Duration::from_secs(60),
                )
            })
            .collect()
    };
    let initial = core.poll_workflow_activation().await.unwrap();
    assert!(!initial.is_replaying);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        initial.run_id,
        la_commands(),
    ))
    .await
    .unwrap();

    let mut starts = HashMap::new();
    let mut execution_counts = HashMap::new();
    for _ in 0..2 {
        let task = core.poll_activity_task().await.unwrap();
        let Some(act_task::Variant::Start(start)) = &task.variant else {
            panic!("Expected LA start, got {task:?}");
        };
        assert!(start.is_local);
        *execution_counts
            .entry(start.activity_id.clone())
            .or_insert(0) += 1;
        assert!(
            starts
                .insert(start.activity_id.clone(), task.task_token)
                .is_none()
        );
    }
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: starts.remove("1").unwrap(),
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    let resolved = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        resolved.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution)),
        }] if resolution.seq == 1
    );

    // Holding the resolution activation keeps LA1's marker buffered while eviction cancels LA2.
    core.request_workflow_eviction(&run_id);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(resolved.run_id))
        .await
        .unwrap();
    let (eviction, cancel) = join!(core.poll_workflow_activation(), core.poll_activity_task());
    let eviction = eviction.unwrap();
    let cancel = cancel.unwrap();
    assert_matches!(
        eviction.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    assert_eq!(cancel.task_token, starts.remove("2").unwrap());
    assert_matches!(cancel.variant, Some(act_task::Variant::Cancel(_)));
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: cancel.task_token,
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();

    // The buffered-marker completion must force another workflow task so the cancelled,
    // unrecorded LA can execute again.
    let history = starter.get_history().await;
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event.event_type() == EventType::WorkflowTaskScheduled)
            .count(),
        2
    );
    let markers = history
        .events
        .iter()
        .filter_map(|event| match &event.attributes {
            Some(MarkerRecordedEventAttributes(marker)) => Some(
                extract_local_activity_marker_details(&mut marker.details.clone()),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].0.as_ref().unwrap().seq, 1);
    assert_eq!(markers[0].1, Some(vec![1].into()));

    let replacement = core.poll_workflow_activation().await.unwrap();
    assert_eq!(replacement.run_id, run_id);
    assert!(replacement.is_replaying);
    assert_matches!(
        replacement.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        replacement.run_id,
        la_commands(),
    ))
    .await
    .unwrap();
    let replayed = core.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_matches!(
        replayed.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution)),
        }] if resolution.seq == 1
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(replayed.run_id))
        .await
        .unwrap();
    let task = core.poll_activity_task().await.unwrap();
    let Some(act_task::Variant::Start(start)) = &task.variant else {
        panic!("Expected replacement LA start, got {task:?}");
    };
    assert!(start.is_local);
    assert_eq!(start.activity_id, "2");
    *execution_counts
        .entry(start.activity_id.clone())
        .or_insert(0) += 1;
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::ok(vec![2].into())),
    })
    .await
    .unwrap();
    let resolved = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        resolved.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution)),
        }] if resolution.seq == 2
    );
    core.complete_execution(&resolved.run_id).await;
    drain_pollers_and_shutdown(&core).await;

    assert_eq!(
        execution_counts,
        HashMap::from([("1".to_owned(), 1), ("2".to_owned(), 2)])
    );
    let history = starter.get_history().await;
    let markers = history
        .events
        .iter()
        .filter_map(|event| match &event.attributes {
            Some(MarkerRecordedEventAttributes(marker)) => Some(
                extract_local_activity_marker_details(&mut marker.details.clone()),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(markers.len(), 2);
    for (index, (metadata, result)) in markers.into_iter().enumerate() {
        assert_eq!(metadata.unwrap().seq, index as u32 + 1);
        assert_eq!(result, Some(vec![index as u8 + 1].into()));
    }
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event.event_type() == EventType::WorkflowTaskCompleted)
            .count(),
        2
    );
    assert!(!history.events.iter().any(|event| matches!(
        event.event_type(),
        EventType::WorkflowTaskFailed | EventType::WorkflowTaskTimedOut
    )));
    assert_eq!(
        history.events.last().unwrap().event_type(),
        EventType::WorkflowExecutionCompleted
    );
}

#[workflow]
#[derive(Default)]
struct OldBatchedLocalActivityHistoryWf;

#[workflow_methods]
impl OldBatchedLocalActivityHistoryWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let long =
            ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default());
        let short_sequence = async {
            ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
                .await?;
            ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
                .await?;
            Ok::<_, ActivityExecutionError>(())
        };
        let (long_result, short_result) = temporalio_sdk::workflows::join!(long, short_sequence);
        long_result?;
        short_result?;
        Ok(())
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn old_batched_local_activity_history_replays() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    history.add_local_activity_result_marker(2, "2", b"short".into());
    history.add_local_activity_result_marker(1, "1", b"long".into());
    history.add_local_activity_result_marker(3, "3", b"short".into());
    history.add_workflow_task_scheduled_and_started();
    let mut mock_cfg = MockPollCfg::from_resps(history, [ResponseType::AllHistory]);

    let mut activation_assertions = ActivationAssertionsInterceptor::default();
    activation_assertions
        .skip_one()
        .then(|activation| {
            assert_matches!(
                activation.jobs.as_slice(),
                [
                    WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(first))
                    },
                    WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(second))
                    }
                ] => {
                    assert_eq!(first.seq, 2);
                    assert_eq!(second.seq, 1);
                }
            );
        })
        .then(|activation| {
            assert_matches!(
                activation.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution))
                }] => assert_eq!(resolution.seq, 3)
            );
        });
    mock_cfg.completion_asserts_from_expectations(|mut assertions| {
        assertions.then(|completion| {
            assert_matches!(
                completion.commands.as_slice(),
                [command] if command.command_type() == CommandType::CompleteWorkflowExecution
            );
        });
    });

    let mut worker = crate::common::build_fake_sdk_intercepted_with_options(
        mock_cfg,
        activation_assertions,
        |options| {
            options
                .register_workflow::<OldBatchedLocalActivityHistoryWf>()
                .unwrap();
            options.register_activities(ResolvedActivity);
        },
    );
    worker.run().await.unwrap();
}

struct MixedCompletionTimesActivity {
    release_slow: CancellationToken,
}

#[activities]
impl MixedCompletionTimesActivity {
    #[allow(unused)]
    #[activity(name = DEFAULT_ACTIVITY_TYPE)]
    async fn run(
        self: Arc<Self>,
        _: ActivityContext,
        wait_for_heartbeat: bool,
    ) -> Result<String, ActivityError> {
        if wait_for_heartbeat {
            self.release_slow.cancelled().await;
        }
        Ok("Result".to_string())
    }
}

#[workflow]
#[derive(Default)]
struct MixedCompletionTimesWf;

#[workflow_methods]
impl MixedCompletionTimesWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let _ = temporalio_sdk::workflows::join!(
            ctx.execute_local_activity(
                MixedCompletionTimesActivity::run,
                true,
                LocalActivityOptions::default()
            ),
            ctx.execute_local_activity(
                MixedCompletionTimesActivity::run,
                false,
                LocalActivityOptions::default()
            )
        );
        Ok(())
    }
}

struct HeartbeatGatedActivity {
    release: CancellationToken,
}

#[activities]
impl HeartbeatGatedActivity {
    #[allow(unused)]
    #[activity(name = DEFAULT_ACTIVITY_TYPE)]
    async fn run(self: Arc<Self>, _: ActivityContext, _: ()) -> Result<String, ActivityError> {
        self.release.cancelled().await;
        Ok("hi".to_string())
    }
}

/// Verifies lookahead preserves completion order when one LA resolves before a heartbeat and the
/// other resolves after it.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[tokio::test]
async fn mixed_la_completion_times(#[values(true, false)] replay: bool) {
    let wft_timeout = Duration::from_millis(500);
    let mut history = TestHistoryBuilder::default();
    history.add_wfe_started_with_wft_timeout(wft_timeout);
    history.add_full_wf_task();
    history.add_local_activity_result_marker(2, "2", b"Result".into());
    history.add_full_wf_task();
    history.add_local_activity_result_marker(1, "1", b"Result".into());
    history.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(history, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(history)
    };

    let mut activation_assertions = ActivationAssertionsInterceptor::default();
    activation_assertions
        .skip_one()
        .then(|activation| {
            assert_matches!(
                activation.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution)),
                }] => assert_eq!(resolution.seq, 2)
            );
        })
        .then(|activation| {
            assert_matches!(
                activation.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(resolution)),
                }] => assert_eq!(resolution.seq, 1)
            );
        });

    let release_slow = CancellationToken::new();
    let release_slow_after_heartbeat = release_slow.clone();
    mock_cfg.completion_asserts_from_expectations(|mut assertions| {
        if replay {
            assertions.then(|completion| {
                assert_matches!(
                    completion.commands.as_slice(),
                    [command] if command.command_type() == CommandType::CompleteWorkflowExecution
                );
            });
        } else {
            assertions
                .then(move |completion| {
                    assert_matches!(
                        completion.commands.as_slice(),
                        [command] if command.command_type() == CommandType::RecordMarker
                    );
                    // Releasing here ensures LA1 cannot complete until Core has reported the
                    // heartbeat containing LA2's marker.
                    release_slow_after_heartbeat.cancel();
                })
                .then(|completion| {
                    assert_matches!(
                        completion.commands.as_slice(),
                        [marker, complete]
                            if marker.command_type() == CommandType::RecordMarker
                                && complete.command_type()
                                    == CommandType::CompleteWorkflowExecution
                    );
                });
        }
    });

    let mut worker = crate::common::build_fake_sdk_intercepted_with_options(
        mock_cfg,
        activation_assertions,
        |options| {
            options
                .register_workflow::<MixedCompletionTimesWf>()
                .unwrap();
            options.register_activities(MixedCompletionTimesActivity { release_slow });
        },
    );
    worker.run().await.unwrap();
}

/// Both LAs remain outstanding through a heartbeat, then resolve in the following WFT. Replay must
/// deliver their resolutions in marker order, even when that differs from schedule order.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[tokio::test]
async fn two_las_with_heartbeat(
    #[values(true, false)] replay: bool,
    #[values(true, false)] complete_in_schedule_order: bool,
) {
    let mut history = TestHistoryBuilder::default();
    history.add_wfe_started_with_wft_timeout(Duration::from_millis(500));
    history.add_full_wf_task();
    history.add_full_wf_task();
    if complete_in_schedule_order {
        history.add_local_activity_result_marker(1, "1", b"hi".into());
        history.add_local_activity_result_marker(2, "2", b"hi".into());
    } else {
        history.add_local_activity_result_marker(2, "2", b"hi".into());
        history.add_local_activity_result_marker(1, "1", b"hi".into());
    }
    history.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(history, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(history)
    };

    let mut activation_assertions = ActivationAssertionsInterceptor::default();
    activation_assertions.skip_one().then(move |activation| {
        if replay {
            let (first_expected_seq, second_expected_seq) = if complete_in_schedule_order {
                (1, 2)
            } else {
                (2, 1)
            };
            assert_matches!(
                activation.jobs.as_slice(),
                [
                    WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(first))
                    },
                    WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::ResolveActivity(second))
                    }
                ] => {
                    assert_eq!(first.seq, first_expected_seq);
                    assert_eq!(second.seq, second_expected_seq);
                }
            );
        }
    });

    let release_activities = CancellationToken::new();
    let release_activities_after_heartbeat = release_activities.clone();
    mock_cfg.completion_asserts_from_expectations(|mut assertions| {
        if replay {
            assertions.then(|completion| {
                assert_matches!(
                    completion.commands.as_slice(),
                    [command] if command.command_type() == CommandType::CompleteWorkflowExecution
                );
            });
        } else {
            assertions
                .then(move |completion| {
                    assert!(completion.commands.is_empty());
                    // Both activities stay blocked until the empty heartbeat WFT is reported.
                    release_activities_after_heartbeat.cancel();
                })
                .then(|completion| {
                    assert_matches!(
                        completion.commands.as_slice(),
                        [first_marker, second_marker, complete]
                            if first_marker.command_type() == CommandType::RecordMarker
                                && second_marker.command_type() == CommandType::RecordMarker
                                && complete.command_type()
                                    == CommandType::CompleteWorkflowExecution
                    );
                });
        }
    });

    let mut worker = crate::common::build_fake_sdk_intercepted_with_options(
        mock_cfg,
        activation_assertions,
        |options| {
            options.register_workflow::<TwoLaWfParallel>().unwrap();
            options.register_activities(HeartbeatGatedActivity {
                release: release_activities,
            });
        },
    );
    worker.run().await.unwrap();
}

struct ResolvedActivity;
#[activities]
impl ResolvedActivity {
    #[allow(unused)]
    #[activity(name = DEFAULT_ACTIVITY_TYPE)]
    async fn echo(_: ActivityContext, _: ()) -> Result<String, ActivityError> {
        Ok("Resolved".to_string())
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[tokio::test]
async fn two_sequential_las(
    #[values(true, false)] replay: bool,
    #[values(true, false)] parallel: bool,
) {
    let t = canned_histories::two_local_activities_one_wft(parallel);
    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };

    let mut aai = ActivationAssertionsInterceptor::default();
    let first_act_ts_seconds: &'static _ = Box::leak(Box::new(AtomicI64::new(-1)));
    aai.then(|a| {
        first_act_ts_seconds.store(a.timestamp.as_ref().unwrap().seconds, Ordering::Relaxed)
    });
    // Verify LAs advance time (they take 1s as defined in the canned history)
    aai.then(move |a| {
        if !parallel {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                }] => assert_eq!(ra.seq, 1)
            );
        } else if replay {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                 }, WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra2))
                }] => {assert_eq!(ra.seq, 1); assert_eq!(ra2.seq, 2)}
            );
        } else {
            // Incremental delivery can race parallel LA completion, so lang may see the first
            // resolution alone or both resolutions after they are already available.
            let mut resolution_seqs = a
                .jobs
                .iter()
                .map(|job| match job.variant.as_ref() {
                    Some(workflow_activation_job::Variant::ResolveActivity(ra)) => ra.seq,
                    unexpected => panic!("Unexpected activation job: {unexpected:?}"),
                })
                .collect::<Vec<_>>();
            resolution_seqs.sort_unstable();
            assert_matches!(resolution_seqs.as_slice(), [1] | [2] | [1, 2]);
        }
        if replay {
            assert!(
                a.timestamp.as_ref().unwrap().seconds
                    > first_act_ts_seconds.load(Ordering::Relaxed)
            )
        }
    });
    if !parallel {
        aai.then(move |a| {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                }] => assert_eq!(ra.seq, 2)
            );
            if replay {
                assert!(
                    a.timestamp.as_ref().unwrap().seconds
                        >= first_act_ts_seconds.load(Ordering::Relaxed) + 2
                )
            }
        });
    }

    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            let commands = &wft.commands;
            if !replay {
                assert_eq!(commands.len(), 3);
                assert_eq!(commands[0].command_type(), CommandType::RecordMarker);
                assert_eq!(commands[1].command_type(), CommandType::RecordMarker);
                assert_matches!(
                    commands[2].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            } else {
                assert_eq!(commands.len(), 1);
                assert_matches!(
                    commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            }
        });
    });

    let mut worker =
        crate::common::build_fake_sdk_intercepted_with_options(mock_cfg, aai, |options| {
            if parallel {
                options.register_workflow::<TwoLaWfParallel>().unwrap();
            } else {
                options.register_workflow::<TwoLaWf>().unwrap();
            }
            options.register_activities(ResolvedActivity);
        });
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct LaTimerLaWf;

#[workflow_methods]
impl LaTimerLaWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
            .await?;
        ctx.timer(Duration::from_secs(5)).await;
        ctx.execute_local_activity(StdActivities::default, (), LocalActivityOptions::default())
            .await?;
        Ok(())
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[case::incremental(false)]
#[case::replay(true)]
#[tokio::test]
async fn las_separated_by_timer(#[case] replay: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", b"hi".into());
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_local_activity_result_marker(2, "2", b"hi2".into());
    t.add_workflow_task_scheduled_and_started();
    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.skip_one()
        .then(|a| {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                }] => assert_eq!(ra.seq, 1)
            );
        })
        .then(|a| {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_))
                }]
            );
        });

    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        if replay {
            asserts.then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type,
                    CommandType::CompleteWorkflowExecution as i32
                );
            });
        } else {
            asserts
                .then(|wft| {
                    let commands = &wft.commands;
                    assert_eq!(commands.len(), 2);
                    assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                    assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
                })
                .then(|wft| {
                    let commands = &wft.commands;
                    assert_eq!(commands.len(), 2);
                    assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                    assert_eq!(
                        commands[1].command_type,
                        CommandType::CompleteWorkflowExecution as i32
                    );
                });
        }
    });

    let mut worker =
        crate::common::build_fake_sdk_intercepted_with_options(mock_cfg, aai, |options| {
            options.register_workflow::<LaTimerLaWf>().unwrap();
            options.register_activities(ResolvedActivity);
        });
    worker.run().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn one_la_heartbeating_wft_failure_still_executes() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    // Heartbeats
    t.add_full_wf_task();
    // fails a wft for some reason
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::NonDeterministicError,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 2);
            assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
            assert_matches!(
                wft.commands[1].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });

    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options.register_workflow::<LaWf>().unwrap();
        options.register_activities(ResolvedActivity);
    });
    worker.run().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[tokio::test]
async fn immediate_cancel(
    #[values(
        ActivityCancellationType::WaitCancellationCompleted,
        ActivityCancellationType::TryCancel,
        ActivityCancellationType::Abandon
    )]
    cancel_type: ActivityCancellationType,
) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.set_wf_input(cancel_type.as_json_payload().unwrap());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|wft| {
            assert_eq!(wft.commands.len(), 2);
            // We record the cancel marker
            assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
            assert_matches!(
                wft.commands[1].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });

    #[workflow]
    #[derive(Default)]
    struct CancelBeforeActStartsWf;

    #[workflow_methods]
    impl CancelBeforeActStartsWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(
            ctx: &mut WorkflowContext<Self>,
            cancel_type: ActivityCancellationType,
        ) -> WorkflowResult<()> {
            let la = ctx.execute_local_activity(
                StdActivities::default,
                (),
                LocalActivityOptions::builder()
                    .cancel_type(cancel_type)
                    .build(),
            );
            la.cancel();
            la.await.ok();
            Ok(())
        }
    }

    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options
            .register_workflow::<CancelBeforeActStartsWf>()
            .unwrap();
    });
    worker.run().await.unwrap();
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest]
#[case::incremental(false)]
#[case::replay(true)]
#[tokio::test]
async fn cancel_after_act_starts_canned(
    #[case] replay: bool,
    #[values(
        ActivityCancellationType::WaitCancellationCompleted,
        ActivityCancellationType::TryCancel,
        ActivityCancellationType::Abandon
    )]
    cancel_type: ActivityCancellationType,
) {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(100));
    t.set_wf_input(cancel_type.as_json_payload().unwrap());
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    // This extra workflow task serves to prevent looking ahead and pre-resolving during
    // wait-cancel.
    // TODO: including this on non wait-cancel seems to cause double-send of
    //   marker recorded cmd
    if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
        t.add_full_wf_task();
    }
    if cancel_type != ActivityCancellationType::WaitCancellationCompleted {
        // With non-wait cancels, the cancel is immediate
        t.add_local_activity_cancel_marker(1, "1");
    }
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
        // With wait cancels, the cancel marker is not recorded until activity reports.
        t.add_local_activity_cancel_marker(1, "1");
    }
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };
    let allow_cancel_barr = CancellationToken::new();
    let allow_cancel_barr_clone = allow_cancel_barr.clone();

    if !replay {
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts
                .then(move |wft| {
                    assert_eq!(wft.commands.len(), 1);
                    assert_eq!(wft.commands[0].command_type, CommandType::StartTimer as i32);
                })
                .then(move |wft| {
                    let commands = &wft.commands;
                    if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                        assert_eq!(commands.len(), 1);
                        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
                    } else {
                        // Try-cancel/abandon immediately recordsmarker (when not replaying)
                        assert_eq!(commands.len(), 2);
                        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                        assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
                    }
                    // Allow the wait-cancel to actually cancel
                    allow_cancel_barr.cancel();
                })
                .then(move |wft| {
                    let commands = &wft.commands;
                    if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
                        assert_eq!(commands[1].command_type, CommandType::RecordMarker as i32);
                    } else {
                        assert_eq!(
                            commands[0].command_type,
                            CommandType::CompleteWorkflowExecution as i32
                        );
                    }
                });
        });
    }

    #[workflow]
    #[derive(Default)]
    struct CancelAfterActStartsCannedWf;

    #[workflow_methods]
    impl CancelAfterActStartsCannedWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(
            ctx: &mut WorkflowContext<Self>,
            cancel_type: ActivityCancellationType,
        ) -> WorkflowResult<()> {
            let la = ctx.execute_local_activity(
                ActivityWithConditionalCancelWait::echo,
                (),
                LocalActivityOptions::builder()
                    .cancel_type(cancel_type)
                    .build(),
            );
            ctx.timer(Duration::from_secs(1)).await;
            la.cancel();
            ctx.timer(Duration::from_secs(1)).await;
            let err = la.await.unwrap_err();
            let ActivityExecutionError::Cancelled(cancel_err) = err else {
                panic!("expected cancelled error, got {err:?}");
            };
            assert!(cancel_err.raw_details().is_none());
            Ok(())
        }
    }

    struct ActivityWithConditionalCancelWait {
        cancel_type: ActivityCancellationType,
        allow_cancel_barr: CancellationToken,
    }
    #[activities]
    impl ActivityWithConditionalCancelWait {
        #[activity(name = DEFAULT_ACTIVITY_TYPE)]
        async fn echo(self: Arc<Self>, ctx: ActivityContext, _: ()) -> Result<(), ActivityError> {
            if self.cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                ctx.cancelled().await;
            }
            self.allow_cancel_barr.cancelled().await;
            Err(ActivityError::cancelled())
        }
    }

    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options
            .register_workflow::<CancelAfterActStartsCannedWf>()
            .unwrap();
        options.register_activities(ActivityWithConditionalCancelWait {
            cancel_type,
            allow_cancel_barr: allow_cancel_barr_clone,
        });
    });
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct HeartbeatTimeoutOverlapWf;

#[workflow_methods]
impl HeartbeatTimeoutOverlapWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let long = ctx.execute_local_activity(
            HeartbeatTimeoutOverlapActivities::run,
            true,
            LocalActivityOptions::default(),
        );
        let short = ctx.execute_local_activity(
            HeartbeatTimeoutOverlapActivities::run,
            false,
            LocalActivityOptions::default(),
        );

        let (long_result, short_result) = temporalio_sdk::workflows::join!(long, short);
        long_result?;
        short_result?;
        Ok(())
    }
}

struct HeartbeatTimeoutOverlapActivities {
    long_started: Arc<Notify>,
    release_long: Arc<Notify>,
}

#[activities]
impl HeartbeatTimeoutOverlapActivities {
    #[activity]
    async fn run(self: Arc<Self>, _: ActivityContext, is_long: bool) -> Result<(), ActivityError> {
        if is_long {
            self.long_started.notify_one();
            self.release_long.notified().await;
        }
        Ok(())
    }
}

struct HoldFirstLocalActivityResolutionCompletion {
    initial_activation_at: Arc<OnceLock<Instant>>,
    resolution_activation_seen: Arc<Notify>,
    release_resolution_activation: Arc<Notify>,
    saw_resolution: AtomicBool,
    hold_next_completion: AtomicBool,
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for HoldFirstLocalActivityResolutionCompletion {
    async fn on_workflow_activation(
        &self,
        activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        if activation.jobs.iter().any(|job| {
            matches!(
                job.variant.as_ref(),
                Some(workflow_activation_job::Variant::InitializeWorkflow(_))
            )
        }) {
            self.initial_activation_at
                .set(Instant::now())
                .expect("initial activation must only be observed once");
        }

        if activation.jobs.iter().any(|job| {
            matches!(
                job.variant.as_ref(),
                Some(workflow_activation_job::Variant::ResolveActivity(_))
            )
        }) && !self.saw_resolution.swap(true, Ordering::SeqCst)
        {
            self.hold_next_completion.store(true, Ordering::SeqCst);
            self.resolution_activation_seen.notify_one();
        }

        Ok(())
    }

    async fn on_workflow_activation_completion(&self, _: &WorkflowActivationCompletion) {
        if self.hold_next_completion.swap(false, Ordering::SeqCst) {
            self.release_resolution_activation.notified().await;
        }
    }

    fn on_shutdown(&self, _: &Worker) {}
}

/// The heartbeat deadline can expire while lang still owns an LA-resolution activation. Core must
/// not issue the heartbeat autocomplete until that activation has completed, since doing so would
/// create two outstanding activations for the same run and panic.
#[tokio::test]
async fn heartbeat_timeout_does_not_overlap_local_activity_resolution_activation() {
    let wf_name = "heartbeat_timeout_does_not_overlap_local_activity_resolution_activation";
    let wft_timeout = Duration::from_secs(4);

    let long_started = Arc::new(Notify::new());
    let release_long = Arc::new(Notify::new());
    let initial_activation_at = Arc::new(OnceLock::new());
    let resolution_activation_seen = Arc::new(Notify::new());
    let release_resolution_activation = Arc::new(Notify::new());

    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_activities(HeartbeatTimeoutOverlapActivities {
            long_started: long_started.clone(),
            release_long: release_long.clone(),
        });
    starter
        .sdk_config
        .register_workflow::<HeartbeatTimeoutOverlapWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            HeartbeatTimeoutOverlapWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned())
                .task_timeout(wft_timeout)
                .build(),
        )
        .await
        .unwrap();

    let interceptor = HoldFirstLocalActivityResolutionCompletion {
        initial_activation_at: initial_activation_at.clone(),
        resolution_activation_seen: resolution_activation_seen.clone(),
        release_resolution_activation: release_resolution_activation.clone(),
        saw_resolution: AtomicBool::new(false),
        hold_next_completion: AtomicBool::new(false),
    };

    let cross_heartbeat_deadline = async {
        long_started.notified().await;
        resolution_activation_seen.notified().await;

        let initial_activation_at = *initial_activation_at
            .get()
            .expect("initial activation timestamp must be recorded");

        // Core's heartbeat deadline is 80% of the WFT timeout. Keep the
        // resolution activation outstanding until the real timer elapses.
        tokio::time::sleep_until((initial_activation_at + wft_timeout.mul_f32(0.85)).into()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        release_resolution_activation.notify_one();
        release_long.notify_one();
    };

    let (_, worker_result) = tokio::join!(
        cross_heartbeat_deadline,
        worker.run_until_done_intercepted(Some(interceptor))
    );

    worker_result.unwrap();
}

#[workflow]
#[derive(Default)]
struct OutOfOrderMarkerWorkflow;

#[workflow_methods]
impl OutOfOrderMarkerWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let mut first = ctx.execute_local_activity(
            OutOfOrderMarkerActivities::run,
            true,
            LocalActivityOptions::default(),
        );
        let mut timer = ctx.timer(Duration::from_millis(100));

        let timer_won = temporalio_sdk::workflows::select! {
            _ = first => false,
            _ = timer => true,
        };
        if !timer_won {
            return Ok(());
        }

        ctx.execute_local_activity(
            OutOfOrderMarkerActivities::run,
            false,
            LocalActivityOptions::default(),
        )
        .await?;
        first.await?;
        Ok(())
    }
}

struct OutOfOrderMarkerActivities {
    release_first: Arc<Notify>,
}

#[activities]
impl OutOfOrderMarkerActivities {
    #[activity]
    async fn run(
        self: Arc<Self>,
        _ctx: ActivityContext,
        wait_for_release: bool,
    ) -> Result<(), ActivityError> {
        if wait_for_release {
            self.release_first.notified().await;
        }
        Ok(())
    }
}

struct ReleaseFirstAfterSecondResolved {
    release_first: Arc<Notify>,
    release_after_completion: AtomicBool,
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for ReleaseFirstAfterSecondResolved {
    async fn on_workflow_activation(
        &self,
        activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        let resolves_second = activation.jobs.iter().any(|job| {
            matches!(
                job.variant.as_ref(),
                Some(workflow_activation_job::Variant::ResolveActivity(resolution))
                    if resolution.seq == 2
            )
        });
        if resolves_second {
            self.release_after_completion.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn on_workflow_activation_completion(&self, _completion: &WorkflowActivationCompletion) {
        if self.release_after_completion.swap(false, Ordering::SeqCst) {
            self.release_first.notify_one();
        }
    }
}

/// Replay lookahead must honor marker order when a later marker belongs to an already-scheduled LA.
/// Resolving LA1 past LA2's earlier marker can change the timer/LA select outcome, skip scheduling
/// LA2, and make the live history nondeterministic on replay.
#[tokio::test]
async fn replay_out_of_order_local_activity_markers_is_deterministic() {
    let test_name = "replay_out_of_order_local_activity_markers_is_deterministic";
    let mut starter = CoreWfStarter::new(test_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    starter
        .sdk_config
        .worker_interceptor(FailOnNondeterminismInterceptor {});

    let release_first = Arc::new(Notify::new());
    starter
        .sdk_config
        .register_activities(OutOfOrderMarkerActivities {
            release_first: release_first.clone(),
        });
    starter
        .sdk_config
        .register_workflow::<OutOfOrderMarkerWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            OutOfOrderMarkerWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), test_name.to_owned()).build(),
        )
        .await
        .unwrap();

    worker
        .run_until_done_intercepted(Some(ReleaseFirstAfterSecondResolved {
            release_first,
            release_after_completion: AtomicBool::new(false),
        }))
        .await
        .unwrap();

    let workflow_id = handle.info().workflow_id.clone();
    let events = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let marker_order = events
        .iter()
        .filter_map(|event| match event.attributes.as_ref() {
            Some(history_event::Attributes::MarkerRecordedEventAttributes(attributes)) => {
                extract_local_activity_marker_data(&attributes.details).map(|data| data.seq)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(marker_order, [2, 1]);

    let replay_core =
        init_core_replay_preloaded(&task_queue, [HistoryForReplay::new(events, workflow_id)]);
    worker
        .inner_mut()
        .with_new_core_worker(Arc::new(replay_core));
    worker.inner_mut().run().await.unwrap();
}
