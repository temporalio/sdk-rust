use crate::{
    common::{
        CoreWfStarter, TestWorker, WorkflowHandleExt, activity_functions::StdActivities,
        integ_sdk_config,
    },
    integ_tests::update_tests::TicketedWftSlots,
};
use assert_matches::assert_matches;
use futures::FutureExt;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};
use temporalio_client::{
    NamespacedClient, UntypedSignal, WorkflowExecuteUpdateOptions, WorkflowSignalOptions,
    WorkflowStartOptions, errors::WorkflowUpdateError, grpc::WorkflowService,
};
use temporalio_common::{
    UntypedActivity, UntypedWorkflow,
    data_converters::RawValue,
    protos::{
        coresdk::{
            AsJsonPayloadExt, FromJsonPayloadExt,
            common::extract_local_activity_marker_details,
            workflow_activation::{WorkflowActivation, workflow_activation_job},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::WorkflowExecution,
            enums::v1::{EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
            history::v1::{
                HistoryEvent,
                history_event::Attributes::{
                    MarkerRecordedEventAttributes, WorkflowExecutionFailedEventAttributes,
                    WorkflowTaskCompletedEventAttributes, WorkflowTaskFailedEventAttributes,
                },
            },
            workflowservice::v1::GetWorkflowExecutionHistoryRequest,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, MemoValue, WorkflowContext,
    WorkflowResult, runtime::WorkflowErrorType, workflows,
};
use temporalio_sdk_core::{
    PollError, TunerBuilder, Worker,
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, canned_histories},
    test_help::{
        CoreInternalFlags, MockPollCfg, ResponseType, mock_worker_client, start_timer_cmd,
    },
};
use tokio::{sync::Semaphore, task::JoinHandle};
use tonic::IntoRequest;

#[workflow]
pub(crate) struct TimerWfNondeterministic {
    run_ct: Arc<AtomicUsize>,
}

#[workflow_methods(factory_only)]
impl TimerWfNondeterministic {
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let run_ct = ctx.state(|wf| wf.run_ct.fetch_add(1, Ordering::Relaxed));

        match run_ct {
            1 | 3 => {
                ctx.timer(Duration::from_secs(1)).await;
                if run_ct == 1 {
                    panic!("dying on purpose");
                }
            }
            2 => {
                ctx.execute_activity(
                    StdActivities::default,
                    (),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                )
                .await?;
            }
            _ => panic!("Ran too many times"),
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_determinism_error_then_recovers() {
    let wf_name = "test_determinism_error_then_recovers";
    let mut starter = CoreWfStarter::new(wf_name);
    let run_ct = Arc::new(AtomicUsize::new(1));
    let run_ct_clone = run_ct.clone();
    starter
        .sdk_config
        .register_workflow_with_factory(move || TimerWfNondeterministic {
            run_ct: run_ct_clone.clone(),
        })
        .unwrap();
    let mut worker = starter.worker().await;
    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            TimerWfNondeterministic::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_task_queue().to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert_eq!(run_ct.load(Ordering::Relaxed), 4);
}

#[workflow]
struct TaskFailReplayWf {
    did_fail: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl TaskFailReplayWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        if ctx.state(|wf| wf.did_fail.load(Ordering::Relaxed)) {
            assert!(ctx.is_replaying());
        }
        let _ = ctx
            .execute_activity(
                StdActivities::echo,
                "hi!".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(2)),
            )
            .await;
        if !ctx.state(|wf| wf.did_fail.load(Ordering::Relaxed)) {
            ctx.state(|wf| wf.did_fail.store(true, Ordering::Relaxed));
            panic!("Die on purpose");
        }
        Ok(())
    }
}

#[tokio::test]
async fn task_fail_causes_replay_unset_too_soon() {
    let wf_name = "task_fail_causes_replay_unset_too_soon";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let did_fail = Arc::new(AtomicBool::new(false));
    let did_fail_clone = did_fail.clone();
    starter
        .sdk_config
        .register_workflow_with_factory(move || TaskFailReplayWf {
            did_fail: did_fail_clone.clone(),
        })
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            TaskFailReplayWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_task_queue().to_owned()).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}

#[workflow]
#[derive(Default)]
struct RandomReplayWf;

#[workflow_methods]
impl RandomReplayWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let orders = ctx.random_stream("example.com/orders");
        let first_order = orders.random::<u64>();
        let _ = ctx.random_stream("example.com/telemetry").random::<u64>();
        let second_order = ctx.random_stream("example.com/orders").random::<u64>();
        Ok(format!(
            "{}:{}:{first_order}:{second_order}",
            ctx.random::<u64>(),
            ctx.uuid4()
        ))
    }
}

#[tokio::test]
async fn random_workflow_replays() {
    let wf_name = "random_workflow_replays";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<RandomReplayWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            RandomReplayWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    let result = handle.get_result(Default::default()).await.unwrap();
    let replay_result = handle
        .fetch_history_and_replay(&mut worker)
        .await
        .unwrap()
        .expect("replayed workflow should return a result");
    assert_eq!(result, String::from_json_payload(&replay_result).unwrap());
    starter.shutdown().await;
}

#[workflow]
struct TimerWfFailsOnce {
    did_fail: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl TimerWfFailsOnce {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(1)).await;
        if ctx
            .state(|wf| {
                wf.did_fail
                    .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            })
            .is_ok()
        {
            panic!("Ahh");
        }
        Ok(())
    }
}

/// Verifies that workflow panics (which in this case the Rust SDK turns into workflow activation
/// failures) are turned into unspecified WFT failures.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn test_panic_wf_task_rejected_properly() {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::workflow_fails_with_failure_after_timer("1");
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 2], mock);
    // We should see one wft failure which has the default cause, since panics don't have a defined
    // type.
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher = Box::new(|_, cause, _| {
        matches!(
            cause,
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        )
    });
    let did_fail = Arc::new(AtomicBool::new(false));
    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |_| {},
        |options| {
            options
                .register_workflow_with_factory(move || TimerWfFailsOnce {
                    did_fail: did_fail.clone(),
                })
                .unwrap();
        },
    );
    let task_queue = "fake_tq".to_owned();
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

#[workflow]
struct NondeterministicTimerWf {
    started_count: Arc<AtomicUsize>,
}

#[workflow_methods(factory_only)]
impl NondeterministicTimerWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        if ctx.state(|wf| wf.started_count.fetch_add(1, Ordering::Relaxed)) == 0 {
            ctx.timer(Duration::from_secs(1)).await;
        }
        ctx.timer(Duration::from_secs(1)).await;
        Ok(())
    }
}

/// Verifies nondeterministic behavior in workflows results in automatic WFT failure with the
/// appropriate nondeterminism cause.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[case::with_cache(true)]
#[case::without_cache(false)]
#[tokio::test]
async fn test_wf_task_rejected_properly_due_to_nondeterminism(#[case] use_cache: bool) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_timer_wf_completes("1");
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher =
        Box::new(|_, cause, _| matches!(cause, WorkflowTaskFailedCause::NonDeterministicError));
    let started_count = Arc::new(AtomicUsize::new(0));
    let count_clone = started_count.clone();
    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |cfg| {
            if use_cache {
                cfg.max_cached_workflows = 2;
            }
        },
        |options| {
            options
                .register_workflow_with_factory(move || NondeterministicTimerWf {
                    started_count: count_clone.clone(),
                })
                .unwrap();
        },
    );

    let task_queue = "fake_tq".to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert_eq!(2, started_count.load(Ordering::Relaxed));
}

#[workflow]
#[derive(Default)]
struct ActivityIdOrTypeChangeWf;

#[workflow_methods]
impl ActivityIdOrTypeChangeWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        (id_change, local_act): (bool, bool),
    ) -> WorkflowResult<()> {
        if local_act {
            if id_change {
                ctx.execute_local_activity(
                    StdActivities::default,
                    (),
                    LocalActivityOptions::builder()
                        .activity_id("I'm bad and wrong!".to_string())
                        .build(),
                )
                .await?;
            } else {
                ctx.execute_local_activity(
                    UntypedActivity::new("not the activity type"),
                    RawValue::empty(),
                    Default::default(),
                )
                .await?;
            }
        } else if id_change {
            ctx.execute_activity(
                StdActivities::default,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .activity_id("I'm bad and wrong!".to_string())
                    .build(),
            )
            .await?;
        } else {
            ctx.execute_activity(
                UntypedActivity::new("not the activity type"),
                RawValue::empty(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
        }
        Ok(())
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[tokio::test]
async fn activity_id_or_type_change_is_nondeterministic(
    #[values(true, false)] use_cache: bool,
    #[values(true, false)] id_change: bool,
    #[values(true, false)] local_act: bool,
) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let mut t: TestHistoryBuilder = if local_act {
        canned_histories::single_local_activity("1")
    } else {
        canned_histories::single_activity("1")
    };
    t.set_flags_first_wft(&[CoreInternalFlags::IdAndTypeDeterminismChecks], &[]);
    t.set_wf_input((id_change, local_act).as_json_payload().unwrap());
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher = Box::new(move |_, cause, f| {
        let should_contain = if id_change {
            "does not match activity id"
        } else {
            "does not match activity type"
        };
        matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
            && matches!(f, Some(Failure {
                message,
                ..
            }) if message.contains(should_contain))
    });
    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |cfg| {
            if use_cache {
                cfg.max_cached_workflows = 2;
            }
        },
        |options| {
            options
                .register_workflow::<ActivityIdOrTypeChangeWf>()
                .unwrap();
        },
    );

    let task_queue = "fake_tq".to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![(id_change, local_act).as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct ChildWfIdOrTypeChangeWf;

#[workflow_methods]
impl ChildWfIdOrTypeChangeWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>, id_change: bool) -> WorkflowResult<()> {
        if id_change {
            ctx.start_child_workflow(
                UntypedWorkflow::new("child"),
                RawValue::new(vec![]),
                ChildWorkflowOptions::workflow_id("I'm bad and wrong!".to_string()),
            )
            .await?;
        } else {
            ctx.start_child_workflow(
                UntypedWorkflow::new("not the child wf type"),
                RawValue::new(vec![]),
                ChildWorkflowOptions::workflow_id("1".to_string()),
            )
            .await?;
        }
        Ok(())
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[tokio::test]
async fn child_wf_id_or_type_change_is_nondeterministic(
    #[values(true, false)] use_cache: bool,
    #[values(true, false)] id_change: bool,
) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let mut t = canned_histories::single_child_workflow("1");
    t.set_flags_first_wft(&[CoreInternalFlags::IdAndTypeDeterminismChecks], &[]);
    t.set_wf_input(id_change.as_json_payload().unwrap());
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher = Box::new(move |_, cause, f| {
        let should_contain = if id_change {
            "does not match child workflow id"
        } else {
            "does not match child workflow type"
        };
        matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
            && matches!(f, Some(Failure {
                message,
                ..
            }) if message.contains(should_contain))
    });
    let mut worker = crate::common::mock_sdk_cfg_with_options(
        mh,
        |cfg| {
            if use_cache {
                cfg.max_cached_workflows = 2;
            }
        },
        |options| {
            options
                .register_workflow::<ChildWfIdOrTypeChangeWf>()
                .unwrap();
        },
    );

    let task_queue = "fake_tq".to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![id_change.as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
struct TokioSleepWf {
    attempt: Arc<AtomicUsize>,
}

#[workflow_methods(factory_only)]
impl TokioSleepWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let attempt = ctx.state(|wf| wf.attempt.fetch_add(1, Ordering::Relaxed));
        // Always start the SDK timer so the history is consistent across retries. On the first
        // attempt, also race a tokio::time::sleep which triggers nondeterminism detection.
        if attempt == 0 {
            workflows::select! {
                _ = ctx.timer(Duration::from_secs(1)) => {},
                _ = tokio::time::sleep(Duration::from_millis(50)).fuse() => {},
            }
        } else {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

/// Verifies that workflows using non-SDK futures (like `tokio::time::sleep`) are detected by the
/// nondeterminism detector and result in a WFT failure recorded in history.
#[tokio::test]
async fn nondeterministic_future_detection_fails_wft() {
    let wf_name = "nondeterministic_future_detection_fails_wft";
    let mut starter = CoreWfStarter::new(wf_name);
    let attempt = Arc::new(AtomicUsize::new(0));
    let attempt_clone = attempt.clone();
    starter
        .sdk_config
        .register_workflow_with_factory(move || TokioSleepWf {
            attempt: attempt_clone.clone(),
        })
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            TokioSleepWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_task_queue().to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    // The workflow should complete successfully after the WFT failure, because the server
    // retries the task and on the second attempt the tokio sleep is skipped.
    handle.get_result(Default::default()).await.unwrap();
    // Should have run at least twice: once with the detected nondeterminism, once to complete.
    assert!(attempt.load(Ordering::Relaxed) >= 2);

    // Verify history contains a WorkflowTaskFailed event with our detection message
    let history = starter.get_history().await;
    let wft_failures: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::WorkflowTaskFailed)
        .collect();
    assert_eq!(
        wft_failures.len(),
        1,
        "Expected one WorkflowTaskFailed event in history"
    );
    let has_detection_failure = wft_failures.iter().any(|e| {
        matches!(
            e.attributes.as_ref(),
            Some(WorkflowTaskFailedEventAttributes(attr))
                if attr.failure.as_ref().is_some_and(|f|
                    f.message.contains("Nondeterministic future detected"))
        )
    });
    assert!(
        has_detection_failure,
        "Expected a WorkflowTaskFailed event with nondeterminism detection message"
    );
}

/// What task 1 of [`ChangedBeforeReplayWf`] runs. In each, a timer gives task 2.
#[derive(Clone, Copy, Debug)]
enum FirstTask {
    Timer,
    TimerWithLocalActivity,
    /// Two local activities start together and end apart, so each result has its own activation
    /// of task 1. The timer starts after both.
    TwoLocalActivitiesThenTimer,
    /// The changed code upserts a memo in place of the timer and completes, so Core finds the
    /// nondeterminism.
    TimerReplacedByMemoUpsert,
}

#[workflow]
struct ChangedBeforeReplayWf {
    first_task: FirstTask,
    /// Set in each instance after the first, as a deploy of changed code would be.
    changed: bool,
}

#[workflow_methods(factory_only)]
impl ChangedBeforeReplayWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let (first_task, changed) = ctx.state(|wf| (wf.first_task, wf.changed));
        // A future that wakes itself while the SDK polls it is a non-SDK wake, which the SDK
        // reports as nondeterminism in that activation. Tokio's `yield_now` wakes after the poll.
        let changed_code = async {
            if changed {
                let mut woke = false;
                futures::future::poll_fn(|cx| {
                    if woke {
                        return Poll::Ready(());
                    }
                    woke = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                })
                .await;
            }
        };
        let echo = || {
            ctx.execute_local_activity(
                StdActivities::echo,
                "hi".to_string(),
                LocalActivityOptions::default(),
            )
        };
        match first_task {
            FirstTask::Timer => {
                changed_code.await;
                ctx.timer(Duration::from_secs(2)).await;
            }
            FirstTask::TimerWithLocalActivity => {
                let after_echo = async {
                    let echoed = echo().await;
                    changed_code.await;
                    echoed
                };
                let (_, echoed) = workflows::join!(ctx.timer(Duration::from_secs(2)), after_echo);
                echoed?;
            }
            FirstTask::TwoLocalActivitiesThenTimer => {
                let after_echo = async {
                    let echoed = echo().await;
                    changed_code.await;
                    echoed
                };
                let delayed = ctx.execute_local_activity(
                    StdActivities::delay,
                    Duration::from_secs(1),
                    LocalActivityOptions::default(),
                );
                let (echoed, delayed) = workflows::join!(after_echo, delayed);
                echoed?;
                delayed?;
                ctx.timer(Duration::from_secs(2)).await;
            }
            FirstTask::TimerReplacedByMemoUpsert if changed => {
                ctx.upsert_memo([("deployed", Some(MemoValue::new(true)))])?;
            }
            FirstTask::TimerReplacedByMemoUpsert => {
                ctx.timer(Duration::from_secs(2)).await;
            }
        }
        Ok(())
    }
}

/// Returns the type of each event of `events`, with the cause and the message of a failed task,
/// the message of the workflow's failure, and the activation index of a local activity's result.
fn history_summary(events: &[HistoryEvent]) -> Vec<String> {
    (events.iter())
        .map(|event| match &event.attributes {
            Some(MarkerRecordedEventAttributes(marker)) => {
                let (data, _) = extract_local_activity_marker_details(&mut marker.details.clone());
                let data = data.expect("Only local activities record markers here");
                format!("MarkerRecorded({:?})", data.activation_index)
            }
            Some(WorkflowTaskFailedEventAttributes(failed)) => format!(
                "WorkflowTaskFailed({:?}: {})",
                failed.cause(),
                failed.failure.as_ref().map_or("", |f| f.message.as_str())
            ),
            Some(WorkflowExecutionFailedEventAttributes(failed)) => format!(
                "WorkflowExecutionFailed({})",
                failed.failure.as_ref().map_or("", |f| f.message.as_str())
            ),
            _ => format!("{:?}", event.event_type()),
        })
        .collect()
}

/// Runs [`ChangedBeforeReplayWf`] on a worker with no workflow cache, so task 2 builds a second
/// instance, which runs the changed code and replays task 1. Returns the summary of history once
/// the workflow closes, or after 30 seconds.
async fn run_changed_before_replay(name: &str, first_task: FirstTask) -> Vec<String> {
    let mut starter = CoreWfStarter::new(name);
    starter.sdk_config.max_cached_workflows = 0;
    starter.sdk_config.workflow_failure_errors = HashSet::from([WorkflowErrorType::Nondeterminism]);
    starter.sdk_config.register_activities(StdActivities);
    let instances = AtomicUsize::new(0);
    starter
        .sdk_config
        .register_workflow_with_factory(move || ChangedBeforeReplayWf {
            first_task,
            changed: instances.fetch_add(1, Ordering::Relaxed) > 0,
        })
        .unwrap();
    let mut worker = starter.worker().await;
    worker.fetch_results = false;
    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            ChangedBeforeReplayWf::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    let core_worker = worker.core_worker();
    let watcher = async {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let events = starter.get_history().await.events;
            let closed = events.iter().any(|e| {
                matches!(
                    e.event_type(),
                    EventType::WorkflowExecutionFailed | EventType::WorkflowExecutionCompleted
                )
            });
            if closed || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        core_worker.initiate_shutdown();
    };
    let runner = async { worker.run_until_done().await.unwrap() };
    tokio::join!(watcher, runner);
    history_summary(&starter.get_history().await.events)
}

/// Asserts that the workflow failed once, with a nondeterminism error whose message starts with
/// `message`, and with no failed task.
fn assert_fails_once_with(summary: &[String], message: &str) {
    let failures: Vec<&String> = (summary.iter())
        .filter(|event| event.contains("Failed("))
        .collect();
    assert_eq!(
        failures.len(),
        1,
        "Expected exactly one failure event in history: {summary:#?}"
    );
    let expected = format!("WorkflowExecutionFailed({message}");
    assert!(
        failures[0].starts_with(&expected),
        "Expected the only failure to be the workflow failing with `{message}`: {summary:#?}"
    );
    assert!(
        !summary.iter().any(|event| event == "WorkflowTaskTimedOut"),
        "Expected no workflow task to time out, which would mean the server refused a \
         completion: {summary:#?}"
    );
}

/// Asserts as [`assert_fails_once_with`] does, with the nondeterminism error that the SDK reported.
fn assert_fails_once_with_langs_error(summary: &[String]) {
    assert_fails_once_with(summary, "[TMPRL1100] Nondeterministic future detected");
}

// The replay fails its first activation, before the timer that task 1 started.
#[tokio::test]
async fn changed_code_before_a_replayed_timer_fails_the_workflow_with_langs_error() {
    let name = "changed_code_before_a_replayed_timer_fails_the_workflow_with_langs_error";
    let summary = run_changed_before_replay(name, FirstTask::Timer).await;
    assert_fails_once_with_langs_error(&summary);
}

// The replay fails the activation with the local activity's result, after the one that started
// the timer, so the timer must not start again.
#[tokio::test]
async fn changed_code_at_a_replayed_local_activity_result_starts_no_second_timer() {
    let name = "changed_code_at_a_replayed_local_activity_result_starts_no_second_timer";
    let summary = run_changed_before_replay(name, FirstTask::TimerWithLocalActivity).await;
    assert_fails_once_with_langs_error(&summary);
    let timers = summary.iter().filter(|event| *event == "TimerStarted");
    assert_eq!(
        timers.count(),
        1,
        "Expected the replayed timer not to be started a second time: {summary:#?}"
    );
}

// Task 1 records the second result for a later activation than the first, so replay holds it
// while the SDK fails the first. The SDK's answer to the held result must not replace its failure.
#[tokio::test]
async fn changed_code_at_the_first_of_two_replayed_local_activity_results_fails_the_workflow_once()
{
    let name =
        "changed_code_at_the_first_of_two_replayed_local_activity_results_fails_the_workflow_once";
    let summary = run_changed_before_replay(name, FirstTask::TwoLocalActivitiesThenTimer).await;
    let first_task = [
        "MarkerRecorded(Some(1))",
        "MarkerRecorded(Some(2))",
        "TimerStarted",
    ];
    assert!(
        summary.windows(3).any(|w| w == first_task),
        "Expected task 1 to record the two results for activations 1 and 2, then the timer: \
         {summary:#?}"
    );
    assert_fails_once_with_langs_error(&summary);
}

// Core finds that history has the timer where the changed code upserts a memo. The changed code's
// completion of the workflow is not in history, and must not go out next to Core's failure.
#[tokio::test]
async fn changed_code_that_core_finds_fails_the_workflow_with_cores_error() {
    let name = "changed_code_that_core_finds_fails_the_workflow_with_cores_error";
    let summary = run_changed_before_replay(name, FirstTask::TimerReplacedByMemoUpsert).await;
    assert_fails_once_with(&summary, "[TMPRL1100] Nondeterminism error");
}

#[workflow]
#[derive(Default)]
struct NonSdkFutureInUpdateWf {
    done: bool,
}

#[workflow_methods]
impl NonSdkFutureInUpdateWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|wf| wf.done).await?;
        Ok(())
    }

    #[update]
    async fn finish(
        ctx: &mut WorkflowContext<Self>,
        _: (),
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // A future that wakes itself while the SDK polls it, as in `ChangedBeforeReplayWf`.
        let mut woke = false;
        futures::future::poll_fn(|cx| {
            if woke {
                return Poll::Ready(());
            }
            woke = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await;
        ctx.state_mut(|wf| wf.done = true);
        Ok(())
    }
}

/// Set to the workflow's task queue in the worker process that
/// [`a_non_sdk_future_in_an_update_of_a_cached_run_fails_the_workflow_once`] starts.
const UPDATE_WORKER_TASK_QUEUE: &str = "UPDATE_WORKER_TASK_QUEUE";

/// Returns a starter for [`NonSdkFutureInUpdateWf`] that fails workflows on nondeterminism and
/// keeps one run in the cache, so an update to a cached run gets a task with only the update.
fn non_sdk_future_in_update_starter(name: &str) -> CoreWfStarter {
    let mut starter = CoreWfStarter::new(name);
    if let Ok(task_queue) = std::env::var(UPDATE_WORKER_TASK_QUEUE) {
        starter.sdk_config = integ_sdk_config(&task_queue);
    }
    starter.sdk_config.max_cached_workflows = 1;
    starter.sdk_config.workflow_failure_errors = HashSet::from([WorkflowErrorType::Nondeterminism]);
    starter
        .sdk_config
        .register_workflow::<NonSdkFutureInUpdateWf>()
        .unwrap();
    starter
}

/// Runs `worker` until `done` holds for the history of the starter's workflow, or for `limit`.
async fn run_until_history(
    starter: &CoreWfStarter,
    worker: &mut TestWorker,
    limit: Duration,
    done: impl Fn(&[HistoryEvent]) -> bool,
) {
    worker.fetch_results = false;
    let core_worker = worker.core_worker();
    let watcher = async {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline && !done(&starter.get_history().await.events) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        core_worker.initiate_shutdown();
    };
    let runner = async { worker.run_until_done().await.unwrap() };
    tokio::join!(watcher, runner);
}

fn closed(events: &[HistoryEvent]) -> bool {
    events.iter().any(|e| {
        matches!(
            e.event_type(),
            EventType::WorkflowExecutionFailed | EventType::WorkflowExecutionCompleted
        )
    })
}

// The worker has the run in its cache, so the update comes in a task with only the update. The
// SDK fails that task, so the workflow must fail once with the SDK's error, and the worker must
// keep running. That worker is a second process, so the test sees what is left if it ends.
#[tokio::test]
async fn a_non_sdk_future_in_an_update_of_a_cached_run_fails_the_workflow_once() {
    let name = "a_non_sdk_future_in_an_update_of_a_cached_run_fails_the_workflow_once";
    let mut starter = non_sdk_future_in_update_starter(name);
    let mut worker = starter.worker().await;
    if std::env::var(UPDATE_WORKER_TASK_QUEUE).is_ok() {
        worker.fetch_results = false;
        let core_worker = worker.core_worker();
        let stop = async {
            tokio::time::sleep(Duration::from_secs(20)).await;
            core_worker.initiate_shutdown();
        };
        let runner = async { worker.run_until_done().await.unwrap() };
        tokio::join!(stop, runner);
        return;
    }

    let task_queue = starter.get_task_queue().to_owned();
    let test_name = format!("{}::{name}", module_path!().split_once("::").unwrap().1);
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            test_name.as_str(),
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(UPDATE_WORKER_TASK_QUEUE, &task_queue)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let child = tokio::task::spawn_blocking(move || child.wait_with_output());
    let mut options = WorkflowStartOptions::new(task_queue.clone(), task_queue).build();
    options.task_timeout = Some(Duration::from_secs(2));
    let handle = worker
        .submit_workflow(NonSdkFutureInUpdateWf::run, (), options)
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        let events = starter.get_history().await.events;
        if events
            .iter()
            .any(|e| e.event_type() == EventType::WorkflowTaskCompleted)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let update = tokio::time::timeout(
        Duration::from_secs(15),
        handle.execute_update(
            NonSdkFutureInUpdateWf::finish,
            (),
            WorkflowExecuteUpdateOptions::default(),
        ),
    )
    .await;
    let child = child.await.unwrap().unwrap();
    let child_stderr = String::from_utf8_lossy(&child.stderr);
    let child_end: Vec<&str> = (child_stderr.lines())
        .filter(|line| line.contains("overflowed") || line.contains("fatal runtime error"))
        .collect();
    let after_the_worker = history_summary(&starter.get_history().await.events);
    println!("update result: {update:?}");
    println!("worker exit: {:?}, its end: {child_end:?}", child.status);
    println!("history after the worker: {after_the_worker:#?}");

    // A new worker with the same code, as a restart would be.
    run_until_history(&starter, &mut worker, Duration::from_secs(15), closed).await;
    let after_the_restart = history_summary(&starter.get_history().await.events);
    let description = handle.describe(Default::default()).await.unwrap();
    let raw = &description.raw_description;
    let status = raw.workflow_execution_info.as_ref().map(|i| i.status());
    let pending_attempt = raw.pending_workflow_task.as_ref().map(|t| t.attempt);
    println!("history after the restart: {after_the_restart:#?}");
    println!("status: {status:?}, pending workflow task attempt: {pending_attempt:?}");

    assert!(
        child.status.success(),
        "Expected the worker to keep running. Exit: {:?}, end: {child_end:?}",
        child.status
    );
    assert_matches!(
        &update,
        Ok(Err(WorkflowUpdateError::NotFound(status))) if status.message().contains("aborted"),
        "Expected the failure to abort the update: {update:?}"
    );
    assert_fails_once_with_langs_error(&after_the_restart);
}

/// The signals that the other worker handles in [`a_task_that_reads_a_newer_history`]. The server
/// pages history by about 256 stored batches, and each signal's task adds three, so these fill
/// more than one page.
const SIGNALS_ON_THE_OTHER_WORKER: usize = 150;

/// Completes `act` as the workflows of
/// [`a_nondeterminism_failure_at_the_end_of_a_newer_history_page_returns`] do. Each task starts an
/// hour timer, numbered by the activation's history length so that a replay sends the same one.
fn start_an_hour_timer(act: &WorkflowActivation) -> WorkflowActivationCompletion {
    if act.is_only_eviction() {
        return WorkflowActivationCompletion::empty(act.run_id.clone());
    }
    WorkflowActivationCompletion::from_cmd(
        act.run_id.clone(),
        start_timer_cmd(act.history_length, Duration::from_secs(3600)),
    )
}

/// The ids of the started events that end a task in `events`, as Core's paginator finds them
/// before it has the last page. The first completed task tells whether chunking is off. Closing
/// events and updates are left out, since these workflows have none.
fn ends_of_tasks(events: &[HistoryEvent]) -> Vec<i64> {
    let chunking_disabled = (events.iter())
        .find(|e| e.event_type() == EventType::WorkflowTaskCompleted)
        .is_some_and(|completed| {
            matches!(
                completed.attributes.as_ref(),
                Some(WorkflowTaskCompletedEventAttributes(a))
                    if a.sdk_metadata.as_ref().is_some_and(|m| m.core_used_flags.contains(
                        &(CoreInternalFlags::DisableWorkflowTaskChunking as u32)
                    ))
            )
        });
    let mut ends = vec![];
    let (mut saw_command, mut saw_command_or_started) = (false, false);
    for (ix, event) in events.iter().enumerate() {
        saw_command |= event.is_command_event();
        saw_command_or_started |=
            saw_command || event.event_type() == EventType::WorkflowExecutionStarted;
        if event.event_type() != EventType::WorkflowTaskStarted {
            continue;
        }
        let ends_here = match events.get(ix + 1).map(HistoryEvent::event_type) {
            Some(EventType::WorkflowTaskFailed | EventType::WorkflowTaskTimedOut) => false,
            None if chunking_disabled => false,
            _ if chunking_disabled => true,
            Some(EventType::WorkflowTaskCompleted) => {
                match events.get(ix + 2).map(HistoryEvent::event_type) {
                    Some(EventType::WorkflowTaskScheduled) => saw_command,
                    Some(_) => true,
                    None => saw_command_or_started,
                }
            }
            _ => saw_command_or_started,
        };
        if ends_here {
            ends.push(event.event_id);
            (saw_command, saw_command_or_started) = (false, false);
        }
    }
    ends
}

/// The last event of the first update that Core's paginator builds from `pages` for a task whose
/// previous started event is `previous_started`. `None` if the paginator loads every page.
fn end_of_the_first_update(pages: &[Vec<HistoryEvent>], previous_started: i64) -> Option<i64> {
    let mut loaded = vec![];
    for (ix, page) in pages.iter().enumerate() {
        loaded.extend(page.iter().cloned());
        let ends = ends_of_tasks(&loaded);
        let ends_after_previous = ends.iter().filter(|end| **end > previous_started).count();
        if ix + 1 < pages.len() && ends_after_previous >= 2 {
            return ends.last().copied();
        }
    }
    None
}

/// A sticky task that reads a history newer than itself, from
/// [`a_task_that_reads_a_newer_history`].
struct TaskThatReadsANewerHistory {
    core: Arc<Worker>,
    slots: Arc<TicketedWftSlots>,
    run_id: String,
    /// The last event of the first update that Core builds from the history it fetches.
    first_update_end: i64,
    /// Lang's poll that gets the first activation of the task.
    first_activation: JoinHandle<Result<WorkflowActivation, PollError>>,
}

/// A sticky task comes for a run that the worker no longer caches while lang holds the only cache
/// slot, so the task waits in Core until it times out, and another worker runs the workflow on.
/// Both workers complete each activation with `complete`. Lang then completes the activation that
/// holds the slot, so Core fetches the history from the start for the task. That history goes past
/// the task's own started event, and the paginator stops at a page that ends inside a later task.
async fn a_task_that_reads_a_newer_history(
    name: &str,
    complete: fn(&WorkflowActivation) -> WorkflowActivationCompletion,
) -> TaskThatReadsANewerHistory {
    let mut starter = CoreWfStarter::new(name);
    starter.set_core_task_types(WorkerTaskTypes::workflow_only());
    let mut other_starter = starter.clone_no_worker();
    let slots = Arc::new(TicketedWftSlots {
        nonsticky: Semaphore::new(1),
        sticky: Semaphore::new(0),
    });
    let mut tuner = TunerBuilder::default();
    tuner.workflow_slot_supplier(slots.clone());
    starter.set_core_tuner(Arc::new(tuner.build()));
    starter.set_core_cfg_mutator(|c| {
        c.max_cached_workflows = 1;
        c.sticky_queue_schedule_to_start_timeout = Duration::from_secs(1);
        c.workflow_failure_errors =
            HashSet::from([temporalio_sdk_core::WorkflowErrorType::Nondeterminism]);
    });
    let core = starter.get_core_worker().await;
    let client = starter.get_core_client().await;
    let task_queue = starter.get_task_queue().to_owned();
    let start = |workflow_id: String, task_timeout: Duration| {
        let mut options = WorkflowStartOptions::new(task_queue.clone(), workflow_id).build();
        options.task_timeout = Some(task_timeout);
        client.start_workflow(UntypedWorkflow::new("waits"), RawValue::empty(), options)
    };
    let handle = client.get_workflow_handle::<UntypedWorkflow>(&task_queue);
    let signal = || {
        handle.signal(
            UntypedSignal::new("go"),
            RawValue::empty(),
            WorkflowSignalOptions::default(),
        )
    };

    // This worker runs the first task.
    start(task_queue.clone(), Duration::from_secs(2))
        .await
        .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    let run_id = act.run_id.clone();
    core.complete_workflow_activation(complete(&act))
        .await
        .unwrap();
    // A sticky poll waits for the signal's task.
    slots.sticky.add_permits(1);
    core.request_workflow_eviction(&run_id);
    let eviction = core.poll_workflow_activation().await.unwrap();
    assert!(eviction.is_only_eviction());
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();

    // Another workflow takes the cache slot, and lang keeps its first activation. Its task timeout
    // outlasts the other worker's signals, so its task is still valid when lang completes it.
    start(format!("{task_queue}-other"), Duration::from_secs(60))
        .await
        .unwrap();
    slots.nonsticky.add_permits(1);
    let kept = core.poll_workflow_activation().await.unwrap();
    assert_ne!(kept.run_id, run_id);
    let poller = core.clone();
    let next_activation = tokio::spawn(async move { poller.poll_workflow_activation().await });

    // The signal's task goes to this worker's sticky queue, and waits in Core for a cache slot.
    signal().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let events = loop {
        let events = starter.get_history().await.events;
        if events
            .iter()
            .any(|e| e.event_type() == EventType::WorkflowTaskTimedOut)
        {
            break events;
        }
        assert!(
            Instant::now() < deadline,
            "Expected the signal's task to time out"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let timed_out = events
        .iter()
        .position(|e| e.event_type() == EventType::WorkflowTaskTimedOut)
        .unwrap();
    assert_eq!(
        events[timed_out - 1].event_type(),
        EventType::WorkflowTaskStarted,
        "Expected this worker to have started the signal's task"
    );
    let previous_started = (events[..timed_out - 1].iter().rev())
        .find(|e| e.event_type() == EventType::WorkflowTaskStarted)
        .unwrap()
        .event_id;

    // Another worker runs the next attempt, then a task for each signal.
    let other_core = other_starter.get_core_worker().await;
    loop {
        let act = other_core.poll_workflow_activation().await.unwrap();
        other_core
            .complete_workflow_activation(complete(&act))
            .await
            .unwrap();
        if !act.is_replaying {
            break;
        }
    }
    for _ in 0..SIGNALS_ON_THE_OTHER_WORKER {
        signal().await.unwrap();
        let act = other_core.poll_workflow_activation().await.unwrap();
        other_core
            .complete_workflow_activation(complete(&act))
            .await
            .unwrap();
    }
    other_core.initiate_shutdown();
    while let Ok(act) = other_core.poll_workflow_activation().await {
        other_core
            .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
            .await
            .unwrap();
    }
    other_core.shutdown().await;

    // The pages that Core gets when it fetches the history from the start.
    let mut pages = vec![];
    let mut next_page_token = vec![];
    loop {
        let page = client
            .clone()
            .get_workflow_execution_history(
                GetWorkflowExecutionHistoryRequest {
                    namespace: client.namespace(),
                    execution: Some(WorkflowExecution {
                        workflow_id: task_queue.clone(),
                        run_id: run_id.clone(),
                    }),
                    next_page_token,
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap()
            .into_inner();
        pages.push(page.history.unwrap_or_default().events);
        next_page_token = page.next_page_token;
        if next_page_token.is_empty() {
            break;
        }
    }
    let page_ends: Vec<_> = (pages.iter())
        .filter_map(|page| page.last().map(|e| e.event_id))
        .collect();
    let first_update_end = end_of_the_first_update(&pages, previous_started).unwrap_or_else(|| {
        panic!("Expected the paginator to stop before the last page. Page ends: {page_ends:?}")
    });
    println!(
        "previous started {previous_started}, page ends {page_ends:?}, the first update ends at \
         {first_update_end}"
    );

    // Lang completes the kept activation, so the other run is evicted and the task gets the slot.
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(kept.run_id))
        .await
        .unwrap();
    TaskThatReadsANewerHistory {
        core,
        slots,
        run_id,
        first_update_end,
        first_activation: next_activation,
    }
}

/// Lets `core` take every task again, and shuts it down.
async fn shut_down(core: &Worker, slots: &TicketedWftSlots) {
    slots.grant_all();
    core.initiate_shutdown();
    while let Ok(act) = core.poll_workflow_activation().await {
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
            .await
            .unwrap();
    }
    core.shutdown().await;
}

// Each task starts a timer, and lang fails the activation of the last task of the first update.
// Its completion must return.
#[tokio::test]
async fn a_nondeterminism_failure_at_the_end_of_a_newer_history_page_returns() {
    let TaskThatReadsANewerHistory {
        core,
        slots,
        run_id,
        first_update_end,
        first_activation,
    } = a_task_that_reads_a_newer_history(
        "a_nondeterminism_failure_at_the_end_of_a_newer_history_page_returns",
        start_an_hour_timer,
    )
    .await;
    let replay = async {
        let mut act = first_activation.await.unwrap().unwrap();
        let mut failed = false;
        loop {
            let ends_the_first_update =
                i64::from(act.history_length) == first_update_end && !act.is_only_eviction();
            let completion = if ends_the_first_update {
                failed = true;
                WorkflowActivationCompletion::fail(
                    act.run_id.clone(),
                    "changed".into(),
                    Some(WorkflowTaskFailedCause::NonDeterministicError),
                )
            } else {
                start_an_hour_timer(&act)
            };
            core.complete_workflow_activation(completion).await.unwrap();
            if act.is_only_eviction() && act.run_id == run_id {
                break failed;
            }
            act = core.poll_workflow_activation().await.unwrap();
        }
    };
    let failed = tokio::time::timeout(Duration::from_secs(30), replay)
        .await
        .expect("Expected lang's completions to return within 30 seconds");
    assert!(
        failed,
        "Expected lang to get the activation that ends at event {first_update_end}"
    );
    shut_down(&core, &slots).await;
}

// No task has a command, so the first update ends at a task that only the events after it on the
// first page show to be complete. Lang must get every signal of the history once, before the
// server refuses the completion of the timed out task and Core evicts the run.
#[tokio::test]
async fn a_task_without_commands_at_the_end_of_a_newer_history_page_reaches_lang() {
    let TaskThatReadsANewerHistory {
        core,
        slots,
        run_id,
        first_activation,
        ..
    } = a_task_that_reads_a_newer_history(
        "a_task_without_commands_at_the_end_of_a_newer_history_page_reaches_lang",
        |act| WorkflowActivationCompletion::empty(act.run_id.clone()),
    )
    .await;
    let replay = async {
        let mut act = first_activation.await.unwrap().unwrap();
        let mut signals = vec![];
        loop {
            core.complete_workflow_activation(WorkflowActivationCompletion::empty(
                act.run_id.clone(),
            ))
            .await
            .unwrap();
            if act.run_id == run_id {
                if act.is_only_eviction() {
                    break (signals, act);
                }
                signals.extend((act.jobs.iter()).filter_map(|job| match &job.variant {
                    Some(workflow_activation_job::Variant::SignalWorkflow(signal)) => {
                        Some(signal.originating_event_id)
                    }
                    _ => None,
                }));
            }
            act = core.poll_workflow_activation().await.unwrap();
        }
    };
    let (signals, eviction) = tokio::time::timeout(Duration::from_secs(30), replay)
        .await
        .expect("Expected lang's completions to return within 30 seconds");
    assert_eq!(
        signals.len(),
        SIGNALS_ON_THE_OTHER_WORKER + 1,
        "Expected lang to get every signal of the history once before the eviction {eviction:?}"
    );
    assert!(
        signals.windows(2).all(|pair| pair[0] < pair[1]),
        "Expected lang to get the signals in history order: {signals:?}"
    );
    shut_down(&core, &slots).await;
}

#[workflow]
#[derive(Default)]
struct ReproChannelMissingWf;

#[workflow_methods]
impl ReproChannelMissingWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.patched("wrongid");
        ctx.timer(Duration::from_secs(1)).await;
        Ok(())
    }
}

/// Repros a situation where if, upon completing a task there is some internal error which causes
/// us to want to auto-fail the workflow task while there is also an outstanding eviction, the wf
/// would get evicted but then try to send some info down the completion channel afterward, causing
/// a panic.
#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
async fn repro_channel_missing_because_nondeterminism() {
    for _ in 1..50 {
        let wf_id = "fakeid";
        let wf_type = DEFAULT_WORKFLOW_TYPE;
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_has_change_marker("patch-1", false);
        let _ts = t.add_by_type(EventType::TimerStarted);
        t.add_workflow_task_scheduled_and_started();

        let mock = mock_worker_client();
        let mut mh =
            MockPollCfg::from_resp_batches(wf_id, t, [1.into(), ResponseType::AllHistory], mock);
        mh.num_expected_fails = 1;
        let mut worker = crate::common::mock_sdk_cfg_with_options(
            mh,
            |cfg| {
                cfg.max_cached_workflows = 2;
            },
            |options| {
                options
                    .register_workflow::<ReproChannelMissingWf>()
                    .unwrap();
            },
        );

        let task_queue = "fake_tq".to_owned();
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
}
