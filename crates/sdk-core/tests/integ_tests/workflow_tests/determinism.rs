use crate::common::{CoreWfStarter, WorkflowHandleExt, activity_functions::StdActivities};
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
use temporalio_client::WorkflowStartOptions;
use temporalio_common::{
    UntypedActivity, UntypedWorkflow,
    data_converters::RawValue,
    protos::{
        coresdk::{AsJsonPayloadExt, FromJsonPayloadExt},
        temporal::api::{
            enums::v1::{EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
            history::v1::{
                HistoryEvent,
                history_event::Attributes::{
                    WorkflowExecutionFailedEventAttributes, WorkflowTaskFailedEventAttributes,
                },
            },
        },
    },
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, MemoValue, WorkflowContext,
    WorkflowResult, runtime::WorkflowErrorType, workflows,
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, canned_histories},
    test_help::{CoreInternalFlags, MockPollCfg, ResponseType, mock_worker_client},
};

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

/// Returns the type of each event of `events`, with the cause and the message of a failed task
/// and the message of the workflow's failure.
fn history_summary(events: &[HistoryEvent]) -> Vec<String> {
    (events.iter())
        .map(|event| match &event.attributes {
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
    assert_eq!(failures.len(), 1, "{summary:#?}");
    let expected = format!("WorkflowExecutionFailed({message}");
    assert!(failures[0].starts_with(&expected), "{summary:#?}");
    assert!(
        !summary.iter().any(|event| event == "WorkflowTaskTimedOut"),
        "{summary:#?}"
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
    assert_eq!(timers.count(), 1, "{summary:#?}");
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
