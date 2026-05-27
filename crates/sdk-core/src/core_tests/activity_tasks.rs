use crate::{
    ActivityHeartbeat, CompleteActivityError, Worker, advance_fut, job_assert, prost_dur,
    replay::{TestHistoryBuilder, canned_histories},
    test_help::{
        FakeWfResponses, MockPollCfg, MockWorkerInputs, MocksHolder, QueueResponse, WorkerExt,
        WorkflowCachingPolicy, build_fake_worker, build_mock_pollers, build_multihist_mock_sg,
        fanout_tasks, gen_assert_and_reply, mock_manual_poller, mock_poller, mock_worker,
        poll_and_reply, single_hist_mock_sg, start_timer_cmd, test_worker_cfg,
    },
    worker::{
        PollerBehavior,
        client::mocks::{mock_manual_worker_client, mock_worker_client},
    },
};
use futures_util::FutureExt;
use itertools::Itertools;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque, hash_map::Entry},
    future,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use temporalio_common::{
    protos::{
        coresdk::{
            ActivityTaskCompletion,
            activity_result::{
                ActivityExecutionResult, ActivityResolution, Success, activity_execution_result,
                activity_resolution,
            },
            activity_task::{ActivityCancelReason, ActivityTask, Cancel, activity_task},
            workflow_activation::{
                ResolveActivity, WorkflowActivationJob, workflow_activation_job,
            },
            workflow_commands::{
                ActivityCancellationType, CompleteWorkflowExecution, RequestCancelActivity,
                ScheduleActivity,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            command::v1::{ScheduleActivityTaskCommandAttributes, command::Attributes},
            enums::v1::EventType,
            workflowservice::v1::{
                PollActivityTaskQueueResponse, RecordActivityTaskHeartbeatResponse,
                RespondActivityTaskCanceledResponse, RespondActivityTaskCompletedResponse,
                RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedResponse,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use tokio::{join, time::sleep};
use tokio_util::sync::CancellationToken;

fn three_tasks() -> VecDeque<PollActivityTaskQueueResponse> {
    VecDeque::from(vec![
        PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            ..Default::default()
        },
        PollActivityTaskQueueResponse {
            task_token: vec![2],
            activity_id: "act2".to_string(),
            ..Default::default()
        },
        PollActivityTaskQueueResponse {
            task_token: vec![3],
            activity_id: "act3".to_string(),
            ..Default::default()
        },
    ])
}

#[tokio::test]
async fn max_activities_respected() {
    let _task_q = "q";
    let mut tasks = three_tasks();
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_poll_activity_task()
        .times(3)
        .returning(move |_, _| Ok(tasks.pop_front().unwrap()));
    mock_client
        .expect_complete_activity_task()
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let worker = Worker::new_test(
        test_worker_cfg()
            .max_outstanding_activities(2_usize)
            .build()
            .unwrap(),
        mock_client,
    );

    // We allow two outstanding activities, therefore first two polls should return right away
    let r1 = worker.poll_activity_task().await.unwrap();
    let _r2 = worker.poll_activity_task().await.unwrap();
    // Third poll should block until we complete one of the first two. To ensure this, manually
    // poll it a bunch to see it's not resolving.
    let poll_fut = worker.poll_activity_task();
    advance_fut!(poll_fut);
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: r1.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    poll_fut.await.unwrap();
}

#[tokio::test]
async fn activity_not_found_returns_ok() {
    let mut mock_client = mock_worker_client();
    // Mock won't even be called, since we weren't tracking activity
    mock_client.expect_complete_activity_task().times(0);

    let core = mock_worker(MocksHolder::from_client_with_activities(mock_client, []));

    core.complete_activity_task(ActivityTaskCompletion {
        task_token: vec![1],
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.drain_activity_poller_and_shutdown().await;
}

#[tokio::test]
async fn heartbeats_report_cancels_only_once() {
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_record_activity_heartbeat()
        .times(2)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
                activity_paused: false,
                activity_reset: false,
            })
        });
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    mock_client
        .expect_cancel_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCanceledResponse::default()));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [
            PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "act1".to_string(),
                heartbeat_timeout: Some(prost_dur!(from_millis(1))),
                ..Default::default()
            }
            .into(),
            PollActivityTaskQueueResponse {
                task_token: vec![2],
                activity_id: "act2".to_string(),
                heartbeat_timeout: Some(prost_dur!(from_millis(1))),
                ..Default::default()
            }
            .into(),
        ],
    ));

    let act = core.poll_activity_task().await.unwrap();
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    // We have to wait a beat for the heartbeat to be processed
    sleep(Duration::from_millis(10)).await;
    let act = core.poll_activity_task().await.unwrap();
    assert_matches!(
        &act,
        ActivityTask {
            task_token,
            variant: Some(activity_task::Variant::Cancel(_)),
            ..
        } => { task_token == &vec![1] }
    );

    // Verify if we try to record another heartbeat for this task we do not issue a double cancel
    // Allow heartbeat delay to elapse
    sleep(Duration::from_millis(10)).await;
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    // Wait delay again to flush heartbeat
    sleep(Duration::from_millis(10)).await;
    // Now complete it as cancelled
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,

        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    // Since cancels always come before new tasks, if we get a new non-cancel task, we did not
    // double-issue cancels.
    let act = core.poll_activity_task().await.unwrap();
    assert_matches!(
        &act,
        ActivityTask {
            task_token,
            variant: Some(activity_task::Variant::Start(_)),
            ..
        } => { task_token == &[2] }
    );
    // Complete it so shutdown goes through
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,

        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.drain_activity_poller_and_shutdown().await;
}

#[tokio::test]
async fn activity_cancel_interrupts_poll() {
    let mut mock_poller = mock_manual_poller();
    let shutdown_token = CancellationToken::new();
    let shutdown_token_clone = shutdown_token.clone();
    let mut poll_resps = VecDeque::from(vec![
        async {
            Some(Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                heartbeat_timeout: Some(prost_dur!(from_secs(1))),
                ..Default::default()
            }))
        }
        .boxed(),
        async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Some(Ok(Default::default()))
        }
        .boxed(),
        async move {
            shutdown_token.cancelled().await;
            None
        }
        .boxed(),
    ]);
    mock_poller
        .expect_poll()
        .times(3)
        .returning(move || poll_resps.pop_front().unwrap());

    let mut mock_client = mock_manual_worker_client();
    mock_client
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            async {
                Ok(RecordActivityTaskHeartbeatResponse {
                    cancel_requested: true,
                    activity_paused: false,
                    activity_reset: false,
                })
            }
            .boxed()
        });
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| async { Ok(RespondActivityTaskCompletedResponse::default()) }.boxed());

    let mw = MockWorkerInputs {
        act_poller: Some(Box::from(mock_poller)),
        ..Default::default()
    };
    let core = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));
    let last_finisher = AtomicUsize::new(0);
    // Perform first poll to get the activity registered
    let act = core.poll_activity_task().await.unwrap();
    // Poll should block until heartbeat is sent, issuing the cancel, and interrupting the poll
    join! {
        async {
            core.record_activity_heartbeat(ActivityHeartbeat {
                task_token: act.task_token,

                details: vec![vec![1_u8, 2, 3].into()],
            });
            last_finisher.store(1, Ordering::SeqCst);
        },
        async {
            let act = core.poll_activity_task().await.unwrap();
            // Must complete this activity for shutdown to finish
            core.complete_activity_task(
                ActivityTaskCompletion {
                    task_token: act.task_token,

                    result: Some(ActivityExecutionResult::ok(vec![1].into())),
                }
            ).await.unwrap();
            last_finisher.store(2, Ordering::SeqCst);
            shutdown_token_clone.cancel();
        }
    };
    // So that we know we blocked
    assert_eq!(last_finisher.load(Ordering::Acquire), 2);
    core.drain_activity_poller_and_shutdown().await;
}

#[tokio::test]
async fn activity_poll_timeout_retries() {
    let mock_client = mock_worker_client();
    let mut calls = 0;
    let mut mock_act_poller = mock_poller();
    mock_act_poller.expect_poll().times(3).returning(move || {
        calls += 1;
        if calls <= 2 {
            Some(Ok(PollActivityTaskQueueResponse::default()))
        } else {
            Some(Ok(PollActivityTaskQueueResponse {
                task_token: b"hello!".to_vec(),
                ..Default::default()
            }))
        }
    });
    let mw = MockWorkerInputs {
        act_poller: Some(Box::from(mock_act_poller)),
        ..Default::default()
    };
    let core = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));
    let r = core.poll_activity_task().await.unwrap();
    assert_matches!(r.task_token.as_slice(), b"hello!");
}

#[tokio::test]
async fn many_concurrent_heartbeat_cancels() {
    // Run a whole bunch of activities in parallel, having the server return cancellations for
    // them after a few successful heartbeats
    const CONCURRENCY_NUM: usize = 5;

    let mut mock_client = mock_manual_worker_client();
    let mut poll_resps = VecDeque::from(
        (0..CONCURRENCY_NUM)
            .map(|i| {
                async move {
                    Ok(PollActivityTaskQueueResponse {
                        task_token: i.to_be_bytes().to_vec(),
                        heartbeat_timeout: Some(prost_dur!(from_millis(200))),
                        ..Default::default()
                    })
                }
                .boxed()
            })
            .collect::<Vec<_>>(),
    );
    poll_resps.push_back(
        async {
            future::pending::<()>().await;
            unreachable!()
        }
        .boxed(),
    );
    let mut calls_map = HashMap::<_, i32>::new();
    mock_client
        .expect_poll_activity_task()
        .returning(move |_, _| poll_resps.pop_front().unwrap());
    mock_client
        .expect_cancel_activity_task()
        .returning(move |_, _| async move { Ok(Default::default()) }.boxed());
    mock_client
        .expect_record_activity_heartbeat()
        .returning(move |tt, _| {
            let calls = match calls_map.entry(tt) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() += 1;
                    *e.get()
                }
                Entry::Vacant(v) => *v.insert(1),
            };
            async move {
                if calls < 5 {
                    Ok(RecordActivityTaskHeartbeatResponse {
                        cancel_requested: false,
                        activity_paused: false,
                        activity_reset: false,
                    })
                } else {
                    Ok(RecordActivityTaskHeartbeatResponse {
                        cancel_requested: true,
                        activity_paused: false,
                        activity_reset: false,
                    })
                }
            }
            .boxed()
        });

    let worker = &Worker::new_test(
        test_worker_cfg()
            .max_outstanding_activities(CONCURRENCY_NUM)
            // Only 1 poll at a time to avoid over-polling and running out of responses
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .build()
            .unwrap(),
        mock_client,
    );

    // Poll all activities first so they are registered
    for _ in 0..CONCURRENCY_NUM {
        worker.poll_activity_task().await.unwrap();
    }

    // Spawn "activities"
    fanout_tasks(CONCURRENCY_NUM, |i| async move {
        let task_token = i.to_be_bytes().to_vec();
        for _ in 0..12 {
            worker.record_activity_heartbeat(ActivityHeartbeat {
                task_token: task_token.clone(),
                details: vec![],
            });
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    // Read all the cancellations and reply to them concurrently
    fanout_tasks(CONCURRENCY_NUM, |_| async move {
        let r = worker.poll_activity_task().await.unwrap();
        assert_matches!(
            r,
            ActivityTask {
                variant: Some(activity_task::Variant::Cancel(_)),
                ..
            }
        );
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: r.task_token.clone(),
                result: Some(ActivityExecutionResult::cancel_from_details(None)),
            })
            .await
            .unwrap();
    })
    .await;

    worker.drain_activity_poller_and_shutdown().await;
}

#[tokio::test]
async fn activity_timeout_no_double_resolve() {
    let t = canned_histories::activity_double_resolve_repro();
    let core = build_fake_worker("fake_wf_id", t, [3]);
    let activity_id = 1;

    poll_and_reply(
        &core,
        WorkflowCachingPolicy::NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        seq: activity_id,
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..Default::default()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![
                    RequestCancelActivity { seq: activity_id }.into(),
                    start_timer_cmd(2, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
                        }),
                        ..
                    }
                )),
                vec![],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::FireTimer(_)
                ),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;

    core.drain_pollers_and_shutdown().await;
}

/// Regression test for a race between stream shutdown and eviction completion.
///
/// With zero-cache and `ignore_evicts_on_shutdown=true`, the workflow stream can
/// decide to shut down (via `shutdown_done()`) after accepting an eviction
/// completion message but before processing it. The BumpStream from
/// `initiate_shutdown` is queued in the local channel ahead of the eviction
/// completion (FIFO). The stream processes BumpStream, sees `shutdown_done()`
/// is true, and exits — dropping the eviction completion's response channel
/// sender without fulfilling it.
#[tokio::test]
async fn eviction_completion_during_shutdown_does_not_panic() {
    let t = canned_histories::activity_double_resolve_repro();
    let mut mh = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: "fake_wf_id".to_owned(),
            hist: t,
            response_batches: vec![3.into()],
        }],
        true,
        0,
    );
    // Prevent PollerDead from arriving so we control shutdown timing exactly.
    mh.make_wft_stream_interminable();
    let core = mock_worker(mh);
    let activity_id = 1;

    poll_and_reply(
        &core,
        WorkflowCachingPolicy::NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        seq: activity_id,
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..Default::default()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![
                    RequestCancelActivity { seq: activity_id }.into(),
                    start_timer_cmd(2, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
                        }),
                        ..
                    }
                )),
                vec![],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::FireTimer(_)
                ),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;

    // The zero-cache eviction produces an eviction activation after the last
    // completion's PostActivation is processed.
    let eviction = core.poll_workflow_activation().await.unwrap();
    assert!(eviction.is_only_eviction());

    // Cancel the shutdown token and enqueue BumpStream into the local channel.
    // The stream will process BumpStream, see shutdown_done()=true (because
    // ignore_evicts_on_shutdown skips the eviction activation), and exit.
    core.initiate_shutdown();

    // Complete the eviction. Its WFActCompleteMsg is queued AFTER BumpStream
    // (same FIFO channel), so the stream exits before processing it — dropping
    // the response channel sender. Without the fix, rx.await returning Err for
    // an empty completion triggers dbg_panic!.
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(eviction.run_id))
        .await
        .unwrap();

    core.finalize_shutdown().await;
}

#[tokio::test]
async fn can_heartbeat_acts_during_shutdown() {
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: false,
                activity_paused: false,
                activity_reset: false,
            })
        });
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            heartbeat_timeout: Some(prost_dur!(from_millis(1))),
            ..Default::default()
        }
        .into()],
    ));

    let act = core.poll_activity_task().await.unwrap();
    // Make sure shutdown has progressed before trying to record heartbeat / complete
    let shutdown_fut = core.shutdown();
    advance_fut!(shutdown_fut);
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),

        details: vec![vec![1_u8, 2, 3].into()],
    });
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,

        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.drain_activity_poller_and_shutdown().await;
}

/// Verifies that if a user has tried to record a heartbeat and then immediately after failed the
/// activity, that we flush those details before reporting the failure completion.
#[tokio::test]
async fn complete_act_with_fail_flushes_heartbeat() {
    let last_hb = 50;
    let mut mock_client = mock_worker_client();
    let last_seen_payload = Rc::new(RefCell::new(None));
    let lsp = last_seen_payload.clone();
    mock_client
        .expect_record_activity_heartbeat()
        // Two times b/c we always record the first heartbeat, and we'll flush the last
        .times(2)
        .returning_st(move |_, payload| {
            *lsp.borrow_mut() = payload;
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: false,
                activity_paused: false,
                activity_reset: false,
            })
        });
    mock_client
        .expect_fail_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskFailedResponse::default()));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            heartbeat_timeout: Some(prost_dur!(from_secs(10))),
            ..Default::default()
        }
        .into()],
    ));

    let act = core.poll_activity_task().await.unwrap();
    // Record a bunch of heartbeats
    for i in 1..=last_hb {
        core.record_activity_heartbeat(ActivityHeartbeat {
            task_token: act.task_token.clone(),
            details: vec![vec![i].into()],
        });
    }
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token.clone(),
        result: Some(ActivityExecutionResult::fail("Ahh".into())),
    })
    .await
    .unwrap();
    core.drain_activity_poller_and_shutdown().await;

    // Verify the last seen call to record a heartbeat had the last detail payload
    let last_seen_payload = &last_seen_payload.take().unwrap().payloads[0];
    assert_eq!(last_seen_payload.data, &[last_hb]);
}

#[tokio::test]
async fn max_tq_acts_set_passed_to_poll_properly() {
    let rate = 9.28;
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_poll_activity_task()
        .returning(move |_, ao| {
            assert_eq!(ao.max_tasks_per_sec, Some(rate));
            Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                ..Default::default()
            })
        });

    let cfg = test_worker_cfg()
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
        .max_task_queue_activities_per_second(rate)
        .build()
        .unwrap();
    let worker = Worker::new_test(cfg, mock_client);
    worker.poll_activity_task().await.unwrap();
}

#[tokio::test]
async fn max_worker_acts_per_second_respected() {
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_poll_activity_task()
        .returning(move |_, _| {
            Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "some-id".to_string(),
                ..Default::default()
            })
        });
    mock_client
        .expect_complete_activity_task()
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let cfg = test_worker_cfg()
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
        .max_outstanding_activities(10_usize)
        .max_worker_activities_per_second(1.0)
        .build()
        .unwrap();
    let worker = Worker::new_test(cfg, mock_client);
    let start = Instant::now();
    let mut received = 0;
    while start.elapsed().as_millis() < 900 {
        let at = worker.poll_activity_task().await.unwrap();
        received += 1;
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: at.task_token,
                result: Some(ActivityExecutionResult::ok("hi".into())),
            })
            .await
            .unwrap();
    }
    // Two will be allowed because of the initial request. Without ratelimit in effect, this number
    // would be comically high due to the mocks responding very fast.
    assert_eq!(received, 2);
}

#[rstest::rstest]
#[tokio::test]
async fn no_eager_activities_requested_when_worker_options_disable_it(
    #[values("no_remote", "throttle")] reason: &'static str,
) {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled("act_id");
    let started_event_id = t.add_activity_task_started(scheduled_event_id);
    t.add_activity_task_completed(scheduled_event_id, started_event_id, b"hi".into());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let num_eager_requested = Arc::new(AtomicUsize::new(0));
    let num_eager_requested_clone = num_eager_requested.clone();

    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(move |req| {
            // Store the number of eager activities requested to be checked below
            let count = req
                .commands
                .into_iter()
                .filter(|c| match c.attributes {
                    Some(Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes {
                            request_eager_execution,
                            ..
                        },
                    )) => request_eager_execution,
                    _ => false,
                })
                .count();
            num_eager_requested_clone.store(count, Ordering::Relaxed);
            Ok(RespondWorkflowTaskCompletedResponse {
                workflow_task: None,
                activity_tasks: vec![],
                reset_history_event_id: 0,
            })
        });
    let mut mock = single_hist_mock_sg(wfid, t, [1], mock, true);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 2;
        if reason == "no_remote" {
            wc.task_types = WorkerTaskTypes::workflow_only();
        } else {
            wc.max_task_queue_activities_per_second = Some(1.0);
        }
    });
    let core = mock_worker(mock);

    // Test start
    let wf_task = core.poll_workflow_activation().await.unwrap();
    let cmds = vec![
        ScheduleActivity {
            seq: 1,
            activity_id: "act_id".to_string(),
            task_queue: core.get_config().task_queue.clone(),
            cancellation_type: ActivityCancellationType::TryCancel as i32,
            ..Default::default()
        }
        .into(),
    ];

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        wf_task.run_id,
        cmds,
    ))
    .await
    .unwrap();

    core.drain_pollers_and_shutdown().await;

    assert_eq!(num_eager_requested.load(Ordering::Relaxed), 0);
}

/// This test verifies that activity tasks which come as replies to completing a WFT are properly
/// delivered via polling.
#[tokio::test]
async fn activity_tasks_from_completion_are_delivered() {
    // Construct the history - one task with 5 activities, 4 on the same task queue, and 1 on a
    // different queue, 3 activities will be executed eagerly as specified by the
    // MAX_EAGER_ACTIVITY_RESERVATIONS_PER_WORKFLOW_TASK constant.
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let act_same_queue_scheduled_ids = (1..4)
        .map(|i| t.add_activity_task_scheduled(format!("act_id_{i}_same_queue")))
        .collect_vec();
    t.add_activity_task_scheduled("act_id_same_queue_not_eager");
    t.add_activity_task_scheduled("act_id_different_queue");
    for scheduled_event_id in act_same_queue_scheduled_ids {
        let started_event_id = t.add_activity_task_started(scheduled_event_id);
        t.add_activity_task_completed(scheduled_event_id, started_event_id, b"hi".into());
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let num_eager_requested = Arc::new(AtomicUsize::new(0));
    // Clone it to move into the callback below
    let num_eager_requested_clone = num_eager_requested.clone();

    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(move |req| {
            // Store the number of eager activities requested to be checked below
            let count = req
                .commands
                .into_iter()
                .filter(|c| match c.attributes {
                    Some(Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes {
                            request_eager_execution,
                            ..
                        },
                    )) => request_eager_execution,
                    _ => false,
                })
                .count();
            num_eager_requested_clone.store(count, Ordering::Relaxed);
            Ok(RespondWorkflowTaskCompletedResponse {
                workflow_task: None,
                activity_tasks: (1..4)
                    .map(|i| PollActivityTaskQueueResponse {
                        task_token: vec![i],
                        activity_id: format!("act_id_{i}_same_queue"),
                        ..Default::default()
                    })
                    .collect_vec(),
                reset_history_event_id: 0,
            })
        });
    mock.expect_complete_activity_task()
        .times(3)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    let act_tasks: Vec<QueueResponse<PollActivityTaskQueueResponse>> = vec![];
    let mut mh = MockPollCfg::from_resp_batches(wfid, t, [1], mock);
    mh.enforce_correct_number_of_polls = true;
    mh.activity_responses = Some(act_tasks);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 2);
    let core = mock_worker(mock);
    let task_queue = core.get_config().task_queue.clone();

    // Test start
    let wf_task = core.poll_workflow_activation().await.unwrap();
    let mut cmds = (1..4)
        .map(|seq| {
            ScheduleActivity {
                seq,
                activity_id: format!("act_id_{seq}_same_queue"),
                task_queue: task_queue.clone(),
                cancellation_type: ActivityCancellationType::TryCancel as i32,
                ..Default::default()
            }
            .into()
        })
        .collect_vec();
    cmds.push(
        ScheduleActivity {
            seq: 4,
            activity_id: "act_id_same_queue_not_eager".to_string(),
            task_queue: task_queue.clone(),
            cancellation_type: ActivityCancellationType::TryCancel as i32,
            ..Default::default()
        }
        .into(),
    );
    cmds.push(
        ScheduleActivity {
            seq: 5,
            activity_id: "act_id_different_queue".to_string(),
            task_queue: "different_queue".to_string(),
            cancellation_type: ActivityCancellationType::Abandon as i32,
            ..Default::default()
        }
        .into(),
    );

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        wf_task.run_id,
        cmds,
    ))
    .await
    .unwrap();

    // We should see the 3 eager activities when we poll now
    for i in 1..4 {
        let act_task = core.poll_activity_task().await.unwrap();
        assert_eq!(act_task.task_token, vec![i]);

        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token.clone(),
            result: Some(ActivityExecutionResult::ok("hi".into())),
        })
        .await
        .unwrap();
    }

    core.drain_pollers_and_shutdown().await;

    // Verify only a single eager activity was scheduled (the one on our worker's task queue)
    assert_eq!(num_eager_requested.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn retryable_net_error_exhaustion_is_nonfatal() {
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Err(tonic::Status::internal("retryable error")));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            heartbeat_timeout: Some(prost_dur!(from_secs(10))),
            ..Default::default()
        }
        .into()],
    ));

    let act = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.drain_activity_poller_and_shutdown().await;
}

#[tokio::test]
async fn cant_complete_activity_with_unset_result_payload() {
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_poll_activity_task()
        .returning(move |_, _| {
            Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                ..Default::default()
            })
        });

    let worker = Worker::new_test(test_worker_cfg().build().unwrap(), mock_client);
    let t = worker.poll_activity_task().await.unwrap();
    let res = worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: t.task_token,
            result: Some(ActivityExecutionResult {
                status: Some(activity_execution_result::Status::Completed(Success {
                    result: None,
                })),
            }),
        })
        .await;
    assert_matches!(
        res,
        Err(CompleteActivityError::MalformedActivityCompletion { .. })
    )
}

#[rstest::rstest]
#[tokio::test]
async fn graceful_shutdown(#[values(true, false)] at_max_outstanding: bool) {
    let grace_period = Duration::from_millis(200);
    let mut tasks = three_tasks();
    let mut mock_act_poller = mock_poller();
    mock_act_poller
        .expect_poll()
        .times(3)
        .returning(move || Some(Ok(tasks.pop_front().unwrap())));
    mock_act_poller
        .expect_poll()
        .times(1)
        .returning(move || None);
    // They shall all be reported as failed
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_fail_activity_task()
        .times(3)
        .returning(|_, _| Ok(Default::default()));

    let max_outstanding = if at_max_outstanding { 3_usize } else { 100 };
    let mw = MockWorkerInputs {
        act_poller: Some(Box::from(mock_act_poller)),
        config: test_worker_cfg()
            .graceful_shutdown_period(grace_period)
            .max_outstanding_activities(max_outstanding)
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize)) // Makes test logic simple
            .build()
            .unwrap(),
        ..Default::default()
    };
    let worker = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));

    let _1 = worker.poll_activity_task().await.unwrap();

    // Wait at least the grace period after one poll - ensuring it doesn't trigger prematurely
    tokio::time::sleep(grace_period.mul_f32(1.1)).await;

    let _2 = worker.poll_activity_task().await.unwrap();
    let _3 = worker.poll_activity_task().await.unwrap();

    worker.initiate_shutdown();
    let expected_tts = HashSet::from([vec![1], vec![2], vec![3]]);
    let mut seen_tts = HashSet::new();
    for _ in 1..=3 {
        let cancel = worker.poll_activity_task().await.unwrap();
        assert_matches!(
            cancel.variant,
            Some(activity_task::Variant::Cancel(Cancel {
                reason,
                details
            })) if reason == ActivityCancelReason::WorkerShutdown as i32 && details.as_ref().is_some_and(|d| d.is_worker_shutdown)
        );
        seen_tts.insert(cancel.task_token);
    }
    assert_eq!(expected_tts, seen_tts);
    for tt in seen_tts {
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: tt,
                result: Some(ActivityExecutionResult::cancel_from_details(None)),
            })
            .await
            .unwrap();
    }
    worker.drain_pollers_and_shutdown().await;
}

#[rstest::rstest]
#[tokio::test]
async fn activities_must_be_flushed_to_server_on_shutdown(#[values(true, false)] use_grace: bool) {
    let grace_period = if use_grace {
        // Even though the grace period is shorter than the client call, the client call will still
        // go through. This is reasonable since the client has a timeout anyway, and it's unlikely
        // that a user *needs* an extremely short grace period (it'd be kind of pointless in that
        // case). They can always force-kill their worker in this situation.
        Duration::from_millis(50)
    } else {
        Duration::from_secs(10)
    };
    let shutdown_finished: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
    let mut tasks = three_tasks();
    let mut mock_act_poller = mock_poller();
    mock_act_poller
        .expect_poll()
        .times(1)
        .returning(move || Some(Ok(tasks.pop_front().unwrap())));
    mock_act_poller
        .expect_poll()
        .times(1)
        .returning(move || None);
    let mut mock_client = mock_manual_worker_client();
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| {
            async {
                // We need some artificial delay here and there's nothing meaningful to sync with
                tokio::time::sleep(Duration::from_millis(100)).await;
                if shutdown_finished.load(Ordering::Acquire) {
                    panic!("Shutdown must complete *after* server sees the activity completion");
                }
                Ok(Default::default())
            }
            .boxed()
        });

    let mw = MockWorkerInputs {
        act_poller: Some(Box::from(mock_act_poller)),
        config: test_worker_cfg()
            .graceful_shutdown_period(grace_period)
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize)) // Makes test logic simple
            .build()
            .unwrap(),
        ..Default::default()
    };
    let worker = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));

    let task = worker.poll_activity_task().await.unwrap();

    let shutdown_task = async {
        worker.drain_activity_poller_and_shutdown().await;
        shutdown_finished.store(true, Ordering::Release);
    };
    let complete_task = async {
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok("hi".into())),
            })
            .await
            .unwrap();
    };
    join!(shutdown_task, complete_task);
}

#[tokio::test]
async fn heartbeat_response_can_be_paused() {
    let mut mock_client = mock_worker_client();
    // First heartbeat returns pause only
    mock_client
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: false,
                activity_paused: true,
                activity_reset: false,
            })
        });
    // Second heartbeat returns cancel only
    mock_client
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
                activity_paused: false,
                activity_reset: false,
            })
        });
    // Third heartbeat does all 3
    mock_client
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
                activity_paused: true,
                activity_reset: true,
            })
        });
    mock_client
        .expect_cancel_activity_task()
        .times(3)
        .returning(|_, _| Ok(RespondActivityTaskCanceledResponse::default()));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [
            PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "act1".to_string(),
                heartbeat_timeout: Some(prost_dur!(from_millis(1))),
                ..Default::default()
            }
            .into(),
            PollActivityTaskQueueResponse {
                task_token: vec![2],
                activity_id: "act2".to_string(),
                heartbeat_timeout: Some(prost_dur!(from_millis(1))),
                ..Default::default()
            }
            .into(),
            PollActivityTaskQueueResponse {
                task_token: vec![3],
                activity_id: "act3".to_string(),
                heartbeat_timeout: Some(prost_dur!(from_millis(1))),
                ..Default::default()
            }
            .into(),
        ],
    ));

    // The general testing pattern for each of these cases is:
    // 1. Poll for activity task
    // 2. Record activity heartbeat, get mocked heartbeat response
    // 3. Sleep for 10ms (waiting for heartbeat request to be flushed)
    // (i.e. sleep enough for the heartbeat flush interval to have elapsed)
    // 4. Poll for activity task.
    // We expect a cancellation activity task as they are prioritized (i.e. ordered before)
    // regular activity tasks.
    // 5. Assert that the received activity task is indeed a cancellation, with the reason
    // and details we expect.
    // 6. Complete the activity with a cancellation result.
    //
    // Repeat for subsequent test case(s).

    // Test pause only
    let act = core.poll_activity_task().await.unwrap();
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    sleep(Duration::from_millis(10)).await;
    let act = core.poll_activity_task().await.unwrap();
    assert_matches!(
        &act,
        ActivityTask {
            task_token,
            variant: Some(activity_task::Variant::Cancel(Cancel { reason, details })),
        } if
            task_token == &vec![1] &&
            *reason == ActivityCancelReason::Paused as i32 &&
            details.as_ref().is_some_and(|d| d.is_paused) &&
            details.as_ref().is_some_and(|d| !d.is_cancelled)
    );
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();

    // Test cancel only
    let act = core.poll_activity_task().await.unwrap();
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    sleep(Duration::from_millis(10)).await;
    let act = core.poll_activity_task().await.unwrap();
    assert_matches!(
        &act,
        ActivityTask {
            task_token,
            variant: Some(activity_task::Variant::Cancel(Cancel { reason, details })),
        } if
            task_token == &vec![2] &&
            *reason == ActivityCancelReason::Cancelled as i32 &&
            details.as_ref().is_some_and(|d| !d.is_paused) &&
            details.as_ref().is_some_and(|d| d.is_cancelled)
    );
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();

    // Test both pause and cancel (should prioritize cancel)
    let act = core.poll_activity_task().await.unwrap();
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    sleep(Duration::from_millis(10)).await;
    let act = core.poll_activity_task().await.unwrap();
    assert_matches!(
        &act,
        ActivityTask {
            task_token,
            variant: Some(activity_task::Variant::Cancel(Cancel { reason, details })),
        } if
            task_token == &vec![3] &&
            *reason == ActivityCancelReason::Cancelled as i32 &&
            details.as_ref().is_some_and(|d| d.is_paused) &&
            details.as_ref().is_some_and(|d| d.is_cancelled) &&
            details.as_ref().is_some_and(|d| d.is_reset)
    );
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();

    core.drain_activity_poller_and_shutdown().await;
}
