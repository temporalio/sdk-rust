mod activities;
mod cancel_external;
mod cancel_wf;
mod child_workflows;
mod client_interactions;
mod continue_as_new;
mod determinism;
mod eager;
mod local_activities;
mod modify_wf_properties;
mod nexus;
mod patches;
mod priority;
mod queries;
mod replay;
mod resets;
mod signals;
mod stickyness;
mod timers;
mod upsert_search_attrs;

use crate::{
    common::{
        CoreWfStarter, activity_functions::StdActivities, get_integ_runtime_options,
        history_from_proto_binary, init_core_and_create_wf, init_core_replay_preloaded,
        mock_sdk_cfg, prom_metrics,
    },
    integ_tests::metrics_tests,
};
use assert_matches::assert_matches;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use temporalio_client::{
    NamespacedClient, UntypedQuery, UntypedSignal, UntypedWorkflow, WorkflowExecutionInfo,
    WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
    errors::WorkflowGetResultError, grpc::WorkflowService,
};
use temporalio_common::{
    data_converters::RawValue,
    protos::{
        coresdk::{
            ActivityTaskCompletion, AsJsonPayloadExt, IntoCompletion,
            activity_result::ActivityExecutionResult,
            workflow_activation::{WorkflowActivationJob, workflow_activation_job},
            workflow_commands::{
                ActivityCancellationType, FailWorkflowExecution, QueryResult, QuerySuccess,
                StartTimer,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::WorkflowExecution,
            enums::v1::{CommandType, EventType},
            failure::v1::Failure,
            history::v1::history_event,
            sdk::v1::UserMetadata,
            workflowservice::v1::ResetStickyTaskQueueRequest,
        },
    },
    worker::{WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, LocalActivityOptions, TimerOptions, WorkflowContext, WorkflowResult,
    interceptors::WorkerInterceptor,
};
use temporalio_sdk_core::{
    CoreRuntime, PollError, PollerBehavior, TunerHolder, WorkflowErrorType, prost_dur,
    replay::{DEFAULT_WORKFLOW_TYPE, HistoryForReplay, canned_histories},
    test_help::{
        MockPollCfg, WorkerTestHelpers, drain_pollers_and_shutdown, schedule_activity_cmd,
    },
};
use tokio::{join, sync::Notify, time::sleep};
use tonic::IntoRequest;
// TODO: We should get expected histories for these tests and confirm that the history at the end
//  matches.

#[workflow]
#[derive(Default)]
struct ParallelWorkflowsWf;

#[workflow_methods]
impl ParallelWorkflowsWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(1)).await;
        Ok(())
    }
}

#[tokio::test]
async fn parallel_workflows_same_queue() {
    let wf_name = "parallel_workflows_same_queue";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut core = starter.worker().await;

    core.register_workflow::<ParallelWorkflowsWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    for i in 0..25 {
        core.submit_workflow(
            ParallelWorkflowsWf::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), format!("{wf_name}-{i}")).build(),
        )
        .await
        .unwrap();
    }
    core.run_until_done().await.unwrap();
}

// Ideally this would be a unit test, but returning a pending future with mockall bloats the mock
// code a bunch and just isn't worth it. Do it when https://github.com/asomers/mockall/issues/189 is
// fixed.
#[tokio::test]
async fn shutdown_aborts_actively_blocked_poll() {
    let mut starter = CoreWfStarter::new("shutdown_aborts_actively_blocked_poll");
    let core = starter.get_worker().await;
    // Begin the poll, and request shutdown from another thread after a small period of time.
    let tcore = core.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        drain_pollers_and_shutdown(&tcore).await;
    });
    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollError::ShutDown
    );
    handle.await.unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollError::ShutDown
    );
}

#[rstest::rstest]
#[tokio::test]
async fn fail_wf_task(#[values(true, false)] replay: bool) {
    let core = if replay {
        // We need to send the history twice, since we fail it the first time.
        let mut hist_proto = history_from_proto_binary("fail_wf_task.bin").await.unwrap();
        let hist = HistoryForReplay::new(hist_proto.clone(), "fake".to_string());
        if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(
            ref mut attrs,
        )) = hist_proto.events[0].attributes
        {
            attrs.original_execution_run_id = "run2".to_string();
            attrs.first_execution_run_id = "run2".to_string();
        }
        let hist2 = HistoryForReplay::new(hist_proto, "fake".to_string());
        Arc::new(init_core_replay_preloaded("fail_wf_task", [hist, hist2]))
    } else {
        let mut starter = init_core_and_create_wf("fail_wf_task").await;
        starter.get_worker().await
    };
    // Start with a timer
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_timer(&task.run_id, 0, Duration::from_millis(200))
        .await;

    // Then break for whatever reason
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::fail(
        task.run_id,
        Failure::application_failure("I did an oopsie".to_string(), false),
        None,
    ))
    .await
    .unwrap();

    // The server will want to retry the task. This time we finish the workflow -- but we need
    // to poll a couple of times as there will be more than one required workflow activation.
    // The first poll response will tell us to evict.
    core.handle_eviction().await;

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(prost_dur!(from_millis(200))),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn fail_workflow_execution() {
    let core = init_core_and_create_wf("fail_workflow_execution")
        .await
        .get_worker()
        .await;
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_timer(&task.run_id, 0, Duration::from_secs(1))
        .await;
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            FailWorkflowExecution {
                failure: Some(Failure::application_failure("I'm ded".to_string(), false)),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn signal_workflow() {
    let mut starter = init_core_and_create_wf("signal_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let signal_id_1 = "signal1";
    let signal_id_2 = "signal2";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Send the signals to the server
    let handle = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: workflow_id.clone(),
        run_id: Some(res.run_id.clone()),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone());
    handle
        .signal(
            UntypedSignal::new(signal_id_1),
            RawValue::empty(),
            WorkflowSignalOptions::default(),
        )
        .await
        .unwrap();
    handle
        .signal(
            UntypedSignal::new(signal_id_2),
            RawValue::empty(),
            WorkflowSignalOptions::default(),
        )
        .await
        .unwrap();

    let mut res = core.poll_workflow_activation().await.unwrap();
    // Sometimes both signals are complete at once, sometimes only one, depending on server
    // Converting test to wf function type would make this shorter
    if res.jobs.len() == 2 {
        assert_matches!(
            res.jobs.as_slice(),
            [
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                }
            ]
        );
    } else if res.jobs.len() == 1 {
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![],
        ))
        .await
        .unwrap();
        res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },]
        );
    }
    core.complete_execution(&res.run_id).await;
}

#[tokio::test]
async fn signal_workflow_signal_not_handled_on_workflow_completion() {
    let mut starter =
        init_core_and_create_wf("signal_workflow_signal_not_handled_on_workflow_completion").await;
    let core = starter.get_worker().await;
    let workflow_id = starter.get_task_queue().to_string();
    let signal_id_1 = "signal1";
    for i in 1..=2 {
        let res = core.poll_workflow_activation().await.unwrap();
        // Task is completed with a timer
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![
                StartTimer {
                    seq: 0,
                    start_to_fire_timeout: Some(prost_dur!(from_millis(10))),
                }
                .into(),
            ],
        ))
        .await
        .unwrap();

        let res = core.poll_workflow_activation().await.unwrap();

        if i == 1 {
            // First attempt we should only see the timer being fired
            assert_matches!(
                res.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                }]
            );

            let run_id = res.run_id.clone();

            // Send the signal to the server
            let sig_client = starter.get_client().await;
            WorkflowExecutionInfo {
                namespace: sig_client.namespace(),
                workflow_id: workflow_id.clone(),
                run_id: Some(res.run_id.clone()),
                first_execution_run_id: None,
            }
            .bind_untyped(sig_client.clone())
            .signal(
                UntypedSignal::new(signal_id_1),
                RawValue::empty(),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

            // Send completion - not having seen a poll response with a signal in it yet (unhandled
            // command error will be logged as a warning and an eviction will be issued)
            core.complete_execution(&run_id).await;
            // We should be told to evict
            core.handle_eviction().await;
            // Loop to the top to handle wf from the beginning
            continue;
        }

        // On the second attempt, we will see the signal we failed to handle as well as the timer
        assert_matches!(
            res.jobs.as_slice(),
            [
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                },
            ]
        );
        core.complete_execution(&res.run_id).await;
    }
}

#[tokio::test]
async fn wft_timeout_doesnt_create_unsolvable_autocomplete() {
    let activity_id = "act-1";
    let signal_at_start = "at-start";
    let signal_at_complete = "at-complete";
    let mut wf_starter = CoreWfStarter::new("wft_timeout_doesnt_create_unsolvable_autocomplete");
    // Test needs eviction on and a short timeout
    wf_starter.sdk_config.max_cached_workflows = 0_usize;
    wf_starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(1, 1, 1, 1));
    wf_starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(1_usize);
    wf_starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let core = wf_starter.get_worker().await;
    let client = wf_starter.get_client().await;
    let task_q = wf_starter.get_task_queue();
    let wf_id = &wf_starter.get_wf_id().to_owned();

    // Set up some helpers for polling and completing
    let poll_sched_act = || async {
        let wf_task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .into_completion(wf_task.run_id.clone()),
        )
        .await
        .unwrap();
        wf_task
    };
    wf_starter.start_wf_with_id(wf_id.to_string()).await;

    // Poll and schedule the activity
    let wf_task = poll_sched_act().await;
    // Before polling for a task again, we start and complete the activity and send the
    // corresponding signals.
    let ac_task = core.poll_activity_task().await.unwrap();
    let handle = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: wf_id.to_string(),
        run_id: Some(wf_task.run_id.clone()),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone());
    // Send the signals to the server & resolve activity -- sometimes this happens too fast
    sleep(Duration::from_millis(200)).await;
    handle
        .signal(
            UntypedSignal::new(signal_at_start),
            RawValue::empty(),
            WorkflowSignalOptions::default(),
        )
        .await
        .unwrap();
    // Complete activity successfully.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: ac_task.task_token,
        result: Some(ActivityExecutionResult::ok(Default::default())),
    })
    .await
    .unwrap();
    handle
        .signal(
            UntypedSignal::new(signal_at_complete),
            RawValue::empty(),
            WorkflowSignalOptions::default(),
        )
        .await
        .unwrap();
    // Now poll again, it will be an eviction b/c non-sticky mode.
    core.handle_eviction().await;
    // Start from the beginning
    poll_sched_act().await;
    let wf_task = core.poll_workflow_activation().await.unwrap();
    // Time out this time
    sleep(Duration::from_secs(2)).await;
    // Poll again, which should not have any work to do and spin, until the complete goes through.
    // Which will be rejected with not found, producing an eviction.
    let (wf_task, _) = tokio::join!(
        async { core.poll_workflow_activation().await.unwrap() },
        async {
            sleep(Duration::from_millis(500)).await;
            // Reply to the first one, finally
            core.complete_execution(&wf_task.run_id).await;
        }
    );
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // Do it all over again, without timing out this time
    poll_sched_act().await;
    let wf_task = core.poll_workflow_activation().await.unwrap();
    // Server can sometimes arbitrarily re-order the activity complete to be after the second signal
    // Seeing 3 jobs is enough info.
    assert_eq!(wf_task.jobs.len(), 3);
    core.complete_execution(&wf_task.run_id).await;
}

/// We had a bug related to polling being faster than completion causing issues with cache
/// overflow. This test intentionally makes completes slower than polls to evaluate that.
///
/// It's expected that this test may generate some task timeouts.
#[workflow]
#[derive(Default)]
struct SlowCompletesWf;

#[workflow_methods]
impl SlowCompletesWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for _ in 0..3 {
            ctx.start_activity(
                StdActivities::echo,
                "hi!".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn slow_completes_with_small_cache() {
    let wf_name = "slow_completes_with_small_cache";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(5, 10, 1, 1));
    starter.sdk_config.max_cached_workflows = 5_usize;
    let mut worker = starter.worker().await;

    worker.register_activities(StdActivities);

    worker.register_workflow::<SlowCompletesWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    for i in 0..20 {
        worker
            .submit_workflow(
                SlowCompletesWf::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), format!("{wf_name}_{i}")).build(),
            )
            .await
            .unwrap();
    }

    struct SlowCompleter {}
    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for SlowCompleter {
        async fn on_workflow_activation_completion(&self, _: &WorkflowActivationCompletion) {
            // They don't need to be much slower
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        fn on_shutdown(&self, _: &temporalio_sdk::Worker) {}
    }
    worker
        .run_until_done_intercepted(Some(SlowCompleter {}))
        .await
        .unwrap();
}

#[tokio::test]
#[rstest::rstest]
async fn deployment_version_correct_in_wf_info(#[values(true, false)] use_only_build_id: bool) {
    let wf_type = "deployment_version_correct_in_wf_info";
    let mut starter = CoreWfStarter::new(wf_type);
    starter.sdk_config.deployment_options = if use_only_build_id {
        WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "".to_string(),
                build_id: "1.0".to_string(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        }
    } else {
        WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "deployment-1".to_string(),
                build_id: "1.0".to_string(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        }
    };
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let core = starter.get_worker().await;
    starter.start_wf().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(
        res.deployment_version_for_current_task
            .as_ref()
            .unwrap()
            .build_id,
        "1.0"
    );
    if !use_only_build_id {
        assert_eq!(
            res.deployment_version_for_current_task
                .unwrap()
                .deployment_name,
            "deployment-1"
        );
    }
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Ensure a query on first wft also sees the correct id
    let query_handle = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: workflow_id.clone(),
        run_id: Some(res.run_id.clone()),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone());
    let query_fut = async {
        query_handle
            .query(
                UntypedQuery::new("q1"),
                RawValue::empty(),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap()
    };
    let complete_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        assert_eq!(
            task.deployment_version_for_current_task
                .as_ref()
                .unwrap()
                .build_id,
            "1.0"
        );
        if !use_only_build_id {
            assert_eq!(
                task.deployment_version_for_current_task
                    .unwrap()
                    .deployment_name,
                "deployment-1"
            );
        }
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(
                    QuerySuccess {
                        response: Some("done".into()),
                    }
                    .into(),
                ),
            }
            .into(),
        ))
        .await
        .unwrap();
    };
    join!(query_fut, complete_fut);
    starter.shutdown().await;
    WorkflowService::reset_sticky_task_queue(
        &mut client.clone(),
        ResetStickyTaskQueueRequest {
            namespace: client.namespace(),
            execution: Some(WorkflowExecution {
                workflow_id: workflow_id.clone(),
                run_id: "".to_string(),
            }),
        }
        .into_request(),
    )
    .await
    .unwrap();

    let mut starter = starter.clone_no_worker();
    starter.sdk_config.deployment_options = if use_only_build_id {
        WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "".to_string(),
                build_id: "2.0".to_string(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        }
    } else {
        WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "deployment-1".to_string(),
                build_id: "2.0".to_string(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        }
    };

    let core = starter.get_worker().await;

    let query_fut = async {
        query_handle
            .query(
                UntypedQuery::new("q2"),
                RawValue::empty(),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap()
    };
    let complete_fut = async {
        let res = core.poll_workflow_activation().await.unwrap();
        assert_eq!(
            res.deployment_version_for_current_task
                .as_ref()
                .unwrap()
                .build_id,
            "1.0"
        );
        if !use_only_build_id {
            assert_eq!(
                res.deployment_version_for_current_task
                    .unwrap()
                    .deployment_name,
                "deployment-1"
            );
        }
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id.clone(),
            vec![],
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        assert_eq!(
            task.deployment_version_for_current_task
                .as_ref()
                .unwrap()
                .build_id,
            "1.0"
        );
        if !use_only_build_id {
            assert_eq!(
                task.deployment_version_for_current_task
                    .unwrap()
                    .deployment_name,
                "deployment-1"
            );
        }
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(
                    QuerySuccess {
                        response: Some("done".into()),
                    }
                    .into(),
                ),
            }
            .into(),
        ))
        .await
        .unwrap();
    };
    join!(query_fut, complete_fut);

    client
        .get_workflow_handle::<UntypedWorkflow>(&workflow_id)
        .signal(
            UntypedSignal::new("whatever"),
            RawValue::empty(),
            WorkflowSignalOptions::default(),
        )
        .await
        .unwrap();

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(
        res.deployment_version_for_current_task
            .as_ref()
            .unwrap()
            .build_id,
        "2.0"
    );
    if !use_only_build_id {
        assert_eq!(
            res.deployment_version_for_current_task
                .unwrap()
                .deployment_name,
            "deployment-1"
        );
    }
    core.complete_execution(&res.run_id).await;
}

const NONDETERMINISM_WF_NAME: &str = "nondeterminism_errors_fail_workflow_when_configured_to";

#[rstest::rstest]
#[tokio::test]
async fn nondeterminism_errors_fail_workflow_when_configured_to(
    #[values(true, false)] whole_worker: bool,
) {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let wf_name = NONDETERMINISM_WF_NAME;
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let typeset = HashSet::from([WorkflowErrorType::Nondeterminism]);
    if whole_worker {
        starter.sdk_config.workflow_failure_errors = typeset;
    } else {
        starter.sdk_config.workflow_types_to_failure_errors =
            HashMap::from([(wf_name.to_owned(), typeset)]);
    }
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;
    worker.fetch_results = false;

    #[workflow]
    #[derive(Default)]
    struct NondeterminismTimerWf;

    #[workflow_methods]
    impl NondeterminismTimerWf {
        #[run(name = NONDETERMINISM_WF_NAME)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.timer(Duration::from_secs(1000)).await;
            Ok(())
        }
    }

    worker.register_workflow::<NondeterminismTimerWf>().unwrap();
    let client = starter.get_client().await;
    let core_worker = worker.core_worker();
    starter.start_with_worker(wf_name, &mut worker).await;

    let stopper = async {
        // Wait for the timer to show up in history and then stop the worker
        loop {
            let hist = starter.get_history().await;
            let has_timer_event = hist
                .events
                .iter()
                .any(|e| matches!(e.event_type(), EventType::TimerStarted));
            if has_timer_event {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        core_worker.initiate_shutdown();
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    join!(stopper, runner);

    // Restart the worker with a new, incompatible wf definition which will cause nondeterminism
    let mut starter = starter.clone_no_worker();
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct NondeterminismActivityWf;

    #[workflow_methods]
    impl NondeterminismActivityWf {
        #[run(name = NONDETERMINISM_WF_NAME)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.start_activity(
                StdActivities::echo,
                "hi".to_owned(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
            Ok(())
        }
    }

    worker
        .register_workflow::<NondeterminismActivityWf>()
        .unwrap();
    // We need to generate a task so that we'll encounter the error (first avoid WFT timeout)
    WorkflowService::reset_sticky_task_queue(
        &mut client.clone(),
        ResetStickyTaskQueueRequest {
            namespace: client.namespace(),
            execution: Some(WorkflowExecution {
                workflow_id: wf_id.clone(),
                run_id: "".to_string(),
            }),
        }
        .into_request(),
    )
    .await
    .unwrap();
    client
        .get_workflow_handle::<UntypedWorkflow>(&wf_id)
        .signal(
            UntypedSignal::new("hi"),
            RawValue::empty(),
            WorkflowSignalOptions::default(),
        )
        .await
        .unwrap();
    worker.expect_workflow_completion(&wf_id, None);
    // If we don't fail the workflow on nondeterminism, we'll get stuck here retrying the WFT
    worker.run_until_done().await.unwrap();

    let body = metrics_tests::get_text(format!("http://{addr}/metrics")).await;
    let match_this = format!(
        "temporal_workflow_failed{{namespace=\"default\",\
         service_name=\"temporal-core-sdk\",\
         task_queue=\"{wf_id}\",workflow_type=\"{wf_name}\"}} 1"
    );
    assert!(body.contains(&match_this));
}

const HISTORY_OUT_OF_ORDER_WF_NAME: &str = "history_out_of_order_on_restart";

#[tokio::test]
async fn history_out_of_order_on_restart() {
    let wf_name = HISTORY_OUT_OF_ORDER_WF_NAME;
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.workflow_failure_errors = HashSet::from([WorkflowErrorType::Nondeterminism]);
    let mut worker = starter.worker().await;
    let mut starter2 = starter.clone_no_worker();
    let mut worker2 = starter2.worker().await;

    let hit_sleep = Arc::new(Notify::new());
    let hit_sleep_clone1 = hit_sleep.clone();
    let hit_sleep_clone2 = hit_sleep.clone();

    #[workflow]
    struct HistoryOutOfOrderWf1 {
        hit_sleep: Arc<Notify>,
    }

    #[workflow_methods(factory_only)]
    impl HistoryOutOfOrderWf1 {
        #[run(name = HISTORY_OUT_OF_ORDER_WF_NAME)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.start_local_activity(
                StdActivities::echo,
                "hi".to_string(),
                LocalActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    ..Default::default()
                },
            )
            .await?;
            ctx.start_activity(
                StdActivities::echo,
                "hi".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
            ctx.state(|wf| wf.hit_sleep.notify_one());
            ctx.timer(Duration::from_secs(5)).await;
            Ok(())
        }
    }

    #[workflow]
    #[derive(Default)]
    struct HistoryOutOfOrderWf2;

    #[workflow_methods]
    impl HistoryOutOfOrderWf2 {
        #[run(name = HISTORY_OUT_OF_ORDER_WF_NAME)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.start_local_activity(
                StdActivities::echo,
                "hi".to_string(),
                LocalActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    ..Default::default()
                },
            )
            .await?;
            // Timer is added after restarting workflow
            ctx.timer(Duration::from_secs(1)).await;
            ctx.start_activity(
                StdActivities::echo,
                "hi".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
            ctx.timer(Duration::from_secs(2)).await;
            Ok(())
        }
    }

    worker.register_activities(StdActivities);
    worker2.register_activities(StdActivities);
    worker
        .register_workflow_with_factory(move || HistoryOutOfOrderWf1 {
            hit_sleep: hit_sleep_clone1.clone(),
        })
        .unwrap();
    worker2.register_workflow::<HistoryOutOfOrderWf2>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            HistoryOutOfOrderWf1::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned())
                .execution_timeout(Duration::from_secs(20))
                .build(),
        )
        .await
        .unwrap();

    let w1 = async {
        worker.run_until_done().await.unwrap();
    };
    let w2 = async {
        // wait to hit sleep
        hit_sleep_clone2.notified().await;
        starter.shutdown().await;
        // start new worker
        worker2.expect_workflow_completion(wf_name, None);
        worker2.run_until_done().await.unwrap();
    };
    join!(w1, w2);
    // The workflow should fail with the nondeterminism error
    let handle = starter
        .get_client()
        .await
        .get_workflow_handle::<UntypedWorkflow>(wf_name);
    let res = handle.get_result(Default::default()).await;
    assert_matches!(res, Err(WorkflowGetResultError::Failed(_)));
}

#[tokio::test]
async fn pass_timer_summary_to_metadata() {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let expected_user_metadata = Some(UserMetadata {
        summary: Some("timer summary".as_json_payload().unwrap()),
        details: None,
    });
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
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

    #[workflow]
    #[derive(Default)]
    struct PassTimerSummaryWf;

    #[workflow_methods]
    impl PassTimerSummaryWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.timer(TimerOptions {
                duration: Duration::from_secs(1),
                summary: Some("timer summary".to_string()),
            })
            .await;
            Ok(())
        }
    }

    let mut worker = mock_sdk_cfg(mock_cfg, |_| {});
    worker.register_workflow::<PassTimerSummaryWf>().unwrap();
    worker
        .submit_wf(
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowStartOptions::new("fake_tq".to_owned(), wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
