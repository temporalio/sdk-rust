use crate::common::{
    CoreWfStarter, WorkflowHandleExt, activity_functions::StdActivities, init_core_and_create_wf,
    init_core_replay_preloaded,
};
use anyhow::anyhow;
use assert_matches::assert_matches;
use futures_util::{future, future::join_all};
use std::{
    sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    Client, NamespacedClient, UntypedSignal, UntypedUpdate, UntypedWorkflow,
    WorkflowExecuteUpdateOptions, WorkflowExecutionInfo, WorkflowSignalOptions,
    WorkflowStartOptions, grpc::WorkflowService,
};
use temporalio_common::{
    data_converters::RawValue,
    protos::{
        coresdk::{
            ActivityTaskCompletion,
            activity_result::ActivityExecutionResult,
            workflow_activation::{
                WorkflowActivationJob, remove_from_cache::EvictionReason, workflow_activation_job,
            },
            workflow_commands::{
                CompleteWorkflowExecution, ScheduleLocalActivity, UpdateResponse, update_response,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::WorkflowExecution,
            enums::v1::{EventType, ResetReapplyType},
            workflowservice::v1::{ResetStickyTaskQueueRequest, ResetWorkflowExecutionRequest},
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, LocalActivityOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_core::{
    Worker, prost_dur,
    replay::HistoryForReplay,
    test_help::{WorkerTestHelpers, drain_pollers_and_shutdown, start_timer_cmd},
};
use tokio::{join, sync::Barrier};
use tonic::IntoRequest;
use uuid::Uuid;

#[derive(Clone, Copy)]
enum FailUpdate {
    Yes,
    No,
}

#[derive(Clone, Copy)]
enum CompleteWorkflow {
    Yes,
    No,
}

#[rstest::rstest]
#[tokio::test]
async fn update_workflow(#[values(FailUpdate::Yes, FailUpdate::No)] will_fail: FailUpdate) {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue();
    let update_id = "some_update";
    send_and_handle_update(
        workflow_id,
        update_id,
        will_fail,
        CompleteWorkflow::Yes,
        core.as_ref(),
        &client,
    )
    .await;

    // Make sure replay works
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(workflow_id)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();
    let with_id = HistoryForReplay::new(events, workflow_id.to_string());
    let replay_worker = init_core_replay_preloaded(workflow_id, [with_id]);
    // Init workflow comes by itself
    let act = replay_worker.poll_workflow_activation().await.unwrap();
    replay_worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    handle_update(will_fail, CompleteWorkflow::Yes, &replay_worker, 0).await;
}

#[tokio::test]
async fn reapplied_updates_due_to_reset() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue();
    let pre_reset_run_id = send_and_handle_update(
        workflow_id,
        "first-update",
        FailUpdate::No,
        CompleteWorkflow::Yes,
        core.as_ref(),
        &client,
    )
    .await;

    // Reset to before the update was accepted
    let workflow_task_finish_event_id = 4;

    let mut client_mut = client.clone();
    let reset_response = WorkflowService::reset_workflow_execution(
        &mut client_mut,
        #[allow(deprecated)]
        ResetWorkflowExecutionRequest {
            namespace: client.namespace(),
            workflow_execution: Some(WorkflowExecution {
                workflow_id: workflow_id.to_string(),
                run_id: pre_reset_run_id.clone(),
            }),
            workflow_task_finish_event_id,
            reset_reapply_type: ResetReapplyType::AllEligible as i32,
            request_id: Uuid::new_v4().to_string(),
            ..Default::default()
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();

    // Accept and complete the reapplied update
    // Index here is 2 because there will be start workflow & update random seed (from the reset)
    // first.
    handle_update(FailUpdate::No, CompleteWorkflow::No, core.as_ref(), 2).await;

    // Send a second update and complete the workflow
    let post_reset_run_id = send_and_handle_update(
        workflow_id,
        "second-update",
        FailUpdate::No,
        CompleteWorkflow::Yes,
        core.as_ref(),
        &client,
    )
    .await;

    assert_eq!(post_reset_run_id, reset_response.run_id);

    // Make sure replay works
    let events = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: workflow_id.to_string(),
        run_id: Some(post_reset_run_id.clone()),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone())
    .fetch_history(Default::default())
    .await
    .unwrap()
    .into_events();
    let with_id = HistoryForReplay::new(events, workflow_id.to_string());

    let replay_worker = init_core_replay_preloaded(workflow_id, [with_id]);
    // We now recapitulate the actions that the worker took on first execution above, pretending
    // that we always followed the post-reset history.
    // First, we handled the post-reset reapplied update and did not complete the workflow.
    handle_update(FailUpdate::No, CompleteWorkflow::No, &replay_worker, 2).await;
    // Then the timer fires
    let act = replay_worker.poll_workflow_activation().await.unwrap();
    replay_worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    // Then the client sent a second update; we handled it and completed the workflow.
    handle_update(FailUpdate::No, CompleteWorkflow::Yes, &replay_worker, 0).await;

    drain_pollers_and_shutdown(&replay_worker).await;
}

// Start a workflow, send an update, accept the update, complete the update, complete the workflow.
async fn send_and_handle_update(
    workflow_id: &str,
    update_id: &str,
    fail_update: FailUpdate,
    complete_workflow: CompleteWorkflow,
    core: &Worker,
    client: &Client,
) -> String {
    // Complete first task with no commands
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id.clone()))
        .await
        .unwrap();

    let handle = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: workflow_id.to_string(),
        run_id: Some(act.run_id.clone()),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone());

    // Send the update to the server
    let update_task = async {
        handle
            .execute_update(
                UntypedUpdate::new(update_id),
                RawValue::from_value(&"hi", client.data_converter().payload_converter()),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
    };

    // Accept update, complete update and complete workflow
    let processing_task = handle_update(fail_update, complete_workflow, core, 0);
    let (ur, _) = join!(update_task, processing_task);

    match fail_update {
        FailUpdate::Yes => assert!(ur.is_err()),
        FailUpdate::No => assert!(ur.is_ok()),
    }
    act.run_id
}

// Accept and then complete update. If `FailUpdate::Yes` then complete the update as a failure; if
// `CompleteWorkflow::Yes` then additionally complete the workflow. Timers are created when further
// activations are required (i.e., on accepting-but-not-completing the update, and on completing the
// update if not also completing the workflow).
async fn handle_update(
    fail_update: FailUpdate,
    complete_workflow: CompleteWorkflow,
    core: &Worker,
    update_job_index: usize,
) {
    let act = core.poll_workflow_activation().await.unwrap();
    let pid = assert_matches!(
        &act.jobs[update_job_index],
        WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
        } => &d.protocol_instance_id
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: pid.to_string(),
                response: Some(update_response::Response::Accepted(())),
            }
            .into(),
            start_timer_cmd(1, Duration::from_millis(1)),
        ],
    ))
    .await
    .unwrap();

    // Timer fires
    let act = core.poll_workflow_activation().await.unwrap();
    let update_response = match fail_update {
        FailUpdate::Yes => UpdateResponse {
            protocol_instance_id: pid.to_string(),
            response: Some(update_response::Response::Rejected(
                "uh oh spaghettios!".into(),
            )),
        },
        FailUpdate::No => UpdateResponse {
            protocol_instance_id: pid.to_string(),
            response: Some(update_response::Response::Completed("done!".into())),
        },
    };
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![
            update_response.into(),
            match complete_workflow {
                CompleteWorkflow::Yes => CompleteWorkflowExecution { result: None }.into(),
                CompleteWorkflow::No => start_timer_cmd(1, Duration::from_millis(1)),
            },
        ],
    ))
    .await
    .unwrap();
    if matches!(complete_workflow, CompleteWorkflow::Yes) {
        core.handle_eviction().await;
    }
}

#[tokio::test]
async fn update_rejection() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    let handle = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: workflow_id.clone(),
        run_id: Some(res.run_id.clone()),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone());

    // Send the update to the server
    let update_task = async {
        // rejected
        let _ = handle
            .execute_update(
                UntypedUpdate::new(update_id),
                RawValue::from_value(&"hi", client.data_converter().payload_converter()),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
    };

    let processing_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        let pid = assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
            }] => &d.protocol_instance_id
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Rejected("bah humbug".into())),
                }
                .into(),
                start_timer_cmd(1, Duration::from_millis(1)),
            ],
        ))
        .await
        .unwrap();

        let res = core.poll_workflow_activation().await.unwrap();
        core.complete_execution(&res.run_id).await;
    };
    join!(update_task, processing_task);
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(&workflow_id)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();
    let has_update_event = events.iter().any(|e| {
        matches!(
            e.event_type(),
            EventType::WorkflowExecutionUpdateAccepted | EventType::WorkflowExecutionUpdateRejected
        )
    });
    assert!(!has_update_event);
}

#[rstest::rstest]
#[tokio::test]
async fn update_insta_complete(#[values(true, false)] accept_first: bool) {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    let handle = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id,
        run_id: Some(res.run_id.clone()),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone());

    // Send the update to the server
    let (update_task, stop_wait_update) = future::abortable(async {
        handle
            .execute_update(
                UntypedUpdate::new(update_id),
                RawValue::from_value(&"hi", client.data_converter().payload_converter()),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
    });

    let processing_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        let pid = assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
            }] => &d.protocol_instance_id
        );
        let mut cmds = vec![
            UpdateResponse {
                protocol_instance_id: pid.to_string(),
                response: Some(update_response::Response::Completed("done!".into())),
            }
            .into(),
            start_timer_cmd(1, Duration::from_millis(1)),
        ];
        if accept_first {
            cmds.insert(
                0,
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Accepted(())),
                }
                .into(),
            )
        };
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id, cmds,
        ))
        .await
        .unwrap();

        if !accept_first {
            // evicted, because lang must send accept first before completing
            let res = core.poll_workflow_activation().await.unwrap();
            let cause = assert_matches!(
                res.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::RemoveFromCache(c))
                }] => c
            );
            assert_eq!(cause.reason(), EvictionReason::Nondeterminism);
            core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
                .await
                .unwrap();
            // Need to do this b/c currently server doesn't properly terminate update client call
            // if workflow completes
            stop_wait_update.abort();
        }

        let res = core.poll_workflow_activation().await.unwrap();
        core.complete_execution(&res.run_id).await;
    };
    let _ = join!(update_task, processing_task);
}

#[tokio::test]
async fn update_complete_after_accept_without_new_task() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    let handle = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id,
        run_id: Some(res.run_id.clone()),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone());

    // Send the update to the server
    let update_task = async {
        handle
            .execute_update(
                UntypedUpdate::new(update_id),
                RawValue::from_value(&"hi", client.data_converter().payload_converter()),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
    };

    let act_polling = async {
        let task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    };

    let processing_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        let pid = assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
            }] => &d.protocol_instance_id
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Accepted(())),
                }
                .into(),
                ScheduleLocalActivity {
                    seq: 1,
                    activity_id: "1".to_string(),
                    activity_type: "test_act".to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                    ..Default::default()
                }
                .into(),
            ],
        ))
        .await
        .unwrap();

        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Completed("done!".into())),
                }
                .into(),
                CompleteWorkflowExecution { result: None }.into(),
            ],
        ))
        .await
        .unwrap();
    };
    join!(update_task, act_polling, processing_task);
}

#[tokio::test]
async fn update_speculative_wft() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";

    let handle = client.get_workflow_handle::<UntypedWorkflow>(workflow_id);

    // Send update after the timer has fired
    let barr = Barrier::new(2);
    let sender_task = async {
        barr.wait().await;
        // rejected
        let _ = handle
            .execute_update(
                UntypedUpdate::new(update_id),
                RawValue::from_value(&"hi", client.data_converter().payload_converter()),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        barr.wait().await;
        handle
            .signal(
                UntypedSignal::new("hi"),
                RawValue::empty(),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let processing_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
            }]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
            .await
            .unwrap();
        barr.wait().await;

        let res = core.poll_workflow_activation().await.unwrap();
        let pid = assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
            }] => &d.protocol_instance_id
        );
        // Reject the update
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Rejected("nope!".into())),
                }
                .into(),
            ],
        ))
        .await
        .unwrap();
        // Send the signal
        barr.wait().await;
        // Without proper implementation of last-started-wft reset, we'd get stuck here b/c we'd
        // skip over the signal event.
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            }]
        );
        core.complete_execution(&res.run_id).await;
    };
    join!(sender_task, processing_task);
}

#[tokio::test]
async fn update_with_local_acts() {
    let wf_name = "update_with_local_acts";
    let mut starter = CoreWfStarter::new(wf_name);
    // Short task timeout to get activities to heartbeat without taking ages
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct UpdateWithLocalActsWf {
        done: bool,
    }

    #[workflow_methods]
    impl UpdateWithLocalActsWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.wait_condition(|s| s.done).await;
            Ok(())
        }

        #[signal]
        fn done_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {
            self.done = true;
        }

        #[update]
        async fn do_update(
            ctx: &mut WorkflowContext<Self>,
            _: (),
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            ctx.start_local_activity(
                StdActivities::delay,
                Duration::from_secs(3),
                LocalActivityOptions::default(),
            )
            .await?;
            Ok("hi".to_string())
        }
    }

    worker.register_workflow::<UpdateWithLocalActsWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            UpdateWithLocalActsWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    let update = async {
        // do a handful at once
        let updates = (1..=5).map(|_| {
            handle.execute_update(
                UpdateWithLocalActsWf::do_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
        });
        for res in join_all(updates).await {
            assert!(res.unwrap() == "hi");
        }
        handle
            .signal(
                UpdateWithLocalActsWf::done_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn update_rejection_sdk() {
    let wf_name = "update_rejection_sdk";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    #[workflow]
    #[derive(Default)]
    struct UpdateRejectionSdkWf;

    #[workflow_methods]
    impl UpdateRejectionSdkWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.timer(Duration::from_secs(1)).await;
            Ok(())
        }

        #[update_validator(do_update)]
        fn validate_do_update(
            &self,
            _ctx: &WorkflowContextView,
            _: &(),
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Err("ahhhhh noooo".into())
        }

        #[update]
        async fn do_update(
            _ctx: &mut WorkflowContext<Self>,
            _: (),
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok("hi".to_string())
        }
    }

    worker.register_workflow::<UpdateRejectionSdkWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            UpdateRejectionSdkWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    let update = async {
        let res = handle
            .execute_update(
                UpdateRejectionSdkWf::do_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(res.is_err());
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn update_fail_sdk() {
    let wf_name = "update_fail_sdk";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    #[workflow]
    #[derive(Default)]
    struct UpdateFailSdkWf;

    #[workflow_methods]
    impl UpdateFailSdkWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.timer(Duration::from_secs(1)).await;
            Ok(())
        }

        #[update]
        async fn do_update(
            _ctx: &mut WorkflowContext<Self>,
            _: (),
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Err("nooooo".into())
        }
    }

    worker.register_workflow::<UpdateFailSdkWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            UpdateFailSdkWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    let update = async {
        let res = handle
            .execute_update(
                UpdateFailSdkWf::do_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(res.is_err());
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn update_timer_sequence() {
    let wf_name = "update_timer_sequence";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    #[workflow]
    #[derive(Default)]
    struct UpdateTimerSequenceWf {
        done: bool,
    }

    #[workflow_methods]
    impl UpdateTimerSequenceWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.wait_condition(|s| s.done).await;
            Ok(())
        }

        #[update]
        async fn do_update(
            ctx: &mut WorkflowContext<Self>,
            _: (),
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            ctx.timer(Duration::from_millis(1)).await;
            ctx.timer(Duration::from_millis(1)).await;
            ctx.state_mut(|s| s.done = true);
            Ok("done".to_string())
        }
    }

    worker.register_workflow::<UpdateTimerSequenceWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            UpdateTimerSequenceWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    let update = async {
        let res = handle
            .execute_update(
                UpdateTimerSequenceWf::do_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(res.unwrap() == "done");
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn task_failure_during_validation() {
    let wf_name = "task_failure_during_validation";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    #[workflow]
    #[derive(Default)]
    struct TaskFailureDuringValidationWf;

    #[workflow_methods]
    impl TaskFailureDuringValidationWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.timer(Duration::from_secs(1)).await;
            Ok(())
        }

        #[update_validator(do_update)]
        fn validate_do_update(
            &self,
            _ctx: &WorkflowContextView,
            _: &(),
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            static FAILCT: AtomicUsize = AtomicUsize::new(0);
            if FAILCT.fetch_add(1, Ordering::Relaxed) < 2 {
                panic!("ahhhhhh");
            }
            Ok(())
        }

        #[update]
        async fn do_update(
            _ctx: &mut WorkflowContext<Self>,
            _: (),
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok("done".to_string())
        }
    }

    worker
        .register_workflow::<TaskFailureDuringValidationWf>()
        .unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            TaskFailureDuringValidationWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    let update = async {
        let res = handle
            .execute_update(
                TaskFailureDuringValidationWf::do_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(res.unwrap() == "done");
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
    // Verify we did not spam task failures. There should only be one.
    let history = starter.get_history().await;
    assert_eq!(
        history
            .events
            .iter()
            .filter(|he| he.event_type() == EventType::WorkflowTaskFailed)
            .count(),
        1
    );
}

#[tokio::test]
async fn task_failure_after_update() {
    let wf_name = "task_failure_after_update";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    #[workflow]
    #[derive(Default)]
    struct TaskFailureAfterUpdateWf;

    #[workflow_methods]
    impl TaskFailureAfterUpdateWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            static FAILCT: AtomicUsize = AtomicUsize::new(0);
            ctx.timer(Duration::from_millis(1)).await;
            if FAILCT.fetch_add(1, Ordering::Relaxed) < 1 {
                panic!("ahhhhhh");
            }
            Ok(())
        }

        #[update]
        async fn do_update(
            _ctx: &mut WorkflowContext<Self>,
            _: (),
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok("done".to_string())
        }
    }

    worker
        .register_workflow::<TaskFailureAfterUpdateWf>()
        .unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            TaskFailureAfterUpdateWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    let update = async {
        let res = handle
            .execute_update(
                TaskFailureAfterUpdateWf::do_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(res.unwrap() == "done");
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

static BARR: LazyLock<Barrier> = LazyLock::new(|| Barrier::new(2));
static ACT_RAN: AtomicBool = AtomicBool::new(false);
#[tokio::test]
async fn worker_restarted_in_middle_of_update() {
    let wf_name = "worker_restarted_in_middle_of_update";
    let mut starter = CoreWfStarter::new(wf_name);

    struct BlockingActivities;
    #[activities]
    impl BlockingActivities {
        #[activity]
        async fn blocks(_ctx: ActivityContext, echo_me: String) -> Result<String, ActivityError> {
            BARR.wait().await;
            if !ACT_RAN.fetch_or(true, Ordering::Relaxed) {
                // On first run fail the task so we'll get retried on the new worker
                return Err(anyhow!("Fail first time").into());
            }
            Ok(echo_me)
        }
    }

    starter.sdk_config.register_activities(BlockingActivities);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;

    #[workflow]
    #[derive(Default)]
    struct WorkerRestartedInMiddleOfUpdateWf {
        done: bool,
    }

    #[workflow_methods]
    impl WorkerRestartedInMiddleOfUpdateWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.wait_condition(|s| s.done).await;
            Ok(())
        }

        #[signal]
        fn done_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {
            self.done = true;
        }

        #[update]
        async fn do_update(
            ctx: &mut WorkflowContext<Self>,
            _: (),
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            ctx.start_activity(
                BlockingActivities::blocks,
                "hi!".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(2)),
            )
            .await?;
            Ok(())
        }
    }

    worker
        .register_workflow::<WorkerRestartedInMiddleOfUpdateWf>()
        .unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            WorkerRestartedInMiddleOfUpdateWf::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();

    let wf_id = task_queue;
    let update = async {
        let res = handle
            .execute_update(
                WorkerRestartedInMiddleOfUpdateWf::do_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(res.is_ok());
        handle
            .signal(
                WorkerRestartedInMiddleOfUpdateWf::done_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };
    let core_worker = starter.get_worker().await;
    let mut second_starter = starter.clone_no_worker();
    let stopper = async {
        // Wait for the activity to start
        BARR.wait().await;
        core_worker.initiate_shutdown();
        // Allow it to start again, the second time
        BARR.wait().await;
        // Poke the workflow off the sticky queue to get it to complete faster than WFT timeout
        let _ = WorkflowService::reset_sticky_task_queue(
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
        .await;
    };
    let run = async {
        // This run attempt will get shut down
        worker.inner_mut().run().await.unwrap();
        // Start up a new worker
        let new_worker = second_starter.get_worker().await;
        // Replace with new core and run again
        worker.inner_mut().with_new_core_worker(new_worker);
        worker.run_until_done().await.unwrap();
    };
    join!(update, run, stopper);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn update_after_empty_wft() {
    let wf_name = "update_after_empty_wft";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    static ACT_STARTED: AtomicBool = AtomicBool::new(false);

    #[workflow]
    #[derive(Default)]
    struct UpdateAfterEmptyWftWf {
        signal_received: bool,
    }

    #[workflow_methods]
    impl UpdateAfterEmptyWftWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let sig_handle = async {
                ctx.wait_condition(|s| s.signal_received).await;
                ACT_STARTED.store(true, Ordering::Release);
                let _ = ctx
                    .start_activity(
                        StdActivities::echo,
                        "hi!".to_string(),
                        ActivityOptions::start_to_close_timeout(Duration::from_secs(2)),
                    )
                    .await;
                ACT_STARTED.store(false, Ordering::Release);
            };
            join!(sig_handle, async {
                ctx.timer(Duration::from_secs(2)).await;
            });
            Ok(())
        }

        #[signal]
        fn signal_handler(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {
            self.signal_received = true;
        }

        #[update]
        async fn do_update(ctx: &mut WorkflowContext<Self>, _: ()) {
            if ACT_STARTED.load(Ordering::Acquire) {
                return;
            }
            let _ = ctx
                .start_activity(
                    StdActivities::echo,
                    "hi!".to_string(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(2)),
                )
                .await;
        }
    }

    worker.register_workflow::<UpdateAfterEmptyWftWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            UpdateAfterEmptyWftWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();

    let update = async {
        handle
            .signal(
                UpdateAfterEmptyWftWf::signal_handler,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        handle
            .execute_update(
                UpdateAfterEmptyWftWf::do_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, runner);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn update_lost_on_activity_mismatch() {
    let wf_name = "update_lost_on_activity_mismatch";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct UpdateLostOnActivityMismatchWf {
        can_run: usize,
    }

    #[workflow_methods]
    impl UpdateLostOnActivityMismatchWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.state_mut(|s| s.can_run = 1);
            for _ in 1..=3 {
                ctx.wait_condition(|s| s.can_run > 0).await;
                let _ = ctx
                    .start_activity(
                        StdActivities::echo,
                        "hi!".to_string(),
                        ActivityOptions::start_to_close_timeout(Duration::from_secs(2)),
                    )
                    .await;
                ctx.state_mut(|s| s.can_run -= 1);
            }
            Ok(())
        }

        #[update]
        fn do_update(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {
            self.can_run += 1;
        }
    }

    worker
        .register_workflow::<UpdateLostOnActivityMismatchWf>()
        .unwrap();
    let core_worker = worker.core_worker();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            UpdateLostOnActivityMismatchWf::run,
            (),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();

    let update = async {
        // Need time to get to condition
        tokio::time::sleep(Duration::from_millis(200)).await;
        // Evict wf
        core_worker.request_workflow_eviction(handle.run_id().unwrap());
        for _ in 1..=2 {
            handle
                .execute_update(
                    UpdateLostOnActivityMismatchWf::do_update,
                    (),
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await
                .unwrap();
        }
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, runner);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}
