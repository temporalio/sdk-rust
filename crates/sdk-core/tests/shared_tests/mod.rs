//! Shared tests that are meant to be run against both local dev server and cloud

use crate::common::{CoreWfStarter, activity_functions::StdActivities};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};
use temporalio_client::{
    WorkflowFetchHistoryOptions, WorkflowStartOptions, WorkflowTerminateOptions,
};
use temporalio_common::{
    ActivityError, UntypedWorkflow,
    protos::{
        coresdk::workflow_commands::ActivityCancellationType,
        temporal::api::{
            common::v1::RetryPolicy,
            enums::v1::{
                EventType,
                WorkflowTaskFailedCause::{self, GrpcMessageTooLarge},
            },
            history::v1::history_event::{
                self,
                Attributes::{
                    WorkflowExecutionTerminatedEventAttributes, WorkflowTaskFailedEventAttributes,
                },
            },
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, WorkflowContext, WorkflowResult, WorkflowTermination,
    activities::ActivityContext,
};
use tracing::warn;

pub(crate) mod priority;

#[workflow]
struct OversizeGrpcMessageWf {
    run_flag: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl OversizeGrpcMessageWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<Vec<u8>> {
        if ctx.state(|wf| wf.run_flag.load(Relaxed)) {
            Ok(vec![])
        } else {
            ctx.state(|wf| wf.run_flag.store(true, Relaxed));
            let result: Vec<u8> = vec![0; 5000000];
            Ok(result)
        }
    }
}

pub(crate) async fn grpc_message_too_large() {
    let run_flag = Arc::new(AtomicBool::new(false));
    let run_flag_clone = run_flag.clone();

    let wf_name = "oversize_grpc_message";
    let mut starter = CoreWfStarter::new_cloud_or_local(wf_name, "")
        .await
        .unwrap();
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter
        .sdk_config
        .register_workflow_with_factory(move || OversizeGrpcMessageWf {
            run_flag: run_flag_clone.clone(),
        })
        .unwrap();

    let mut sdk = starter.worker().await;
    sdk.submit_workflow(
        OversizeGrpcMessageWf::run,
        (),
        starter.workflow_options.clone(),
    )
    .await
    .unwrap();
    sdk.run_until_done().await.unwrap();

    let events = starter.get_history().await.events;
    // Depending on the version of server, it may terminate the workflow, or simply be a task
    // failure
    assert!(
        events.iter().any(is_oversize_grpc_event),
        "Expected workflow task failure or termination b/c grpc message too large: {events:?}",
    );
}

pub(crate) fn is_oversize_grpc_event(
    e: &temporalio_common::protos::temporal::api::history::v1::HistoryEvent,
) -> bool {
    // Task failure
    e.event_type == EventType::WorkflowTaskFailed as i32
        && if let WorkflowTaskFailedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
            attr.cause == GrpcMessageTooLarge as i32
                && attr.failure.as_ref().unwrap().message == "GRPC Message too large"
        } else {
            false
        }
    // Workflow terminated
    ||
    e.event_type == EventType::WorkflowExecutionTerminated as i32
        && if let WorkflowExecutionTerminatedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
            attr.reason == "GrpcMessageTooLarge"
        } else {
            false
        }
}

#[workflow]
#[derive(Default)]
struct ShutdownTimerActivityLoopWf;

#[workflow_methods]
impl ShutdownTimerActivityLoopWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        loop {
            ctx.timer(Duration::from_millis(10)).await;
            ctx.start_activity(
                StdActivities::no_op,
                (),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await
            .map_err(|e| WorkflowTermination::from(anyhow::Error::from(e)))?;
        }
    }
}

/// Starts 10 workflows that each run a tight timer+activity loop, then shuts down the worker
/// and verifies:
///   1. Shutdown completes rapidly (< 5s)
///   2. No workflow task failures or timeouts appear in any workflow's history
pub(crate) async fn shutdown_during_active_timer_activity_workflows() {
    let wf_name = "shutdown_during_active_timer_activity_workflows";
    let num_workflows = 10;

    let mut starter =
        if let Some(wfs) = CoreWfStarter::new_cloud_or_local(wf_name, ">=1.6.3-serverless").await {
            wfs
        } else {
            return;
        };
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<ShutdownTimerActivityLoopWf>()
        .unwrap();

    let core = worker.core_worker();
    core.validate().await.unwrap();
    assert!(
        core.get_namespace_capabilities().graceful_poll_shutdown(),
        "Server must support graceful poll shutdown for this test"
    );

    let task_queue = starter.get_task_queue().to_owned();
    let mut wf_ids = Vec::with_capacity(num_workflows);
    for i in 0..num_workflows {
        let wf_id = format!("{task_queue}-{i}");
        worker
            .submit_workflow(
                ShutdownTimerActivityLoopWf::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), wf_id.clone()).build(),
            )
            .await
            .unwrap();
        wf_ids.push(wf_id);
    }
    // Don't wait for workflow completion — these loop forever
    worker.fetch_results = false;

    let shutdown_handle = worker.inner_mut().shutdown_handle();
    let run_fut = async { worker.run_until_done().await.unwrap() };

    let shutdown_fut = async {
        // Let workflows run a few iterations
        tokio::time::sleep(Duration::from_secs(2)).await;
        shutdown_handle();
    };

    let shutdown_start = std::time::Instant::now();
    tokio::join!(run_fut, shutdown_fut);
    let shutdown_elapsed = shutdown_start.elapsed();

    assert!(
        shutdown_elapsed < Duration::from_secs(5),
        "Worker shutdown took {shutdown_elapsed:?}, expected < 5s"
    );

    let client = starter.get_client().await;
    for wf_id in &wf_ids {
        client
            .get_workflow_handle::<UntypedWorkflow>(wf_id)
            .terminate(WorkflowTerminateOptions::default())
            .await
            .unwrap();

        let history = client
            .get_workflow_handle::<UntypedWorkflow>(wf_id)
            .fetch_history(WorkflowFetchHistoryOptions::default())
            .await
            .unwrap();
        let bad_events: Vec<_> = history
            .events()
            .iter()
            .filter(|e| match &e.attributes {
                Some(history_event::Attributes::WorkflowTaskFailedEventAttributes(f))
                    if f.cause() != WorkflowTaskFailedCause::ForceCloseCommand =>
                {
                    true
                }
                Some(history_event::Attributes::WorkflowTaskTimedOutEventAttributes(_)) => true,
                _ => false,
            })
            .collect();
        assert!(
            bad_events.is_empty(),
            "Workflow {wf_id} had unexpected WFT failures/timeouts: {bad_events:?}"
        );
    }
}

/// Verifies that activity cancellation is delivered via the nexus worker command channel
/// even when the activity does not heartbeat.
pub(crate) async fn activity_cancel_delivered_without_heartbeat() {
    let wf_name = "activity_cancel_delivered_without_heartbeat";
    let mut starter = CoreWfStarter::new_cloud_or_local(wf_name, "")
        .await
        .unwrap();

    struct WaitForCancelActivities {}
    #[activities]
    impl WaitForCancelActivities {
        #[activity]
        async fn wait_for_cancel(
            self: Arc<Self>,
            ctx: ActivityContext,
            _: String,
        ) -> Result<String, ActivityError> {
            ctx.cancelled().await;
            Ok("done".to_string())
        }
    }

    starter
        .sdk_config
        .register_activities(WaitForCancelActivities {});
    let mut worker = starter.worker().await;
    if !worker
        .core_worker()
        .get_namespace_capabilities()
        .worker_commands()
    {
        warn!("Skipping test: worker_commands not supported in this namespace");
        return;
    }

    #[workflow]
    #[derive(Default)]
    struct CancelWithoutHeartbeatWorkflow;

    #[workflow_methods]
    impl CancelWithoutHeartbeatWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let act_fut = ctx.start_activity(
                WaitForCancelActivities::wait_for_cancel,
                "hi".to_string(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                    .retry_policy(RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    })
                    .cancellation_type(ActivityCancellationType::WaitCancellationCompleted)
                    .build(),
            );
            // Timer needed to avoid cancel-before-sent
            ctx.timer(Duration::from_millis(10)).await;
            act_fut.cancel();
            let _ = act_fut.await;
            Ok(())
        }
    }

    worker
        .register_workflow::<CancelWithoutHeartbeatWorkflow>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            CancelWithoutHeartbeatWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned())
                .run_timeout(Duration::from_secs(10))
                .build(),
        )
        .await
        .unwrap();
    // Fails with workflow timeout if cancel doesn't work
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}
