use crate::common::{ActivationAssertionsInterceptor, CoreWfStarter};
use std::{sync::Arc, time::Duration};
use temporalio_client::{
    UntypedWorkflow, WorkflowCancelOptions, WorkflowDescribeOptions, WorkflowStartOptions,
    errors::WorkflowGetResultError,
};
use temporalio_common::protos::{
    coresdk::workflow_activation::{WorkflowActivationJob, workflow_activation_job},
    temporal::api::enums::v1::{CommandType, WorkflowExecutionStatus},
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityCancellationType, ActivityOptions, ChildWorkflowCancellationType, ChildWorkflowOptions,
    LocalActivityOptions, SyncWorkflowContext, TimerOptions, TimerResult,
    WorkflowCancellationToken, WorkflowContext, WorkflowResult, WorkflowTermination,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, canned_histories},
    test_help::MockPollCfg,
};
use tokio::sync::Semaphore;

#[workflow]
#[derive(Default)]
struct CancelledWf;

#[workflow_methods]
impl CancelledWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let err = ctx
            .wait_condition(|_| false)
            .await
            .expect_err("condition wait should inherit workflow cancellation");
        assert_eq!(err.reason(), Some("Dieee"));
        Err(err.into())
    }
}

#[tokio::test]
async fn cancel_during_timer() {
    let wf_name = "cancel_during_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<CancelledWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    let client = starter.get_core_client().await;
    let task_queue = starter.get_task_queue().to_owned();
    let wf_id = task_queue.clone();
    let wf_handle = worker
        .submit_workflow(
            CancelledWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_id.clone()).build(),
        )
        .await
        .unwrap();

    let canceller = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Cancel the workflow externally
        wf_handle
            .cancel(WorkflowCancelOptions::builder().reason("Dieee").build())
            .await
            .unwrap();
    };

    let (_, res) = tokio::join!(canceller, worker.run_until_done());
    res.unwrap();
    let desc = client
        .get_workflow_handle::<UntypedWorkflow>(wf_id)
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap();

    assert_eq!(
        desc.raw_description.workflow_execution_info.unwrap().status,
        WorkflowExecutionStatus::Canceled as i32
    );
}

#[workflow]
#[derive(Default)]
struct ShieldedCancellationWf;

#[workflow_methods]
impl ShieldedCancellationWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let reason = ctx.cancelled().await;
        assert_eq!(reason.as_deref(), Some("shield me"));
        let shield = WorkflowCancellationToken::new();
        let timer_result = ctx
            .timer(
                TimerOptions::builder(Duration::from_millis(10))
                    .cancellation_token(shield.clone())
                    .build(),
            )
            .await;

        assert_eq!(timer_result, TimerResult::Fired);
        assert!(!shield.is_cancelled());
        Ok(format!("shielded after {}", reason.unwrap()))
    }
}

#[tokio::test]
async fn detached_token_shields_work_after_workflow_cancellation() {
    let wf_name = "detached_token_shields_work_after_workflow_cancellation";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<ShieldedCancellationWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    let task_queue = starter.get_task_queue().to_owned();
    let wf_handle = worker
        .submit_workflow(
            ShieldedCancellationWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();

    let (cancel_result, worker_result) = tokio::join!(
        wf_handle.cancel(WorkflowCancelOptions::builder().reason("shield me").build()),
        worker.run_until_done()
    );
    cancel_result.unwrap();
    worker_result.unwrap();

    assert_eq!(
        wf_handle.get_result(Default::default()).await.unwrap(),
        "shielded after shield me"
    );
}

struct CancellationPropagationActivities {
    started: Arc<Semaphore>,
}

#[activities]
impl CancellationPropagationActivities {
    #[activity]
    async fn wait_for_cancellation(
        self: Arc<Self>,
        ctx: ActivityContext,
        _: (),
    ) -> Result<(), ActivityError> {
        self.started.add_permits(1);
        let mut heartbeat = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = ctx.cancelled() => return Err(ActivityError::cancelled()),
                _ = heartbeat.tick() => ctx.record_heartbeat(()).await?,
            }
        }
    }
}

#[workflow]
struct CancellationPropagationChild {
    started: Arc<Semaphore>,
}

#[workflow_methods(factory_only)]
impl CancellationPropagationChild {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.state(|wf| wf.started.add_permits(1));
        ctx.cancelled().await;
        Err(WorkflowTermination::Cancelled)
    }

    #[signal]
    fn noop(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {}
}

#[workflow]
#[derive(Default)]
struct CancellationPropagationParent;

#[workflow_methods]
impl CancellationPropagationParent {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let ctx = &*ctx;
        let timer = ctx.timer(Duration::from_secs(30));
        let activity = ctx.execute_activity(
            CancellationPropagationActivities::wait_for_cancellation,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                .heartbeat_timeout(Duration::from_secs(1))
                .cancellation_type(ActivityCancellationType::WaitCancellationCompleted)
                .build(),
        );
        let local_activity = ctx.execute_local_activity(
            CancellationPropagationActivities::wait_for_cancellation,
            (),
            LocalActivityOptions::builder()
                .cancel_type(ActivityCancellationType::WaitCancellationCompleted)
                .build(),
        );
        let child = ctx.start_child_workflow(
            CancellationPropagationChild::run,
            (),
            ChildWorkflowOptions::builder()
                .cancel_type(ChildWorkflowCancellationType::WaitCancellationCompleted)
                .build(),
        );
        let condition = ctx.wait_condition(|_| false);
        let child_and_signals = async {
            let started_child = child.await.expect("child should start");
            ctx.cancelled().await;

            let child_signal_error = started_child
                .signal(CancellationPropagationChild::noop, (), Default::default())
                .await
                .expect_err("child signal should inherit workflow cancellation");
            assert!(
                child_signal_error
                    .reason()
                    .is_some_and(|reason| reason.as_cancelled().is_some())
            );

            let external_signal_error = ctx
                .external_workflow("cancellation-propagation-target", None)
                .signal(CancellationPropagationChild::noop, (), Default::default())
                .await
                .expect_err("external signal should inherit workflow cancellation");
            assert!(
                external_signal_error
                    .reason()
                    .is_some_and(|reason| reason.as_cancelled().is_some())
            );

            started_child.result().await
        };

        let (timer_result, activity_result, local_activity_result, child_result, condition_result) = temporalio_sdk::workflows::join!(
            timer,
            activity,
            local_activity,
            child_and_signals,
            condition
        );

        assert_eq!(timer_result, TimerResult::Cancelled);
        let activity_error = activity_result.unwrap_err();
        assert!(activity_error.as_cancelled().is_some());
        assert!(local_activity_result.unwrap_err().as_cancelled().is_some());
        assert!(child_result.unwrap_err().as_cancelled().is_some());
        assert_eq!(condition_result.unwrap_err().reason(), Some("propagate"));
        Err(activity_error.into())
    }
}

#[tokio::test]
async fn workflow_cancellation_propagates_to_operations() {
    let wf_name = "workflow_cancellation_propagates_to_operations";
    let mut starter = CoreWfStarter::new(wf_name);
    let started = Arc::new(Semaphore::new(0));
    starter
        .sdk_config
        .register_activities(CancellationPropagationActivities {
            started: started.clone(),
        });
    starter
        .sdk_config
        .register_workflow::<CancellationPropagationParent>()
        .unwrap();
    starter
        .sdk_config
        .register_workflow_with_factory({
            let started = started.clone();
            move || CancellationPropagationChild {
                started: started.clone(),
            }
        })
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let wf_handle = worker
        .submit_workflow(
            CancellationPropagationParent::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();

    let canceller = async {
        let _started = started.acquire_many(3).await.unwrap();
        wf_handle
            .cancel(WorkflowCancelOptions::builder().reason("propagate").build())
            .await
            .unwrap();
    };
    let (_, worker_result) = tokio::join!(canceller, worker.run_until_done());
    worker_result.unwrap();

    assert_matches!(
        wf_handle.get_result(Default::default()).await,
        Err(WorkflowGetResultError::Cancelled { .. })
    );
}

#[workflow]
#[derive(Default)]
struct WfWithTimer;

#[workflow_methods]
impl WfWithTimer {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(500)).await;
        Err(WorkflowTermination::Cancelled)
    }
}

#[tokio::test]
async fn wf_completing_with_cancelled() {
    let t = canned_histories::timer_wf_cancel_req_cancelled("1");

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
            [
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::CancelWorkflow(_)),
                }
            ]
        );
    });

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(wft.commands[0].command_type(), CommandType::StartTimer);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::CancelWorkflowExecution
                );
            });
    });

    let mut worker =
        crate::common::build_fake_sdk_intercepted_with_options(mock_cfg, aai, |options| {
            options.register_workflow::<WfWithTimer>().unwrap();
        });
    worker.run().await.unwrap();
}
