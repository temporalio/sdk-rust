use crate::common::{CoreWfStarter, WorkflowHandleExt, build_fake_sdk, mock_sdk, mock_sdk_cfg};
use anyhow::anyhow;
use assert_matches::assert_matches;
use std::{sync::Arc, time::Duration};
use temporalio_client::{WorkflowCancelOptions, WorkflowStartOptions};
use temporalio_common::{
    UntypedWorkflow,
    data_converters::RawValue,
    error::IncomingError,
    protos::{
        coresdk::{
            AsJsonPayloadExt,
            child_workflow::{
                ChildWorkflowCancellationType, ParentClosePolicy,
                StartChildWorkflowExecutionFailedCause,
            },
            workflow_activation::{WorkflowActivationJob, workflow_activation_job},
            workflow_commands::{
                CancelChildWorkflowExecution, CancelWorkflowExecution, CompleteWorkflowExecution,
                StartChildWorkflowExecution,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
            history::v1::{
                StartChildWorkflowExecutionFailedEventAttributes,
                StartChildWorkflowExecutionInitiatedEventAttributes, history_event,
            },
            sdk::v1::UserMetadata,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, ChildWorkflowExecutionError, ChildWorkflowOptions, ChildWorkflowStartError,
    SyncWorkflowContext, WorkflowContext, WorkflowResult, WorkflowSignalError, WorkflowTermination,
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, canned_histories},
    test_help::{MockPollCfg, ResponseType, mock_worker, mock_worker_client, single_hist_mock_sg},
};
use tokio::{
    join,
    sync::{Barrier, Notify},
};

static PARENT_WF_TYPE: &str = "parent_wf";
static CHILD_WF_TYPE: &str = "child_wf";
const SIGNAME: &str = "SIGNAME";

#[workflow]
#[derive(Default)]
struct ChildWf;

#[workflow_methods]
impl ChildWf {
    #[run(name = "child_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        assert_eq!(
            ctx.workflow_initial_info()
                .parent_workflow_info
                .as_ref()
                .unwrap()
                .workflow_id,
            ctx.workflow_initial_info()
                .root_workflow
                .as_ref()
                .unwrap()
                .workflow_id
        );
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct HappyParent;

#[workflow_methods]
impl HappyParent {
    #[run(name = "parent_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                ChildWf::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: "child-1".to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("Child should start OK");
        started.result().await?;
        Ok(())
    }
}

#[tokio::test]
async fn child_workflow_happy_path() {
    let mut starter = CoreWfStarter::new("child-workflows");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker.register_workflow::<HappyParent>().unwrap();
    worker.register_workflow::<ChildWf>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_wf(
            PARENT_WF_TYPE.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, "parent".to_string()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
struct AbandonedChildBugReproParent {
    barr: Arc<Barrier>,
}

#[workflow_methods(factory_only)]
impl AbandonedChildBugReproParent {
    #[run(name = "parent_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                AbandonedChildBugReproChild::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: "abandoned-child".to_owned(),
                    parent_close_policy: ParentClosePolicy::Abandon,
                    cancel_type: ChildWorkflowCancellationType::Abandon,
                    ..Default::default()
                },
            )
            .await
            .expect("Child should start OK");
        let barr = ctx.state(|wf| wf.barr.clone());
        barr.wait().await;
        ctx.cancelled().await;
        started.cancel("Die reason!".to_string());
        ctx.timer(Duration::from_secs(1)).await;
        let _ = started.result().await;
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct AbandonedChildBugReproChild;

#[workflow_methods]
impl AbandonedChildBugReproChild {
    #[run(name = "child_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.cancelled().await;
        Err(WorkflowTermination::Cancelled)
    }
}

#[tokio::test]
async fn abandoned_child_bug_repro() {
    let mut starter = CoreWfStarter::new("child-workflow-abandon-bug");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    let barr = Arc::new(Barrier::new(2));
    let barr_clone = barr.clone();
    worker
        .register_workflow_with_factory(move || AbandonedChildBugReproParent {
            barr: barr_clone.clone(),
        })
        .unwrap();
    worker
        .register_workflow::<AbandonedChildBugReproChild>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            AbandonedChildBugReproParent::run,
            (),
            WorkflowStartOptions::new(task_queue, "parent-abandoner").build(),
        )
        .await
        .unwrap();
    let client = starter.get_client().await;
    let canceller = async {
        barr.wait().await;
        let parent_handle = client.get_workflow_handle::<UntypedWorkflow>("parent-abandoner");
        parent_handle
            .cancel(WorkflowCancelOptions::builder().reason("die").build())
            .await
            .unwrap();
        let child_handle = client.get_workflow_handle::<UntypedWorkflow>("abandoned-child");
        child_handle
            .cancel(WorkflowCancelOptions::builder().reason("die").build())
            .await
            .unwrap();
    };
    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(canceller, runner);
}

#[workflow]
struct AbandonedChildResolvesPostCancelParent {
    barr: Arc<Barrier>,
}

#[workflow_methods(factory_only)]
impl AbandonedChildResolvesPostCancelParent {
    #[run(name = "parent_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                AbandonedChildResolvesPostCancelChild::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: "abandoned-child-resolve-post-cancel".to_owned(),
                    parent_close_policy: ParentClosePolicy::Abandon,
                    cancel_type: ChildWorkflowCancellationType::Abandon,
                    ..Default::default()
                },
            )
            .await
            .expect("Child should start OK");
        let barr = ctx.state(|wf| wf.barr.clone());
        barr.wait().await;
        ctx.cancelled().await;
        started.cancel("Die reason".to_string());
        ctx.timer(Duration::from_secs(1)).await;
        let _ = started.result().await;
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct AbandonedChildResolvesPostCancelChild;

#[workflow_methods]
impl AbandonedChildResolvesPostCancelChild {
    #[run(name = "child_wf")]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        Ok("I'm done".to_string())
    }
}

#[tokio::test]
async fn abandoned_child_resolves_post_cancel() {
    let mut starter = CoreWfStarter::new("child-workflow-resolves-post-cancel");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    let barr = Arc::new(Barrier::new(2));
    let barr_clone = barr.clone();
    worker
        .register_workflow_with_factory(move || AbandonedChildResolvesPostCancelParent {
            barr: barr_clone.clone(),
        })
        .unwrap();
    worker
        .register_workflow::<AbandonedChildResolvesPostCancelChild>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            AbandonedChildResolvesPostCancelParent::run,
            (),
            WorkflowStartOptions::new(task_queue, "parent-abandoner-resolving").build(),
        )
        .await
        .unwrap();
    let client = starter.get_client().await;
    let canceller = async {
        barr.wait().await;
        handle
            .cancel(WorkflowCancelOptions::builder().reason("die").build())
            .await
            .unwrap();
    };
    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(canceller, runner);

    // Verify no WFT failures on the child workflow. A failure here indicates
    // the child couldn't deserialize its input (e.g., sending a payload when none expected).
    let child_handle =
        client.get_workflow_handle::<UntypedWorkflow>("abandoned-child-resolve-post-cancel");
    let history = child_handle
        .fetch_history(Default::default())
        .await
        .unwrap();
    let wft_failures: Vec<_> = history
        .events()
        .iter()
        .filter(|e| {
            matches!(
                &e.attributes,
                Some(history_event::Attributes::WorkflowTaskFailedEventAttributes(_))
            )
        })
        .collect();
    assert!(
        wft_failures.is_empty(),
        "Expected no WorkflowTaskFailed events on child workflow, but found: {wft_failures:?}",
    );
}

#[workflow]
#[derive(Default)]
struct CancelledChildGetsReasonParent;

#[workflow_methods]
impl CancelledChildGetsReasonParent {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                CancelledChildGetsReasonChild::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: format!("{}-child", ctx.task_queue()),
                    cancel_type: ChildWorkflowCancellationType::WaitCancellationRequested,
                    ..Default::default()
                },
            )
            .await
            .expect("Child should start OK");
        started.cancel("Die reason".to_string());
        let r: String = started.result().await.expect("Child should complete OK");
        assert_eq!(r, "Die reason");
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct CancelledChildGetsReasonChild;

#[workflow_methods]
impl CancelledChildGetsReasonChild {
    #[run(name = "child_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let r = ctx.cancelled().await;
        Ok(r)
    }
}

#[tokio::test]
async fn cancelled_child_gets_reason() {
    let wf_name = "cancelled-child-gets-reason";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker
        .register_workflow::<CancelledChildGetsReasonParent>()
        .unwrap();
    worker
        .register_workflow::<CancelledChildGetsReasonChild>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            CancelledChildGetsReasonParent::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
struct SignalChildWorkflowWf {
    serial: bool,
}

#[workflow_methods(factory_only)]
impl SignalChildWorkflowWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let start_res = ctx
            .start_child_workflow(
                UnusedChildWf::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: "child-id-1".to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect("Child should get started");
        let serial = ctx.state(|wf| wf.serial);
        if serial {
            start_res
                .signal(UnusedChildWf::signal, "Hi!".to_string())
                .await?;
            start_res.result().await?;
        } else {
            let sigfut = start_res.signal(UnusedChildWf::signal, "Hi!".to_string());
            let resfut = start_res.result();
            let (sigres, res) = join!(sigfut, resfut);
            sigres?;
            res?;
        };
        Ok(())
    }
}

/// Workflow never expected to be used, just exists to match child workflow type in canned history
#[derive(Debug, Default)]
#[workflow]
struct UnusedChildWf;

#[workflow_methods]
impl UnusedChildWf {
    #[run(name = "child")]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        panic!("Should never get run")
    }

    #[signal(name = SIGNAME)]
    fn signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: String) {
        panic!("Should never get called")
    }
}

#[rstest::rstest]
#[case::signal_then_result(true)]
#[case::signal_and_result_concurrent(false)]
#[tokio::test]
async fn signal_child_workflow(#[case] serial: bool) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_child_workflow_signaled("child-id-1", SIGNAME);
    let mock = mock_worker_client();
    let mut worker = mock_sdk(MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::AllHistory],
        mock,
    ));

    worker
        .register_workflow_with_factory(move || SignalChildWorkflowWf { serial })
        .unwrap();
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

#[workflow]
#[derive(Default)]
struct ParentCancelsChildWf;

#[workflow_methods]
impl ParentCancelsChildWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let start_res = ctx
            .start_child_workflow(
                UntypedWorkflow::new("child"),
                RawValue::new(vec![]),
                ChildWorkflowOptions {
                    workflow_id: "child-id-1".to_string(),
                    cancel_type: ChildWorkflowCancellationType::WaitCancellationCompleted,
                    ..Default::default()
                },
            )
            .await
            .expect("Child should get started");
        start_res.cancel("cancel reason".to_string());
        let err = start_res
            .result()
            .await
            .expect_err("child should be cancelled");
        let ChildWorkflowExecutionError::Failed(failure) = err else {
            panic!("started child cancellation should stay wrapper-shaped");
        };
        let Some(cancelled) = failure.as_cancelled() else {
            panic!("child failure should retain cancelled reason");
        };
        assert!(cancelled.raw_details().is_none());
        Ok(())
    }
}

#[tokio::test]
async fn cancel_child_workflow() {
    let t = canned_histories::single_child_workflow_cancelled("child-id-1");
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    worker.register_workflow::<ParentCancelsChildWf>().unwrap();
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct RuntimeParentCancelsChildWf;

#[workflow_methods]
impl RuntimeParentCancelsChildWf {
    #[run(name = "runtime_parent_cancels_child")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                GrandchildCancelled::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: format!("{}-runtime-cancelled-child", ctx.task_queue()),
                    cancel_type: ChildWorkflowCancellationType::WaitCancellationCompleted,
                    ..Default::default()
                },
            )
            .await
            .expect("child should start");
        started.cancel("child cancel".to_string());
        let err = started
            .result()
            .await
            .expect_err("child should be cancelled");
        let ChildWorkflowExecutionError::Failed(failure) = err else {
            panic!("started child cancellation should stay wrapper-shaped");
        };
        let Some(cancelled) = failure.as_cancelled() else {
            panic!("child failure should retain cancelled reason");
        };
        assert!(cancelled.raw_details().is_none());
        assert!(cancelled.cause().is_none());
        Ok(())
    }
}

#[tokio::test]
async fn cancel_child_workflow_runtime_shape() {
    let mut starter = CoreWfStarter::new("cancel-child-workflow-runtime-shape");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker
        .register_workflow::<RuntimeParentCancelsChildWf>()
        .unwrap();
    worker.register_workflow::<GrandchildCancelled>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            RuntimeParentCancelsChildWf::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct GrandchildCancelled;

#[workflow_methods]
impl GrandchildCancelled {
    #[run(name = "grandchild_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.cancelled().await;
        Err(WorkflowTermination::Cancelled)
    }
}

#[workflow]
#[derive(Default)]
struct PropagatesChildCancellationWf;

#[workflow_methods]
impl PropagatesChildCancellationWf {
    #[run(name = "child_propagates_cancellation")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                GrandchildCancelled::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: format!("{}-grandchild", ctx.task_queue()),
                    cancel_type: ChildWorkflowCancellationType::WaitCancellationCompleted,
                    ..Default::default()
                },
            )
            .await
            .expect("grandchild should start");
        started.cancel("grandchild cancel".to_string());
        started.result().await?;
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct GrandchildCancellationWf;

#[workflow_methods]
impl GrandchildCancellationWf {
    #[run(name = "parent_observes_child_failure_from_grandchild_cancellation")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let child_workflow_id = format!("{}-child", ctx.task_queue());
        let started = ctx
            .start_child_workflow(
                PropagatesChildCancellationWf::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: child_workflow_id.clone(),
                    ..Default::default()
                },
            )
            .await?;
        let err = started.result().await.expect_err("child should fail");
        let ChildWorkflowExecutionError::Failed(failure) = err else {
            panic!("child should fail with a child-workflow failure");
        };
        assert_eq!(
            failure
                .workflow_execution()
                .map(|wf| wf.workflow_id.as_str()),
            Some(child_workflow_id.as_str())
        );
        assert_eq!(
            failure.workflow_type().map(|wf| wf.name.as_str()),
            Some("child_propagates_cancellation")
        );
        let grandchild_workflow_id = format!("{}-grandchild", ctx.task_queue());
        let Some(IncomingError::ChildWorkflowExecution(grandchild_failure)) = failure.cause()
        else {
            panic!("child failure should retain the grandchild failure wrapper");
        };
        assert_eq!(
            grandchild_failure
                .workflow_execution()
                .map(|wf| wf.workflow_id.as_str()),
            Some(grandchild_workflow_id.as_str())
        );
        assert_eq!(
            grandchild_failure
                .workflow_type()
                .map(|wf| wf.name.as_str()),
            Some("grandchild_wf")
        );
        let Some(cancelled) = grandchild_failure.as_cancelled() else {
            panic!("grandchild failure should retain the cancelled reason");
        };
        assert!(cancelled.raw_details().is_none());
        Ok(())
    }
}

#[tokio::test]
async fn child_workflow_cancellation_propigates() {
    let mut starter = CoreWfStarter::new("child-workflow-cancellation-propigates");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker
        .register_workflow::<GrandchildCancellationWf>()
        .unwrap();
    worker
        .register_workflow::<PropagatesChildCancellationWf>()
        .unwrap();
    worker.register_workflow::<GrandchildCancelled>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            GrandchildCancellationWf::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[case::abandon(ChildWorkflowCancellationType::Abandon)]
#[case::try_cancel(ChildWorkflowCancellationType::TryCancel)]
#[case::wait_cancel_completed(ChildWorkflowCancellationType::WaitCancellationCompleted)]
#[tokio::test]
async fn cancel_child_workflow_lang_thinks_not_started_but_is(
    #[case] cancellation_type: ChildWorkflowCancellationType,
) {
    // Since signal handlers always run first, it's possible lang might try to cancel
    // a child workflow it thinks isn't started, but we've told it is in the same activation.
    // It would be annoying for lang to have to peek ahead at jobs to be consistent in that case.
    let t = match cancellation_type {
        ChildWorkflowCancellationType::Abandon => {
            canned_histories::single_child_workflow_abandon_cancelled("child-id-1")
        }
        ChildWorkflowCancellationType::TryCancel => {
            canned_histories::single_child_workflow_try_cancelled("child-id-1")
        }
        _ => canned_histories::single_child_workflow_cancelled("child-id-1"),
    };
    let mock = mock_worker_client();
    let mock = single_hist_mock_sg("fakeid", t, [ResponseType::AllHistory], mock, true);
    let core = mock_worker(mock);
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        StartChildWorkflowExecution {
            seq: 1,
            cancellation_type: cancellation_type as i32,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveChildWorkflowExecutionStart(_)),
        }]
    );
    // Issue the cancel command
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        CancelChildWorkflowExecution {
            child_workflow_seq: 1,
            reason: "dieee".to_string(),
        }
        .into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    // Make sure that a resolve for the "request cancel external workflow" command does *not* appear
    // since lang didn't actually issue one. The only job should be resolving the child workflow.
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveChildWorkflowExecution(_)),
        }]
    );
    // Request cancel external is technically fallible, but the only reasons relate to targeting
    // a not-found workflow, which couldn't happen in this case.
}

#[tokio::test]
async fn cancel_already_complete_child_ignored() {
    let t = canned_histories::single_child_workflow("child-id-1");
    let mock = mock_worker_client();
    let mock = single_hist_mock_sg("fakeid", t, [ResponseType::AllHistory], mock, true);
    let core = mock_worker(mock);
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        StartChildWorkflowExecution {
            seq: 1,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveChildWorkflowExecutionStart(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveChildWorkflowExecution(_)),
        }]
    );
    // Try to cancel post-completion, it should be ignored. Also complete the wf.
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![
            CancelChildWorkflowExecution {
                child_workflow_seq: 1,
                reason: "go away!".to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

#[workflow]
struct PassChildWorkflowSummaryToMetadata {
    child_wf_id: String,
}

#[workflow_methods(factory_only)]
impl PassChildWorkflowSummaryToMetadata {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let child_wf_id = ctx.state(|wf| wf.child_wf_id.clone());
        ctx.start_child_workflow(
            UntypedWorkflow::new("child"),
            RawValue::new(vec![]),
            ChildWorkflowOptions {
                workflow_id: child_wf_id,
                static_summary: Some("child summary".to_string()),
                static_details: Some("child details".to_string()),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn pass_child_workflow_summary_to_metadata() {
    let wf_id = "1";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_child_workflow(wf_id);
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_user_metadata = Some(UserMetadata {
        summary: Some("child summary".as_json_payload().unwrap()),
        details: Some("child details".as_json_payload().unwrap()),
    });
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
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
    let child_wf_id = wf_id.to_string();
    worker
        .register_workflow_with_factory(move || PassChildWorkflowSummaryToMetadata {
            child_wf_id: child_wf_id.clone(),
        })
        .unwrap();
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

#[derive(Clone, Copy)]
enum Expectation {
    Success,
    Failure,
    StartFailure,
}

impl Expectation {
    const fn try_from_u8(x: u8) -> Option<Self> {
        Some(match x {
            0 => Self::Success,
            1 => Self::Failure,
            2 => Self::StartFailure,
            _ => return None,
        })
    }
}

#[fixture]
fn child_workflow_happy_hist() -> MockPollCfg {
    let mut t = canned_histories::single_child_workflow("child-id-1");
    t.set_wf_input((Expectation::Success as u8).as_json_payload().unwrap());
    MockPollCfg::from_hist_builder(t)
}

#[fixture]
fn child_workflow_fail_hist() -> MockPollCfg {
    let mut t = canned_histories::single_child_workflow_fail("child-id-1");
    t.set_wf_input((Expectation::Failure as u8).as_json_payload().unwrap());
    MockPollCfg::from_hist_builder(t)
}

#[workflow]
#[derive(Default)]
struct ParentWf;

#[workflow_methods]
impl ParentWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>, expectation_u8: u8) -> WorkflowResult<()> {
        let expectation = Expectation::try_from_u8(expectation_u8).unwrap();
        let start_res = ctx
            .start_child_workflow(
                UntypedWorkflow::new("child"),
                RawValue::new(vec![]),
                ChildWorkflowOptions {
                    workflow_id: "child-id-1".to_string(),
                    ..Default::default()
                },
            )
            .await;
        if let Expectation::StartFailure = expectation {
            match start_res {
                Err(ChildWorkflowStartError::StartFailed { .. }) => return Ok(()),
                _ => return Err(anyhow!("Expected start failure").into()),
            }
        }
        let started = start_res?;
        match (expectation, started.result().await) {
            (Expectation::Success, Ok(_)) => Ok(()),
            (Expectation::Failure, Err(ChildWorkflowExecutionError::Failed(failure))) => {
                assert_eq!(
                    failure
                        .workflow_execution()
                        .map(|wf| wf.workflow_id.as_str()),
                    Some("child-id-1")
                );
                assert_eq!(
                    failure.workflow_type().map(|wf| wf.name.as_str()),
                    Some("child")
                );
                Ok(())
            }
            _ => Err(anyhow!("Unexpected child WF status").into()),
        }
    }
}

#[rstest(
    mock_cfg,
    case::success(child_workflow_happy_hist()),
    case::failure(child_workflow_fail_hist())
)]
#[tokio::test]
async fn single_child_workflow_until_completion(mut mock_cfg: MockPollCfg) {
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
                );
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 0);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<ParentWf>().unwrap();
    worker.run().await.unwrap();
}

#[tokio::test]
async fn single_child_workflow_start_fail() {
    let child_wf_id = "child-id-1";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.set_wf_input((Expectation::StartFailure as u8).as_json_payload().unwrap());
    t.add_full_wf_task();
    let initiated_event_id = t.add(StartChildWorkflowExecutionInitiatedEventAttributes {
        workflow_id: child_wf_id.to_owned(),
        workflow_type: Some("child".into()),
        ..Default::default()
    });
    t.add(StartChildWorkflowExecutionFailedEventAttributes {
        workflow_id: child_wf_id.to_owned(),
        initiated_event_id,
        cause: StartChildWorkflowExecutionFailedCause::WorkflowAlreadyExists as i32,
        ..Default::default()
    });
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
                );
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<ParentWf>().unwrap();
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct CancelBeforeSendWf;

#[workflow_methods]
impl CancelBeforeSendWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let workflow_id = "child-id-1";
        let start = ctx.start_child_workflow(
            UntypedWorkflow::new("child"),
            RawValue::new(vec![]),
            ChildWorkflowOptions {
                workflow_id: workflow_id.to_string(),
                ..Default::default()
            },
        );
        start.cancel();
        match start.await {
            Err(ChildWorkflowStartError::Cancelled(_)) => Ok(()),
            _ => Err(anyhow!("Unexpected start status").into()),
        }
    }
}

#[tokio::test]
async fn single_child_workflow_cancel_before_sent() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 1);
            assert_matches!(
                wft.commands[0].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<CancelBeforeSendWf>().unwrap();
    worker.run().await.unwrap();
}

#[tokio::test]
async fn cancel_child_before_started_event() {
    // Reproduce the bug where parent cancel arrives while child workflow machine is in
    // StartEventRecorded (i.e. before ChildWorkflowExecutionStarted event).
    let t = canned_histories::cancel_child_workflow_before_started_event("child-id-1");
    let mock = mock_worker_client();
    let mock = single_hist_mock_sg("fakeid", t, [ResponseType::AllHistory], mock, true);
    let core = mock_worker(mock);

    // WFT1: Initialize workflow, issue start child workflow command
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        StartChildWorkflowExecution {
            seq: 1,
            workflow_id: "child-id-1".to_string(),
            workflow_type: "child".to_string(),
            cancellation_type: ChildWorkflowCancellationType::WaitCancellationCompleted as i32,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    // WFT2: Cancel arrives before ChildWorkflowExecutionStarted. Lang sees a CancelWorkflow job.
    // Lang cancels the child and the workflow.
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![
            CancelChildWorkflowExecution {
                child_workflow_seq: 1,
                reason: "parent cancelled".to_string(),
            }
            .into(),
            CancelWorkflowExecution {}.into(),
        ],
    ))
    .await
    .unwrap();

    // WFT3: Resolve the child workflow start and completion (both arrive now), then workflow done.
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        CancelWorkflowExecution {}.into(),
    ))
    .await
    .unwrap();
}

#[workflow]
#[derive(Default)]
struct CancelChildBeforeStartedCannedWf;

#[workflow_methods]
impl CancelChildBeforeStartedCannedWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let start = ctx.start_child_workflow(
            UntypedWorkflow::new("child"),
            RawValue::new(vec![]),
            ChildWorkflowOptions {
                workflow_id: "child-id-1".to_string(),
                cancel_type: ChildWorkflowCancellationType::WaitCancellationCompleted,
                ..Default::default()
            },
        );
        ctx.cancelled().await;
        start.cancel();
        let started = start
            .await
            .expect("child should still report a successful start");
        let err = started
            .result()
            .await
            .expect_err("child result should be cancelled");
        let ChildWorkflowExecutionError::Failed(failure) = err else {
            panic!("started child cancellation should stay wrapper-shaped");
        };
        let Some(cancelled) = failure.as_cancelled() else {
            panic!("child failure should retain cancelled reason");
        };
        assert!(cancelled.raw_details().is_none());
        assert!(cancelled.cause().is_none());
        Err(WorkflowTermination::Cancelled)
    }
}

#[tokio::test]
async fn cancel_child_before_started_event_exposes_cancelled_error() {
    let t = canned_histories::cancel_child_workflow_before_started_event("child-id-1");
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    worker
        .register_workflow::<CancelChildBeforeStartedCannedWf>()
        .unwrap();
    worker.run().await.unwrap();
}

#[workflow]
struct CancelChildBeforeStartedParent {
    barr: Arc<Notify>,
}

#[workflow_methods(factory_only)]
impl CancelChildBeforeStartedParent {
    #[run(name = "parent_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx.start_child_workflow(
            AbandonedChildBugReproChild::run,
            (),
            ChildWorkflowOptions {
                workflow_id: "cancel-before-started-child".to_owned(),
                cancel_type: ChildWorkflowCancellationType::WaitCancellationCompleted,
                ..Default::default()
            },
        );

        ctx.state(|wf| wf.barr.notify_one());
        // Wait for parent cancellation
        ctx.cancelled().await;
        started.cancel();
        Err(WorkflowTermination::Cancelled)
    }
}

#[tokio::test]
async fn cancel_child_wf_before_started_event_real_server() {
    let mut starter = CoreWfStarter::new("child-wf-cancel-before-start");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    let barr = Arc::new(Notify::new());
    let barr_clone = barr.clone();
    worker
        .register_workflow_with_factory(move || CancelChildBeforeStartedParent {
            barr: barr_clone.clone(),
        })
        .unwrap();
    worker
        .register_workflow::<AbandonedChildBugReproChild>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            CancelChildBeforeStartedParent::run,
            (),
            WorkflowStartOptions::new(task_queue, "cancel-before-started-parent").build(),
        )
        .await
        .unwrap();

    let canceller = async {
        barr.notified().await;
        handle
            .cancel(WorkflowCancelOptions::builder().reason("die").build())
            .await
            .unwrap();
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(canceller, runner);

    // Verify no unexpected workflow task failures in history. The bug manifests as a WFT failure
    // with a nondeterminism error. UnhandledCommand failures are acceptable since the server
    // may reject a cancel command if it races with the child workflow start.
    let history = handle.fetch_history(Default::default()).await.unwrap();
    let unexpected_wft_failures: Vec<_> = history
        .events()
        .iter()
        .filter(|e| {
            if let Some(history_event::Attributes::WorkflowTaskFailedEventAttributes(attrs)) =
                &e.attributes
            {
                attrs.cause != WorkflowTaskFailedCause::UnhandledCommand as i32
            } else {
                false
            }
        })
        .collect();
    assert!(
        unexpected_wft_failures.is_empty(),
        "Expected no unexpected WorkflowTaskFailed events, but found: {:?}",
        unexpected_wft_failures
    );

    // Replay the history to verify determinism
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[workflow]
#[derive(Default)]
struct UntypedHappyParent;

#[workflow_methods]
impl UntypedHappyParent {
    #[run(name = "parent_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                UntypedWorkflow::new(CHILD_WF_TYPE),
                RawValue::new(vec![]),
                ChildWorkflowOptions {
                    workflow_id: "untyped-child-1".to_owned(),
                    ..Default::default()
                },
            )
            .await?;
        started.result().await?;
        Ok(())
    }
}

#[tokio::test]
async fn untyped_child_workflow_happy_path() {
    let mut starter = CoreWfStarter::new("untyped-child-workflows");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker.register_workflow::<UntypedHappyParent>().unwrap();
    worker.register_workflow::<ChildWf>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_wf(
            PARENT_WF_TYPE.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, "untyped-parent".to_string()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

// --- Serialization failure tests ---

/// Input type whose `Serialize` impl always fails.
struct AlwaysFailsSerialize;

impl serde::Serialize for AlwaysFailsSerialize {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "intentional serialization failure",
        ))
    }
}

impl<'de> serde::Deserialize<'de> for AlwaysFailsSerialize {
    fn deserialize<D: serde::Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Ok(AlwaysFailsSerialize)
    }
}

#[workflow]
#[derive(Default)]
struct UnserializableStartInputChild;

#[workflow_methods]
impl UnserializableStartInputChild {
    #[run]
    async fn run(
        _ctx: &mut WorkflowContext<Self>,
        _input: AlwaysFailsSerialize,
    ) -> WorkflowResult<()> {
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct UnserializableSignalChild;

#[workflow_methods]
impl UnserializableSignalChild {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.cancelled().await;
        Err(WorkflowTermination::Cancelled)
    }

    #[signal]
    fn bad_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: AlwaysFailsSerialize) {}
}

#[workflow]
#[derive(Default)]
struct ChildStartSerializationFailParent;

#[workflow_methods]
impl ChildStartSerializationFailParent {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let result = ctx
            .start_child_workflow(
                UnserializableStartInputChild::run,
                AlwaysFailsSerialize,
                ChildWorkflowOptions {
                    workflow_id: "unserializable-child".to_owned(),
                    ..Default::default()
                },
            )
            .await;
        assert_matches!(result, Err(ChildWorkflowStartError::Serialization(_)));
        Ok(())
    }
}

#[tokio::test]
async fn child_workflow_start_serialization_failure_returns_error() {
    let mut starter = CoreWfStarter::new("child-wf-start-ser-fail");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker
        .register_workflow::<ChildStartSerializationFailParent>()
        .unwrap();
    worker
        .register_workflow::<UnserializableStartInputChild>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_wf(
            "ChildStartSerializationFailParent".to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, "ser-fail-parent".to_string()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct ChildSignalSerializationFailParent;

#[workflow_methods]
impl ChildSignalSerializationFailParent {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                UnserializableSignalChild::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: "signal-ser-fail-child".to_owned(),
                    ..Default::default()
                },
            )
            .await?;

        let signal_result = started
            .signal(UnserializableSignalChild::bad_signal, AlwaysFailsSerialize)
            .await;
        assert_matches!(signal_result, Err(WorkflowSignalError::Serialization(_)));

        // Cancel child so parent can complete cleanly.
        started.cancel("test done".to_string());
        let _ = started.result().await;
        Ok(())
    }
}

#[tokio::test]
async fn child_workflow_signal_serialization_failure_returns_error() {
    let mut starter = CoreWfStarter::new("child-wf-signal-ser-fail");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker
        .register_workflow::<ChildSignalSerializationFailParent>()
        .unwrap();
    worker
        .register_workflow::<UnserializableSignalChild>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_wf(
            "ChildSignalSerializationFailParent".to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, "signal-ser-fail-parent".to_string()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct UnitChildWf;

#[workflow_methods]
impl UnitChildWf {
    #[run(name = "child")]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct UnitChildParentWf;

#[workflow_methods]
impl UnitChildParentWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                UnitChildWf::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: "child-id-1".to_string(),
                    ..Default::default()
                },
            )
            .await?;
        started.result().await?;
        Ok(())
    }
}

/// Parent that starts a typed child returning () and awaits its result.
/// With a canned history whose completion is missing a result (simulating a
/// non-Rust child workflow that might not have a result payload).
#[tokio::test]
async fn child_workflow_unit_result_none_payload() {
    // single_child_workflow produces a completion with result: None
    let t = canned_histories::single_child_workflow("child-id-1");
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    worker.register_workflow::<UnitChildParentWf>().unwrap();
    worker.run().await.unwrap();
}

/// Parent that starts a child, then cancels the result future
#[workflow]
#[derive(Default)]
struct CancelResultFutureParent;

#[workflow_methods]
impl CancelResultFutureParent {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = ctx
            .start_child_workflow(
                CancelledChildGetsReasonChild::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: format!("{}-child", ctx.task_queue()),
                    cancel_type: ChildWorkflowCancellationType::WaitCancellationCompleted,
                    ..Default::default()
                },
            )
            .await?;

        let result_fut = started.result();
        result_fut.cancel();
        let _ = result_fut.await;
        Ok(())
    }
}

#[tokio::test]
async fn cancel_child_result_future_does_not_fail_wft() {
    let wf_name = "cancel-child-result-future";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker
        .register_workflow::<CancelResultFutureParent>()
        .unwrap();
    worker
        .register_workflow::<CancelledChildGetsReasonChild>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();

    worker
        .submit_workflow(
            CancelResultFutureParent::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();

    // The bug causes an infinite WFT failure loop; timeout detects it.
    tokio::time::timeout(Duration::from_secs(30), worker.run_until_done())
        .await
        .expect("Timed out — workflow stuck in WFT failure loop")
        .unwrap();
}

#[workflow]
#[derive(Default)]
struct CancelExternalTarget;

#[workflow_methods]
impl CancelExternalTarget {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let r = ctx.cancelled().await;
        Ok(r)
    }
}

/// Cancels an external workflow, then starts a child and cancels it.
///
/// This diverges the `cancel_external_wf_seq` counter from the
/// `child_workflow_seq` counter. Previously, `started.cancel()` read the
/// cancel-external seq instead of the child seq, triggering NDE.
#[workflow]
#[derive(Default)]
struct CancelExternalThenChildParent;

#[workflow_methods]
impl CancelExternalThenChildParent {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        // Advance cancel_external_wf_seq without touching child_workflow_seq.
        let cancel_target_id = format!("{}-cancel-target", ctx.task_queue());
        let handle = ctx.external_workflow(cancel_target_id, None);
        handle.cancel(None).await.unwrap();

        // Now start a child — child_seq=1, but cancel_external_wf_seq=3.
        let started = ctx
            .start_child_workflow(
                CancelledChildGetsReasonChild::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: format!("{}-child", ctx.task_queue()),
                    cancel_type: ChildWorkflowCancellationType::WaitCancellationRequested,
                    ..Default::default()
                },
            )
            .await
            .expect("Child should start OK");

        started.cancel("no longer needed".to_string());
        let reason: String = started.result().await.expect("Child should complete OK");
        assert!(
            reason.contains("no longer needed"),
            "Expected cancel reason in child result, got: {reason}",
        );
        Ok(())
    }
}

#[tokio::test]
async fn cancel_child_after_cancel_external_uses_correct_seq() {
    let wf_name = "cancel-child-after-cancel-external";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker
        .register_workflow::<CancelExternalThenChildParent>()
        .unwrap();
    worker
        .register_workflow::<CancelledChildGetsReasonChild>()
        .unwrap();
    worker.register_workflow::<CancelExternalTarget>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();

    worker
        .submit_workflow(
            CancelExternalTarget::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), format!("{task_queue}-cancel-target"))
                .build(),
        )
        .await
        .unwrap();

    worker
        .submit_workflow(
            CancelExternalThenChildParent::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
}
