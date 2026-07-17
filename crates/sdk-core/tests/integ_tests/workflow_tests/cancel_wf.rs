use crate::common::{ActivationAssertionsInterceptor, CoreWfStarter, build_fake_sdk};
use std::time::Duration;
use temporalio_client::{
    UntypedWorkflow, WorkflowCancelOptions, WorkflowDescribeOptions, WorkflowStartOptions,
};
use temporalio_common::{
    protos::{
        coresdk::workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        temporal::api::enums::v1::{CommandType, WorkflowExecutionStatus},
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult, WorkflowTermination};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, canned_histories},
    test_help::MockPollCfg,
};

#[workflow]
#[derive(Default)]
struct CancelledWf;

#[workflow_methods]
impl CancelledWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let mut reason = "".to_string();
        let cancelled = temporalio_sdk::workflows::select! {
            _ = ctx.timer(Duration::from_secs(500)) => false,
            r = ctx.cancelled() => {
                reason = r;
                true
            }
        };

        assert_eq!(reason, "Dieee");

        if cancelled {
            Err(WorkflowTermination::Cancelled)
        } else {
            panic!("Should have been cancelled")
        }
    }
}

#[tokio::test]
async fn cancel_during_timer() {
    let wf_name = "cancel_during_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_workflow::<CancelledWf>().unwrap();
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

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<WfWithTimer>().unwrap();
    worker.set_worker_interceptor(aai);
    worker.run().await.unwrap();
}
