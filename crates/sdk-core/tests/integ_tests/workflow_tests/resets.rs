use crate::common::{CoreWfStarter, NAMESPACE, activity_functions::StdActivities};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    WorkflowSignalOptions, WorkflowStartOptions, errors::WorkflowGetResultError,
    grpc::WorkflowService,
};
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution, workflowservice::v1::ResetWorkflowExecutionRequest,
};

use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{LocalActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult};
use tokio::sync::Notify;
use tonic::IntoRequest;

const POST_FAIL_SIG: &str = "post-fail";
const POST_RESET_SIG: &str = "post-reset";

#[workflow]
struct ResetMeWf {
    notify: Arc<Notify>,
    post_reset_received: bool,
}

#[workflow_methods(factory_only)]
impl ResetMeWf {
    #[run(name = "reset_me_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(1)).await;
        ctx.timer(Duration::from_secs(1)).await;
        ctx.state(|wf| wf.notify.notify_one());
        ctx.wait_condition(|s| s.post_reset_received).await;
        Ok(())
    }

    #[signal(name = POST_RESET_SIG)]
    fn post_reset(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.post_reset_received = true;
    }
}

#[tokio::test]
async fn reset_workflow() {
    let wf_name = "reset_me_wf";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.fetch_results = false;

    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();
    worker
        .register_workflow_with_factory(move || ResetMeWf {
            notify: notify_clone.clone(),
            post_reset_received: false,
        })
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ResetMeWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    let run_id = handle.info().run_id.clone().unwrap();

    let mut client = starter.get_client().await;
    let resetter_fut = async {
        notify.notified().await;
        // Do the reset
        client
            .reset_workflow_execution(
                ResetWorkflowExecutionRequest {
                    namespace: NAMESPACE.to_owned(),
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: wf_name.to_owned(),
                        run_id,
                    }),
                    // End of first WFT
                    workflow_task_finish_event_id: 4,
                    request_id: "test-req-id".to_owned(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();

        // Unblock the workflow by sending the signal. Run ID will have changed after reset so
        // we re-obtain handle.
        let handle = client.get_workflow_handle::<reset_me_wf::Run>(wf_name.to_owned());
        handle
            .signal(ResetMeWf::post_reset, (), WorkflowSignalOptions::default())
            .await
            .unwrap();

        // Wait for the now-reset workflow to finish
        handle.get_result(Default::default()).await.unwrap();
        starter.shutdown().await;
    };
    let run_fut = worker.run_until_done();
    let (_, rr) = tokio::join!(resetter_fut, run_fut);
    rr.unwrap();
}

#[workflow]
struct ResetRandomseedWf {
    did_fail: Arc<AtomicBool>,
    rand_seed: Arc<AtomicU64>,
    saw_updated_seed: Arc<AtomicBool>,
    notify: Arc<Notify>,
    post_fail_received: bool,
    post_reset_received: bool,
}

#[workflow_methods(factory_only)]
impl ResetRandomseedWf {
    #[run(name = "reset_randomseed")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let _ = ctx.state(|wf| {
            wf.rand_seed.compare_exchange(
                0,
                ctx.random_seed(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
        });
        ctx.timer(Duration::from_millis(100)).await;
        ctx.timer(Duration::from_millis(100)).await;
        if ctx
            .state(|wf| {
                wf.did_fail
                    .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            })
            .is_ok()
        {
            ctx.state(|wf| wf.notify.notify_one());
            panic!("Ahh");
        }
        if ctx.state(|wf| wf.rand_seed.load(Ordering::Relaxed)) == ctx.random_seed() {
            ctx.timer(Duration::from_millis(100)).await;
        } else {
            ctx.state(|wf| {
                wf.saw_updated_seed.store(true, Ordering::Relaxed);
            });
            ctx.start_local_activity(
                StdActivities::echo,
                "hi!".to_string(),
                LocalActivityOptions::default(),
            )
            .await?;
        }
        ctx.wait_condition(|s| s.post_fail_received).await;
        ctx.state(|wf| wf.notify.notify_one());
        ctx.wait_condition(|s| s.post_reset_received).await;
        Ok(())
    }

    #[signal(name = POST_FAIL_SIG)]
    fn post_fail(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.post_fail_received = true;
    }

    #[signal(name = POST_RESET_SIG)]
    fn post_reset(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.post_reset_received = true;
    }
}

#[tokio::test]
async fn reset_randomseed() {
    let wf_name = "reset_randomseed";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: true,
        enable_remote_activities: false,
        enable_nexus: true,
    };
    let mut worker = starter.worker().await;
    worker.fetch_results = false;

    let did_fail = Arc::new(AtomicBool::new(false));
    let rand_seed = Arc::new(AtomicU64::new(0));
    let saw_updated_seed = Arc::new(AtomicBool::new(false));
    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();
    let saw_updated_seed_for_wf = saw_updated_seed.clone();
    worker
        .register_workflow_with_factory(move || ResetRandomseedWf {
            did_fail: did_fail.clone(),
            rand_seed: rand_seed.clone(),
            saw_updated_seed: saw_updated_seed_for_wf.clone(),
            notify: notify_clone.clone(),
            post_fail_received: false,
            post_reset_received: false,
        })
        .unwrap();
    worker.register_activities(StdActivities);

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ResetRandomseedWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    let run_id = handle.info().run_id.clone().unwrap();

    let mut client = starter.get_client().await;
    let client_fur = async {
        notify.notified().await;
        handle
            .signal(
                ResetRandomseedWf::post_fail,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        notify.notified().await;
        // Reset the workflow to be after first timer has fired
        client
            .reset_workflow_execution(
                ResetWorkflowExecutionRequest {
                    namespace: NAMESPACE.to_owned(),
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id: wf_name.to_owned(),
                        run_id: run_id.clone(),
                    }),
                    workflow_task_finish_event_id: 14,
                    request_id: "test-req-id".to_owned(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();

        // Unblock the workflow by sending the signal. Run ID will have changed after reset so
        // we re-obtain the handle.
        let reset_handle =
            client.get_workflow_handle::<reset_randomseed_wf::Run>(wf_name.to_owned());
        reset_handle
            .signal(
                ResetRandomseedWf::post_reset,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        // Wait for the original run to terminate and the reset run to finish.
        let result = handle.get_result(Default::default()).await;
        assert_matches!(result, Err(WorkflowGetResultError::Terminated { .. }));
        reset_handle.get_result(Default::default()).await.unwrap();
        assert!(
            saw_updated_seed.load(Ordering::Relaxed),
            "workflow should observe the server-supplied updated random seed after reset"
        );
        starter.shutdown().await;
    };
    let run_fut = worker.run_until_done();
    let (_, rr) = tokio::join!(client_fur, run_fut);
    rr.unwrap();
}
