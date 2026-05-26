use crate::{common::CoreWfStarter, integ_tests::workflow_tests::timers::TimerWf};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::WorkflowStartOptions;
use temporalio_common::{protos::temporal::api::enums::v1::EventType, worker::WorkerTaskTypes};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};
use temporalio_sdk_core::{PollerBehavior, TunerHolder};
use tokio::sync::Barrier;

#[tokio::test]
async fn timer_workflow_not_sticky() {
    let wf_name = "timer_wf_not_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.max_cached_workflows = 0_usize;
    let mut worker = starter.worker().await;
    worker.register_workflow::<TimerWf>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            TimerWf::run,
            (),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
struct TimerTimeoutWf {
    timed_out_once: Arc<AtomicBool>,
    run_ct: Arc<AtomicUsize>,
}

#[workflow_methods(factory_only)]
impl TimerTimeoutWf {
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.state(|wf| wf.run_ct.fetch_add(1, Ordering::SeqCst));
        let t = ctx.timer(Duration::from_secs(1));
        if !ctx.state(|wf| wf.timed_out_once.load(Ordering::SeqCst)) {
            ctx.force_task_fail(anyhow::anyhow!("I AM SLAIN!"));
            ctx.state(|wf| wf.timed_out_once.store(true, Ordering::SeqCst));
        }
        t.await;
        Ok(())
    }
}

#[tokio::test]
async fn timer_workflow_timeout_on_sticky() {
    // This test intentionally times out a workflow task in order to make the next task be scheduled
    // on a not-sticky queue
    let wf_name = "timer_workflow_timeout_on_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.workflow_options.task_timeout = Some(Duration::from_secs(2));
    let mut worker = starter.worker().await;

    let timed_out_once = Arc::new(AtomicBool::new(false));
    let run_ct = Arc::new(AtomicUsize::new(0));
    let run_ct_clone = run_ct.clone();
    worker
        .register_workflow_with_factory(move || TimerTimeoutWf {
            timed_out_once: timed_out_once.clone(),
            run_ct: run_ct_clone.clone(),
        })
        .unwrap();

    worker
        .submit_workflow(TimerTimeoutWf::run, (), starter.workflow_options.clone())
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // If it didn't run twice it didn't time out
    assert_eq!(run_ct.load(Ordering::SeqCst), 2);

    // `force_task_fail` happens after creating the timer, so the workflow StartTimer command
    // buffered when the task fails. The failure path should evict the workflow and drop that host
    // before replay retries the task. If the old buffer survived, the retry would send the stale
    // StartTimer along with the new one.
    let history = starter.get_history().await;
    let timer_started_count = history
        .events
        .iter()
        .filter(|event| event.event_type() == EventType::TimerStarted)
        .count();
    let wft_failed_count = history
        .events
        .iter()
        .filter(|event| event.event_type() == EventType::WorkflowTaskFailed)
        .count();
    assert_eq!(wft_failed_count, 1);
    assert_eq!(timer_started_count, 1);
}

#[workflow]
struct CacheMissWf {
    barr: Arc<Barrier>,
}

#[workflow_methods(factory_only)]
impl CacheMissWf {
    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.state(|wf| wf.barr.clone()).wait().await;
        ctx.timer(Duration::from_secs(1)).await;
        Ok(())
    }
}

#[tokio::test]
async fn cache_miss_ok() {
    let wf_name = "cache_miss_ok";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(2, 1, 1, 1));
    starter.sdk_config.max_cached_workflows = 0_usize;
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(1_usize);
    let mut worker = starter.worker().await;

    let barr = Arc::new(Barrier::new(2));
    let barr_clone = barr.clone();
    worker
        .register_workflow_with_factory(move || CacheMissWf {
            barr: barr_clone.clone(),
        })
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            CacheMissWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    let core = starter.get_worker().await;
    let run_id = handle.info().run_id.clone().unwrap();
    let (r1, _) = tokio::join!(worker.run_until_done(), async move {
        barr.wait().await;
        core.request_workflow_eviction(&run_id);
        // We need to signal the barrier again since the wf gets evicted and will hit it again
        barr.wait().await;
    });
    r1.unwrap();
}
