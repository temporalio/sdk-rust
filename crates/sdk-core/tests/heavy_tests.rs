// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
pub(crate) mod common;
// This prevents cargo from auto-picking up this file as its own test binary. Odd but functional.
#[path = "heavy_tests/fuzzy_workflow.rs"]
mod fuzzy_workflow;

use crate::common::get_integ_runtime_options;
use common::{
    CoreWfStarter, activity_functions::StdActivities, init_integ_telem, prom_metrics, rand_6_chars,
    workflows::LaProblemWorkflow,
};
use futures_util::{
    StreamExt,
    future::{AbortHandle, Abortable, join_all},
    sink, stream,
    stream::FuturesUnordered,
};
use rand::RngExt;
use std::{
    mem,
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_client::{
    NamespacedClient, UntypedSignal, UntypedWorkflow, WorkflowExecutionInfo,
    WorkflowGetResultOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::{
    data_converters::RawValue, protos::temporal::api::enums::v1::WorkflowIdConflictPolicy,
};
use temporalio_macros::{activities, workflow, workflow_methods};

use temporalio_common::{
    protos::{
        coresdk::workflow_commands::ActivityCancellationType,
        temporal::api::enums::v1::WorkflowIdReusePolicy,
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk::{
    ActivityCloseTimeouts, ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    workflows,
};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, ResourceBasedTuner, ResourceSlotOptions, TunerHolder,
};

#[workflow]
#[derive(Clone, Default)]
struct ActivityLoadWf;

#[workflow_methods]
impl ActivityLoadWf {
    #[run(name = "activity_load")]
    async fn run(ctx: &mut WorkflowContext<Self>, tq: String) -> WorkflowResult<()> {
        let input_str = "yo".to_string();
        let res = ctx
            .start_activity(
                StdActivities::echo,
                input_str.clone(),
                ActivityOptions::with_close_timeouts(ActivityCloseTimeouts::Both {
                    start_to_close: Duration::from_secs(8),
                    schedule_to_close: Duration::from_secs(8),
                })
                .activity_id("act-1".to_string())
                .task_queue(tq)
                .schedule_to_start_timeout(Duration::from_secs(8))
                .heartbeat_timeout(Duration::from_secs(8))
                .cancellation_type(ActivityCancellationType::TryCancel)
                .build(),
            )
            .await?;
        assert_eq!(res, input_str);
        Ok(())
    }
}

#[tokio::test]
async fn activity_load() {
    const CONCURRENCY: usize = 512;

    let mut starter = CoreWfStarter::new("activity_load");
    starter.sdk_config.max_cached_workflows = CONCURRENCY;
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::SimpleMaximum(10);
    starter.sdk_config.tuner =
        Arc::new(TunerHolder::fixed_size(CONCURRENCY, CONCURRENCY, 100, 100));
    starter.sdk_config.register_activities(StdActivities);
    let task_queue = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let starting = Instant::now();
    worker.register_workflow::<ActivityLoadWf>().unwrap();
    join_all((0..CONCURRENCY).map(|i| {
        let worker = &worker;
        let wf_id = format!("activity_load_{i}");
        let tq = task_queue.clone();
        async move {
            worker
                .submit_workflow(
                    ActivityLoadWf::run,
                    tq.clone(),
                    WorkflowStartOptions::new(tq, wf_id).build(),
                )
                .await
                .unwrap();
        }
    }))
    .await;
    dbg!(starting.elapsed());

    let running = Instant::now();

    worker.run_until_done().await.unwrap();
    dbg!(running.elapsed());
}

#[workflow]
#[derive(Default)]
struct ChunkyActivityWf;

#[workflow_methods]
impl ChunkyActivityWf {
    #[run(name = "chunky_activity_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let input_str = "yo".to_string();
        let res = ctx
            .start_activity(
                ChunkyActivities::chunky_echo,
                input_str.clone(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                    .activity_id("act-1".to_string())
                    .build(),
            )
            .await?;
        assert_eq!(res, input_str);
        Ok(())
    }
}

struct ChunkyActivities;
#[activities]
impl ChunkyActivities {
    #[activity]
    async fn chunky_echo(_ctx: ActivityContext, echo: String) -> Result<String, ActivityError> {
        tokio::task::spawn_blocking(move || {
            let mut mem = vec![0_u8; 1000 * 1024 * 1024];
            for _ in 1..10 {
                for i in 0..mem.len() {
                    mem[i] &= mem[mem.len() - 1 - i]
                }
            }
            Ok(echo)
        })
        .await?
    }
}

#[tokio::test]
async fn chunky_activities_resource_based() {
    const WORKFLOWS: usize = 100;

    let mut starter = CoreWfStarter::new("chunky_activities_resource_based");
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(10_usize);
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::SimpleMaximum(10_usize);
    let mut tuner = ResourceBasedTuner::new(0.7, 0.7);
    tuner
        .with_workflow_slots_options(ResourceSlotOptions::new(
            25,
            WORKFLOWS,
            Duration::from_millis(0),
        ))
        .with_activity_slots_options(ResourceSlotOptions::new(5, 1000, Duration::from_millis(50)));
    starter.sdk_config.tuner = Arc::new(tuner);

    starter.sdk_config.register_activities(ChunkyActivities);
    let task_queue = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let starting = Instant::now();
    worker.register_workflow::<ChunkyActivityWf>().unwrap();
    join_all((0..WORKFLOWS).map(|i| {
        let worker = &worker;
        let wf_id = format!("chunk_activity_{i}");
        let tq = task_queue.clone();
        async move {
            worker
                .submit_workflow(
                    ChunkyActivityWf::run,
                    (),
                    WorkflowStartOptions::new(tq, wf_id)
                        .id_conflict_policy(WorkflowIdConflictPolicy::TerminateExisting)
                        .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicate)
                        .build(),
                )
                .await
                .unwrap();
        }
    }))
    .await;
    dbg!(starting.elapsed());

    let running = Instant::now();

    worker.run_until_done().await.unwrap();
    dbg!(running.elapsed());
}

#[workflow]
#[derive(Default)]
struct WorkflowLoadWf;

#[workflow_methods]
impl WorkflowLoadWf {
    #[run(name = "workflow_load")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for _ in 0..5 {
            let _ = ctx
                .start_activity(
                    StdActivities::echo,
                    "hi!".to_string(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                )
                .await;
            ctx.timer(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    #[signal]
    fn drain_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_load() {
    let num_workflows = 500;
    let wf_name = "workflow_load";
    let (mut telemopts, _, _aborter) = prom_metrics(None);
    // Avoid initting two logging systems, since when this test is run with others it can
    // cause us to encounter the tracing span drop bug
    telemopts.logging = None;
    init_integ_telem();
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("workflow_load", rt);
    starter.sdk_config.max_cached_workflows = 200;
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::SimpleMaximum(10);
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(5, 100, 100, 100));
    starter.sdk_config.register_activities(StdActivities);
    let task_queue = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;
    worker.register_workflow::<WorkflowLoadWf>().unwrap();

    let mut workflow_handles = vec![];
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}");
        let handle = worker
            .submit_workflow(
                WorkflowLoadWf::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), wfid).build(),
            )
            .await
            .unwrap();
        workflow_handles.push(handle);
    }

    let sig_sender = async {
        loop {
            let sends: FuturesUnordered<_> = workflow_handles
                .iter()
                .map(|handle| async move {
                    handle
                        .signal(
                            WorkflowLoadWf::drain_signal,
                            (),
                            WorkflowSignalOptions::default(),
                        )
                        .await
                })
                .collect();
            sends
                .map(|_| Ok(()))
                .forward(sink::drain())
                .await
                .expect("Sending signals works");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    };
    tokio::select! { r1 = worker.run_until_done() => {r1.unwrap()}, _ = sig_sender => {}}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evict_while_la_running_no_interference() {
    let wf_name = "evict_while_la_running_no_interference";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.max_cached_workflows = 20;
    // Though it doesn't make sense to set wft higher than cached workflows, leaving this commented
    // introduces more instability that can be useful in the test.
    // starter.max_wft(20);
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(100, 10, 20, 1));
    starter.sdk_config.register_activities(StdActivities);
    let task_queue = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    worker.register_workflow::<LaProblemWorkflow>().unwrap();

    let client = starter.get_client().await;
    let subfs = FuturesUnordered::new();
    for i in 1..100 {
        let wf_id = format!("{wf_name}-{i}");
        let handle = worker
            .submit_workflow(
                LaProblemWorkflow::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), wf_id.clone()).build(),
            )
            .await
            .unwrap();
        let run_id = handle.run_id().unwrap().to_owned();
        let cw = worker.core_worker();
        let client = client.clone();
        subfs.push(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            cw.request_workflow_eviction(&run_id);
            WorkflowExecutionInfo {
                namespace: client.namespace(),
                workflow_id: wf_id,
                run_id: Some(run_id),
                first_execution_run_id: None,
            }
            .bind_untyped(client)
            .signal(
                UntypedSignal::new("whaatever"),
                RawValue::empty(),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        });
    }
    let runf = async {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(subfs.collect::<Vec<_>>(), runf);
}

#[workflow]
#[derive(Default)]
struct ManyParallelTimersLonghistWf;

#[workflow_methods]
impl ManyParallelTimersLonghistWf {
    #[run(name = "can_paginate_long_history")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for _ in 0..120 {
            let mut futs = vec![];
            for _ in 0..100 {
                futs.push(ctx.timer(Duration::from_millis(100)));
            }
            workflows::join_all(futs).await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn can_paginate_long_history() {
    let wf_name = "can_paginate_long_history";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.max_cached_workflows = 0;

    let mut worker = starter.worker().await;
    worker
        .register_workflow::<ManyParallelTimersLonghistWf>()
        .unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ManyParallelTimersLonghistWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    let run_id = handle.run_id().unwrap().to_owned();
    let client = starter.get_client().await;
    tokio::spawn(async move {
        let handle = WorkflowExecutionInfo {
            namespace: client.namespace(),
            workflow_id: wf_name.into(),
            run_id: Some(run_id),
            first_execution_run_id: None,
        }
        .bind_untyped(client);
        loop {
            for _ in 0..10 {
                handle
                    .signal(
                        UntypedSignal::new("sig"),
                        RawValue::empty(),
                        WorkflowSignalOptions::default(),
                    )
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });
    worker.run_until_done().await.unwrap();
}

struct JitteryActivities;
#[activities]
impl JitteryActivities {
    #[activity]
    async fn jittery_echo(_ctx: ActivityContext, echo: String) -> Result<String, ActivityError> {
        let rand_millis = rand::rng().random_range(0..500);
        tokio::time::sleep(Duration::from_millis(rand_millis)).await;
        Ok(echo)
    }
}

#[workflow]
#[derive(Default)]
struct PollerLoadWf;

#[workflow_methods]
impl PollerLoadWf {
    #[run(name = "poller_load")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for _ in 0..5 {
            let _ = ctx
                .start_activity(
                    JitteryActivities::jittery_echo,
                    "hi!".to_string(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                )
                .await;
        }

        Ok(())
    }

    #[signal(name = "signame")]
    fn drain_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {}
}

#[tokio::test]
async fn poller_autoscaling_basic_loadtest() {
    const SIGNAME: &str = "signame";
    let num_workflows = 100;
    let wf_name = "poller_load";
    let mut starter = CoreWfStarter::new("poller_load");
    starter.sdk_config.max_cached_workflows = 5000;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(1000, 1000, 100, 1));
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 200,
        initial: 5,
    };
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 200,
        initial: 5,
    };

    starter.sdk_config.register_activities(JitteryActivities);
    let mut worker = starter.worker().await;
    let shutdown_handle = worker.inner_mut().shutdown_handle();
    worker.register_workflow::<PollerLoadWf>().unwrap();
    let client = starter.get_client().await;

    let task_queue = starter.get_task_queue().to_owned();
    let mut workflow_handles = vec![];
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}-{}", rand_6_chars());
        let handle = worker
            .submit_workflow(
                PollerLoadWf::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), wfid)
                    .execution_timeout(Duration::from_secs(120))
                    .build(),
            )
            .await
            .unwrap();
        workflow_handles.push(handle);
    }

    let (ah, abort_reg) = AbortHandle::new_pair();
    let all_workflows_are_done = async {
        stream::iter(mem::take(&mut workflow_handles))
            .for_each_concurrent(25, |handle| async move {
                let _ = handle.get_result(WorkflowGetResultOptions::default()).await;
            })
            .await;
        ah.abort();
        shutdown_handle();
    };

    let sig_sender = Abortable::new(
        async {
            loop {
                let sends: FuturesUnordered<_> = (0..num_workflows)
                    .map(|i| {
                        let handle =
                            client.get_workflow_handle::<UntypedWorkflow>(format!("{wf_name}_{i}"));
                        async move {
                            handle
                                .signal(
                                    UntypedSignal::new(SIGNAME),
                                    RawValue::empty(),
                                    WorkflowSignalOptions::default(),
                                )
                                .await
                        }
                    })
                    .collect();
                sends
                    .map(|_| Ok(()))
                    .forward(sink::drain())
                    .await
                    .expect("Sending signals works");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        },
        abort_reg,
    );
    let (runres, _, _) = tokio::join!(worker.inner_mut().run(), sig_sender, all_workflows_are_done);
    runres.unwrap();
}
