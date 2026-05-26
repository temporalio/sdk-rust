//! These tests are expensive and meant to be run manually. Use them for things like perf and
//! load testing.

// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
mod common;

use crate::common::get_integ_runtime_options;
use common::{CoreWfStarter, prom_metrics, rand_6_chars};
use futures_util::{
    StreamExt,
    future::{AbortHandle, Abortable},
    sink, stream,
    stream::FuturesUnordered,
};
use rand::{RngExt, SeedableRng};
use std::{
    mem,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_client::{
    NamespacedClient, UntypedSignal, UntypedWorkflow, WorkflowExecutionInfo,
    WorkflowGetResultOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::{
    data_converters::RawValue, telemetry::PrometheusExporterOptions, worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_core::{CoreRuntime, PollerBehavior, TunerHolder};
use tracing::info;

struct JitteryEchoActivities;
#[activities]
impl JitteryEchoActivities {
    #[activity]
    async fn echo(_ctx: ActivityContext, echo: String) -> Result<String, ActivityError> {
        // Add some jitter to completions
        let rand_millis = rand::rng().random_range(0..500);
        tokio::time::sleep(Duration::from_millis(rand_millis)).await;
        Ok(echo)
    }
}

#[workflow]
#[derive(Default)]
struct PollerLoadSpikyWf;

#[workflow_methods]
impl PollerLoadSpikyWf {
    #[run(name = "poller_load")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for _ in 0..5 {
            let _ = ctx
                .start_activity(
                    JitteryEchoActivities::echo,
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

#[workflow]
#[derive(Default)]
struct PollerLoadSustainedWf;

#[workflow_methods]
impl PollerLoadSustainedWf {
    #[run(name = "poller_load")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let rs = ctx.random_seed();
        let mut rand = rand::rngs::SmallRng::seed_from_u64(rs);
        for _ in 0..100 {
            let jitterms = rand.random_range(1000..3000);
            ctx.timer(Duration::from_millis(jitterms)).await;
        }

        Ok(())
    }

    #[signal(name = "signame")]
    fn drain_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {}
}

#[workflow]
#[derive(Default)]
struct PollerLoadSpikeThenSustainedWf;

#[workflow_methods]
impl PollerLoadSpikeThenSustainedWf {
    #[run(name = "poller_load")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for _ in 0..5 {
            let _ = ctx
                .start_activity(
                    JitteryEchoActivities::echo,
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
async fn poller_load_spiky() {
    const SIGNAME: &str = "signame";
    let num_workflows = 1000;
    let wf_name = "poller_load";
    let (telemopts, addr, _aborter) =
        if std::env::var("PAR_JOBNUM").unwrap_or("1".to_string()) == "1" {
            prom_metrics(Some(
                PrometheusExporterOptions::builder()
                    .socket_addr(SocketAddr::V4("0.0.0.0:9999".parse().unwrap()))
                    .build(),
            ))
        } else {
            prom_metrics(None)
        };
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("poller_load", rt);
    starter.sdk_config.max_cached_workflows = 5000;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(1000, 1000, 100, 100));
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
    let mut worker = starter.worker().await;
    let submitter = worker.get_submitter_handle();

    worker.register_activities(JitteryEchoActivities);
    worker.register_workflow::<PollerLoadSpikyWf>().unwrap();
    let client = starter.get_client().await;
    let tq = starter.get_task_queue().to_owned();

    info!("Prom bound to {:?}", addr);

    let mut workflow_handles = vec![];
    info!("Starting workflows...");
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}-{}", rand_6_chars());
        let rid = worker
            .submit_wf(
                wf_name.to_owned(),
                vec![],
                WorkflowStartOptions::new(tq.clone(), wfid.clone())
                    .execution_timeout(Duration::from_secs(120))
                    .build(),
            )
            .await
            .unwrap();
        workflow_handles.push(
            WorkflowExecutionInfo {
                namespace: client.namespace(),
                workflow_id: wfid,
                run_id: Some(rid),
                first_execution_run_id: None,
            }
            .bind_untyped(client.clone()),
        );
    }
    info!("Done starting workflows");
    let start_processing = Instant::now();

    let (ah, abort_reg) = AbortHandle::new_pair();
    let all_workflows_are_done = async {
        stream::iter(mem::take(&mut workflow_handles))
            .for_each_concurrent(25, |handle| async move {
                let _ = handle.get_result(WorkflowGetResultOptions::default()).await;
            })
            .await;
        info!("Initial load ran for {:?}", start_processing.elapsed());
        ah.abort();
        // Wait a minute for poller count to drop
        tokio::time::sleep(Duration::from_secs(60)).await;
        // round two
        let start_processing = Instant::now();
        for i in 0..500 {
            let wfid = format!("{wf_name}_2_{i}-{}", rand_6_chars());
            let rid = submitter
                .submit_wf(
                    wf_name.to_owned(),
                    vec![],
                    WorkflowStartOptions::new(tq.clone(), wfid.clone())
                        .execution_timeout(Duration::from_secs(120))
                        .build(),
                )
                .await
                .unwrap();
            workflow_handles.push(
                WorkflowExecutionInfo {
                    namespace: client.namespace(),
                    workflow_id: wfid,
                    run_id: Some(rid),
                    first_execution_run_id: None,
                }
                .bind_untyped(client.clone()),
            );
        }
        stream::iter(workflow_handles)
            .for_each_concurrent(25, |handle| async move {
                let _ = handle.get_result(WorkflowGetResultOptions::default()).await;
            })
            .await;
        info!("Second load ran for {:?}", start_processing.elapsed());

        info!("Done. Go look at yer metrics, must interrupt to end.");
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

#[tokio::test]
async fn poller_load_sustained() {
    let num_workflows = 150;
    let wf_name = "poller_load";
    let (telemopts, addr, _aborter) =
        if std::env::var("PAR_JOBNUM").unwrap_or("1".to_string()) == "1" {
            prom_metrics(Some(
                PrometheusExporterOptions::builder()
                    .socket_addr(SocketAddr::V4("0.0.0.0:9999".parse().unwrap()))
                    .build(),
            ))
        } else {
            prom_metrics(None)
        };
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("poller_load", rt);
    starter.sdk_config.max_cached_workflows = 5000;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(1000, 100, 100, 100));
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 200,
        initial: 5,
    };
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<PollerLoadSustainedWf>().unwrap();
    let client = starter.get_client().await;
    let tq = starter.get_task_queue().to_owned();

    info!("Prom bound to {:?}", addr);

    let mut workflow_handles = vec![];
    info!("Starting workflows...");
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}-{}", rand_6_chars());
        let rid = worker
            .submit_wf(
                wf_name.to_owned(),
                vec![],
                WorkflowStartOptions::new(tq.clone(), wfid.clone())
                    .execution_timeout(Duration::from_secs(800))
                    .build(),
            )
            .await
            .unwrap();
        workflow_handles.push(
            WorkflowExecutionInfo {
                namespace: client.namespace(),
                workflow_id: wfid,
                run_id: Some(rid),
                first_execution_run_id: None,
            }
            .bind_untyped(client.clone()),
        );
    }
    info!("Done starting workflows");
    let start_processing = Instant::now();

    let all_workflows_are_done = async {
        stream::iter(mem::take(&mut workflow_handles))
            .for_each_concurrent(25, |handle| async move {
                let _ = handle.get_result(WorkflowGetResultOptions::default()).await;
            })
            .await;
        info!("Initial load ran for {:?}", start_processing.elapsed());
        info!("Done. Go look at yer metrics, must interrupt to end.");
    };

    let (runres, _) = tokio::join!(worker.inner_mut().run(), all_workflows_are_done);
    runres.unwrap();
}

// Here we want to see pollers scale up a lot to handle the spike, then scale down, ideally avoiding
// overshooting, and definitely avoiding overshooting and staying there.
#[tokio::test]
async fn poller_load_spike_then_sustained() {
    const SIGNAME: &str = "signame";
    let num_workflows = 1000;
    let wf_name = "poller_load";
    let (telemopts, addr, _aborter) =
        if std::env::var("PAR_JOBNUM").unwrap_or("1".to_string()) == "1" {
            prom_metrics(Some(
                PrometheusExporterOptions::builder()
                    .socket_addr(SocketAddr::V4("0.0.0.0:9999".parse().unwrap()))
                    .build(),
            ))
        } else {
            prom_metrics(None)
        };
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("poller_load", rt);
    starter.sdk_config.max_cached_workflows = 5000;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(1000, 100, 100, 100));
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
    let mut worker = starter.worker().await;
    let submitter = worker.get_submitter_handle();

    worker.register_activities(JitteryEchoActivities);
    worker
        .register_workflow::<PollerLoadSpikeThenSustainedWf>()
        .unwrap();
    let client = starter.get_client().await;
    let tq = starter.get_task_queue().to_owned();

    info!("Prom bound to {:?}", addr);

    let mut workflow_handles = vec![];
    info!("Starting workflows...");
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}-{}", rand_6_chars());
        let rid = worker
            .submit_wf(
                wf_name.to_owned(),
                vec![],
                WorkflowStartOptions::new(tq.clone(), wfid.clone())
                    .execution_timeout(Duration::from_secs(100))
                    .build(),
            )
            .await
            .unwrap();
        workflow_handles.push(
            WorkflowExecutionInfo {
                namespace: client.namespace(),
                workflow_id: wfid,
                run_id: Some(rid),
                first_execution_run_id: None,
            }
            .bind_untyped(client.clone()),
        );
    }
    info!("Done starting workflows");
    let start_processing = Instant::now();

    let (ah, abort_reg) = AbortHandle::new_pair();
    let all_workflows_are_done = async {
        stream::iter(mem::take(&mut workflow_handles))
            .for_each_concurrent(25, |handle| async move {
                let _ = handle.get_result(WorkflowGetResultOptions::default()).await;
            })
            .await;
        info!("Initial load ran for {:?}", start_processing.elapsed());
        ah.abort();
        info!("beginning sustained load");
        // round two
        let start_processing = Instant::now();
        for i in 0..100 {
            let wfid = format!("{wf_name}_2_{i}-{}", rand_6_chars());
            let rid = submitter
                .submit_wf(
                    wf_name.to_owned(),
                    vec![],
                    WorkflowStartOptions::new(tq.clone(), wfid.clone())
                        .execution_timeout(Duration::from_secs(100))
                        .build(),
                )
                .await
                .unwrap();
            workflow_handles.push(
                WorkflowExecutionInfo {
                    namespace: client.namespace(),
                    workflow_id: wfid,
                    run_id: Some(rid),
                    first_execution_run_id: None,
                }
                .bind_untyped(client.clone()),
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        stream::iter(workflow_handles)
            .for_each_concurrent(25, |handle| async move {
                let _ = handle.get_result(WorkflowGetResultOptions::default()).await;
            })
            .await;
        info!("Second load ran for {:?}", start_processing.elapsed());

        info!("Done. Go look at yer metrics, must interrupt to end.");
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
