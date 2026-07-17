use crate::common::{
    ANY_PORT, CoreWfStarter, activity_functions::StdActivities, eventually, get_integ_telem_options,
};
use anyhow::anyhow;
use crossbeam_utils::atomic::AtomicCell;
use prost_types::{Duration as PbDuration, Timestamp};
use std::{
    collections::HashSet,
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_client::{
    Client, NamespacedClient, WorkflowExecutionInfo, WorkflowSignalOptions, WorkflowStartOptions,
    grpc::WorkflowService,
};
use temporalio_common::{
    protos::{
        coresdk::AsJsonPayloadExt,
        temporal::api::{
            common::v1::RetryPolicy,
            enums::v1::WorkerStatus,
            worker::v1::{PluginInfo, StorageDriverInfo, WorkerHeartbeat},
            workflowservice::v1::{DescribeWorkerRequest, ListWorkersRequest},
        },
    },
    telemetry::{
        OtelCollectorOptions, PrometheusExporterOptions, TelemetryOptions,
        build_otlp_metric_exporter, start_prometheus_metric_exporter,
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, ResourceBasedTuner, ResourceSlotOptions, RuntimeOptions,
    TunerHolder, prost_dur,
};
use tokio::{sync::Notify, time::sleep};
use tonic::IntoRequest;
use url::Url;

fn within_two_minutes_ts(ts: Timestamp) -> bool {
    let ts_time = UNIX_EPOCH + Duration::new(ts.seconds as u64, ts.nanos as u32);

    let now = SystemTime::now();
    // ts should be at most 2 minutes before the current time
    now.duration_since(ts_time).unwrap() <= Duration::from_secs(2 * 60)
}

fn within_duration(dur: PbDuration, threshold: Duration) -> bool {
    let std_dur = Duration::new(dur.seconds as u64, dur.nanos as u32);
    std_dur <= threshold
}

fn new_no_metrics_starter(wf_name: &str) -> CoreWfStarter {
    let runtimeopts = RuntimeOptions::builder()
        .telemetry_options(TelemetryOptions::builder().build())
        .heartbeat_interval(Some(Duration::from_secs(1)))
        .build()
        .unwrap();
    CoreWfStarter::new_with_runtime(wf_name, CoreRuntime::new_assume_tokio(runtimeopts).unwrap())
}

fn to_system_time(ts: Timestamp) -> SystemTime {
    UNIX_EPOCH + Duration::new(ts.seconds as u64, ts.nanos as u32)
}

#[allow(deprecated)]
async fn list_worker_heartbeats(client: &Client, query: impl Into<String>) -> Vec<WorkerHeartbeat> {
    let mut raw_client = client.clone();
    let response = WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 200,
            next_page_token: Vec::new(),
            query: query.into(),
            include_system_workers: false,
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();
    #[allow(deprecated)]
    let workers_info = response.workers_info;
    workers_info
        .into_iter()
        .filter_map(|info| info.worker_heartbeat)
        .collect()
}

// Tests that rely on Prometheus running in a docker container need to start
// with `docker_` and set the `DOCKER_PROMETHEUS_RUNNING` env variable to run
#[rstest::rstest]
#[tokio::test]
async fn docker_worker_heartbeat_basic(#[values("otel", "prom", "no_metrics")] backing: &str) {
    if env::var("DOCKER_PROMETHEUS_RUNNING").is_err() {
        return;
    }
    let telemopts = if backing == "no_metrics" {
        TelemetryOptions::builder().build()
    } else {
        get_integ_telem_options()
    };
    let runtimeopts = RuntimeOptions::builder()
        .telemetry_options(telemopts)
        .heartbeat_interval(Some(Duration::from_secs(1)))
        .build()
        .unwrap();
    let mut rt = CoreRuntime::new_assume_tokio(runtimeopts).unwrap();
    match backing {
        "otel" => {
            let url = Some("grpc://localhost:4317")
                .map(|x| x.parse::<Url>().unwrap())
                .unwrap();
            let opts_build = OtelCollectorOptions::builder();
            let opts = opts_build.url(url).build();
            rt.telemetry_mut()
                .attach_late_init_metrics(Arc::new(build_otlp_metric_exporter(opts).unwrap()));
        }
        "prom" => {
            let opts_build =
                PrometheusExporterOptions::builder().socket_addr(ANY_PORT.parse().unwrap());
            let opts = opts_build.build();
            rt.telemetry_mut()
                .attach_late_init_metrics(start_prometheus_metric_exporter(opts).unwrap().meter);
        }
        "no_metrics" => {}
        _ => unreachable!(),
    }
    let wf_name = format!("worker_heartbeat_basic_{backing}");
    let mut starter = CoreWfStarter::new_with_runtime(&wf_name, rt);
    starter.sdk_config.max_cached_workflows = 5_usize;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(5, 5, 100, 0));
    starter.set_core_cfg_mutator(|c| {
        c.plugins = vec![
            PluginInfo {
                name: "plugin1".to_string(),
                version: "1".to_string(),
            },
            PluginInfo {
                name: "plugin2".to_string(),
                version: "2".to_string(),
            },
        ]
        .into_iter()
        .collect();
        c.storage_drivers = vec![
            StorageDriverInfo {
                r#type: "driver1".to_string(),
            },
            StorageDriverInfo {
                r#type: "driver2".to_string(),
            },
        ]
        .into_iter()
        .collect();
    });
    let acts_started = Arc::new(Notify::new());
    let acts_done = Arc::new(Notify::new());

    struct NotifyActivities {
        acts_started: Arc<Notify>,
        acts_done: Arc<Notify>,
    }
    #[activities]
    impl NotifyActivities {
        #[activity]
        async fn pass_fail_act(
            self: Arc<Self>,
            _ctx: ActivityContext,
            i: String,
        ) -> Result<String, ActivityError> {
            self.acts_started.notify_one();
            self.acts_done.notified().await;
            Ok(i)
        }
    }

    starter.sdk_config.register_activities(NotifyActivities {
        acts_started: acts_started.clone(),
        acts_done: acts_done.clone(),
    });
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    #[workflow]
    #[derive(Default)]
    struct HeartbeatBasicWf;

    #[workflow_methods]
    impl HeartbeatBasicWf {
        #[run]
        #[allow(dead_code)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let _ = ctx
                .start_activity(
                    NotifyActivities::pass_fail_act,
                    "pass".to_string(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                )
                .await;
            Ok(())
        }
    }

    worker.register_workflow::<HeartbeatBasicWf>().unwrap();

    starter
        .start_with_worker(HeartbeatBasicWf::name(), &mut worker)
        .await;

    let start_time = AtomicCell::new(None);
    let heartbeat_time = AtomicCell::new(None);

    let test_fut = async {
        // Give enough time to ensure heartbeat interval has been hit
        tokio::time::sleep(Duration::from_millis(1500)).await;
        acts_started.notified().await;
        let client = starter.get_client().await;
        let mut raw_client = client.clone();
        let workers_list = WorkflowService::list_workers(
            &mut raw_client,
            ListWorkersRequest {
                namespace: client.namespace().to_owned(),
                page_size: 100,
                next_page_token: Vec::new(),
                query: String::new(),
                include_system_workers: false,
            }
            .into_request(),
        )
        .await
        .unwrap()
        .into_inner();
        #[allow(deprecated)]
        let worker_info = workers_list
            .workers_info
            .iter()
            .find(|worker_info| {
                if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
                    hb.worker_instance_key == worker_instance_key.to_string()
                } else {
                    false
                }
            })
            .unwrap();
        let heartbeat = worker_info.worker_heartbeat.as_ref().unwrap();
        assert_eq!(
            heartbeat.worker_instance_key,
            worker_instance_key.to_string()
        );
        in_activity_checks(heartbeat, &start_time, &heartbeat_time);
        acts_done.notify_one();

        // Poll until the heartbeat reflects shutdown with the second WFT processed.
        // The worker stays alive (join! waits for both futures) so heartbeats keep firing.
        eventually(
            || {
                let mut rc = raw_client.clone();
                let ns = client.namespace().to_owned();
                async move {
                    let workers_list = WorkflowService::list_workers(
                        &mut rc,
                        ListWorkersRequest {
                            namespace: ns,
                            page_size: 100,
                            next_page_token: Vec::new(),
                            query: String::new(),
                            include_system_workers: false,
                        }
                        .into_request(),
                    )
                    .await
                    .unwrap()
                    .into_inner();
                    #[allow(deprecated)]
                    let hb = workers_list
                        .workers_info
                        .iter()
                        .find_map(|wi| {
                            wi.worker_heartbeat.as_ref().filter(|hb| {
                                hb.worker_instance_key == worker_instance_key.to_string()
                            })
                        })
                        .unwrap()
                        .clone();
                    let tasks_done = hb
                        .workflow_task_slots_info
                        .as_ref()
                        .is_some_and(|s| s.total_processed_tasks >= 2);
                    let is_shutting_down = hb.status == WorkerStatus::ShuttingDown as i32;
                    if tasks_done && is_shutting_down {
                        Ok(hb)
                    } else {
                        Err(anyhow::anyhow!(
                            "Heartbeat not ready: tasks={}, shutting_down={}",
                            hb.workflow_task_slots_info
                                .as_ref()
                                .map_or(0, |s| s.total_processed_tasks),
                            is_shutting_down,
                        ))
                    }
                }
            },
            Duration::from_secs(5),
        )
        .await
        .map(|hb| after_shutdown_checks(&hb, &wf_name, &start_time, &heartbeat_time))
        .unwrap();
    };

    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(test_fut, runner);
}

// Tests that rely on Prometheus running in a docker container need to start
// with `docker_` and set the `DOCKER_PROMETHEUS_RUNNING` env variable to run
#[tokio::test]
async fn docker_worker_heartbeat_tuner() {
    if env::var("DOCKER_PROMETHEUS_RUNNING").is_err() {
        return;
    }
    let runtimeopts = RuntimeOptions::builder()
        .telemetry_options(get_integ_telem_options())
        .heartbeat_interval(Some(Duration::from_secs(1)))
        .build()
        .unwrap();
    let mut rt = CoreRuntime::new_assume_tokio(runtimeopts).unwrap();

    let url = Some("grpc://localhost:4317")
        .map(|x| x.parse::<Url>().unwrap())
        .unwrap();
    let opts_build = OtelCollectorOptions::builder();
    let opts = opts_build.url(url).build();

    rt.telemetry_mut()
        .attach_late_init_metrics(Arc::new(build_otlp_metric_exporter(opts).unwrap()));
    let wf_name = "worker_heartbeat_tuner";
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    let mut tuner = ResourceBasedTuner::new(0.0, 0.0);
    tuner
        .with_workflow_slots_options(ResourceSlotOptions::new(2, 10, Duration::from_millis(0)))
        .with_activity_slots_options(ResourceSlotOptions::new(5, 10, Duration::from_millis(50)));
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 200,
        initial: 5,
    };
    starter.sdk_config.nexus_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 200,
        initial: 5,
    };
    starter.sdk_config.tuner = Arc::new(tuner);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    #[workflow]
    #[derive(Default)]
    struct HeartbeatTunerWf;

    #[workflow_methods]
    impl HeartbeatTunerWf {
        #[run]
        #[allow(dead_code)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let _ = ctx
                .start_activity(
                    StdActivities::echo,
                    "pass".to_string(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(1)),
                )
                .await;
            Ok(())
        }
    }

    worker.register_workflow::<HeartbeatTunerWf>().unwrap();

    starter
        .start_with_worker(HeartbeatTunerWf::name(), &mut worker)
        .await;
    worker.run_until_done().await.unwrap();

    let client = starter.get_client().await;
    let mut raw_client = client.clone();
    let workers_list = WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: String::new(),
            include_system_workers: false,
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();
    // Since list_workers finds all workers in the namespace, must find specific worker used in this
    // test
    #[allow(deprecated)]
    let worker_info = workers_list
        .workers_info
        .iter()
        .find(|worker_info| {
            if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
                hb.worker_instance_key == worker_instance_key.to_string()
            } else {
                false
            }
        })
        .unwrap();
    let heartbeat = worker_info.worker_heartbeat.as_ref().unwrap();
    assert!(heartbeat.task_queue.starts_with(wf_name));

    assert_eq!(
        heartbeat
            .workflow_task_slots_info
            .clone()
            .unwrap()
            .slot_supplier_kind,
        "ResourceBased"
    );
    assert_eq!(
        heartbeat
            .activity_task_slots_info
            .clone()
            .unwrap()
            .slot_supplier_kind,
        "ResourceBased"
    );
    assert_eq!(
        heartbeat
            .nexus_task_slots_info
            .clone()
            .unwrap()
            .slot_supplier_kind,
        "ResourceBased"
    );
    assert_eq!(
        heartbeat
            .local_activity_slots_info
            .clone()
            .unwrap()
            .slot_supplier_kind,
        "ResourceBased"
    );

    let workflow_poller_info = heartbeat.workflow_poller_info.unwrap();
    assert!(workflow_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        workflow_poller_info.last_successful_poll_time.unwrap()
    ));
    let sticky_poller_info = heartbeat.workflow_sticky_poller_info.unwrap();
    assert!(sticky_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        sticky_poller_info.last_successful_poll_time.unwrap()
    ));
    let nexus_poller_info = heartbeat.nexus_poller_info.unwrap();
    assert!(nexus_poller_info.is_autoscaling);
    assert!(nexus_poller_info.last_successful_poll_time.is_none());
    let activity_poller_info = heartbeat.activity_poller_info.unwrap();
    assert!(!activity_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        activity_poller_info.last_successful_poll_time.unwrap()
    ));
}

fn in_activity_checks(
    heartbeat: &WorkerHeartbeat,
    start_time: &AtomicCell<Option<Timestamp>>,
    heartbeat_time: &AtomicCell<Option<Timestamp>>,
) {
    assert_eq!(heartbeat.status, WorkerStatus::Running as i32);

    let workflow_task_slots = heartbeat.workflow_task_slots_info.clone().unwrap();
    assert_eq!(workflow_task_slots.total_processed_tasks, 1);
    assert_eq!(workflow_task_slots.current_available_slots, 5);
    assert_eq!(workflow_task_slots.current_used_slots, 0);
    assert_eq!(workflow_task_slots.slot_supplier_kind, "Fixed");
    let activity_task_slots = heartbeat.activity_task_slots_info.clone().unwrap();
    assert_eq!(activity_task_slots.current_available_slots, 4);
    assert_eq!(activity_task_slots.current_used_slots, 1);
    assert_eq!(activity_task_slots.slot_supplier_kind, "Fixed");
    let nexus_task_slots = heartbeat.nexus_task_slots_info.clone().unwrap();
    assert_eq!(nexus_task_slots.current_available_slots, 0);
    assert_eq!(nexus_task_slots.current_used_slots, 0);
    assert_eq!(nexus_task_slots.slot_supplier_kind, "Fixed");
    let local_activity_task_slots = heartbeat.local_activity_slots_info.clone().unwrap();
    assert_eq!(local_activity_task_slots.current_available_slots, 100);
    assert_eq!(local_activity_task_slots.current_used_slots, 0);
    assert_eq!(local_activity_task_slots.slot_supplier_kind, "Fixed");

    let workflow_poller_info = heartbeat.workflow_poller_info.unwrap();
    assert_eq!(workflow_poller_info.current_pollers, 1);
    let sticky_poller_info = heartbeat.workflow_sticky_poller_info.unwrap();
    assert_ne!(sticky_poller_info.current_pollers, 0);
    let nexus_poller_info = heartbeat.nexus_poller_info.unwrap();
    assert_eq!(nexus_poller_info.current_pollers, 0);
    let activity_poller_info = heartbeat.activity_poller_info.unwrap();
    assert_ne!(activity_poller_info.current_pollers, 0);
    assert_ne!(heartbeat.current_sticky_cache_size, 0);
    start_time.store(Some(heartbeat.start_time.unwrap()));
    heartbeat_time.store(Some(heartbeat.heartbeat_time.unwrap()));
}

fn after_shutdown_checks(
    heartbeat: &WorkerHeartbeat,
    wf_name: &str,
    start_time: &AtomicCell<Option<Timestamp>>,
    heartbeat_time: &AtomicCell<Option<Timestamp>>,
) {
    assert_eq!(heartbeat.worker_identity, "integ_tester");
    let host_info = heartbeat.host_info.clone().unwrap();
    assert!(!host_info.host_name.is_empty());
    assert!(!host_info.worker_grouping_key.is_empty());
    assert!(!host_info.process_id.is_empty());
    assert!(host_info.current_host_cpu_usage >= 0.0);
    assert!(host_info.current_host_mem_usage >= 0.0);

    assert!(heartbeat.task_queue.starts_with(wf_name));
    assert_eq!(
        heartbeat.deployment_version.clone().unwrap().build_id,
        "test_build_id"
    );
    assert_eq!(heartbeat.sdk_name, "temporal-core");
    assert_eq!(heartbeat.sdk_version, "0.1.0");
    assert_eq!(heartbeat.status, WorkerStatus::ShuttingDown as i32);

    assert_eq!(start_time.load().unwrap(), heartbeat.start_time.unwrap());
    assert_ne!(
        heartbeat_time.load().unwrap(),
        heartbeat.heartbeat_time.unwrap()
    );
    assert!(within_two_minutes_ts(heartbeat.start_time.unwrap()));
    assert!(within_two_minutes_ts(heartbeat.heartbeat_time.unwrap()));
    assert!(
        to_system_time(heartbeat_time.load().unwrap())
            < to_system_time(heartbeat.heartbeat_time.unwrap())
    );
    assert!(within_duration(
        heartbeat.elapsed_since_last_heartbeat.unwrap(),
        Duration::from_millis(2000)
    ));

    let workflow_task_slots = heartbeat.workflow_task_slots_info.clone().unwrap();
    assert_eq!(workflow_task_slots.total_processed_tasks, 2);

    let workflow_poller_info = heartbeat.workflow_poller_info.unwrap();
    assert!(!workflow_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        workflow_poller_info.last_successful_poll_time.unwrap()
    ));
    let sticky_poller_info = heartbeat.workflow_sticky_poller_info.unwrap();
    assert!(!sticky_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        sticky_poller_info.last_successful_poll_time.unwrap()
    ));
    let nexus_poller_info = heartbeat.nexus_poller_info.unwrap();
    assert!(!nexus_poller_info.is_autoscaling);
    assert!(nexus_poller_info.last_successful_poll_time.is_none());
    let activity_poller_info = heartbeat.activity_poller_info.unwrap();
    assert!(!activity_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        activity_poller_info.last_successful_poll_time.unwrap()
    ));

    assert_eq!(heartbeat.total_sticky_cache_hit, 1);
    assert_eq!(
        heartbeat.plugins,
        vec![
            PluginInfo {
                name: "plugin1".to_string(),
                version: "1".to_string()
            },
            PluginInfo {
                name: "plugin2".to_string(),
                version: "2".to_string()
            }
        ]
    );
    assert_eq!(
        heartbeat.drivers,
        vec![
            StorageDriverInfo {
                r#type: "driver1".to_string()
            },
            StorageDriverInfo {
                r#type: "driver2".to_string()
            }
        ]
    );
}

static HISTORY_WF1_ACTIVITY_STARTED: Notify = Notify::const_new();
static HISTORY_WF1_ACTIVITY_FINISH: Notify = Notify::const_new();
static HISTORY_WF2_ACTIVITY_STARTED: Notify = Notify::const_new();
static HISTORY_WF2_ACTIVITY_FINISH: Notify = Notify::const_new();
#[tokio::test]
async fn worker_heartbeat_sticky_cache_miss() {
    let wf_name = "worker_heartbeat_cache_miss";
    let mut starter = new_no_metrics_starter(wf_name);
    starter.sdk_config.max_cached_workflows = 1_usize;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(2, 10, 10, 10));

    struct StickyCacheActivities;
    #[activities]
    impl StickyCacheActivities {
        #[activity]
        async fn sticky_cache_history_act(
            _ctx: ActivityContext,
            marker: String,
        ) -> Result<String, ActivityError> {
            match marker.as_str() {
                "wf1" => {
                    HISTORY_WF1_ACTIVITY_STARTED.notify_one();
                    HISTORY_WF1_ACTIVITY_FINISH.notified().await;
                }
                "wf2" => {
                    HISTORY_WF2_ACTIVITY_STARTED.notify_one();
                    HISTORY_WF2_ACTIVITY_FINISH.notified().await;
                }
                _ => {}
            }
            Ok(marker)
        }
    }

    starter
        .sdk_config
        .register_activities(StickyCacheActivities);

    let mut worker = starter.worker().await;
    worker.fetch_results = false;
    let worker_key = worker.worker_instance_key().to_string();
    let worker_core = worker.core_worker();
    let submitter = worker.get_submitter_handle();
    let wf_opts = starter.workflow_options.clone();
    let client = starter.get_client().await;
    let client_for_orchestrator = client.clone();

    #[workflow]
    #[derive(Default)]
    struct StickyCacheMissWf;

    #[workflow_methods]
    impl StickyCacheMissWf {
        #[run]
        #[allow(dead_code)]
        async fn run(ctx: &mut WorkflowContext<Self>, wf_marker: String) -> WorkflowResult<()> {
            let _ = ctx
                .start_activity(
                    StickyCacheActivities::sticky_cache_history_act,
                    wf_marker.clone(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                )
                .await;

            Ok(())
        }
    }

    worker.register_workflow::<StickyCacheMissWf>().unwrap();

    let wf1_id = format!("{wf_name}_wf1");
    let wf2_id = format!("{wf_name}_wf2");

    let orchestrator = async move {
        let mut opts = wf_opts.clone();
        opts.workflow_id = wf1_id.clone();
        let wf1_run = submitter
            .submit_wf(
                StickyCacheMissWf::name(),
                vec!["wf1".to_string().as_json_payload().unwrap()],
                opts,
            )
            .await
            .unwrap();

        HISTORY_WF1_ACTIVITY_STARTED.notified().await;

        let mut opts = wf_opts.clone();
        opts.workflow_id = wf2_id.clone();
        let wf2_run = submitter
            .submit_wf(
                StickyCacheMissWf::name(),
                vec!["wf2".to_string().as_json_payload().unwrap()],
                opts,
            )
            .await
            .unwrap();

        HISTORY_WF2_ACTIVITY_STARTED.notified().await;

        HISTORY_WF1_ACTIVITY_FINISH.notify_one();
        let handle1 = WorkflowExecutionInfo {
            namespace: client_for_orchestrator.namespace(),
            workflow_id: wf1_id,
            run_id: Some(wf1_run),
            first_execution_run_id: None,
        }
        .bind_untyped(client_for_orchestrator.clone());
        handle1
            .get_result(Default::default())
            .await
            .expect("wf1 result");

        HISTORY_WF2_ACTIVITY_FINISH.notify_one();
        let handle2 = WorkflowExecutionInfo {
            namespace: client_for_orchestrator.namespace(),
            workflow_id: wf2_id,
            run_id: Some(wf2_run),
            first_execution_run_id: None,
        }
        .bind_untyped(client_for_orchestrator.clone());
        handle2
            .get_result(Default::default())
            .await
            .expect("wf2 result");

        worker_core.initiate_shutdown();
    };

    let mut worker_runner = worker;
    let runner = async move {
        worker_runner.run_until_done().await.unwrap();
    };

    tokio::join!(orchestrator, runner);

    sleep(Duration::from_secs(2)).await;
    let mut heartbeats =
        list_worker_heartbeats(&client, format!("WorkerInstanceKey=\"{worker_key}\"")).await;
    assert_eq!(heartbeats.len(), 1);
    let heartbeat = heartbeats.pop().unwrap();

    assert!(heartbeat.total_sticky_cache_miss >= 1);
    assert_eq!(heartbeat.worker_instance_key, worker_key);
}

#[tokio::test]
async fn worker_heartbeat_multiple_workers() {
    let wf_name = "worker_heartbeat_multi_workers";
    let mut starter = new_no_metrics_starter(wf_name);
    starter.sdk_config.max_cached_workflows = 5_usize;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(5, 10, 10, 10));
    starter.sdk_config.register_activities(StdActivities);

    let client = starter.get_client().await;
    let starting_hb_len = list_worker_heartbeats(&client, String::new()).await.len();

    #[workflow]
    #[derive(Default)]
    struct MultiWorkersWf;

    #[workflow_methods]
    impl MultiWorkersWf {
        #[run]
        #[allow(dead_code)]
        async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            Ok(())
        }
    }

    let mut worker_a = starter.worker().await;
    worker_a.register_workflow::<MultiWorkersWf>().unwrap();

    let mut starter_b = starter.clone_no_worker();
    let mut worker_b = starter_b.worker().await;
    worker_b.register_workflow::<MultiWorkersWf>().unwrap();

    let worker_a_key = worker_a.worker_instance_key().to_string();
    let worker_b_key = worker_b.worker_instance_key().to_string();
    let _ = starter
        .start_with_worker(MultiWorkersWf::name(), &mut worker_a)
        .await;
    worker_a.run_until_done().await.unwrap();

    let _ = starter_b
        .start_with_worker(MultiWorkersWf::name(), &mut worker_b)
        .await;
    worker_b.run_until_done().await.unwrap();

    sleep(Duration::from_secs(2)).await;

    let all = list_worker_heartbeats(&client, String::new()).await;
    let keys: HashSet<_> = all
        .iter()
        .map(|hb| hb.worker_instance_key.clone())
        .collect();
    assert!(keys.contains(&worker_a_key));
    assert!(keys.contains(&worker_b_key));

    // Verify both heartbeats contain the same shared worker_grouping_key
    let worker_grouping_keys: HashSet<_> = all
        .iter()
        .filter_map(|hb| {
            hb.host_info
                .as_ref()
                .map(|info| info.worker_grouping_key.clone())
        })
        .collect();
    assert!(worker_grouping_keys.len() > starting_hb_len);

    let filtered =
        list_worker_heartbeats(&client, format!("WorkerInstanceKey=\"{worker_a_key}\"")).await;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].worker_instance_key, worker_a_key);

    // Verify describe worker gives the same heartbeat as listworker
    let mut raw_client = client.clone();
    let describe_worker_a = WorkflowService::describe_worker(
        &mut raw_client,
        DescribeWorkerRequest {
            namespace: client.namespace().to_owned(),
            worker_instance_key: worker_a_key.to_string(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner()
    .worker_info
    .unwrap()
    .worker_heartbeat
    .unwrap();
    assert_eq!(describe_worker_a, filtered[0]);

    let filtered_b =
        list_worker_heartbeats(&client, format!("WorkerInstanceKey = \"{worker_b_key}\"")).await;
    assert_eq!(filtered_b.len(), 1);
    assert_eq!(filtered_b[0].worker_instance_key, worker_b_key);
    let describe_worker_b = WorkflowService::describe_worker(
        &mut raw_client,
        DescribeWorkerRequest {
            namespace: client.namespace().to_owned(),
            worker_instance_key: worker_b_key.to_string(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner()
    .worker_info
    .unwrap()
    .worker_heartbeat
    .unwrap();
    assert_eq!(describe_worker_b, filtered_b[0]);
}

static ACT_COUNT: AtomicU64 = AtomicU64::new(0);
static WF_COUNT: AtomicU64 = AtomicU64::new(0);
static ACT_FAIL: Notify = Notify::const_new();
static WF_FAIL: Notify = Notify::const_new();
#[tokio::test]
async fn worker_heartbeat_failure_metrics() {
    let wf_name = "worker_heartbeat_failure_metrics";
    let mut starter = new_no_metrics_starter(wf_name);
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(10, 5, 10, 10));
    // This test uses tokio::sync::Notify from workflow code for test coordination.
    starter.sdk_config.detect_nondeterministic_futures = false;

    struct FailingActivities;
    #[activities]
    impl FailingActivities {
        #[activity]
        async fn failing_act(_ctx: ActivityContext, _: String) -> Result<(), ActivityError> {
            if ACT_COUNT.load(Ordering::Relaxed) == 3 {
                return Ok(());
            }
            ACT_COUNT.fetch_add(1, Ordering::Relaxed);
            ACT_FAIL.notify_one();
            Err(anyhow!("Expected error").into())
        }
    }

    starter.sdk_config.register_activities(FailingActivities);

    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    #[workflow]
    #[derive(Default)]
    struct FailureMetricsWf {
        signal_received: bool,
    }

    #[workflow_methods]
    impl FailureMetricsWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let _ = ctx
                .start_activity(
                    FailingActivities::failing_act,
                    "boom".to_string(),
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                        .retry_policy(RetryPolicy {
                            initial_interval: Some(prost_dur!(from_millis(10))),
                            backoff_coefficient: 1.0,
                            maximum_attempts: 4,
                            ..Default::default()
                        })
                        .build(),
                )
                .await;

            if WF_COUNT.load(Ordering::Relaxed) == 0 {
                WF_COUNT.fetch_add(1, Ordering::Relaxed);
                WF_FAIL.notify_one();
                panic!("expected WF panic");
            }

            ctx.wait_condition(|s| s.signal_received).await;
            Ok(())
        }

        #[signal]
        fn handle_continue_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {
            self.signal_received = true;
        }
    }

    worker.register_workflow::<FailureMetricsWf>().unwrap();

    let worker_key = worker_instance_key.to_string();
    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = starter.get_wf_id();
    let handle = worker
        .submit_workflow(
            FailureMetricsWf::run,
            (),
            WorkflowStartOptions::new(task_queue, workflow_id)
                .retry_policy(RetryPolicy {
                    maximum_attempts: 2,
                    ..Default::default()
                })
                .build(),
        )
        .await
        .unwrap();

    let query = format!("WorkerInstanceKey=\"{worker_key}\"");
    let test_fut = async {
        ACT_FAIL.notified().await;
        let client = starter.get_client().await;
        eventually(
            || async {
                let heartbeats = list_worker_heartbeats(&client, query.clone()).await;
                let heartbeat = heartbeats
                    .into_iter()
                    .find(|hb| hb.worker_instance_key == worker_key)
                    .ok_or("worker not found in list_workers response")?;
                let activity_slots = heartbeat.activity_task_slots_info.unwrap();
                if activity_slots.last_interval_failure_tasks >= 1 {
                    return Ok(());
                }
                Err("activity_slots.last_interval_failure_tasks still 0, retrying")
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();

        WF_FAIL.notified().await;

        eventually(
            || async {
                let heartbeats = list_worker_heartbeats(&client, query.clone()).await;
                #[allow(deprecated)]
                let heartbeat = heartbeats
                    .into_iter()
                    .find(|hb| hb.worker_instance_key == worker_key)
                    .ok_or("worker not found in list_workers response")?;
                let workflow_slots = heartbeat.workflow_task_slots_info.unwrap();
                if workflow_slots.last_interval_failure_tasks >= 1 {
                    return Ok(());
                }
                Err("workflow_slots.last_interval_failure_tasks still 0, retrying")
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        handle
            .signal(
                FailureMetricsWf::handle_continue_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(test_fut, runner);

    let client = starter.get_client().await;
    let mut heartbeats = list_worker_heartbeats(&client, query).await;
    assert_eq!(heartbeats.len(), 1);
    let heartbeat = heartbeats.pop().unwrap();

    let activity_slots = heartbeat.activity_task_slots_info.unwrap();
    assert_eq!(activity_slots.total_failed_tasks, 3);

    let workflow_slots = heartbeat.workflow_task_slots_info.unwrap();
    assert_eq!(workflow_slots.total_failed_tasks, 1);
}

#[tokio::test]
async fn worker_heartbeat_no_runtime_heartbeat() {
    let wf_name = "worker_heartbeat_no_runtime_heartbeat";
    let runtimeopts = RuntimeOptions::builder()
        .telemetry_options(get_integ_telem_options())
        .heartbeat_interval(None)
        .build()
        .unwrap();
    let rt = CoreRuntime::new_assume_tokio(runtimeopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    #[workflow]
    #[derive(Default)]
    struct NoRuntimeHeartbeatWf;

    #[workflow_methods]
    impl NoRuntimeHeartbeatWf {
        #[run]
        #[allow(dead_code)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let _ = ctx
                .start_activity(
                    StdActivities::echo,
                    "pass".to_string(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(1)),
                )
                .await;
            Ok(())
        }
    }

    worker.register_workflow::<NoRuntimeHeartbeatWf>().unwrap();

    starter
        .start_with_worker(NoRuntimeHeartbeatWf::name(), &mut worker)
        .await;

    worker.run_until_done().await.unwrap();
    let client = starter.get_client().await;
    let mut raw_client = client.clone();
    let workers_list = WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: String::new(),
            include_system_workers: false,
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();

    // Ensure worker has not ever heartbeated
    #[allow(deprecated)]
    let heartbeat = workers_list.workers_info.iter().find(|worker_info| {
        if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
            hb.worker_instance_key == worker_instance_key.to_string()
        } else {
            false
        }
    });
    assert!(heartbeat.is_none());
}

#[tokio::test]
async fn worker_heartbeat_skip_client_worker_set_check() {
    let wf_name = "worker_heartbeat_skip_client_worker_set_check";
    let runtimeopts = RuntimeOptions::builder()
        .telemetry_options(get_integ_telem_options())
        .heartbeat_interval(Some(Duration::from_secs(1)))
        .build()
        .unwrap();
    let rt = CoreRuntime::new_assume_tokio(runtimeopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.set_core_cfg_mutator(|m| m.skip_client_worker_set_check = true);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    #[workflow]
    #[derive(Default)]
    struct SkipClientWorkerSetCheckWf;

    #[workflow_methods]
    impl SkipClientWorkerSetCheckWf {
        #[run]
        #[allow(dead_code)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let _ = ctx
                .start_activity(
                    StdActivities::echo,
                    "pass".to_string(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(1)),
                )
                .await;
            Ok(())
        }
    }

    worker
        .register_workflow::<SkipClientWorkerSetCheckWf>()
        .unwrap();

    starter
        .start_with_worker(SkipClientWorkerSetCheckWf::name(), &mut worker)
        .await;

    worker.run_until_done().await.unwrap();
    let client = starter.get_client().await;
    let mut raw_client = client.clone();
    let workers_list = WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: String::new(),
            include_system_workers: false,
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();

    // Ensure worker still heartbeats
    #[allow(deprecated)]
    let heartbeat = workers_list.workers_info.iter().find(|worker_info| {
        if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
            hb.worker_instance_key == worker_instance_key.to_string()
        } else {
            false
        }
    });
    assert!(heartbeat.is_some());
}
