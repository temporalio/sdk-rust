use crate::{
    common::{
        ANY_PORT, CoreWfStarter, NAMESPACE, OTEL_URL_ENV_VAR, PROMETHEUS_QUERY_API, eventually,
        get_integ_client, get_integ_connection, get_integ_runtime_options,
        get_integ_server_options, get_integ_telem_options, prom_metrics,
    },
    integ_tests::mk_nexus_endpoint,
};
use anyhow::{anyhow, bail};
use assert_matches::assert_matches;
use std::{
    collections::HashMap,
    env,
    string::ToString,
    sync::{Arc, OnceLock},
    time::Duration,
};
use temporalio_client::{
    Connection, NamespacedClient, REQUEST_LATENCY_HISTOGRAM_NAME, UntypedQuery, UntypedWorkflow,
    WorkflowExecutionInfo, WorkflowQueryOptions, WorkflowStartOptions, grpc::WorkflowService,
};
use temporalio_common::{
    data_converters::RawValue,
    protos::{
        coresdk::{
            ActivityTaskCompletion,
            activity_result::ActivityExecutionResult,
            nexus::{NexusTaskCompletion, nexus_task, nexus_task_completion},
            workflow_activation::{WorkflowActivationJob, workflow_activation_job},
            workflow_commands::{
                CancelWorkflowExecution, CompleteWorkflowExecution, ContinueAsNewWorkflowExecution,
                FailWorkflowExecution, QueryResult, QuerySuccess, ScheduleActivity,
                ScheduleLocalActivity, workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::RetryPolicy,
            enums::v1::{
                NexusHandlerErrorRetryBehavior, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
            },
            failure::v1::Failure,
            nexus::{
                self,
                v1::{
                    HandlerError, StartOperationResponse, UnsuccessfulOperationError,
                    request::Variant, start_operation_response,
                },
            },
            workflowservice::v1::{DescribeNamespaceRequest, ListNamespacesRequest},
        },
    },
    telemetry::{
        HistogramBucketOverrides, OtelCollectorOptions, OtlpProtocol, PrometheusExporterOptions,
        TaskQueueLabelStrategy, TelemetryOptions, build_otlp_metric_exporter,
        metrics::{
            CoreMeter, CounterBase, Gauge, GaugeBase, HistogramBase, MetricKeyValue,
            MetricParameters, NewAttributes, UpDownCounterBase,
            WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME,
        },
        start_prometheus_metric_exporter,
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, LocalActivityOptions, NexusOperationOptions,
    WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_core::{
    CoreRuntime, FixedSizeSlotSupplier, PollError, PollerBehavior, SlotKind, SlotMarkUsedContext,
    SlotReleaseContext, SlotReservationContext, SlotSupplier, SlotSupplierPermit,
    TokioRuntimeBuilder, TunerBuilder, WorkerConfig, WorkerVersioningStrategy, WorkflowSlotKind,
    init_worker, prost_dur,
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, TemporalMeter, WorkerExt, WorkerTestHelpers, build_mock_pollers,
        mock_worker, mock_worker_client,
    },
};
use tokio::{join, sync::Barrier};
use tonic::IntoRequest;
use url::Url;

pub(crate) async fn get_text(endpoint: String) -> String {
    reqwest::get(endpoint).await.unwrap().text().await.unwrap()
}

#[rstest::rstest]
#[tokio::test]
async fn prometheus_metrics_exported(
    #[values(true, false)] use_seconds_latency: bool,
    #[values(true, false)] custom_buckets: bool,
) {
    let opts = PrometheusExporterOptions::builder()
        .global_tags(HashMap::from([("global".to_string(), "hi!".to_string())]))
        .socket_addr(ANY_PORT.parse().unwrap())
        .use_seconds_for_durations(use_seconds_latency)
        .histogram_bucket_overrides(if custom_buckets {
            HistogramBucketOverrides {
                overrides: {
                    let mut hm = HashMap::new();
                    hm.insert(REQUEST_LATENCY_HISTOGRAM_NAME.to_string(), vec![1337.0]);
                    hm
                },
            }
        } else {
            Default::default()
        })
        .build();
    let (telemopts, addr, _aborter) = prom_metrics(Some(opts));
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut opts = get_integ_server_options();
    opts.metrics_meter = rt.telemetry().get_temporal_metric_meter();
    let mut connection = Connection::connect(opts).await.unwrap();
    assert!(connection.capabilities().is_some());

    let _ = connection
        .list_namespaces(ListNamespacesRequest::default().into_request())
        .await
        .unwrap();

    let body = get_text(format!("http://{addr}/metrics")).await;
    assert!(body.contains(
        "temporal_request_latency_count{operation=\"ListNamespaces\",service_name=\"temporal-core-sdk\",global=\"hi!\"} 1"
    ));
    assert!(body.contains(
        "temporal_request_latency_count{operation=\"GetSystemInfo\",service_name=\"temporal-core-sdk\",global=\"hi!\"} 1"
    ));
    if custom_buckets {
        assert!(body.contains(
            "temporal_request_latency_bucket{\
             operation=\"GetSystemInfo\",service_name=\"temporal-core-sdk\",global=\"hi!\",le=\"1337\"}"
        ));
    } else if use_seconds_latency {
        assert!(body.contains(
            "temporal_request_latency_bucket{\
             operation=\"GetSystemInfo\",service_name=\"temporal-core-sdk\",global=\"hi!\",le=\"0.05\"}"
        ));
    } else {
        assert!(body.contains(
            "temporal_request_latency_bucket{\
             operation=\"GetSystemInfo\",service_name=\"temporal-core-sdk\",global=\"hi!\",le=\"50\"}"
        ));
    }
    // Verify counter names are appropriate (don't end w/ '_total')
    assert!(body.contains("temporal_request{"));
    // Verify non-temporal metrics meter does not prefix
    let mm = rt.telemetry().get_metric_meter().unwrap();
    let g = mm.gauge(MetricParameters::from("mygauge"));
    let attrs = mm.new_attributes(NewAttributes::new(vec![]));
    g.record(42, &attrs);
    let body = get_text(format!("http://{addr}/metrics")).await;
    assert!(body.contains("\nmygauge{global=\"hi!\"} 42"));
}

#[tokio::test]
async fn one_slot_worker_reports_available_slot() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let tq = "one_slot_worker_tq";
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();

    let worker_cfg = WorkerConfig::builder()
        .namespace(NAMESPACE)
        .task_queue(tq)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "test_build_id".to_owned(),
        })
        .max_cached_workflows(2_usize)
        .max_outstanding_activities(1_usize)
        .max_outstanding_local_activities(1_usize)
        // Need to use two for WFTs because there are a minimum of 2 pollers b/c of sticky polling
        .max_outstanding_workflow_tasks(2_usize)
        .max_outstanding_nexus_tasks(1_usize)
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2_usize))
        .task_types(WorkerTaskTypes::all())
        .build()
        .unwrap();

    let connection = get_integ_connection(None).await;
    let client = Arc::new(get_integ_client(worker_cfg.namespace.clone(), None).await);
    let worker = init_worker(&rt, worker_cfg, connection).expect("Worker inits cleanly");
    let wf_task_barr = Barrier::new(2);
    let act_task_barr = Barrier::new(2);

    let wf_polling = async {
        let task = worker.poll_workflow_activation().await.unwrap();
        wf_task_barr.wait().await;
        wf_task_barr.wait().await;
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                ScheduleActivity {
                    seq: 1,
                    activity_id: "1".to_string(),
                    activity_type: "test_act".to_string(),
                    task_queue: tq.to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                    ..Default::default()
                }
                .into(),
            ))
            .await
            .unwrap();
        wf_task_barr.wait().await;

        let task = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                ScheduleLocalActivity {
                    seq: 2,
                    activity_id: "2".to_string(),
                    activity_type: "test_act".to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                    ..Default::default()
                }
                .into(),
            ))
            .await
            .unwrap();
    };

    let act_polling = async {
        let task = worker.poll_activity_task().await.unwrap();
        act_task_barr.wait().await;
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
        act_task_barr.wait().await;

        let task = worker.poll_activity_task().await.unwrap();
        act_task_barr.wait().await;
        act_task_barr.wait().await;
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
        act_task_barr.wait().await;
    };

    let nexus_polling = async {
        let _ = worker.poll_nexus_task().await;
    };

    let testing = async {
        // Wait just a beat for the poller to initiate
        tokio::time::sleep(Duration::from_millis(50)).await;
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"WorkflowWorker\"}} 2"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"NexusWorker\"}} 1"
        )));

        // Start a workflow so that a task will get delivered
        client
            .start_workflow(
                UntypedWorkflow::new("whatever"),
                RawValue::default(),
                WorkflowStartOptions::new(tq.to_owned(), "one_slot_metric_test".to_owned())
                    .id_conflict_policy(WorkflowIdConflictPolicy::TerminateExisting)
                    .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicate)
                    .execution_timeout(Duration::from_secs(5))
                    .build(),
            )
            .await
            .unwrap();

        wf_task_barr.wait().await;

        // At this point the workflow task is outstanding and the activities haven't started
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"WorkflowWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"WorkflowWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 0"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 0"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"NexusWorker\"}} 0"
        )));

        // Now we allow the complete to proceed. Once it goes through, there should be 2 WFT slot
        // open but 0 activity slots
        wf_task_barr.wait().await;
        wf_task_barr.wait().await;
        // Sometimes the recording takes an extra bit. 🤷
        tokio::time::sleep(Duration::from_millis(100)).await;
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"WorkflowWorker\"}} 2"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 0"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 1"
        )));

        // Now complete the activity and watch it go up
        act_task_barr.wait().await;
        // Wait for completion to finish
        act_task_barr.wait().await;
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 1"
        )));

        // Proceed to local activity command
        act_task_barr.wait().await;
        // Ensure that, once we have the LA task, slots are 0
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 0"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 1"
        )));
        // When completion is done, we have 1 again
        act_task_barr.wait().await;
        act_task_barr.wait().await;
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 1"
        )));
        worker.initiate_shutdown();
    };
    join!(wf_polling, act_polling, nexus_polling, testing);
}

#[rstest::rstest]
#[tokio::test]
async fn query_of_closed_workflow_doesnt_tick_terminal_metric(
    #[values(
        CompleteWorkflowExecution { result: None }.into(),
        FailWorkflowExecution {
            failure: Some(Failure::application_failure("I'm ded".to_string(), false)),
        }.into(),
        ContinueAsNewWorkflowExecution::default().into(),
        CancelWorkflowExecution { }.into()
    )]
    completion: workflow_command::Variant,
) {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter =
        CoreWfStarter::new_with_runtime("query_of_closed_workflow_doesnt_tick_terminal_metric", rt);
    // Disable cache to ensure replay happens completely
    starter.sdk_config.max_cached_workflows = 0_usize;
    let worker = starter.get_worker().await;
    let run_id = starter.start_wf().await;
    let task = worker.poll_workflow_activation().await.unwrap();
    // Fail wf task
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::fail(
            task.run_id,
            "whatever".into(),
            None,
        ))
        .await
        .unwrap();
    // Handle cache eviction
    let task = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    // Immediately complete the workflow
    let task = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            completion.clone(),
        ))
        .await
        .unwrap();

    let metric_name = match &completion {
        workflow_command::Variant::CompleteWorkflowExecution(_) => "temporal_workflow_completed",
        workflow_command::Variant::FailWorkflowExecution(_) => "temporal_workflow_failed",
        workflow_command::Variant::ContinueAsNewWorkflowExecution(_) => {
            "temporal_workflow_continue_as_new"
        }
        workflow_command::Variant::CancelWorkflowExecution(_) => "temporal_workflow_canceled",
        _ => unreachable!(),
    };

    // Verify there is one tick for the completion metric
    let body = get_text(format!("http://{addr}/metrics")).await;
    let matching_line = body
        .lines()
        .find(|l| l.starts_with(metric_name))
        .expect("Must find matching metric");
    assert!(matching_line.ends_with('1'));

    // Handle cache eviction
    let task = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    // Query the now-closed workflow
    let client = starter.get_client().await;
    let queryer = async {
        WorkflowExecutionInfo {
            namespace: client.namespace(),
            workflow_id: starter.get_wf_id().to_string(),
            run_id: Some(run_id),
            first_execution_run_id: None,
        }
        .bind_untyped(client.clone())
        .query(
            UntypedQuery::new("fake_query"),
            RawValue::empty(),
            WorkflowQueryOptions::default(),
        )
        .await
        .unwrap();
    };
    let query_reply = async {
        // Need to re-complete b/c replay
        let task = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                completion,
            ))
            .await
            .unwrap();

        let task = worker.poll_workflow_activation().await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                QueryResult {
                    query_id: query.query_id.clone(),
                    variant: Some(
                        QuerySuccess {
                            response: Some("hi".into()),
                        }
                        .into(),
                    ),
                }
                .into(),
            ))
            .await
            .unwrap()
    };
    join!(query_reply, queryer);

    // Verify there is still only one tick
    let body = get_text(format!("http://{addr}/metrics")).await;
    let matching_line = body
        .lines()
        .find(|l| l.starts_with(metric_name))
        .expect("Must find matching metric");
    assert!(matching_line.ends_with('1'));
}

#[test]
fn runtime_new() {
    let mut rt = CoreRuntime::new(
        get_integ_runtime_options(get_integ_telem_options()),
        TokioRuntimeBuilder::default(),
    )
    .unwrap();
    let handle = rt.tokio_handle();
    let _rt = handle.enter();
    let (telemopts, addr, _aborter) = prom_metrics(None);
    rt.telemetry_mut()
        .attach_late_init_metrics(telemopts.metrics.unwrap());
    let mut opts = get_integ_server_options();
    opts.metrics_meter = rt.telemetry().get_temporal_metric_meter();
    handle.block_on(async {
        let mut raw_client = Connection::connect(opts).await.unwrap();
        assert!(raw_client.capabilities().is_some());
        let _ = raw_client
            .list_namespaces(ListNamespacesRequest::default().into_request())
            .await
            .unwrap();
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains("temporal_request"));
    });
}

#[rstest::rstest]
#[tokio::test]
async fn latency_metrics(
    #[values(true, false)] use_seconds_latency: bool,
    #[values(true, false)] show_units: bool,
) {
    let (telemopts, addr, _aborter) = prom_metrics(Some(
        PrometheusExporterOptions::builder()
            .socket_addr(ANY_PORT.parse().unwrap())
            .use_seconds_for_durations(use_seconds_latency)
            .unit_suffix(show_units)
            .histogram_bucket_overrides(HistogramBucketOverrides {
                overrides: {
                    let mut hm = HashMap::new();
                    hm.insert(
                        WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME.to_string(),
                        vec![1337.0],
                    );
                    hm
                },
            })
            .build(),
    ));
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("latency_metrics", rt);
    let worker = starter.get_worker().await;
    starter.start_wf().await;
    // Immediately finish workflow
    let task = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            CompleteWorkflowExecution { result: None }.into(),
        ))
        .await
        .unwrap();

    struct MetricLines {
        end_to_end: String,
        execution: String,
        long_request: Vec<String>,
    }

    let metrics = eventually(
        || {
            let endpoint = format!("http://{addr}/metrics");
            async move {
                let body = get_text(endpoint).await;
                let end_to_end = body
                    .lines()
                    .find(|l| l.starts_with("temporal_workflow_endtoend_latency"))
                    .ok_or_else(|| anyhow!("endtoend_latency metric not found"))?
                    .to_string();
                let execution = body
                    .lines()
                    .find(|l| l.starts_with("temporal_workflow_task_execution_latency"))
                    .ok_or_else(|| anyhow!("task_execution_latency metric not found"))?
                    .to_string();
                let long_request: Vec<String> = body
                    .lines()
                    .filter(|l| l.starts_with("temporal_long_request_latency"))
                    .map(String::from)
                    .collect();
                if long_request.len() <= 1 {
                    bail!("long_request_latency metrics not yet available");
                }
                Ok(MetricLines {
                    end_to_end,
                    execution,
                    long_request,
                })
            }
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    if use_seconds_latency {
        if show_units {
            assert!(
                metrics
                    .end_to_end
                    .contains("temporal_workflow_endtoend_latency_seconds")
            );
        }
        assert!(metrics.end_to_end.contains("le=\"0.1\""));
    } else {
        if show_units {
            assert!(
                metrics
                    .end_to_end
                    .contains("temporal_workflow_endtoend_latency_milliseconds")
            );
        }
        assert!(metrics.end_to_end.contains("le=\"100\""));
    }

    assert!(metrics.execution.contains("le=\"1337\""));
    assert!(
        metrics
            .long_request
            .iter()
            .any(|l| l.contains("PollWorkflowTaskQueue"))
    );
}

#[tokio::test]
async fn request_fail_codes() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut client = get_integ_client(
        NAMESPACE.to_string(),
        rt.telemetry().get_temporal_metric_meter(),
    )
    .await;

    // Describe namespace w/ invalid argument (unset namespace field)
    WorkflowService::describe_namespace(
        &mut client,
        DescribeNamespaceRequest::default().into_request(),
    )
    .await
    .unwrap_err();

    let body = get_text(format!("http://{addr}/metrics")).await;
    let matching_line = body
        .lines()
        .find(|l| l.starts_with("temporal_request_failure"))
        .unwrap();
    assert!(matching_line.contains("operation=\"DescribeNamespace\""));
    assert!(matching_line.contains("status_code=\"INVALID_ARGUMENT\""));
    assert!(matching_line.contains("} 1"));
}

// OTel collector shutdown hangs in a single-threaded Tokio environment. We used to, in the past
// have a dedicated runtime just for telemetry which was meant to address problems like this.
// In reality, users are unlikely to run a single-threaded runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_fail_codes_otel() {
    let exporter = if let Some(url) = env::var(OTEL_URL_ENV_VAR)
        .ok()
        .map(|x| x.parse::<Url>().unwrap())
    {
        let opts = OtelCollectorOptions::builder().url(url).build();
        build_otlp_metric_exporter(opts).unwrap()
    } else {
        // skip
        return;
    };
    let exporter = Arc::new(exporter);
    let telemopts = TelemetryOptions::builder().metrics(exporter as Arc<dyn CoreMeter>);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts.build())).unwrap();
    let mut client = get_integ_client(
        NAMESPACE.to_string(),
        rt.telemetry().get_temporal_metric_meter(),
    )
    .await;

    for _ in 0..10 {
        // Describe namespace w/ invalid argument (unset namespace field)
        WorkflowService::describe_namespace(
            &mut client,
            DescribeNamespaceRequest::default().into_request(),
        )
        .await
        .unwrap_err();

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// Tests that rely on Prometheus running in a docker container need to start
// with `docker_` and set the `DOCKER_PROMETHEUS_RUNNING` env variable to run
#[rstest::rstest]
#[tokio::test]
async fn docker_metrics_with_prometheus(
    #[values(
        ("http://localhost:4318/v1/metrics", OtlpProtocol::Http),
        ("http://localhost:4317", OtlpProtocol::Grpc)
    )]
    otel_collector: (&str, OtlpProtocol),
) {
    if env::var("DOCKER_PROMETHEUS_RUNNING").is_err() {
        return;
    }
    let (otel_collector_addr, otel_protocol) = otel_collector;
    let test_uid = format!(
        "test_{}_",
        uuid::Uuid::new_v4().to_string().replace("-", "")
    );

    // Configure the OTLP exporter with HTTP
    let opts = OtelCollectorOptions::builder()
        .url(otel_collector_addr.parse().unwrap())
        .protocol(otel_protocol)
        .global_tags(HashMap::from([("test_id".to_string(), test_uid.clone())]))
        .build();
    let exporter = Arc::new(build_otlp_metric_exporter(opts).unwrap());
    let telemopts = TelemetryOptions::builder()
        .metrics(exporter as Arc<dyn CoreMeter>)
        .metric_prefix(test_uid.clone())
        .build();
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let test_name = "docker_metrics_with_prometheus";
    let mut starter = CoreWfStarter::new_with_runtime(test_name, rt);
    let worker = starter.get_worker().await;
    starter.start_wf().await;

    // Immediately finish the workflow
    let task = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            CompleteWorkflowExecution { result: None }.into(),
        ))
        .await
        .unwrap();

    let client = starter.get_client().await;
    WorkflowService::list_namespaces(
        &mut client.clone(),
        ListNamespacesRequest::default().into_request(),
    )
    .await
    .unwrap();

    eventually(
        || async {
            // Query Prometheus API for metrics
            let client = reqwest::Client::new();
            let query = format!("temporal_sdk_{}num_pollers", test_uid.clone());
            let response = client
                .get(PROMETHEUS_QUERY_API)
                .query(&[("query", query.clone())])
                .send()
                .await?
                .json::<serde_json::Value>()
                .await?;

            // Validate the Prometheus response
            if let Some(data) = response["data"]["result"].as_array() {
                if data.is_empty() {
                    bail!("No metrics found for query: {query}");
                }
                assert_eq!(data[0]["metric"]["exported_job"], "temporal-core-sdk");
                assert_eq!(data[0]["metric"]["job"], "otel-collector");
                assert!(
                    data[0]["metric"]["task_queue"]
                        .as_str()
                        .unwrap()
                        .starts_with(test_name)
                );
            } else {
                bail!("Invalid Prometheus response: {response:?}");
            }
            Ok(())
        },
        Duration::from_secs(45),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn activity_metrics() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let wf_name = "activity_metrics";
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.sdk_config.graceful_shutdown_period = Some(Duration::from_secs(1));

    struct PassFailActivities;
    #[activities]
    impl PassFailActivities {
        #[activity(name = "pass_fail_act")]
        async fn pass_fail_act(ctx: ActivityContext, i: String) -> Result<String, ActivityError> {
            match i.as_str() {
                "pass" => Ok("pass".to_string()),
                "cancel" => {
                    ctx.cancelled().await;
                    Err(ActivityError::cancelled())
                }
                _ => Err(anyhow!("fail").into()),
            }
        }
    }

    starter.sdk_config.register_activities(PassFailActivities);
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct ActivityMetricsWf;

    #[workflow_methods]
    impl ActivityMetricsWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let normal_act_pass = ctx.start_activity(
                PassFailActivities::pass_fail_act,
                "pass".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(1)),
            );
            let normal_act_fail = ctx.start_activity(
                PassFailActivities::pass_fail_act,
                "fail".to_string(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(1))
                    .retry_policy(RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    })
                    .build(),
            );
            let _ = join!(normal_act_pass, normal_act_fail);
            let local_act_pass = ctx.start_local_activity(
                PassFailActivities::pass_fail_act,
                "pass".to_string(),
                LocalActivityOptions::default(),
            );
            let local_act_fail = ctx.start_local_activity(
                PassFailActivities::pass_fail_act,
                "fail".to_string(),
                LocalActivityOptions {
                    retry_policy: RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            );
            let local_act_cancel = ctx.start_local_activity(
                PassFailActivities::pass_fail_act,
                "cancel".to_string(),
                LocalActivityOptions {
                    retry_policy: RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            );
            let _ = join!(local_act_pass, local_act_fail);
            // TODO: Currently takes a WFT b/c of https://github.com/temporalio/sdk-core/issues/856
            local_act_cancel.cancel();
            let _ = local_act_cancel.await;
            Ok(())
        }
    }

    worker.register_workflow::<ActivityMetricsWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = wf_name.to_owned();
    worker
        .submit_workflow(
            ActivityMetricsWf::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), workflow_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let body = get_text(format!("http://{addr}/metrics")).await;
    let wf_type = ActivityMetricsWf::name();
    assert!(body.contains(&format!(
        "temporal_activity_execution_failed{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",workflow_type=\"{wf_type}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_activity_schedule_to_start_latency_count{{\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\"}} 2"
    )));
    assert!(body.contains(&format!(
        "temporal_activity_execution_latency_count{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",workflow_type=\"{wf_type}\"}} 2"
    )));
    assert!(body.contains(&format!(
        "temporal_activity_succeed_endtoend_latency_count{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",workflow_type=\"{wf_type}\"}} 1"
    )));

    assert!(body.contains(&format!(
        "temporal_local_activity_total{{activity_type=\"pass_fail_act\",namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_type}\"}} 3"
    )));
    assert!(body.contains(&format!(
        "temporal_local_activity_execution_failed{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_type}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_local_activity_execution_cancelled{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_type}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_local_activity_execution_latency_count{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_type}\"}} 3"
    )));
    assert!(body.contains(&format!(
        "temporal_local_activity_succeed_endtoend_latency_count{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_type}\"}} 1"
    )));
}

#[tokio::test]
async fn nexus_metrics() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let wf_name = "nexus_metrics";
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.sdk_config.task_types = WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    };
    // Nexus operation handling involves internal async coordination that can
    // trigger false positives in nondeterminism detection.
    starter.sdk_config.detect_nondeterministic_futures = false;
    let mut worker = starter.worker().await;
    let core_worker = starter.get_worker().await;
    let endpoint = mk_nexus_endpoint(&mut starter).await;

    #[workflow]
    #[derive(Default)]
    struct NexusMetricsWf;

    #[workflow_methods]
    impl NexusMetricsWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>, endpoint: String) -> WorkflowResult<()> {
            let partial_op = NexusOperationOptions {
                endpoint: endpoint.clone(),
                service: "mysvc".to_string(),
                operation: "myop".to_string(),
                ..Default::default()
            };
            join!(
                async {
                    ctx.start_nexus_operation(partial_op.clone())
                        .await
                        .unwrap()
                        .result()
                        .await
                },
                async {
                    let _ = ctx
                        .start_nexus_operation(NexusOperationOptions {
                            input: Some("fail".into()),
                            ..partial_op.clone()
                        })
                        .await;
                },
                async {
                    let _ = ctx
                        .start_nexus_operation(NexusOperationOptions {
                            input: Some("handler-fail".into()),
                            ..partial_op.clone()
                        })
                        .await;
                },
                async {
                    let _ = ctx
                        .start_nexus_operation(NexusOperationOptions {
                            input: Some("timeout".into()),
                            schedule_to_close_timeout: Some(Duration::from_secs(2)),
                            ..partial_op.clone()
                        })
                        .await;
                }
            );
            Ok(())
        }
    }

    worker.register_workflow::<NexusMetricsWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = wf_name.to_owned();
    worker
        .submit_workflow(
            NexusMetricsWf::run,
            endpoint,
            WorkflowStartOptions::new(task_queue.clone(), workflow_id).build(),
        )
        .await
        .unwrap();

    let nexus_polling = async {
        for _ in 0..5 {
            let nt = core_worker.poll_nexus_task().await.unwrap();
            let task_token = nt.task_token().to_vec();
            let status = if matches!(nt.variant, Some(nexus_task::Variant::CancelTask(_))) {
                nexus_task_completion::Status::AckCancel(true)
            } else {
                let nt = nt.unwrap_task();
                match nt.request.unwrap().variant.unwrap() {
                    Variant::StartOperation(s) => match s.payload {
                        Some(p) if p.data.is_empty() => {
                            nexus_task_completion::Status::Completed(nexus::v1::Response {
                                variant: Some(nexus::v1::response::Variant::StartOperation(
                                    StartOperationResponse {
                                        variant: Some(
                                            start_operation_response::Variant::SyncSuccess(
                                                start_operation_response::Sync {
                                                    payload: Some("yay".into()),
                                                    links: vec![],
                                                },
                                            ),
                                        ),
                                    },
                                )),
                            })
                        }
                        Some(p) if p == "fail".into() => {
                            nexus_task_completion::Status::Completed(nexus::v1::Response {
                                variant: Some(nexus::v1::response::Variant::StartOperation(
                                    StartOperationResponse {
                                        variant: Some(
                                            #[allow(deprecated)]
                                            start_operation_response::Variant::OperationError(
                                                UnsuccessfulOperationError {
                                                    operation_state: "failed".to_string(),
                                                    failure: Some(nexus::v1::Failure {
                                                        message: "fail".to_string(),
                                                        ..Default::default()
                                                    }),
                                                },
                                            ),
                                        ),
                                    },
                                )),
                            })
                        }
                        Some(p) if p == "handler-fail".into() =>
                        {
                            #[allow(deprecated)]
                            nexus_task_completion::Status::Error(HandlerError {
                                error_type: "BAD_REQUEST".to_string(),
                                failure: Some(nexus::v1::Failure {
                                    message: "handler-fail".to_string(),
                                    ..Default::default()
                                }),
                                retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable.into(),
                            })
                        }
                        Some(p) if p == "timeout".into() => {
                            // Don't do anything, will wait for timeout task
                            continue;
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            };
            core_worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token,
                    status: Some(status),
                })
                .await
                .unwrap();
        }
        // Gotta get shutdown poll
        assert_matches!(
            core_worker.poll_nexus_task().await,
            Err(PollError::ShutDown)
        );
    };

    join!(nexus_polling, async {
        worker.run_until_done().await.unwrap()
    });

    let body = get_text(format!("http://{addr}/metrics")).await;
    assert!(body.contains(&format!(
        "temporal_nexus_task_execution_failed{{failure_reason=\"handler_error_BAD_REQUEST\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_nexus_task_execution_failed{{failure_reason=\"timeout\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_nexus_task_execution_failed{{failure_reason=\"operation_failed\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_nexus_task_schedule_to_start_latency_count{{\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\"}} 4"
    )));
    assert!(body.contains(&format!(
        "temporal_nexus_task_execution_latency_count{{\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\"}} 4"
    )));
    // Only 3 actually finished - the timed-out one will not have an e2e latency
    assert!(body.contains(&format!(
        "temporal_nexus_task_endtoend_latency_count{{\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\"}} 3"
    )));
}

#[tokio::test]
async fn evict_on_complete_does_not_count_as_forced_eviction() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let wf_name = "evict_on_complete_does_not_count_as_forced_eviction";
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct EvictOnCompleteWf;

    #[workflow_methods]
    impl EvictOnCompleteWf {
        #[run]
        async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            Ok(())
        }
    }

    worker.register_workflow::<EvictOnCompleteWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = wf_name.to_owned();
    worker
        .submit_workflow(
            EvictOnCompleteWf::run,
            (),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let body = get_text(format!("http://{addr}/metrics")).await;
    // Metric shouldn't show up at all, since it's zero the whole time.
    assert!(!body.contains("temporal_sticky_cache_total_forced_eviction"));
}

struct MetricRecordingSlotSupplier<SK> {
    inner: FixedSizeSlotSupplier<SK>,
    metrics: OnceLock<(Gauge, Gauge, Gauge)>,
}

#[async_trait::async_trait]
impl<SK> SlotSupplier for MetricRecordingSlotSupplier<SK>
where
    SK: SlotKind + Send + Sync,
{
    type SlotKind = SK;

    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        let (g, _, _) = self.metrics.get_or_init(|| {
            let meter = ctx.get_metrics_meter().unwrap();
            let g1 = meter.gauge(MetricParameters::from("custom_reserve"));
            let g2 = meter.gauge(MetricParameters::from("custom_mark_used"));
            let g3 = meter.gauge(MetricParameters::from("custom_release"));
            (g1, g2, g3)
        });
        let attrs = ctx
            .get_metrics_meter()
            .unwrap()
            .new_attributes(NewAttributes::new(vec![]));
        g.record(1, &attrs);
        self.inner.reserve_slot(ctx).await
    }

    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        self.inner.try_reserve_slot(ctx)
    }

    fn mark_slot_used(&self, ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {
        let meter = ctx.get_metrics_meter().unwrap();
        let attrs = meter.new_attributes(NewAttributes::new(vec![]));
        let (_, g, _) = self.metrics.get().unwrap();
        g.record(1, &attrs);
        self.inner.mark_slot_used(ctx);
    }

    fn release_slot(&self, ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {
        let meter = ctx.get_metrics_meter().unwrap();
        let attrs = meter.new_attributes(NewAttributes::new(vec![]));
        let (_, _, g) = self.metrics.get().unwrap();
        g.record(1, &attrs);
        self.inner.release_slot(ctx);
    }

    fn available_slots(&self) -> Option<usize> {
        self.inner.available_slots()
    }
}

#[tokio::test]
async fn metrics_available_from_custom_slot_supplier() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter =
        CoreWfStarter::new_with_runtime("metrics_available_from_custom_slot_supplier", rt);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut tb = TunerBuilder::default();
    tb.workflow_slot_supplier(Arc::new(MetricRecordingSlotSupplier::<WorkflowSlotKind> {
        inner: FixedSizeSlotSupplier::new(5),
        metrics: OnceLock::new(),
    }));
    starter.sdk_config.tuner = Arc::new(tb.build());
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct CustomSlotSupplierWf;

    #[workflow_methods]
    impl CustomSlotSupplierWf {
        #[run]
        async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            Ok(())
        }
    }

    worker.register_workflow::<CustomSlotSupplierWf>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            CustomSlotSupplierWf::run,
            (),
            WorkflowStartOptions::new(task_queue, "s_wf".to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let body = get_text(format!("http://{addr}/metrics")).await;
    assert!(body.contains("custom_reserve"));
    assert!(body.contains("custom_mark_used"));
    assert!(body.contains("custom_release"));
}

#[tokio::test]
async fn test_prometheus_endpoint_integration() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let meter = telemopts.metrics.unwrap();

    let counter = meter.counter(MetricParameters {
        name: "test_requests_total".into(),
        description: "Total number of test requests".into(),
        unit: "".into(),
    });
    let histogram = meter.histogram(MetricParameters {
        name: "test_request_duration_ms".into(),
        description: "Duration of test requests in milliseconds".into(),
        unit: "ms".into(),
    });
    let gauge = meter.gauge(MetricParameters {
        name: "test_active_connections".into(),
        description: "Number of active test connections".into(),
        unit: "".into(),
    });
    let up_down_counter = meter.up_down_counter(MetricParameters {
        name: "test_inflight_operations".into(),
        description: "Number of inflight test operations".into(),
        unit: "".into(),
    });

    counter.adds(5);
    histogram.records(100);
    gauge.records(10);
    up_down_counter.adds(5);
    up_down_counter.adds(-2);

    let url = format!("http://{addr}/metrics");
    let response = tokio::time::timeout(Duration::from_secs(10), reqwest::get(&url))
        .await
        .expect("Request timed out")
        .expect("Request failed");

    assert!(response.status().is_success());

    let content_type = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.contains("text/plain"));

    let body = response.text().await.expect("Failed to read response body");

    assert!(body.contains("test_requests_total"),);
    assert!(body.contains("test_request_duration_ms"),);
    assert!(body.contains("test_active_connections"),);
    assert!(body.contains("test_requests_total 5"),);
    assert!(body.contains("test_active_connections 10"),);
    assert!(body.contains("test_request_duration_ms_count 1"),);
    assert!(body.contains("test_request_duration_ms_sum 100"),);
    assert!(body.contains("test_inflight_operations"),);
    assert!(body.contains("test_inflight_operations 3"),);
}

#[tokio::test]
async fn test_prometheus_metric_format_consistency() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let meter = telemopts.metrics.unwrap();

    let workflow_counter = meter.counter(MetricParameters {
        name: "temporal_workflow_completed_total".into(),
        description: "Total number of completed workflows".into(),
        unit: "".into(),
    });
    let activity_histogram = meter.histogram_duration(MetricParameters {
        name: "temporal_activity_execution_latency".into(),
        description: "Duration of activity execution".into(),
        unit: "ms".into(),
    });

    let attrs = meter.new_attributes(NewAttributes::new(vec![]));

    workflow_counter.add(1, &attrs);
    activity_histogram.record(Duration::from_millis(150), &attrs);

    let url = format!("http://{addr}/metrics");
    let response = tokio::time::timeout(Duration::from_secs(10), reqwest::get(&url))
        .await
        .expect("Request timed out")
        .expect("Request failed");

    let body = response.text().await.expect("Failed to read response body");

    assert!(body.contains("# HELP temporal_workflow_completed_total"),);
    assert!(body.contains("# TYPE temporal_workflow_completed_total counter"),);
    assert!(body.contains("# HELP temporal_activity_execution_latency"),);
    assert!(body.contains("# TYPE temporal_activity_execution_latency histogram"),);
    assert!(body.contains("temporal_workflow_completed_total 1"),);
    assert!(body.contains("temporal_activity_execution_latency_count 1"),);
    assert!(body.contains("temporal_activity_execution_latency_bucket"),);
    assert!(body.contains("le=\""));
}

#[tokio::test]
async fn prometheus_label_nonsense() {
    let opts_builder = PrometheusExporterOptions::builder().socket_addr(ANY_PORT.parse().unwrap());
    let (telemopts, addr, _aborter) = prom_metrics(Some(opts_builder.build()));
    let meter = telemopts.metrics.clone().unwrap();

    let ctr = meter.counter(MetricParameters::builder().name("some_counter").build());
    let a1 = meter.new_attributes(NewAttributes::from([MetricKeyValue::new("thing", "foo")]));
    let a2 = meter.new_attributes(NewAttributes::from([MetricKeyValue::new("blerp", "baz")]));
    ctr.add(1, &a1);
    ctr.add(1, &a2);
    ctr.add(1, &a2);
    ctr.add(1, &a1);

    let body = get_text(format!("http://{addr}/metrics")).await;
    assert!(body.contains("some_counter{thing=\"foo\"} 2"));
    assert!(body.contains("some_counter{blerp=\"baz\"} 2"));
}

#[rstest::rstest]
#[tokio::test]
async fn sticky_queue_label_strategy(
    #[values(
        TaskQueueLabelStrategy::UseNormal,
        TaskQueueLabelStrategy::UseNormalAndSticky
    )]
    strategy: TaskQueueLabelStrategy,
) {
    let (mut telemopts, addr, _aborter) = prom_metrics(Some(
        PrometheusExporterOptions::builder()
            .socket_addr(ANY_PORT.parse().unwrap())
            .build(),
    ));
    telemopts.task_queue_label_strategy = strategy;
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let wf_name = format!("sticky_queue_label_strategy_{strategy:?}");
    let mut starter = CoreWfStarter::new_with_runtime(&wf_name, rt);
    // Enable sticky queues by setting a reasonable cache size
    starter.sdk_config.max_cached_workflows = 10_usize;
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct StickyQueueLabelStrategyWf;

    #[workflow_methods]
    impl StickyQueueLabelStrategyWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.timer(Duration::from_millis(1)).await;
            Ok(())
        }
    }

    worker
        .register_workflow::<StickyQueueLabelStrategyWf>()
        .unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            StickyQueueLabelStrategyWf::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), wf_name.clone())
                .enable_eager_workflow_start(false)
                .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    // Give metrics time to be recorded
    tokio::time::sleep(Duration::from_millis(200)).await;

    let body = get_text(format!("http://{addr}/metrics")).await;

    let poll_metrics: Vec<_> = body
        .lines()
        .filter(|l| {
            l.contains("temporal_long_request")
                && l.contains("operation=\"PollWorkflowTaskQueue\"")
                && l.contains(&format!("namespace=\"{NAMESPACE}\""))
        })
        .collect();

    assert!(!poll_metrics.is_empty(), "Should have poll metrics");

    match strategy {
        TaskQueueLabelStrategy::UseNormalAndSticky => {
            // With UseNormalAndSticky, we should see sticky queue names
            let has_sticky = poll_metrics.iter().any(|l| {
                l.contains("task_queue=")
                    && l.contains("WorkflowTask")
                    && !l.contains(&format!("task_queue=\"{task_queue}\""))
            });

            assert!(
                has_sticky,
                "With UseNormalAndSticky, should see sticky queue names in metrics. Metrics:\n{}",
                poll_metrics.join("\n")
            );
        }
        TaskQueueLabelStrategy::UseNormal => {
            // With UseNormal, ALL metrics should use the normal queue name
            for l in &poll_metrics {
                if l.contains("task_queue=") && l.contains("WorkflowTask") {
                    assert!(
                        l.contains(&format!("task_queue=\"{task_queue}\"")),
                        "With UseNormal, all workflow task_queue labels should use normal name. Found: {l}",
                    );
                }
            }
        }
        _ => unreachable!("Test only covers UseNormal and UseNormalAndSticky"),
    }
}

#[tokio::test]
async fn resource_based_tuner_metrics() {
    use temporalio_sdk_core::ResourceBasedTuner;

    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let wf_name = "resource_based_tuner_metrics";
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    // Create a resource-based tuner with reasonable thresholds
    let tuner = ResourceBasedTuner::new(0.8, 0.8);
    starter.sdk_config.tuner = Arc::new(tuner);

    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct ResourceBasedTunerMetricsWf;

    #[workflow_methods]
    impl ResourceBasedTunerMetricsWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.timer(Duration::from_millis(100)).await;
            Ok(())
        }
    }

    worker
        .register_workflow::<ResourceBasedTunerMetricsWf>()
        .unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = wf_name.to_owned();
    worker
        .submit_workflow(
            ResourceBasedTunerMetricsWf::run,
            (),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();

    // Give metrics time to be recorded (metrics are emitted every 1 second)
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let body = get_text(format!("http://{addr}/metrics")).await;

    // Verify that the resource-based tuner metrics are present
    assert!(
        body.contains("temporal_resource_slots_mem_usage"),
        "Memory usage metric should be present"
    );
    assert!(
        body.contains("temporal_resource_slots_cpu_usage"),
        "CPU usage metric should be present"
    );
    assert!(
        body.contains("temporal_resource_slots_mem_pid_output"),
        "Memory PID output metric should be present"
    );
    assert!(
        body.contains("temporal_resource_slots_cpu_pid_output"),
        "CPU PID output metric should be present"
    );
}

#[tokio::test]
async fn terminal_metric_not_recorded_on_rejected_completion() {
    let prom_info = start_prometheus_metric_exporter(
        PrometheusExporterOptions::builder()
            .socket_addr(ANY_PORT.parse().unwrap())
            .build(),
    )
    .unwrap();
    let addr = prom_info.bound_addr;
    let _abort = prom_info.abort_handle;
    let meter = TemporalMeter::new(
        prom_info.meter as Arc<dyn CoreMeter>,
        NewAttributes { attributes: vec![] },
        TaskQueueLabelStrategy::UseNormal,
    );

    // Build a simple workflow history: start + WFT
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(
        temporalio_common::protos::temporal::api::enums::v1::EventType::WorkflowExecutionStarted,
    );
    t.add_workflow_task_scheduled_and_started();

    // First WFT completion is rejected by server, second succeeds
    let mut call_count = 0;
    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock_worker_client(),
    );
    mh.completion_mock_fn = Some(Box::new(move |_| {
        call_count += 1;
        if call_count == 1 {
            Err(tonic::Status::not_found("Workflow task not found"))
        } else {
            Ok(Default::default())
        }
    }));

    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    mock.set_temporal_meter(meter);
    let core = mock_worker(mock);

    // First attempt: get WFT activation and complete the workflow
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&act.run_id).await;

    // Handle eviction (triggered by NotFound error from server)
    core.handle_eviction().await;

    // Second attempt (replay after eviction): complete workflow again
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&act.run_id).await;

    core.drain_pollers_and_shutdown().await;

    // The workflow_completed metric should be recorded exactly once — only for
    // the successful server response, not for the rejected first attempt.
    let body = get_text(format!("http://{addr}/metrics")).await;
    let matching_line = body.lines().find(|l| l.starts_with("workflow_completed{"));
    match matching_line {
        Some(line) => {
            assert!(
                line.ends_with(" 1"),
                "Expected workflow_completed count of 1, got: {line}"
            );
            assert!(
                line.contains("workflow_type=\"default_wf_type\""),
                "Expected workflow_type label on metric, got: {line}"
            );
        }
        None => panic!(
            "workflow_completed metric not found. Available metrics:\n{}",
            body.lines()
                .filter(|l| l.contains("workflow"))
                .collect::<Vec<_>>()
                .join("\n")
        ),
    }
}

#[tokio::test]
async fn wf_task_latency_recorded_on_dropped_wft() {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let meter = rt.telemetry().get_temporal_metric_meter().unwrap();

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(
        temporalio_common::protos::temporal::api::enums::v1::EventType::WorkflowExecutionStarted,
    );
    t.add_workflow_task_scheduled_and_started();

    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock_worker_client(),
    );
    mh.num_expected_fails = 1;
    mh.num_expected_completions = Some(0.into());

    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    mock.set_temporal_meter(meter);
    let core = mock_worker(mock);

    // First poll (attempt=1): fail the activation — this is reported to the server
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::fail(
        act.run_id,
        "test failure".into(),
        None,
    ))
    .await
    .unwrap();
    core.handle_eviction().await;

    // Second poll (attempt=2): fail again — this goes through WFTFailedDontReport/DropWft
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::fail(
        act.run_id,
        "test failure".into(),
        None,
    ))
    .await
    .unwrap();

    core.drain_pollers_and_shutdown().await;

    // Both WFT processing attempts should have recorded latency, even the dropped one
    eventually(
        || {
            let endpoint = format!("http://{addr}/metrics");
            async move {
                let body = get_text(endpoint).await;
                let line = body
                    .lines()
                    .find(|l| l.starts_with("temporal_workflow_task_execution_latency_count{"))
                    .ok_or_else(|| anyhow!("wf_task_execution_latency metric not found"))?
                    .to_string();
                if !line.ends_with(" 2") {
                    bail!(
                        "Expected latency count of 2 (both reported and dropped WFT), got: {line}"
                    );
                }
                Ok(())
            }
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();
}
