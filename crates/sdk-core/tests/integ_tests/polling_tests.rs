use crate::common::{
    CoreWfStarter, INTEG_CLIENT_NAME, INTEG_CLIENT_VERSION, activity_functions::StdActivities,
    get_integ_client, init_core_and_create_wf, init_integ_telem, integ_dev_server_config,
    integ_worker_config,
};
use assert_matches::assert_matches;
use futures_util::{FutureExt, StreamExt, future::join_all};
use std::{
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    Client, Connection, ConnectionOptions, NamespacedClient, UntypedWorkflow,
    WorkflowExecutionInfo, WorkflowStartOptions,
};
use temporalio_common::{
    data_converters::RawValue,
    protos::{
        coresdk::{
            IntoCompletion,
            activity_task::activity_task as act_task,
            workflow_activation::{FireTimer, WorkflowActivationJob, workflow_activation_job},
            workflow_commands::{ActivityCancellationType, RequestCancelActivity, StartTimer},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::enums::v1::EventType,
    },
    telemetry::{CoreLogStreamConsumer, Logger, TelemetryOptions},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, RuntimeOptions, TunerHolder,
    ephemeral_server::{TemporalDevServerConfig, default_cached_download},
    init_worker, prost_dur,
    test_help::{NAMESPACE, WorkerTestHelpers, drain_pollers_and_shutdown, schedule_activity_cmd},
};
use tokio::{sync::Notify, time::timeout};
use tracing::info;
use url::Url;

#[tokio::test]
async fn out_of_order_completion_doesnt_hang() {
    let mut starter = init_core_and_create_wf("out_of_order_completion_doesnt_hang").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is try-cancelled.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(
                    FireTimer { seq: t_seq }
                )),
            },
        ] => {
            assert_eq!(*t_seq, 1);
        }
    );

    // Start polling again *before* we complete the WFT
    let cc = core.clone();
    let jh = tokio::spawn(async move {
        // We want to fail the test if this takes too long -- we should not hit long poll timeout
        let task = timeout(Duration::from_secs(1), cc.poll_workflow_activation())
            .await
            .expect("Poll should come back right away")
            .unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        cc.complete_execution(&task.run_id).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    // Then complete the (last) WFT with a request to cancel the AT, which should produce a
    // pending activation, unblocking the (already started) poll
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();

    jh.await.unwrap();
}

#[tokio::test]
async fn switching_worker_client_changes_poll() {
    // Start two servers
    info!("Starting servers");
    let server_config = TemporalDevServerConfig::builder()
        .exe(default_cached_download())
        // We need to lower the poll timeout so the poll call rolls over
        .extra_args(vec![
            "--dynamic-config-value".to_string(),
            "matching.longPollExpirationInterval=\"1s\"".to_string(),
        ])
        .build();
    let mut server1 = server_config
        .start_server_with_output(Stdio::null(), Stdio::null())
        .await
        .unwrap();
    let mut server2 = server_config
        .start_server_with_output(Stdio::null(), Stdio::null())
        .await
        .unwrap();

    let result = std::panic::AssertUnwindSafe(async {
        // Connect clients to both servers
        info!("Connecting clients");
        let opts1 =
            ConnectionOptions::new(Url::parse(&format!("http://{}", server1.target)).unwrap())
                .identity("integ_tester".to_owned())
                .client_name("temporal-core".to_owned())
                .client_version("0.1.0".to_owned())
                .build();
        let connection1 = Connection::connect(opts1).await.unwrap();
        let client_opts1 = temporalio_client::ClientOptions::new("default").build();
        let client1 = Client::new(connection1, client_opts1).unwrap();

        let opts2 =
            ConnectionOptions::new(Url::parse(&format!("http://{}", server2.target)).unwrap())
                .identity("integ_tester".to_owned())
                .client_name("temporal-core".to_owned())
                .client_version("0.1.0".to_owned())
                .build();
        let connection2 = Connection::connect(opts2).await.unwrap();
        let client_opts2 = temporalio_client::ClientOptions::new("default").build();
        let client2 = Client::new(connection2, client_opts2).unwrap();

        // Start a workflow on both servers
        info!("Starting workflows");
        let wf1 = client1
            .start_workflow(
                UntypedWorkflow::new("my-workflow-type"),
                RawValue::default(),
                WorkflowStartOptions::new("my-task-queue".to_owned(), "my-workflow-1".to_owned())
                    .build(),
            )
            .await
            .unwrap();
        let wf1_run_id = wf1.run_id().unwrap().to_string();
        let wf2 = client2
            .start_workflow(
                UntypedWorkflow::new("my-workflow-type"),
                RawValue::default(),
                WorkflowStartOptions::new("my-task-queue".to_owned(), "my-workflow-2".to_owned())
                    .build(),
            )
            .await
            .unwrap();
        let wf2_run_id = wf2.run_id().unwrap().to_string();

        // Create a worker only on the first server
        let mut config = integ_worker_config("my-task-queue");
        // We want a cache so we don't get extra remove-job activations
        config.max_cached_workflows = 100_usize;
        let worker = init_worker(
            init_integ_telem().unwrap(),
            config,
            client1.connection().clone(),
        )
        .unwrap();

        // Poll for first task, confirm it's first wf, complete, and wait for complete
        info!("Doing initial poll");
        let act1 = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(wf1_run_id, act1.run_id);
        worker.complete_execution(&act1.run_id).await;
        worker.handle_eviction().await;
        info!("Waiting on first workflow complete");
        WorkflowExecutionInfo {
            namespace: client1.namespace(),
            workflow_id: "my-workflow-1".into(),
            run_id: Some(wf1_run_id.clone()),
            first_execution_run_id: None,
        }
        .bind_untyped(client1.clone())
        .get_result(Default::default())
        .await
        .unwrap();

        // Swap client, poll for next task, confirm it's second wf, and respond w/ empty
        info!("Replacing client and polling again");
        worker.replace_client(client2.connection().clone()).unwrap();
        let act2 = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(wf2_run_id, act2.run_id);
        worker.complete_execution(&act2.run_id).await;
        worker.handle_eviction().await;
        info!("Waiting on second workflow complete");
        WorkflowExecutionInfo {
            namespace: client2.namespace(),
            workflow_id: "my-workflow-2".into(),
            run_id: Some(wf2_run_id),
            first_execution_run_id: None,
        }
        .bind_untyped(client2.clone())
        .get_result(Default::default())
        .await
        .unwrap();

        // Shutdown workers and servers
        drain_pollers_and_shutdown(&worker).await;
    })
    .catch_unwind()
    .await;

    let shutdown_results = join_all([server1.shutdown(), server2.shutdown()]).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
    for r in shutdown_results {
        r.unwrap();
    }
}

#[workflow]
#[derive(Default)]
struct OnlyOneWorkflowSlotAndTwoPollers;

#[workflow_methods]
impl OnlyOneWorkflowSlotAndTwoPollers {
    #[run(name = "only_one_workflow_slot_and_two_pollers")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for _ in 0..3 {
            let _ = ctx
                .start_activity(
                    StdActivities::echo,
                    "hi!".to_string(),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                )
                .await;
        }
        Ok(())
    }
}

#[rstest::rstest]
#[tokio::test]
async fn small_workflow_slots_and_pollers(#[values(false, true)] use_autoscaling: bool) {
    let wf_name = "only_one_workflow_slot_and_two_pollers";
    let mut starter = CoreWfStarter::new(wf_name);
    if use_autoscaling {
        starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 5,
            initial: 1,
        };
    } else {
        starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(2);
    }
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::SimpleMaximum(1);
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(2, 1, 1, 1));
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    worker
        .register_workflow::<OnlyOneWorkflowSlotAndTwoPollers>()
        .unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            OnlyOneWorkflowSlotAndTwoPollers::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), task_queue.clone()).build(),
        )
        .await
        .unwrap();
    let wf2id = format!("{}-2", starter.get_task_queue());
    worker
        .submit_workflow(
            OnlyOneWorkflowSlotAndTwoPollers::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), wf2id.clone()).build(),
        )
        .await
        .unwrap();
    // If we don't fail the workflow on nondeterminism, we'll get stuck here retrying the WFT
    worker.run_until_done().await.unwrap();
    // Verify no task timeouts happened
    let history = starter.get_history().await;
    let any_task_timeouts = history
        .events
        .iter()
        .any(|e| e.event_type() == EventType::WorkflowTaskTimedOut);
    assert!(!any_task_timeouts);
    let events = starter
        .get_client()
        .await
        .get_workflow_handle::<UntypedWorkflow>(&wf2id)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();
    let any_task_timeouts = events
        .iter()
        .any(|e| e.event_type() == EventType::WorkflowTaskTimedOut);
    assert!(!any_task_timeouts);
}

#[tokio::test]
async fn replace_client_works_after_polling_failure() {
    let (log_consumer, mut log_rx) = CoreLogStreamConsumer::new(100);
    let telem_opts = TelemetryOptions::builder()
        .logging(Logger::Push {
            filter: "OFF,temporalio_client=DEBUG".into(),
            consumer: Arc::new(log_consumer),
        })
        .build();
    let runtime_opts = RuntimeOptions::builder()
        .telemetry_options(telem_opts)
        .build()
        .unwrap();
    let rt = Arc::new(CoreRuntime::new_assume_tokio(runtime_opts).unwrap());

    // Spawning background task to read logs and notify the test when polling failure occurs.
    let look_for_poll_failure_log = Arc::new(AtomicBool::new(false));
    let poll_retry_log_found = Arc::new(Notify::new());
    let log_reader_join_handle = tokio::spawn({
        let look_for_poll_retry_log = look_for_poll_failure_log.clone();
        let poll_retry_log_found = poll_retry_log_found.clone();
        async move {
            let mut enabled = false;
            while let Some(log) = log_rx.next().await {
                if !enabled {
                    enabled = look_for_poll_retry_log.load(Ordering::Acquire);
                }
                if enabled
                    && (log
                        .message
                        .starts_with("gRPC call poll_workflow_task_queue failed")
                        || log
                            .message
                            .starts_with("gRPC call poll_workflow_task_queue retried"))
                {
                    poll_retry_log_found.notify_one();
                    break;
                }
            }
        }
    });
    let abort_handles = Arc::new(Mutex::new(vec![log_reader_join_handle.abort_handle()]));

    // Starting a second dev server for the worker to connect to initially. Later this server will be shut down
    // and the worker client replaced with a client connected to the main integration test server.
    let initial_server_config = integ_dev_server_config(vec![], false);
    let initial_server = Arc::new(Mutex::new(Some(
        initial_server_config
            .start_server_with_output(Stdio::null(), Stdio::null())
            .await
            .unwrap(),
    )));

    let result = {
        let initial_server = initial_server.clone();
        let abort_handles = abort_handles.clone();
        std::panic::AssertUnwindSafe(async move {
            let initial_server_target = format!(
                "http://{}",
                initial_server.lock().unwrap().as_ref().unwrap().target
            );
            let opts = ConnectionOptions::new(Url::parse(&initial_server_target).unwrap())
                .identity("client_for_initial_server".to_string())
                .client_name(INTEG_CLIENT_NAME.to_string())
                .client_version(INTEG_CLIENT_VERSION.to_string())
                .build();
            let connection = Connection::connect(opts).await.unwrap();
            let client_opts = temporalio_client::ClientOptions::new(NAMESPACE).build();
            let client_for_initial_server = Client::new(connection, client_opts).unwrap();

            let wf_name = "replace_client_works_after_polling_failure";
            let task_queue = format!("{wf_name}_tq");

            let mut config = integ_worker_config(&task_queue);
            config.max_cached_workflows = 100_usize;
            let worker = Arc::new(
                init_worker(&rt, config, client_for_initial_server.connection().clone()).unwrap(),
            );

            // Polling the initial server the first time is successful.
            let wf_1 = client_for_initial_server
                .start_workflow(
                    UntypedWorkflow::new(wf_name),
                    RawValue::default(),
                    WorkflowStartOptions::new(task_queue.clone(), wf_name.to_string()).build(),
                )
                .await
                .unwrap();
            let wf_1_run_id = wf_1.run_id().unwrap().to_string();
            let act_1 =
                tokio::time::timeout(Duration::from_secs(60), worker.poll_workflow_activation())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(act_1.run_id, wf_1_run_id);

            // Initial server is shut down.
            let mut server = initial_server.lock().unwrap().take().unwrap();
            server.shutdown().await.unwrap();

            // Start polling in a background task.
            look_for_poll_failure_log.store(true, Ordering::Release);
            let poll_join_handle = tokio::spawn({
                let worker = worker.clone();
                async move { worker.poll_workflow_activation().await }
            });
            abort_handles
                .try_lock()
                .unwrap()
                .push(poll_join_handle.abort_handle());

            // Wait until polling failure is detected.
            tokio::time::timeout(Duration::from_secs(60), poll_retry_log_found.notified())
                .await
                .unwrap();

            // Start a new WF on main integration server.
            let client_for_integ_server = get_integ_client(
                NAMESPACE.to_string(),
                rt.telemetry().get_temporal_metric_meter(),
            )
            .await;
            let wf_2 = client_for_integ_server
                .start_workflow(
                    UntypedWorkflow::new(wf_name),
                    RawValue::default(),
                    WorkflowStartOptions::new(task_queue, wf_name)
                        .execution_timeout(Duration::from_secs(60))
                        .build(),
                )
                .await
                .unwrap();
            let wf_2_run_id = wf_2.run_id().unwrap().to_string();

            // Switch worker over to the main integration server.
            // The polling started on the initial server should complete with a task from the new server.
            worker
                .replace_client(client_for_integ_server.connection().clone())
                .unwrap();
            let act_2 = tokio::time::timeout(Duration::from_secs(60), poll_join_handle)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(act_2.run_id, wf_2_run_id);
        })
    }
    .catch_unwind()
    .await;

    // Cleaning up spawned background tasks if they're still running.
    for handle in &*abort_handles.lock().unwrap() {
        handle.abort();
    }

    // If the test panicked, we may or may not need to shut down the server here.
    // If the test succeeded, the server should always be shut down by this point.
    let server = initial_server.lock().unwrap().take();
    if let Some(mut server) = server {
        let _ = server.shutdown().await;
        assert_matches!(
            result,
            Err(_),
            "Server should have been shut down during the test"
        );
    }

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
