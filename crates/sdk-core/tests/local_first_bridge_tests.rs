use serde::Deserialize;
use std::{
    env, fs,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowExecutionInfo,
    WorkflowGetResultOptions, grpc::WorkflowService,
};
use temporalio_common::{
    protos::{
        coresdk::{
            ActivityTaskCompletion,
            activity_result::ActivityExecutionResult,
            activity_task::activity_task,
            workflow_activation::workflow_activation_job,
            workflow_commands::{
                ActivityCancellationType, CompleteWorkflowExecution, ScheduleActivity,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::{Payload, WorkflowExecution},
            enums::v1::EventType,
            history::v1::HistoryEvent,
            workflowservice::v1::GetWorkflowExecutionHistoryRequest,
        },
    },
    telemetry::TelemetryOptions,
    worker::WorkerTaskTypes,
};
use temporalio_sdk_core::{
    CoreRuntime, RuntimeOptions, TunerHolder, WorkerConfig, WorkerVersioningStrategy, init_worker,
    test_help::drain_pollers_and_shutdown,
};
use tonic::IntoRequest;
use url::Url;
use uuid::Uuid;

const SERVER_BINARY_ENV: &str = "LOCAL_FIRST_DEMO_SERVER";

#[derive(Deserialize)]
struct ReadyMessage {
    upstream_address: String,
    local_address: String,
    namespace: String,
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    activity_type: String,
    task_queue: String,
    iterations: u32,
}

struct DemoServer {
    child: Child,
    state_directory: Option<PathBuf>,
}

impl Drop for DemoServer {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(err) = self.child.kill() {
                    eprintln!("failed to kill local-first demo server: {err}");
                }
                if let Err(err) = self.child.wait() {
                    eprintln!("failed to reap local-first demo server: {err}");
                }
            }
            Err(err) => eprintln!("failed to inspect local-first demo server: {err}"),
        }
        if let Some(state_directory) = &self.state_directory
            && let Err(err) = fs::remove_dir_all(state_directory)
        {
            eprintln!("failed to remove local-first demo state: {err}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn local_first_core_bridge() {
    let (upstream_server, upstream_ready) = start_upstream_server();
    let (bridge_server, ready) = start_bridge_server(&upstream_ready);
    let (local_connection, _) = connect(&ready.local_address, &ready.namespace).await;
    let (upstream_connection, upstream_client) =
        connect(&ready.upstream_address, &ready.namespace).await;

    let upstream_baseline = tokio::time::timeout(
        Duration::from_secs(5),
        fetch_history(&upstream_connection, &ready),
    )
    .await
    .expect("initial upstream history fetch completed");
    assert_eq!(upstream_baseline.len(), 2);

    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()
            .unwrap(),
    )
    .unwrap();
    let core = init_worker(
        &runtime,
        WorkerConfig::builder()
            .namespace(ready.namespace.clone())
            .task_queue(ready.task_queue.clone())
            .task_types(WorkerTaskTypes::all())
            .tuner(Arc::new(TunerHolder::fixed_size(1, 1, 1, 1)))
            .max_cached_workflows(10_usize)
            .ignore_evicts_on_shutdown(true)
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "local-first-core-steel-thread".to_string(),
            })
            .build()
            .unwrap(),
        local_connection.clone(),
    )
    .unwrap();
    core.validate().await.unwrap();

    let mut activation = core.poll_workflow_activation().await.unwrap();
    assert_eq!(activation.run_id, ready.run_id);
    let initialization = activation
        .jobs
        .iter()
        .find_map(|job| match job.variant.as_ref() {
            Some(workflow_activation_job::Variant::InitializeWorkflow(initialization)) => {
                Some(initialization)
            }
            _ => None,
        })
        .expect("first Core activation initializes the workflow");
    assert_eq!(initialization.workflow_type, ready.workflow_type);

    let mut activity_results = Vec::with_capacity(ready.iterations as usize);
    let mut first_synchronized_prefix = None;
    for iteration in 0..ready.iterations {
        if iteration > 0 {
            assert!(activation.jobs.iter().any(|job| matches!(
                job.variant.as_ref(),
                Some(workflow_activation_job::Variant::ResolveActivity(resolution))
                    if resolution.seq == iteration - 1
            )));
        }

        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            activation.run_id.clone(),
            ScheduleActivity {
                seq: iteration,
                activity_id: format!("activity-{iteration}"),
                activity_type: ready.activity_type.clone(),
                task_queue: ready.task_queue.clone(),
                start_to_close_timeout: Some(Duration::from_secs(5).try_into().unwrap()),
                cancellation_type: ActivityCancellationType::TryCancel as i32,
                ..Default::default()
            }
            .into(),
        ))
        .await
        .unwrap();

        let activity_task = core.poll_activity_task().await.unwrap();
        let start = match activity_task.variant.as_ref() {
            Some(activity_task::Variant::Start(start)) => start,
            other => panic!("expected a normal Activity start, got {other:?}"),
        };
        assert_eq!(start.activity_type, ready.activity_type);
        let result = Payload {
            data: format!("activity-{iteration}").into_bytes(),
            ..Default::default()
        };
        activity_results.push(result.data.clone());
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: activity_task.task_token,
            result: Some(ActivityExecutionResult::ok(result)),
        })
        .await
        .unwrap();
        activation = core.poll_workflow_activation().await.unwrap();

        if iteration == 0 {
            let local_at_boundary = fetch_history(&local_connection, &ready).await;
            let synchronized =
                wait_for_history_after(&upstream_connection, &ready, upstream_baseline.len()).await;
            assert!(synchronized.len() <= local_at_boundary.len());
            assert_eq!(
                synchronized.as_slice(),
                &local_at_boundary[..synchronized.len()]
            );
            eprintln!(
                "bridge synchronized an intermediate {}-event prefix while local execution remained active",
                synchronized.len()
            );
            first_synchronized_prefix = Some(synchronized);
        }
    }

    assert!(activation.jobs.iter().any(|job| matches!(
        job.variant.as_ref(),
        Some(workflow_activation_job::Variant::ResolveActivity(resolution))
            if resolution.seq == ready.iterations - 1
    )));
    let workflow_result = Payload {
        data: activity_results.concat(),
        ..Default::default()
    };
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        CompleteWorkflowExecution {
            result: Some(workflow_result.clone()),
        }
        .into(),
    ))
    .await
    .unwrap();

    let local_history = tokio::time::timeout(
        Duration::from_secs(5),
        fetch_history(&local_connection, &ready),
    )
    .await
    .expect("local history fetch completed");
    assert!(local_history.len() > 2);
    assert_eq!(
        count_events(&local_history, EventType::ActivityTaskScheduled),
        ready.iterations as usize
    );
    assert_eq!(
        count_events(&local_history, EventType::ActivityTaskStarted),
        ready.iterations as usize
    );
    assert_eq!(
        count_events(&local_history, EventType::ActivityTaskCompleted),
        ready.iterations as usize
    );
    assert!(count_events(&local_history, EventType::WorkflowTaskCompleted) >= 4);
    assert!(
        first_synchronized_prefix
            .as_ref()
            .expect("an intermediate synchronization was observed")
            .len()
            < local_history.len()
    );
    eprintln!(
        "Core built {} local events across {} full Activities",
        local_history.len(),
        ready.iterations
    );

    let handle = WorkflowExecutionInfo::builder()
        .namespace(ready.namespace.clone())
        .workflow_id(ready.workflow_id.clone())
        .maybe_run_id(Some(ready.run_id.clone()))
        .build()
        .bind_untyped(upstream_client);
    let upstream_result = tokio::time::timeout(
        Duration::from_secs(45),
        handle.get_result(WorkflowGetResultOptions::default()),
    )
    .await
    .expect("history sync completed before the demo timeout")
    .unwrap();
    assert_eq!(upstream_result.payloads, vec![workflow_result]);
    eprintln!("upstream returned the Core-produced workflow result after synchronization");

    let upstream_after_sync = tokio::time::timeout(
        Duration::from_secs(5),
        fetch_history(&upstream_connection, &ready),
    )
    .await
    .expect("post-sync upstream history fetch completed");
    assert_eq!(local_history, upstream_after_sync);
    eprintln!("local and upstream histories are protobuf-identical");

    drain_pollers_and_shutdown(&core).await;
    core.finalize_shutdown().await;
    drop(bridge_server);
    drop(upstream_server);
}

fn start_upstream_server() -> (DemoServer, ReadyMessage) {
    let binary = env::var_os(SERVER_BINARY_ENV)
        .unwrap_or_else(|| panic!("{SERVER_BINARY_ENV} must point to local-first-demo-server"));
    let mut command = Command::new(binary);
    command.arg("--mode").arg("upstream");
    start_demo_server(command, None)
}

fn start_bridge_server(upstream: &ReadyMessage) -> (DemoServer, ReadyMessage) {
    let binary = env::var_os(SERVER_BINARY_ENV)
        .unwrap_or_else(|| panic!("{SERVER_BINARY_ENV} must point to local-first-demo-server"));
    let state_directory = env::temp_dir().join(format!("local-first-core-{}", Uuid::new_v4()));
    fs::create_dir(&state_directory).unwrap();
    let mut command = Command::new(binary);
    command
        .arg("--mode")
        .arg("bridge")
        .arg("--state-dir")
        .arg(&state_directory)
        .arg("--sync-interval")
        .arg("3s")
        .arg("--fail-sync-attempts")
        .arg("2")
        .arg("--upstream-address")
        .arg(&upstream.upstream_address)
        .arg("--namespace")
        .arg(&upstream.namespace)
        .arg("--workflow-id")
        .arg(&upstream.workflow_id)
        .arg("--run-id")
        .arg(&upstream.run_id)
        .arg("--workflow-type")
        .arg(&upstream.workflow_type)
        .arg("--activity-type")
        .arg(&upstream.activity_type)
        .arg("--task-queue")
        .arg(&upstream.task_queue)
        .arg("--iterations")
        .arg(upstream.iterations.to_string());
    start_demo_server(command, Some(state_directory))
}

fn start_demo_server(
    mut command: Command,
    state_directory: Option<PathBuf>,
) -> (DemoServer, ReadyMessage) {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let server = DemoServer {
        child,
        state_directory,
    };
    let mut line = String::new();
    let bytes_read = BufReader::new(stdout).read_line(&mut line).unwrap();
    assert!(bytes_read > 0, "demo server exited before becoming ready");
    let ready = serde_json::from_str(&line).unwrap();
    (server, ready)
}

async fn connect(address: &str, namespace: &str) -> (Connection, Client) {
    let connection = Connection::connect(
        ConnectionOptions::new(Url::parse(&format!("http://{address}")).unwrap())
            .identity("local-first-core-steel-thread")
            .client_name("sdk-core-local-first-demo")
            .client_version("0.1.0")
            .build(),
    )
    .await
    .unwrap();
    let client = Client::new(
        connection.clone(),
        ClientOptions::new(namespace.to_string()).build(),
    )
    .unwrap();
    (connection, client)
}

async fn fetch_history(connection: &Connection, ready: &ReadyMessage) -> Vec<HistoryEvent> {
    let mut connection = connection.clone();
    connection
        .get_workflow_execution_history(
            GetWorkflowExecutionHistoryRequest {
                namespace: ready.namespace.clone(),
                execution: Some(WorkflowExecution {
                    workflow_id: ready.workflow_id.clone(),
                    run_id: ready.run_id.clone(),
                }),
                skip_archival: true,
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap()
        .into_inner()
        .history
        .unwrap()
        .events
}

async fn wait_for_history_after(
    connection: &Connection,
    ready: &ReadyMessage,
    event_count: usize,
) -> Vec<HistoryEvent> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let history = fetch_history(connection, ready).await;
            if history.len() > event_count {
                return history;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("bridge synchronized an intermediate history prefix")
}

fn count_events(history: &[HistoryEvent], event_type: EventType) -> usize {
    history
        .iter()
        .filter(|event| event.event_type() == event_type)
        .count()
}
