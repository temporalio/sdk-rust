use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode,
    body::Incoming,
    client::conn::http1 as client_http1,
    header::{CACHE_CONTROL, CONTENT_TYPE, HOST, HeaderValue},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    env, fs,
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, grpc::WorkflowService,
};
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution,
    history::v1::HistoryEvent,
    query::v1::WorkflowQuery,
    workflowservice::v1::{
        GetWorkflowExecutionHistoryRequest, QueryWorkflowRequest, SignalWorkflowExecutionRequest,
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, Runtime, SyncWorkflowContext, Worker, WorkerOptions, WorkflowContext,
    WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use tonic::IntoRequest;
use url::Url;
use uuid::Uuid;

const SERVER_BINARY_ENV: &str = "LOCAL_FIRST_DEMO_SERVER";
const UI_SERVER_BINARY_ENV: &str = "LOCAL_FIRST_DEMO_UI_SERVER";
const WORKFLOW_TYPE: &str = "local-first-core-loop";
const ACTIVITY_TYPE: &str = "local-first-core-activity";
const SIGNAL_NAME: &str = "next_turn";
const TURN_OPERATIONS: [&[&str]; 3] = [
    &[
        "Search the repository for timeout handling",
        "Inspect the relevant workflow code",
        "Reproduce the failing behavior",
    ],
    &[
        "Edit the timeout implementation",
        "Add regression coverage",
        "Run the focused test suite",
    ],
    &[
        "Inspect the final diff",
        "Run formatting and lint checks",
        "Summarize the completed changes",
    ],
];
const INDEX_HTML: &str = include_str!("local_first_agent_demo/index.html");
const APP_CSS: &str = include_str!("local_first_agent_demo/app.css");
const APP_JS: &str = include_str!("local_first_agent_demo/app.js");

#[derive(Parser)]
#[command(about = "Interactive local-first Temporal coding-agent demo")]
struct Options {
    #[arg(long, default_value_t = 0)]
    http_port: u16,
    #[arg(
        long,
        default_value_t = 1_000,
        value_parser = clap::value_parser!(u64).range(1..=20_000)
    )]
    activity_delay_ms: u64,
    #[arg(long)]
    smoke: bool,
}

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
    #[serde(default)]
    sync_address: String,
}

#[derive(Deserialize)]
struct UiReadyMessage {
    address: String,
}

struct DemoServer {
    child: Child,
    _state_directory: Option<tempfile::TempDir>,
}

impl Drop for DemoServer {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None))
            && let Err(err) = self
                .child
                .kill()
                .and_then(|_| self.child.wait().map(|_| ()))
        {
            eprintln!("failed to stop local-first demo server: {err}");
        }
    }
}

#[derive(Clone)]
struct AppContext {
    local_connection: Connection,
    upstream_connection: Connection,
    ready: Arc<ReadyMessage>,
    submitted_turns: Arc<Mutex<usize>>,
    sync_error: Arc<Mutex<Option<String>>>,
    local_ui_address: Arc<str>,
    upstream_ui_address: Arc<str>,
}

struct AgentActivities {
    delay: Duration,
}

#[activities]
impl AgentActivities {
    #[activity(name = ACTIVITY_TYPE)]
    async fn run_tool(self: Arc<Self>, _ctx: ActivityContext) -> Result<(), ActivityError> {
        tokio::time::sleep(self.delay).await;
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct AgentWorkflow {
    requested_turns: usize,
    completed_turns: usize,
}

#[workflow_methods]
impl AgentWorkflow {
    #[run(name = WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>, _iterations: i32) -> WorkflowResult<()> {
        loop {
            ctx.wait_condition(|state| state.requested_turns > state.completed_turns)
                .await?;
            let turn = ctx.state(|state| state.completed_turns);
            let Some(operations) = TURN_OPERATIONS.get(turn) else {
                ctx.wait_condition(|_| false).await?;
                continue;
            };
            for summary in *operations {
                ctx.execute_activity(
                    AgentActivities::run_tool,
                    (),
                    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                        .summary((*summary).to_owned())
                        .build(),
                )
                .await?;
            }
            ctx.state_mut(|state| state.completed_turns += 1);
        }
    }

    #[signal(name = SIGNAL_NAME)]
    fn next_turn(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        if self.requested_turns < TURN_OPERATIONS.len() {
            self.requested_turns += 1;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::parse();
    let (upstream_server, upstream_ready) = start_upstream_server()?;
    let (bridge_server, ready) = start_bridge_server(&upstream_ready)?;
    validate_ready_message(&ready)?;
    let (local_ui, local_ui_ready) = start_ui_server(&ready.local_address, "/ui/local")?;
    let (upstream_ui, upstream_ui_ready) =
        start_ui_server(&ready.upstream_address, "/ui/upstream")?;
    let ready = Arc::new(ready);
    let (local_connection, local_client) = connect(&ready.local_address, &ready.namespace).await?;
    let (upstream_connection, _) = connect(&ready.upstream_address, &ready.namespace).await?;
    let runtime = Runtime::from_current_tokio(Default::default())?;
    let worker_options = WorkerOptions::new(ready.task_queue.clone())
        .register_workflow::<AgentWorkflow>()?
        .register_activities(AgentActivities {
            delay: Duration::from_millis(options.activity_delay_ms),
        })
        .build();
    let mut worker = Worker::new(&runtime, local_client, worker_options)?;
    let shutdown = worker.shutdown_handle();
    let app = AppContext {
        local_connection,
        upstream_connection,
        ready,
        submitted_turns: Arc::new(Mutex::new(0)),
        sync_error: Arc::new(Mutex::new(None)),
        local_ui_address: local_ui_ready.address.into(),
        upstream_ui_address: upstream_ui_ready.address.into(),
    };
    wait_for_initial_histories(&app).await?;
    let (http_address, http_task) = start_http_server(app.clone(), options.http_port).await?;
    spawn_boundary_sync(app.clone(), 0);
    println!("Local-first agent demo: http://{http_address}");
    println!("Activity delay: {} ms", options.activity_delay_ms);

    let smoke_result = if options.smoke {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = result_tx.send(run_smoke(app).await);
            shutdown();
        });
        Some(result_rx)
    } else {
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown();
        });
        None
    };

    let worker_result = worker.run().await;
    http_task.abort();
    worker_result?;
    if let Some(smoke_result) = smoke_result {
        smoke_result.await.context("smoke runner stopped")??;
        println!("Smoke demo completed nine Activities with synchronized histories");
    }
    drop(upstream_ui);
    drop(local_ui);
    drop(bridge_server);
    drop(upstream_server);
    Ok(())
}

fn start_upstream_server() -> Result<(DemoServer, ReadyMessage)> {
    let binary = env::var_os(SERVER_BINARY_ENV).context(format!(
        "{SERVER_BINARY_ENV} must point to local-first-demo-server"
    ))?;
    let mut command = Command::new(binary);
    command.arg("--mode").arg("upstream");
    start_demo_server(command, None)
}

fn start_bridge_server(upstream: &ReadyMessage) -> Result<(DemoServer, ReadyMessage)> {
    let binary = env::var_os(SERVER_BINARY_ENV).context(format!(
        "{SERVER_BINARY_ENV} must point to local-first-demo-server"
    ))?;
    let state_directory = tempfile::Builder::new()
        .prefix("local-first-agent-bridge-")
        .tempdir()?;
    set_private_directory_permissions(state_directory.path())?;
    let mut command = Command::new(binary);
    command
        .arg("--mode")
        .arg("bridge")
        .arg("--state-dir")
        .arg(state_directory.path())
        .arg("--sync-interval")
        .arg("1m")
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
        .arg(&upstream.task_queue);
    start_demo_server(command, Some(state_directory))
}

fn start_ui_server(
    temporal_address: &str,
    public_path: &str,
) -> Result<(DemoServer, UiReadyMessage)> {
    let binary = env::var_os(UI_SERVER_BINARY_ENV).context(format!(
        "{UI_SERVER_BINARY_ENV} must point to local-first-demo-ui"
    ))?;
    let mut command = Command::new(binary);
    command
        .arg("--temporal-address")
        .arg(temporal_address)
        .arg("--public-path")
        .arg(public_path);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("start Temporal UI server")?;
    let stdout = child.stdout.take().context("capture Temporal UI output")?;
    let mut line = String::new();
    if BufReader::new(stdout).read_line(&mut line)? == 0 {
        bail!("Temporal UI server exited before becoming ready");
    }
    let ready = serde_json::from_str(&line).context("decode Temporal UI ready message")?;
    Ok((
        DemoServer {
            child,
            _state_directory: None,
        },
        ready,
    ))
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn validate_ready_message(ready: &ReadyMessage) -> Result<()> {
    if ready.workflow_type != WORKFLOW_TYPE || ready.activity_type != ACTIVITY_TYPE {
        bail!("demo server and Worker registrations do not match");
    }
    if ready.sync_address.is_empty() {
        bail!("bridge did not advertise an explicit synchronization endpoint");
    }
    Ok(())
}

fn start_demo_server(
    mut command: Command,
    state_directory: Option<tempfile::TempDir>,
) -> Result<(DemoServer, ReadyMessage)> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("start local-first demo server")?;
    let stdout = child.stdout.take().context("capture demo server output")?;
    let mut line = String::new();
    if BufReader::new(stdout).read_line(&mut line)? == 0 {
        bail!("demo server exited before becoming ready");
    }
    let ready = serde_json::from_str(&line).context("decode demo server ready message")?;
    Ok((
        DemoServer {
            child,
            _state_directory: state_directory,
        },
        ready,
    ))
}

async fn connect(address: &str, namespace: &str) -> Result<(Connection, Client)> {
    let connection = Connection::connect(
        ConnectionOptions::new(Url::parse(&format!("http://{address}"))?)
            .identity("local-first-agent-demo")
            .client_name("sdk-core-local-first-agent-demo")
            .client_version("0.1.0")
            .build(),
    )
    .await?;
    let client = Client::new(
        connection.clone(),
        ClientOptions::new(namespace.to_owned()).build(),
    )?;
    Ok((connection, client))
}

async fn start_http_server(
    app: AppContext,
    requested_port: u16,
) -> Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", requested_port)).await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let app = app.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| handle_request(request, app.clone()));
                if let Err(err) = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    eprintln!("demo HTTP connection failed: {err}");
                }
            });
        }
    });
    Ok((address, task))
}

async fn handle_request(
    request: Request<Incoming>,
    app: AppContext,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let ui_address = if request.uri().path().starts_with("/ui/local") {
        Some(app.local_ui_address.clone())
    } else if request.uri().path().starts_with("/ui/upstream") {
        Some(app.upstream_ui_address.clone())
    } else {
        None
    };
    if let Some(address) = ui_address {
        return Ok(match proxy_ui_request(request, &address).await {
            Ok(response) => response,
            Err(err) => error_response(StatusCode::BAD_GATEWAY, err.to_string()),
        });
    }
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => static_response("text/html; charset=utf-8", INDEX_HTML),
        (&Method::GET, "/app.css") => static_response("text/css; charset=utf-8", APP_CSS),
        (&Method::GET, "/app.js") => static_response("text/javascript; charset=utf-8", APP_JS),
        (&Method::GET, "/api/state") => match state_json(&app).await {
            Ok(state) => json_response(StatusCode::OK, state),
            Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        },
        (&Method::POST, "/api/turn") => match submit_turn(&app).await {
            Ok(turn) => json_response(StatusCode::ACCEPTED, json!({ "turn": turn })),
            Err(err) => error_response(StatusCode::CONFLICT, err.to_string()),
        },
        _ => error_response(StatusCode::NOT_FOUND, "not found"),
    };
    Ok(response)
}

fn static_response(content_type: &'static str, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .header(CACHE_CONTROL, "no-store")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .expect("static response is valid")
}

fn json_response(status: StatusCode, body: Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .header(CACHE_CONTROL, "no-store")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("JSON response is valid")
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response<Full<Bytes>> {
    json_response(status, json!({ "error": message.into() }))
}

async fn proxy_ui_request(
    mut request: Request<Incoming>,
    address: &str,
) -> Result<Response<Full<Bytes>>> {
    request
        .headers_mut()
        .insert(HOST, HeaderValue::from_str(address)?);
    let stream = tokio::net::TcpStream::connect(address).await?;
    let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("Temporal UI proxy connection failed: {err}");
        }
    });
    let response = sender.send_request(request).await?;
    let (parts, body) = response.into_parts();
    let body = body.collect().await?.to_bytes();
    Ok(Response::from_parts(parts, Full::new(body)))
}

fn spawn_boundary_sync(app: AppContext, completed_turns: usize) {
    tokio::spawn(async move {
        if let Err(err) = synchronize_boundary(&app, completed_turns).await {
            eprintln!("explicit turn synchronization failed: {err}");
            *app.sync_error.lock() = Some(err.to_string());
        }
    });
}

async fn synchronize_boundary(app: &AppContext, target_completed_turns: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let local_history = fetch_history(&app.local_connection, &app.ready).await?;
            if completed_turns(&local_history) == target_completed_turns
                && workflow_quiescent(&local_history)
            {
                return request_explicit_sync(&app.ready.sync_address).await;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("waiting for the workflow to reach its user-input boundary")?
}

async fn request_explicit_sync(address: &str) -> Result<()> {
    let stream = tokio::net::TcpStream::connect(address).await?;
    let (mut sender, connection) = client_http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("explicit synchronization connection failed: {err}");
        }
    });
    let request = Request::builder()
        .method(Method::POST)
        .uri("/sync")
        .header(HOST, address)
        .body(Full::new(Bytes::new()))?;
    let response = sender.send_request(request).await?;
    if response.status() != StatusCode::ACCEPTED {
        bail!(
            "bridge rejected explicit synchronization: {}",
            response.status()
        );
    }
    Ok(())
}

async fn submit_turn(app: &AppContext) -> Result<usize> {
    let (local_history, upstream_history) = histories(app).await?;
    let completed_turns = completed_turns(&local_history);
    if local_history != upstream_history || !workflow_quiescent(&local_history) {
        bail!("wait for the current turn to synchronize upstream");
    }
    let turn = {
        let mut submitted_turns = app.submitted_turns.lock();
        if *submitted_turns != completed_turns {
            bail!("the current turn is still running");
        }
        if *submitted_turns >= TURN_OPERATIONS.len() {
            bail!("the canned session is complete");
        }
        let turn = *submitted_turns;
        *submitted_turns += 1;
        turn
    };
    if let Err(err) = signal_local_workflow(app).await {
        *app.submitted_turns.lock() = turn;
        return Err(err);
    }
    spawn_boundary_sync(app.clone(), turn + 1);
    Ok(turn)
}

async fn signal_local_workflow(app: &AppContext) -> Result<()> {
    let mut connection = app.local_connection.clone();
    connection
        .signal_workflow_execution(
            SignalWorkflowExecutionRequest {
                namespace: app.ready.namespace.clone(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: app.ready.workflow_id.clone(),
                    run_id: app.ready.run_id.clone(),
                }),
                signal_name: SIGNAL_NAME.to_owned(),
                identity: "local-first-agent-demo".to_owned(),
                request_id: Uuid::new_v4().to_string(),
                ..Default::default()
            }
            .into_request(),
        )
        .await?;
    Ok(())
}

async fn state_json(app: &AppContext) -> Result<Value> {
    let (local_history, upstream_history) = histories(app).await?;
    let submitted_turns = *app.submitted_turns.lock();
    let completed_turns = completed_turns(&local_history);
    let active_turn = (completed_turns < submitted_turns).then_some(completed_turns);
    let aligned = local_history == upstream_history;
    let quiescent = workflow_quiescent(&local_history);
    let sync_error = app.sync_error.lock().clone();
    let phase = if sync_error.is_some() {
        "error"
    } else if active_turn.is_some() {
        "running"
    } else if !aligned || !quiescent {
        "syncing"
    } else if completed_turns == TURN_OPERATIONS.len() {
        "complete"
    } else {
        "ready"
    };
    Ok(json!({
        "phase": phase,
        "completed_turns": completed_turns,
        "active_turn": active_turn,
        "can_submit": phase == "ready",
        "error": sync_error,
        "histories_aligned": aligned,
        "sync_cursor": upstream_history.last().map(|event| event.event_id).unwrap_or_default(),
        "local_event_count": local_history.len(),
        "upstream_event_count": upstream_history.len(),
        "tools": activities_json(&local_history),
        "workflow_id": app.ready.workflow_id,
        "run_id": app.ready.run_id,
        "local_ui_path": workflow_ui_path("/ui/local", &app.ready),
        "upstream_ui_path": workflow_ui_path("/ui/upstream", &app.ready),
    }))
}

fn workflow_ui_path(prefix: &str, ready: &ReadyMessage) -> String {
    format!(
        "{prefix}/namespaces/{}/workflows/{}/{}/history",
        ready.namespace, ready.workflow_id, ready.run_id
    )
}

async fn histories(app: &AppContext) -> Result<(Vec<HistoryEvent>, Vec<HistoryEvent>)> {
    tokio::try_join!(
        async {
            fetch_history(&app.local_connection, &app.ready)
                .await
                .context("fetch local workflow history")
        },
        async {
            fetch_history(&app.upstream_connection, &app.ready)
                .await
                .context("fetch upstream workflow history")
        },
    )
}

fn workflow_quiescent(history: &[HistoryEvent]) -> bool {
    history.last().is_some_and(|event| {
        event.event_type().as_str_name() == "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"
    })
}

fn completed_turns(history: &[HistoryEvent]) -> usize {
    let completed_activities = history
        .iter()
        .filter(|event| event.event_type().as_str_name() == "EVENT_TYPE_ACTIVITY_TASK_COMPLETED")
        .count();
    let mut total = 0;
    TURN_OPERATIONS
        .iter()
        .take_while(|operations| {
            total += operations.len();
            completed_activities >= total
        })
        .count()
}

fn activities_json(history: &[HistoryEvent]) -> Vec<Value> {
    let completed = history
        .iter()
        .filter(|event| event.event_type().as_str_name() == "EVENT_TYPE_ACTIVITY_TASK_COMPLETED")
        .count();
    history
        .iter()
        .filter(|event| event.event_type().as_str_name() == "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED")
        .enumerate()
        .filter_map(|(index, event)| {
            let (turn, step) = activity_position(index)?;
            Some(json!({
                "turn": turn,
                "step": step,
                "summary": event_summary(event),
                "status": if index < completed { "completed" } else { "running" },
            }))
        })
        .collect()
}

fn activity_position(mut index: usize) -> Option<(usize, usize)> {
    for (turn, operations) in TURN_OPERATIONS.iter().enumerate() {
        if index < operations.len() {
            return Some((turn, index));
        }
        index -= operations.len();
    }
    None
}

fn event_summary(event: &HistoryEvent) -> Option<String> {
    let payload = event.user_metadata.as_ref()?.summary.as_ref()?;
    serde_json::from_slice(&payload.data).ok()
}

async fn fetch_history(connection: &Connection, ready: &ReadyMessage) -> Result<Vec<HistoryEvent>> {
    let mut connection = connection.clone();
    let response = connection
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
        .await?
        .into_inner();
    Ok(response.history.unwrap_or_default().events)
}

async fn wait_for_initial_histories(app: &AppContext) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match histories(app).await {
                Ok((local, upstream)) if !local.is_empty() && !upstream.is_empty() => return Ok(()),
                Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .context("waiting for the initial local and upstream histories")?
}

async fn run_smoke(app: AppContext) -> Result<()> {
    wait_until_ready(&app, 0).await?;
    verify_unsupported_query_does_not_stop_bridge(&app).await?;
    for turn in 0..TURN_OPERATIONS.len() {
        if submit_turn(&app).await? != turn {
            bail!("unexpected submitted turn");
        }
        wait_until_ready(&app, turn + 1).await?;
    }
    let state = state_json(&app).await?;
    if state["phase"] != "complete"
        || state["histories_aligned"] != true
        || state["tools"].as_array().map(Vec::len) != Some(9)
    {
        bail!("final demo state was not complete and synchronized");
    }
    let summaries = state["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["summary"].as_str())
        .collect::<Vec<_>>();
    let expected = TURN_OPERATIONS
        .into_iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    if summaries != expected {
        bail!("Activity summaries were not preserved in history");
    }
    Ok(())
}

async fn verify_unsupported_query_does_not_stop_bridge(app: &AppContext) -> Result<()> {
    let mut connection = app.upstream_connection.clone();
    let query_result = tokio::time::timeout(
        Duration::from_secs(10),
        connection.query_workflow(
            QueryWorkflowRequest {
                namespace: app.ready.namespace.clone(),
                execution: Some(WorkflowExecution {
                    workflow_id: app.ready.workflow_id.clone(),
                    run_id: app.ready.run_id.clone(),
                }),
                query: Some(WorkflowQuery {
                    query_type: "__temporal_workflow_metadata".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }
            .into_request(),
        ),
    )
    .await
    .context("waiting for the unsupported upstream Query to be rejected")?;
    if query_result.is_ok() {
        bail!("unsupported upstream Query unexpectedly succeeded");
    }
    wait_until_ready(app, 0)
        .await
        .context("bridge stopped after rejecting an unsupported upstream Query")
}

async fn wait_until_ready(app: &AppContext, completed_turns: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let state = state_json(app).await?;
            let completed = state["completed_turns"].as_u64().unwrap_or_default() as usize;
            let ready = state["phase"] == "ready" || state["phase"] == "complete";
            if completed == completed_turns && ready {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("waiting for the local turn and upstream synchronization")?
}
