//! A legacy query is answered after replaying all history the server sent with it, which can
//! include events newer than the last completed workflow task. Workflow code reacting to those
//! events runs before any workflow task exists to commit its effects. These tests check that the
//! history committed by the next real workflow task still replays deterministically.

use crate::{
    internal_flags::CoreInternalFlags,
    prost_dur,
    replay::TestHistoryBuilder,
    test_help::{
        MocksHolder, PollWFTRespExt, ResponseType, WorkerExt, hist_to_poll_resp, mock_worker,
        query_ok,
    },
    worker::client::mocks::mock_worker_client,
};
use futures_util::{StreamExt, stream};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use temporalio_common::protos::{
    coresdk::{
        common::NamespacedWorkflowExecution,
        workflow_activation::{WorkflowActivation, workflow_activation_job::Variant as Job},
        workflow_commands::{
            ModifyWorkflowProperties, SignalExternalWorkflowExecution, StartTimer, UpdateResponse,
            signal_external_workflow_execution, update_response, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::{Command, command::Attributes as CmdAttrs},
        common::v1::{Memo, Payload},
        enums::v1::EventType,
        history::v1::{TimerStartedEventAttributes, WorkflowPropertiesModifiedEventAttributes},
        protocol::v1::Message as ProtocolMessage,
        query::v1::WorkflowQuery,
        workflowservice::v1::{PollWorkflowTaskQueueResponse, RespondQueryTaskCompletedResponse},
    },
};
use tokio::sync::Notify;

const WFID: &str = "legacy-query-replay";
const UPDATE_ID: &str = "upd";

/// Stand-in for a workflow that signals another workflow, upserts a memo once that signal is
/// acknowledged, starts a timer per incoming signal, and accepts and completes updates. Like the
/// Python SDK, it handles updates and signals before other jobs in the same activation, which is
/// what makes the grouping of jobs into activations visible in the command order.
fn lang(act: &WorkflowActivation) -> Vec<workflow_command::Variant> {
    let jobs: Vec<&Job> = act.jobs.iter().filter_map(|j| j.variant.as_ref()).collect();
    let mut cmds = vec![];
    for job in &jobs {
        match job {
            Job::QueryWorkflow(q) => cmds.push(query_ok(q.query_id.clone(), "answer")),
            Job::DoUpdate(u) => {
                for response in [
                    update_response::Response::Accepted(()),
                    update_response::Response::Completed(Payload::default()),
                ] {
                    cmds.push(
                        UpdateResponse {
                            protocol_instance_id: u.protocol_instance_id.clone(),
                            response: Some(response),
                        }
                        .into(),
                    );
                }
            }
            Job::SignalWorkflow(_) => cmds.push(
                StartTimer {
                    seq: 1,
                    start_to_fire_timeout: Some(prost_dur!(from_secs(60))),
                }
                .into(),
            ),
            _ => {}
        }
    }
    for job in &jobs {
        match job {
            Job::InitializeWorkflow(_) => cmds.push(
                SignalExternalWorkflowExecution {
                    seq: 1,
                    target: Some(
                        signal_external_workflow_execution::Target::WorkflowExecution(
                            NamespacedWorkflowExecution {
                                namespace: "default".to_string(),
                                workflow_id: "signal-target".to_string(),
                                run_id: "".to_string(),
                            },
                        ),
                    ),
                    signal_name: "wake".to_string(),
                    ..Default::default()
                }
                .into(),
            ),
            Job::ResolveSignalExternalWorkflow(_) => cmds.push(
                ModifyWorkflowProperties {
                    upserted_memo: Some(Memo {
                        fields: HashMap::from([("progress".to_string(), Payload::default())]),
                    }),
                }
                .into(),
            ),
            _ => {}
        }
    }
    cmds
}

#[derive(Default)]
struct Observed {
    /// Job names of every activation, in order, including evictions.
    activations: Vec<Vec<String>>,
    completions: Vec<(Vec<Command>, Vec<ProtocolMessage>)>,
    wft_failures: Vec<String>,
    legacy_query_responses: usize,
}

impl Observed {
    fn server_calls(&self) -> usize {
        self.completions.len() + self.wft_failures.len() + self.legacy_query_responses
    }

    fn position(&self, job: &str) -> Option<usize> {
        self.activations
            .iter()
            .position(|a| a.iter().any(|j| j == job))
    }

    fn count(&self, job: &str) -> usize {
        self.activations
            .iter()
            .filter(|a| a.iter().any(|j| j == job))
            .count()
    }
}

/// Mocked server side: records what the worker reported, lets the next gated task through, and
/// signals `all_answered` once every task has been answered.
#[derive(Clone)]
struct Server {
    obs: Arc<Mutex<Observed>>,
    answered: Arc<Notify>,
    all_answered: Arc<Notify>,
    expected_calls: usize,
}

impl Server {
    fn record(&self, f: impl FnOnce(&mut Observed)) {
        let mut obs = self.obs.lock().unwrap();
        f(&mut obs);
        if obs.server_calls() >= self.expected_calls {
            self.all_answered.notify_one();
        }
        self.answered.notify_one();
    }
}

/// Each task is delivered only after the mocked server has answered the previous one when its
/// flag is set, so legacy queries and workflow tasks reach the worker in the order a server that
/// had already received the prior completion would hand them out.
async fn run(tasks: Vec<(bool, PollWorkflowTaskQueueResponse)>, cache: usize) -> Observed {
    let server = Server {
        obs: Arc::new(Mutex::new(Observed::default())),
        answered: Arc::new(Notify::new()),
        all_answered: Arc::new(Notify::new()),
        expected_calls: tasks.len(),
    };
    let mut client = mock_worker_client();
    {
        let server = server.clone();
        client
            .expect_complete_workflow_task()
            .returning(move |c, _| {
                server.record(|o| o.completions.push((c.commands, c.messages)));
                Ok(Default::default())
            });
    }
    {
        let server = server.clone();
        client.expect_respond_legacy_query().returning(move |_, _| {
            server.record(|o| o.legacy_query_responses += 1);
            Ok(RespondQueryTaskCompletedResponse::default())
        });
    }
    {
        let server = server.clone();
        client
            .expect_fail_workflow_task()
            .returning(move |_, _, failure| {
                let message = failure.map(|f| f.message).unwrap_or_default();
                server.record(|o| o.wft_failures.push(message));
                Ok(Default::default())
            });
    }
    let answered = server.answered.clone();
    let tasks = stream::iter(tasks)
        .then(move |(wait, resp)| {
            let answered = answered.clone();
            async move {
                if wait {
                    answered.notified().await;
                }
                resp
            }
        })
        .chain(stream::pending());
    let mut mock = MocksHolder::from_wft_stream(client, tasks);
    mock.worker_cfg(|wc| wc.max_cached_workflows = cache);
    let core = mock_worker(mock);
    // A task that brings no new work can be completed without activating lang, so waiting only on
    // activations would never notice the last answer.
    loop {
        let act = tokio::select! {
            biased;
            _ = server.all_answered.notified() => break,
            act = core.poll_workflow_activation() => act.unwrap(),
        };
        let jobs: Vec<String> = act
            .jobs
            .iter()
            .filter_map(|j| j.variant.as_ref())
            .map(|job| {
                match job {
                    Job::InitializeWorkflow(_) => "Init",
                    Job::ResolveSignalExternalWorkflow(_) => "ResolveSignalExternal",
                    Job::DoUpdate(_) => "DoUpdate",
                    Job::SignalWorkflow(_) => "Signal",
                    Job::QueryWorkflow(_) => "Query",
                    Job::RemoveFromCache(_) => "Evict",
                    _ => "Other",
                }
                .to_string()
            })
            .collect();
        let completion = if jobs.iter().any(|j| j == "Evict") {
            WorkflowActivationCompletion::empty(act.run_id.clone())
        } else {
            WorkflowActivationCompletion::from_cmds(act.run_id.clone(), lang(&act))
        };
        server.obs.lock().unwrap().activations.push(jobs);
        core.complete_workflow_activation(completion).await.unwrap();
    }
    core.drain_pollers_and_shutdown().await;
    std::mem::take(&mut *server.obs.lock().unwrap())
}

fn poll(t: &TestHistoryBuilder) -> PollWorkflowTaskQueueResponse {
    hist_to_poll_resp(t, WFID, ResponseType::AllHistory).resp
}

#[derive(Clone, Copy, PartialEq)]
enum NextWft {
    /// Carries an update request message and no new history beyond its own started event.
    Update,
    /// Has a new signal event in its history.
    Signal,
    /// Adds only its own started event.
    NothingNew,
}

/// History: 1 started, 2-4 WFT1, 5 signal initiated, 6 signal acknowledged, 7 WFT2 scheduled, then
/// optional signals and WFT2 started. Each legacy query is dispatched with the history up to that
/// point, after WFT1 completed, so the first one already sees the acknowledgement.
fn scenario(
    advancing_queries: usize,
    next: NextWft,
) -> (
    Vec<(bool, PollWorkflowTaskQueueResponse)>,
    TestHistoryBuilder,
) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let mut tasks = vec![(false, poll(&t))];
    t.add_workflow_task_completed();
    if cfg!(feature = "disable_workflow_task_chunking") {
        // Such a worker records this flag on the first completion and the server persists it, so
        // the history served afterwards carries it and every later task is replayed unchunked.
        t.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
    }
    let initiated = t.add_signal_wf("wake", "signal-target", "");
    t.add_external_signal_completed(initiated);
    t.add_workflow_task_scheduled();
    for i in 0..advancing_queries {
        if i > 0 {
            t.add_we_signaled("poke", vec![]);
        }
        // A non-sticky legacy query carries the full history read when it is dispatched, with no
        // started event of its own and the last started workflow task as the previous one.
        let mut query = poll(&t);
        query.query = Some(WorkflowQuery {
            query_type: "progress".to_owned(),
            ..Default::default()
        });
        query.started_event_id = 0;
        query.previous_started_event_id = 3;
        tasks.push((true, query));
    }
    if next == NextWft::Signal {
        t.add_we_signaled("poke", vec![]);
    }
    t.add_workflow_task_started();
    let mut wft = poll(&t);
    if next == NextWft::Update {
        wft.add_update_request(UPDATE_ID, wft.started_event_id - 1);
    }
    tasks.push((true, wft));
    (tasks, t)
}

/// Appends the events the server records for a workflow task completion, in command order.
fn commit(t: &mut TestHistoryBuilder, commands: &[Command], messages: &[ProtocolMessage]) {
    t.add_workflow_task_completed();
    let completed_id = t.current_event_id();
    let mut accepted_id = 0;
    for c in commands {
        match &c.attributes {
            Some(CmdAttrs::ModifyWorkflowPropertiesCommandAttributes(a)) => {
                t.add(WorkflowPropertiesModifiedEventAttributes {
                    workflow_task_completed_event_id: completed_id,
                    upserted_memo: a.upserted_memo.clone(),
                });
            }
            Some(CmdAttrs::StartTimerCommandAttributes(a)) => {
                t.add(TimerStartedEventAttributes {
                    timer_id: a.timer_id.clone(),
                    start_to_fire_timeout: a.start_to_fire_timeout,
                    workflow_task_completed_event_id: completed_id,
                });
            }
            Some(CmdAttrs::ProtocolMessageCommandAttributes(a)) => {
                let msg = messages
                    .iter()
                    .find(|m| m.id == a.message_id)
                    .expect("protocol message command must reference a sent message");
                if msg.id.ends_with("/accept") {
                    accepted_id = t.add_update_accepted(UPDATE_ID, "update_fn");
                } else {
                    t.add_update_completed(accepted_id);
                }
            }
            other => panic!("unexpected command {other:?}"),
        }
    }
    t.add_workflow_task_scheduled_and_started();
}

/// Runs the scenario on a cached worker, commits what the worker reported for the second workflow
/// task, and replays the result on a fresh worker.
async fn live_then_replay(advancing_queries: usize, next: NextWft) -> (Observed, Observed) {
    let (tasks, mut history) = scenario(advancing_queries, next);
    let live = run(tasks, 1).await;
    assert_eq!(live.wft_failures, Vec::<String>::new());
    assert_eq!(live.completions.len(), 2);
    assert_eq!(live.legacy_query_responses, advancing_queries);
    let memos = live
        .completions
        .iter()
        .flat_map(|(cmds, _)| cmds)
        .filter(|c| {
            matches!(
                c.attributes,
                Some(CmdAttrs::ModifyWorkflowPropertiesCommandAttributes(_))
            )
        })
        .count();
    assert_eq!(memos, 1, "workflow progress must be committed exactly once");

    let (cmds, msgs) = &live.completions[1];
    commit(&mut history, cmds, msgs);
    let replay = run(vec![(false, poll(&history))], 1).await;
    assert!(
        replay.wft_failures.is_empty(),
        "committed history must replay: {:?}\nlive activations: {:?}\nreplay activations: {:?}",
        replay.wft_failures,
        live.activations,
        replay.activations
    );
    (live, replay)
}

/// The query's replay applies the signal acknowledgement and the workflow upserts its memo. The
/// next task's update must still end up in the same activation as that acknowledgement, as it
/// does on replay, so the run is rebuilt before the update is delivered (exactly once).
#[tokio::test]
async fn update_after_query_advanced_workflow_commits_replayable_history() {
    let (live, _) = live_then_replay(1, NextWft::Update).await;
    assert_eq!(live.count("Evict"), 1);
    assert!(live.position("Evict") < live.position("DoUpdate"));
    assert_eq!(live.count("DoUpdate"), 1);
    let update_act = &live.activations[live.position("DoUpdate").unwrap()];
    assert!(update_act.contains(&"ResolveSignalExternal".to_string()));
}

/// Same as above, but the next task brings new work through history instead of a message.
#[tokio::test]
async fn signal_after_query_advanced_workflow_commits_replayable_history() {
    let (live, _) = live_then_replay(1, NextWft::Signal).await;
    assert_eq!(live.count("Evict"), 1);
    assert!(live.position("Evict") < live.position("Signal"));
}

/// When the next task adds nothing that replay would group with the query's work, the cached run
/// is reused rather than rebuilt.
#[tokio::test]
async fn task_without_new_work_after_query_advanced_workflow_reuses_run() {
    let (live, _) = live_then_replay(1, NextWft::NothingNew).await;
    assert_eq!(live.count("Evict"), 0);
}

/// Two queries each advanced the workflow in their own activation, while replay runs that work in
/// a single activation, so the run must be rebuilt even though the next task adds nothing.
#[tokio::test]
async fn task_after_two_queries_advanced_workflow_commits_replayable_history() {
    let (live, _) = live_then_replay(2, NextWft::NothingNew).await;
    assert_eq!(live.count("Evict"), 1);
}
