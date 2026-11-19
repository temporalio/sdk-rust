//! The Stream calls of Workflow code, through a Worker on a mock server. The mock server writes the
//! History of each completion, so a replay Worker can replay it.

use super::stream_store::{MemoryStore, Op, Request};
use crate::{
    StreamRuntimes, StreamSettings, Worker, WorkerConfig, init_replay_worker,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MockPollCfg, MocksHolder, PollWFTRespExt, ResponseType, WorkerExt, build_mock_pollers,
        hist_to_poll_resp, mock_worker, test_worker_cfg,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use futures_util::{StreamExt, stream};
use parking_lot::Mutex;
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::protos::{
    constants::{LOCAL_ACTIVITY_MARKER_NAME, STREAM_MARKER_NAME},
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        external_data::{
            StreamAppendOffsets, StreamAppendResult, StreamCloseIntent, StreamCloseResult,
            StreamMarkerData, stream_append_result,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, StreamAppended, WorkflowActivation,
            resolve_stream_append, workflow_activation_job::Variant as Job,
        },
        workflow_commands::{
            AppendToStream, CloseStream, CompleteWorkflowExecution, QueryResult, QuerySuccess,
            ScheduleLocalActivity, StartTimer, UpdateResponse, query_result, update_response,
            workflow_command::Variant as Command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::Payload,
        enums::v1::{CommandType, WorkflowTaskFailedCause},
        history::v1::{History, MarkerRecordedEventAttributes},
        query::v1::WorkflowQuery,
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, PollWorkflowTaskQueueResponse,
            RespondWorkflowTaskCompletedResponse,
        },
    },
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_streams::{
    StreamError, StreamRuntime, StreamStore,
    proto::{
        AppendRecord, AppendRecordsRequest, CreateStreamRequest, Producer, StreamRef,
        WorkflowProducer, WorkflowTaskEpoch, producer::Kind as ProducerKind,
    },
    record::BatchLimits,
    workflow::marker_details,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

const STORE: &str = "mem";

fn stream(id: &str) -> StreamRef {
    StreamRef {
        id: id.to_string(),
        store: STORE.to_string(),
    }
}

/// A Store with the Streams `ids`.
async fn store(ids: &[&str]) -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::default());
    for id in ids {
        let create = CreateStreamRequest {
            stream_ref: Some(stream(id)),
            ..Default::default()
        };
        store.create_stream(create).await.unwrap();
    }
    store
}

fn runtimes(store: &Arc<MemoryStore>) -> StreamRuntimes {
    let runtime = StreamRuntime::new(store.clone(), BatchLimits::default());
    StreamRuntimes::new([(STORE.to_string(), Arc::new(runtime))])
}

/// A History up to its first Workflow Task, whose timeout is `task_timeout`.
fn history(task_timeout: Duration) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(task_timeout);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// The task that the mock server gives inline for forced completion `number`, if any.
type NextTask =
    Box<dyn FnMut(usize, &mut TestHistoryBuilder) -> Option<PollWorkflowTaskQueueResponse> + Send>;

struct StartOpts {
    /// The tasks of the History that the poller gives.
    responses: Vec<ResponseType>,
    store: Arc<MemoryStore>,
    cache: usize,
    /// The mock server answers a forced completion with the next task, inline.
    served: Option<NextTask>,
    /// Runs a replay Worker on the History.
    replay: bool,
    /// The completion that the server refuses for an event that the Worker didn't see.
    refuse: Option<usize>,
    /// The failed Workflow Tasks that the server gets.
    fails: usize,
    /// The pages of History that the client fetches, in order.
    pages: Vec<GetWorkflowExecutionHistoryResponse>,
    configure: fn(&mut WorkerConfig),
}

impl StartOpts {
    fn new(store: &Arc<MemoryStore>) -> Self {
        Self {
            responses: vec![ResponseType::AllHistory],
            store: store.clone(),
            cache: 1,
            served: Some(Box::new(|_, t| Some(next_task(t)))),
            replay: false,
            refuse: None,
            fails: 0,
            pages: vec![],
            configure: |_| {},
        }
    }
}

struct Run {
    worker: Worker,
    store: Arc<MemoryStore>,
    sent: Arc<Mutex<Vec<WorkflowTaskCompletion>>>,
    /// The cause and the message of each failed Workflow Task.
    failed: Arc<Mutex<Vec<(WorkflowTaskFailedCause, String)>>>,
    /// The History that the completions wrote.
    history: Arc<Mutex<TestHistoryBuilder>>,
}

fn start(t: TestHistoryBuilder, opts: StartOpts) -> Run {
    let StartOpts {
        responses,
        store,
        cache,
        mut served,
        replay,
        refuse,
        fails,
        pages,
        configure,
    } = opts;
    let sent = Arc::new(Mutex::new(vec![]));
    let failed = Arc::new(Mutex::new(vec![]));
    let history = Arc::new(Mutex::new(t.clone()));
    if replay {
        let mut config = test_worker_cfg().build().unwrap();
        config.stream_runtimes = runtimes(&store);
        let t = HistoryForReplay::new(t.get_full_history_info().unwrap(), "fake_wf_id");
        let input = ReplayWorkerInput::new(config, stream::once(async move { t }));
        let worker = init_replay_worker(input).unwrap();
        return Run {
            worker,
            store,
            sent,
            failed,
            history,
        };
    }
    let mut cfg = MockPollCfg::from_resps(t, responses);
    cfg.make_poll_stream_interminable = true;
    cfg.num_expected_completions = Some((0..).into());
    cfg.num_expected_fails = fails;
    for page in pages {
        (cfg.mock_client.expect_get_workflow_execution_history())
            .times(1)
            .returning(move |_, _, _| Ok(page.clone()));
    }
    let (kept, written) = (sent.clone(), history.clone());
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        let number = kept.lock().len();
        if refuse == Some(number) {
            return Err(tonic::Status::invalid_argument("UnhandledCommand"));
        }
        let mut history = written.lock();
        write(&mut history, completion);
        let forced = completion.force_create_new_workflow_task;
        let workflow_task = served
            .as_mut()
            .filter(|_| forced && completion.return_new_workflow_task)
            .and_then(|next_task| next_task(number, &mut history));
        Ok(RespondWorkflowTaskCompletedResponse {
            workflow_task,
            ..Default::default()
        })
    }));
    let kept = failed.clone();
    cfg.expect_fail_wft_matcher = Box::new(move |_, cause, failure| {
        let message = failure.as_ref().map(|f| f.message.clone());
        kept.lock().push((*cause, message.unwrap_or_default()));
        true
    });
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = cache;
        wc.stream_runtimes = runtimes(&store);
        configure(wc);
    });
    Run {
        worker: mock_worker(mock),
        store,
        sent,
        failed,
        history,
    }
}

/// The next Workflow Task of `t`, with its whole History.
fn next_task(t: &mut TestHistoryBuilder) -> PollWorkflowTaskQueueResponse {
    t.add_workflow_task_scheduled_and_started();
    let mut task = hist_to_poll_resp(t, "fake_wf_id", ResponseType::AllHistory).resp;
    task.attempt = 1;
    task
}

/// Completes the last task of `t` with the marker `data`.
fn complete_with_marker(t: &mut TestHistoryBuilder, data: &StreamMarkerData) {
    t.add_workflow_task_completed();
    t.add(MarkerRecordedEventAttributes {
        marker_name: STREAM_MARKER_NAME.to_string(),
        details: marker_details(data),
        workflow_task_completed_event_id: t.current_event_id(),
        ..Default::default()
    });
}

/// The marker of a task that asked for close 1 of the Stream `stream_id`.
fn close_intent(stream_id: &str) -> StreamMarkerData {
    StreamMarkerData {
        streams: vec![stream(stream_id)],
        close_intents: vec![StreamCloseIntent { seq: 1, stream: 0 }],
        ..Default::default()
    }
}

/// The events of a completion that the server took.
fn write(history: &mut TestHistoryBuilder, completion: &WorkflowTaskCompletion) {
    history.add_workflow_task_completed();
    let completed = history.current_event_id();
    for command in &completion.commands {
        match command.attributes.as_ref() {
            Some(Attributes::RecordMarkerCommandAttributes(marker)) => {
                history.add(MarkerRecordedEventAttributes {
                    marker_name: marker.marker_name.clone(),
                    details: marker.details.clone(),
                    workflow_task_completed_event_id: completed,
                    ..Default::default()
                });
            }
            Some(Attributes::StartTimerCommandAttributes(timer)) => {
                history.add_timer_started(timer.timer_id.clone());
            }
            Some(Attributes::CompleteWorkflowExecutionCommandAttributes(_)) => {
                history.add_workflow_execution_completed();
            }
            // No test replays an Update.
            Some(Attributes::ProtocolMessageCommandAttributes(_)) => {}
            other => panic!("The mock server can't write {other:?}"),
        }
    }
}

type Workflow = fn(&WorkflowActivation) -> Vec<Command>;

/// Completes each activation of `worker` with the commands of `workflow` until the run leaves the
/// cache, and gives the jobs of each activation.
async fn drive(worker: &Worker, workflow: Workflow) -> Vec<Vec<Job>> {
    let mut jobs = vec![];
    while let Ok(activation) = worker.poll_workflow_activation().await {
        let run_id = activation.run_id.clone();
        if activation.is_only_eviction() {
            let _ = worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id))
                .await;
            break;
        }
        jobs.push(
            activation
                .jobs
                .iter()
                .map(|job| job.variant.clone().unwrap())
                .collect(),
        );
        let completion = WorkflowActivationCompletion::from_cmds(run_id, workflow(&activation));
        worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
    }
    jobs
}

impl Run {
    async fn drive(&self, workflow: Workflow) -> Vec<Vec<Job>> {
        drive(&self.worker, workflow).await
    }

    /// Replays the History that the completions wrote on a replay Worker with the same Store, and
    /// gives the jobs of each activation.
    async fn replay(&self, workflow: Workflow) -> Vec<Vec<Job>> {
        let t = self.history.lock().clone();
        let mut opts = StartOpts::new(&self.store);
        opts.replay = true;
        let replay = start(t, opts);
        let jobs = replay.drive(workflow).await;
        replay.worker.drain_pollers_and_shutdown_ref().await;
        jobs
    }

    fn sent(&self) -> Vec<WorkflowTaskCompletion> {
        self.sent.lock().clone()
    }
}

fn append(seq: u32, id: &str) -> Command {
    AppendToStream {
        seq,
        stream: Some(stream(id)),
        bodies: vec![Payload {
            data: vec![seq as u8],
            ..Default::default()
        }],
        sequence: 0,
    }
    .into()
}

fn close(seq: u32, id: &str) -> Command {
    CloseStream {
        seq,
        stream: Some(stream(id)),
    }
    .into()
}

fn complete() -> Command {
    CompleteWorkflowExecution { result: None }.into()
}

/// The data of the `core_stream` marker of a completion.
fn marker(completion: &WorkflowTaskCompletion) -> Option<StreamMarkerData> {
    completion
        .commands
        .iter()
        .find_map(|command| match command.attributes.as_ref()? {
            Attributes::RecordMarkerCommandAttributes(marker)
                if marker.marker_name == STREAM_MARKER_NAME =>
            {
                let payload = &marker.details["data"].payloads[0];
                Some(StreamMarkerData::decode(payload.data.as_slice()).unwrap())
            }
            _ => None,
        })
}

fn command_types(completion: &WorkflowTaskCompletion) -> Vec<CommandType> {
    (completion.commands.iter())
        .map(|command| command.command_type())
        .collect()
}

fn producer(run_id: &str, epoch: WorkflowTaskEpoch) -> Producer {
    Producer {
        id: format!("wf:{run_id}"),
        kind: Some(ProducerKind::Workflow(WorkflowProducer {
            run_id: run_id.to_string(),
            epoch: Some(epoch),
        })),
    }
}

#[tokio::test]
async fn an_append_writes_as_its_workflow_task_and_replays_from_its_marker() {
    let store = store(&["s"]).await;
    let t = history(Duration::from_secs(10));
    let run_id = t.get_orig_run_id().to_string();
    let started_time = t.get_full_history_info().unwrap().events()[2].event_time;
    let run = start(t, StartOpts::new(&store));
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        _ => vec![complete()],
    };
    let live = run.drive(workflow).await;

    let records = store.records("s");
    let epoch = WorkflowTaskEpoch {
        started_event_id: 3,
        attempt: 1,
        started_time,
    };
    assert_eq!(records[0].producer, Some(producer(&run_id, epoch)));
    let offset = records[0].offset.clone();
    let appended = ResolveStreamAppend {
        seq: 1,
        status: Some(resolve_stream_append::Status::Appended(StreamAppended {
            first_offset: offset.clone(),
            last_offset: offset.clone(),
        })),
    };
    assert_eq!(live[1], [Job::ResolveStreamAppend(appended)]);
    let entry = StreamAppendResult {
        seq: 1,
        stream: 0,
        activation: 1,
        result: Some(stream_append_result::Result::Appended(
            StreamAppendOffsets {
                first_offset: offset.clone(),
                last_offset: offset,
            },
        )),
    };
    let expected = StreamMarkerData {
        streams: vec![stream("s")],
        appends: vec![entry],
        ..Default::default()
    };
    assert_eq!(marker(&run.sent()[0]), Some(expected));
    run.worker.drain_pollers_and_shutdown_ref().await;
    let writes = store.requests().len();
    assert_eq!(run.replay(workflow).await, live);
    assert_eq!(store.requests().len(), writes);
}

#[rstest::rstest]
#[case::the_server_takes_it(None)]
#[case::a_failed_task(Some(1))]
#[case::a_refused_completion(Some(2))]
#[tokio::test]
async fn a_close_takes_effect_only_after_the_server_took_its_task(#[case] failure: Option<u8>) {
    let store = store(&["c"]).await;
    let mut opts = StartOpts::new(&store);
    let watched = store.clone();
    opts.served = Some(Box::new(move |_, t| {
        assert_eq!(
            watched.requests(),
            [],
            "The close ran before the server took its task"
        );
        Some(next_task(t))
    }));
    opts.fails = usize::from(failure == Some(1));
    opts.refuse = (failure == Some(2)).then_some(1);
    let run = start(history(Duration::from_secs(10)), opts);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    // An append over its limit gives its failure in a second activation of the task.
    let commands = vec![close(1, "c"), append(1, &"s".repeat(4097))];
    let completion = WorkflowActivationCompletion::from_cmds(first.run_id.clone(), commands);
    run.worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
    let refused = run.worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        refused.jobs[0].variant,
        Some(Job::ResolveStreamAppend(_))
    ));
    let completion = match failure {
        Some(1) => WorkflowActivationCompletion::fail(refused.run_id, "boom".into(), None),
        _ => WorkflowActivationCompletion::empty(refused.run_id),
    };
    run.worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
    let rest = run.drive(|_| vec![complete()]).await;
    if failure.is_none() {
        let closed = ResolveStreamClose {
            seq: 1,
            failed: None,
        };
        assert_eq!(rest, [[Job::ResolveStreamClose(closed)]]);
        let intent = StreamCloseIntent { seq: 1, stream: 0 };
        assert_eq!(marker(&run.sent()[0]).unwrap().close_intents, [intent]);
    } else {
        assert_eq!(rest, Vec::<Vec<Job>>::new());
        assert_eq!(store.requests(), []);
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(store.is_closed("c"), failure.is_none());
}

/// What comes in the task in which the close of the hold runs.
#[derive(Clone, Copy, Debug)]
enum InTheHold {
    Nothing,
    Signal,
    Cancel,
    Timer,
    Update,
    Query,
    LocalActivity,
    RefusedOnce,
}

#[rstest::rstest]
#[case(InTheHold::Nothing)]
#[case(InTheHold::Signal)]
#[case(InTheHold::Cancel)]
#[case(InTheHold::Timer)]
#[case(InTheHold::Update)]
#[case(InTheHold::Query)]
#[case(InTheHold::LocalActivity)]
#[case(InTheHold::RefusedOnce)]
#[tokio::test]
async fn the_last_command_waits_for_a_close_without_a_result(#[case] in_the_hold: InTheHold) {
    let store = store(&["s", "c"]).await;
    let mut opts = StartOpts::new(&store);
    opts.served = Some(Box::new(move |_, t| {
        match in_the_hold {
            InTheHold::Signal => t.add_we_signaled("hi", vec![]),
            InTheHold::Cancel => t.add_cancel_requested(),
            InTheHold::Timer => t.add_timer_fired(5, "1".to_string()),
            InTheHold::Nothing | InTheHold::Update | InTheHold::Query => {}
            InTheHold::LocalActivity | InTheHold::RefusedOnce => {}
        }
        let mut task = next_task(t);
        if let InTheHold::Update = in_the_hold {
            task.add_update_request("u", t.current_event_id() - 1);
        }
        if let InTheHold::Query = in_the_hold {
            let query = WorkflowQuery {
                query_type: "q".to_string(),
                ..Default::default()
            };
            task.queries.insert("q1".to_string(), query);
        }
        Some(task)
    }));
    opts.refuse = matches!(in_the_hold, InTheHold::RefusedOnce).then_some(2);
    let run = start(history(Duration::from_secs(10)), opts);
    let workflow: Workflow = match in_the_hold {
        // The Local Activity runs when the last command comes.
        InTheHold::LocalActivity => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![timer(), local_activity(), append(1, "s")],
            Some(Job::ResolveStreamAppend(_)) => vec![close(1, "c"), complete()],
            other => panic!("Workflow code got {other:?} after its last command"),
        },
        _ => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![timer(), close(1, "c"), complete()],
            Some(Job::QueryWorkflow(query)) => {
                let answer = QueryResult {
                    query_id: query.query_id.clone(),
                    variant: Some(query_result::Variant::Succeeded(QuerySuccess::default())),
                };
                vec![answer.into()]
            }
            other => panic!("Workflow code got {other:?} after its last command"),
        },
    };
    let mut ended = vec![
        CommandType::RecordMarker,
        CommandType::CompleteWorkflowExecution,
    ];
    let live = if let InTheHold::LocalActivity = in_the_hold {
        // The result of the Local Activity comes in task 2, before the result of the close.
        store.hold(Op::Close);
        let local_activity = async {
            let start = run.worker.poll_activity_task().await.unwrap();
            while !(store.requests().iter()).any(|request| matches!(request, Request::Close(_))) {
                tokio::task::yield_now().await;
            }
            let completion = ActivityTaskCompletion {
                task_token: start.task_token,
                result: Some(ActivityExecutionResult::ok(Payload::default())),
            };
            run.worker.complete_activity_task(completion).await.unwrap();
            store.release();
        };
        ended.insert(0, CommandType::RecordMarker);
        tokio::join!(run.drive(workflow), local_activity).0
    } else {
        run.drive(workflow).await
    };
    let sent = run.sent();
    assert_eq!(
        command_types(&sent[0]),
        [CommandType::StartTimer, CommandType::RecordMarker]
    );
    assert!(sent[0].force_create_new_workflow_task);
    let last = sent.last().unwrap();
    if let InTheHold::RefusedOnce = in_the_hold {
        // A Signal came while task 2 ran. The run that replaces the evicted one replays the hold,
        // and sends the close again.
        run.worker.drain_pollers_and_shutdown_ref().await;
        let mut t = run.history.lock().clone();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::UnhandledCommand,
            Default::default(),
        );
        t.add_we_signaled("again", vec![]);
        t.add_workflow_task_scheduled_and_started();
        let retry = start(t, StartOpts::new(&store));
        assert_eq!(retry.drive(workflow).await, live);
        assert_eq!(command_types(retry.sent().last().unwrap()), ended);
        let closes = (store.requests().into_iter()).filter(|r| matches!(r, Request::Close(_)));
        assert_eq!(closes.count(), 2);
        retry.worker.drain_pollers_and_shutdown_ref().await;
        assert_eq!(retry.replay(workflow).await, live);
        return;
    }
    assert_eq!(command_types(last), ended);
    if let InTheHold::LocalActivity = in_the_hold {
        let first = &last.commands[0].attributes;
        let local = matches!(first, Some(Attributes::RecordMarkerCommandAttributes(marker))
            if marker.marker_name == LOCAL_ACTIVITY_MARKER_NAME);
        assert!(local, "{first:?}");
    }
    assert!(last.messages.is_empty(), "{:?}", last.messages);
    let result = StreamCloseResult {
        seq: 1,
        activation: 0,
        failure: None,
    };
    assert_eq!(marker(last).unwrap().closes, [result]);
    let (before_the_command, after_the_command) = match in_the_hold {
        InTheHold::LocalActivity => (2, 0),
        InTheHold::Query => (1, 1),
        _ => (1, 0),
    };
    assert_eq!(
        live.len(),
        before_the_command + after_the_command,
        "{live:?}"
    );
    assert!(store.is_closed("c"));
    run.worker.drain_pollers_and_shutdown_ref().await;
    let replayed = run.replay(workflow).await;
    assert_eq!(replayed, live[..before_the_command]);
}

fn fast_tries(wc: &mut WorkerConfig) {
    wc.stream_runtimes.settings = StreamSettings {
        retry_first_wait: Duration::from_millis(10),
        retry_max_wait: Duration::from_millis(20),
        try_deadline: Duration::from_millis(50),
    };
}

#[derive(Clone, Copy, Debug)]
enum Outage {
    AppendFails,
    AppendGetsNoAnswer,
    CloseFails,
    AppendFailsWithNoLocalActivityManager,
}

#[rstest::rstest]
#[case(Outage::AppendFails)]
#[case(Outage::AppendGetsNoAnswer)]
#[case(Outage::CloseFails)]
#[case(Outage::AppendFailsWithNoLocalActivityManager)]
#[tokio::test]
async fn a_call_in_an_outage_fails_its_task_at_the_heartbeat(#[case] outage: Outage) {
    let store = store(&["s"]).await;
    let down = StreamError::storage_unavailable("The Store is down");
    match outage {
        Outage::AppendGetsNoAnswer => store.hold(Op::Append),
        Outage::CloseFails => store.fail(Op::Close, down, usize::MAX),
        _ => store.fail(Op::Append, down, usize::MAX),
    }
    let mut opts = StartOpts::new(&store);
    opts.fails = 1;
    opts.configure = match outage {
        Outage::AppendFailsWithNoLocalActivityManager => |wc| {
            fast_tries(wc);
            wc.task_types = WorkerTaskTypes::workflow_only();
        },
        _ => fast_tries,
    };
    let run = start(history(Duration::from_millis(200)), opts);
    let workflow: Workflow = match outage {
        Outage::CloseFails => |_| vec![close(1, "s")],
        _ => |_| vec![append(1, "s")],
    };
    assert_eq!(run.drive(workflow).await.len(), 1);
    let (call, error) = match outage {
        Outage::CloseFails => ("close", "The Store is down"),
        Outage::AppendGetsNoAnswer => ("append", "The Store gave no answer in 50ms"),
        _ => ("append", "The Store is down"),
    };
    let message = format!(
        "StreamStoreUnavailable: {call} of Stream \"s\" on Store \"{STORE}\": \
         stream_storage_unavailable: {error}"
    );
    let failed = run.failed.lock().clone();
    assert_eq!(
        failed,
        [(
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
            message
        )]
    );
    // The task that the outage failed sent no completion, so it forced no task.
    let closes = usize::from(matches!(outage, Outage::CloseFails));
    assert_eq!(run.sent().len(), closes);
    assert!(store.requests().len() > 1, "{:?}", store.requests());
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[rstest::rstest]
#[case::at_the_heartbeat(false)]
#[case::at_a_query(true)]
#[tokio::test]
async fn a_call_in_flight_goes_on_in_a_forced_task(#[case] query: bool) {
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let task_timeout = Duration::from_millis(if query { 10_000 } else { 200 });
    let t = history(task_timeout);
    let mut opts = StartOpts::new(&store);
    if query {
        let mut task = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::AllHistory).resp;
        let question = WorkflowQuery {
            query_type: "q".to_string(),
            ..Default::default()
        };
        task.queries.insert("q1".to_string(), question);
        opts.responses = vec![ResponseType::Raw(task)];
    }
    let held = store.clone();
    // The append answers once the server took the task, so its result needs the next one.
    opts.served = Some(Box::new(move |_, t| {
        held.release();
        Some(next_task(t))
    }));
    let run = start(t, opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        Some(Job::QueryWorkflow(query)) => vec![
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(query_result::Variant::Succeeded(QuerySuccess::default())),
            }
            .into(),
        ],
        _ => vec![complete()],
    };
    let jobs = run.drive(workflow).await;
    assert!(matches!(
        jobs.last().unwrap()[..],
        [Job::ResolveStreamAppend(_)]
    ));
    let sent = run.sent();
    assert!(sent[0].force_create_new_workflow_task && sent[0].commands.is_empty());
    assert_eq!(sent[0].query_responses.len(), usize::from(query));
    assert_eq!(marker(&sent[1]).unwrap().appends.len(), 1);
    assert_eq!(store.requests().len(), 1);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[rstest::rstest]
#[case::with_a_marker_to_send(true)]
#[case::with_nothing_else(false)]
#[tokio::test]
async fn an_eviction_completes_a_task_that_a_call_holds(#[case] with_a_marker: bool) {
    let store = store(&["s"]).await;
    let run = start(history(Duration::from_secs(10)), StartOpts::new(&store));
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    if !with_a_marker {
        store.hold(Op::Append);
    }
    let completion = WorkflowActivationCompletion::from_cmd(run_id.clone(), append(1, "s"));
    run.worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
    if with_a_marker {
        let result = run.worker.poll_workflow_activation().await.unwrap();
        assert!(matches!(
            result.jobs[0].variant,
            Some(Job::ResolveStreamAppend(_))
        ));
        store.hold(Op::Append);
        let completion = WorkflowActivationCompletion::from_cmd(run_id.clone(), append(2, "s"));
        run.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
    }
    run.worker.request_workflow_eviction(&run_id);
    assert_eq!(run.drive(|_| vec![]).await.len(), 0);
    let sent = run.sent();
    let last = sent.last().unwrap();
    assert!(last.force_create_new_workflow_task);
    assert!(!last.return_new_workflow_task && last.sticky_attributes.is_none());
    let recorded = marker(last).map(|marker| marker.appends.len());
    assert_eq!(recorded, with_a_marker.then_some(1));
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[tokio::test]
async fn a_replay_worker_makes_no_live_call() {
    let store = store(&["s", "c"]).await;
    let mut t = history(Duration::from_secs(10));
    complete_with_marker(&mut t, &close_intent("c"));
    t.add_workflow_task_scheduled_and_started();
    let mut opts = StartOpts::new(&store);
    opts.replay = true;
    let run = start(t, opts);
    let jobs = run.drive(|_| vec![append(1, "s"), close(1, "c")]).await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(store.requests(), []);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[rstest::rstest]
#[case::results(false)]
#[case::close_results_in_the_hold(true)]
#[tokio::test]
async fn a_task_at_a_bound_completes_with_a_new_one_forced(#[case] in_the_hold: bool) {
    let ids: Vec<String> = (1..=70).map(|seq| format!("s{seq}")).collect();
    let store = store(&ids.iter().map(String::as_str).collect::<Vec<_>>()).await;
    let long = StreamError::invalid("x".repeat(4000));
    store.fail(if in_the_hold { Op::Close } else { Op::Append }, long, 70);
    let run = start(history(Duration::from_secs(10)), StartOpts::new(&store));
    let workflow: Workflow = if in_the_hold {
        |_| {
            (1..=70)
                .map(|seq| close(seq, &format!("s{seq}")))
                .chain([complete()])
                .collect()
        }
    } else {
        |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => (1..=70)
                .map(|seq| append(seq, &format!("s{seq}")))
                .collect(),
            _ if activation.jobs.iter().any(
                |job| matches!(&job.variant, Some(Job::ResolveStreamAppend(r)) if r.seq == 70),
            ) =>
            {
                vec![complete()]
            }
            _ => vec![],
        }
    };
    let live = run.drive(workflow).await;
    let sent = run.sent();
    let (last, forced) = sent.split_last().unwrap();
    assert!(forced.iter().all(|c| c.force_create_new_workflow_task) && !forced.is_empty());
    assert!(!last.force_create_new_workflow_task);
    let markers: Vec<StreamMarkerData> = sent.iter().filter_map(marker).collect();
    assert!(markers.iter().all(|m| m.encoded_len() <= 256 * 1024));
    let results = (markers.iter())
        .map(|m| m.appends.len() + m.closes.len())
        .sum::<usize>();
    assert_eq!(results, 70);
    let mut got: Vec<u32> = (live.iter().flatten())
        .filter_map(|job| match job {
            Job::ResolveStreamAppend(append) => Some(append.seq),
            _ => None,
        })
        .collect();
    got.sort_unstable();
    let expected = if in_the_hold {
        vec![]
    } else {
        (1..=70).collect()
    };
    assert_eq!(got, expected);
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(run.replay(workflow).await, live);
}

/// What came of task 2, which wrote append 1 and gave no result to History.
#[derive(Clone, Copy, Debug)]
enum EarlierAttempt {
    FailedTask,
    LaterRetryOfAFailedTask,
    TaskTimeout,
    ForcedTaskOfAnotherWorker,
    Eviction,
    UpdateAccepted,
    UpdateRejected,
}

/// True if `activation` has the result of append `seq`.
fn resolves_append(activation: &WorkflowActivation, seq: u32) -> bool {
    (activation.jobs.iter()).any(
        |job| matches!(&job.variant, Some(Job::ResolveStreamAppend(append)) if append.seq == seq),
    )
}

/// The id, the started event id and the attempt of a Workflow's Producer.
fn writer(producer: Option<Producer>) -> (String, i64, i32) {
    let producer = producer.unwrap();
    let Some(ProducerKind::Workflow(WorkflowProducer {
        epoch: Some(epoch), ..
    })) = &producer.kind
    else {
        panic!("{producer:?}");
    };
    (producer.id.clone(), epoch.started_event_id, epoch.attempt)
}

/// The writer of each Record of the Stream `id`.
fn writers(store: &MemoryStore, id: &str) -> Vec<(String, i64, i32)> {
    (store.records(id).into_iter())
        .map(|record| writer(record.producer))
        .collect()
}

#[rstest::rstest]
#[case(EarlierAttempt::FailedTask)]
#[case(EarlierAttempt::LaterRetryOfAFailedTask)]
#[case(EarlierAttempt::TaskTimeout)]
#[case(EarlierAttempt::ForcedTaskOfAnotherWorker)]
#[case(EarlierAttempt::Eviction)]
#[case(EarlierAttempt::UpdateAccepted)]
#[case(EarlierAttempt::UpdateRejected)]
#[tokio::test]
async fn a_run_on_a_new_attempt_or_worker_writes_above_each_earlier_epoch(
    #[case] earlier: EarlierAttempt,
) {
    let store = store(&["s", "c"]).await;
    // Task 1 recorded the intent of close 1.
    let mut t = history(Duration::from_secs(10));
    complete_with_marker(&mut t, &close_intent("c"));
    let second = next_task(&mut t);
    let run_id = t.get_orig_run_id().to_string();
    match earlier {
        EarlierAttempt::TaskTimeout => t.add_workflow_task_timed_out(),
        EarlierAttempt::ForcedTaskOfAnotherWorker | EarlierAttempt::Eviction => {
            t.add_workflow_task_completed();
        }
        _ => {
            let cause = WorkflowTaskFailedCause::Unspecified;
            t.add_workflow_task_failed_with_failure(cause, Default::default());
        }
    }
    let mut task = next_task(&mut t);
    if let EarlierAttempt::UpdateAccepted | EarlierAttempt::UpdateRejected = earlier {
        task.add_update_request("u", 9);
    }
    let runs_the_earlier_attempt = matches!(
        earlier,
        EarlierAttempt::LaterRetryOfAFailedTask | EarlierAttempt::Eviction
    );
    if !runs_the_earlier_attempt {
        let epoch = WorkflowTaskEpoch {
            started_event_id: 7,
            attempt: 1,
            started_time: None,
        };
        let write = AppendRecordsRequest {
            r#ref: Some(stream("s")),
            producer: Some(producer(&run_id, epoch)),
            sequence: 1,
            records: vec![AppendRecord::default()],
            ..Default::default()
        };
        store.append_records(write).await.unwrap();
    }
    let mut opts = StartOpts::new(&store);
    // The mock poller numbers the attempts of a task from 1.
    opts.responses = match earlier {
        EarlierAttempt::LaterRetryOfAFailedTask => {
            vec![ResponseType::Raw(task.clone()), ResponseType::Raw(task)]
        }
        EarlierAttempt::Eviction => vec![ResponseType::Raw(second), ResponseType::Raw(task)],
        _ => vec![ResponseType::Raw(task)],
    };
    opts.fails = usize::from(matches!(earlier, EarlierAttempt::LaterRetryOfAFailedTask));
    let run = start(t, opts);
    let workflow: Workflow = match earlier {
        EarlierAttempt::UpdateAccepted => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![append(1, "s"), close(1, "c")],
            Some(Job::DoUpdate(_)) => {
                let accepted = update_response::Response::Accepted(());
                vec![update_response(activation, accepted), append(2, "s")]
            }
            _ if resolves_append(activation, 2) => vec![complete()],
            _ => vec![],
        },
        _ => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![append(1, "s"), close(1, "c")],
            Some(Job::DoUpdate(_)) => {
                let rejected = update_response::Response::Rejected(Default::default());
                vec![update_response(activation, rejected)]
            }
            _ if resolves_append(activation, 1) => vec![complete()],
            _ => vec![],
        },
    };
    if runs_the_earlier_attempt {
        // The close of the earlier attempt never reaches the Store.
        store.hold(Op::Close);
        if let EarlierAttempt::Eviction = earlier {
            store.lose_answers(Op::Append, usize::MAX);
        }
        let init = run.worker.poll_workflow_activation().await.unwrap();
        let commands = workflow(&init);
        let completion = WorkflowActivationCompletion::from_cmds(init.run_id.clone(), commands);
        run.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
        if let EarlierAttempt::Eviction = earlier {
            while store.records("s").is_empty() {
                tokio::task::yield_now().await;
            }
            run.worker.request_workflow_eviction(&init.run_id);
        } else {
            let appended = run.worker.poll_workflow_activation().await.unwrap();
            assert!(resolves_append(&appended, 1), "{appended:?}");
            let failed = WorkflowActivationCompletion::fail(appended.run_id, "boom".into(), None);
            run.worker
                .complete_workflow_activation(failed)
                .await
                .unwrap();
        }
        assert_eq!(run.drive(workflow).await.len(), 0);
        store.lose_answers(Op::Append, 0);
        store.release();
    }
    let jobs = run.drive(workflow).await;
    let appended = jobs.iter().flatten().any(|job| {
        matches!(
            job,
            Job::ResolveStreamAppend(ResolveStreamAppend {
                seq: 1,
                status: Some(resolve_stream_append::Status::Appended(_)),
            })
        )
    });
    assert!(appended, "{jobs:?}");
    let id = producer(&run_id, Default::default()).id;
    let (earlier_task, go_live) = match earlier {
        EarlierAttempt::LaterRetryOfAFailedTask => ((10, 1), (10, 2)),
        EarlierAttempt::UpdateRejected => ((7, 1), (13, 1)),
        _ => ((7, 1), (10, 1)),
    };
    let mut expected = vec![
        (id.clone(), earlier_task.0, earlier_task.1),
        (id.clone(), go_live.0, go_live.1),
    ];
    if let EarlierAttempt::UpdateAccepted = earlier {
        expected.push(expected[1].clone());
    }
    assert_eq!(writers(&store, "s"), expected);
    assert!(store.is_closed("c"));
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// When the task of another Worker comes to a cached run.
#[derive(Clone, Copy, Debug)]
enum OtherWorkersTask {
    /// Once task 1 completed with an append in flight.
    WithNoActivationOut,
    /// While the first activation is out. Its completion appends.
    WithAnActivationOut,
}

#[rstest::rstest]
#[case(OtherWorkersTask::WithNoActivationOut)]
#[case(OtherWorkersTask::WithAnActivationOut)]
#[tokio::test]
async fn a_cached_run_that_meets_another_workers_task_replays_from_history(
    #[case] other_workers_task: OtherWorkersTask,
) {
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let t = history(Duration::from_secs(10));
    let mut first = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::AllHistory).resp;
    first.attempt = 1;
    // Another Worker took task 2, and its marker has the result of append 1.
    let mut other = t.clone();
    match other_workers_task {
        OtherWorkersTask::WithNoActivationOut => other.add_workflow_task_completed(),
        OtherWorkersTask::WithAnActivationOut => other.add_workflow_task_timed_out(),
    }
    other.add_workflow_task_scheduled_and_started();
    let offsets = StreamAppendOffsets {
        first_offset: vec![7],
        last_offset: vec![7],
    };
    let result = StreamAppendResult {
        seq: 1,
        stream: 0,
        activation: 0,
        result: Some(stream_append_result::Result::Appended(offsets)),
    };
    let marker = StreamMarkerData {
        streams: vec![stream("s")],
        appends: vec![result],
        ..Default::default()
    };
    complete_with_marker(&mut other, &marker);
    other.add_we_signaled("done", vec![]);
    let third = next_task(&mut other);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        Some(Job::QueryWorkflow(query)) => vec![
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(query_result::Variant::Succeeded(QuerySuccess::default())),
            }
            .into(),
        ],
        Some(Job::SignalWorkflow(_)) => vec![complete()],
        _ => vec![],
    };
    let run = match other_workers_task {
        OtherWorkersTask::WithNoActivationOut => {
            // The answer of a query completes task 1 while the append runs.
            let question = WorkflowQuery {
                query_type: "q".to_string(),
                ..Default::default()
            };
            first.queries.insert("q1".to_string(), question);
            let mut opts = StartOpts::new(&store);
            opts.responses = vec![ResponseType::Raw(first), ResponseType::Raw(third)];
            opts.served = None;
            let run = start(t, opts);
            assert_eq!(run.drive(workflow).await.len(), 2);
            run
        }
        OtherWorkersTask::WithAnActivationOut => {
            let (tasks, input) = tokio::sync::mpsc::unbounded_channel();
            let (processed, all_processed) = tokio::sync::oneshot::channel();
            let input = UnboundedReceiverStream::new(input).chain(stream::once(async move {
                let _ = processed.send(());
                std::future::pending().await
            }));
            let sent = Arc::new(Mutex::new(vec![]));
            let kept = sent.clone();
            let mut client = mock_worker_client();
            (client.expect_complete_workflow_task()).returning(move |completion, _| {
                kept.lock().push(completion);
                Ok(Default::default())
            });
            let mut mocks = MocksHolder::from_wft_stream(client, input);
            mocks.worker_cfg(|wc| {
                wc.max_cached_workflows = 1;
                wc.stream_runtimes = runtimes(&store);
                // With one extractor, Core polls past a task once it has processed the task.
                wc.fetching_concurrency = 1;
            });
            let worker = mock_worker(mocks);
            tasks.send(first).unwrap();
            let init = worker.poll_workflow_activation().await.unwrap();
            tasks.send(third).unwrap();
            drop(tasks);
            all_processed.await.unwrap();
            let commands = workflow(&init);
            let completion = WorkflowActivationCompletion::from_cmds(init.run_id.clone(), commands);
            worker
                .complete_workflow_activation(completion)
                .await
                .unwrap();
            assert_eq!(drive(&worker, workflow).await.len(), 0);
            // The append came after the run stopped its calls, so it holds no task.
            let stale = sent.lock()[0].clone();
            assert!(stale.commands.is_empty() && !stale.force_create_new_workflow_task);
            Run {
                worker,
                store: store.clone(),
                sent,
                failed: Default::default(),
                history: Default::default(),
            }
        }
    };
    let replayed = run.drive(workflow).await;
    let appended = ResolveStreamAppend {
        seq: 1,
        status: Some(resolve_stream_append::Status::Appended(StreamAppended {
            first_offset: vec![7],
            last_offset: vec![7],
        })),
    };
    assert_eq!(replayed[1], [Job::ResolveStreamAppend(appended)]);
    assert!(matches!(replayed[2][..], [Job::SignalWorkflow(_)]));
    // The append of the cached run was in flight, or never started.
    let in_flight = matches!(other_workers_task, OtherWorkersTask::WithNoActivationOut);
    assert_eq!(store.requests().len(), usize::from(in_flight));
    store.release();
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(store.records("s"), []);
}

#[derive(Clone, Copy, Debug)]
enum UpdateTask {
    OfACachedRun,
    OfANewRun,
}

fn local_activity() -> Command {
    let local_activity = ScheduleLocalActivity {
        seq: 1,
        activity_id: "1".to_string(),
        activity_type: "la".to_string(),
        start_to_close_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
        ..Default::default()
    };
    local_activity.into()
}

fn timer() -> Command {
    let timer = StartTimer {
        seq: 1,
        start_to_fire_timeout: Some(Duration::from_secs(1).try_into().unwrap()),
    };
    timer.into()
}

fn update_response(
    activation: &WorkflowActivation,
    response: update_response::Response,
) -> Command {
    let Some(Job::DoUpdate(update)) = &activation.jobs[0].variant else {
        panic!("{activation:?}");
    };
    UpdateResponse {
        protocol_instance_id: update.protocol_instance_id.clone(),
        response: Some(response),
    }
    .into()
}

#[rstest::rstest]
#[case(UpdateTask::OfACachedRun)]
#[case(UpdateTask::OfANewRun)]
#[tokio::test]
async fn a_call_of_an_update_handler_runs_after_the_update_is_accepted(#[case] case: UpdateTask) {
    let store = store(&["s"]).await;
    // Task 1 starts a timer, so that task 2 is a task of its own in a replay.
    let mut t = history(Duration::from_secs(10));
    t.add_workflow_task_completed();
    t.add_timer_started("1".to_string());
    let mut update = next_task(&mut t);
    update.add_update_request("u", 6);
    let run_id = t.get_orig_run_id().to_string();
    let mut opts = StartOpts::new(&store);
    opts.responses = match case {
        UpdateTask::OfACachedRun => vec![ResponseType::ToTaskNum(1), ResponseType::Raw(update)],
        UpdateTask::OfANewRun => vec![ResponseType::Raw(update)],
    };
    let run = start(t, opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer()],
        Some(Job::DoUpdate(_)) => {
            let accepted = update_response::Response::Accepted(());
            vec![update_response(activation, accepted), append(1, "s")]
        }
        _ => vec![complete()],
    };
    let jobs = run.drive(workflow).await;
    assert!(
        matches!(jobs.last().unwrap()[..], [Job::ResolveStreamAppend(_)]),
        "{jobs:?}"
    );
    let id = producer(&run_id, Default::default()).id;
    assert_eq!(writers(&store, "s"), [(id, 7, 1)]);
    // A completion at the heartbeat would force a new task.
    let sent = run.sent();
    assert_eq!(
        sent.len(),
        1 + usize::from(matches!(case, UpdateTask::OfACachedRun))
    );
    assert!(
        sent.iter()
            .all(|completion| !completion.force_create_new_workflow_task)
    );
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[tokio::test]
async fn a_cached_run_that_meets_another_workers_marker_on_a_later_page_replays_from_history() {
    let store = store(&["c"]).await;
    let mut t = history(Duration::from_secs(10));
    t.add_workflow_task_completed();
    let started = t.add_timer_started("1".to_string());
    t.add_timer_fired(started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    // Another Worker took task 2, and its marker has the intent of close 1.
    complete_with_marker(&mut t, &close_intent("c"));
    let run_id = t.get_orig_run_id().to_string();
    let mut third = next_task(&mut t);
    // The marker is on the last of three pages. The update of a task holds each event up to the
    // task, so the check sees it.
    let events = third.history.take().unwrap().events;
    let page = |events: &[_], token: Vec<u8>| GetWorkflowExecutionHistoryResponse {
        history: Some(History {
            events: events.to_vec(),
        }),
        next_page_token: token,
        ..Default::default()
    };
    third.history = Some(History {
        events: events[..4].to_vec(),
    });
    third.next_page_token = vec![1];
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::ToTaskNum(1), ResponseType::Raw(third)];
    opts.pages = vec![page(&events[4..9], vec![2]), page(&events[9..], vec![])];
    let run = start(t, opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer()],
        Some(Job::FireTimer(_)) => vec![close(1, "c")],
        _ => vec![complete()],
    };
    assert_eq!(run.drive(workflow).await.len(), 1);
    let replayed = run.drive(workflow).await;
    let closed = matches!(replayed.last().unwrap()[..], [Job::ResolveStreamClose(_)]);
    assert!(closed, "{replayed:?}");
    let closes: Vec<_> = (store.requests().into_iter())
        .filter_map(|request| match request {
            Request::Close(close) => Some(writer(close.producer)),
            Request::Append(_) => None,
        })
        .collect();
    let id = producer(&run_id, Default::default()).id;
    assert_eq!(closes, [(id, 12, 1)]);
    assert!(
        run.sent()
            .iter()
            .all(|sent| !sent.force_create_new_workflow_task)
    );
    run.worker.drain_pollers_and_shutdown_ref().await;
}
