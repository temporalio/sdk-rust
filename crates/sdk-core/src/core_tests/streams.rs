//! The Stream calls of Workflow code, through a Worker on a mock server. The mock server writes the
//! History of each completion, so a replay Worker can replay it.

mod reads;

use super::stream_store::{MemoryStore, Op, Request};
use crate::{
    StreamRuntimes, StreamSettings, Worker, WorkerConfig, init_replay_worker,
    internal_flags::CoreInternalFlags,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MockPollCfg, MocksHolder, PollWFTRespExt, ResponseType, WorkerExt, build_mock_pollers,
        hist_to_poll_resp, mock_worker, test_worker_cfg,
    },
    worker::client::{MockWorkerClient, WorkflowTaskCompletion, mocks::mock_worker_client},
};
use futures_util::{StreamExt, stream};
use parking_lot::Mutex;
use prost::Message;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::{
    collections::BTreeMap,
    iter,
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    constants::{LOCAL_ACTIVITY_MARKER_NAME, STREAM_MARKER_NAME},
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        external_data::{
            StreamAppendOffsets, StreamAppendResult, StreamCloseIntent, StreamCloseResult,
            StreamMarkerData, StreamReadEntry, stream_append_result,
        },
        workflow_activation::{
            FireTimer, ResolveStreamAppend, ResolveStreamClose, StreamAppended, WorkflowActivation,
            resolve_stream_append, workflow_activation_job::Variant as Job,
        },
        workflow_commands::{
            AppendToStream, CloseStream, CompleteWorkflowExecution, QueryResult, QuerySuccess,
            ScheduleLocalActivity, StartStreamRead, StartTimer, UpdateResponse, query_result,
            update_response, workflow_command::Variant as Command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::Payload,
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::{History, HistoryEvent, MarkerRecordedEventAttributes},
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
        AppendRecord, AppendRecordsRequest, ClientProducer, CreateStreamRequest, Producer,
        StreamRecord, StreamRef, WorkflowProducer, WorkflowTaskEpoch,
        producer::Kind as ProducerKind,
    },
    record::BatchLimits,
    workflow::marker_details,
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot::{self, error::TryRecvError},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

const STORE: &str = "mem";

fn stream(id: &str) -> StreamRef {
    StreamRef {
        id: id.to_string(),
        store: STORE.to_string(),
    }
}

/// Creates a Store with the Streams `ids`.
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

/// Returns a History up to its first Workflow Task, with a task timeout of `task_timeout`.
fn history(task_timeout: Duration) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(task_timeout);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// The task that the mock server gives after completion `number`, if any.
type NextTask =
    Box<dyn FnMut(usize, &mut TestHistoryBuilder) -> Option<PollWorkflowTaskQueueResponse> + Send>;

struct StartOpts {
    /// The tasks of the History that the poller gives.
    responses: Vec<ResponseType>,
    /// When set, the poller gives the tasks that the test sends here instead of `responses`, as
    /// soon as they come. Core can then get a task while an activation is out.
    tasks: Option<UnboundedReceiver<PollWorkflowTaskQueueResponse>>,
    /// While the mock server answers completion `number`, it sends the task that this returns to
    /// `tasks` and closes it. It answers once Core took the task.
    during_report: Option<(UnboundedSender<PollWorkflowTaskQueueResponse>, NextTask)>,
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
            tasks: None,
            during_report: None,
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
    /// Resolves once Core has taken each task of `StartOpts::tasks`, after the test dropped its
    /// sender.
    all_taken: Option<oneshot::Receiver<()>>,
}

fn start(t: TestHistoryBuilder, opts: StartOpts) -> Run {
    let StartOpts {
        responses,
        tasks,
        during_report,
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
            all_taken: None,
        };
    }
    // Resolves once Core polled past the last task of `tasks`.
    let (taken, all_taken) = oneshot::channel();
    let (mut during_report, all_taken) = match during_report {
        Some((sender, next_task)) => (Some((Some(sender), next_task, all_taken)), None),
        None => (None, Some(all_taken)),
    };
    let (kept, written) = (sent.clone(), history.clone());
    let complete = move |completion: &WorkflowTaskCompletion| {
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
        if let Some((sender, next_task, taken)) = during_report.as_mut()
            && let Some(task) = next_task(number, &mut history)
        {
            let sender = sender.take().expect("One task comes during a report");
            sender.send(task).unwrap();
            drop(sender);
            // Core takes the task on its own thread while this one waits.
            let deadline = Instant::now() + Duration::from_secs(10);
            while matches!(taken.try_recv(), Err(TryRecvError::Empty)) {
                assert!(Instant::now() < deadline, "Core never took the task");
                std::thread::yield_now();
            }
        }
        Ok(RespondWorkflowTaskCompletedResponse {
            workflow_task,
            ..Default::default()
        })
    };
    let kept = failed.clone();
    let starts: Vec<_> = (pages.iter())
        .map(|page| {
            page.history
                .as_ref()
                .and_then(|h| h.events.first())
                .map(|e| e.event_id)
        })
        .collect();
    let fail = move |cause: &WorkflowTaskFailedCause, failure: &Option<Failure>| {
        let message = failure.as_ref().map(|f| f.message.clone());
        let mut failed = kept.lock();
        failed.push((*cause, message.unwrap_or_default()));
        assert!(
            failed.len() <= fails,
            "A task failed with pages from events {starts:?}: {failed:?}"
        );
    };
    let from_channel = tasks.is_some();
    let mut mock = match tasks {
        None => {
            let mut cfg = MockPollCfg::from_resps(t, responses);
            cfg.make_poll_stream_interminable = true;
            cfg.num_expected_completions = Some((0..).into());
            cfg.num_expected_fails = fails;
            expect_pages(&mut cfg.mock_client, pages);
            cfg.completion_mock_fn = Some(Box::new(complete));
            cfg.expect_fail_wft_matcher = Box::new(move |_, cause, failure| {
                fail(cause, failure);
                true
            });
            build_mock_pollers(cfg)
        }
        Some(tasks) => {
            let input = UnboundedReceiverStream::new(tasks).chain(stream::once(async move {
                let _ = taken.send(());
                std::future::pending().await
            }));
            let mut client = mock_worker_client();
            let mut complete = complete;
            (client.expect_complete_workflow_task())
                .returning(move |completion, _| complete(&completion));
            (client.expect_fail_workflow_task()).times(fails).returning(
                move |_, cause, failure| {
                    fail(&cause, &failure);
                    Ok(Default::default())
                },
            );
            expect_pages(&mut client, pages);
            MocksHolder::from_wft_stream(client, input)
        }
    };
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = cache;
        wc.stream_runtimes = runtimes(&store);
        if from_channel {
            // With one extractor, Core polls past a task of the channel once it processed it.
            wc.fetching_concurrency = 1;
        }
        configure(wc);
    });
    Run {
        worker: mock_worker(mock),
        store,
        sent,
        failed,
        history,
        all_taken: all_taken.filter(|_| from_channel),
    }
}

fn page(events: &[HistoryEvent], next_page_token: Vec<u8>) -> GetWorkflowExecutionHistoryResponse {
    GetWorkflowExecutionHistoryResponse {
        history: Some(History {
            events: events.to_vec(),
        }),
        next_page_token,
        ..Default::default()
    }
}

fn expect_pages(client: &mut MockWorkerClient, pages: Vec<GetWorkflowExecutionHistoryResponse>) {
    for page in pages {
        (client.expect_get_workflow_execution_history())
            .times(1)
            .returning(move |_, _, _| Ok(page.clone()));
    }
}

/// Adds the next Workflow Task to `t`, and returns it with the whole History.
fn next_task(t: &mut TestHistoryBuilder) -> PollWorkflowTaskQueueResponse {
    t.add_workflow_task_scheduled_and_started();
    task_of(t)
}

/// Returns the Workflow Task at the end of `t`, with the whole History.
fn task_of(t: &TestHistoryBuilder) -> PollWorkflowTaskQueueResponse {
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

/// Returns the marker of a task that asked for close 1 of the Stream `stream_id`.
fn close_intent(stream_id: &str) -> StreamMarkerData {
    StreamMarkerData {
        streams: vec![stream(stream_id)],
        close_intents: vec![StreamCloseIntent { seq: 1, stream: 0 }],
        ..Default::default()
    }
}

/// Adds the events of a completion that the server took to `history`.
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
            Some(Attributes::FailWorkflowExecutionCommandAttributes(_)) => {
                history.add_workflow_execution_failed();
            }
            // No test replays an Update.
            Some(Attributes::ProtocolMessageCommandAttributes(_)) => {}
            other => panic!("The mock server can't write {other:?}"),
        }
    }
}

type Workflow = fn(&WorkflowActivation) -> Vec<Command>;

/// Completes each activation of `worker` with the commands of `workflow` until the run leaves the
/// cache, and returns the jobs of each activation.
async fn drive(
    worker: &Worker,
    mut workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
) -> Vec<Vec<Job>> {
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
    async fn drive(
        &self,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        drive(&self.worker, workflow).await
    }

    /// Replays the History that the completions wrote on a replay Worker with the same Store, and
    /// returns the jobs of each activation.
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

/// Returns the data of the `core_stream` marker of a completion, if it has one.
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

/// What comes in the task in which the close runs while Core holds the last command.
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
            Some(Job::InitializeWorkflow(_)) => vec![timer(1), local_activity(1), append(1, "s")],
            Some(Job::ResolveStreamAppend(_)) => vec![close(1, "c"), complete()],
            other => panic!("Workflow code got {other:?} after its last command"),
        },
        _ => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![timer(1), close(1, "c"), complete()],
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
            wait_until(|| {
                (store.requests().iter()).any(|request| matches!(request, Request::Close(_)))
            })
            .await;
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
        ..StreamSettings::default()
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

#[tokio::test]
async fn a_call_in_an_outage_goes_on_when_a_full_marker_completes_its_task() {
    let store = store(&["c", "f"]).await;
    // The close is in an outage once it runs, and each append fails with a large message.
    store.fail(
        Op::Close,
        StreamError::storage_unavailable("down"),
        usize::MAX,
    );
    store.fail(
        Op::Append,
        StreamError::invalid("x".repeat(4000)),
        usize::MAX,
    );
    store.hold(Op::Append);
    let mut opts = StartOpts::new(&store);
    opts.configure = fast_tries;
    // Task 2 has a Signal, so that Workflow code gets an activation in it.
    opts.served = Some(Box::new(|number, t| {
        if number == 1 {
            t.add_we_signaled("go", vec![]);
        }
        Some(next_task(t))
    }));
    let run = start(history(Duration::from_secs(10)), opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![close(1, "c")],
        Some(Job::SignalWorkflow(_)) => (1..=70).map(|seq| append(seq, "f")).collect(),
        Some(Job::ResolveStreamClose(_)) => vec![complete()],
        _ => vec![],
    };
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, workflow).await;
    let signal = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &signal, workflow).await;
    // The close is in its outage once the Store saw it a second time.
    let is_close = |request: &Request| matches!(request, Request::Close(_));
    wait_until(|| {
        store
            .requests()
            .iter()
            .filter(|request| is_close(request))
            .count()
            >= 2
    })
    .await;
    // The appends of one Stream run one at a time. Each result comes while no activation is out,
    // so the one that the marker can't take completes the task with a new one forced.
    let mut results = vec![];
    while results.len() < 70 {
        store.release();
        store.hold(Op::Append);
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        for job in &activation.jobs {
            let Some(Job::ResolveStreamAppend(result)) = &job.variant else {
                panic!("{activation:?}");
            };
            results.push(result.seq);
        }
        respond(&run.worker, &activation, workflow).await;
    }
    assert_eq!(results, (1..=70).collect::<Vec<_>>());
    assert_eq!(run.failed.lock().clone(), []);
    // The close goes on in the next task, and ends once the Store is back.
    store.fail(Op::Close, StreamError::storage_unavailable("down"), 0);
    let closed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        closed.jobs[0].variant,
        Some(Job::ResolveStreamClose(_))
    ));
    respond(&run.worker, &closed, workflow).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
    // Task 2 completed with a new task forced when its marker was full, and the results that
    // didn't fit went into the marker of task 3.
    let sent = run.sent();
    let appends = |sent: &WorkflowTaskCompletion| -> Vec<u32> {
        let marker = marker(sent).unwrap_or_default();
        marker.appends.iter().map(|append| append.seq).collect()
    };
    let (second, third) = (appends(&sent[1]), appends(&sent[2]));
    assert!(sent[1].force_create_new_workflow_task && !third.is_empty());
    assert_eq!([second, third].concat(), results);
    assert_eq!(
        command_types(sent.last().unwrap()).last(),
        Some(&CommandType::CompleteWorkflowExecution)
    );
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

/// Returns true if `activation` has the result of append `seq`.
fn resolves_append(activation: &WorkflowActivation, seq: u32) -> bool {
    (activation.jobs.iter()).any(
        |job| matches!(&job.variant, Some(Job::ResolveStreamAppend(append)) if append.seq == seq),
    )
}

/// Returns the id, the started event id and the attempt of a Workflow's Producer.
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

/// Returns the writer of each Record of the Stream `id`.
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
            wait_until(|| !store.records("s").is_empty()).await;
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

/// How a task comes to a cached run after another attempt or Worker took over its task.
#[derive(Clone, Copy, Debug, PartialEq)]
enum OtherTask {
    /// Task 1 completed with an append in flight, and another Worker completed task 2.
    WithNoActivationOut,
    /// Task 1 timed out while its first activation was out, and another Worker completed the
    /// retry.
    WithAnActivationOut,
    /// Another Worker completed a later attempt of task 1, which has the same started event id,
    /// while the first activation of attempt 2 was out.
    AtTheSameStartedEventId,
    /// Task 2 timed out while its activation was out. Its retry comes after task 1, whose started
    /// event id is lower than the one of task 2.
    AfterATimeout,
    /// The cached run has no Stream call, and the marker of the other Worker is on the last of
    /// three pages of History.
    WithTheMarkerOnALaterPage,
    /// The cached run has no Stream call, and the marker of a read of the other Worker is in the
    /// lookahead of the update.
    WithAReadMarkerInTheLookahead,
}

/// Waits until `done` returns true, for at most 10 seconds, so that a test fails rather than hangs.
async fn wait_until(mut done: impl FnMut() -> bool) {
    let wait = async {
        while !done() {
            tokio::task::yield_now().await;
        }
    };
    (tokio::time::timeout(Duration::from_secs(10), wait).await).expect("The wait timed out");
}

/// Completes `activation` with the commands of `workflow`.
async fn respond(worker: &Worker, activation: &WorkflowActivation, workflow: Workflow) {
    let commands = workflow(activation);
    let completion = WorkflowActivationCompletion::from_cmds(activation.run_id.clone(), commands);
    worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case(OtherTask::WithNoActivationOut)]
#[case(OtherTask::WithAnActivationOut)]
#[case(OtherTask::AtTheSameStartedEventId)]
#[case(OtherTask::AfterATimeout)]
#[case(OtherTask::WithTheMarkerOnALaterPage)]
#[case(OtherTask::WithAReadMarkerInTheLookahead)]
#[tokio::test]
async fn a_cached_run_that_meets_another_workers_task_replays_from_history(
    #[case] other_task: OtherTask,
) {
    match other_task {
        OtherTask::AfterATimeout => return after_a_timeout().await,
        OtherTask::WithTheMarkerOnALaterPage => return with_the_marker_on_a_later_page().await,
        OtherTask::WithAReadMarkerInTheLookahead => {
            return reads::with_a_read_marker_in_the_lookahead().await;
        }
        _ => {}
    }
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let t = history(Duration::from_secs(10));
    let mut first = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::AllHistory).resp;
    first.attempt = if other_task == OtherTask::AtTheSameStartedEventId {
        2
    } else {
        1
    };
    // The other attempt or Worker completed its task, with the result of append 1 in its marker.
    let mut other = t.clone();
    match other_task {
        OtherTask::WithNoActivationOut => other.add_workflow_task_completed(),
        OtherTask::WithAnActivationOut => other.add_workflow_task_timed_out(),
        _ => {}
    }
    if other_task != OtherTask::AtTheSameStartedEventId {
        other.add_workflow_task_scheduled_and_started();
    }
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
    let next = next_task(&mut other);
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
    let mut opts = StartOpts::new(&store);
    opts.served = None;
    let run = if other_task == OtherTask::WithNoActivationOut {
        // The answer of a query completes task 1 while the append runs.
        let question = WorkflowQuery {
            query_type: "q".to_string(),
            ..Default::default()
        };
        first.queries.insert("q1".to_string(), question);
        opts.responses = vec![ResponseType::Raw(first), ResponseType::Raw(next)];
        let run = start(t, opts);
        assert_eq!(run.drive(workflow).await.len(), 2);
        run
    } else {
        let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
        opts.tasks = Some(receiver);
        let mut run = start(t, opts);
        tasks.send(first).unwrap();
        let init = run.worker.poll_workflow_activation().await.unwrap();
        tasks.send(next).unwrap();
        drop(tasks);
        run.all_taken.take().unwrap().await.unwrap();
        respond(&run.worker, &init, workflow).await;
        assert_eq!(run.drive(workflow).await.len(), 0);
        // The append came after the run stopped its calls, so it holds no task.
        let stale = run.sent()[0].clone();
        assert!(stale.commands.is_empty() && !stale.force_create_new_workflow_task);
        run
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
    let in_flight = other_task == OtherTask::WithNoActivationOut;
    assert_eq!(store.requests().len(), usize::from(in_flight));
    store.release();
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(store.records("s"), []);
}

#[tokio::test]
async fn a_task_that_comes_while_core_reports_a_completion_keeps_the_run() {
    let store = store(&["s"]).await;
    let t = history(Duration::from_secs(10));
    let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
    tasks.send(task_of(&t)).unwrap();
    let mut opts = StartOpts::new(&store);
    opts.tasks = Some(receiver);
    opts.served = None;
    // Core takes task 2, which has a Signal, before the server answers the completion of task 1.
    opts.during_report = Some((
        tasks,
        Box::new(|number, t| {
            (number == 1).then(|| {
                t.add_we_signaled("go", vec![]);
                next_task(t)
            })
        }),
    ));
    let run = start(t, opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        Some(Job::SignalWorkflow(_)) => vec![complete()],
        _ => vec![],
    };
    let jobs = run.drive(workflow).await;
    // The cached run takes task 2 once the report is done, with no eviction and no replay.
    assert_eq!(jobs.len(), 3, "{jobs:?}");
    assert!(
        matches!(jobs[1][..], [Job::ResolveStreamAppend(_)]),
        "{jobs:?}"
    );
    assert!(matches!(jobs[2][..], [Job::SignalWorkflow(_)]), "{jobs:?}");
    assert_eq!(run.sent().len(), 2);
    assert_eq!(store.requests().len(), 1);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

async fn after_a_timeout() {
    let store = store(&["s"]).await;
    let mut t = history(Duration::from_secs(10));
    let first = task_of(&t);
    t.add_workflow_task_completed();
    let started = t.add_timer_started("1".to_string());
    t.add_timer_fired(started, "1".to_string());
    let second = next_task(&mut t);
    t.add_workflow_task_timed_out();
    let mut retry = next_task(&mut t);
    retry.attempt = 2;
    let run_id = t.get_orig_run_id().to_string();
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        Some(Job::FireTimer(_)) => vec![append(1, "s")],
        _ => vec![complete()],
    };
    let mut opts = StartOpts::new(&store);
    opts.served = None;
    let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
    opts.tasks = Some(receiver);
    let mut run = start(t, opts);
    tasks.send(first).unwrap();
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, workflow).await;
    tasks.send(second).unwrap();
    let fired = run.worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(fired.jobs[0].variant, Some(Job::FireTimer(_))));
    tasks.send(retry).unwrap();
    drop(tasks);
    run.all_taken.take().unwrap().await.unwrap();
    // The activation of the timed-out task appends, and its task is gone, so no Store sees it.
    respond(&run.worker, &fired, workflow).await;
    assert_eq!(run.drive(workflow).await.len(), 0);
    assert_eq!(store.requests().len(), 0);
    let jobs = run.drive(workflow).await;
    assert!(
        matches!(jobs.last().unwrap()[..], [Job::ResolveStreamAppend(_)]),
        "{jobs:?}"
    );
    let id = producer(&run_id, Default::default()).id;
    assert_eq!(writers(&store, "s"), [(id, 11, 2)]);
    assert_eq!(store.requests().len(), 1);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

async fn with_the_marker_on_a_later_page() {
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
    // The update of a task holds each event up to the task, so the check sees the marker on the
    // last of three pages.
    let events = third.history.take().unwrap().events;
    third.history = Some(History {
        events: events[..4].to_vec(),
    });
    third.next_page_token = vec![1];
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::ToTaskNum(1), ResponseType::Raw(third)];
    opts.pages = vec![page(&events[4..9], vec![2]), page(&events[9..], vec![])];
    let run = start(t, opts);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
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
            _ => None,
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

/// Returns a History up to task 3. Task 1 starts timer 1, task 2 completes with no command, and a
/// Signal schedules task 3.
fn history_to_task_3() -> TestHistoryBuilder {
    let mut t = history(Duration::from_secs(10));
    t.add_workflow_task_completed();
    let timer = t.add_timer_started("1".to_string());
    t.add_timer_fired(timer, "1".to_string());
    t.add_full_wf_task();
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// Adds tasks 4 and 5 to the History of a run whose task 3 started timer 3. The firing of timer 3
/// schedules task 4, which starts timer 4, and its firing schedules task 5.
fn add_tasks_4_and_5(t: &mut TestHistoryBuilder) {
    let events = t.get_full_history_info().unwrap().into_events();
    let started = (events.iter().rev())
        .find(|event| event.event_type() == EventType::TimerStarted)
        .unwrap();
    t.add_timer_fired(started.event_id, "3".to_string());
    t.add_full_wf_task();
    let timer = t.add_timer_started("4".to_string());
    t.add_timer_fired(timer, "4".to_string());
    t.add_workflow_task_scheduled_and_started();
}

/// Replays `t` from the poll `task`, with the rest of its History on `pages`, and returns the jobs
/// of each activation.
async fn replay_in_pages(
    store: &Arc<MemoryStore>,
    t: &TestHistoryBuilder,
    task: &PollWorkflowTaskQueueResponse,
    pages: Vec<GetWorkflowExecutionHistoryResponse>,
    workflow: Workflow,
) -> Vec<Vec<Job>> {
    let mut opts = StartOpts::new(store);
    opts.responses = vec![ResponseType::Raw(task.clone())];
    opts.pages = pages;
    let run = start(t.clone(), opts);
    let jobs = run.drive(workflow).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
    jobs
}

/// Replays `t` from a poll for task 2, whose History goes on past that task. The rest of the
/// History comes on one page, and then on two pages for each cut. Checks that each cut gives the
/// jobs of the one page, which end with the firing of timer 4.
async fn replay_at_each_cut(store: &Arc<MemoryStore>, t: &TestHistoryBuilder, workflow: Workflow) {
    let mut task = hist_to_poll_resp(t, "fake_wf_id", ResponseType::ToTaskNum(2)).resp;
    let polled = task.history.as_ref().unwrap().events.len();
    task.next_page_token = vec![1];
    let events = t.get_full_history_info().unwrap().into_events();
    let pages = vec![page(&events[polled..], vec![])];
    let whole = replay_in_pages(store, t, &task, pages, workflow).await;
    let fired = Job::FireTimer(FireTimer { seq: 4 });
    assert_eq!(whole.last(), Some(&vec![fired]), "{whole:?}");
    // The last cut leaves the second page with no events.
    for cut in polled + 1..=events.len() {
        let pages = vec![
            page(&events[polled..cut], vec![2]),
            page(&events[cut..], vec![]),
        ];
        let paged = replay_in_pages(store, t, &task, pages, workflow).await;
        assert_eq!(paged, whole, "cut {cut}");
    }
}

/// Appends to Stream `s` at the Signal, and starts timer 3 once the append resolves.
fn append_then_timers(activation: &WorkflowActivation) -> Vec<Command> {
    let fired = |seq| {
        (activation.jobs.iter())
            .any(|job| matches!(&job.variant, Some(Job::FireTimer(f)) if f.seq == seq))
    };
    match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        Some(Job::SignalWorkflow(_)) => vec![append(1, "s")],
        Some(Job::ResolveStreamAppend(_)) => vec![timer(3)],
        _ if fired(3) => vec![timer(4)],
        _ if fired(4) => vec![complete()],
        _ => vec![],
    }
}

// The completion of the task before a page end can be on the next page, with the marker of an
// append whose result Workflow code waits for before it starts a timer.
#[tokio::test]
async fn an_append_replays_the_same_wherever_a_page_ends() {
    let store = store(&["s"]).await;
    let first = start(history_to_task_3(), StartOpts::new(&store));
    loop {
        let activation = first.worker.poll_workflow_activation().await.unwrap();
        respond(&first.worker, &activation, append_then_timers).await;
        if resolves_append(&activation, 1) {
            break;
        }
    }
    first.worker.drain_pollers_and_shutdown_ref().await;
    let mut t = first.history.lock().clone();
    add_tasks_4_and_5(&mut t);
    replay_at_each_cut(&store, &t, append_then_timers).await;
    assert_eq!(store.requests().len(), 1);
}

#[derive(Clone, Copy, Debug)]
enum UpdateTask {
    OfACachedRun,
    OfANewRun,
}

fn local_activity(seq: u32) -> Command {
    let local_activity = ScheduleLocalActivity {
        seq,
        activity_id: seq.to_string(),
        activity_type: "la".to_string(),
        start_to_close_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
        ..Default::default()
    };
    local_activity.into()
}

fn timer(seq: u32) -> Command {
    let timer = StartTimer {
        seq,
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
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
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

/// The activations of task 2 in which the first run got each result, by the seq of its call.
/// Workflow code asks for the closes in task 1. It schedules the Local Activities, and starts the
/// appends and the reads, at the Signal that starts task 2, which has no other job.
#[derive(Debug)]
struct Held {
    locals: Vec<u64>,
    appends: Vec<u64>,
    closes: Vec<u64>,
    /// The activations of the pages of each read, each with one Record.
    reads: Vec<Vec<u64>>,
}

impl Held {
    fn generate(rng: &mut StdRng) -> Self {
        let mut at = |count: usize, first: u64| -> Vec<u64> {
            let count = rng.random_range(0..=count);
            (0..count).map(|_| rng.random_range(first..=6)).collect()
        };
        // A result of a call that Workflow code made at the Signal comes after it.
        let (locals, appends, closes) = (at(3, 1), at(3, 1), at(2, 0));
        // A read gives at most one page in an activation.
        let reads = (0..rng.random_range(0..=2))
            .map(|_| (1..=6).filter(|_| rng.random_bool(0.3)).collect())
            .filter(|pages: &Vec<u64>| !pages.is_empty())
            .collect();
        Self {
            locals,
            appends,
            closes,
            reads,
        }
    }

    fn count(&self) -> usize {
        let pages: usize = self.reads.iter().map(Vec::len).sum();
        self.locals.len() + self.appends.len() + self.closes.len() + pages
    }

    /// Returns the History of the first run, with chunking disabled.
    fn history(&self) -> TestHistoryBuilder {
        let seqs = |activations: &Vec<u64>| (1..).zip(activations.clone());
        let mut t = history(Duration::from_secs(10));
        let intents = StreamMarkerData {
            streams: seqs(&self.closes)
                .map(|(seq, _)| stream(&format!("c{seq}")))
                .collect(),
            close_intents: seqs(&self.closes)
                .map(|(seq, _)| StreamCloseIntent {
                    seq,
                    stream: seq - 1,
                })
                .collect(),
            ..Default::default()
        };
        if self.closes.is_empty() {
            t.add_workflow_task_completed();
        } else {
            complete_with_marker(&mut t, &intents);
        }
        t.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
        t.add_we_signaled("go", vec![]);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_completed();
        let completed = t.current_event_id();
        // The marker of a Local Activity comes when it resolves.
        let mut locals: Vec<(u32, u64)> = seqs(&self.locals).collect();
        locals.sort_by_key(|(_, activation)| *activation);
        for (seq, activation) in locals {
            let result = Some(Payload::default());
            t.add_local_activity_marker(seq, &seq.to_string(), result, None, |marker| {
                marker.activity_type = "la".to_string();
                marker.activation_index = Some(activation);
            });
        }
        let offsets = StreamAppendOffsets {
            first_offset: vec![1],
            last_offset: vec![1],
        };
        let reads = (1..).zip(&self.reads);
        let read = |(seq, pages): (u32, &Vec<u64>)| StreamReadEntry {
            seq,
            stream: self.appends.len() as u32 + seq - 1,
            page_records: vec![1; pages.len()],
            page_activations: pages.clone(),
            records: (1..=pages.len() as u8)
                .map(|offset| StreamRecord {
                    producer: Some(Producer {
                        id: "client:test".to_string(),
                        kind: Some(ProducerKind::Client(ClientProducer {})),
                    }),
                    body: Some(Payload::default()),
                    offset: vec![offset],
                })
                .collect(),
            ..Default::default()
        };
        let data = StreamMarkerData {
            streams: (seqs(&self.appends).map(|(seq, _)| format!("a{seq}")))
                .chain((1..=self.reads.len()).map(|seq| format!("r{seq}")))
                .map(|id| stream(&id))
                .collect(),
            appends: seqs(&self.appends)
                .map(|(seq, activation)| StreamAppendResult {
                    seq,
                    stream: seq - 1,
                    activation,
                    result: Some(stream_append_result::Result::Appended(offsets.clone())),
                })
                .collect(),
            closes: seqs(&self.closes)
                .map(|(seq, activation)| StreamCloseResult {
                    seq,
                    activation,
                    failure: None,
                })
                .collect(),
            reads: reads.map(read).collect(),
            ..Default::default()
        };
        if !data.appends.is_empty() || !data.closes.is_empty() || !data.reads.is_empty() {
            t.add(MarkerRecordedEventAttributes {
                marker_name: STREAM_MARKER_NAME.to_string(),
                details: marker_details(&data),
                workflow_task_completed_event_id: completed,
                ..Default::default()
            });
        }
        t.add_workflow_execution_completed();
        t
    }

    /// Returns the names of the jobs of each activation of the first run, sorted.
    fn jobs(&self) -> Vec<Vec<String>> {
        let named = |kind: &'static str, activations: &Vec<u64>| {
            let named = (1..).zip(activations.clone());
            named.map(move |(seq, activation)| (activation, format!("{kind} {seq}")))
        };
        let mut task: BTreeMap<u64, Vec<String>> = BTreeMap::new();
        task.entry(0).or_default().push("signal".to_string());
        let pages = (1..).zip(&self.reads).flat_map(|(seq, pages)| {
            (pages.iter()).map(move |activation| (*activation, format!("read {seq}")))
        });
        let results = (named("local activity", &self.locals))
            .chain(named("append", &self.appends))
            .chain(named("close", &self.closes))
            .chain(pages);
        for (activation, name) in results {
            task.entry(activation).or_default().push(name);
        }
        let task = task.into_values().map(|mut names| {
            names.sort();
            names
        });
        iter::once(vec!["init".to_string()]).chain(task).collect()
    }
}

fn job_name(job: &Job) -> String {
    match job {
        Job::InitializeWorkflow(_) => "init".to_string(),
        Job::SignalWorkflow(_) => "signal".to_string(),
        Job::ResolveActivity(result) => format!("local activity {}", result.seq),
        Job::ResolveStreamAppend(result) => format!("append {}", result.seq),
        Job::ResolveStreamClose(result) => format!("close {}", result.seq),
        Job::ResolveStreamRead(page) => format!("read {}", page.seq),
        other => format!("{other:?}"),
    }
}

// A replay gives each result that a task holds, of a Local Activity, an append, a close or a read,
// in the activation in which the first run got it, when no other job of the task reaches that
// activation.
#[tokio::test]
async fn a_replay_gives_each_held_result_in_its_activation() {
    let store = store(&[]).await;
    for seed in 0..200 {
        // Seed 0 holds an append result at activation 3 and a Local Activity result at 5.
        let held = match seed {
            0 => Held {
                locals: vec![5],
                appends: vec![3],
                closes: vec![],
                reads: vec![],
            },
            _ => Held::generate(&mut StdRng::seed_from_u64(seed)),
        };
        if held.count() == 0 {
            continue;
        }
        let mut opts = StartOpts::new(&store);
        opts.replay = true;
        let replay = start(held.history(), opts);
        let count = |activations: &Vec<u64>| 1..=activations.len() as u32;
        let (locals, appends, closes) = (
            count(&held.locals),
            count(&held.appends),
            count(&held.closes),
        );
        let reads = 1..=held.reads.len() as u32;
        let mut left = held.count();
        let jobs = replay
            .drive(|activation| {
                let mut commands = vec![];
                for job in &activation.jobs {
                    match job.variant.as_ref() {
                        Some(Job::InitializeWorkflow(_)) => commands
                            .extend(closes.clone().map(|seq| close(seq, &format!("c{seq}")))),
                        Some(Job::SignalWorkflow(_)) => {
                            commands.extend(locals.clone().map(local_activity));
                            let append = |seq| append(seq, &format!("a{seq}"));
                            commands.extend(appends.clone().map(append));
                            commands.extend(reads.clone().map(|seq| {
                                StartStreamRead {
                                    seq,
                                    stream: Some(stream(&format!("r{seq}"))),
                                    records_in_history: true,
                                    ..Default::default()
                                }
                                .into()
                            }));
                        }
                        _ => left -= 1,
                    }
                }
                if left == 0 {
                    commands.push(complete());
                }
                commands
            })
            .await;
        replay.worker.drain_pollers_and_shutdown_ref().await;
        let names: Vec<Vec<String>> = (jobs.iter())
            .map(|jobs| {
                let mut names: Vec<String> = jobs.iter().map(job_name).collect();
                names.sort();
                names
            })
            .collect();
        assert_eq!(names, held.jobs(), "seed {seed}: {held:?}");
    }
}
