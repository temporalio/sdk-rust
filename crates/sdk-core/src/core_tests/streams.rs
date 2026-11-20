//! The Stream calls of Workflow code, through a Worker on a mock server. The mock server writes the
//! History of each completion, so a replay Worker can replay it.

use crate::{
    StreamRuntimes, StreamSettings, Worker, WorkerConfig, WorkflowErrorType, init_replay_worker,
    internal_flags::CoreInternalFlags,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MockPollCfg, MocksHolder, NAMESPACE, PollWFTRespExt, ResponseType, WorkerExt,
        build_mock_pollers, hist_to_poll_resp, mock_worker,
        stream_store::{MemoryStore, Op, Request},
        test_worker_cfg,
    },
    worker::{
        client::{WorkflowTaskCompletion, mocks::mock_worker_client},
        streams::marker_details,
    },
};
use futures_util::{StreamExt, stream};
use parking_lot::Mutex;
use prost::Message;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap, HashSet},
    iter,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        FromJsonPayloadExt,
        external_data::{
            StreamAppendOffsets, StreamAppendResult, StreamCloseIntent, StreamCloseResult,
            StreamMarkerData, stream_append_result,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, StreamAppended, WorkflowActivation,
            remove_from_cache::EvictionReason, resolve_stream_append,
            workflow_activation_job::Variant as Job,
        },
        workflow_commands::{
            AppendToStream, CancelWorkflowExecution, CloseStream, CompleteWorkflowExecution,
            ContinueAsNewWorkflowExecution, FailWorkflowExecution, QueryResult, QuerySuccess,
            ScheduleLocalActivity, StartTimer, UpdateResponse, query_result, update_response,
            workflow_command::Variant as Command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::Payload,
        enums::v1::{CommandType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::MarkerRecordedEventAttributes,
        query::v1::WorkflowQuery,
        workflowservice::v1::{
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_streams::{
    StreamError, StreamRuntime, StreamStore,
    attempt::producer_key,
    proto::{
        AppendRecord, AppendRecordsRequest, CreateStreamRequest, Producer, StreamRef,
        WorkflowProducer, WorkflowTaskAttempt, producer::Kind as ProducerKind,
    },
    record::StoreLimits,
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
        store.create_stream(create, id.to_string()).await.unwrap();
    }
    store
}

fn runtimes(store: &Arc<MemoryStore>) -> StreamRuntimes {
    let runtime = StreamRuntime::new(store.clone(), StoreLimits::default());
    StreamRuntimes::new([(STORE.to_string(), Arc::new(runtime))])
}

/// Prints the seed of a generated test if the test panics while this lives. A panic on a thread of
/// Core reaches the test with no seed in its message.
struct SeedOnPanic(u64);

impl Drop for SeedOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("The test failed at seed {}", self.0);
        }
    }
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
            let deadline = Instant::now() + Duration::from_secs(30);
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
    let fail = move |cause: &WorkflowTaskFailedCause, failure: &Option<Failure>| {
        let message = failure.as_ref().map(|f| f.message.clone());
        let mut failed = kept.lock();
        failed.push((*cause, message.unwrap_or_default()));
        assert!(failed.len() <= fails, "A task failed: {failed:?}");
    };
    let from_channel = tasks.is_some();
    let mut mock = match tasks {
        None => {
            let mut cfg = MockPollCfg::from_resps(t, responses);
            cfg.make_poll_stream_interminable = true;
            cfg.num_expected_completions = Some((0..).into());
            cfg.num_expected_fails = fails;
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
        close_intents: vec![StreamCloseIntent {
            seq: 1,
            stream_id: stream_id.to_string(),
            store: STORE.to_string(),
        }],
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
            Some(Attributes::CancelWorkflowExecutionCommandAttributes(_)) => {
                history.add_cancelled();
            }
            Some(Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(_)) => {
                history.add_continued_as_new();
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
    workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
) -> Vec<Vec<Job>> {
    drive_until_evicted(worker, workflow).await.0
}

/// Does what [`drive`] does, and also returns the reason of the eviction.
async fn drive_until_evicted(
    worker: &Worker,
    mut workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
) -> (Vec<Vec<Job>>, Option<EvictionReason>) {
    let mut jobs = vec![];
    let mut evicted = None;
    while let Ok(activation) = worker.poll_workflow_activation().await {
        let run_id = activation.run_id.clone();
        if let [job] = &activation.jobs[..]
            && let Some(Job::RemoveFromCache(eviction)) = &job.variant
        {
            evicted = Some(eviction.reason());
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
    (jobs, evicted)
}

impl Run {
    async fn drive(
        &self,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        drive(&self.worker, workflow).await
    }

    /// Replays the History that the completions wrote on a replay Worker with the same Store, and
    /// returns the jobs of each activation. The replay must reach the end of that History.
    async fn replay(
        &self,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        let t = self.history.lock().clone();
        let mut opts = StartOpts::new(&self.store);
        opts.replay = true;
        let replay = start(t, opts);
        let (jobs, evicted) = drive_until_evicted(&replay.worker, workflow).await;
        assert_eq!(evicted, Some(EvictionReason::LangRequested), "{jobs:?}");
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
                Some(StreamMarkerData::from_json_payload(payload).unwrap())
            }
            _ => None,
        })
}

fn command_types(completion: &WorkflowTaskCompletion) -> Vec<CommandType> {
    (completion.commands.iter())
        .map(|command| command.command_type())
        .collect()
}

fn producer(run_id: &str, attempt: WorkflowTaskAttempt) -> Producer {
    Producer {
        kind: Some(ProducerKind::Workflow(WorkflowProducer {
            namespace: NAMESPACE.to_string(),
            workflow_id: "fake_wf_id".to_string(),
            run_id: run_id.to_string(),
            attempt: Some(attempt),
        })),
    }
}

#[tokio::test]
async fn an_append_writes_as_its_workflow_task_and_replays_from_its_marker() {
    let store = store(&["s"]).await;
    let t = history(Duration::from_secs(60));
    let run_id = t.get_orig_run_id().to_string();
    let started_time = t.get_full_history_info().unwrap().events()[2].event_time;
    let run = start(t, StartOpts::new(&store));
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        _ => vec![complete()],
    };
    let live = run.drive(workflow).await;

    let records = store.records("s");
    let attempt = WorkflowTaskAttempt {
        started_event_id: 3,
        attempt: 1,
        started_time,
    };
    assert_eq!(records[0].producer, Some(producer(&run_id, attempt)));
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
        stream_id: "s".to_string(),
        store: STORE.to_string(),
        activation_index: 1,
        result: Some(stream_append_result::Result::Appended(
            StreamAppendOffsets {
                first_offset: offset.clone(),
                last_offset: offset,
            },
        )),
    };
    let expected = StreamMarkerData {
        appends: vec![entry],
        ..Default::default()
    };
    assert_eq!(marker(&run.sent()[0]), Some(expected));
    run.worker.drain_pollers_and_shutdown_ref().await;
    let writes = store.requests().len();
    assert_eq!(run.replay(workflow).await, live);
    assert_eq!(store.requests().len(), writes);
    // Workflow code that ends with the append never gets the result that History has for it.
    let mut opts = StartOpts::new(&store);
    opts.replay = true;
    let changed = start(run.history.lock().clone(), opts);
    let ends_early = |_: &WorkflowActivation| vec![append(1, "s"), complete()];
    let (jobs, evicted) = drive_until_evicted(&changed.worker, ends_early).await;
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    assert_eq!(evicted, Some(EvictionReason::Nondeterminism));
    changed.worker.drain_pollers_and_shutdown_ref().await;
}

#[rstest::rstest]
#[case::the_server_takes_it(None)]
#[case::a_failed_task(Some(1))]
#[case::a_refused_completion(Some(2))]
#[tokio::test]
async fn a_close_takes_effect_only_after_the_server_took_its_task(#[case] failure: Option<u8>) {
    let store = store(&["c", "s"]).await;
    let closes = |store: &MemoryStore| {
        let closes = (store.requests().into_iter()).filter(|r| matches!(r, Request::Close(_)));
        closes.count()
    };
    let mut opts = StartOpts::new(&store);
    let watched = store.clone();
    opts.served = Some(Box::new(move |_, t| {
        let ran = closes(&watched);
        assert_eq!(ran, 0, "The close ran before the server took its task");
        Some(next_task(t))
    }));
    opts.fails = usize::from(failure == Some(1));
    opts.refuse = (failure == Some(2)).then_some(1);
    let run = start(history(Duration::from_secs(60)), opts);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    // The result of the append gives a second activation of the task.
    let commands = vec![close(1, "c"), append(1, "s")];
    let completion = WorkflowActivationCompletion::from_cmds(first.run_id.clone(), commands);
    run.worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
    let appended = run.worker.poll_workflow_activation().await.unwrap();
    assert!(resolves_append(&appended, 1), "{appended:?}");
    let completion = match failure {
        Some(1) => WorkflowActivationCompletion::fail(appended.run_id, "boom".into(), None),
        _ => WorkflowActivationCompletion::empty(appended.run_id),
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
        let marker = marker(&run.sent()[0]).unwrap();
        assert_eq!(marker.close_intents, close_intent("c").close_intents);
    } else {
        assert_eq!(rest, Vec::<Vec<Job>>::new());
        assert_eq!(closes(&store), 0);
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(store.is_closed("c"), failure.is_none());
}

/// The state of a call when the last command of the Workflow comes.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AtTheEnd {
    /// An activation took the result.
    Taken,
    /// The Store answered while the last activation was out, so no activation took the result.
    Answered,
    /// The Store has the request, and gave no answer.
    Runs,
    /// The close waits behind a close of its Stream that runs.
    Waits,
    /// Workflow code asked for the close in the last task, so History has no intent of it.
    Asked,
    /// The call comes in the completion that has the last command.
    WithTheCommand,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum End {
    Complete,
    Fail,
    Cancel,
    ContinueAsNew,
    /// Lang reports a non-determinism error, and Core fails the Workflow for it.
    CoreFails,
}

#[derive(Clone, Debug)]
struct EndingCall {
    op: Op,
    seq: u32,
    stream: String,
    state: AtTheEnd,
}

impl EndingCall {
    fn command(&self) -> Command {
        match self.op {
            Op::Append => append(self.seq, &self.stream),
            Op::Close => close(self.seq, &self.stream),
        }
    }

    fn reaches_the_store(&self) -> bool {
        use AtTheEnd::*;
        matches!(self.state, Taken | Answered | Runs)
    }
}

/// A Workflow that ends in task 2 while it has Stream calls. Task 1 asked for the closes whose
/// intents History has, and a Signal starts task 2.
#[derive(Debug)]
struct Ending {
    end: End,
    calls: Vec<EndingCall>,
    /// The number of the closes that task 1 asked for. They're the first closes.
    in_history: usize,
    /// The indexes of the calls whose results Workflow code takes, in the order of their results.
    taken: Vec<usize>,
    /// The number of the calls that come ahead of the last command in its completion.
    ahead: usize,
}

impl Ending {
    fn generate(rng: &mut StdRng) -> Self {
        use AtTheEnd::*;
        let end = [
            End::Complete,
            End::Fail,
            End::Cancel,
            End::ContinueAsNew,
            End::CoreFails,
        ][rng.random_range(0..5)];
        let mut calls: Vec<EndingCall> = vec![];
        // Adds a call with the next seq of its kind, on a Stream that no other call has.
        let add = |calls: &mut Vec<EndingCall>, op: Op, state| {
            let seq = calls.iter().filter(|call| call.op == op).count() as u32 + 1;
            calls.push(EndingCall {
                op,
                seq,
                stream: format!("{op:?}{seq}"),
                state,
            });
        };
        for _ in 0..rng.random_range(0..=3) {
            let state = [Taken, Answered, Runs][rng.random_range(0..3)];
            add(&mut calls, Op::Close, state);
            if state == Runs && rng.random_bool(0.5) {
                let ahead = calls.last().unwrap().stream.clone();
                add(&mut calls, Op::Close, Waits);
                calls.last_mut().unwrap().stream = ahead;
            }
        }
        let in_history = calls.len();
        for _ in 0..rng.random_range(0..=2) {
            add(&mut calls, Op::Close, Asked);
        }
        for _ in 0..rng.random_range(0..=3) {
            let state = [Taken, Answered, Runs][rng.random_range(0..3)];
            add(&mut calls, Op::Append, state);
        }
        // With no result to take, the Signal's activation is the last one.
        if calls.iter().all(|call| call.state != Taken) {
            for call in &mut calls[in_history..] {
                call.state = WithTheCommand;
            }
        }
        for _ in 0..rng.random_range(0..=2) {
            let op = [Op::Append, Op::Close][rng.random_range(0..2)];
            add(&mut calls, op, WithTheCommand);
        }
        // A failed completion has no command.
        if end == End::CoreFails {
            calls.retain(|call| call.state != WithTheCommand);
        }
        let mut taken: Vec<usize> = (0..calls.len())
            .filter(|at| calls[*at].state == Taken)
            .collect();
        for at in (1..taken.len()).rev() {
            taken.swap(at, rng.random_range(0..=at));
        }
        let with_the_command = calls.iter().filter(|call| call.state == WithTheCommand);
        let ahead = rng.random_range(0..=with_the_command.count());
        Self {
            end,
            calls,
            in_history,
            taken,
            ahead,
        }
    }

    fn with(&self, state: AtTheEnd) -> impl Iterator<Item = &EndingCall> {
        self.calls.iter().filter(move |call| call.state == state)
    }

    /// Returns the History up to task 2.
    fn history(&self, task_timeout: Duration) -> TestHistoryBuilder {
        let mut t = history(task_timeout);
        let closes = &self.calls[..self.in_history];
        let mut intents = StreamMarkerData::default();
        for close in closes {
            intents.close_intents.push(StreamCloseIntent {
                seq: close.seq,
                stream_id: close.stream.clone(),
                store: STORE.to_string(),
            });
        }
        if closes.is_empty() {
            t.add_workflow_task_completed();
        } else {
            complete_with_marker(&mut t, &intents);
        }
        t.add_we_signaled("go", vec![]);
        if self.end == End::Cancel {
            t.add_cancel_requested();
        }
        t.add_workflow_task_scheduled_and_started();
        t
    }

    /// Returns the commands of activation `number` of the Workflow. A replay sends the command that
    /// fails the Workflow where the first run reported its error.
    fn commands(&self, number: usize) -> Vec<Command> {
        use AtTheEnd::*;
        let mut commands = vec![];
        if number == 0 {
            let closes = &self.calls[..self.in_history];
            return closes.iter().map(EndingCall::command).collect();
        }
        if number == 1 {
            let sent =
                (self.calls[self.in_history..].iter()).filter(|call| call.state != WithTheCommand);
            commands.extend(sent.map(EndingCall::command));
        }
        if number == 1 + self.taken.len() {
            commands.extend(self.with(WithTheCommand).map(EndingCall::command));
            let last: Command = match self.end {
                End::Complete => complete(),
                End::Fail | End::CoreFails => FailWorkflowExecution::default().into(),
                End::Cancel => CancelWorkflowExecution::default().into(),
                End::ContinueAsNew => ContinueAsNewWorkflowExecution {
                    workflow_type: "wf".to_string(),
                    ..Default::default()
                }
                .into(),
            };
            commands.insert(
                commands.len() - self.with(WithTheCommand).count() + self.ahead,
                last,
            );
        }
        commands
    }
}

#[tokio::test]
async fn the_last_command_of_a_workflow_drops_each_call_with_no_result() {
    use AtTheEnd::*;
    let mut seen = HashSet::new();
    let mut had_outage = false;
    for seed in 0..40 {
        let _seed = SeedOnPanic(seed);
        let ending = Ending::generate(&mut StdRng::seed_from_u64(seed));
        seen.extend(ending.calls.iter().map(|call| (call.op, call.state as u8)));
        let ids: Vec<&str> = (ending.calls.iter())
            .map(|call| call.stream.as_str())
            .collect();
        let store = store(&ids).await;
        // In the first seed whose closes of History all run, their Store is down, and the
        // heartbeat of the task comes while its last activation is out.
        let closes = &ending.calls[..ending.in_history];
        let run_only = closes.iter().all(|call| matches!(call.state, Runs | Waits));
        let down = !had_outage && !closes.is_empty() && run_only;
        had_outage |= down;
        let in_outage = |id: &&&str| down && closes.iter().any(|close| close.stream == **id);
        for id in ids.iter().filter(|id| !in_outage(id)) {
            store.hold_stream(id);
        }
        if down {
            let error = StreamError::store_unavailable("The Store is down");
            store.fail(Op::Close, error, usize::MAX);
        }
        let mut opts = StartOpts::new(&store);
        opts.served = None;
        opts.configure = |wc| {
            wc.workflow_failure_errors = HashSet::from([WorkflowErrorType::Nondeterminism]);
        };
        let task_timeout = Duration::from_secs(if down { 1 } else { 60 });
        let run = start(ending.history(task_timeout), opts);
        let worker = &run.worker;
        let mut live = vec![];
        let last = 1 + ending.taken.len();
        let mut activation = worker.poll_workflow_activation().await.unwrap();
        // Core got the task before it gave this activation, so the heartbeat of the task comes
        // before `heartbeat`.
        let heartbeat = Instant::now() + task_timeout.mul_f32(0.9);
        for number in 0..=last {
            if number > 1 {
                // The Store answers one call, and Workflow code takes its result.
                store.release_stream(&ending.calls[ending.taken[number - 2]].stream);
            }
            if number > 0 {
                activation = worker.poll_workflow_activation().await.unwrap();
            }
            let jobs = activation.jobs.iter();
            live.push(
                jobs.map(|job| job.variant.clone().unwrap())
                    .collect::<Vec<_>>(),
            );
            let run_id = activation.run_id.clone();
            let mut completion =
                WorkflowActivationCompletion::from_cmds(run_id.clone(), ending.commands(number));
            if number == last {
                // Each call that reaches the Store is there, and the Store answers some of them
                // while the activation is out.
                let reached = ending.calls.iter().filter(|call| call.reaches_the_store());
                let reached = reached.count();
                wait_until(|| calls_at(&store).len() == reached).await;
                for call in ending.with(Answered) {
                    store.release_stream(&call.stream);
                    wait_until(|| written(&store, call)).await;
                }
                if down {
                    tokio::time::sleep(heartbeat.saturating_duration_since(Instant::now())).await;
                }
                if ending.end == End::CoreFails {
                    let cause = WorkflowTaskFailedCause::NonDeterministicError;
                    completion =
                        WorkflowActivationCompletion::fail(run_id, "changed".into(), Some(cause));
                }
            }
            worker
                .complete_workflow_activation(completion)
                .await
                .unwrap();
        }
        // No job reaches lang after the last command.
        let after = run.drive(|_| vec![]).await;
        assert_eq!(after, Vec::<Vec<Job>>::new(), "seed {seed}: {ending:?}");
        worker.drain_pollers_and_shutdown_ref().await;

        // The completion has the results that Workflow code took, and then the last command. It
        // has no intent of a close, and forces no task.
        let mut expected = StreamMarkerData::default();
        for (activation, at) in (1..).zip(&ending.taken) {
            let call = &ending.calls[*at];
            if call.op == Op::Close {
                expected.closes.push(StreamCloseResult {
                    seq: call.seq,
                    activation_index: activation,
                    failure: None,
                });
                continue;
            }
            let offset = store.records(&call.stream)[0].offset.clone();
            let offsets = StreamAppendOffsets {
                first_offset: offset.clone(),
                last_offset: offset,
            };
            expected.appends.push(StreamAppendResult {
                seq: call.seq,
                stream_id: call.stream.clone(),
                store: STORE.to_string(),
                activation_index: activation,
                result: Some(stream_append_result::Result::Appended(offsets)),
            });
        }
        let sent = run.sent();
        assert_eq!(sent.len(), 1, "seed {seed}: {ending:?}");
        let recorded = (!ending.taken.is_empty()).then_some(expected);
        assert_eq!(marker(&sent[0]), recorded, "seed {seed}: {ending:?}");
        let last_command = match ending.end {
            End::Complete => CommandType::CompleteWorkflowExecution,
            End::Fail | End::CoreFails => CommandType::FailWorkflowExecution,
            End::Cancel => CommandType::CancelWorkflowExecution,
            End::ContinueAsNew => CommandType::ContinueAsNewWorkflowExecution,
        };
        let types: Vec<CommandType> = (recorded.iter().map(|_| CommandType::RecordMarker))
            .chain([last_command])
            .collect();
        assert_eq!(command_types(&sent[0]), types, "seed {seed}: {ending:?}");
        assert!(!sent[0].force_create_new_workflow_task, "seed {seed}");

        // No Store call starts after the last command, and a call with no answer writes nothing.
        // A call reaches the Store once, but for a close in an outage, which tries again.
        let mut reached: Vec<(Op, String)> = (ending.calls.iter())
            .filter(|call| call.reaches_the_store())
            .map(|call| (call.op, call.stream.clone()))
            .collect();
        reached.sort();
        assert_eq!(calls_at(&store), reached, "seed {seed}: {ending:?}");
        assert!(
            down || store.requests().len() == reached.len(),
            "seed {seed}"
        );
        for call in &ending.calls {
            let answered = matches!(call.state, Taken | Answered);
            assert_eq!(written(&store, call), answered, "seed {seed}: {call:?}");
        }

        let mut number = 0;
        let replayed = run
            .replay(|_| {
                number += 1;
                ending.commands(number - 1)
            })
            .await;
        assert_eq!(replayed, live, "seed {seed}: {ending:?}");
    }
    assert!(had_outage);
    // The seeds reach each state of a call, for an append and for a close.
    let states = [Taken, Answered, Runs, WithTheCommand];
    let mut expected: HashSet<(Op, u8)> = (states.iter())
        .flat_map(|state| [(Op::Append, *state as u8), (Op::Close, *state as u8)])
        .collect();
    expected.extend([(Op::Close, Waits as u8), (Op::Close, Asked as u8)]);
    assert_eq!(seen, expected);
}

/// Returns the kind and the Stream of each call that reached the Store, once for each call.
fn calls_at(store: &MemoryStore) -> Vec<(Op, String)> {
    let mut calls: Vec<(Op, String)> = (store.requests().into_iter())
        .map(|request| match request {
            Request::Append(append, _) => (Op::Append, append.r#ref.unwrap().id),
            Request::Close(close) => (Op::Close, close.r#ref.unwrap().id),
        })
        .collect();
    calls.sort();
    calls.dedup();
    calls
}

/// Returns true if the Store wrote the append or the close `call`.
fn written(store: &MemoryStore, call: &EndingCall) -> bool {
    match call.op {
        Op::Append => !store.records(&call.stream).is_empty(),
        Op::Close => store.is_closed(&call.stream),
    }
}

fn fast_tries(wc: &mut WorkerConfig) {
    wc.stream_runtimes.settings = StreamSettings {
        retry_initial_interval: Duration::from_millis(10),
        retry_max_interval: Duration::from_millis(20),
        try_deadline: Duration::from_millis(50),
        ..Default::default()
    };
}

#[derive(Clone, Copy, Debug)]
enum Outage {
    AppendFails,
    AppendGetsNoAnswer,
    AppendPanics,
    CloseFails,
    AppendFailsWithNoLocalActivityManager,
    /// The task fails on two attempts. Core reports the failure of a task only on its first
    /// attempt, so the server learns of the second one by the task timeout.
    AppendFailsOnALaterAttempt,
}

#[rstest::rstest]
#[case(Outage::AppendFails)]
#[case(Outage::AppendGetsNoAnswer)]
#[case(Outage::AppendPanics)]
#[case(Outage::CloseFails)]
#[case(Outage::AppendFailsWithNoLocalActivityManager)]
#[case(Outage::AppendFailsOnALaterAttempt)]
#[tokio::test]
async fn a_call_in_an_outage_fails_its_task_at_the_heartbeat(#[case] outage: Outage) {
    let store = store(&["s"]).await;
    let down = StreamError::store_unavailable("The Store is down");
    match outage {
        Outage::AppendGetsNoAnswer => store.hold(Op::Append),
        Outage::AppendPanics => store.panic(Op::Append),
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
    let t = history(Duration::from_secs(1));
    let mut attempts = 1;
    if let Outage::AppendFailsOnALaterAttempt = outage {
        // The mock poller numbers the attempts of a task from 1.
        attempts = 2;
        let task = task_of(&t);
        opts.responses = vec![ResponseType::Raw(task.clone()), ResponseType::Raw(task)];
    }
    let run = start(t, opts);
    let workflow: Workflow = match outage {
        Outage::CloseFails => |_| vec![close(1, "s")],
        _ => |_| vec![append(1, "s")],
    };
    for _ in 0..attempts {
        let driven = tokio::time::timeout(Duration::from_secs(30), run.drive(workflow)).await;
        assert_eq!(driven.expect("The call never failed its task").len(), 1);
    }
    let (call, error) = match outage {
        Outage::CloseFails => ("close", "The Store is down"),
        Outage::AppendGetsNoAnswer => ("append", "The Store gave no answer in 50ms"),
        Outage::AppendPanics => ("append", "The Store panicked: The Store has a bug"),
        _ => ("append", "The Store is down"),
    };
    let message = format!(
        "StreamStoreUnavailable: {call} of Stream \"s\" on Store \"{STORE}\": \
         stream_store_unavailable: {error}"
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
    let task_timeout = Duration::from_millis(if query { 60_000 } else { 200 });
    let t = history(task_timeout);
    let mut opts = StartOpts::new(&store);
    // An append that waits at the hold must not count as a try with no answer.
    opts.configure = |wc| wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
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
    // A heartbeat of the forced task can complete it before the result comes.
    let recorded: Vec<usize> = (sent[1..].iter().filter_map(marker))
        .map(|marker| marker.appends.len())
        .collect();
    assert_eq!(recorded, [1]);
    assert_eq!(store.requests().len(), 1);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// An append of `a_batch_is_written_once_while_the_store_remembers_its_request_id`.
struct Append {
    seq: u32,
    stream: &'static str,
    bodies: Vec<Payload>,
    /// The answers that the Store loses.
    lost: usize,
    /// The number of later batches on the Stream, or all of them if there are fewer, after which
    /// the Store writes the first try. That try gets no answer.
    late: Option<usize>,
}

#[tokio::test]
async fn a_batch_is_written_once_while_the_store_remembers_its_request_id() {
    let ids = ["a", "b", "c"];
    let (mut lost_twice, mut dropped, mut written_twice) = (0, 0, 0);
    for seed in 0..12 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let mut appends: Vec<Append> = (1..=rng.random_range(2..=6))
            .map(|seq| Append {
                seq,
                stream: ids[rng.random_range(0..3)],
                bodies: (0..rng.random_range(1..=3))
                    .map(|at| Payload {
                        data: vec![seq as u8, at],
                        ..Default::default()
                    })
                    .collect(),
                lost: rng.random_range(0..=3),
                late: rng.random_bool(0.3).then(|| rng.random_range(0..=3)),
            })
            .collect();
        // The first try of append 1 comes after all later batches on its Stream, which has one at
        // least and no other late try. The Store of an even seed remembers fewer batches than
        // that, and the Store of an odd seed more.
        let first = appends[0].stream;
        appends[1].stream = first;
        for append in appends.iter_mut().filter(|append| append.stream == first) {
            append.late = None;
        }
        let later = appends[1..]
            .iter()
            .filter(|append| append.stream == first)
            .count();
        appends[0].late = Some(later);
        let window = match seed % 2 {
            0 => rng.random_range(1..=later),
            _ => later + 1,
        };
        lost_twice += usize::from(appends.iter().any(|append| append.lost > 1));
        let store = store(&ids).await;
        store.window.store(window, Ordering::Relaxed);
        let mut opts = StartOpts::new(&store);
        opts.configure = fast_tries;
        let run = start(history(Duration::from_secs(60)), opts);
        // The model of each Stream, as the seqs of its batches in the order that they're written.
        let mut model: HashMap<&str, Vec<u32>> = HashMap::new();
        let mut done = 0;
        // Returns the appends whose late first try the Store writes once `done` appends have a
        // result, and marks them in `landed`.
        let due = |done: usize, landed: &mut [bool]| -> Vec<usize> {
            let mut due = vec![];
            for at in 0..done {
                let Some(late) = appends[at].late else {
                    continue;
                };
                let on_stream = (appends[at + 1..done].iter())
                    .filter(|append| append.stream == appends[at].stream)
                    .count();
                if !landed[at] && (on_stream >= late || done == appends.len()) {
                    landed[at] = true;
                    due.push(at);
                }
            }
            due
        };
        let mut landed = vec![false; appends.len()];
        for results in 0..=appends.len() {
            if results > 0 {
                model
                    .entry(appends[results - 1].stream)
                    .or_default()
                    .push(results as u32);
            }
            for at in due(results, &mut landed) {
                let batches = model.entry(appends[at].stream).or_default();
                let seq = appends[at].seq;
                if batches[batches.len().saturating_sub(window)..].contains(&seq) {
                    dropped += 1;
                } else {
                    batches.push(seq);
                    written_twice += 1;
                }
            }
        }
        // The next append starts once the last one has its result, so each fault hits the tries
        // of one append, and a late try lands with no append in flight.
        let mut landed = vec![false; appends.len()];
        let jobs = run
            .drive(|_| {
                for at in due(done, &mut landed) {
                    let seq = appends[at].seq as u8;
                    let late = (store.requests().into_iter()).find_map(|request| match request {
                        Request::Append(append, request_id) => {
                            (append.records[0].body.as_ref().unwrap().data[0] == seq)
                                .then_some(request_id)
                        }
                        Request::Close(_) => None,
                    });
                    store.write_late(&late.unwrap());
                }
                let Some(append) = appends.get(done) else {
                    return vec![complete()];
                };
                done += 1;
                store.lose_answers(Op::Append, append.lost);
                store.delay_appends(usize::from(append.late.is_some()));
                let command = AppendToStream {
                    seq: append.seq,
                    stream: Some(stream(append.stream)),
                    bodies: append.bodies.clone(),
                };
                vec![command.into()]
            })
            .await;
        run.worker.drain_pollers_and_shutdown_ref().await;
        for id in ids {
            let written: Vec<Payload> = (store.records(id).into_iter())
                .map(|record| record.body.unwrap())
                .collect();
            let batches = model.get(id).into_iter().flatten();
            let expected: Vec<Payload> = (batches)
                .flat_map(|seq| appends[*seq as usize - 1].bodies.clone())
                .collect();
            assert_eq!(written, expected, "seed {seed}: Stream {id}");
        }
        let got: HashMap<u32, StreamAppended> = (jobs.iter().flatten())
            .filter_map(|job| match job {
                Job::ResolveStreamAppend(ResolveStreamAppend {
                    seq,
                    status: Some(resolve_stream_append::Status::Appended(appended)),
                }) => Some((*seq, appended.clone())),
                _ => None,
            })
            .collect();
        let mut tries: HashMap<u8, Vec<(AppendRecordsRequest, String)>> = HashMap::new();
        for request in store.requests() {
            if let Request::Append(append, request_id) = request {
                let seq = append.records[0].body.as_ref().unwrap().data[0];
                tries.entry(seq).or_default().push((append, request_id));
            }
        }
        let mut request_ids = HashSet::new();
        for append in &appends {
            // Workflow code got the offsets of the batch's first write.
            let records = store.records(append.stream);
            let seq = append.seq;
            let first = (records.iter())
                .position(|record| record.body.as_ref().unwrap().data[0] == seq as u8)
                .unwrap();
            let offsets = StreamAppended {
                first_offset: records[first].offset.clone(),
                last_offset: records[first + append.bodies.len() - 1].offset.clone(),
            };
            assert_eq!(got.get(&seq), Some(&offsets), "seed {seed}: append {seq}");
            let sent = &tries[&(seq as u8)];
            let count = append.lost + 1 + usize::from(append.late.is_some());
            assert_eq!(sent.len(), count, "seed {seed}: append {seq}");
            let request_id = &sent[0].1;
            assert!(
                sent.iter().all(|request| request == &sent[0])
                    && !request_id.is_empty()
                    && request_ids.insert(request_id.clone()),
                "seed {seed}: append {seq}: {sent:?}"
            );
        }
    }
    // A late try that the Store still remembers writes nothing, and one that it forgot writes its
    // batch a second time, after the later ones.
    let counts = [lost_twice, dropped, written_twice];
    assert!(counts.iter().all(|count| *count > 0), "{counts:?}");
}

#[tokio::test]
async fn a_call_in_an_outage_goes_on_when_a_full_marker_completes_its_task() {
    let store = store(&["c", "f"]).await;
    // The close is in an outage once it runs, and each append fails with a large message.
    store.fail(
        Op::Close,
        StreamError::store_unavailable("down"),
        usize::MAX,
    );
    store.fail(
        Op::Append,
        StreamError::invalid_argument("x".repeat(4000)),
        usize::MAX,
    );
    store.hold(Op::Append);
    let mut opts = StartOpts::new(&store);
    // An append that waits at the hold must not count as a try with no answer.
    opts.configure = |wc| {
        fast_tries(wc);
        wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
    };
    // Task 2 has a Signal, so that Workflow code gets an activation in it.
    opts.served = Some(Box::new(|number, t| {
        if number == 1 {
            t.add_we_signaled("go", vec![]);
        }
        Some(next_task(t))
    }));
    let run = start(history(Duration::from_secs(60)), opts);
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
    let is_append = |request: &Request| matches!(request, Request::Append(..));
    while results.len() < 70 {
        // A release lets go only an append that the Store already holds.
        wait_until(|| store.requests().into_iter().filter(is_append).count() > results.len()).await;
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
    store.fail(Op::Close, StreamError::store_unavailable("down"), 0);
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
#[case::with_a_marker_to_send(true, 1)]
#[case::with_nothing_else(false, 1)]
#[case::with_no_cache(false, 0)]
#[tokio::test]
async fn an_eviction_completes_a_task_that_a_call_holds(
    #[case] with_a_marker: bool,
    #[case] cache: usize,
) {
    let store = store(&["s"]).await;
    let mut opts = StartOpts::new(&store);
    opts.cache = cache;
    opts.served = None;
    let run = start(history(Duration::from_secs(60)), opts);
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
    // The completion has the sticky queue of a Worker with a cache, as any completion has.
    assert_eq!(last.sticky_attributes.is_some(), cache > 0);
    let recorded = marker(last).map(|marker| marker.appends.len());
    assert_eq!(recorded, with_a_marker.then_some(1));
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[tokio::test]
async fn a_replay_worker_makes_no_live_call() {
    let store = store(&["s", "c"]).await;
    let mut t = history(Duration::from_secs(60));
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

#[tokio::test]
async fn a_task_at_a_bound_completes_with_a_new_one_forced() {
    let ids: Vec<String> = (1..=70).map(|seq| format!("s{seq}")).collect();
    let store = store(&ids.iter().map(String::as_str).collect::<Vec<_>>()).await;
    let long = StreamError::invalid_argument("x".repeat(4000));
    store.fail(Op::Append, long, 70);
    let run = start(history(Duration::from_secs(60)), StartOpts::new(&store));
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => (1..=70)
            .map(|seq| append(seq, &format!("s{seq}")))
            .collect(),
        _ if resolves_append(activation, 70) => vec![complete()],
        _ => vec![],
    };
    let live = run.drive(workflow).await;
    let sent = run.sent();
    let (last, forced) = sent.split_last().unwrap();
    assert!(forced.iter().all(|c| c.force_create_new_workflow_task) && !forced.is_empty());
    assert!(!last.force_create_new_workflow_task);
    let markers: Vec<StreamMarkerData> = sent.iter().filter_map(marker).collect();
    assert!(markers.iter().all(|m| m.encoded_len() <= 256 * 1024));
    let results = markers.iter().map(|m| m.appends.len()).sum::<usize>();
    assert_eq!(results, 70);
    let mut got: Vec<u32> = (live.iter().flatten())
        .filter_map(|job| match job {
            Job::ResolveStreamAppend(append) => Some(append.seq),
            _ => None,
        })
        .collect();
    got.sort_unstable();
    assert_eq!(got, (1..=70).collect::<Vec<_>>());
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

/// Returns the key, the started event id and the attempt of a Workflow's Producer.
fn writer(producer: Option<Producer>) -> (Vec<u8>, i64, i32) {
    let producer = producer.unwrap();
    let Some(ProducerKind::Workflow(WorkflowProducer {
        attempt: Some(task),
        ..
    })) = &producer.kind
    else {
        panic!("{producer:?}");
    };
    let key = producer_key(&producer).unwrap();
    (key, task.started_event_id, task.attempt)
}

/// Returns the writer of each Record of the Stream `id`.
fn writers(store: &MemoryStore, id: &str) -> Vec<(Vec<u8>, i64, i32)> {
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
async fn a_run_on_a_new_attempt_or_worker_writes_above_each_earlier_attempt(
    #[case] earlier: EarlierAttempt,
) {
    let store = store(&["s", "c"]).await;
    // Task 1 recorded the intent of close 1.
    let mut t = history(Duration::from_secs(60));
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
        let attempt = WorkflowTaskAttempt {
            started_event_id: 7,
            attempt: 1,
            started_time: None,
        };
        let write = AppendRecordsRequest {
            r#ref: Some(stream("s")),
            producer: Some(producer(&run_id, attempt)),
            records: vec![AppendRecord::default()],
        };
        store.append_records(write, "x".to_string()).await.unwrap();
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
    if let EarlierAttempt::Eviction = earlier {
        // The next task of the evicted run comes by a poll, as the task of another Worker.
        opts.served = None;
    }
    opts.fails = usize::from(matches!(earlier, EarlierAttempt::LaterRetryOfAFailedTask));
    let run = start(t, opts);
    // The Workflow ends once its last append and its close have their results.
    let last_append = if let EarlierAttempt::UpdateAccepted = earlier {
        2
    } else {
        1
    };
    let left = Cell::new(2usize);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        let results = (activation.jobs.iter()).filter(|job| match &job.variant {
            Some(Job::ResolveStreamAppend(append)) => append.seq == last_append,
            Some(Job::ResolveStreamClose(_)) => true,
            _ => false,
        });
        left.set(left.get() - results.count());
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![append(1, "s"), close(1, "c")],
            Some(Job::DoUpdate(_)) if last_append == 2 => {
                let accepted = update_response::Response::Accepted(());
                vec![update_response(activation, accepted), append(2, "s")]
            }
            Some(Job::DoUpdate(_)) => {
                let rejected = update_response::Response::Rejected(Default::default());
                vec![update_response(activation, rejected)]
            }
            _ if left.get() == 0 => vec![complete()],
            _ => vec![],
        }
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
    let id = producer_key(&producer(&run_id, Default::default())).unwrap();
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
    // A request id is unique to one append call, and the append of a later attempt is another call.
    let mut attempts = HashMap::new();
    for request in store.requests() {
        if let Request::Append(append, request_id) = request {
            let (_, task, attempt) = writer(append.producer);
            let first = *attempts.entry(request_id).or_insert((task, attempt));
            assert_eq!(first, (task, attempt), "{earlier:?}");
        }
    }
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
}

/// Waits until `done` returns true, for at most 30 seconds, so that a test fails rather than hangs.
async fn wait_until(mut done: impl FnMut() -> bool) {
    let wait = async {
        while !done() {
            tokio::task::yield_now().await;
        }
    };
    (tokio::time::timeout(Duration::from_secs(30), wait).await).expect("The wait timed out");
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
#[tokio::test]
async fn a_cached_run_that_meets_another_workers_task_replays_from_history(
    #[case] other_task: OtherTask,
) {
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let t = history(Duration::from_secs(60));
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
        stream_id: "s".to_string(),
        store: STORE.to_string(),
        activation_index: 0,
        result: Some(stream_append_result::Result::Appended(offsets)),
    };
    let marker = StreamMarkerData {
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
    // An append that waits at the hold must not count as a try with no answer.
    opts.configure = |wc| wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
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
    let t = history(Duration::from_secs(60));
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

// Task 2 timed out while its append ran. Its retry comes after task 1, whose started event id is
// lower than the one of task 2.
#[tokio::test]
async fn a_cached_run_that_meets_the_retry_of_its_timed_out_task_replays_from_history() {
    let store = store(&["s"]).await;
    store.hold(Op::Append);
    let mut t = history(Duration::from_secs(60));
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
    // An append that waits at the hold must not count as a try with no answer.
    opts.configure = |wc| wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
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
    respond(&run.worker, &fired, workflow).await;
    wait_until(|| store.requests().len() == 1).await;
    tasks.send(retry).unwrap();
    drop(tasks);
    run.all_taken.take().unwrap().await.unwrap();
    // The run stops the append of the timed-out task, so the Store writes nothing for it.
    assert_eq!(run.drive(workflow).await.len(), 0);
    store.release();
    let jobs = run.drive(workflow).await;
    assert!(
        matches!(jobs.last().unwrap()[..], [Job::ResolveStreamAppend(_)]),
        "{jobs:?}"
    );
    let id = producer_key(&producer(&run_id, Default::default())).unwrap();
    assert_eq!(writers(&store, "s"), [(id, 11, 2)]);
    assert_eq!(store.requests().len(), 2);
    run.worker.drain_pollers_and_shutdown_ref().await;
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
    let mut t = history(Duration::from_secs(60));
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
    let id = producer_key(&producer(&run_id, Default::default())).unwrap();
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

/// The result of a Local Activity or an append in task 2.
#[derive(Clone, Copy, Debug)]
struct Got {
    /// The activation in which the first run got it.
    activation: u64,
    /// The seq of the call of the other kind whose result started this call, or `None` if the
    /// Signal did.
    after: Option<u32>,
}

/// The results of task 2, by the seq of their calls. Workflow code asks for the closes in task 1.
/// It schedules a Local Activity or sends an append at the Signal that starts task 2, which has no
/// other job, or at the result of a call of the other kind.
#[derive(Debug)]
struct Held {
    locals: Vec<Got>,
    appends: Vec<Got>,
    /// The activation in which the first run got the result of each close.
    closes: Vec<u64>,
}

impl Held {
    fn generate(rng: &mut StdRng) -> Self {
        let mut at = |count: usize, first: u64| -> Vec<u64> {
            let count = rng.random_range(0..=count);
            (0..count).map(|_| rng.random_range(first..=6)).collect()
        };
        // A result of a call that Workflow code made at the Signal comes after it.
        let (locals, appends, closes) = (at(3, 1), at(3, 1), at(2, 0));
        let signal = |activations: &Vec<u64>| -> Vec<Got> {
            let got = |activation| Got {
                activation,
                after: None,
            };
            activations.iter().copied().map(got).collect()
        };
        let mut held = Self {
            locals: signal(&locals),
            appends: signal(&appends),
            closes,
        };
        // A call that a result starts gets its own result in a later activation. The calls of a
        // kind take seqs in the order that Workflow code starts them.
        let mut after = |seq, activation| {
            let activation = rng.random_range(activation + 1..=activation + 3);
            rng.random_bool(0.4).then_some(Got {
                activation,
                after: Some(seq),
            })
        };
        let mut started_by = |results: Vec<u64>| -> Vec<Got> {
            let mut started: Vec<(u64, Got)> = (1..)
                .zip(results)
                .filter_map(|(seq, activation)| Some((activation, after(seq, activation)?)))
                .collect();
            started.sort_by_key(|(at, _)| *at);
            started.into_iter().map(|(_, got)| got).collect()
        };
        held.locals.extend(started_by(appends));
        held.appends.extend(started_by(locals));
        held
    }

    fn count(&self) -> usize {
        self.locals.len() + self.appends.len() + self.closes.len()
    }

    /// Returns the History of the first run.
    fn history(&self) -> TestHistoryBuilder {
        let activations = |got: &Vec<Got>| got.iter().map(|got| got.activation).collect();
        let (locals, appends): (Vec<u64>, Vec<u64>) =
            (activations(&self.locals), activations(&self.appends));
        let seqs = |activations: &Vec<u64>| (1..).zip(activations.clone());
        let mut t = history(Duration::from_secs(60));
        let intents = StreamMarkerData {
            close_intents: seqs(&self.closes)
                .map(|(seq, _)| StreamCloseIntent {
                    seq,
                    stream_id: format!("c{seq}"),
                    store: STORE.to_string(),
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
        let mut locals: Vec<(u32, u64)> = seqs(&locals).collect();
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
        let data = StreamMarkerData {
            appends: seqs(&appends)
                .map(|(seq, activation)| StreamAppendResult {
                    seq,
                    stream_id: format!("a{seq}"),
                    store: STORE.to_string(),
                    activation_index: activation,
                    result: Some(stream_append_result::Result::Appended(offsets.clone())),
                })
                .collect(),
            closes: seqs(&self.closes)
                .map(|(seq, activation)| StreamCloseResult {
                    seq,
                    activation_index: activation,
                    failure: None,
                })
                .collect(),
            ..Default::default()
        };
        if !data.appends.is_empty() || !data.closes.is_empty() {
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

    /// Returns the names of the jobs of each activation of the first run, in their order. The
    /// results of Local Activities come first, and then those of appends and of closes, by seq.
    fn jobs(&self) -> Vec<Vec<String>> {
        let named = |kind: &'static str, activations: Vec<u64>| {
            let named = (1..).zip(activations);
            named.map(move |(seq, activation)| (activation, format!("{kind} {seq}")))
        };
        let activations = |got: &Vec<Got>| got.iter().map(|got| got.activation).collect();
        let mut task: BTreeMap<u64, Vec<String>> = BTreeMap::new();
        task.entry(0).or_default().push("signal".to_string());
        let results = (named("local activity", activations(&self.locals)))
            .chain(named("append", activations(&self.appends)))
            .chain(named("close", self.closes.clone()));
        for (activation, name) in results {
            task.entry(activation).or_default().push(name);
        }
        let task = task.into_values();
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
        other => format!("{other:?}"),
    }
}

// With `DisableWorkflowTaskChunking` in History, a replay gives each result in the activation of
// the first run, and the jobs of an activation in the order of the first run.
#[tokio::test]
async fn a_replay_gives_each_held_result_in_its_activation() {
    let store = store(&[]).await;
    let signal = |activation| Got {
        activation,
        after: None,
    };
    let mut chains = (0, 0);
    for seed in 0..200 {
        let _seed = SeedOnPanic(seed);
        let held = match seed {
            // An append result at activation 3, and a Local Activity result at 5.
            0 => Held {
                locals: vec![signal(5)],
                appends: vec![signal(3)],
                closes: vec![],
            },
            _ => Held::generate(&mut StdRng::seed_from_u64(seed)),
        };
        if held.count() == 0 {
            continue;
        }
        let mut opts = StartOpts::new(&store);
        opts.replay = true;
        let replay = start(held.history(), opts);
        // The seqs of the calls that the Signal or the result of call `after` starts.
        let started = |calls: &Vec<Got>, after: Option<u32>| -> Vec<u32> {
            let seqs = (1..).zip(calls);
            seqs.filter_map(|(seq, got)| (got.after == after).then_some(seq))
                .collect()
        };
        let append = |seq| append(seq, &format!("a{seq}"));
        let mut left = held.count();
        let jobs = replay
            .drive(|activation| {
                let (mut locals, mut appends) = (vec![], vec![]);
                let mut commands = vec![];
                for job in &activation.jobs {
                    match job.variant.as_ref() {
                        Some(Job::InitializeWorkflow(_)) => {
                            let seqs = 1..=held.closes.len() as u32;
                            commands.extend(seqs.map(|seq| close(seq, &format!("c{seq}"))));
                        }
                        Some(Job::SignalWorkflow(_)) => {
                            locals.extend(started(&held.locals, None));
                            appends.extend(started(&held.appends, None));
                        }
                        Some(Job::ResolveStreamAppend(result)) => {
                            locals.extend(started(&held.locals, Some(result.seq)));
                        }
                        Some(Job::ResolveActivity(result)) => {
                            appends.extend(started(&held.appends, Some(result.seq)));
                        }
                        _ => {}
                    }
                }
                let results = (activation.jobs.iter()).filter(|job| {
                    let variant = job.variant.as_ref();
                    !matches!(
                        variant,
                        Some(Job::InitializeWorkflow(_) | Job::SignalWorkflow(_))
                    )
                });
                left = (left.checked_sub(results.count()))
                    .unwrap_or_else(|| panic!("seed {seed}: more results than calls"));
                locals.sort_unstable();
                appends.sort_unstable();
                commands.extend(locals.into_iter().map(local_activity));
                commands.extend(appends.into_iter().map(append));
                if left == 0 {
                    commands.push(complete());
                }
                commands
            })
            .await;
        replay.worker.drain_pollers_and_shutdown_ref().await;
        let names: Vec<Vec<String>> = (jobs.iter())
            .map(|jobs| jobs.iter().map(job_name).collect())
            .collect();
        assert_eq!(names, held.jobs(), "seed {seed}: {held:?}");
        let chained = |calls: &Vec<Got>| usize::from(calls.iter().any(|got| got.after.is_some()));
        chains.0 += chained(&held.locals);
        chains.1 += chained(&held.appends);
    }
    // Some seeds have an append result that starts a Local Activity, and some the other way.
    assert!(chains.0 > 0 && chains.1 > 0, "{chains:?}");
}

// With chunking on, a replay gives the results of a task with Stream calls and Local Activities
// in the order of the first run. With no job to give, it gives the results of History up to the
// first one whose call Workflow code hasn't made. Only with `DisableWorkflowTaskChunking` in
// History does a result keep the activation of the first run.
//
// Task 1 sends two appends and Local Activity 1, and completes with no command while they run.
// Their results come in task 2, where Workflow code schedules Local Activity 2 at the result of
// the first one. With chunking on, a replay takes two such tasks as one.
#[tokio::test]
async fn a_replay_gives_a_stream_result_as_it_gives_the_result_of_a_local_activity() {
    let store = store(&[]).await;
    let (mut together, mut batched) = (0, 0);
    let results = [
        "local activity 1",
        "local activity 2",
        "append 1",
        "append 2",
    ];
    for seed in 0..100 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        // A timer of task 1 is a command, so a replay with chunking on keeps the two tasks apart.
        // An Update that the first run rejected in the first activation of task 2 left no event,
        // so the first result has the index 1. The activations of the results are in the order of
        // `results`, and Local Activity 2 gives its result after the first one.
        let (chunking, timer_in_task_1, first, picked) = match seed {
            // Local Activity 2 and append 1 give their results in one activation, after the one of
            // Local Activity 1. Seed 0 has chunking on, and seed 1 has it off.
            0 | 1 => (seed == 0, true, 1, vec![0, 1, 1, 0]),
            _ => {
                let local_1 = rng.random_range(0..3);
                let local_2 = rng.random_range(local_1 + 1..5);
                let appends = [rng.random_range(0..5), rng.random_range(0..5)];
                let picked = vec![local_1, local_2, appends[0], appends[1]];
                let chunking = rng.random_bool(0.5);
                (
                    chunking,
                    rng.random_bool(0.5),
                    rng.random_range(0..=1),
                    picked,
                )
            }
        };
        // Each activation of task 2 after its first one comes for a result, so the activations
        // of the results are consecutive.
        let mut ranks = picked.clone();
        ranks.sort_unstable();
        ranks.dedup();
        let recorded: Vec<u64> = (picked.iter())
            .map(|at| first + ranks.iter().position(|rank| rank == at).unwrap() as u64)
            .collect();

        let mut t = history(Duration::from_secs(60));
        t.add_workflow_task_completed();
        if !chunking {
            t.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
        }
        if timer_in_task_1 {
            t.add_timer_started("1".to_string());
        }
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_completed();
        let completed = t.current_event_id();
        for seq in 1..=2 {
            let result = Some(Payload::default());
            t.add_local_activity_marker(seq, &seq.to_string(), result, None, |marker| {
                marker.activity_type = "la".to_string();
                marker.activation_index = Some(recorded[seq as usize - 1]);
            });
        }
        let offsets = StreamAppendOffsets {
            first_offset: vec![1],
            last_offset: vec![1],
        };
        let data = StreamMarkerData {
            appends: (1..=2)
                .map(|seq| StreamAppendResult {
                    seq,
                    stream_id: format!("a{seq}"),
                    store: STORE.to_string(),
                    activation_index: recorded[seq as usize + 1],
                    result: Some(stream_append_result::Result::Appended(offsets.clone())),
                })
                .collect(),
            ..Default::default()
        };
        t.add(MarkerRecordedEventAttributes {
            marker_name: STREAM_MARKER_NAME.to_string(),
            details: marker_details(&data),
            workflow_task_completed_event_id: completed,
            ..Default::default()
        });
        t.add_workflow_execution_completed();

        let mut opts = StartOpts::new(&store);
        opts.replay = true;
        let replay = start(t, opts);
        let mut left = results.len();
        let jobs = replay
            .drive(|activation| {
                if let Some(Job::InitializeWorkflow(_)) = &activation.jobs[0].variant {
                    let calls = vec![append(1, "a1"), append(2, "a2"), local_activity(1)];
                    let timer = timer_in_task_1.then(|| timer(1));
                    return timer.into_iter().chain(calls).collect();
                }
                left -= activation.jobs.len();
                let first_result = |job: &Job| matches!(job, Job::ResolveActivity(r) if r.seq == 1);
                let jobs = activation
                    .jobs
                    .iter()
                    .filter_map(|job| job.variant.as_ref());
                match left {
                    0 => vec![complete()],
                    _ if jobs.clone().any(first_result) => vec![local_activity(2)],
                    _ => vec![],
                }
            })
            .await;
        replay.worker.drain_pollers_and_shutdown_ref().await;
        let names: Vec<Vec<String>> = (jobs[1..].iter())
            .map(|jobs| jobs.iter().map(job_name).collect())
            .collect();
        let case = format!("seed {seed}: chunking {chunking}, {recorded:?}, {names:?}");
        // Each result comes once.
        assert_eq!(names.concat().len(), results.len(), "{case}");
        let came_in = |result: &str| {
            let came = names
                .iter()
                .position(|names| names.iter().any(|name| name == result));
            came.unwrap_or_else(|| panic!("{case}"))
        };
        // Workflow code sent the appends with Local Activity 1. So the result of an append comes
        // with the one of that Local Activity, if the first run got both in one activation.
        for append in [2, 3] {
            if recorded[0] == recorded[append] {
                assert_eq!(came_in(results[0]), came_in(results[append]), "{case}");
                together += 1;
            }
        }
        // The jobs of an activation are the results of Local Activities, and then the Stream
        // results in the order of the first run.
        let place = |name: &String| {
            let result = results.iter().position(|result| result == name).unwrap();
            (
                result >= 2,
                if result >= 2 { recorded[result] } else { 0 },
                result,
            )
        };
        for jobs in &names {
            let mut ordered = jobs.clone();
            ordered.sort_by_key(place);
            assert_eq!(*jobs, ordered, "{case}");
        }
        // With chunking off, and in the two fixed seeds, the activations of the replay are those
        // of the first run.
        let of_first_run: Vec<Vec<String>> = (first..first + ranks.len() as u64)
            .map(|at| {
                let came = (0..results.len()).filter(|result| recorded[*result] == at);
                came.map(|result| results[result].to_string()).collect()
            })
            .collect();
        if chunking && seed > 1 {
            batched += usize::from(names.len() < of_first_run.len());
        } else {
            assert_eq!(names, of_first_run, "{case}");
        }
    }
    // Some seeds have a Stream result and a Local Activity result in one activation, and in some
    // a replay with chunking on gives the results of two activations in one.
    assert!(together > 0 && batched > 0, "{together} {batched}");
}

/// The kind of a call of a first run that [`replay_calls`] replays. In one activation the first
/// run gave the results of Local Activities before those of appends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    LocalActivity,
    Append,
}

/// A call of a first run whose task 2 gave its result. Its seq is its place in the list of calls.
#[derive(Clone, Copy, Debug)]
struct Call {
    kind: Kind,
    /// The activation of task 2 that gave the result.
    activation: u64,
    /// The seq of the call at whose result Workflow code made this one, or `None` for task 1.
    after: Option<u32>,
}

/// Returns the seqs of the results of each activation of the first run of `calls`, in the order
/// of its jobs.
fn first_run(calls: &[Call]) -> Vec<Vec<u32>> {
    let mut results: Vec<(u64, Kind, u32)> = (1..)
        .zip(calls)
        .map(|(seq, call)| (call.activation, call.kind, seq))
        .collect();
    results.sort_unstable();
    let activations = results.chunk_by(|a, b| a.0 == b.0);
    activations
        .map(|results| results.iter().map(|result| result.2).collect())
        .collect()
}

/// Replays the History of the first run of `calls`, and returns the seqs of the results of each
/// activation, in the order of its jobs.
async fn replay_calls(
    store: &Arc<MemoryStore>,
    calls: &[Call],
    (chunking, timer_in_task_1): (bool, bool),
) -> Vec<Vec<u32>> {
    let mut t = history(Duration::from_secs(60));
    t.add_workflow_task_completed();
    if !chunking {
        t.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
    }
    if timer_in_task_1 {
        t.add_timer_started("1".to_string());
    }
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_completed();
    let completed = t.current_event_id();
    let offsets = StreamAppendOffsets {
        first_offset: vec![1],
        last_offset: vec![1],
    };
    let mut data = StreamMarkerData::default();
    // History has the results in the order that the first run gave them.
    for seq in first_run(calls).concat() {
        let activation_index = calls[seq as usize - 1].activation;
        match calls[seq as usize - 1].kind {
            Kind::LocalActivity => {
                let result = Some(Payload::default());
                t.add_local_activity_marker(seq, &seq.to_string(), result, None, |marker| {
                    marker.activity_type = "la".to_string();
                    marker.activation_index = Some(activation_index);
                });
            }
            Kind::Append => data.appends.push(StreamAppendResult {
                seq,
                stream_id: format!("a{seq}"),
                store: STORE.to_string(),
                activation_index,
                result: Some(stream_append_result::Result::Appended(offsets.clone())),
            }),
        }
    }
    if !data.appends.is_empty() {
        t.add(MarkerRecordedEventAttributes {
            marker_name: STREAM_MARKER_NAME.to_string(),
            details: marker_details(&data),
            workflow_task_completed_event_id: completed,
            ..Default::default()
        });
    }
    t.add_workflow_execution_completed();
    let mut opts = StartOpts::new(store);
    opts.replay = true;
    let replay = start(t, opts);
    // The calls that Workflow code makes at the result of call `after`.
    let made_at = |after: Option<u32>| {
        let made = (1..)
            .zip(calls)
            .filter(move |(_, call)| call.after == after);
        made.map(|(seq, call)| match call.kind {
            Kind::LocalActivity => local_activity(seq),
            Kind::Append => append(seq, &format!("a{seq}")),
        })
    };
    let (mut activations, mut left) = (vec![], calls.len());
    let workflow = |activation: &WorkflowActivation| {
        if let Some(Job::InitializeWorkflow(_)) = &activation.jobs[0].variant {
            let timer = timer_in_task_1.then(|| timer(1));
            return timer.into_iter().chain(made_at(None)).collect();
        }
        let seqs: Vec<u32> = (activation.jobs.iter())
            .map(|job| match job.variant.as_ref() {
                Some(Job::ResolveActivity(result)) => result.seq,
                Some(Job::ResolveStreamAppend(result)) => result.seq,
                other => panic!("{other:?}"),
            })
            .collect();
        let mut commands: Vec<Command> =
            (seqs.iter()).flat_map(|seq| made_at(Some(*seq))).collect();
        left -= seqs.len();
        if left == 0 {
            commands.push(complete());
        }
        activations.push(seqs);
        commands
    };
    // A replay with a nondeterminism error ends with another reason.
    let (_, evicted) = drive_until_evicted(&replay.worker, workflow).await;
    assert_eq!(evicted, Some(EvictionReason::LangRequested), "{calls:?}");
    replay.worker.drain_pollers_and_shutdown_ref().await;
    activations
}

// A replay gives the result of an append where it gives the result of a Local Activity at the
// same place. A task with both gives its results in the order of the first run.
#[tokio::test]
async fn a_replay_gives_a_stream_result_where_it_gives_the_result_of_a_local_activity() {
    let store = store(&[]).await;
    let (local, append) = (Kind::LocalActivity, Kind::Append);
    let (mut batched, mut apart) = (0, 0);
    for seed in 0..200 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        // A timer of task 1 keeps the two tasks apart in a replay with chunking on. A rejected
        // Update at the start of task 2 left no event, so the first result has the index 1.
        let (timer_in_task_1, first, picked) = match seed {
            // Workflow code makes call 3 at the result of call 1, and the first run got the
            // result of call 3 before the one of call 2.
            0 => (
                true,
                1,
                vec![(local, 0, None), (local, 2, None), (append, 1, Some(1))],
            ),
            // The first run got the result of the append one activation before the other one.
            1 => (false, 0, vec![(append, 0, None), (local, 1, None)]),
            2 => (true, 1, vec![(append, 0, None), (local, 1, None)]),
            _ => {
                let kind = |rng: &mut StdRng| [local, append][rng.random_range(0..2)];
                let mut picked: Vec<(Kind, u64, Option<u32>)> = (0..rng.random_range(1..=3))
                    .map(|_| (kind(&mut rng), rng.random_range(0..4), None))
                    .collect();
                // The calls take seqs in the order that Workflow code makes them.
                let mut made: Vec<(Kind, u64, u32)> = (0..rng.random_range(0..=2))
                    .map(|_| {
                        let after = rng.random_range(0..picked.len());
                        let at = picked[after].1 + rng.random_range(1..=2);
                        (kind(&mut rng), at, after as u32 + 1)
                    })
                    .collect();
                made.sort_by_key(|(_, _, after)| (picked[*after as usize - 1].1, *after));
                picked.extend(
                    made.into_iter()
                        .map(|(kind, at, after)| (kind, at, Some(after))),
                );
                (rng.random_bool(0.5), rng.random_range(0..=1), picked)
            }
        };
        // Each activation of task 2 after its first one comes for a result, so the activations
        // of the results are consecutive.
        let mut ranks: Vec<u64> = picked.iter().map(|call| call.1).collect();
        ranks.sort_unstable();
        ranks.dedup();
        let rank = |at: u64| first + ranks.iter().position(|rank| *rank == at).unwrap() as u64;
        let calls: Vec<Call> = (picked.iter())
            .map(|&(kind, at, after)| Call {
                kind,
                activation: rank(at),
                after,
            })
            .collect();
        // Core gives a result as soon as it comes. So the result of a call that comes in the
        // activation after the one that made the call is alone in it.
        let alone = |call: &Call| {
            let with = calls
                .iter()
                .filter(|other| other.activation == call.activation);
            with.count() == 1
        };
        let made_one_before = |call: &Call| {
            let after = call.after.map(|after| calls[after as usize - 1].activation);
            after.is_some_and(|after| after + 1 == call.activation)
        };
        if calls
            .iter()
            .any(|call| made_one_before(call) && !alone(call))
        {
            continue;
        }
        let all = |kind: Kind| -> Vec<Call> {
            let of_kind = |call: &Call| Call { kind, ..*call };
            calls.iter().map(of_kind).collect()
        };
        for chunking in [true, false] {
            let shape = (chunking, timer_in_task_1);
            let case = format!("seed {seed}: {shape:?}, {calls:?}");
            let mut replays = vec![];
            for calls in [all(local), all(append), calls.clone()] {
                let replayed = replay_calls(&store, &calls, shape).await;
                // The results come in the order of the first run, and with chunking off in its
                // activations.
                let of_first_run = first_run(&calls);
                assert_eq!(replayed.concat(), of_first_run.concat(), "{case}");
                if !chunking {
                    assert_eq!(replayed, of_first_run, "{case}");
                }
                replays.push(replayed);
            }
            assert_eq!(replays[1], replays[0], "{case}");
            if chunking {
                // With Local Activities alone, the result of call 2 comes with the one of call 3,
                // and after it. The fixed shapes with both kinds replay as their first run.
                if seed == 0 {
                    assert_eq!(replays[0], [vec![1], vec![3, 2]], "{case}");
                }
                if seed <= 2 {
                    assert_eq!(replays[2], first_run(&calls), "{case}");
                }
                batched += usize::from(replays[0] != first_run(&all(local)));
                apart += usize::from(replays[2].len() > replays[0].len());
            }
        }
    }
    // In some seeds a replay with chunking on gives the results of two activations in one, and
    // in some a task with both kinds keeps apart what Local Activities alone give in one.
    assert!(batched > 0 && apart > 0, "{batched} {apart}");
}
