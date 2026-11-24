//! The Stream calls of Workflow code, through a Worker on a mock server. The mock server writes the
//! History of each completion, so a replay Worker can replay it.

mod reads;

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
    worker::client::{
        LegacyQueryResult, MockWorkerClient, WorkflowTaskCompletion, mocks::mock_worker_client,
    },
};
use futures_util::{StreamExt, stream};
use parking_lot::Mutex;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    iter,
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        activity_task::activity_task,
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, ResolveStreamRead, StreamAppended,
            WorkflowActivation, remove_from_cache::EvictionReason, resolve_stream_append,
            resolve_stream_read, workflow_activation_job::Variant as Job,
        },
        workflow_commands::{
            AppendToStream, CancelWorkflowExecution, CloseStream, CompleteWorkflowExecution,
            ContinueAsNewWorkflowExecution, FailWorkflowExecution, QueryResult, QuerySuccess,
            ReadStream, ScheduleLocalActivity, StartTimer, UpdateResponse, query_result,
            update_response, workflow_command::Variant as Command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::Payload,
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::{
            History, HistoryEvent, MarkerRecordedEventAttributes,
            UpsertWorkflowSearchAttributesEventAttributes, history_event,
        },
        query::v1::WorkflowQuery,
        sdk::v1::{
            WorkflowTaskStreamCalls,
            workflow_task_stream_calls::{
                AppendOffsets, AppendResult, CloseIntent, CloseResult, append_result,
            },
        },
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, PollWorkflowTaskQueueResponse,
            RespondWorkflowTaskCompletedResponse,
        },
    },
};
use temporalio_common::{protos::TaskToken, worker::WorkerTaskTypes};
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
    Notify,
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

fn history(task_timeout: Duration) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(task_timeout);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// The task that the mock server gives after completion `number`, if any.
type NextTask =
    Box<dyn FnMut(usize, &mut TestHistoryBuilder) -> Option<PollWorkflowTaskQueueResponse> + Send>;

/// The task that the mock server gives in its answer to a completion, if any.
type Eager = Box<
    dyn FnMut(
            &WorkflowTaskCompletion,
            &mut TestHistoryBuilder,
        ) -> Option<PollWorkflowTaskQueueResponse>
        + Send,
>;

struct StartOpts {
    /// The tasks of the History that the poller gives.
    responses: Vec<ResponseType>,
    /// When set, the poller gives the tasks that the test sends here instead of `responses`, as
    /// soon as they arrive. Core can then get a task while an activation is outstanding.
    tasks: Option<UnboundedReceiver<PollWorkflowTaskQueueResponse>>,
    /// While the mock server answers completion `number`, it sends the task that this returns to
    /// `tasks` and closes it. It answers once Core took the task.
    during_report: Option<(UnboundedSender<PollWorkflowTaskQueueResponse>, NextTask)>,
    store: Arc<MemoryStore>,
    cache: usize,
    /// The mock server answers a forced completion with the next task, inline.
    served: Option<NextTask>,
    /// When set, the mock server answers each completion with the task that this returns, inline,
    /// in place of `served`.
    eager: Option<Eager>,
    /// Runs a replay Worker on the History.
    replay: bool,
    /// The completion that the server refuses for an event that the Worker didn't see.
    refuse: Option<usize>,
    /// The failed Workflow Tasks that the server gets.
    fails: usize,
    /// The query tasks that the server gets an answer for.
    queries: usize,
    /// The pages of History that the client fetches, in order.
    pages: Vec<GetWorkflowExecutionHistoryResponse>,
    /// The fetches of a page of History that fail, after `pages`.
    failed_fetches: usize,
    configure: Box<dyn FnOnce(&mut WorkerConfig)>,
    /// The generated input that the run comes from, for the message of a failed task.
    label: String,
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
            eager: None,
            replay: false,
            refuse: None,
            fails: 0,
            queries: 0,
            pages: vec![],
            failed_fetches: 0,
            configure: Box::new(|_| {}),
            label: String::new(),
        }
    }
}

/// The task token of a query task that got an answer, with the message of a failed answer.
type Answer = (Vec<u8>, Option<String>);

struct Run {
    worker: Worker,
    store: Arc<MemoryStore>,
    sent: Arc<Mutex<Vec<WorkflowTaskCompletion>>>,
    /// The cause and the message of each failed Workflow Task.
    failed: Arc<Mutex<Vec<(WorkflowTaskFailedCause, String)>>>,
    answers: Arc<Mutex<Vec<Answer>>>,
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
        mut eager,
        replay,
        refuse,
        fails,
        queries,
        pages,
        failed_fetches,
        configure,
        label,
    } = opts;
    let sent = Arc::new(Mutex::new(vec![]));
    let failed = Arc::new(Mutex::new(vec![]));
    let answers = Arc::new(Mutex::new(vec![]));
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
            answers,
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
        let workflow_task = match eager.as_mut() {
            Some(eager) => eager(completion, &mut history),
            None => served
                .as_mut()
                .filter(|_| forced && completion.return_new_workflow_task)
                .and_then(|next_task| next_task(number, &mut history)),
        };
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
        assert!(failed.len() <= fails, "A task failed: {failed:?} {label}");
    };
    let kept = answers.clone();
    let answer = move |token: &TaskToken, result: &LegacyQueryResult| {
        let failure = match result {
            LegacyQueryResult::Succeeded(_) => None,
            LegacyQueryResult::Failed(failed) => {
                Some((failed.failure.as_ref()).map_or_else(String::new, |f| f.message.clone()))
            }
        };
        kept.lock().push((token.clone().into_inner(), failure));
    };
    let from_channel = tasks.is_some();
    let mut mock = match tasks {
        None => {
            let mut cfg = MockPollCfg::from_resps(t, responses);
            cfg.make_poll_stream_interminable = true;
            cfg.num_expected_completions = Some((0..).into());
            cfg.num_expected_fails = fails;
            cfg.num_expected_legacy_query_resps = queries;
            cfg.expect_legacy_query_matcher = Box::new(move |token, result| {
                answer(token, result);
                true
            });
            expect_pages(&mut cfg.mock_client, pages, failed_fetches);
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
            (client.expect_respond_legacy_query())
                .times(queries)
                .returning(move |token, result| {
                    answer(&token, &result);
                    Ok(Default::default())
                });
            expect_pages(&mut client, pages, failed_fetches);
            MocksHolder::from_wft_stream(client, input)
        }
    };
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = cache;
        wc.stream_runtimes = runtimes(&store);
        // A read with no answer ends at its read limit, so a short one keeps the tests fast.
        wc.stream_runtimes.settings.read_time = Some(Duration::from_millis(250));
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
        answers,
        history,
        all_taken: all_taken.filter(|_| from_channel),
    }
}

/// Returns the one page of a History with `events`.
fn page(events: &[HistoryEvent]) -> GetWorkflowExecutionHistoryResponse {
    GetWorkflowExecutionHistoryResponse {
        history: Some(History {
            events: events.to_vec(),
        }),
        ..Default::default()
    }
}

fn expect_pages(
    client: &mut MockWorkerClient,
    pages: Vec<GetWorkflowExecutionHistoryResponse>,
    failed_fetches: usize,
) {
    for page in pages {
        (client.expect_get_workflow_execution_history())
            .times(1)
            .returning(move |_, _, _| Ok(page.clone()));
    }
    (client.expect_get_workflow_execution_history())
        .times(failed_fetches)
        .returning(|_, _, _| Err(tonic::Status::not_found("The Workflow is gone")));
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

/// Returns the query task that the server gave out directly before it scheduled `task`, when the
/// last event of History was `last_event_id`.
fn query_before(
    task: &PollWorkflowTaskQueueResponse,
    last_event_id: i64,
) -> PollWorkflowTaskQueueResponse {
    let mut query = task.clone();
    query.task_token = rand::random::<[u8; 16]>().to_vec();
    query.started_event_id = 0;
    let history = query.history.as_mut().expect("The task has its History");
    history
        .events
        .retain(|event| event.event_id <= last_event_id);
    let started = (history.events.iter().rev())
        .find(|event| event.event_type() == EventType::WorkflowTaskStarted);
    query.previous_started_event_id = started.map_or(0, |event| event.event_id);
    query.query = Some(WorkflowQuery {
        query_type: "q".to_string(),
        ..Default::default()
    });
    query
}

/// Answers the query of `activation`, if that's its first job.
fn answer(activation: &WorkflowActivation) -> Option<Command> {
    let Some(Job::QueryWorkflow(query)) = &activation.jobs[0].variant else {
        return None;
    };
    let answer = QueryResult {
        query_id: query.query_id.clone(),
        variant: Some(query_result::Variant::Succeeded(QuerySuccess::default())),
    };
    Some(answer.into())
}

/// Completes the last task of `t` with the ledger `data`.
fn complete_with_ledger(t: &mut TestHistoryBuilder, data: &WorkflowTaskStreamCalls) {
    t.add_workflow_task_completed();
    t.set_stream_calls_last_wft(data.clone());
}

/// Returns the ledger of a task that asked for close 1 of the Stream `stream_id`.
fn close_intent(stream_id: &str) -> WorkflowTaskStreamCalls {
    WorkflowTaskStreamCalls {
        close_intents: vec![CloseIntent {
            seq: 1,
            r#ref: Some(stream(stream_id)),
        }],
        ..Default::default()
    }
}

/// Adds the events of a completion that the server took to `history`.
fn write(history: &mut TestHistoryBuilder, completion: &WorkflowTaskCompletion) {
    history.add_workflow_task_completed();
    // A replay decides some commands by the flags that the completion used, such as a patch's
    // search attribute. The flag that turns workflow task chunking off goes, so a History has
    // chunking on, also from a build with the Cargo feature, unless a test turns it off.
    let unchunked = CoreInternalFlags::DisableWorkflowTaskChunking as u32;
    let mut metadata = completion.sdk_metadata.clone();
    metadata.core_used_flags.retain(|flag| *flag != unchunked);
    let event = history.last_event().map(|event| event.attributes.as_mut());
    if let Some(Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(attrs))) =
        event
    {
        attrs.sdk_metadata = Some(metadata);
    }
    let completed = history.current_event_id();
    let mut accepted = HashMap::new();
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
            Some(Attributes::UpsertWorkflowSearchAttributesCommandAttributes(upsert)) => {
                history.add(UpsertWorkflowSearchAttributesEventAttributes {
                    workflow_task_completed_event_id: completed,
                    search_attributes: upsert.search_attributes.clone(),
                });
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
            // A rejected Update leaves no event.
            Some(Attributes::ProtocolMessageCommandAttributes(command)) => {
                let message = (completion.messages.iter())
                    .find(|message| message.id == command.message_id)
                    .expect("A protocol command has its message");
                let id = &message.protocol_instance_id;
                match message.body.as_ref().map(|body| body.type_url.as_str()) {
                    Some("type.googleapis.com/temporal.api.update.v1.Acceptance") => {
                        accepted.insert(id, history.add_update_accepted(id, "update_fn"));
                    }
                    Some("type.googleapis.com/temporal.api.update.v1.Response") => {
                        history.add_update_completed(accepted[id]);
                    }
                    _ => {}
                }
            }
            other => panic!("The mock server can't write {other:?}"),
        }
    }
}

type Workflow = fn(&WorkflowActivation) -> Vec<Command>;

/// Completes each activation of `worker` with the commands of `workflow` until the run is
/// evicted, and returns the jobs of each activation with the reason of the eviction.
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
    /// Drives the run until it's evicted, and returns the jobs of each activation.
    async fn drive(
        &self,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        drive_until_evicted(&self.worker, workflow).await.0
    }

    /// Replays the History that the completions wrote on a replay Worker with the same Store, and
    /// returns the jobs of each activation. The replay must reach the end of that History.
    async fn replay(
        &self,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        self.replay_with(true, workflow).await
    }

    /// Replays as [`Run::replay`] does, with chunking on or off. The mock server drops the flag
    /// that turns chunking off, so a History has chunking on unless this turns it off.
    async fn replay_with(
        &self,
        chunking: bool,
        workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
    ) -> Vec<Vec<Job>> {
        let mut t = self.history.lock().clone();
        if !chunking {
            t.set_flags_first_wft(&[CoreInternalFlags::DisableWorkflowTaskChunking], &[]);
        }
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

fn ledger(completion: &WorkflowTaskCompletion) -> Option<WorkflowTaskStreamCalls> {
    completion.sdk_metadata.stream_calls.as_deref().cloned()
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
async fn an_append_writes_as_its_workflow_task_and_replays_from_its_ledger() {
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
    let bodies: Vec<&[u8]> = (records.iter())
        .map(|record| &record.body.as_ref().unwrap().data[..])
        .collect();
    assert_eq!(bodies, [[1]]);
    let offset = records[0].offset.clone();
    let appended = ResolveStreamAppend {
        seq: 1,
        status: Some(resolve_stream_append::Status::Appended(StreamAppended {
            first_offset: offset.clone(),
            last_offset: offset.clone(),
        })),
    };
    assert_eq!(live[1], [Job::ResolveStreamAppend(appended)]);
    let entry = AppendResult {
        seq: 1,
        r#ref: Some(stream("s")),
        result: Some(append_result::Result::Appended(AppendOffsets {
            first_offset: offset.clone(),
            last_offset: offset,
        })),
        activation_index: 1,
    };
    let expected = WorkflowTaskStreamCalls {
        append_results: vec![entry],
        ..Default::default()
    };
    assert_eq!(ledger(&run.sent()[0]), Some(expected));
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
        let ledger = ledger(&run.sent()[0]).unwrap();
        assert_eq!(ledger.close_intents, close_intent("c").close_intents);
    } else {
        assert_eq!(rest, Vec::<Vec<Job>>::new());
        assert_eq!(closes(&store), 0);
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(store.is_closed("c"), failure.is_none());
}

/// The state of a call when Workflow code sends its last command.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AtTheEnd {
    /// An activation took the result.
    Taken,
    /// The Store answered while the last activation was outstanding, so no activation got the
    /// result.
    Answered,
    /// The Store has the request, and gave no answer.
    Runs,
    /// The close waits behind a close of its Stream that runs.
    Waits,
    /// Workflow code asked for the close in the last task, so History has no intent of it.
    Asked,
    /// The call is in the completion that has the last command.
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
            // The read waits for a Record that never comes.
            Op::Read => ReadStream {
                seq: self.seq,
                stream: Some(stream(&self.stream)),
                wait: Some(Duration::from_secs(60).try_into().unwrap()),
                ..Default::default()
            }
            .into(),
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
    /// The number of calls before the last command in its completion.
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
            let op = [Op::Append, Op::Close, Op::Read][rng.random_range(0..3)];
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
        let mut intents = WorkflowTaskStreamCalls::default();
        for close in closes {
            intents.close_intents.push(CloseIntent {
                seq: close.seq as i32,
                r#ref: Some(stream(&close.stream)),
            });
        }
        if closes.is_empty() {
            t.add_workflow_task_completed();
        } else {
            complete_with_ledger(&mut t, &intents);
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
        // In the first seed whose closes of History all run, their Store is down, and the heartbeat
        // of the task fires while its last activation is outstanding.
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
        opts.configure = Box::new(|wc| {
            wc.workflow_failure_errors = HashSet::from([WorkflowErrorType::Nondeterminism]);
        });
        let task_timeout = Duration::from_secs(if down { 1 } else { 60 });
        let run = start(ending.history(task_timeout), opts);
        let worker = &run.worker;
        let mut live = vec![];
        let last = 1 + ending.taken.len();
        let mut activation = worker.poll_workflow_activation().await.unwrap();
        // Core got the task before it gave this activation, so the heartbeat of the task fires
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
                // while the activation is outstanding.
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
        // Lang gets no job after the last command.
        let after = run.drive(|_| vec![]).await;
        assert_eq!(after, Vec::<Vec<Job>>::new(), "seed {seed}: {ending:?}");
        worker.drain_pollers_and_shutdown_ref().await;

        // The completion has the results that Workflow code took, and then the last command. It
        // has no intent of a close, and forces no task.
        let mut expected = WorkflowTaskStreamCalls::default();
        for (activation, at) in (1..).zip(&ending.taken) {
            let call = &ending.calls[*at];
            if call.op == Op::Close {
                expected.close_results.push(CloseResult {
                    seq: call.seq as i32,
                    failure: None,
                    activation_index: activation,
                });
                continue;
            }
            let offset = store.records(&call.stream)[0].offset.clone();
            let offsets = AppendOffsets {
                first_offset: offset.clone(),
                last_offset: offset,
            };
            expected.append_results.push(AppendResult {
                seq: call.seq as i32,
                r#ref: Some(stream(&call.stream)),
                result: Some(append_result::Result::Appended(offsets)),
                activation_index: activation,
            });
        }
        let sent = run.sent();
        assert_eq!(sent.len(), 1, "seed {seed}: {ending:?}");
        let recorded = (!ending.taken.is_empty()).then_some(expected);
        assert_eq!(ledger(&sent[0]), recorded, "seed {seed}: {ending:?}");
        let last_command = match ending.end {
            End::Complete => CommandType::CompleteWorkflowExecution,
            End::Fail | End::CoreFails => CommandType::FailWorkflowExecution,
            End::Cancel => CommandType::CancelWorkflowExecution,
            End::ContinueAsNew => CommandType::ContinueAsNewWorkflowExecution,
        };
        let types = command_types(&sent[0]);
        assert_eq!(types, [last_command], "seed {seed}: {ending:?}");
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
        // A read writes nothing.
        for call in ending.calls.iter().filter(|call| call.op != Op::Read) {
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
    expected.insert((Op::Read, WithTheCommand as u8));
    assert_eq!(seen, expected);
}

/// Returns the kind and the Stream of each call that reached the Store, once for each call.
fn calls_at(store: &MemoryStore) -> Vec<(Op, String)> {
    let mut calls: Vec<(Op, String)> = (store.requests().into_iter())
        .map(|request| match request {
            Request::Append(append, _) => (Op::Append, append.r#ref.unwrap().id),
            Request::Close(close) => (Op::Close, close.r#ref.unwrap().id),
            Request::Read(read) => (Op::Read, read.r#ref.unwrap().id),
        })
        .collect();
    calls.sort();
    calls.dedup();
    calls
}

fn written(store: &MemoryStore, call: &EndingCall) -> bool {
    match call.op {
        Op::Append => !store.records(&call.stream).is_empty(),
        Op::Close => store.is_closed(&call.stream),
        Op::Read => unreachable!("A read writes nothing"),
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
    CloseFails,
    AppendFailsWithNoLocalActivityManager,
    /// The task fails on two attempts. Core reports the failure of a task only on its first
    /// attempt, so the server learns of the second one by the task timeout.
    AppendFailsOnALaterAttempt,
}

#[rstest::rstest]
#[case(Outage::AppendFails)]
#[case(Outage::AppendGetsNoAnswer)]
#[case(Outage::CloseFails)]
#[case(Outage::AppendFailsWithNoLocalActivityManager)]
#[case(Outage::AppendFailsOnALaterAttempt)]
#[tokio::test]
async fn a_call_in_an_outage_fails_its_task_at_the_heartbeat(#[case] outage: Outage) {
    let store = store(&["s"]).await;
    let down = StreamError::store_unavailable("The Store is down");
    match outage {
        Outage::AppendGetsNoAnswer => store.hold(Op::Append),
        Outage::CloseFails => store.fail(Op::Close, down, usize::MAX),
        _ => store.fail(Op::Append, down, usize::MAX),
    }
    let mut opts = StartOpts::new(&store);
    opts.fails = 1;
    let configure: fn(&mut WorkerConfig) = match outage {
        Outage::AppendFailsWithNoLocalActivityManager => |wc| {
            fast_tries(wc);
            wc.task_types = WorkerTaskTypes::workflow_only();
        },
        _ => fast_tries,
    };
    opts.configure = Box::new(configure);
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
    // The try with no answer ends at the deadline of the Worker's settings.
    let (call, error) = match outage {
        Outage::CloseFails => ("close", "The Store is down"),
        Outage::AppendGetsNoAnswer => ("append", "The Store gave no answer in 50ms"),
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
    opts.configure =
        Box::new(|wc| wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600));
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
        Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
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
    // A heartbeat of the forced task can complete it before the result arrives.
    let recorded: Vec<usize> = (sent[1..].iter().filter_map(ledger))
        .map(|ledger| ledger.append_results.len())
        .collect();
    assert_eq!(recorded, [1]);
    assert_eq!(store.requests().len(), 1);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[tokio::test]
async fn a_call_in_an_outage_goes_on_when_a_full_ledger_completes_its_task() {
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
    opts.configure = Box::new(|wc| {
        fast_tries(wc);
        wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
    });
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
    // The appends of one Stream run one at a time. Each result arrives while no activation is
    // outstanding, so the one that doesn't fit in the ledger completes the task with a new one
    // forced.
    let mut results = vec![];
    let is_append = |request: &Request| matches!(request, Request::Append(..));
    while results.len() < 70 {
        // A release frees only an append that the Store already holds.
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
    // The close continues in the next task, and ends when the Store is available again.
    store.fail(Op::Close, StreamError::store_unavailable("down"), 0);
    let closed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        closed.jobs[0].variant,
        Some(Job::ResolveStreamClose(_))
    ));
    respond(&run.worker, &closed, workflow).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
    // Task 2 completed with a new task forced when its ledger was full, and the results that
    // didn't fit went into the ledger of task 3.
    let sent = run.sent();
    let appends = |sent: &WorkflowTaskCompletion| -> Vec<u32> {
        let ledger = ledger(sent).unwrap_or_default();
        (ledger.append_results.iter())
            .map(|append| append.seq as u32)
            .collect()
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
#[case::with_a_ledger_to_send(true, 1)]
#[case::with_nothing_else(false, 1)]
#[case::with_no_cache(false, 0)]
#[tokio::test]
async fn an_eviction_completes_a_task_that_a_call_holds(
    #[case] with_a_ledger: bool,
    #[case] cache: usize,
) {
    let store = store(&["s"]).await;
    let mut opts = StartOpts::new(&store);
    opts.cache = cache;
    opts.served = None;
    let run = start(history(Duration::from_secs(60)), opts);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    if !with_a_ledger {
        store.hold(Op::Append);
    }
    let completion = WorkflowActivationCompletion::from_cmd(run_id.clone(), append(1, "s"));
    run.worker
        .complete_workflow_activation(completion)
        .await
        .unwrap();
    if with_a_ledger {
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
    let recorded = ledger(last).map(|ledger| ledger.append_results.len());
    assert_eq!(recorded, Some(usize::from(with_a_ledger)));
    run.worker.drain_pollers_and_shutdown_ref().await;
}

#[tokio::test]
async fn a_replay_worker_makes_no_live_call() {
    let store = store(&["s", "c"]).await;
    let mut t = history(Duration::from_secs(60));
    complete_with_ledger(&mut t, &close_intent("c"));
    t.add_workflow_task_scheduled_and_started();
    let mut opts = StartOpts::new(&store);
    opts.replay = true;
    let run = start(t, opts);
    let jobs = run.drive(|_| vec![append(1, "s"), close(1, "c")]).await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(store.requests(), []);
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// What happened to task 2, which wrote append 1 and gave no result to History.
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
    complete_with_ledger(&mut t, &close_intent("c"));
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
        task.add_update_request("u", 8);
    }
    let runs_the_earlier_attempt = matches!(
        earlier,
        EarlierAttempt::LaterRetryOfAFailedTask | EarlierAttempt::Eviction
    );
    if !runs_the_earlier_attempt {
        let attempt = WorkflowTaskAttempt {
            started_event_id: 6,
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
        // The evicted run gets its next task from a poll, as a task of another Worker.
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
        EarlierAttempt::LaterRetryOfAFailedTask => ((9, 1), (9, 2)),
        EarlierAttempt::UpdateRejected => ((6, 1), (12, 1)),
        _ => ((6, 1), (9, 1)),
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

/// How a cached run gets a task after another attempt or Worker took over its task.
#[derive(Clone, Copy, Debug, PartialEq)]
enum OtherTask {
    /// Task 1 completed with an append in flight, and another Worker completed task 2.
    WithNoActivationOut,
    /// Task 1 timed out while its first activation was outstanding, and another Worker completed
    /// the retry.
    WithAnActivationOut,
    /// Another Worker completed a later attempt of task 1, which has the same started event id,
    /// while the first activation of attempt 2 was outstanding.
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

async fn respond(
    worker: &Worker,
    activation: &WorkflowActivation,
    workflow: impl FnOnce(&WorkflowActivation) -> Vec<Command>,
) {
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
    // The other attempt or Worker completed its task, with the result of append 1 in its ledger.
    let mut other = t.clone();
    match other_task {
        OtherTask::WithNoActivationOut => other.add_workflow_task_completed(),
        OtherTask::WithAnActivationOut => other.add_workflow_task_timed_out(),
        _ => {}
    }
    if other_task != OtherTask::AtTheSameStartedEventId {
        other.add_workflow_task_scheduled_and_started();
    }
    let offsets = AppendOffsets {
        first_offset: vec![7],
        last_offset: vec![7],
    };
    let result = AppendResult {
        seq: 1,
        r#ref: Some(stream("s")),
        result: Some(append_result::Result::Appended(offsets)),
        activation_index: 0,
    };
    let ledger = WorkflowTaskStreamCalls {
        append_results: vec![result],
        ..Default::default()
    };
    complete_with_ledger(&mut other, &ledger);
    other.add_we_signaled("done", vec![]);
    let next = next_task(&mut other);
    let workflow: Workflow = |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![append(1, "s")],
        Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
        Some(Job::SignalWorkflow(_)) => vec![complete()],
        _ => vec![],
    };
    let mut opts = StartOpts::new(&store);
    // An append that waits at the hold must not count as a try with no answer.
    opts.configure =
        Box::new(|wc| wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600));
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
        // Workflow code sent the append after the run stopped its calls, so it holds no task.
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

/// Rejects the Update `u`, whose request came in an earlier activation.
fn reject_update() -> Command {
    UpdateResponse {
        protocol_instance_id: "u".to_string(),
        response: Some(update_response::Response::Rejected(Failure::default())),
    }
    .into()
}

// The server drops the completion of a speculative task that has no command and only rejects
// Updates. A ledger isn't a command, so Core forces a new task to make the server keep it. Task 2
// has a Signal too, because the validator of an Update can't append.
#[rstest::rstest]
#[case::with_an_append(true)]
#[case::with_no_stream_call(false)]
#[tokio::test]
async fn a_completion_with_a_ledger_that_only_rejects_updates_forces_a_task(#[case] appends: bool) {
    let store = store(&["s"]).await;
    let mut t = history(Duration::from_secs(60));
    t.add_workflow_task_completed();
    t.add_timer_started("1".to_string());
    t.add_we_signaled("go", vec![]);
    let mut update = next_task(&mut t);
    update.add_update_request("u", 7);
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::ToTaskNum(1), ResponseType::Raw(update)];
    opts.served = None;
    let run = start(t, opts);
    let signaled = |activation: &WorkflowActivation| {
        (activation.jobs.iter()).any(|job| matches!(job.variant, Some(Job::SignalWorkflow(_))))
    };
    let workflow = |activation: &WorkflowActivation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        _ if signaled(activation) && appends => vec![append(1, "s"), reject_update()],
        _ if signaled(activation) => vec![reject_update()],
        _ => vec![],
    };
    let respond = async {
        while run.sent().len() < 2 {
            let activation = run.worker.poll_workflow_activation().await.unwrap();
            let commands = workflow(&activation);
            let completion = WorkflowActivationCompletion::from_cmds(activation.run_id, commands);
            run.worker
                .complete_workflow_activation(completion)
                .await
                .unwrap();
        }
    };
    tokio::time::timeout(Duration::from_secs(10), respond)
        .await
        .expect("Task 2 never completed");
    let completion = &run.sent()[1];
    assert_eq!(command_types(completion), []);
    assert_eq!(completion.messages.len(), 1);
    let appended = ledger(completion).map(|ledger| ledger.append_results.len());
    assert_eq!(appended, appends.then_some(1));
    assert_eq!(completion.force_create_new_workflow_task, appends);
    run.worker.drain_pollers_and_shutdown_ref().await;
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
    let offsets = AppendOffsets {
        first_offset: vec![1],
        last_offset: vec![1],
    };
    let mut data = WorkflowTaskStreamCalls::default();
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
            Kind::Append => data.append_results.push(AppendResult {
                seq: seq as i32,
                r#ref: Some(stream(&format!("a{seq}"))),
                result: Some(append_result::Result::Appended(offsets.clone())),
                activation_index: activation_index as i32,
            }),
        }
    }
    if !data.append_results.is_empty() {
        t.set_stream_calls_last_wft(data.clone());
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
            // Workflow code makes Local Activity 4 at the result of call 1, and the first run got
            // its result with the one of append 2, after the one of append 3.
            3 => (
                true,
                1,
                vec![
                    (local, 0, None),
                    (append, 2, None),
                    (append, 1, None),
                    (local, 2, Some(1)),
                ],
            ),
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
        // Each activation of task 2 after the first one is for a result, so the activations of the
        // results are consecutive.
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
        // Core gives a result as soon as it arrives. So a result that arrives in the activation
        // after the one that made its call is alone in it.
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
                // The results are in the order of the first run, and with chunking off also in its
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
                // With Local Activities alone, the result of call 2 is in the same activation as
                // the one of call 3, after it. The fixed shapes with both kinds replay as their
                // first run.
                if seed == 0 {
                    assert_eq!(replays[0], [vec![1], vec![3, 2]], "{case}");
                }
                if seed <= 2 {
                    assert_eq!(replays[2], first_run(&calls), "{case}");
                }
                // Append 2 waits for the result of Local Activity 4, which the first run gave
                // with it.
                if seed == 3 {
                    assert_eq!(replays[2], [vec![1, 3], vec![4, 2]], "{case}");
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

/// Returns Workflow code that a generated test runs: the commands of each activation that has a
/// job other than a query or an Update, in order. The last of them ends the Workflow.
fn script(rng: &mut StdRng) -> Vec<Vec<Command>> {
    let (mut appends, mut closes, mut reads, mut locals, mut timers) = (0, 0, 0, 0, 0);
    let steps = rng.random_range(3..=10);
    (1..=steps)
        .map(|step| {
            let mut commands = vec![];
            for _ in 0..rng.random_range(0..=3) {
                let id = format!("s{}", rng.random_range(0..3));
                match rng.random_range(0..12) {
                    0..=4 => {
                        appends += 1;
                        commands.push(append(appends, &id));
                    }
                    5 => {
                        closes += 1;
                        commands.push(close(closes, &id));
                    }
                    // A read of the first Records of a Stream. Some reads wait for one if it has
                    // none.
                    6..=7 => {
                        reads += 1;
                        let wait = rng.random_bool(0.5).then(|| Duration::from_secs(60));
                        let read = ReadStream {
                            seq: reads,
                            stream: Some(stream(&id)),
                            wait: wait.map(|wait| wait.try_into().unwrap()),
                            max_records: 2,
                            ..Default::default()
                        };
                        commands.push(read.into());
                    }
                    8..=10 => {
                        locals += 1;
                        commands.push(local_activity(locals));
                    }
                    _ => {
                        timers += 1;
                        commands.push(timer(timers));
                    }
                }
            }
            if step == steps {
                // A Local Activity that starts with the last command can resolve before the
                // completion goes out, and its marker then follows the last command.
                commands.retain(|command| !matches!(command, Command::ScheduleLocalActivity(_)));
                commands.push(complete());
            }
            commands
        })
        .collect()
}

/// How far the Workflow code of a script got.
#[derive(Default)]
struct Progress {
    /// The next step of the script.
    step: usize,
    /// The Local Activities that have no result yet.
    running: usize,
    /// The steps of the script that each activation with an event took.
    taken: Vec<Vec<usize>>,
}

/// The activations that a run had on a Worker before a new Worker took it over, with the steps of
/// the script that each of them took.
struct Earlier {
    first: Vec<Vec<String>>,
    taken: Vec<Vec<usize>>,
    /// The number of those activations that the new Worker gave again.
    at: usize,
}

/// Returns the commands of `script` for `activation`. It answers each query, rejects each Update,
/// and takes the next step for an activation with any other job.
///
/// It keeps the last step while a Local Activity runs. A result that comes with the last command
/// gets its marker after that command, and a replay Worker then never evicts the run.
///
/// On a Worker that took the run over, an activation that repeats activations of the `earlier`
/// Worker takes the steps that they took.
fn scripted(
    script: &[Vec<Command>],
    progress: &mut Progress,
    earlier: Option<&mut Earlier>,
    activation: &WorkflowActivation,
) -> Vec<Command> {
    let mut commands = vec![];
    let mut leaves_events = false;
    for job in &activation.jobs {
        match job.variant.as_ref() {
            Some(Job::QueryWorkflow(query)) => commands.push(
                QueryResult {
                    query_id: query.query_id.clone(),
                    variant: Some(query_result::Variant::Succeeded(QuerySuccess::default())),
                }
                .into(),
            ),
            Some(Job::DoUpdate(update)) => commands.push(
                UpdateResponse {
                    protocol_instance_id: update.protocol_instance_id.clone(),
                    response: Some(update_response::Response::Rejected(Failure::default())),
                }
                .into(),
            ),
            Some(Job::ResolveActivity(_)) => {
                progress.running -= 1;
                leaves_events = true;
            }
            _ => leaves_events = true,
        }
    }
    if !leaves_events {
        return commands;
    }
    let steps = match earlier {
        Some(earlier) if earlier.at < earlier.first.len() => {
            let jobs: Vec<Job> = (activation.jobs.iter())
                .filter_map(|job| job.variant.clone())
                .collect();
            let jobs = with_events(&[jobs]).concat();
            let count = joined(&earlier.first, earlier.at, &jobs);
            let again = &earlier.first[earlier.at..];
            assert!(count > 0, "{jobs:?} doesn't give {again:?} again");
            let steps = earlier.taken[earlier.at..earlier.at + count].concat();
            earlier.at += count;
            steps
        }
        _ => {
            let last = progress.step + 1 == script.len();
            match progress.step < script.len() && (!last || progress.running == 0) {
                true => vec![progress.step],
                false => vec![],
            }
        }
    };
    if let Some(step) = steps.last() {
        progress.step = step + 1;
    }
    let step_commands = steps.iter().flat_map(|step| &script[*step]);
    progress.running += (step_commands.clone())
        .filter(|command| matches!(command, Command::ScheduleLocalActivity(_)))
        .count();
    commands.extend(step_commands.cloned());
    progress.taken.push(steps);
    commands
}

/// Returns the number of activations of the first run (`first`), from `at` on, whose jobs one after
/// the other are the jobs of one activation of a replay (`jobs`). Returns 0 if there are none.
fn joined(first: &[Vec<String>], at: usize, jobs: &[String]) -> usize {
    let mut rest = jobs;
    for (count, activation) in (1..).zip(&first[at.min(first.len())..]) {
        let Some(after) = rest.strip_prefix(&activation[..]) else {
            return 0;
        };
        rest = after;
        if rest.is_empty() {
            return count;
        }
    }
    0
}

/// Returns the commands of a replay for an activation with the jobs `jobs`. With chunking on, a
/// replay can give the jobs of several activations of the first run (`first`) in one, and Workflow
/// code then takes the steps of `script` that the first run took in each of them (`taken`).
fn replayed(
    first: &[Vec<String>],
    script: &[Vec<Command>],
    taken: &[Vec<usize>],
    at: &mut usize,
    jobs: &[String],
) -> Vec<Command> {
    let count = joined(first, *at, jobs).max(1);
    let steps = taken.iter().skip(*at).take(count).flatten();
    *at += count;
    steps
        .flat_map(|step| script[*step].iter().cloned())
        .collect()
}

/// Returns a job as `append 3`, `append 3 failed`, `close 1`, `read 2 failed`, `local 2` or the
/// name of its kind. A page has its number of Records and its `last_offset`, as in `read 2 1 [5]`.
fn summary(job: &Job) -> String {
    match job {
        Job::ResolveStreamRead(ResolveStreamRead {
            seq,
            status: Some(resolve_stream_read::Status::Page(page)),
        }) => format!("read {seq} {} {:?}", page.records.len(), page.last_offset),
        Job::ResolveStreamRead(read) => format!("read {} failed", read.seq),
        Job::ResolveStreamAppend(ResolveStreamAppend {
            seq,
            status: Some(resolve_stream_append::Status::Failed(_)),
        }) => format!("append {seq} failed"),
        Job::ResolveStreamAppend(append) => format!("append {}", append.seq),
        Job::ResolveStreamClose(close) if close.failed.is_some() => {
            format!("close {} failed", close.seq)
        }
        Job::ResolveStreamClose(close) => format!("close {}", close.seq),
        Job::ResolveActivity(local) => format!("local {}", local.seq),
        other => format!("{other:?}")
            .split(['(', ' '])
            .next()
            .unwrap()
            .to_string(),
    }
}

/// Returns the jobs of each activation that leaves an event in History, with no query and no
/// Update.
fn with_events(jobs: &[Vec<Job>]) -> Vec<Vec<String>> {
    let kept = jobs.iter().map(|jobs| {
        (jobs.iter())
            .filter(|job| !matches!(job, Job::QueryWorkflow(_) | Job::DoUpdate(_)))
            .map(summary)
            .collect::<Vec<_>>()
    });
    kept.filter(|jobs| !jobs.is_empty()).collect()
}

/// Completes each Local Activity of `worker` after a random wait, and each cancel of one at once.
async fn complete_local_activities(worker: &Worker, rng: &mut StdRng) {
    while let Ok(task) = worker.poll_activity_task().await {
        let result = match task.variant {
            Some(activity_task::Variant::Cancel(_)) => {
                ActivityExecutionResult::cancel_from_details(None)
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(rng.random_range(0..30))).await;
                ActivityExecutionResult::ok(vec![1].into())
            }
        };
        let completion = ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(result),
        };
        worker.complete_activity_task(completion).await.unwrap();
    }
    std::future::pending().await
}

/// The cases that the generated runs reached, over all seeds.
#[derive(Debug, Default)]
struct Reached {
    /// The Stream results that came in the activation right after the one of their call.
    next: usize,
    /// The Stream results that came in a later activation.
    later: usize,
    queries: usize,
    updates: usize,
    /// The completions that asked for a new task.
    forced: usize,
    /// The runs that a new Worker took over.
    taken_over: usize,
    /// The pages of reads with Records, and those with none.
    pages: usize,
    empty_pages: usize,
    /// The pages of reads that don't wait, of a Stream that had a Record.
    no_wait_pages: usize,
}

// Generated Workflow code runs with Local Activities, queries, rejected Updates and slow Store
// calls. A replay of its History must give the same jobs in the same activations. With chunking
// on, one replay activation can have the jobs of several, in order. In some runs a new Worker
// takes over midway.
#[tokio::test]
async fn a_replay_gives_the_activations_of_the_first_run() {
    let ids = ["s0", "s1", "s2"];
    let mut reached = Reached::default();
    for seed in 0..40 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let script = script(&mut rng);
        let store = store(&ids).await;
        // Each task has a Signal, and some have a query or an Update. A forced task has no new
        // event. Each completion lets the Store answer the calls that wait. Once in a run, the
        // server keeps the next task for a new Worker.
        let (handover, handed_over) = (Arc::new(Notify::new()), Arc::new(Mutex::new(false)));
        let server = {
            let (released, handover, handed_over) =
                (store.clone(), handover.clone(), handed_over.clone());
            let mut rng = StdRng::seed_from_u64(!seed);
            Arc::new(Mutex::new(
                move |completion: &WorkflowTaskCompletion, t: &mut TestHistoryBuilder| {
                    for id in ids {
                        released.release_stream(id);
                    }
                    let types = command_types(completion);
                    if types.contains(&CommandType::CompleteWorkflowExecution) {
                        return None;
                    }
                    let forced = completion.force_create_new_workflow_task;
                    if !forced {
                        t.add_we_signaled("go", vec![]);
                    }
                    let heartbeat = forced
                        && completion.sdk_metadata.stream_calls.is_none()
                        && types
                            .iter()
                            .all(|kind| *kind == CommandType::ProtocolMessage);
                    let mut handed = handed_over.lock();
                    if !*handed && rng.random_bool(if heartbeat { 1.0 } else { 0.1 }) {
                        t.add_workflow_task_scheduled_and_started();
                        *handed = true;
                        handover.notify_one();
                        return None;
                    }
                    let mut task = next_task(t);
                    if rng.random_bool(0.3) {
                        let question = WorkflowQuery {
                            query_type: "q".to_string(),
                            ..Default::default()
                        };
                        task.queries
                            .insert(format!("q{}", t.current_event_id()), question);
                    }
                    if !forced && rng.random_bool(0.3) {
                        let update = format!("u{}", t.current_event_id());
                        task.add_update_request(update, t.current_event_id() - 1);
                    }
                    Some(task)
                },
            ))
        };
        let opts = || {
            let mut opts = StartOpts::new(&store);
            let server = server.clone();
            opts.eager = Some(Box::new(move |completion, t| {
                (*server.lock())(completion, t)
            }));
            opts
        };
        let mut run = start(history(Duration::from_secs(1)), opts());
        let mut locals_rng = StdRng::seed_from_u64(seed + 1);
        let (mut earlier, mut progress, mut made) = (None, Progress::default(), HashMap::new());
        // For each read that doesn't wait, true if its Stream had a Record when the first run sent
        // it.
        let mut had_records = HashMap::new();
        let live = loop {
            let mut number = 0;
            let workflow = |activation: &WorkflowActivation| {
                // The Store answers some of the calls that wait, and some calls of this activation
                // wait.
                for id in ids.iter().filter(|_| rng.random_bool(0.5)) {
                    store.release_stream(id);
                }
                let commands = scripted(&script, &mut progress, earlier.as_mut(), activation);
                for command in &commands {
                    let (call, stream) = match command {
                        Command::AppendToStream(append) => {
                            (format!("append {}", append.seq), &append.stream)
                        }
                        Command::CloseStream(close) => {
                            (format!("close {}", close.seq), &close.stream)
                        }
                        Command::ReadStream(read) => {
                            if read.wait.is_none() {
                                let id = &read.stream.as_ref().unwrap().id;
                                let had = !store.records(id).is_empty();
                                had_records.entry(read.seq).or_insert(had);
                            }
                            (format!("read {}", read.seq), &read.stream)
                        }
                        _ => continue,
                    };
                    made.insert(call, number);
                    if rng.random_bool(0.3) {
                        store.hold_stream(&stream.as_ref().unwrap().id);
                    }
                }
                number += 1;
                commands
            };
            let run_id = run.history.lock().get_orig_run_id().to_string();
            let evict = async {
                handover.notified().await;
                run.worker.request_workflow_eviction(&run_id);
                std::future::pending().await
            };
            let live = tokio::select! {
                jobs = run.drive(workflow) => jobs,
                () = complete_local_activities(&run.worker, &mut locals_rng) => unreachable!(),
                () = evict => unreachable!(),
            };
            run.worker.drain_pollers_and_shutdown_ref().await;
            let sent = run.sent();
            reached.forced += sent
                .iter()
                .filter(|c| c.force_create_new_workflow_task)
                .count();
            if !*handed_over.lock() || earlier.is_some() {
                break live;
            }
            reached.taken_over += 1;
            // A new Worker with an empty cache gets the task that the server kept.
            let taken = std::mem::take(&mut progress.taken);
            earlier = Some(Earlier {
                first: with_events(&live),
                taken,
                at: 0,
            });
            (progress, made) = (Progress::default(), HashMap::new());
            let history = run.history.lock().clone();
            run = start(history, opts());
        };
        let first = with_events(&live);
        for chunking in [false, true] {
            let mut at = 0;
            let workflow = |activation: &WorkflowActivation| {
                let jobs: Vec<Job> = (activation.jobs.iter())
                    .filter_map(|job| job.variant.clone())
                    .collect();
                let jobs = with_events(&[jobs]).concat();
                replayed(&first, &script, &progress.taken, &mut at, &jobs)
            };
            let replayed = with_events(&run.replay_with(chunking, workflow).await);
            if !chunking {
                assert_eq!(replayed, first, "seed {seed}");
                continue;
            }
            // Each activation of the replay has the jobs of one or more activations of the first
            // run, one after the other.
            let mut at = 0;
            for jobs in &replayed {
                let count = joined(&first, at, jobs);
                assert!(count > 0, "seed {seed}: {replayed:?} {first:?}");
                at += count;
            }
            assert_eq!(at, first.len(), "seed {seed}: {replayed:?} {first:?}");
        }

        // Only the Store can end a read that doesn't wait with no Record, so such a read of a
        // Stream that had a Record gets a page with one, or a failure.
        let results = live.iter().flatten().filter_map(|job| match job {
            Job::ResolveStreamRead(read) => Some(read),
            _ => None,
        });
        for read in results.filter(|read| had_records.get(&read.seq) == Some(&true)) {
            if let Some(resolve_stream_read::Status::Page(page)) = &read.status {
                assert!(!page.records.is_empty(), "seed {seed}: {read:?}");
                reached.no_wait_pages += 1;
            }
        }
        for (at, jobs) in live.iter().enumerate() {
            let summaries: Vec<String> = jobs.iter().map(summary).collect();
            for done in summaries
                .iter()
                .filter_map(|s| made.get(&s.split(' ').take(2).collect::<Vec<_>>().join(" ")))
            {
                match at == done + 1 {
                    true => reached.next += 1,
                    false => reached.later += 1,
                }
            }
            for summary in summaries.iter().filter(|s| s.starts_with("read")) {
                match summary.split(' ').nth(2) {
                    Some("0") => reached.empty_pages += 1,
                    Some("failed") => {}
                    _ => reached.pages += 1,
                }
            }
            for job in jobs {
                reached.queries += usize::from(matches!(job, Job::QueryWorkflow(_)));
                reached.updates += usize::from(matches!(job, Job::DoUpdate(_)));
            }
        }
    }
    let Reached {
        next,
        later,
        queries,
        updates,
        forced,
        taken_over,
        pages,
        empty_pages,
        no_wait_pages,
    } = reached;
    let counts = [
        next,
        later,
        queries,
        updates,
        forced,
        taken_over,
        pages,
        empty_pages,
        no_wait_pages,
    ];
    assert!(counts.iter().all(|count| *count > 0), "{reached:?}");
}
