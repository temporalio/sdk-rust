use crate::common::{CoreWfStarter, TestWorker, eventually};
use futures_util::future::LocalBoxFuture;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    PluginError, WorkflowHistory, WorkflowQueryOptions, WorkflowStartOptions,
    WorkflowTerminateOptions, errors::WorkflowGetResultError,
};
use temporalio_common::protos::temporal::api::{enums::v1::EventType, history::v1::History};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ApplicationFailure, SimplePlugin, WorkerPlugin, WorkerRunError,
    WorkflowContext, WorkflowContextView, WorkflowDefinitions, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::{Next, RunWorkerInput, WithWorkflowReplayWorkerInput, WorkerInterceptor},
    workflow_replayer::{
        WorkflowReplayError, WorkflowReplayFailure, WorkflowReplayer, WorkflowReplayerOptions,
    },
};

struct SayHelloActivities;

#[activities]
impl SayHelloActivities {
    #[activity]
    async fn say_hello(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}

#[derive(Default, serde::Deserialize, serde::Serialize, bon::Builder)]
struct SayHelloInput {
    #[builder(into)]
    name: String,
    #[builder(default)]
    should_hang: bool,
    #[builder(default)]
    should_error: bool,
    #[builder(default)]
    should_fail_task: bool,
    #[builder(default)]
    should_cause_nondeterminism: bool,
}

impl SayHelloInput {
    fn new(name: impl Into<String>) -> Self {
        Self::builder().name(name).build()
    }
}

#[workflow]
#[derive(Default)]
struct SayHelloWorkflow {
    waiting: bool,
}

#[workflow_methods]
impl SayHelloWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: SayHelloInput) -> WorkflowResult<String> {
        let greeting = ctx
            .execute_activity(
                SayHelloActivities::say_hello,
                input.name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;

        if input.should_hang {
            ctx.state_mut(|workflow| workflow.waiting = true);
            ctx.wait_condition(|_| false).await?;
        }
        if input.should_error {
            return Err(ApplicationFailure::new("Intentional workflow failure").into());
        }
        if input.should_fail_task {
            panic!("Intentional workflow task failure");
        }
        if input.should_cause_nondeterminism && ctx.is_replaying() {
            ctx.timer(Duration::from_secs(1)).await;
        }

        Ok(greeting)
    }

    #[query]
    fn waiting(&self, _ctx: &WorkflowContextView) -> bool {
        self.waiting
    }
}

async fn replay_test_worker(test_name: &str) -> (CoreWfStarter, TestWorker) {
    let mut starter = CoreWfStarter::new(test_name);
    starter
        .sdk_config
        .register_activities(SayHelloActivities)
        .register_workflow::<SayHelloWorkflow>()
        .unwrap();
    let worker = starter.worker().await;
    (starter, worker)
}

fn replayer() -> WorkflowReplayer {
    WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<SayHelloWorkflow>()
            .unwrap()
            .build(),
    )
    .unwrap()
}

#[tokio::test]
async fn workflow_replayer_replays_completed_workflow() {
    let (starter, mut worker) =
        replay_test_worker("workflow_replayer_replays_completed_workflow").await;
    let handle = worker
        .submit_workflow(
            SayHelloWorkflow::run,
            SayHelloInput::new("Temporal"),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    assert_eq!(
        handle.get_result(Default::default()).await.unwrap(),
        "Hello, Temporal!"
    );
    let history = handle.fetch_history(Default::default());
    let workflow_id = history.workflow_id().map(str::to_owned);
    let history_json = history.to_json().await.unwrap();
    let history_from_json = WorkflowHistory::from_json(&history_json).unwrap();
    assert_eq!(history_from_json.workflow_id(), workflow_id.as_deref());

    let replayer = replayer();
    replayer.replay_workflow(history_from_json).await.unwrap();
    let results = replayer
        .replay_workflows([
            WorkflowHistory::from_json(&history_json).unwrap(),
            WorkflowHistory::from_json(&history_json).unwrap(),
        ])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.replay_failure.is_none()));
    let cloned_result = results[0].clone();
    assert_eq!(
        cloned_result.history.workflow_id(),
        Some(starter.get_wf_id())
    );
    assert!(!cloned_result.history.events().is_empty());
}

#[tokio::test]
async fn workflow_replayer_replays_incomplete_workflow() {
    let (starter, mut worker) =
        replay_test_worker("workflow_replayer_replays_incomplete_workflow").await;
    let handle = worker
        .submit_workflow(
            SayHelloWorkflow::run,
            SayHelloInput::builder()
                .name("Temporal")
                .should_hang(true)
                .build(),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let fetch_open_history = async {
        eventually(
            || async {
                handle
                    .query(
                        SayHelloWorkflow::waiting,
                        (),
                        WorkflowQueryOptions::default(),
                    )
                    .await
                    .unwrap()
                    .then_some(())
                    .ok_or("workflow is not waiting")
            },
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let history: WorkflowHistory = History {
            events: handle
                .fetch_history(Default::default())
                .into_events()
                .await
                .unwrap(),
        }
        .into();
        handle
            .terminate(WorkflowTerminateOptions::default())
            .await
            .unwrap();
        history
    };
    let (history, worker_result) = tokio::join!(fetch_open_history, worker.run_until_done());
    worker_result.unwrap();

    replayer().replay_workflow(history).await.unwrap();
}

#[tokio::test]
async fn workflow_replayer_replays_failed_workflow() {
    let (starter, mut worker) =
        replay_test_worker("workflow_replayer_replays_failed_workflow").await;
    let handle = worker
        .submit_workflow(
            SayHelloWorkflow::run,
            SayHelloInput::builder()
                .name("Temporal")
                .should_error(true)
                .build(),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    assert!(matches!(
        handle.get_result(Default::default()).await,
        Err(WorkflowGetResultError::Failed(_))
    ));
    let history = handle.fetch_history(Default::default());

    replayer().replay_workflow(history).await.unwrap();
}

#[tokio::test]
async fn workflow_replayer_reports_nondeterminism() {
    let (starter, mut worker) =
        replay_test_worker("workflow_replayer_reports_nondeterminism").await;
    let handle = worker
        .submit_workflow(
            SayHelloWorkflow::run,
            SayHelloInput::builder()
                .name("Temporal")
                .should_cause_nondeterminism(true)
                .build(),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    let events = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let replayer = replayer();

    assert!(matches!(
        replayer
            .replay_workflow(
                History {
                    events: events.clone(),
                }
                .into()
            )
            .await,
        Err(WorkflowReplayError::Replay(
            WorkflowReplayFailure::Nondeterminism { .. }
        ))
    ));
    let results = replayer
        .replay_workflows([History { events }.into()])
        .await
        .unwrap();
    assert!(matches!(
        results[0].replay_failure,
        Some(WorkflowReplayFailure::Nondeterminism { .. })
    ));
}

#[tokio::test]
async fn workflow_replayer_replays_history_with_workflow_task_failure() {
    let (starter, mut worker) =
        replay_test_worker("workflow_replayer_replays_history_with_workflow_task_failure").await;
    let handle = worker
        .submit_workflow(
            SayHelloWorkflow::run,
            SayHelloInput::builder()
                .name("Temporal")
                .should_fail_task(true)
                .build(),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let fetch_failed_history = async {
        let history = eventually(
            || async {
                let events = handle
                    .fetch_history(Default::default())
                    .into_events()
                    .await
                    .unwrap();
                events
                    .iter()
                    .any(|event| event.event_type() == EventType::WorkflowTaskFailed)
                    .then_some(History { events }.into())
                    .ok_or("workflow task failure not yet recorded")
            },
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        handle
            .terminate(WorkflowTerminateOptions::default())
            .await
            .unwrap();
        history
    };
    let (history, worker_result) = tokio::join!(fetch_failed_history, worker.run_until_done());
    worker_result.unwrap();

    replayer().replay_workflow(history).await.unwrap();
}

#[tokio::test]
async fn workflow_replayer_returns_ordered_results_for_multiple_histories() {
    let (starter, mut worker) =
        replay_test_worker("workflow_replayer_returns_ordered_results_for_multiple_histories")
            .await;
    let successful_handle = worker
        .submit_workflow(
            SayHelloWorkflow::run,
            SayHelloInput::new("Temporal"),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                format!("{}-success", starter.get_wf_id()),
            )
            .build(),
        )
        .await
        .unwrap();
    let nondeterministic_handle = worker
        .submit_workflow(
            SayHelloWorkflow::run,
            SayHelloInput::builder()
                .name("Temporal")
                .should_cause_nondeterminism(true)
                .build(),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                format!("{}-nondeterministic", starter.get_wf_id()),
            )
            .build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    let successful_history = successful_handle.fetch_history(Default::default());
    let nondeterministic_history = nondeterministic_handle.fetch_history(Default::default());

    let results = replayer()
        .replay_workflows([successful_history, nondeterministic_history])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].replay_failure.is_none());
    assert!(matches!(
        results[1].replay_failure,
        Some(WorkflowReplayFailure::Nondeterminism { .. })
    ));
}

struct ReplayConfigPlugin {
    configure_calls: Arc<AtomicUsize>,
}

struct ReplayLifecycleInterceptor {
    run_calls: Arc<AtomicUsize>,
    replay_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for ReplayLifecycleInterceptor {
    fn run_worker<'a>(
        &'a self,
        input: RunWorkerInput<'a>,
        next: Next<'a, RunWorkerInput<'a>, LocalBoxFuture<'a, Result<(), WorkerRunError>>>,
    ) -> LocalBoxFuture<'a, Result<(), WorkerRunError>> {
        self.run_calls.fetch_add(1, Ordering::Relaxed);
        next.run(input)
    }

    fn with_workflow_replay_worker<'a>(
        &'a self,
        input: WithWorkflowReplayWorkerInput<'a>,
        next: Next<
            'a,
            WithWorkflowReplayWorkerInput<'a>,
            LocalBoxFuture<'a, Result<(), WorkerRunError>>,
        >,
    ) -> LocalBoxFuture<'a, Result<(), WorkerRunError>> {
        self.replay_calls.fetch_add(1, Ordering::Relaxed);
        next.run(input)
    }
}

impl WorkerPlugin for ReplayConfigPlugin {
    fn name(&self) -> &str {
        "replay-config"
    }

    fn configure_workflow_replayer_options(
        &self,
        options: &mut WorkflowReplayerOptions,
    ) -> Result<(), PluginError> {
        self.configure_calls.fetch_add(1, Ordering::Relaxed);
        options
            .register_workflow::<SayHelloWorkflow>()
            .map_err(PluginError::new)?;
        Ok(())
    }
}

#[tokio::test]
async fn workflow_replayer_applies_plugins() {
    let (starter, mut worker) = replay_test_worker("workflow_replayer_applies_plugins").await;
    let handle = worker
        .submit_workflow(
            SayHelloWorkflow::run,
            SayHelloInput::new("Temporal"),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let events = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();

    let configure_calls = Arc::new(AtomicUsize::new(0));
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .worker_plugin(ReplayConfigPlugin {
                configure_calls: configure_calls.clone(),
            })
            .build(),
    )
    .unwrap();
    assert_eq!(
        replayer
            .options()
            .workflows()
            .workflow_definitions()
            .count(),
        1
    );
    replayer
        .replay_workflow(
            History {
                events: events.clone(),
            }
            .into(),
        )
        .await
        .unwrap();
    assert_eq!(configure_calls.load(Ordering::Relaxed), 1);

    let run_calls = Arc::new(AtomicUsize::new(0));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<SayHelloWorkflow>()
            .unwrap()
            .worker_interceptor(ReplayLifecycleInterceptor {
                run_calls: run_calls.clone(),
                replay_calls: replay_calls.clone(),
            })
            .build(),
    )
    .unwrap();
    replayer
        .replay_workflows([
            History {
                events: events.clone(),
            }
            .into(),
            History {
                events: events.clone(),
            }
            .into(),
        ])
        .await
        .unwrap();
    assert_eq!(run_calls.load(Ordering::Relaxed), 0);
    assert_eq!(replay_calls.load(Ordering::Relaxed), 1);

    let mut workflows = WorkflowDefinitions::new();
    workflows.register_workflow::<SayHelloWorkflow>().unwrap();
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .worker_plugin(
                SimplePlugin::builder("simple-replay")
                    .workflows(workflows)
                    .build(),
            )
            .build(),
    )
    .unwrap();
    replayer
        .replay_workflow(History { events }.into())
        .await
        .unwrap();
}
