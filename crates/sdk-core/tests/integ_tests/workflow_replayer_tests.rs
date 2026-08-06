use crate::common::{CoreWfStarter, TestWorker};
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
use temporalio_common::protos::temporal::api::enums::v1::EventType;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, SimplePlugin, WorkerPlugin, WorkflowContext, WorkflowContextView,
    WorkflowDefinitions, WorkflowResult,
    activities::{ActivityContext, ActivityError},
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
            return Err(anyhow::anyhow!("Intentional workflow failure").into());
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
    starter.sdk_config.register_activities(SayHelloActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<SayHelloWorkflow>().unwrap();
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
    let history = handle.fetch_history(Default::default()).await.unwrap();
    let history_from_json = WorkflowHistory::from_json(&history.to_json().unwrap()).unwrap();
    assert_eq!(history_from_json.workflow_id(), history.workflow_id());

    let replayer = replayer();
    replayer
        .replay_workflow(history_from_json.clone())
        .await
        .unwrap();
    let results = replayer
        .replay_workflows([history_from_json.clone(), history_from_json])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.replay_failure.is_none()));
    assert_eq!(results[0].history.run_id(), results[1].history.run_id());
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
        while !handle
            .query(
                SayHelloWorkflow::waiting,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap()
        {}
        let history = handle.fetch_history(Default::default()).await.unwrap();
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
    let history = handle.fetch_history(Default::default()).await.unwrap();

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
    let history = handle.fetch_history(Default::default()).await.unwrap();
    let replayer = replayer();

    assert!(matches!(
        replayer.replay_workflow(history.clone()).await,
        Err(WorkflowReplayError::Replay(
            WorkflowReplayFailure::Nondeterminism { .. }
        ))
    ));
    let results = replayer.replay_workflows([history]).await.unwrap();
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
        loop {
            let history = handle.fetch_history(Default::default()).await.unwrap();
            if history
                .events()
                .iter()
                .any(|event| event.event_type() == EventType::WorkflowTaskFailed)
            {
                handle
                    .terminate(WorkflowTerminateOptions::default())
                    .await
                    .unwrap();
                break history;
            }
        }
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
    let successful_history = successful_handle
        .fetch_history(Default::default())
        .await
        .unwrap();
    let nondeterministic_history = nondeterministic_handle
        .fetch_history(Default::default())
        .await
        .unwrap();

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
    let history = handle.fetch_history(Default::default()).await.unwrap();

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
    replayer.replay_workflow(history.clone()).await.unwrap();
    assert_eq!(configure_calls.load(Ordering::Relaxed), 1);

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
    replayer.replay_workflow(history).await.unwrap();
}
