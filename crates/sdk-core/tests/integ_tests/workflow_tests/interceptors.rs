use crate::common::{CoreWfStarter, WorkflowHandleExt, activity_functions::StdActivities};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use temporalio_client::{
    WorkflowExecuteUpdateOptions, WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::{
    common::v1::Payload,
    enums::v1::{EventType, WorkflowTaskFailedCause},
    history::v1::{History, history_event::Attributes::WorkflowTaskFailedEventAttributes},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, NexusOperationOptions,
    SyncWorkflowContext, TimerResult, WorkflowContext, WorkflowContextKey, WorkflowContextView,
    WorkflowResult,
    workflow_interceptors::{
        CancellableWorkflowOutboundFuture, ExecuteWorkflowInput, ExecuteWorkflowResult,
        HandleQueryInput, HandleQueryResult, HandleSignalInput, HandleSignalResult,
        HandleUpdateInput, HandleUpdateResult, InitializeWorkflowInput, InitializeWorkflowOutput,
        ScheduleActivityInput, ScheduleActivityResult, ScheduleLocalActivityInput,
        StartChildWorkflowInput, StartChildWorkflowResult, StartTimerInput,
        SyncWorkflowInterceptorContext, ValidateUpdateInput, ValidateUpdateResult,
        WorkflowCancellationHandle, WorkflowInterceptor, WorkflowInterceptorConstructor,
        WorkflowInterceptorContext, WorkflowInterceptorFuture, WorkflowNext, WorkflowOutboundValue,
        WorkflowOutputValue,
    },
};
use tokio::{join, sync::Notify};

#[workflow]
#[derive(Default)]
struct InboundInterceptorWorkflow {
    signal_value: Option<String>,
    update_value: Option<String>,
    finish: bool,
}

#[workflow_methods]
impl InboundInterceptorWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        assert_eq!(input, "run-mutated");
        ctx.wait_condition(|state| {
            state.signal_value.is_some() && state.update_value.is_some() && state.finish
        })
        .await?;
        Ok("run-original-output".to_string())
    }

    #[signal]
    fn set_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: String) {
        assert_eq!(input, "signal-mutated");
        self.signal_value = Some(input);
    }

    #[signal]
    fn finish(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.finish = true;
    }

    #[update_validator(set_update)]
    fn validate_set_update(
        &self,
        ctx: &WorkflowContextView,
        input: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        assert_eq!(
            ctx.context_value::<CurrentContextLabel>().as_deref(),
            Some(&"validator")
        );
        assert!(input.ends_with("-validated"));
        if input.starts_with("reject") {
            Err("update rejected by validator".into())
        } else {
            Ok(())
        }
    }

    #[update]
    fn set_update(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: String) -> String {
        assert_eq!(input, "update-handled");
        self.update_value = Some(input);
        "update-original-output".to_string()
    }

    #[query]
    fn get_status(&self, ctx: &WorkflowContextView, input: String) -> String {
        assert_eq!(
            ctx.context_value::<CurrentContextLabel>().as_deref(),
            Some(&"query")
        );
        assert_eq!(input, "query-mutated");
        "query-original-output".to_string()
    }
}

struct MutatingWorkflowInterceptor {
    signal_post_handler_done: Arc<Notify>,
    saw_query_history_replay: Arc<Mutex<Option<bool>>>,
}

impl WorkflowInterceptor for MutatingWorkflowInterceptor {
    fn execute<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        mut input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        WorkflowInterceptorFuture::new(async move {
            assert!(!ctx.is_replaying_history_events());
            if let Some(input) = input.input_mut::<String>() {
                *input = "run-mutated".to_string();
            }

            let result = next.run(input).await?;
            assert_eq!(
                result.downcast_ref::<String>().map(String::as_str),
                Some("run-original-output")
            );
            Ok(Box::new("run-replaced-output".to_string()) as Box<dyn WorkflowOutputValue>)
        })
    }

    fn handle_signal<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        mut input: HandleSignalInput,
        next: WorkflowNext<
            'a,
            HandleSignalInput,
            WorkflowInterceptorFuture<'a, HandleSignalResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
        let signal_post_handler_done = self.signal_post_handler_done.clone();
        WorkflowInterceptorFuture::new(async move {
            assert!(!ctx.is_replaying_history_events());
            let should_notify_after_signal = input.name() == "set_signal";
            if let Some(input) = input.input_mut::<String>() {
                *input = "signal-mutated".to_string();
            }

            let result = next.run(input).await;
            if result.is_ok() && should_notify_after_signal {
                signal_post_handler_done.notify_one();
            }
            result
        })
    }

    fn handle_update<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        mut input: HandleUpdateInput,
        next: WorkflowNext<
            'a,
            HandleUpdateInput,
            WorkflowInterceptorFuture<'a, HandleUpdateResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleUpdateResult> {
        assert_eq!(input.id(), "accepted-update-id");
        WorkflowInterceptorFuture::new(async move {
            assert!(!ctx.is_replaying_history_events());
            if let Some(input) = input.input_mut::<String>() {
                assert_eq!(input.as_str(), "update");
                *input = "update-handled".to_string();
            }

            let result = next.run(input).await?;
            assert_eq!(
                result.downcast_ref::<String>().map(String::as_str),
                Some("update-original-output")
            );
            Ok(Box::new("update-replaced-output".to_string()) as Box<dyn WorkflowOutputValue>)
        })
    }

    fn handle_query(
        &self,
        ctx: SyncWorkflowInterceptorContext,
        mut input: HandleQueryInput,
        next: WorkflowNext<'_, HandleQueryInput, HandleQueryResult>,
    ) -> HandleQueryResult {
        assert!(!input.id().is_empty());
        *self.saw_query_history_replay.lock().unwrap() = Some(ctx.is_replaying_history_events());
        if let Some(input) = input.input_mut::<String>() {
            *input = "query-mutated".to_string();
        }

        assert!(ctx.context_value::<CurrentContextLabel>().is_none());
        let result =
            ctx.with_context_value::<CurrentContextLabel, _>("query", || next.run(input))?;
        assert!(ctx.context_value::<CurrentContextLabel>().is_none());
        assert_eq!(
            result.downcast_ref::<String>().map(String::as_str),
            Some("query-original-output")
        );
        Ok(Box::new("query-replaced-output".to_string()) as Box<dyn WorkflowOutputValue>)
    }

    fn validate_update(
        &self,
        ctx: SyncWorkflowInterceptorContext,
        mut input: ValidateUpdateInput,
        next: WorkflowNext<'_, ValidateUpdateInput, ValidateUpdateResult>,
    ) -> ValidateUpdateResult {
        let expected_id = match input.input_ref::<String>().map(String::as_str) {
            Some("reject") => "rejected-update-id",
            Some("update") => "accepted-update-id",
            input => panic!("unexpected update validation input: {input:?}"),
        };
        assert_eq!(input.id(), expected_id);
        if let Some(input) = input.input_mut::<String>() {
            input.push_str("-validated");
        }
        assert!(ctx.context_value::<CurrentContextLabel>().is_none());
        let result =
            ctx.with_context_value::<CurrentContextLabel, _>("validator", || next.run(input));
        assert!(ctx.context_value::<CurrentContextLabel>().is_none());
        result
    }
}

#[tokio::test]
async fn workflow_interceptors_mutate_inputs_and_replace_outputs() {
    let mut starter = CoreWfStarter::new("workflow_interceptors_mutate_inputs_and_replace_outputs");
    let signal_post_handler_done = Arc::new(Notify::new());
    let saw_query_history_replay = Arc::new(Mutex::new(None));
    let signal_post_handler_done_ref = signal_post_handler_done.clone();
    let saw_query_history_replay_ref = saw_query_history_replay.clone();
    starter
        .sdk_config
        .register_workflow::<InboundInterceptorWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            MutatingWorkflowInterceptor {
                signal_post_handler_done: signal_post_handler_done_ref.clone(),
                saw_query_history_replay: saw_query_history_replay_ref.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InboundInterceptorWorkflow::run,
            "run".to_string(),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();

    let driver = async {
        let query_result = handle
            .query(
                InboundInterceptorWorkflow::get_status,
                "query".to_string(),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(query_result, "query-replaced-output");
        assert_eq!(*saw_query_history_replay.lock().unwrap(), Some(false));

        let rejected = handle
            .execute_update(
                InboundInterceptorWorkflow::set_update,
                "reject".to_string(),
                WorkflowExecuteUpdateOptions::builder()
                    .update_id("rejected-update-id".to_string())
                    .build(),
            )
            .await;
        assert!(rejected.is_err());

        handle
            .signal(
                InboundInterceptorWorkflow::set_signal,
                "signal".to_string(),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        signal_post_handler_done.notified().await;

        let update_result = handle
            .execute_update(
                InboundInterceptorWorkflow::set_update,
                "update".to_string(),
                WorkflowExecuteUpdateOptions::builder()
                    .update_id("accepted-update-id".to_string())
                    .build(),
            )
            .await
            .unwrap();
        assert_eq!(update_result, "update-replaced-output");

        handle
            .signal(
                InboundInterceptorWorkflow::finish,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        let result = handle.get_result(Default::default()).await.unwrap();
        assert_eq!(result, "run-replaced-output");
    };

    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(driver, run);
}

#[workflow]
#[derive(Default)]
struct AllHandlersFinishedWorkflow {
    handler_started: bool,
}

#[workflow_methods]
impl AllHandlersFinishedWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<bool> {
        ctx.wait_condition(|state| state.handler_started).await?;
        let handlers_finished = ctx.all_handlers_finished();
        let ctx_clone = ctx.clone();
        ctx.wait_condition(move |_| ctx_clone.all_handlers_finished())
            .await?;
        Ok(handlers_finished)
    }

    #[signal]
    fn sync_signal(&mut self, ctx: &mut SyncWorkflowContext<Self>) {
        assert!(!ctx.all_handlers_finished());
        self.handler_started = true;
    }

    #[signal]
    async fn async_signal(ctx: &mut WorkflowContext<Self>) {
        ctx.state_mut(|state| state.handler_started = true);
    }

    #[signal]
    fn wake(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {}

    #[update_validator(async_update)]
    fn validate_async_update(
        &self,
        _ctx: &WorkflowContextView,
        reject: &bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if *reject {
            Err("rejected by validator".into())
        } else {
            Ok(())
        }
    }

    #[update]
    async fn async_update(ctx: &mut WorkflowContext<Self>, _reject: bool) {
        ctx.state_mut(|state| state.handler_started = true);
    }
}

struct PostHandlerTimerInterceptor;

impl WorkflowInterceptor for PostHandlerTimerInterceptor {
    fn handle_signal<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        input: HandleSignalInput,
        next: WorkflowNext<
            'a,
            HandleSignalInput,
            WorkflowInterceptorFuture<'a, HandleSignalResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
        if input.name() == "wake" {
            return next.run(input);
        }
        WorkflowInterceptorFuture::new(async move {
            let result = next.run(input).await;
            ctx.timer(Duration::from_millis(1)).await;
            result
        })
    }

    fn handle_update<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        input: HandleUpdateInput,
        next: WorkflowNext<
            'a,
            HandleUpdateInput,
            WorkflowInterceptorFuture<'a, HandleUpdateResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleUpdateResult> {
        WorkflowInterceptorFuture::new(async move {
            let result = next.run(input).await;
            ctx.timer(Duration::from_millis(1)).await;
            result
        })
    }
}

#[derive(Clone, Copy)]
enum HandlerKind {
    SyncSignal,
    AsyncSignal,
    Update,
}

#[rstest::rstest]
#[tokio::test]
async fn all_handlers_finished_waits_for_handler_chain(
    #[values(
        HandlerKind::SyncSignal,
        HandlerKind::AsyncSignal,
        HandlerKind::Update
    )]
    handler_kind: HandlerKind,
    #[values(false, true)] with_interceptor: bool,
) {
    let mut starter = CoreWfStarter::new("all_handlers_finished_waits_for_handler_chain");
    starter
        .sdk_config
        .register_workflow::<AllHandlersFinishedWorkflow>()
        .unwrap();
    if with_interceptor {
        starter.sdk_config.register_workflow_interceptors(vec![
            WorkflowInterceptorConstructor::new(|_| PostHandlerTimerInterceptor),
        ]);
    }
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            AllHandlersFinishedWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let driver = async {
        match handler_kind {
            HandlerKind::SyncSignal => {
                handle
                    .signal(
                        AllHandlersFinishedWorkflow::sync_signal,
                        (),
                        WorkflowSignalOptions::default(),
                    )
                    .await
                    .unwrap();
            }
            HandlerKind::AsyncSignal => {
                handle
                    .signal(
                        AllHandlersFinishedWorkflow::async_signal,
                        (),
                        WorkflowSignalOptions::default(),
                    )
                    .await
                    .unwrap();
            }
            HandlerKind::Update => {
                handle
                    .execute_update(
                        AllHandlersFinishedWorkflow::async_update,
                        false,
                        WorkflowExecuteUpdateOptions::default(),
                    )
                    .await
                    .unwrap();
            }
        }
        assert_eq!(
            // If interceptor wasn't registered, no timer was scheduled after the handlers so they should
            // be finished after the first `wait_condition`.
            !with_interceptor,
            handle.get_result(Default::default()).await.unwrap()
        );
    };
    let (_, worker_result) = join!(driver, worker.run_until_done());
    worker_result.unwrap();
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}

struct CurrentContextLabel;

impl WorkflowContextKey for CurrentContextLabel {
    type Value = &'static str;
}

#[workflow]
#[derive(Default)]
struct WorkflowContextPropagationWorkflow {
    finish: bool,
}

#[workflow_methods]
impl WorkflowContextPropagationWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let run_ctx = ctx.clone();
        ctx.with_context_value::<CurrentContextLabel, _>("run", async move {
            let left_ctx = run_ctx.clone();
            let left = run_ctx.with_context_value::<CurrentContextLabel, _>("left", async move {
                left_ctx.timer(Duration::from_millis(1)).await;
                assert_eq!(
                    left_ctx.context_value::<CurrentContextLabel>().as_deref(),
                    Some(&"left")
                );
            });
            let right_ctx = run_ctx.clone();
            let right = run_ctx.with_context_value::<CurrentContextLabel, _>("right", async move {
                right_ctx.timer(Duration::from_millis(1)).await;
                assert_eq!(
                    right_ctx.context_value::<CurrentContextLabel>().as_deref(),
                    Some(&"right")
                );
            });
            temporalio_sdk::workflows::join!(left, right);

            assert_eq!(
                run_ctx.context_value::<CurrentContextLabel>().as_deref(),
                Some(&"run")
            );
            run_ctx.timer(Duration::from_millis(1)).await;
            run_ctx.wait_condition(|state| state.finish).await
        })
        .await?;
        assert!(ctx.context_value::<CurrentContextLabel>().is_none());
        Ok(())
    }

    #[signal]
    async fn finish(ctx: &mut WorkflowContext<Self>) {
        let signal_ctx = ctx.clone();
        ctx.with_context_value::<CurrentContextLabel, _>("signal", async move {
            signal_ctx.timer(Duration::from_millis(1)).await;
            assert_eq!(
                signal_ctx.context_value::<CurrentContextLabel>().as_deref(),
                Some(&"signal")
            );
            signal_ctx.state_mut(|state| state.finish = true);
        })
        .await;
        assert!(ctx.context_value::<CurrentContextLabel>().is_none());
    }
}

struct ObserveWorkflowContextInterceptor {
    observed: Arc<Mutex<Vec<&'static str>>>,
}

impl WorkflowInterceptor for ObserveWorkflowContextInterceptor {
    fn start_timer(
        &self,
        ctx: WorkflowInterceptorContext,
        input: StartTimerInput,
        next: WorkflowNext<
            'static,
            StartTimerInput,
            CancellableWorkflowOutboundFuture<TimerResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<TimerResult> {
        self.observed.lock().unwrap().push(
            *ctx.context_value::<CurrentContextLabel>()
                .expect("timer must have workflow context"),
        );
        next.run(input)
    }
}

#[tokio::test]
async fn workflow_context_is_branch_and_handler_local_during_replay() {
    let mut starter =
        CoreWfStarter::new("workflow_context_is_branch_and_handler_local_during_replay");
    let observed = Arc::new(Mutex::new(Vec::new()));
    let interceptor_observed = observed.clone();
    starter
        .sdk_config
        .register_workflow::<WorkflowContextPropagationWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            ObserveWorkflowContextInterceptor {
                observed: interceptor_observed.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            WorkflowContextPropagationWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();
    let driver = async {
        handle
            .signal(
                WorkflowContextPropagationWorkflow::finish,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        handle.get_result(Default::default()).await.unwrap();
    };
    let (_, worker_result) = join!(driver, worker.run_until_done());
    worker_result.unwrap();
    let mut live_observed = observed.lock().unwrap().clone();
    live_observed.sort_unstable();
    assert_eq!(live_observed, ["left", "right", "run", "signal"]);

    observed.lock().unwrap().clear();
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
    let mut replay_observed = observed.lock().unwrap().clone();
    replay_observed.sort_unstable();
    assert_eq!(replay_observed, ["left", "right", "run", "signal"]);
}

#[tokio::test]
async fn rejected_update_does_not_leave_a_handler_in_progress() {
    let mut starter = CoreWfStarter::new("rejected_update_does_not_leave_a_handler_in_progress");
    starter
        .sdk_config
        .register_workflow::<AllHandlersFinishedWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(|_| {
            PostHandlerTimerInterceptor
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            AllHandlersFinishedWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let driver = async {
        assert!(
            handle
                .execute_update(
                    AllHandlersFinishedWorkflow::async_update,
                    true,
                    WorkflowExecuteUpdateOptions::default(),
                )
                .await
                .is_err()
        );
        handle
            .signal(
                AllHandlersFinishedWorkflow::sync_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        assert!(!handle.get_result(Default::default()).await.unwrap());
    };
    let (_, worker_result) = join!(driver, worker.run_until_done());
    worker_result.unwrap();
}

struct NonTemporalPostHandlerInterceptor {
    waiting: Arc<Notify>,
    release: Arc<Notify>,
}

impl WorkflowInterceptor for NonTemporalPostHandlerInterceptor {
    fn handle_signal<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: HandleSignalInput,
        next: WorkflowNext<
            'a,
            HandleSignalInput,
            WorkflowInterceptorFuture<'a, HandleSignalResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
        if input.name() != "async_signal" {
            return next.run(input);
        }
        let waiting = self.waiting.clone();
        let release = self.release.clone();
        WorkflowInterceptorFuture::new(async move {
            let result = next.run(input).await;
            waiting.notify_one();
            release.notified().await;
            result
        })
    }
}

#[tokio::test]
async fn all_handlers_finished_tracks_nondeterministic_futures() {
    let mut starter =
        CoreWfStarter::new("all_handlers_finished_tracks_non_temporal_interceptor_futures");
    starter.sdk_config.detect_nondeterministic_futures = false;
    let waiting = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let waiting_ref = waiting.clone();
    let release_ref = release.clone();
    starter
        .sdk_config
        .register_workflow::<AllHandlersFinishedWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            NonTemporalPostHandlerInterceptor {
                waiting: waiting_ref.clone(),
                release: release_ref.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            AllHandlersFinishedWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let driver = async {
        handle
            .signal(
                AllHandlersFinishedWorkflow::async_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        waiting.notified().await;
        release.notify_one();
        handle
            .signal(
                AllHandlersFinishedWorkflow::wake,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        assert!(!handle.get_result(Default::default()).await.unwrap());
    };
    let (_, worker_result) = join!(driver, worker.run_until_done());
    worker_result.unwrap();
}

#[workflow]
#[derive(Default)]
struct InboundInterceptorOrderWorkflow;

#[workflow_methods]
impl InboundInterceptorOrderWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        ctx.timer(Duration::from_millis(1)).await;
        Ok(input)
    }
}

struct RecordingWorkflowInterceptor {
    name: &'static str,
    records: Arc<Mutex<Vec<String>>>,
}

impl WorkflowInterceptor for RecordingWorkflowInterceptor {
    fn execute<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        WorkflowInterceptorFuture::new(async move {
            self.records
                .lock()
                .unwrap()
                .push(format!("{} before", self.name));
            let result = next.run(input).await;
            self.records
                .lock()
                .unwrap()
                .push(format!("{} after", self.name));
            result
        })
    }

    fn start_timer(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: StartTimerInput,
        next: WorkflowNext<
            'static,
            StartTimerInput,
            CancellableWorkflowOutboundFuture<TimerResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<TimerResult> {
        self.records
            .lock()
            .unwrap()
            .push(format!("{} outbound before", self.name));
        let name = self.name;
        let records = self.records.clone();
        next.run(input).map(move |result| {
            records
                .lock()
                .unwrap()
                .push(format!("{name} outbound after"));
            result
        })
    }
}

#[tokio::test]
async fn workflow_interceptors_wrap_execute_in_order() {
    let mut starter = CoreWfStarter::new("workflow_interceptors_wrap_execute_in_order");

    let records = Arc::new(Mutex::new(Vec::new()));
    let outer_records = records.clone();
    let inner_records = records.clone();
    starter
        .sdk_config
        .register_workflow::<InboundInterceptorOrderWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![
            WorkflowInterceptorConstructor::new(move |_| RecordingWorkflowInterceptor {
                name: "outer",
                records: outer_records.clone(),
            }),
            WorkflowInterceptorConstructor::new(move |_| RecordingWorkflowInterceptor {
                name: "inner",
                records: inner_records.clone(),
            }),
        ]);
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InboundInterceptorOrderWorkflow::run,
            "hello".to_string(),
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert_eq!(
        handle.get_result(Default::default()).await.unwrap(),
        "hello"
    );

    assert_eq!(
        records.lock().unwrap().as_slice(),
        &[
            "outer before".to_string(),
            "inner before".to_string(),
            "inner outbound before".to_string(),
            "outer outbound before".to_string(),
            "outer outbound after".to_string(),
            "inner outbound after".to_string(),
            "inner after".to_string(),
            "outer after".to_string(),
        ]
    );
}

#[workflow]
struct InitInputInterceptorWorkflow {
    value: String,
}

#[workflow_methods]
impl InitInputInterceptorWorkflow {
    #[init]
    fn init(_ctx: &WorkflowContextView, value: String) -> Self {
        Self { value }
    }

    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        Ok(ctx.state(|workflow| workflow.value.clone()))
    }
}

struct InitInputMutationInterceptor {
    received_input: Arc<AtomicUsize>,
}

impl WorkflowInterceptor for InitInputMutationInterceptor {
    fn initialize_workflow(
        &self,
        _ctx: WorkflowContextView,
        mut input: InitializeWorkflowInput,
        next: WorkflowNext<'_, InitializeWorkflowInput, InitializeWorkflowOutput>,
    ) -> InitializeWorkflowOutput {
        if let Some(value) = input.input_mut::<String>() {
            self.received_input.fetch_add(1, Ordering::Relaxed);
            *value = "intercepted".to_string();
        }
        next.run(input)
    }

    fn execute<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        assert!(input.input_ref::<String>().is_none());
        next.run(input)
    }
}

#[tokio::test]
async fn workflow_initialize_interceptor_mutates_init_input() {
    let mut starter = CoreWfStarter::new("workflow_initialize_interceptor_mutates_init_input");

    let received_input = Arc::new(AtomicUsize::new(0));
    let received_input_ref = received_input.clone();
    starter
        .sdk_config
        .register_workflow::<InitInputInterceptorWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            InitInputMutationInterceptor {
                received_input: received_input_ref.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            InitInputInterceptorWorkflow::run,
            "original".to_string(),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    assert_eq!(received_input.load(Ordering::Relaxed), 1);
    assert_eq!(
        handle.get_result(Default::default()).await.unwrap(),
        "intercepted"
    );
}

#[workflow]
#[derive(Default)]
struct InterceptorConstructionPollingWorkflow {
    sync_signal_handled: bool,
    deferred_signal_handled: bool,
    async_signal_handled: bool,
}

#[workflow_methods]
impl InterceptorConstructionPollingWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|state| {
            state.sync_signal_handled && state.deferred_signal_handled && state.async_signal_handled
        })
        .await?;
        Ok(())
    }

    #[signal]
    fn sync_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.sync_signal_handled = true;
    }

    #[signal]
    fn deferred_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.deferred_signal_handled = true;
    }

    #[signal]
    async fn async_signal(ctx: &mut WorkflowContext<Self>) {
        ctx.state_mut(|state| state.async_signal_handled = true);
    }
}

struct CountPolls<F> {
    inner: Pin<Box<F>>,
    polls: Arc<AtomicUsize>,
}

impl<F> CountPolls<F> {
    fn new(inner: F, polls: Arc<AtomicUsize>) -> Self {
        Self {
            inner: Box::pin(inner),
            polls,
        }
    }
}

impl<F: Future> Future for CountPolls<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        self.inner.as_mut().poll(cx)
    }
}

#[derive(Default)]
struct PendingOnce(bool);

impl Future for PendingOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            Poll::Pending
        }
    }
}

struct ConstructionPollingInterceptor {
    sync_polls: Arc<AtomicUsize>,
    deferred_polls: Arc<AtomicUsize>,
    async_polls: Arc<AtomicUsize>,
}

#[workflow]
#[derive(Default)]
struct ConstructionWakeWorkflow;

#[workflow_methods]
impl ConstructionWakeWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct ConstructionWakeHandlerWorkflow {
    handled: bool,
}

#[workflow_methods]
impl ConstructionWakeHandlerWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|state| state.handled).await?;
        Ok(())
    }

    #[signal]
    async fn async_signal(ctx: &mut WorkflowContext<Self>) {
        ctx.state_mut(|state| state.handled = true);
    }

    #[update]
    async fn async_update(ctx: &mut WorkflowContext<Self>) {
        ctx.state_mut(|state| state.handled = true);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConstructionWakePoint {
    Execute,
    Signal,
    Update,
}

struct ConstructionWakeInterceptor {
    wake_point: ConstructionWakePoint,
    // Only inject a sleep on first attempt so if ND future detection is enabled
    // we succeed the wft on next attempt.
    attempts: Arc<AtomicUsize>,
}

impl WorkflowInterceptor for ConstructionWakeInterceptor {
    fn execute<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        if self.wake_point != ConstructionWakePoint::Execute {
            return next.run(input);
        }
        let should_sleep = self.attempts.fetch_add(1, Ordering::Relaxed) == 0;
        WorkflowInterceptorFuture::new(async move {
            if should_sleep {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            next.run(input).await
        })
    }

    fn handle_signal<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: HandleSignalInput,
        next: WorkflowNext<
            'a,
            HandleSignalInput,
            WorkflowInterceptorFuture<'a, HandleSignalResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
        if self.wake_point != ConstructionWakePoint::Signal {
            return next.run(input);
        }
        let should_sleep = self.attempts.fetch_add(1, Ordering::Relaxed) == 0;
        WorkflowInterceptorFuture::new(async move {
            if should_sleep {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            next.run(input).await
        })
    }

    fn handle_update<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: HandleUpdateInput,
        next: WorkflowNext<
            'a,
            HandleUpdateInput,
            WorkflowInterceptorFuture<'a, HandleUpdateResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleUpdateResult> {
        if self.wake_point != ConstructionWakePoint::Update {
            return next.run(input);
        }
        let should_sleep = self.attempts.fetch_add(1, Ordering::Relaxed) == 0;
        WorkflowInterceptorFuture::new(async move {
            if should_sleep {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            next.run(input).await
        })
    }
}

fn nondeterministic_future_failure_count(history: &History) -> usize {
    history
        .events
        .iter()
        .filter(|event| {
            event.event_type() == EventType::WorkflowTaskFailed
                && matches!(
                    event.attributes.as_ref(),
                    Some(WorkflowTaskFailedEventAttributes(attributes))
                        if attributes.cause
                            == WorkflowTaskFailedCause::NonDeterministicError as i32
                            && attributes.failure.as_ref().is_some_and(|failure|
                                failure.message.contains("Nondeterministic future detected"))
                )
        })
        .count()
}

struct SdkTimerBeforeNextInterceptor;

impl WorkflowInterceptor for SdkTimerBeforeNextInterceptor {
    fn execute<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        WorkflowInterceptorFuture::new(async move {
            ctx.timer(Duration::from_millis(1)).await;
            next.run(input).await
        })
    }
}

#[tokio::test]
async fn sdk_future_before_next_produces_an_activation() {
    let mut starter = CoreWfStarter::new("sdk_future_before_next_produces_an_activation");

    starter
        .sdk_config
        .register_workflow::<ConstructionWakeWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(|_| {
            SdkTimerBeforeNextInterceptor
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            ConstructionWakeWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let (result, worker_result) = join!(
        handle.get_result(Default::default()),
        worker.run_until_done()
    );
    result.unwrap();
    worker_result.unwrap();

    let history = starter.get_history().await;
    assert_eq!(nondeterministic_future_failure_count(&history), 0);
}

#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test]
async fn nondeterministic_future_detection_is_respected_for_interceptors(
    #[case] detect_nondeterministic_futures: bool,
) {
    let mut starter = CoreWfStarter::new(&format!(
        "nde_future_detection_{}",
        detect_nondeterministic_futures
    ));
    starter.sdk_config.detect_nondeterministic_futures = detect_nondeterministic_futures;
    starter
        .sdk_config
        .register_workflow::<ConstructionWakeWorkflow>()
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let interceptor_attempts = attempts.clone();
    starter
        .sdk_config
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            ConstructionWakeInterceptor {
                wake_point: ConstructionWakePoint::Execute,
                attempts: interceptor_attempts.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            ConstructionWakeWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let (result, worker_result) = join!(
        handle.get_result(Default::default()),
        worker.run_until_done()
    );
    result.unwrap();
    worker_result.unwrap();

    let history = starter.get_history().await;
    assert_eq!(
        nondeterministic_future_failure_count(&history),
        usize::from(detect_nondeterministic_futures)
    );
}

#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test]
async fn nondeterministic_future_detection_is_respected_for_async_signal_interceptors(
    #[case] detect_nondeterministic_futures: bool,
) {
    let mut starter = CoreWfStarter::new(&format!(
        "async_signal_nde_future_detection_{}",
        detect_nondeterministic_futures
    ));
    starter.sdk_config.detect_nondeterministic_futures = detect_nondeterministic_futures;
    starter
        .sdk_config
        .register_workflow::<ConstructionWakeHandlerWorkflow>()
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let interceptor_attempts = attempts.clone();
    starter
        .sdk_config
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            ConstructionWakeInterceptor {
                wake_point: ConstructionWakePoint::Signal,
                attempts: interceptor_attempts.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            ConstructionWakeHandlerWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let driver = async {
        handle
            .signal(
                ConstructionWakeHandlerWorkflow::async_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        handle.get_result(Default::default()).await.unwrap();
    };
    let (_, worker_result) = join!(driver, worker.run_until_done());
    worker_result.unwrap();
    let history = starter.get_history().await;
    assert_eq!(
        nondeterministic_future_failure_count(&history),
        usize::from(detect_nondeterministic_futures)
    );
}

#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test]
async fn nondeterministic_future_detection_is_respected_for_async_update_interceptors(
    #[case] detect_nondeterministic_futures: bool,
) {
    let mut starter = CoreWfStarter::new(&format!(
        "async_update_nde_future_detection_{}",
        detect_nondeterministic_futures
    ));
    starter.sdk_config.detect_nondeterministic_futures = detect_nondeterministic_futures;
    starter
        .sdk_config
        .register_workflow::<ConstructionWakeHandlerWorkflow>()
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let interceptor_attempts = attempts.clone();
    starter
        .sdk_config
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            ConstructionWakeInterceptor {
                wake_point: ConstructionWakePoint::Update,
                attempts: interceptor_attempts.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            ConstructionWakeHandlerWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let driver = async {
        handle
            .execute_update(
                ConstructionWakeHandlerWorkflow::async_update,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        handle.get_result(Default::default()).await.unwrap();
    };
    let (_, worker_result) = join!(driver, worker.run_until_done());
    worker_result.unwrap();

    let history = starter.get_history().await;
    assert_eq!(
        nondeterministic_future_failure_count(&history),
        usize::from(detect_nondeterministic_futures)
    );
}

impl WorkflowInterceptor for ConstructionPollingInterceptor {
    fn handle_signal<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: HandleSignalInput,
        next: WorkflowNext<
            'a,
            HandleSignalInput,
            WorkflowInterceptorFuture<'a, HandleSignalResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
        let signal_name = input.name();
        let is_deferred = signal_name == "deferred_signal";
        let polls = match signal_name {
            "deferred_signal" => self.deferred_polls.clone(),
            "async_signal" => self.async_polls.clone(),
            _ => self.sync_polls.clone(),
        };
        WorkflowInterceptorFuture::new(CountPolls::new(
            async move {
                if is_deferred {
                    PendingOnce::default().await;
                }
                next.run(input).await
            },
            polls,
        ))
    }
}

#[tokio::test]
async fn workflow_interceptors_are_polled_once_during_construction() {
    let mut starter =
        CoreWfStarter::new("workflow_interceptors_are_polled_once_during_construction");

    let sync_polls = Arc::new(AtomicUsize::new(0));
    let deferred_polls = Arc::new(AtomicUsize::new(0));
    let async_polls = Arc::new(AtomicUsize::new(0));
    let sync_polls_ref = sync_polls.clone();
    let deferred_polls_ref = deferred_polls.clone();
    let async_polls_ref = async_polls.clone();
    starter
        .sdk_config
        .register_workflow::<InterceptorConstructionPollingWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            ConstructionPollingInterceptor {
                sync_polls: sync_polls_ref.clone(),
                deferred_polls: deferred_polls_ref.clone(),
                async_polls: async_polls_ref.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            InterceptorConstructionPollingWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();

    let driver = async {
        handle
            .signal(
                InterceptorConstructionPollingWorkflow::sync_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        handle
            .signal(
                InterceptorConstructionPollingWorkflow::deferred_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        handle
            .signal(
                InterceptorConstructionPollingWorkflow::async_signal,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        handle.get_result(Default::default()).await.unwrap();
    };

    let (_, worker_result) = join!(driver, worker.run_until_done());
    worker_result.unwrap();
    assert_eq!(sync_polls.load(Ordering::Relaxed), 1);
    assert_eq!(deferred_polls.load(Ordering::Relaxed), 2);
    assert_eq!(async_polls.load(Ordering::Relaxed), 2);
}

#[workflow]
#[derive(Default)]
struct ConstructorOutboundInterceptorWorkflow;

#[workflow_methods]
impl ConstructorOutboundInterceptorWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        assert_eq!(
            ctx.timer(Duration::from_secs(60)).await,
            TimerResult::Cancelled
        );
        Ok("result".into())
    }
}

#[derive(Default)]
struct ConstructorOutboundInterceptor {
    inbound_calls: AtomicUsize,
    timer_calls: AtomicUsize,
    workflow_id: String,
}

impl WorkflowInterceptor for ConstructorOutboundInterceptor {
    fn execute<'a>(
        &'a self,
        _ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        self.inbound_calls.fetch_add(1, Ordering::Relaxed);
        let workflow_id = self.workflow_id.clone();
        WorkflowInterceptorFuture::new(async move {
            let result = next.run(input).await?;
            if let Some(result) = result.downcast_ref::<String>() {
                Ok(Box::new(format!("{result}-{workflow_id}")) as Box<dyn WorkflowOutputValue>)
            } else {
                Ok(result)
            }
        })
    }

    fn start_timer(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: StartTimerInput,
        _next: WorkflowNext<
            'static,
            StartTimerInput,
            CancellableWorkflowOutboundFuture<TimerResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<TimerResult> {
        assert_eq!(self.inbound_calls.load(Ordering::Relaxed), 1);
        assert_eq!(self.timer_calls.fetch_add(1, Ordering::Relaxed), 0);
        assert_eq!(input.options().duration, Duration::from_secs(60));
        CancellableWorkflowOutboundFuture::new(
            async { TimerResult::Cancelled },
            WorkflowCancellationHandle::new(|_| {}),
        )
    }
}

#[tokio::test]
async fn workflow_interceptor_constructors_create_unified_per_instance_interceptors() {
    let mut starter = CoreWfStarter::new(
        "workflow_interceptor_constructors_create_unified_per_instance_interceptors",
    );

    let expected_task_queue = starter.get_task_queue().to_owned();
    starter
        .sdk_config
        .register_workflow::<ConstructorOutboundInterceptorWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |ctx| {
            assert_eq!(
                ctx.workflow_type(),
                "ConstructorOutboundInterceptorWorkflow"
            );
            assert_eq!(ctx.task_queue(), expected_task_queue);
            ConstructorOutboundInterceptor {
                workflow_id: ctx.workflow_id().to_string(),
                ..Default::default()
            }
        })]);
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let first_workflow_id = format!("{}-1", starter.get_wf_id());
    let second_workflow_id = format!("{}-2", starter.get_wf_id());
    let first = worker
        .submit_workflow(
            ConstructorOutboundInterceptorWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), first_workflow_id.clone()).build(),
        )
        .await
        .unwrap();
    let second = worker
        .submit_workflow(
            ConstructorOutboundInterceptorWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, second_workflow_id.clone()).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    let first_result = first.get_result(Default::default()).await.unwrap();
    let second_result = second.get_result(Default::default()).await.unwrap();
    assert_eq!(first_result, format!("result-{first_workflow_id}"));
    assert_eq!(second_result, format!("result-{second_workflow_id}"));
}

#[workflow]
#[derive(Default)]
struct InboundContextOutboundWorkflow;

#[workflow_methods]
impl InboundContextOutboundWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        assert_eq!(
            ctx.timer(Duration::from_secs(2)).await,
            TimerResult::Cancelled
        );
        Ok(())
    }
}

struct InboundContextOutboundInterceptor {
    events: Arc<Mutex<Vec<String>>>,
}

impl WorkflowInterceptor for InboundContextOutboundInterceptor {
    fn execute<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        let events = self.events.clone();
        WorkflowInterceptorFuture::new(async move {
            events.lock().unwrap().push("inbound-before".to_string());
            assert_eq!(
                ctx.timer(Duration::from_secs(1)).await,
                TimerResult::Cancelled
            );
            let result = next.run(input).await;
            events
                .lock()
                .unwrap()
                .push("inbound-next-returned".to_string());
            assert_eq!(
                ctx.timer(Duration::from_secs(3)).await,
                TimerResult::Cancelled
            );
            events.lock().unwrap().push("inbound-after".to_string());
            result
        })
    }

    fn start_timer(
        &self,
        _ctx: WorkflowInterceptorContext,
        input: StartTimerInput,
        _next: WorkflowNext<
            'static,
            StartTimerInput,
            CancellableWorkflowOutboundFuture<TimerResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<TimerResult> {
        self.events
            .lock()
            .unwrap()
            .push(format!("timer-{}", input.options().duration.as_secs()));
        CancellableWorkflowOutboundFuture::new(
            async { TimerResult::Cancelled },
            WorkflowCancellationHandle::new(|_| {}),
        )
    }
}

#[tokio::test]
async fn inbound_interceptor_context_operations_use_the_outbound_chain_around_next() {
    let mut starter = CoreWfStarter::new(
        "inbound_interceptor_context_operations_use_the_outbound_chain_around_next",
    );

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_ref = events.clone();
    starter
        .sdk_config
        .register_workflow::<InboundContextOutboundWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(move |_| {
            InboundContextOutboundInterceptor {
                events: events_ref.clone(),
            }
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            InboundContextOutboundWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();

    assert_eq!(
        *events.lock().unwrap(),
        [
            "inbound-before",
            "timer-1",
            "timer-2",
            "inbound-next-returned",
            "timer-3",
            "inbound-after",
        ]
    );
}

#[allow(dead_code)]
fn assert_workflow_interceptor_context_outbound_api(ctx: &WorkflowInterceptorContext) {
    let _timer = ctx.timer(Duration::from_secs(1));
    let _activity = ctx.execute_activity(
        StdActivities::echo,
        String::new(),
        ActivityOptions::start_to_close_timeout(Duration::from_secs(1)),
    );
    let _local_activity = ctx.execute_local_activity(
        StdActivities::echo,
        String::new(),
        LocalActivityOptions::builder()
            .start_to_close_timeout(Duration::from_secs(1))
            .build(),
    );
    let _child = ctx.start_child_workflow(
        OutboundChildInterceptorChild::run,
        String::new(),
        ChildWorkflowOptions::workflow_id("child".into()),
    );
    let _external = ctx.external_workflow("external", None);
    let _nexus = ctx.start_nexus_operation(
        NexusOperationOptions::builder()
            .endpoint("endpoint")
            .service("service")
            .operation("operation")
            .build(),
    );
}

#[workflow]
#[derive(Default)]
struct OutboundActivityInterceptorWorkflow;

#[workflow_methods]
impl OutboundActivityInterceptorWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let activity = ctx
            .execute_activity(
                StdActivities::echo,
                "activity-original".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
        assert_eq!(activity, "activity-wrapped");

        let local_activity = ctx
            .execute_local_activity(
                StdActivities::echo,
                "local-original".to_string(),
                LocalActivityOptions::builder()
                    .start_to_close_timeout(Duration::from_secs(5))
                    .build(),
            )
            .await?;
        assert_eq!(local_activity, "local-wrapped");
        Ok(())
    }
}

struct OutboundActivityInterceptor;

#[allow(clippy::result_large_err)]
impl WorkflowInterceptor for OutboundActivityInterceptor {
    fn schedule_activity(
        &self,
        _ctx: WorkflowInterceptorContext,
        mut input: ScheduleActivityInput,
        next: WorkflowNext<
            'static,
            ScheduleActivityInput,
            CancellableWorkflowOutboundFuture<ScheduleActivityResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<ScheduleActivityResult> {
        *input.input_mut::<String>().unwrap() = "activity-mutated".to_string();
        input
            .headers_mut()
            .insert("intercepted".to_string(), Payload::default());
        next.run(input).map(|result| {
            assert_eq!(
                result
                    .as_ref()
                    .unwrap()
                    .downcast_ref::<String>()
                    .map(String::as_str),
                Some("activity-mutated")
            );
            Ok(Box::new("activity-wrapped".to_string()) as Box<dyn WorkflowOutboundValue>)
        })
    }

    fn schedule_local_activity(
        &self,
        _ctx: WorkflowInterceptorContext,
        mut input: ScheduleLocalActivityInput,
        next: WorkflowNext<
            'static,
            ScheduleLocalActivityInput,
            CancellableWorkflowOutboundFuture<ScheduleActivityResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<ScheduleActivityResult> {
        *input.input_mut::<String>().unwrap() = "local-mutated".to_string();
        next.run(input).map(|result| {
            assert_eq!(
                result
                    .as_ref()
                    .unwrap()
                    .downcast_ref::<String>()
                    .map(String::as_str),
                Some("local-mutated")
            );
            Ok(Box::new("local-wrapped".to_string()) as Box<dyn WorkflowOutboundValue>)
        })
    }
}

#[tokio::test]
async fn workflow_outbound_interceptors_mutate_activity_calls_and_results() {
    let mut starter =
        CoreWfStarter::new("workflow_outbound_interceptors_mutate_activity_calls_and_results");
    starter
        .sdk_config
        .register_activities(StdActivities)
        .register_workflow::<OutboundActivityInterceptorWorkflow>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(|_| {
            OutboundActivityInterceptor
        })]);
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            OutboundActivityInterceptorWorkflow::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}

#[workflow]
#[derive(Default)]
struct OutboundChildInterceptorChild;

#[workflow_methods]
impl OutboundChildInterceptorChild {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        assert_eq!(input, "child-mutated");
        Ok("child-original-output".to_string())
    }
}

#[workflow]
#[derive(Default)]
struct OutboundChildInterceptorParent;

#[workflow_methods]
impl OutboundChildInterceptorParent {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let child = ctx
            .start_child_workflow(
                OutboundChildInterceptorChild::run,
                "child-original".to_string(),
                ChildWorkflowOptions::workflow_id(format!("{}-child", ctx.workflow_id())),
            )
            .await?;
        assert_eq!(child.result().await?, "child-wrapped-output");
        Ok(())
    }
}

struct OutboundChildInterceptor;

impl WorkflowInterceptor for OutboundChildInterceptor {
    fn start_child_workflow(
        &self,
        _ctx: WorkflowInterceptorContext,
        mut input: StartChildWorkflowInput,
        next: WorkflowNext<
            'static,
            StartChildWorkflowInput,
            CancellableWorkflowOutboundFuture<StartChildWorkflowResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<StartChildWorkflowResult> {
        *input.input_mut::<String>().unwrap() = "child-mutated".to_string();
        input
            .headers_mut()
            .insert("intercepted".to_string(), Payload::default());
        next.run(input).map(|result| {
            result.map(|output| {
                output.map_result(|result| {
                    result.map(|result| {
                        result.map(|output| {
                            assert_eq!(
                                output.downcast_ref::<String>().map(String::as_str),
                                Some("child-original-output")
                            );
                            Box::new("child-wrapped-output".to_string())
                                as Box<dyn WorkflowOutboundValue>
                        })
                    })
                })
            })
        })
    }
}

#[tokio::test]
async fn workflow_outbound_interceptors_wrap_child_start_and_completion() {
    let mut starter =
        CoreWfStarter::new("workflow_outbound_interceptors_wrap_child_start_and_completion");

    starter
        .sdk_config
        .register_workflow::<OutboundChildInterceptorParent>()
        .unwrap()
        .register_workflow::<OutboundChildInterceptorChild>()
        .unwrap()
        .register_workflow_interceptors(vec![WorkflowInterceptorConstructor::new(|_| {
            OutboundChildInterceptor
        })]);

    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            OutboundChildInterceptorParent::run,
            (),
            WorkflowStartOptions::new(
                starter.get_task_queue().to_owned(),
                starter.get_wf_id().to_owned(),
            )
            .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();
}
