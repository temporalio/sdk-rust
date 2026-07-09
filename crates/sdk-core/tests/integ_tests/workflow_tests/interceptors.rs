use crate::common::CoreWfStarter;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use temporalio_client::{
    WorkflowExecuteUpdateOptions, WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
    workflow_interceptors::{
        ExecuteWorkflowInput, ExecuteWorkflowResult, HandleQueryInput, HandleQueryResult,
        HandleSignalInput, HandleSignalResult, HandleUpdateInput, HandleUpdateResult,
        SyncWorkflowInterceptorContext, ValidateUpdateInput, ValidateUpdateResult,
        WorkflowInboundInterceptor, WorkflowInterceptorContext, WorkflowInterceptorFuture,
        WorkflowNext, WorkflowOutputValue,
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
        .await;
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
        _ctx: &WorkflowContextView,
        input: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    fn get_status(&self, _ctx: &WorkflowContextView, input: String) -> String {
        assert_eq!(input, "query-mutated");
        "query-original-output".to_string()
    }
}

struct MutatingWorkflowInterceptor {
    signal_post_handler_done: Arc<Notify>,
    saw_query_history_replay: Arc<Mutex<Option<bool>>>,
}

impl WorkflowInboundInterceptor for MutatingWorkflowInterceptor {
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
        *self.saw_query_history_replay.lock().unwrap() = Some(ctx.is_replaying_history_events());
        if let Some(input) = input.input_mut::<String>() {
            *input = "query-mutated".to_string();
        }

        let result = next.run(input)?;
        assert_eq!(
            result.downcast_ref::<String>().map(String::as_str),
            Some("query-original-output")
        );
        Ok(Box::new("query-replaced-output".to_string()) as Box<dyn WorkflowOutputValue>)
    }

    fn validate_update(
        &self,
        _ctx: SyncWorkflowInterceptorContext,
        mut input: ValidateUpdateInput,
        next: WorkflowNext<'_, ValidateUpdateInput, ValidateUpdateResult>,
    ) -> ValidateUpdateResult {
        if let Some(input) = input.input_mut::<String>() {
            input.push_str("-validated");
        }
        next.run(input)
    }
}

#[tokio::test]
async fn workflow_inbound_interceptors_mutate_inputs_and_replace_outputs() {
    let mut starter =
        CoreWfStarter::new("workflow_inbound_interceptors_mutate_inputs_and_replace_outputs");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<InboundInterceptorWorkflow>()
        .unwrap();

    let signal_post_handler_done = Arc::new(Notify::new());
    let saw_query_history_replay = Arc::new(Mutex::new(None));
    worker
        .inner_mut()
        .add_workflow_inbound_interceptor(MutatingWorkflowInterceptor {
            signal_post_handler_done: signal_post_handler_done.clone(),
            saw_query_history_replay: saw_query_history_replay.clone(),
        });

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
                WorkflowExecuteUpdateOptions::default(),
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
                WorkflowExecuteUpdateOptions::default(),
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
struct InboundInterceptorOrderWorkflow;

#[workflow_methods]
impl InboundInterceptorOrderWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        Ok(input)
    }
}

struct RecordingWorkflowInterceptor {
    name: &'static str,
    records: Arc<Mutex<Vec<String>>>,
}

impl WorkflowInboundInterceptor for RecordingWorkflowInterceptor {
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
}

#[tokio::test]
async fn workflow_inbound_interceptors_wrap_execute_in_order() {
    let mut starter = CoreWfStarter::new("workflow_inbound_interceptors_wrap_execute_in_order");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<InboundInterceptorOrderWorkflow>()
        .unwrap();

    let records = Arc::new(Mutex::new(Vec::new()));
    worker
        .inner_mut()
        .add_workflow_inbound_interceptor(RecordingWorkflowInterceptor {
            name: "outer",
            records: records.clone(),
        });
    worker
        .inner_mut()
        .add_workflow_inbound_interceptor(RecordingWorkflowInterceptor {
            name: "inner",
            records: records.clone(),
        });

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
            "inner after".to_string(),
            "outer after".to_string(),
        ]
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
        .await;
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

impl WorkflowInboundInterceptor for ConstructionPollingInterceptor {
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
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<InterceptorConstructionPollingWorkflow>()
        .unwrap();

    let sync_polls = Arc::new(AtomicUsize::new(0));
    let deferred_polls = Arc::new(AtomicUsize::new(0));
    let async_polls = Arc::new(AtomicUsize::new(0));
    worker
        .inner_mut()
        .add_workflow_inbound_interceptor(ConstructionPollingInterceptor {
            sync_polls: sync_polls.clone(),
            deferred_polls: deferred_polls.clone(),
            async_polls: async_polls.clone(),
        });

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
