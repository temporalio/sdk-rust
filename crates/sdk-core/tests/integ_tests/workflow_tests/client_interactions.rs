use crate::common::CoreWfStarter;
use temporalio_client::{
    UntypedQuery, UntypedSignal, UntypedUpdate, WorkflowDescribeOptions,
    WorkflowExecuteUpdateOptions, WorkflowQueryOptions, WorkflowSignalOptions,
    WorkflowStartOptions,
};
use temporalio_common::{
    data_converters::{PayloadConverter, RawValue},
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult};

#[workflow]
#[derive(Default)]
struct InteractionWorkflow {
    counter: i32,
    log: Vec<&'static str>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ReturnVal {
    counter: i32,
    log: Vec<String>,
}

#[workflow_methods]
impl InteractionWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        wait_for_value: i32,
    ) -> WorkflowResult<ReturnVal> {
        ctx.state_mut(|s| s.log.push("run"));
        ctx.wait_condition(|s| s.counter == wait_for_value).await;
        let rval = ctx.state_mut(|s| {
            s.log.push("run_done");
            ReturnVal {
                counter: s.counter,
                log: std::mem::take(&mut s.log)
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            }
        });
        Ok(rval)
    }

    #[signal]
    fn increment(&mut self, _ctx: &mut SyncWorkflowContext<Self>, amount: i32) {
        self.counter += amount;
    }

    #[signal]
    async fn increment_and_wait(ctx: &mut WorkflowContext<Self>, amount_and_target: (i32, i32)) {
        ctx.state_mut(|s| s.counter += amount_and_target.0);
        ctx.wait_condition(|s| s.counter >= amount_and_target.1)
            .await;
        ctx.state_mut(|s| s.log.push("async signal done"));
    }

    #[query]
    fn get_counter(&self, _ctx: &WorkflowContextView) -> i32 {
        self.counter
    }

    #[query]
    fn get_counter_checked(
        &self,
        _ctx: &WorkflowContextView,
        min: i32,
    ) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
        if self.counter < min {
            Err(format!("Counter {} is below minimum {}", self.counter, min).into())
        } else {
            Ok(self.counter)
        }
    }

    #[update]
    fn set_counter(&mut self, _ctx: &mut SyncWorkflowContext<Self>, value: i32) -> i32 {
        self.log.push("set counter");
        let old = self.counter;
        self.counter = value;
        old
    }

    #[update]
    fn invalidate_vec(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.log = vec![]
    }

    #[update]
    async fn change_and_wait(ctx: &mut WorkflowContext<Self>, amount_and_wait: (i32, i32)) {
        ctx.state_mut(|s| {
            s.log.push("starting change_and_wait");
            s.counter += amount_and_wait.0;
        });
        ctx.wait_condition(|s| s.counter == amount_and_wait.1).await;
        ctx.state_mut(|s| s.log.push("done change_and_wait"));
    }

    #[update_validator(validated_set)]
    fn validate_validated_set(
        &self,
        _ctx: &WorkflowContextView,
        value: &i32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if *value < 0 {
            Err("Value must be non-negative".into())
        } else {
            Ok(())
        }
    }
    #[update]
    fn validated_set(&mut self, _ctx: &mut SyncWorkflowContext<Self>, value: i32) -> i32 {
        let old = self.counter;
        self.counter = value;
        old
    }
}

// TODO: add another test that uses wait condition concurrently with some future that uses
// state_mut

#[tokio::test]
async fn test_typed_signal() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            42, // Wait for counter == 42
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_signal", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    // Send signal concurrently with worker
    let signaler = async {
        handle
            .signal(
                InteractionWorkflow::increment,
                42,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(signaler, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result.counter, 42);
}

#[tokio::test]
async fn test_typed_update() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            999, // Wait for counter == 999
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_update", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let updates = async {
        let old_value = handle
            .execute_update(
                InteractionWorkflow::set_counter,
                2,
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(old_value, 0);
        handle
            .execute_update(
                InteractionWorkflow::invalidate_vec,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        handle
            .execute_update(
                InteractionWorkflow::change_and_wait,
                (997, 999),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, workerres) = tokio::join!(updates, worker.run_until_done());
    workerres.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result.counter, 999);
}

#[tokio::test]
async fn test_typed_query() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            100,
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_query", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let querier = async {
        let counter = handle
            .query(
                InteractionWorkflow::get_counter,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(counter, 0);
        handle
            .signal(
                InteractionWorkflow::increment,
                50,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        let counter = handle
            .query(
                InteractionWorkflow::get_counter,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(counter, 50);
        handle
            .signal(
                InteractionWorkflow::increment,
                50,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(querier, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result.counter, 100);
}

#[tokio::test]
async fn test_update_validation() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            42,
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_validation", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let updater = async {
        let old_value = handle
            .execute_update(
                InteractionWorkflow::validated_set,
                10,
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(old_value, 0);

        let result = handle
            .execute_update(
                InteractionWorkflow::validated_set,
                -5,
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("non-negative"));

        let old_value = handle
            .execute_update(
                InteractionWorkflow::validated_set,
                42,
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(old_value, 10);
    };

    let (_, worker_res) = tokio::join!(updater, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result.counter, 42);
}

#[tokio::test]
async fn test_async_signal() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            100,
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_async_signal", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let signaler = async {
        // Send async signal that adds 20 and waits for counter >= 50
        handle
            .signal(
                InteractionWorkflow::increment_and_wait,
                (20, 50),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        // Send another signal to reach the target (20 + 30 = 50)
        handle
            .signal(
                InteractionWorkflow::increment,
                30,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        // Now send final signal to complete the workflow (50 + 50 = 100)
        handle
            .signal(
                InteractionWorkflow::increment,
                50,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(signaler, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result.counter, 100);
    assert!(result.log.contains(&"async signal done".to_owned()));
}

#[tokio::test]
async fn test_fallible_query() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            100,
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_fallible_query", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let querier = async {
        let result = handle
            .query(
                InteractionWorkflow::get_counter_checked,
                10,
                WorkflowQueryOptions::default(),
            )
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("below minimum"));
        handle
            .signal(
                InteractionWorkflow::increment,
                50,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        let counter = handle
            .query(
                InteractionWorkflow::get_counter_checked,
                10,
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(counter, 50);
        handle
            .signal(
                InteractionWorkflow::increment,
                50,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(querier, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result.counter, 100);
}

#[tokio::test]
async fn test_untyped_signal_query_update() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            100,
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_untyped", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let pc = PayloadConverter::serde_json();

    let interactions = async {
        handle
            .signal(
                UntypedSignal::new("increment"),
                RawValue::from_value(&25i32, &pc),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        let counter_raw = handle
            .query(
                UntypedQuery::new("get_counter"),
                RawValue::from_value(&(), &pc),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        let counter: i32 = counter_raw.to_value(&pc);
        assert_eq!(counter, 25);

        let old_raw = handle
            .execute_update(
                UntypedUpdate::new("set_counter"),
                RawValue::from_value(&50i32, &pc),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        let old_value: i32 = old_raw.to_value(&pc);
        assert_eq!(old_value, 25);

        handle
            .signal(
                UntypedSignal::new("increment"),
                RawValue::from_value(&50i32, &pc),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(interactions, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result.counter, 100);
}

#[tokio::test]
async fn test_typed_signal_query_update() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let wfid = format!("{}_typed", starter.get_task_queue());
    worker
        .submit_workflow(
            InteractionWorkflow::run,
            100,
            WorkflowStartOptions::new(task_queue.clone(), wfid.clone()).build(),
        )
        .await
        .unwrap();
    let handle = starter
        .get_client()
        .await
        .get_workflow_handle::<InteractionWorkflow>(wfid);

    let interactions = async {
        handle
            .signal(
                InteractionWorkflow::increment,
                25,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        let counter = handle
            .query(
                InteractionWorkflow::get_counter,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(counter, 25);

        let old_value = handle
            .execute_update(
                InteractionWorkflow::set_counter,
                50,
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(old_value, 25);

        handle
            .signal(
                InteractionWorkflow::increment,
                50,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(interactions, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result.counter, 100);
}

#[workflow]
#[derive(Default)]
struct ImmediatelyCompletingWf;

#[workflow_methods]
impl ImmediatelyCompletingWf {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

/// Verify that `static_summary` and `static_details` set in `WorkflowStartOptions` are
/// stored on the server and visible via `DescribeWorkflowExecution`.
#[tokio::test]
async fn static_summary_and_details_visible_after_start() {
    let wf_name = "static_summary_and_details_visible_after_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<ImmediatelyCompletingWf>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ImmediatelyCompletingWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name)
                .static_summary("my static summary")
                .static_details("my static details")
                .build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();

    let description = handle
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap();

    let summary = description.static_summary().expect("summary present");
    assert_eq!(summary, "my static summary",);

    let details = description.static_details().expect("details present");
    assert_eq!(details, "my static details",);
}
