use crate::{
    common::{get_integ_client, integ_namespace, rand_6_chars},
    integ_tests::mk_nexus_endpoint_for_client,
};
use nexus_rpc::service;
use std::time::Duration;
use temporalio_client::WorkflowStartOptions;
use temporalio_common::protos::temporal::api::{common::v1::link, enums::v1::EventType};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    Runtime, SyncWorkflowContext, Worker, WorkerOptions, WorkflowContext, WorkflowContextView,
    WorkflowResult, WorkflowTermination,
    nexus::{
        NexusContext, NexusOperationOptions, NexusServiceClientOptions, NexusServiceHandler,
        TemporalNexusClient, TemporalOperationError, TemporalOperationResult,
        temporal_nexus_service_handler,
    },
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChargeInput {
    pub(super) amount: u32,
    pub(super) wait_for_cancel: bool,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChargeOutput {
    pub(super) receipt: String,
}

#[service(name = "payment-worker-service")]
pub(crate) trait PaymentService {
    #[operation(name = "quote-payment")]
    fn quote(input: ChargeInput) -> ChargeOutput;

    #[operation(name = "charge-payment")]
    fn charge(input: ChargeInput) -> ChargeOutput;
}

pub(super) struct PaymentHandlers {
    pub(super) task_queue: String,
}

#[temporal_nexus_service_handler(service = PaymentService)]
impl PaymentHandlers {
    #[temporal_operation]
    async fn quote(
        &self,
        ctx: NexusContext,
        _client: TemporalNexusClient,
        input: ChargeInput,
    ) -> Result<TemporalOperationResult<ChargeOutput>, TemporalOperationError> {
        assert!(!ctx.info().request_id.is_empty());
        Ok(TemporalOperationResult::Sync(ChargeOutput {
            receipt: format!("quote-{}", input.amount),
        }))
    }

    #[temporal_operation]
    async fn charge(
        &self,
        ctx: NexusContext,
        client: TemporalNexusClient,
        input: ChargeInput,
    ) -> Result<TemporalOperationResult<ChargeOutput>, TemporalOperationError> {
        client
            .start_workflow(
                ChargeWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    self.task_queue.clone(),
                    format!("charge-{}", ctx.info().request_id),
                )
                .build(),
            )
            .await
    }
}

#[workflow]
pub(super) struct ChargeWorkflow {
    input: ChargeInput,
}

#[workflow_methods]
impl ChargeWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, input: ChargeInput) -> Self {
        Self { input }
    }

    #[run]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<ChargeOutput> {
        if ctx.state(|wf| wf.input.wait_for_cancel) {
            ctx.cancelled().await;
            return Err(WorkflowTermination::cancelled());
        }
        Ok(ChargeOutput {
            receipt: ctx.state(|wf| format!("charged-{}", wf.input.amount)),
        })
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub(crate) enum Outcome {
    Sync,
    Async,
    Cancel,
}

#[workflow]
struct PaymentCaller {
    endpoint: String,
    outcome: Outcome,
}

#[workflow_methods]
impl PaymentCaller {
    #[init]
    fn new(_ctx: &WorkflowContextView, (endpoint, outcome): (String, Outcome)) -> Self {
        Self { endpoint, outcome }
    }

    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let client = ctx.nexus_client(
            PaymentService,
            NexusServiceClientOptions::new(ctx.state(|wf| wf.endpoint.clone())).build(),
        );
        let outcome = ctx.state(|wf| wf.outcome);
        let input = ChargeInput {
            amount: 123,
            wait_for_cancel: matches!(outcome, Outcome::Cancel),
        };
        let options = NexusOperationOptions::builder()
            .schedule_to_close_timeout(Duration::from_secs(30))
            .build();
        let handle = match outcome {
            Outcome::Sync => client.start(PaymentService::quote, input, options).await?,
            Outcome::Async | Outcome::Cancel => {
                client.start(PaymentService::charge, input, options).await?
            }
        };
        assert_eq!(
            handle.operation_token().is_none(),
            matches!(outcome, Outcome::Sync)
        );
        if matches!(outcome, Outcome::Cancel) {
            handle.cancel();
            let error = handle.result().await.unwrap_err();
            assert!(error.as_cancelled().is_some());
        } else {
            let result: ChargeOutput = handle.result().await?;
            assert_eq!(
                result.receipt,
                if matches!(outcome, Outcome::Sync) {
                    "quote-123"
                } else {
                    "charged-123"
                },
            );
        }
        Ok(())
    }
}

#[temporalio_macros::cloud_test_exclusion(
    crate::CloudTestExclusionReason::RequiresCloudProvisioning,
    "Creates a Nexus endpoint through the Operator Service, unavailable to isolated Cloud namespace credentials."
)]
#[rstest::rstest]
#[tokio::test]
async fn nexus_worker(#[values(Outcome::Sync, Outcome::Async, Outcome::Cancel)] outcome: Outcome) {
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let client = get_integ_client(integ_namespace(), None).await;
    let task_queue = format!("nexus-worker-{}", rand_6_chars());
    let options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<PaymentCaller>()
        .unwrap()
        .register_workflow::<ChargeWorkflow>()
        .unwrap()
        .register_nexus_service(PaymentHandlers {
            task_queue: task_queue.clone(),
        })
        .unwrap()
        .build();
    let endpoint = mk_nexus_endpoint_for_client(&client, &task_queue).await;
    let worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let caller = client
        .start_workflow(
            PaymentCaller::run,
            (endpoint, outcome),
            WorkflowStartOptions::new(task_queue.clone(), task_queue)
                .execution_timeout(Duration::from_secs(60))
                .build(),
        )
        .await
        .unwrap();
    worker
        .run_until(async { caller.get_result(Default::default()).await.unwrap() })
        .await
        .unwrap();
    let history = caller.fetch_history(Default::default());
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<PaymentCaller>()
            .unwrap()
            .build(),
    )
    .unwrap();
    replayer.replay_workflow(history).await.unwrap();
}

#[workflow]
#[derive(Default)]
struct SignalTarget {
    received: bool,
}

#[workflow_methods]
impl SignalTarget {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|wf| wf.received).await?;
        Ok(())
    }

    #[signal]
    fn finish(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.received = true;
    }
}

#[temporalio_macros::cloud_test_exclusion(
    crate::CloudTestExclusionReason::RequiresCloudProvisioning,
    "Creates a Nexus endpoint through the Operator Service, unavailable to isolated Cloud namespace credentials."
)]
#[tokio::test]
async fn nexus_worker_signal_links() {
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let client = get_integ_client(integ_namespace(), None).await;
    let task_queue = format!("nexus-signal-{}", rand_6_chars());
    let caller_id = format!("{task_queue}-caller");
    let target_id = format!("{task_queue}-target");
    let target = client
        .start_workflow(
            SignalTarget::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), target_id.clone())
                .execution_timeout(Duration::from_secs(60))
                .build(),
        )
        .await
        .unwrap();
    let service = NexusServiceHandler::new(PaymentService)
        .operation(PaymentService::quote, move |_, client, input| {
            let target = client
                .client()
                .get_workflow_handle::<signal_target::Run>(&target_id);
            async move {
                tokio::spawn(async move {
                    target
                        .signal(SignalTarget::finish, (), Default::default())
                        .await
                })
                .await
                .unwrap()?;
                Ok(TemporalOperationResult::Sync(ChargeOutput {
                    receipt: format!("quote-{}", input.amount),
                }))
            }
        })
        .unwrap();
    let options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<PaymentCaller>()
        .unwrap()
        .register_workflow::<SignalTarget>()
        .unwrap()
        .register_nexus_service(service)
        .unwrap()
        .build();
    let endpoint = mk_nexus_endpoint_for_client(&client, &task_queue).await;
    let worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let caller = client
        .start_workflow(
            PaymentCaller::run,
            (endpoint, Outcome::Sync),
            WorkflowStartOptions::new(task_queue, caller_id.clone())
                .execution_timeout(Duration::from_secs(60))
                .build(),
        )
        .await
        .unwrap();
    worker
        .run_until(async {
            caller.get_result(Default::default()).await.unwrap();
            target.get_result(Default::default()).await.unwrap();
        })
        .await
        .unwrap();

    let history = target
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let signal = history
        .iter()
        .find(|event| event.event_type() == EventType::WorkflowExecutionSignaled)
        .unwrap();
    let [signal_link] = signal.links.as_slice() else {
        panic!("expected one caller link, got {:?}", signal.links);
    };
    let Some(link::Variant::WorkflowEvent(event)) = &signal_link.variant else {
        panic!("expected a link to the calling workflow");
    };
    assert_eq!(event.workflow_id, caller_id);
    let Some(link::workflow_event::Reference::EventRef(reference)) = &event.reference else {
        panic!("expected a link to the Nexus scheduled event");
    };
    assert_eq!(reference.event_type(), EventType::NexusOperationScheduled);
}
