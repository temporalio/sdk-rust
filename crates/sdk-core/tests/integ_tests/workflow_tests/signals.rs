use crate::common::{ActivationAssertionsInterceptor, CoreWfStarter, build_fake_sdk};
use std::collections::HashMap;
use temporalio_client::{WorkflowStartOptions, WorkflowStartSignal};
use temporalio_common::protos::{
    coresdk::{
        AsJsonPayloadExt, IntoPayloadsExt,
        workflow_activation::{
            ResolveSignalExternalWorkflow, WorkflowActivationJob, workflow_activation_job,
        },
    },
    temporal::api::{
        command::v1::{Command, command},
        common::v1::Payload,
        enums::v1::{CommandType, EventType},
    },
};
use temporalio_sdk_core::replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder};

use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, ChildWorkflowOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult,
};
use temporalio_sdk_core::test_help::MockPollCfg;
use uuid::Uuid;

const SIGNAME: &str = "signame";
const RECEIVER_WFID: &str = "sends-signal-signal-receiver";

#[workflow]
#[derive(Default)]
struct SignalSender;

#[workflow_methods]
impl SignalSender {
    #[run(name = "sender")]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        (run_id, expect_failure): (String, bool),
    ) -> WorkflowResult<()> {
        let handle = ctx.external_workflow(RECEIVER_WFID, Some(run_id));
        let sigres = handle
            .signal(SignalReceiver::handle_signal, "hi!".into())
            .await;
        if expect_failure {
            assert!(sigres.is_err());
        } else {
            sigres.unwrap();
        }
        Ok(())
    }
}

#[tokio::test]
async fn sends_signal_to_missing_wf() {
    let wf_name = "sends_signal_to_missing_wf";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<SignalSender>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            SignalSender::run,
            (Uuid::new_v4().to_string(), true),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct SignalReceiver {
    received: bool,
}

#[workflow_methods]
impl SignalReceiver {
    #[run(name = "receiver")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.received).await;
        Ok(())
    }

    #[signal(name = "signame")]
    fn handle_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: String) {
        assert_eq!(input, "hi!");
        self.received = true;
    }
}

#[workflow]
#[derive(Default)]
struct SignalWithCreateWfReceiver {
    received: bool,
}

#[workflow_methods]
impl SignalWithCreateWfReceiver {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.received).await;
        Ok(())
    }

    #[signal(name = "signame")]
    fn handle_signal(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: String) {
        assert_eq!(input, "tada");
        let headers = ctx.headers();
        assert_eq!(
            *headers.get("tupac").expect("tupac header exists"),
            b"shakur".into()
        );
        self.received = true;
    }
}

#[tokio::test]
async fn sends_signal_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_to_other_wf");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<SignalSender>().unwrap();
    worker.register_workflow::<SignalReceiver>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let receiver_run_id = worker
        .submit_wf(
            "receiver",
            vec![().as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue.clone(), RECEIVER_WFID).build(),
        )
        .await
        .unwrap();
    worker
        .submit_wf(
            "sender",
            vec![(receiver_run_id, false).as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue, "sends-signal-sender").build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn sends_signal_with_create_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_with_create_wf");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<SignalWithCreateWfReceiver>()
        .unwrap();

    let client = starter.get_client().await;
    let mut header: HashMap<String, Payload> = HashMap::new();
    header.insert("tupac".into(), "shakur".into());
    let task_queue = worker.inner_mut().task_queue().to_string();
    let start_signal = WorkflowStartSignal::new(SIGNAME)
        .maybe_input(vec!["tada".to_string().as_json_payload().unwrap()].into_payloads())
        .maybe_header(Some(header.into()))
        .build();
    let options = WorkflowStartOptions::new(task_queue, "sends_signal_with_create_wf")
        .start_signal(start_signal)
        .build();
    let handle = client
        .start_workflow(SignalWithCreateWfReceiver::run, (), options)
        .await
        .expect("request succeeds.qed");

    worker.expect_workflow_completion(
        "sends_signal_with_create_wf",
        Some(handle.run_id().unwrap().to_string()),
    );
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct SignalsChild;

#[workflow_methods]
impl SignalsChild {
    #[run(name = "child_signaler")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started_child = ctx
            .start_child_workflow(
                ChildSignalReceiver::run,
                (),
                ChildWorkflowOptions {
                    workflow_id: "my_precious_child".to_string(),
                    ..Default::default()
                },
            )
            .await?;
        started_child
            .signal(ChildSignalReceiver::handle_signal, "hi!".into())
            .await?;
        started_child.result().await.expect("child wf result is ok");
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct ChildSignalReceiver {
    received: bool,
}

#[workflow_methods]
impl ChildSignalReceiver {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.received).await;
        Ok(())
    }

    #[signal]
    fn handle_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: String) {
        assert_eq!(input, "hi!");
        self.received = true;
    }
}

#[tokio::test]
async fn sends_signal_to_child() {
    let mut starter = CoreWfStarter::new("sends_signal_to_child");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<SignalsChild>().unwrap();
    worker.register_workflow::<ChildSignalReceiver>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_wf(
            "child_signaler",
            vec![().as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue, "sends-signal-to-child").build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct SignalSenderCanned;

#[workflow_methods]
impl SignalSenderCanned {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let handle = ctx.external_workflow("fake_wid", Some("fake_rid".into()));
        let res = handle
            .signal(SignalReceiver::handle_signal, "hi!".into())
            .await;
        if res.is_err() {
            Err(anyhow::anyhow!("Signal fail!").into())
        } else {
            Ok(())
        }
    }
}

#[rstest::rstest]
#[case::succeeds(false)]
#[case::fails(true)]
#[tokio::test]
async fn sends_signal(#[case] fails: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let id = t.add_signal_wf(SIGNAME, "fake_wid", "fake_rid");
    if fails {
        t.add_external_signal_failed(id);
    } else {
        t.add_external_signal_completed(id);
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(move |wft| {
                assert_matches!(wft.commands.as_slice(),
                    [Command { attributes: Some(
                        command::Attributes::SignalExternalWorkflowExecutionCommandAttributes(attrs)),..}] => {
                        assert_eq!(attrs.signal_name, SIGNAME);
                        assert_eq!(attrs.input.as_ref().unwrap().payloads[0], "hi!".to_string().as_json_payload().unwrap());
                    }
                );
            }).then(move |wft| {
                let cmds = &wft.commands;
                assert_eq!(cmds.len(), 1);
                if fails {
                    assert_eq!(cmds[0].command_type(), CommandType::FailWorkflowExecution);
                } else {
                    assert_eq!(
                        cmds[0].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                }
            });
        });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<SignalSenderCanned>().unwrap();
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct CancelsBeforeSending;

#[workflow_methods]
impl CancelsBeforeSending {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let handle = ctx.external_workflow("fake_wid", Some("fake_rid".into()));
        let sig = handle.signal(SignalReceiver::handle_signal, "hi!".into());
        sig.cancel();
        let _res = sig.await;
        Ok(())
    }
}

#[tokio::test]
async fn cancels_before_sending() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let mut aai = ActivationAssertionsInterceptor::default();
    aai.skip_one().then(move |act| {
        assert_matches!(
            &act.jobs[0],
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveSignalExternalWorkflow(
                    ResolveSignalExternalWorkflow {
                        failure: Some(c),
                        ..
                    }
                ))
            } => c.message == "Signal was cancelled before being sent"
        );
    });
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 1);
            assert_eq!(
                wft.commands[0].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.set_worker_interceptor(aai);
    worker.register_workflow::<CancelsBeforeSending>().unwrap();
    worker.run().await.unwrap();
}

#[derive(serde::Deserialize)]
struct BadSignalInput;

impl serde::Serialize for BadSignalInput {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "intentional serialization failure",
        ))
    }
}

#[workflow]
#[derive(Default)]
struct SignalSerializationFailure;

#[workflow_methods]
impl SignalSerializationFailure {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let handle = ctx.external_workflow("irrelevant", None);
        let result = handle
            .signal(SignalSerializationFailure::bad_signal, BadSignalInput)
            .await;
        Ok(result.unwrap_err().to_string())
    }

    #[signal]
    fn bad_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: BadSignalInput) {}
}

#[tokio::test]
async fn signal_serialization_failure() {
    let wf_name = "signal_serialization_failure";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<SignalSerializationFailure>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            SignalSerializationFailure::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert!(
        result.contains("intentional serialization failure"),
        "expected serialization error message, got: {result}"
    );
}

#[workflow]
#[derive(Default)]
struct CrossTypeSignalSender;

#[workflow_methods]
impl CrossTypeSignalSender {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        (run_id, workflow_id): (String, String),
    ) -> WorkflowResult<()> {
        let handle = ctx.external_workflow(workflow_id, Some(run_id));
        handle
            .signal(CrossTypeSignalReceiver::handle_signal, "hi!".into())
            .await
            .unwrap();
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct CrossTypeSignalReceiver {
    received: bool,
}

#[workflow_methods]
impl CrossTypeSignalReceiver {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.received).await;
        Ok(())
    }

    #[signal]
    fn handle_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: String) {
        assert_eq!(input, "hi!");
        self.received = true;
    }
}

#[tokio::test]
async fn external_workflow_signal() {
    let mut starter = CoreWfStarter::new("cross_type_signal_sends_successfully");
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<CrossTypeSignalSender>().unwrap();
    worker
        .register_workflow::<CrossTypeSignalReceiver>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let receiver_wfid = "cross-type-signal-receiver";
    let receiver_handle = worker
        .submit_workflow(
            CrossTypeSignalReceiver::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), receiver_wfid).build(),
        )
        .await
        .unwrap();
    let receiver_run_id = receiver_handle.run_id().unwrap();
    worker
        .submit_workflow(
            CrossTypeSignalSender::run,
            (receiver_run_id.to_owned(), receiver_wfid.to_owned()),
            WorkflowStartOptions::new(task_queue, "cross-type-signal-sender").build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
