use crate::common::CoreWfStarter;
use temporalio_client::{
    NamespacedClient, WorkflowDescribeOptions, WorkflowExecutionInfo, WorkflowStartOptions,
};
use temporalio_common::protos::{
    coresdk::FromJsonPayloadExt,
    temporal::api::{
        command::v1::{Command, command},
        enums::v1::EventType,
    },
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{MemoValue, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    test_help::MockPollCfg,
};
use uuid::Uuid;

static FIELD_A: &str = "cat_name";
static FIELD_B: &str = "cute_level";
static REMOVED_FIELD: &str = "temporary";

#[workflow]
#[derive(Default)]
struct MemoUpserter;

#[workflow_methods]
impl MemoUpserter {
    #[run(name = "can_upsert_memo")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.upsert_memo([
            (FIELD_A, Some(MemoValue::new("enchi".to_string()))),
            (FIELD_B, Some(MemoValue::new(9001))),
            (REMOVED_FIELD, Some(MemoValue::new(true))),
        ])?;
        assert_eq!(ctx.memo().get::<bool>(REMOVED_FIELD)?, Some(true));

        ctx.upsert_memo([(REMOVED_FIELD, None)])?;
        assert!(!ctx.memo().contains_key(REMOVED_FIELD));
        Ok(())
    }
}

#[tokio::test]
async fn sends_modify_wf_props() {
    let wf_name = "can_upsert_memo";
    let wf_id = Uuid::new_v4();
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<MemoUpserter>()
        .unwrap();
    let mut worker = starter.worker().await;
    let task_queue = starter.get_task_queue().to_owned();
    let run_id = worker
        .submit_wf(
            wf_name,
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_string()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let client = starter.get_core_client().await;
    let description = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: wf_id.to_string(),
        run_id: Some(run_id),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone())
    .describe(WorkflowDescribeOptions::default())
    .await
    .unwrap();
    assert_eq!(
        description.memo().get::<String>(FIELD_A).unwrap(),
        Some("enchi".to_string())
    );
    assert_eq!(
        description.memo().get::<usize>(FIELD_B).unwrap(),
        Some(9001)
    );
    assert!(!description.memo().contains_key(REMOVED_FIELD));
}

#[workflow]
#[derive(Default)]
struct ModifyPropsWf;

#[workflow_methods]
impl ModifyPropsWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.upsert_memo([
            ("foo", Some(MemoValue::new(1_u8))),
            ("bar", Some(MemoValue::new(2_u8))),
        ])?;
        Ok(())
    }
}

#[tokio::test]
async fn workflow_modify_props() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let (k1, k2) = ("foo", "bar");

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|wft| {
            assert_matches!(
                wft.commands.as_slice(),
                [Command {
                    attributes: Some(
                        command::Attributes::ModifyWorkflowPropertiesCommandAttributes(msg)
                    ),
                    ..
                }, ..] => {
                    let fields = &msg.upserted_memo.as_ref().unwrap().fields;
                    let payload1 = fields.get(k1).unwrap();
                    let payload2 = fields.get(k2).unwrap();
                    assert_eq!(u8::from_json_payload(payload1).unwrap(), 1);
                    assert_eq!(u8::from_json_payload(payload2).unwrap(), 2);
                    assert_eq!(fields.len(), 2);
                }
            );
        });
    });

    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options.register_workflow::<ModifyPropsWf>().unwrap();
    });
    worker.run().await.unwrap();
}
