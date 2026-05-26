use crate::common::{CoreWfStarter, SEARCH_ATTR_INT, SEARCH_ATTR_TXT, build_fake_sdk};
use assert_matches::assert_matches;
use std::{collections::HashMap, time::Duration};
use temporalio_client::{
    UntypedWorkflow, WorkflowDescribeOptions, WorkflowGetResultOptions, WorkflowStartOptions,
};
use temporalio_common::{
    protos::{
        coresdk::{AsJsonPayloadExt, FromJsonPayloadExt},
        temporal::api::{
            command::v1::{Command, command},
            common::v1::Payload,
            enums::v1::EventType,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult, WorkflowTermination};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    test_help::MockPollCfg,
};
use uuid::Uuid;

#[workflow]
#[derive(Default)]
struct SearchAttrUpdater;

#[workflow_methods]
impl SearchAttrUpdater {
    #[run(name = "sends_upsert_search_attrs")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let mut int_val = ctx
            .search_attributes()
            .indexed_fields
            .get(SEARCH_ATTR_INT)
            .cloned()
            .unwrap_or_default();
        let orig_val = int_val.data[0];
        int_val.data[0] += 1;
        ctx.upsert_search_attributes([
            (SEARCH_ATTR_TXT.to_string(), "goodbye".as_json_payload()?),
            (SEARCH_ATTR_INT.to_string(), int_val),
        ]);
        if orig_val == 49 {
            Err(WorkflowTermination::continue_as_new(Default::default()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn sends_upsert() {
    let wf_name = "sends_upsert_search_attrs";
    let wf_id = Uuid::new_v4();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker.register_workflow::<SearchAttrUpdater>().unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_wf(
            wf_name,
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_string())
                .search_attributes(HashMap::from([
                    (
                        SEARCH_ATTR_TXT.to_string(),
                        "hello".as_json_payload().unwrap(),
                    ),
                    (SEARCH_ATTR_INT.to_string(), 1.as_json_payload().unwrap()),
                ]))
                .execution_timeout(Duration::from_secs(4))
                .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let client = starter.get_client().await;
    let search_attrs = client
        .get_workflow_handle::<UntypedWorkflow>(wf_id.to_string())
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap()
        .raw_description
        .workflow_execution_info
        .unwrap()
        .search_attributes
        .unwrap()
        .indexed_fields;
    let txt_attr_payload = search_attrs.get(SEARCH_ATTR_TXT).unwrap();
    let int_attr_payload = search_attrs.get(SEARCH_ATTR_INT).unwrap();
    for payload in [txt_attr_payload, int_attr_payload] {
        assert!(payload.is_json_payload());
    }
    assert_eq!(
        "goodbye",
        String::from_json_payload(txt_attr_payload).unwrap()
    );
    assert_eq!(3, usize::from_json_payload(int_attr_payload).unwrap());
    let handle = client.get_workflow_handle::<UntypedWorkflow>(wf_id.to_string());
    handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .unwrap();
}

#[workflow]
#[derive(Default)]
struct UpsertTestWf;

#[workflow_methods]
impl UpsertTestWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        const K1: &str = "foo";
        const K2: &str = "bar";
        ctx.upsert_search_attributes([
            (
                String::from(K1),
                Payload {
                    data: vec![0x01],
                    ..Default::default()
                },
            ),
            (
                String::from(K2),
                Payload {
                    data: vec![0x02],
                    ..Default::default()
                },
            ),
        ]);
        Ok(())
    }
}

#[tokio::test]
async fn upsert_search_attrs_from_workflow() {
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
                [Command { attributes: Some(
                      command::Attributes::UpsertWorkflowSearchAttributesCommandAttributes(msg)
                    ), .. }, ..] => {
                    let fields = &msg.search_attributes.as_ref().unwrap().indexed_fields;
                    let payload1 = fields.get(k1).unwrap();
                    let payload2 = fields.get(k2).unwrap();
                    assert_eq!(payload1.data[0], 0x01);
                    assert_eq!(payload2.data[0], 0x02);
                    assert_eq!(fields.len(), 2);
                }
            );
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<UpsertTestWf>().unwrap();
    worker.run().await.unwrap();
}
