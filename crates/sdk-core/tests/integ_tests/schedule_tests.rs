use crate::common::{NAMESPACE, eventually, get_integ_client, rand_6_chars};
use futures::TryStreamExt;
use std::time::{Duration, SystemTime};
use temporalio_client::{
    UntypedWorkflow,
    schedules::{
        CreateScheduleOptions, ListSchedulesOptions, ScheduleAction, ScheduleBackfill,
        ScheduleCalendarSpec, ScheduleOverlapPolicy, ScheduleSpec,
    },
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};

async fn test_client() -> temporalio_client::Client {
    get_integ_client(NAMESPACE.to_string(), None).await
}

#[workflow]
#[derive(Default)]
struct ScheduleInputWorkflow;

#[workflow_methods]
impl ScheduleInputWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        Ok(input)
    }
}

/// Pull the `NewWorkflowExecutionInfo` out of a described schedule's start-workflow action.
fn start_workflow_info(
    desc: &temporalio_client::schedules::ScheduleDescription,
) -> &temporalio_common::protos::temporal::api::workflow::v1::NewWorkflowExecutionInfo {
    let raw_action = desc
        .raw()
        .schedule
        .as_ref()
        .expect("schedule present")
        .action
        .as_ref()
        .expect("action present")
        .action
        .as_ref()
        .expect("action oneof present");
    #[allow(irrefutable_let_patterns)]
    let temporalio_common::protos::temporal::api::schedule::v1::schedule_action::Action::StartWorkflow(
        info,
    ) = raw_action
    else {
        panic!("expected StartWorkflow action")
    };
    info
}

#[tokio::test]
async fn create_and_describe_schedule() {
    let client = test_client().await;
    let schedule_id = format!("sched-create-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .note("created paused for testing")
                .build(),
        )
        .await
        .unwrap();

    assert_eq!(handle.schedule_id(), schedule_id);

    let desc = handle.describe().await.unwrap();
    assert!(desc.paused());
    assert_eq!(desc.note(), Some("created paused for testing"));
    assert!(!desc.conflict_token().is_empty());

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn create_schedule_with_calendar_spec() {
    let client = test_client().await;
    let schedule_id = format!("sched-calendar-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_calendar(
                    ScheduleCalendarSpec::builder().hour("2-7").build(),
                ))
                .paused(true)
                .build(),
        )
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();
    let raw_spec = desc.raw().schedule.as_ref().unwrap().spec.as_ref().unwrap();
    assert_eq!(raw_spec.structured_calendar.len(), 1);

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn create_schedule_with_trigger_immediately() {
    let client = test_client().await;
    let schedule_id = format!("sched-trigimm-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(86400)))
                .trigger_immediately(true)
                .build(),
        )
        .await
        .unwrap();

    let desc = eventually(
        || {
            let handle = client.get_schedule_handle(&schedule_id);
            async move {
                let desc = handle.describe().await.map_err(|e| e.to_string())?;
                if desc.action_count() > 0 {
                    Ok(desc)
                } else {
                    Err("action_count still 0".to_string())
                }
            }
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    assert!(desc.action_count() >= 1);

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn pause_and_unpause_schedule() {
    let client = test_client().await;
    let schedule_id = format!("sched-pause-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .build(),
        )
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();
    assert!(!desc.paused());

    handle.pause(Some("pausing for maintenance")).await.unwrap();
    let desc = handle.describe().await.unwrap();
    assert!(desc.paused());
    assert_eq!(desc.note(), Some("pausing for maintenance"));

    handle.unpause(Some("maintenance complete")).await.unwrap();
    let desc = handle.describe().await.unwrap();
    assert!(!desc.paused());
    assert_eq!(desc.note(), Some("maintenance complete"));

    // Verify None uses default note
    handle.pause(None::<&str>).await.unwrap();
    let desc = handle.describe().await.unwrap();
    assert!(desc.paused());
    assert!(desc.note().is_some());

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn update_schedule() {
    let client = test_client().await;
    let schedule_id = format!("sched-update-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .note("before update")
                .build(),
        )
        .await
        .unwrap();

    handle
        .update(|u| {
            u.set_note("updated notes");
        })
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();
    assert_eq!(desc.note(), Some("updated notes"));

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn trigger_schedule() {
    let client = test_client().await;
    let schedule_id = format!("sched-trigger-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .build(),
        )
        .await
        .unwrap();

    handle
        .trigger(ScheduleOverlapPolicy::AllowAll)
        .await
        .unwrap();

    let desc = eventually(
        || {
            let handle = client.get_schedule_handle(&schedule_id);
            async move {
                let desc = handle.describe().await.map_err(|e| e.to_string())?;
                if desc.action_count() > 0 {
                    Ok(desc)
                } else {
                    Err("action_count still 0".to_string())
                }
            }
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    assert!(desc.action_count() >= 1);

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn backfill_schedule() {
    let client = test_client().await;
    let schedule_id = format!("sched-backfill-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .build(),
        )
        .await
        .unwrap();

    let now = SystemTime::now();
    let two_hours_ago = now - Duration::from_secs(7200);
    handle
        .backfill([ScheduleBackfill::new(two_hours_ago, now)
            .overlap_policy(ScheduleOverlapPolicy::AllowAll)
            .build()])
        .await
        .unwrap();

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn delete_schedule() {
    let client = test_client().await;
    let schedule_id = format!("sched-delete-{}", rand_6_chars());

    client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .build(),
        )
        .await
        .unwrap();

    let handle = client.get_schedule_handle(&schedule_id);
    handle.delete().await.unwrap();

    let err = handle.describe().await.unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("NotFound"));
}

#[tokio::test]
async fn get_schedule_handle_for_existing_schedule() {
    let client = test_client().await;
    let schedule_id = format!("sched-handle-{}", rand_6_chars());

    client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .build(),
        )
        .await
        .unwrap();

    let handle = client.get_schedule_handle(&schedule_id);
    let desc = handle.describe().await.unwrap();
    assert!(desc.paused());

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn list_schedules() {
    let client = test_client().await;
    let suffix = rand_6_chars();
    let num_schedules = 3;
    let mut schedule_ids = Vec::new();

    for i in 0..num_schedules {
        let schedule_id = format!("sched-list-{suffix}-{i}");
        schedule_ids.push(schedule_id.clone());
        client
            .create_schedule(
                &schedule_id,
                CreateScheduleOptions::builder()
                    .action(ScheduleAction::start_workflow(
                        ScheduleInputWorkflow::run,
                        String::new(),
                        "my-task-queue",
                        format!("wf-{}", rand_6_chars()),
                    ))
                    .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                    .paused(true)
                    .build(),
            )
            .await
            .unwrap();
    }

    let found_ids = eventually(
        || {
            let client = client.clone();
            let expected = schedule_ids.clone();
            async move {
                let schedules: Vec<_> = client
                    .list_schedules(ListSchedulesOptions::default())
                    .try_collect()
                    .await
                    .map_err(|e| e.to_string())?;
                let found: Vec<_> = schedules
                    .iter()
                    .filter(|s| expected.contains(&s.schedule_id().to_string()))
                    .map(|s| s.schedule_id().to_string())
                    .collect();
                if found.len() == expected.len() {
                    Ok(found)
                } else {
                    Err(format!(
                        "Expected {} schedules, found {}",
                        expected.len(),
                        found.len()
                    ))
                }
            }
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    assert_eq!(found_ids.len(), num_schedules);

    for id in &schedule_ids {
        client.get_schedule_handle(id).delete().await.unwrap();
    }
}

#[tokio::test]
async fn describe_accessors_match_created_values() {
    let client = test_client().await;
    let schedule_id = format!("sched-accessors-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    String::new(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .note("accessors test")
                .build(),
        )
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();

    assert!(desc.paused());
    assert_eq!(desc.note(), Some("accessors test"));
    assert_eq!(desc.action_count(), 0);
    assert_eq!(desc.missed_catchup_window(), 0);
    assert_eq!(desc.overlap_skipped(), 0);
    assert!(desc.recent_actions().is_empty());
    assert!(desc.running_actions().is_empty());
    assert!(desc.create_time().is_some());

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn create_schedule_with_workflow_input() {
    use temporalio_common::{
        data_converters::RawValue, protos::temporal::api::common::v1::Payload,
    };

    let client = test_client().await;
    let schedule_id = format!("sched-with-input-{}", rand_6_chars());

    let expected_payload = Payload {
        metadata: [("encoding".to_string(), b"json/plain".to_vec())]
            .into_iter()
            .collect(),
        data: b"\"hello\"".to_vec(),
        ..Default::default()
    };

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    UntypedWorkflow::new("MyWorkflow"),
                    RawValue::new(vec![expected_payload.clone()]),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .build(),
        )
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();
    assert!(desc.paused());

    let wf_info = start_workflow_info(&desc);
    let stored_payloads = wf_info.input.as_ref().expect("input should be present");
    assert_eq!(stored_payloads.payloads, vec![expected_payload]);

    handle.delete().await.unwrap();
}

#[tokio::test]
async fn schedule_action_start_workflow_encodes_typed_input() {
    let client = test_client().await;
    let schedule_id = format!("sched-typed-input-{}", rand_6_chars());

    let handle = client
        .create_schedule(
            &schedule_id,
            CreateScheduleOptions::builder()
                .action(ScheduleAction::start_workflow(
                    ScheduleInputWorkflow::run,
                    "hello".to_string(),
                    "my-task-queue",
                    format!("wf-{}", rand_6_chars()),
                ))
                .spec(ScheduleSpec::from_interval(Duration::from_secs(3600)))
                .paused(true)
                .build(),
        )
        .await
        .unwrap();

    let desc = handle.describe().await.unwrap();
    let wf_info = start_workflow_info(&desc);
    assert_eq!(
        wf_info.workflow_type.as_ref().unwrap().name,
        "ScheduleInputWorkflow"
    );
    let stored = wf_info.input.as_ref().expect("input should be present");
    assert_eq!(stored.payloads.len(), 1);
    // Default data converter encodes the String as json/plain.
    assert_eq!(stored.payloads[0].data, b"\"hello\"");

    handle.delete().await.unwrap();
}
