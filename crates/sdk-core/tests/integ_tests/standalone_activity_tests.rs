use crate::common::CoreWfStarter;
use futures_util::{FutureExt, StreamExt, pin_mut, stream};
use std::{
    collections::HashSet,
    panic,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Arc,
    time::Duration,
};
use temporalio_client::{
    ActivityCancelOptions, ActivityDescribeOptions, ActivityExecutionInfoLike,
    ActivityExecutionStatus, ActivityStartOptions, ActivityStartOptionsBuilder,
    ActivityTerminateOptions, Client, NamespacedClient, errors::ActivityResultError,
};
use temporalio_common::ActivityError;
use temporalio_macros::activities;
use temporalio_sdk::activities::ActivityContext;
use uuid::Uuid;

const TASK_QUEUE_PREFIX: &str = "standalone_activity_tests";

struct Activities;

#[activities]
impl Activities {
    #[activity]
    async fn echo(_ctx: ActivityContext, e: String) -> Result<String, ActivityError> {
        Ok(e)
    }

    #[activity]
    async fn wait_for_cancel(self: Arc<Self>, ctx: ActivityContext) -> Result<(), ActivityError> {
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! { biased;
                _ = ctx.cancelled() => return Err(ActivityError::Cancelled {details: None}),
                _ = ticker.tick() => { let _ = ctx.record_heartbeat(()).await; },
            }
        }
    }
}

async fn run_test(test: impl AsyncFnOnce(Client, String)) {
    let mut starter = CoreWfStarter::new(TASK_QUEUE_PREFIX);
    starter.sdk_config.register_activities(Activities);
    let mut worker = starter.worker().await;
    let client = starter.get_core_client().await;
    let shutdown_handle = worker.inner_mut().shutdown_handle();

    let worker_fut = worker.inner_mut().run();
    let test_fut = async {
        let result = AssertUnwindSafe(test(client, starter.sdk_config.task_queue.clone()))
            .catch_unwind()
            .await;
        shutdown_handle();
        result
    };
    pin_mut!(worker_fut);
    pin_mut!(test_fut);

    tokio::select! {
        test_result = &mut test_fut => {
            let worker_result = worker_fut.await;
            if let Err(panic) = test_result {
                resume_unwind(panic);
            }
            worker_result.unwrap();
        },
        worker_result = &mut worker_fut => {
            worker_result.unwrap();
            if let Err(panic) = test_fut.await {
                resume_unwind(panic);
            }
        }
    }
}

fn test_options(task_queue: String) -> ActivityStartOptionsBuilder {
    ActivityStartOptions::with_schedule_to_close_timeout(
        task_queue,
        Uuid::new_v4(),
        Duration::from_secs(60),
    )
}

#[tokio::test]
async fn get_result() {
    run_test(async |client, tq| {
        let options = test_options(tq).build();
        let arg = "Hello";

        let handle = client
            .start_activity(Activities::echo, arg.into(), options.clone())
            .await
            .unwrap();
        assert_eq!(handle.activity_id(), options.id);
        assert!(handle.run_id().is_some());
        assert_eq!(handle.result().await.unwrap(), arg);

        let new_handle = client.get_activity_handle(
            Activities::echo,
            handle.activity_id(),
            handle.run_id().map(Into::into),
        );
        assert_eq!(new_handle.result().await.unwrap(), arg);

        let untyped_handle = client
            .get_untyped_activity_handle(handle.activity_id(), handle.run_id().map(Into::into));
        assert_eq!(
            untyped_handle
                .result()
                .await
                .unwrap()
                .to_value::<String>(client.data_converter().payload_converter()),
            arg
        );

        let wrong_run_id = loop {
            let uuid = Some(Uuid::new_v4().to_string());
            if uuid.as_deref() != handle.run_id() {
                break uuid;
            }
        };

        let handle_wrong_run_id =
            client.get_activity_handle(Activities::echo, handle.activity_id(), wrong_run_id);
        assert_matches!(
            handle_wrong_run_id.result().await,
            Err(ActivityResultError::NotFound(_))
        );

        let handle_no_run_id =
            client.get_activity_handle(Activities::echo, handle.activity_id(), None);
        assert_eq!(handle_no_run_id.result().await.unwrap(), arg);
    })
    .await;
}

#[tokio::test]
async fn describe() {
    run_test(async |client, tq| {
        let options = test_options(tq).build();
        let arg = "Hello";

        let handle = client
            .start_activity(Activities::echo, arg.into(), options.clone())
            .await
            .unwrap();
        let result = handle.result().await.unwrap();

        let desc = handle
            .describe(
                ActivityDescribeOptions::builder()
                    .include_input(true)
                    .include_outcome(true)
                    .build(),
            )
            .await
            .unwrap();

        assert_eq!(desc.activity_id(), options.id);
        assert_eq!(Some(desc.activity_run_id()), handle.run_id());
        assert_eq!(desc.status(), ActivityExecutionStatus::Completed);
        assert_eq!(desc.input().await.unwrap(), Some(arg.to_string()));
        assert_eq!(desc.outcome().await.unwrap().unwrap().unwrap(), result);
    })
    .await;
}

#[tokio::test]
async fn cancel() {
    run_test(async |client, tq| {
        let reason = "test cancel";
        let handle = client
            .start_activity(Activities::wait_for_cancel, (), test_options(tq).build())
            .await
            .unwrap();
        handle
            .cancel(ActivityCancelOptions::builder().reason(reason).build())
            .await
            .unwrap();

        assert_matches!(
            handle.result().await,
            Err(ActivityResultError::Cancelled { .. })
        );
        let desc = handle.describe(Default::default()).await.unwrap();
        assert_eq!(desc.status(), ActivityExecutionStatus::Canceled);
        assert_eq!(desc.canceled_reason(), Some(reason));
    })
    .await;
}

#[tokio::test]
async fn terminate() {
    run_test(async |client, tq| {
        let reason = "test terminate";
        let handle = client
            .start_activity(Activities::wait_for_cancel, (), test_options(tq).build())
            .await
            .unwrap();
        handle
            .terminate(ActivityTerminateOptions::builder().reason(reason).build())
            .await
            .unwrap();

        assert_matches!(handle.result().await, Err(ActivityResultError::Terminated));
        let desc = handle.describe(Default::default()).await.unwrap();
        assert_eq!(desc.status(), ActivityExecutionStatus::Terminated);
    })
    .await;
}

#[tokio::test]
async fn list_and_count() {
    run_test(async |client, tq| {
        let query = format!("TaskQueue='{tq}'");

        let started_activity_ids: HashSet<_> = stream::iter(0..3)
            .then(async |_| {
                client
                    .start_activity(
                        Activities::echo,
                        "Hello".into(),
                        test_options(tq.clone()).build(),
                    )
                    .await
                    .unwrap()
                    .activity_id()
                    .to_string()
            })
            .collect()
            .await;

        // in loop because of eventual consistency
        loop {
            let count = client
                .count_activities(query.clone(), Default::default())
                .await
                .unwrap();
            if count.count() == started_activity_ids.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let list_activity_ids: HashSet<_> = client
            .list_activities(query.clone(), Default::default())
            .map(|a| a.unwrap().activity_id().to_string())
            .collect()
            .await;
        assert_eq!(list_activity_ids, started_activity_ids);
    })
    .await;
}
