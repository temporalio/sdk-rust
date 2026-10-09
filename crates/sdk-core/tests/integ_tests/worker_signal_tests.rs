use std::{
    io::{self, Write},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::{ClientOptions, tonic};
use temporalio_common::{
    protos::temporal::api::workflowservice::v1::{
        PollActivityTaskQueueResponse, RespondActivityTaskCompletedResponse,
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::activities;
use temporalio_sdk::{
    Worker, WorkerOptions, WorkerRunError,
    activities::{ActivityContext, ActivityError},
    interceptors::WorkerInterceptor,
};
use temporalio_sdk_core::test_help::{MockPollCfg, build_mock_pollers, mock_worker};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    time::timeout,
};

const CHILD_TEST: &str = "integ_tests::worker_signal_tests::worker_signal_subprocess";
const CHILD_MODE: &str = "TEMPORAL_WORKER_SIGNAL_TEST_MODE";

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[tokio::test]
async fn worker_signal_run_finishes_without_signal(#[values(false, true)] validation_fails: bool) {
    let mut cfg = MockPollCfg::new(vec![], false, 0);
    cfg.make_poll_stream_interminable = true;
    if validation_fails {
        cfg.mock_client
            .expect_describe_namespace()
            .times(1)
            .returning(|| Err(tonic::Status::not_found("namespace does not exist")));
    }
    let mut mocks = build_mock_pollers(cfg);
    mocks.worker_cfg(|cfg| cfg.task_types = WorkerTaskTypes::workflow_only());
    let core = Arc::new(mock_worker(mocks));
    let options = WorkerOptions::new(core.get_config().task_queue.clone()).build();
    let mut worker =
        Worker::new_from_core_options(core, ClientOptions::new("default").build(), options)
            .unwrap();
    if !validation_fails {
        worker.shutdown_handle()();
    }
    let result = timeout(Duration::from_secs(5), worker.run_until_signal())
        .await
        .expect("worker waited for a signal after finishing");
    if validation_fails {
        assert!(matches!(result, Err(WorkerRunError::Validation(_))));
    } else {
        result.unwrap();
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[rstest::rstest]
#[tokio::test]
async fn worker_signal_shutdown(
    #[values("INT", "TERM")] signal: &str,
    #[values("drain", "cancel")] mode: &str,
) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([CHILD_TEST, "--exact", "--ignored", "--nocapture"])
        .env(CHILD_MODE, mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    timeout(Duration::from_secs(30), async {
        loop {
            let line = output
                .next_line()
                .await
                .unwrap()
                .expect("child exited before starting");
            if line.contains("activity-started") {
                break;
            }
        }
    })
    .await
    .expect("child worker did not start its activity");

    assert!(
        Command::new("kill")
            .args(["-s", signal, &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    if mode == "cancel" {
        timeout(Duration::from_secs(30), async {
            loop {
                let line = output
                    .next_line()
                    .await
                    .unwrap()
                    .expect("child exited before cancellation");
                if line.contains("activity-cancelled") {
                    break;
                }
            }
        })
        .await
        .expect("signal did not initiate worker shutdown");
    }
    assert!(
        child.try_wait().unwrap().is_none(),
        "worker exited before activity cleanup"
    );
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"finish\n")
        .await
        .unwrap();

    let (status, remaining_output) = timeout(Duration::from_secs(30), async {
        tokio::join!(child.wait(), async {
            let mut lines = Vec::new();
            while let Some(line) = output.next_line().await.unwrap() {
                lines.push(line);
            }
            lines.join("\n")
        })
    })
    .await
    .expect("worker shutdown did not complete");
    assert!(status.unwrap().success(), "{remaining_output}");
    assert!(
        remaining_output.contains("worker-shutdown-complete"),
        "{remaining_output}"
    );
}

struct SignalActivities {
    cancel: bool,
}

#[activities]
impl SignalActivities {
    #[activity]
    async fn wait_for_cleanup(self: Arc<Self>, ctx: ActivityContext) -> Result<(), ActivityError> {
        println!("activity-started");
        io::stdout().flush().unwrap();
        if self.cancel {
            ctx.cancelled().await;
            println!("activity-cancelled");
            io::stdout().flush().unwrap();
        }
        tokio::task::spawn_blocking(|| {
            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap();
            assert_eq!(input.trim(), "finish");
        })
        .await
        .unwrap();
        assert_eq!(ctx.is_cancelled(), self.cancel);
        Ok(())
    }
}

struct ShutdownAsserter(Arc<AtomicBool>);

impl WorkerInterceptor for ShutdownAsserter {
    fn on_shutdown(&self, _: &Worker) {
        assert!(
            self.0.load(Ordering::SeqCst),
            "activity completion was not reported before shutdown"
        );
    }
}

#[temporalio_macros::cloud_test_exclusion(crate::CloudTestExclusionReason::DoesNotUseServer)]
#[tokio::test]
#[ignore = "only run in a subprocess so OS signals cannot terminate the test runner"]
async fn worker_signal_subprocess() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let cancel = mode == "cancel";
    let completed = Arc::new(AtomicBool::new(false));
    let completion_recorded = completed.clone();
    let mut cfg = MockPollCfg::new(vec![], false, 0);
    cfg.using_rust_sdk = true;
    cfg.make_poll_stream_interminable = true;
    cfg.activity_responses = Some(vec![
        PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "activity".to_owned(),
            activity_type: Some(SignalActivities::wait_for_cleanup.name().into()),
            ..Default::default()
        }
        .into(),
    ]);
    cfg.mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(move |_, _| {
            completion_recorded.store(true, Ordering::SeqCst);
            Ok(RespondActivityTaskCompletedResponse::default())
        });
    let mut mocks = build_mock_pollers(cfg);
    mocks.worker_cfg(|cfg| {
        cfg.graceful_shutdown_period = Some(if cancel {
            Duration::ZERO
        } else {
            Duration::from_secs(60)
        });
    });
    let core = Arc::new(mock_worker(mocks));
    let options = WorkerOptions::new(core.get_config().task_queue.clone())
        .register_activities(SignalActivities { cancel })
        .worker_interceptor(ShutdownAsserter(completed.clone()))
        .build();
    let mut worker =
        Worker::new_from_core_options(core.clone(), ClientOptions::new("default").build(), options)
            .unwrap();
    worker.run_until_signal().await.unwrap();
    assert!(completed.load(Ordering::SeqCst));
    assert!(matches!(
        core.poll_activity_task().await,
        Err(temporalio_sdk_core::PollError::ShutDown)
    ));
    assert!(matches!(
        core.poll_workflow_activation().await,
        Err(temporalio_sdk_core::PollError::ShutDown)
    ));
    println!("worker-shutdown-complete");
}
