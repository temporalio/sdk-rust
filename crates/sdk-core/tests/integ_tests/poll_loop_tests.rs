use crate::common::CoreWfStarter;
use futures_util::future::join_all;
use std::time::Duration;
use temporalio_client::{
    WorkflowExecuteUpdateOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};

// ---------------------------------------------------------------------------
// Scripted wait_condition / state_mut test harness
// ---------------------------------------------------------------------------
//
// A single workflow that executes arbitrary chains of wait_condition /
// state_mut calls across run, update, and async signal handlers.  The
// entire script is passed as workflow input; individual handlers take
// no meaningful arguments — they pop the next script from workflow state.
//
// Each step is either Wait(flag) or Set(flag).
// A handler's script is a Vec of steps executed sequentially.
// The workflow input contains scripts for run, each update, and each signal.

/// A single step: either wait for a flag or set a flag.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
enum Step {
    Wait(usize),
    Set(usize),
}

fn wait(flag: usize) -> Step {
    Step::Wait(flag)
}
fn set(flag: usize) -> Step {
    Step::Set(flag)
}

/// Full test script passed as workflow input.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct TestScript {
    run_steps: Vec<Step>,
    update_scripts: Vec<Vec<Step>>,
    signal_scripts: Vec<Vec<Step>>,
}

#[workflow]
#[derive(Default)]
struct ChainWf {
    flags: [bool; 8],
    script: TestScript,
    script_ready: bool,
    next_update: usize,
    next_signal: usize,
}

async fn execute_steps(ctx: &mut WorkflowContext<ChainWf>, steps: Vec<Step>) {
    for step in steps {
        match step {
            Step::Wait(w) => {
                ctx.wait_condition(move |s| s.flags[w]).await;
            }
            Step::Set(f) => {
                ctx.state_mut(move |s| s.flags[f] = true);
            }
        }
    }
}

#[workflow_methods]
impl ChainWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, input: TestScript) -> WorkflowResult<()> {
        let steps = ctx.state_mut(|s| {
            s.script = input;
            s.script_ready = true;
            s.script.run_steps.clone()
        });
        execute_steps(ctx, steps).await;
        Ok(())
    }

    #[update]
    async fn step(
        ctx: &mut WorkflowContext<Self>,
        _: (),
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        ctx.wait_condition(|s| s.script_ready).await;
        let steps = ctx.state_mut(|s| {
            let idx = s.next_update;
            s.next_update += 1;
            s.script.update_scripts[idx].clone()
        });
        execute_steps(ctx, steps).await;
        Ok("done".to_string())
    }

    #[signal]
    async fn trigger(ctx: &mut WorkflowContext<Self>, _: ()) {
        ctx.wait_condition(|s| s.script_ready).await;
        let steps = ctx.state_mut(|s| {
            let idx = s.next_signal;
            s.next_signal += 1;
            s.script.signal_scripts[idx].clone()
        });
        execute_steps(ctx, steps).await;
    }
}

/// Run a scripted test. Starts the workflow, sends the requested number of
/// updates and signals, and asserts everything completes within 5 seconds.
async fn run_scripted_test(name: &str, script: TestScript) {
    let mut starter = CoreWfStarter::new(name);
    let mut worker = starter.worker().await;
    worker.register_workflow::<ChainWf>().unwrap();

    let num_updates = script.update_scripts.len();
    let num_signals = script.signal_scripts.len();
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ChainWf::run,
            script,
            WorkflowStartOptions::new(task_queue, starter.get_wf_id().to_owned()).build(),
        )
        .await
        .unwrap();

    let mut update_futs = Vec::new();
    for _ in 0..num_updates {
        update_futs.push(handle.execute_update(
            ChainWf::step,
            (),
            WorkflowExecuteUpdateOptions::default(),
        ));
    }
    for _ in 0..num_signals {
        handle
            .signal(ChainWf::trigger, (), WorkflowSignalOptions::default())
            .await
            .unwrap();
    }

    let shutdown = worker.inner_mut().shutdown_handle();

    let runner = async {
        worker.inner_mut().run().await.unwrap();
    };

    tokio::select! {
        results = join_all(update_futs) => {
            for r in results {
                assert_eq!(r.unwrap(), "done");
            }
            shutdown();
        }
        _ = runner => {
            panic!("worker exited before updates completed");
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("deadlock: test stuck for 5 s");
        }
    }
}

/// Update → Workflow → Update relay chain.
/// update[0] sets 1 → run waits 1, sets 2 → update[1] waits 2, sets 3
/// → run waits 3 and completes.
#[tokio::test]
async fn scripted_update_workflow_update() {
    run_scripted_test(
        "scr_upd_wf_upd",
        TestScript {
            run_steps: vec![wait(1), set(2), wait(3)],
            update_scripts: vec![vec![set(1)], vec![wait(2), set(3)]],
            signal_scripts: vec![],
        },
    )
    .await;
}

/// Two updates directly unblock each other.
/// update[0] waits 1, sets 2.  update[1] sets 1, waits 2.
/// Run waits for both to finish (flag 3).
#[tokio::test]
async fn scripted_update_cross_unblock() {
    run_scripted_test(
        "scr_upd_cross",
        TestScript {
            run_steps: vec![wait(3)],
            update_scripts: vec![vec![wait(1), set(2), set(3)], vec![set(1), wait(2), set(3)]],
            signal_scripts: vec![],
        },
    )
    .await;
}

/// Signal → Update → Workflow chain.
/// signal[0] sets 1 → update[0] waits 1, sets 2 → run waits 2.
#[tokio::test]
async fn scripted_signal_update_workflow() {
    run_scripted_test(
        "scr_sig_upd_wf",
        TestScript {
            run_steps: vec![wait(2)],
            update_scripts: vec![vec![wait(1), set(2)]],
            signal_scripts: vec![vec![set(1)]],
        },
    )
    .await;
}

/// Update → Signal dependency.
/// update[0] sets 1 → signal[0] waits 1, sets 2 → run waits 2.
/// Exercises the reverse direction: signals poll before updates, so
/// signal[0] only sees flag 1 after the re-poll loop re-polls.
#[tokio::test]
async fn scripted_update_unblocks_signal() {
    run_scripted_test(
        "scr_upd_sig",
        TestScript {
            run_steps: vec![wait(2)],
            update_scripts: vec![vec![set(1)]],
            signal_scripts: vec![vec![wait(1), set(2)]],
        },
    )
    .await;
}

/// Full chain across all handler types with multiple waits each.
/// signal[0] sets 1 → update[0] waits 1, sets 2, waits 4, sets 5
/// → run waits 2, sets 3, waits 5, sets 6
/// → update[1] waits 3, sets 4
/// → signal[1] waits 6, sets 7 → run waits 7 and completes.
#[tokio::test]
async fn scripted_full_chain() {
    run_scripted_test(
        "scr_full_chain",
        TestScript {
            run_steps: vec![wait(2), set(3), wait(5), set(6), wait(7)],
            update_scripts: vec![
                vec![wait(1), set(2), wait(4), set(5)],
                vec![wait(3), set(4)],
            ],
            signal_scripts: vec![vec![set(1)], vec![wait(6), set(7)]],
        },
    )
    .await;
}
