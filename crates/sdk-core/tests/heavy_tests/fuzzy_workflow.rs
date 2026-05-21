use crate::common::{CoreWfStarter, activity_functions::StdActivities};
use futures_util::{StreamExt, sink, stream::FuturesUnordered};
use rand::{Rng, RngExt, SeedableRng, prelude::Distribution, rngs::SmallRng};
use std::{sync::Arc, time::Duration};
use temporalio_client::{
    UntypedSignal, UntypedWorkflow, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::{data_converters::RawValue, protos::coresdk::AsJsonPayloadExt};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, LocalActivityOptions, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::TunerHolder;

const FUZZY_SIG: &str = "fuzzy_sig";

#[derive(serde::Serialize, serde::Deserialize, Copy, Clone)]
enum FuzzyWfAction {
    Shutdown,
    DoAct,
    DoLocalAct,
}

struct FuzzyWfActionSampler;
impl Distribution<FuzzyWfAction> for FuzzyWfActionSampler {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> FuzzyWfAction {
        let v: u8 = rng.random_range(1..=2);
        match v {
            1 => FuzzyWfAction::DoAct,
            2 => FuzzyWfAction::DoLocalAct,
            _ => unreachable!(),
        }
    }
}

#[workflow]
#[derive(Default)]
struct FuzzyWf {
    done: bool,
}

#[workflow_methods]
impl FuzzyWf {
    #[run(name = "fuzzy_wf")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.done).await;
        Ok(())
    }

    #[signal(name = "fuzzy_sig")]
    async fn fuzzy_signal(ctx: &mut WorkflowContext<Self>, action: FuzzyWfAction) {
        match action {
            FuzzyWfAction::Shutdown => ctx.state_mut(|s| s.done = true),
            FuzzyWfAction::DoAct => {
                let _ = ctx
                    .start_activity(
                        StdActivities::echo,
                        "hi!".to_string(),
                        ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                    )
                    .await;
            }
            FuzzyWfAction::DoLocalAct => {
                let _ = ctx
                    .start_local_activity(
                        StdActivities::echo,
                        "hi!".to_string(),
                        LocalActivityOptions {
                            start_to_close_timeout: Some(Duration::from_secs(5)),
                            ..Default::default()
                        },
                    )
                    .await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzzy_workflow() {
    let num_workflows = 200;
    let wf_name = "fuzzy_wf";
    let mut starter = CoreWfStarter::new("fuzzy_workflow");
    starter.sdk_config.max_cached_workflows = 25;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(25, 25, 100, 100));
    let mut worker = starter.worker().await;
    worker.register_workflow::<FuzzyWf>().unwrap();
    worker.register_activities(StdActivities);

    let client = starter.get_client().await;
    let task_queue = starter.get_task_queue().to_owned();

    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}");
        worker
            .submit_workflow(
                FuzzyWf::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), wfid).build(),
            )
            .await
            .unwrap();
    }

    let rng = SmallRng::seed_from_u64(523189);
    let mut actions: Vec<FuzzyWfAction> = rng.sample_iter(FuzzyWfActionSampler).take(15).collect();
    actions.push(FuzzyWfAction::Shutdown);

    let sig_sender = async {
        for action in actions {
            let sends: FuturesUnordered<_> = (0..num_workflows)
                .map(|i| {
                    let handle =
                        client.get_workflow_handle::<UntypedWorkflow>(format!("{wf_name}_{i}"));
                    async move {
                        handle
                            .signal(
                                UntypedSignal::new(FUZZY_SIG),
                                RawValue::new(vec![
                                    action.as_json_payload().expect("Serializes ok"),
                                ]),
                                WorkflowSignalOptions::default(),
                            )
                            .await
                    }
                })
                .collect();
            sends
                .map(|_| Ok(()))
                .forward(sink::drain())
                .await
                .expect("Sending signals works");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    let (r1, _) = tokio::join!(worker.run_until_done(), sig_sender);
    r1.unwrap();
}
