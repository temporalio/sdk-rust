//! This module implements support for creating special core instances and workers which can be used
//! to replay canned histories. It should be used by Lang SDKs to provide replay capabilities to
//! users during testing.

mod history_info;

/// Pre-built test histories for common workflow patterns.
#[cfg(any(feature = "test-utilities", test))]
pub mod canned_histories;

#[cfg(any(feature = "test-utilities", test))]
mod history_builder;

pub use history_info::HistoryInfo;

use crate::{
    Worker, WorkerConfig,
    worker::{
        PollerBehavior, PostActivateHookData,
        client::mocks::{MockManualWorkerClient, mock_manual_worker_client},
    },
};
use futures_util::{FutureExt, Stream, StreamExt};
use parking_lot::Mutex;
use std::{
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};
use temporalio_common::{
    protos::{
        TaskToken,
        coresdk::workflow_activation::remove_from_cache::EvictionReason,
        temporal::api::{
            common::v1::WorkflowExecution,
            history::v1::History,
            workflowservice::v1::{
                DescribeNamespaceResponse, PollWorkflowTaskQueueResponse,
                RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedResponse,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use tokio::sync::{Mutex as TokioMutex, mpsc, mpsc::UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

#[cfg(any(feature = "test-utilities", test))]
pub use history_builder::{
    DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, default_act_sched,
    default_wes_attribs,
};

/// Contains inputs to a replay worker
pub struct ReplayWorkerInput<I> {
    /// The worker's configuration. Some portions will be overridden to required values once the
    /// worker is instantiated.
    pub config: WorkerConfig,
    history_stream: I,
    /// If specified use this as the basis for the internal mocked client
    pub(crate) client_override: Option<MockManualWorkerClient>,
}

impl<I> ReplayWorkerInput<I>
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    /// Build a replay worker
    pub fn new(config: WorkerConfig, history_stream: I) -> Self {
        Self {
            config,
            history_stream,
            client_override: None,
        }
    }

    pub(crate) fn into_core_worker(mut self) -> Result<Worker, anyhow::Error> {
        self.config.max_cached_workflows = 1;
        self.config.workflow_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(1));
        self.config.task_types = WorkerTaskTypes::workflow_only();
        self.config.skip_client_worker_set_check = true;
        let historator = Historator::new(self.history_stream);
        let post_activate = historator.get_post_activate_hook();
        let shutdown_tok = historator.get_shutdown_setter();
        // Create a mock client which can be used by a replay worker to serve up canned histories.
        let mut client = if let Some(c) = self.client_override {
            c
        } else {
            mock_manual_worker_client()
        };
        // Worker::run validates before polling. Installing this after an optional client override
        // lets a test-provided describe expectation take precedence over the fallback.
        client
            .expect_describe_namespace()
            .times(0..)
            .returning(|| async { Ok(DescribeNamespaceResponse::default()) }.boxed());

        let hist_allow_tx = historator.replay_done_tx.clone();
        let historator = Arc::new(TokioMutex::new(historator));

        // TODO: Should use `new_with_pollers` and avoid re-doing mocking stuff
        client.expect_poll_workflow_task().returning(move |_, _| {
            let historator = historator.clone();
            async move { Ok(historator.lock().await.next_poll_response().await) }.boxed()
        });

        client
            .expect_complete_workflow_task()
            .returning(move |_a, _b| {
                async move { Ok(RespondWorkflowTaskCompletedResponse::default()) }.boxed()
            });
        client
            .expect_fail_workflow_task()
            .returning(move |task, _, _| {
                hist_allow_tx.send(task.task_token).unwrap();
                async move { Ok(RespondWorkflowTaskFailedResponse::default()) }.boxed()
            });
        let mut worker = Worker::new(self.config, None, Arc::new(client), None, None, None)?;
        worker.set_post_activate_hook(post_activate);
        shutdown_tok(worker.shutdown_token());
        Ok(worker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::test_worker_cfg;
    use futures_util::{FutureExt, stream};

    #[tokio::test]
    async fn client_override_describe_namespace_precedes_fallback() {
        let mut client = mock_manual_worker_client();
        client
            .expect_describe_namespace()
            .times(1)
            .returning(|| async { Ok(DescribeNamespaceResponse::default()) }.boxed());

        let mut input = ReplayWorkerInput::new(test_worker_cfg().build().unwrap(), stream::empty());
        input.client_override = Some(client);

        input.into_core_worker().unwrap().validate().await.unwrap();
    }

    #[test]
    fn hands_out_a_history_only_after_the_one_before_reports_back() {
        // The same History three times, so run ids can't tell the Histories apart.
        let history = HistoryForReplay::from(canned_histories::single_timer("1"));
        let mut historator = Historator::new(stream::iter(vec![history; 3]));
        let closer = CancellationToken::new();
        historator.get_shutdown_setter()(closer.clone());
        let report = historator.replay_done_tx.clone();
        let mut earlier_reports: Vec<TaskToken> = vec![];

        for _ in 0..3 {
            let resp = historator.next_poll_response().now_or_never().unwrap();
            let task_token = TaskToken::from(resp.task_token);
            // Earlier Histories report back again with each activation they complete.
            for earlier in &earlier_reports {
                report.send(earlier.clone()).unwrap();
            }
            assert!(historator.next_poll_response().now_or_never().is_none());
            assert!(!closer.is_cancelled());

            report.send(task_token.clone()).unwrap();
            earlier_reports.push(task_token);
        }
        assert_eq!(
            historator.next_poll_response().now_or_never(),
            Some(Default::default())
        );
        assert!(closer.is_cancelled());
    }
}

/// A history which will be used during replay verification. Since histories do not include the
/// workflow id, it must be manually attached.
#[derive(Debug, Clone)]
pub struct HistoryForReplay {
    hist: History,
    workflow_id: String,
}
impl HistoryForReplay {
    /// Create a new history from replay from something that looks like a history and a workflow id.
    pub fn new(history: impl Into<History>, workflow_id: impl Into<String>) -> Self {
        Self {
            hist: history.into(),
            workflow_id: workflow_id.into(),
        }
    }
}
#[cfg(any(feature = "test-utilities", test))]
impl From<TestHistoryBuilder> for HistoryForReplay {
    fn from(thb: TestHistoryBuilder) -> Self {
        thb.get_full_history_info().unwrap().into()
    }
}
#[cfg(any(feature = "test-utilities", test))]
impl From<HistoryInfo> for HistoryForReplay {
    fn from(histinfo: HistoryInfo) -> Self {
        HistoryForReplay::new(histinfo, "fake".to_owned())
    }
}

/// Allows lang to feed histories into the replayer one at a time. Simply drop the feeder to signal
/// to the worker that you're done and it should initiate shutdown.
pub struct HistoryFeeder {
    tx: mpsc::Sender<HistoryForReplay>,
}
/// The stream half of a [HistoryFeeder]
pub struct HistoryFeederStream {
    rcvr: mpsc::Receiver<HistoryForReplay>,
}

impl HistoryFeeder {
    /// Make a new history feeder, which will store at most `buffer_size` histories before `feed`
    /// blocks.
    ///
    /// Returns a feeder which will be used to feed in histories, and a stream you can pass to
    /// one of the replay worker init functions.
    pub fn new(buffer_size: usize) -> (Self, HistoryFeederStream) {
        let (tx, rcvr) = mpsc::channel(buffer_size);
        (Self { tx }, HistoryFeederStream { rcvr })
    }
    /// Feed a new history into the replayer, blocking if there is not room to accept another
    /// history.
    pub async fn feed(&self, history: HistoryForReplay) -> anyhow::Result<()> {
        self.tx.send(history).await?;
        Ok(())
    }
}

impl Stream for HistoryFeederStream {
    type Item = HistoryForReplay;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rcvr.poll_recv(cx)
    }
}

pub(crate) struct Historator {
    iter: Pin<Box<dyn Stream<Item = HistoryForReplay> + Send>>,
    allow_stream: UnboundedReceiverStream<TaskToken>,
    worker_closer: Arc<OnceLock<CancellationToken>>,
    dat: Arc<Mutex<HistoratorDat>>,
    replay_done_tx: UnboundedSender<TaskToken>,
    /// The task token of the History handed out last, which it reports back with.
    handed_out: Option<TaskToken>,
}
impl Historator {
    pub(crate) fn new(histories: impl Stream<Item = HistoryForReplay> + Send + 'static) -> Self {
        let dat = Arc::new(Mutex::new(HistoratorDat::default()));
        let (replay_done_tx, replay_done_rx) = mpsc::unbounded_channel();
        Self {
            iter: Box::pin(histories.fuse()),
            allow_stream: UnboundedReceiverStream::new(replay_done_rx),
            worker_closer: Arc::new(OnceLock::new()),
            dat,
            replay_done_tx,
            handed_out: None,
        }
    }

    /// Hands out the next History once the one before it has reported back, or ends the worker
    /// when none are left.
    async fn next_poll_response(&mut self) -> PollWorkflowTaskQueueResponse {
        // Earlier Histories can report back again. Only a report from the last one shows Core took
        // it from the poll buffer, and ending the worker before that drops it.
        if let Some(handed_out) = &self.handed_out {
            while let Some(report) = self.allow_stream.next().await {
                if report == *handed_out {
                    break;
                }
            }
        }

        if let Some(history) = self.next().await {
            let hist_info = HistoryInfo::from_owned_history(history.hist).unwrap();
            let execution = WorkflowExecution {
                workflow_id: history.workflow_id,
                run_id: hist_info.orig_run_id().to_string(),
            };
            let mut resp = hist_info.into_poll_wft_response();
            resp.workflow_execution = Some(execution);
            self.handed_out = Some(resp.task_token.clone().into());
            resp
        } else {
            if let Some(wc) = self.worker_closer.get() {
                wc.cancel();
            }
            Default::default()
        }
    }

    /// Returns the post-activation hook. Its reports tell the poll when Core holds the History it
    /// handed out last.
    pub(crate) fn get_post_activate_hook(
        &self,
    ) -> impl Fn(&Worker, PostActivateHookData) + Send + Sync + use<> {
        let done_tx = self.replay_done_tx.clone();
        move |worker, data| {
            if !data.replaying {
                worker.request_wf_eviction(
                    data.run_id,
                    "Always evict workflows after replay",
                    EvictionReason::LangRequested,
                );
            }
            // Report every completion. A History that fails while replaying, like one with
            // nondeterminism set to fail the Workflow, never completes an activation after replay.
            if let Some(task_token) = data.task_token {
                done_tx.send(task_token.clone()).unwrap();
            }
        }
    }

    pub(crate) fn get_shutdown_setter(&self) -> impl FnOnce(CancellationToken) + 'static {
        let wc = self.worker_closer.clone();
        move |ct| {
            wc.set(ct).expect("Shutdown token must only be set once");
        }
    }
}

impl Stream for Historator {
    type Item = HistoryForReplay;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iter.poll_next_unpin(cx) {
            Poll::Ready(Some(history)) => {
                history
                    .hist
                    .extract_run_id_from_start()
                    .expect(
                        "Histories provided for replay must contain run ids in their workflow \
                                 execution started events",
                    )
                    .to_string();
                Poll::Ready(Some(history))
            }
            Poll::Ready(None) => {
                self.dat.lock().all_dispatched = true;
                Poll::Ready(None)
            }
            o => o,
        }
    }
}

#[derive(Default)]
struct HistoratorDat {
    all_dispatched: bool,
}
