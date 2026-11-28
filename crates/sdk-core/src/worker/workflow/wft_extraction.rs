use crate::{
    abstractions::OwnedMeteredSemPermit,
    protosext::ValidPollWFTQResponse,
    worker::{
        WorkflowSlotKind,
        client::WorkerClient,
        workflow::{
            CacheMissFetchReq, HistoryUpdate, NextPageReq, PermittedWFT, UnstoredWftFailInfo,
            history_update::HistoryPaginator,
        },
    },
};
use futures_util::{FutureExt, Stream, StreamExt, stream, stream::PollNext};
use std::sync::Arc;
use temporalio_common::protos::coresdk::WorkflowSlotInfo;
use tokio_util::task::TaskTracker;
use tracing::Span;

/// Transforms incoming validated WFTs and history fetching requests into [PermittedWFT]s ready
/// for application to workflow state
pub(super) struct WFTExtractor {}

pub(super) enum WFTExtractorOutput {
    NewWFT(PermittedWFT),
    FetchResult(
        PermittedWFT,
        // Field isn't read, but we need to hold on to it.
        #[allow(dead_code)] Arc<HistfetchRC>,
    ),
    NextPage {
        paginator: HistoryPaginator,
        update: HistoryUpdate,
        span: Span,
        rc: Arc<HistfetchRC>,
    },
    FailedFetch {
        run_id: String,
        err: tonic::Status,
        auto_reply_fail: Option<UnstoredWftFailInfo>,
    },
    PollerDead,
}

pub(crate) type WFTStreamIn = Result<
    (
        ValidPollWFTQResponse,
        OwnedMeteredSemPermit<WorkflowSlotKind>,
    ),
    tonic::Status,
>;
#[derive(derive_more::From, Debug)]
pub(super) enum HistoryFetchReq {
    Full(Box<CacheMissFetchReq>, Arc<HistfetchRC>),
    NextPage(Box<NextPageReq>, Arc<HistfetchRC>),
}
/// Used inside of `Arc`s to ensure we don't shutdown while there are outstanding fetches.
#[derive(Debug)]
pub(super) struct HistfetchRC {}

impl WFTExtractor {
    pub(super) fn build(
        client: Arc<dyn WorkerClient>,
        max_fetch_concurrency: usize,
        wft_stream: impl Stream<Item = WFTStreamIn> + Send + 'static,
        fetch_stream: impl Stream<Item = HistoryFetchReq> + Send + 'static,
    ) -> impl Stream<Item = Result<WFTExtractorOutput, tonic::Status>> + Send + 'static {
        let fetch_client = client.clone();
        let pending_fetches = TaskTracker::new();
        let fetch_tracker = pending_fetches.clone();
        let wft_stream = wft_stream
            .map(move |stream_in| {
                let client = client.clone();
                let guard = fetch_tracker.token();
                async move {
                    let _guard = guard;
                    match stream_in {
                        Ok((wft, permit)) => {
                            let run_id = wft.workflow_execution.run_id.clone();
                            let fail_info = UnstoredWftFailInfo {
                                task_token: wft.task_token.clone(),
                                workflow_id: wft.workflow_execution.workflow_id.clone(),
                                attempt: wft.attempt,
                                workflow_type: wft.workflow_type.clone(),
                            };
                            Ok(match HistoryPaginator::from_poll(wft, client).await {
                                Ok((pag, prep)) => WFTExtractorOutput::NewWFT(PermittedWFT {
                                    permit: permit.into_used(WorkflowSlotInfo {
                                        workflow_type: prep.workflow_type.clone(),
                                        is_sticky: prep.is_incremental(),
                                    }),
                                    work: prep,
                                    paginator: pag,
                                }),
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id,
                                    err,
                                    auto_reply_fail: Some(fail_info),
                                },
                            })
                        }
                        Err(e) => Err(e),
                    }
                }
                // This is... unattractive, but lets us avoid boxing all the futs in the stream
                .left_future()
                .left_future()
            })
            .chain(stream::iter([async move {
                // `buffer_unordered` can otherwise deliver `PollerDead` before a slower fetch.
                pending_fetches.close();
                pending_fetches.wait().await;
                Ok(WFTExtractorOutput::PollerDead)
            }
            .right_future()
            .left_future()]));

        stream::select_with_strategy(
            wft_stream,
            fetch_stream.map(move |fetchreq: HistoryFetchReq| {
                let client = fetch_client.clone();
                async move {
                    Ok(match fetchreq {
                        // It's OK to simply drop the refcounters in the event of fetch
                        // failure. We'll just proceed with shutdown.
                        HistoryFetchReq::Full(req, rc) => {
                            let run_id = req.original_wft.work.execution.run_id.clone();
                            let fail_info = UnstoredWftFailInfo {
                                task_token: req.original_wft.work.task_token.clone(),
                                workflow_id: req.original_wft.work.execution.workflow_id.clone(),
                                attempt: req.original_wft.work.attempt,
                                workflow_type: req.original_wft.work.workflow_type.clone(),
                            };
                            match HistoryPaginator::from_fetchreq(req, client).await {
                                Ok(r) => WFTExtractorOutput::FetchResult(r, rc),
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id,
                                    err,
                                    auto_reply_fail: Some(fail_info),
                                },
                            }
                        }
                        HistoryFetchReq::NextPage(mut req, rc) => {
                            match req.paginator.extract_next_update().await {
                                Ok(update) => WFTExtractorOutput::NextPage {
                                    paginator: req.paginator,
                                    update,
                                    span: req.span,
                                    rc,
                                },
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id: req.paginator.run_id,
                                    err,
                                    auto_reply_fail: None,
                                },
                            }
                        }
                    })
                }
                .right_future()
            }),
            // Priority always goes to the fetching stream
            |_: &mut ()| PollNext::Right,
        )
        .buffer_unordered(max_fetch_concurrency)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer,
        replay::TestHistoryBuilder,
        test_help::{ResponseType, hist_to_poll_resp},
        worker::client::mocks::mock_manual_worker_client,
    };
    use temporalio_common::protos::temporal::api::{
        enums::v1::EventType, workflowservice::v1::GetWorkflowExecutionHistoryResponse,
    };
    use tokio::sync::Notify;

    #[tokio::test]
    async fn poller_end_waits_for_initial_history_fetch() {
        let mut history = TestHistoryBuilder::default();
        history.add_by_type(EventType::WorkflowExecutionStarted);
        history.add_full_wf_task();
        let mut task = hist_to_poll_resp(&history, "workflow", ResponseType::AllHistory).resp;
        let second_page = task.history.as_mut().unwrap().events.split_off(1);
        task.next_page_token = vec![1];
        let release = Arc::new(Notify::new());
        let mut client = mock_manual_worker_client();
        client
            .expect_get_workflow_execution_history()
            .times(1)
            .return_once({
                let release = release.clone();
                move |_, _, _| {
                    async move {
                        release.notified().await;
                        Ok(GetWorkflowExecutionHistoryResponse {
                            history: Some(second_page.into()),
                            ..Default::default()
                        })
                    }
                    .boxed()
                }
            });
        let permit = fixed_size_permit_dealer(1).acquire_owned().await;
        let incoming = stream::iter([Ok((task.try_into().unwrap(), permit))]);
        let output = WFTExtractor::build(Arc::new(client), 2, incoming, stream::pending());
        futures_util::pin_mut!(output);

        assert!(output.next().now_or_never().is_none());
        release.notify_one();
        assert!(matches!(
            output.next().await,
            Some(Ok(WFTExtractorOutput::NewWFT(_)))
        ));
        assert!(matches!(
            output.next().await,
            Some(Ok(WFTExtractorOutput::PollerDead))
        ));
    }
}
