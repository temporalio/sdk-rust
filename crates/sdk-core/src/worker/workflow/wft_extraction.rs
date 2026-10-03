use crate::{
    abstractions::OwnedMeteredSemPermit,
    protosext::ValidPollWFTQResponse,
    worker::{
        WorkflowSlotKind,
        client::WorkerClient,
        workflow::{
            CacheMissFetchReq, HistoryUpdate, NextPageReq, PermittedWFT, UnstoredTask,
            UnstoredWftFailInfo, WftFailureKind, history_update::HistoryPaginator,
        },
    },
};
use futures_util::{FutureExt, Stream, StreamExt, stream, stream::PollNext};
use std::{future, sync::Arc};
use temporalio_common::protos::coresdk::WorkflowSlotInfo;
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
        failed_task: Option<Box<UnstoredTask>>,
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
        let wft_stream = wft_stream
            .map(move |stream_in| {
                let client = client.clone();
                async move {
                    match stream_in {
                        Ok((wft, permit)) => {
                            let run_id = wft.workflow_execution.run_id.clone();
                            let fail_info = UnstoredWftFailInfo {
                                task_token: wft.task_token.clone(),
                                attempt: wft.attempt,
                                workflow_type: wft.workflow_type.clone(),
                            };
                            let kind = if wft.legacy_query.is_some() {
                                WftFailureKind::LegacyQuery
                            } else {
                                WftFailureKind::Task
                            };
                            let slot_info = WorkflowSlotInfo {
                                workflow_type: wft.workflow_type.clone(),
                                is_sticky: wft
                                    .history
                                    .events
                                    .first()
                                    .is_none_or(|event| event.event_id > 1),
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
                                    failed_task: Some(Box::new(UnstoredTask {
                                        info: fail_info,
                                        kind,
                                        permit: permit.into_used(slot_info),
                                    })),
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
            .chain(stream::iter([future::ready(Ok(
                WFTExtractorOutput::PollerDead,
            ))
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
                            match HistoryPaginator::from_fetchreq(req, client).await {
                                Ok(r) => WFTExtractorOutput::FetchResult(r, rc),
                                Err(failure) => {
                                    let (err, task) = *failure;
                                    WFTExtractorOutput::FailedFetch {
                                        run_id,
                                        err,
                                        failed_task: Some(Box::new(task.into())),
                                    }
                                }
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
                                    failed_task: None,
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
