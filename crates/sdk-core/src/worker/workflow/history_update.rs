use crate::{
    abstractions::dbg_panic,
    internal_flags::CoreInternalFlags,
    protosext::ValidPollWFTQResponse,
    worker::{
        client::WorkerClient,
        workflow::{CacheMissFetchReq, PermittedWFT, PreparedWFT},
    },
};
use futures_util::{FutureExt, Stream, TryFutureExt, future::BoxFuture};
use itertools::Itertools;
use std::{
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    mem,
    mem::transmute,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};
use temporalio_common::protos::temporal::api::{
    enums::v1::EventType,
    history::v1::{
        History, HistoryEvent, WorkflowTaskCompletedEventAttributes, history_event::Attributes,
    },
};
use tracing::Instrument;

static EMPTY_FETCH_ERR: LazyLock<tonic::Status> =
    LazyLock::new(|| tonic::Status::unknown("Fetched empty history page"));
static EMPTY_TASK_ERR: LazyLock<tonic::Status> = LazyLock::new(|| {
    tonic::Status::unknown("Received an empty workflow task with no queries or history")
});

/// Represents one or more complete WFT sequences. History events are expected to be consumed from
/// it and applied to the state machines via [HistoryUpdate::take_next_wft_sequence]
pub(crate) struct HistoryUpdate {
    events: Vec<HistoryEvent>,
    /// The event ID of the last started WFT, as according to the WFT which this update was
    /// extracted from. Hence, while processing multiple logical WFTs during replay which were part
    /// of one large history fetched from server, multiple updates may have the same value here.
    pub(crate) previous_wft_started_id: i64,
    /// The `started_event_id` field from the WFT which this update is tied to. Multiple updates
    /// may have the same value if they're associated with the same WFT.
    pub(crate) wft_started_id: i64,
    /// True if this update contains the final WFT in history, and no more attempts to extract
    /// additional updates should be made.
    has_last_wft: bool,
    wft_count: usize,
    /// True if the speculative WFT (i.e. the current, non-replayed task from the
    /// server) carries pending update messages. When set, the heartbeat-collapsing
    /// heuristic will avoid merging the last WFT in history into a preceding
    /// heartbeat chain, because the update needs its own activation.
    has_pending_speculative_updates: bool,
    /// True if any WFTCompleted event in this update carries the
    /// `WftChunkingV2` flag, indicating WFT chunking v2 should be used
    /// instead of v1 (the legacy algorithm).
    has_wft_chunking_v2: bool,
}

impl Debug for HistoryUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_real() {
            write!(
                f,
                "HistoryUpdate(previous_started_event_id: {}, started_id: {}, \
                 length: {}, first_event_id: {:?})",
                self.previous_wft_started_id,
                self.wft_started_id,
                self.events.len(),
                self.events.first().map(|e| e.event_id)
            )
        } else {
            write!(f, "DummyHistoryUpdate")
        }
    }
}

impl HistoryUpdate {
    pub(crate) fn get_events(&self) -> &[HistoryEvent] {
        &self.events
    }
}

#[derive(Debug)]
pub(crate) enum NextWFT {
    ReplayOver,
    WFT(Vec<HistoryEvent>, bool),
    NeedFetch,
}

#[derive(derive_more::Debug)]
#[debug("HistoryPaginator(run_id: {run_id})")]
pub(crate) struct HistoryPaginator {
    pub(crate) wf_id: String,
    pub(crate) run_id: String,
    pub(crate) previous_wft_started_id: i64,
    pub(crate) wft_started_event_id: i64,
    id_of_last_event_in_last_extracted_update: Option<i64>,

    client: Arc<dyn WorkerClient>,
    event_queue: VecDeque<HistoryEvent>,
    next_page_token: NextPageToken,
    /// These are events that should be returned once pagination has finished. This only happens
    /// during cache misses, where we got a partial task but need to fetch history from the start.
    final_events: Vec<HistoryEvent>,
    /// True if the speculative WFT associated with this paginator carries pending update
    /// messages. Passed through to `find_end_index_of_next_wft_seq` so the heartbeat
    /// heuristic avoids collapsing the last WFT when an update needs its own activation.
    has_pending_speculative_updates: bool,
    /// True if the workflow uses WFT chunking v2. Detected from the initial
    /// events and propagated to all subsequent `from_events` calls.
    has_wft_chunking_v2: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum NextPageToken {
    /// There is no page token, we need to fetch history from the beginning
    FetchFromStart,
    /// There is a page token
    Next(Vec<u8>),
    /// There is no page token, we are done fetching history
    Done,
}

// If we're converting from a page token from the server, if it's empty, then we're done.
impl From<Vec<u8>> for NextPageToken {
    fn from(page_token: Vec<u8>) -> Self {
        if page_token.is_empty() {
            NextPageToken::Done
        } else {
            NextPageToken::Next(page_token)
        }
    }
}

impl HistoryPaginator {
    /// Use a new poll response to create a new [WFTPaginator], returning it and the
    /// [PreparedWFT] extracted from it that can be fed into workflow state.
    pub(super) async fn from_poll(
        wft: ValidPollWFTQResponse,
        client: Arc<dyn WorkerClient>,
    ) -> Result<(Self, PreparedWFT), tonic::Status> {
        let empty_hist = wft.history.events.is_empty();
        let npt = if empty_hist {
            NextPageToken::FetchFromStart
        } else {
            wft.next_page_token.into()
        };
        let has_pending_speculative_updates = !wft.messages.is_empty();
        let mut paginator = HistoryPaginator::new(
            wft.history,
            wft.previous_started_event_id,
            wft.started_event_id,
            wft.workflow_execution.workflow_id.clone(),
            wft.workflow_execution.run_id.clone(),
            npt,
            client,
            has_pending_speculative_updates,
        );
        if empty_hist && wft.legacy_query.is_none() && wft.query_requests.is_empty() {
            return Err(EMPTY_TASK_ERR.clone());
        }
        let update = if empty_hist {
            HistoryUpdate::from_events(
                [],
                wft.previous_started_event_id,
                wft.started_event_id,
                true,
                has_pending_speculative_updates,
                paginator.has_wft_chunking_v2,
            )
            .0
        } else {
            paginator.extract_next_update().await?
        };
        let prepared = PreparedWFT {
            task_token: wft.task_token,
            attempt: wft.attempt,
            execution: wft.workflow_execution,
            workflow_type: wft.workflow_type,
            legacy_query: wft.legacy_query,
            query_requests: wft.query_requests,
            update,
            messages: wft.messages,
        };
        Ok((paginator, prepared))
    }

    pub(super) async fn from_fetchreq(
        mut req: Box<CacheMissFetchReq>,
        client: Arc<dyn WorkerClient>,
    ) -> Result<PermittedWFT, tonic::Status> {
        let mut paginator = Self {
            wf_id: req.original_wft.work.execution.workflow_id.clone(),
            run_id: req.original_wft.work.execution.run_id.clone(),
            previous_wft_started_id: req.original_wft.work.update.previous_wft_started_id,
            wft_started_event_id: req.original_wft.work.update.wft_started_id,
            id_of_last_event_in_last_extracted_update: req
                .original_wft
                .paginator
                .id_of_last_event_in_last_extracted_update,
            client,
            event_queue: Default::default(),
            next_page_token: NextPageToken::FetchFromStart,
            final_events: req.original_wft.work.update.events,
            has_pending_speculative_updates: !req.original_wft.work.messages.is_empty(),
            has_wft_chunking_v2: req.original_wft.paginator.has_wft_chunking_v2,
        };
        let first_update = paginator.extract_next_update().await?;
        req.original_wft.work.update = first_update;
        req.original_wft.paginator = paginator;
        Ok(req.original_wft)
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        initial_history: History,
        previous_wft_started_id: i64,
        wft_started_event_id: i64,
        wf_id: String,
        run_id: String,
        next_page_token: impl Into<NextPageToken>,
        client: Arc<dyn WorkerClient>,
        has_pending_speculative_updates: bool,
    ) -> Self {
        let has_wft_chunking_v2 = events_have_wft_chunking_v2(&initial_history.events);
        let next_page_token = next_page_token.into();
        let (event_queue, final_events) =
            if matches!(next_page_token, NextPageToken::FetchFromStart) {
                (VecDeque::new(), initial_history.events)
            } else {
                (initial_history.events.into(), vec![])
            };
        Self {
            client,
            event_queue,
            wf_id,
            run_id,
            next_page_token,
            final_events,
            previous_wft_started_id,
            wft_started_event_id,
            id_of_last_event_in_last_extracted_update: None,
            has_pending_speculative_updates,
            has_wft_chunking_v2,
        }
    }

    /// Return at least the next two WFT sequences (as determined by the passed-in ID) as a
    /// [HistoryUpdate]. Two sequences supports the required peek-ahead during replay without
    /// unnecessary back-and-forth.
    ///
    /// If there are already enough events buffered in memory, they will all be returned. Including
    /// possibly (likely, during replay) more than just the next two WFTs.
    ///
    /// If there are insufficient events to constitute two WFTs, then we will fetch pages until
    /// we have two, or until we are at the end of history.
    pub(crate) async fn extract_next_update(&mut self) -> Result<HistoryUpdate, tonic::Status> {
        loop {
            let no_next_page = !self.get_next_page().await?;
            let current_events = mem::take(&mut self.event_queue);
            let seen_enough_events = current_events
                .back()
                .map(|e| e.event_id)
                .unwrap_or_default()
                >= self.wft_started_event_id;

            // This handles a special case where the server might send us a page token along with
            // a real page which ends at the current end of history. The page token then points to
            // en empty page. We need to detect this, and consider it the end of history.
            //
            // This case unfortunately cannot be handled earlier, because we might fetch a page
            // from the server which contains two complete WFTs, and thus we are happy to return
            // an update at that time. But, if the page has a next page token, we *cannot* conclude
            // we are done with replay until we fetch that page. So, we have to wait until the next
            // extraction to determine (after fetching the next page and finding it to be empty)
            // that we are done. Fetching the page eagerly is another option, but would be wasteful
            // the overwhelming majority of the time.
            let already_sent_update_with_enough_events = self
                .id_of_last_event_in_last_extracted_update
                .unwrap_or_default()
                >= self.wft_started_event_id;
            if current_events.is_empty() && no_next_page && already_sent_update_with_enough_events {
                // We must return an empty update which also says is contains the final WFT so we
                // know we're done with replay.
                return Ok(HistoryUpdate::from_events(
                    [],
                    self.previous_wft_started_id,
                    self.wft_started_event_id,
                    true,
                    self.has_pending_speculative_updates,
                    self.has_wft_chunking_v2,
                )
                .0);
            }

            if current_events.is_empty() || (no_next_page && !seen_enough_events) {
                // If next page fetching happened, and we still ended up with no or insufficient
                // events, something is wrong. We're expecting there to be more events to be able to
                // extract this update, but server isn't giving us any. We have no choice except to
                // give up and evict.
                error!(
                    current_events=?current_events,
                    no_next_page,
                    seen_enough_events,
                    "We expected to be able to fetch more events but server says there are none"
                );
                return Err(EMPTY_FETCH_ERR.clone());
            }
            let first_event_id = current_events.front().unwrap().event_id;
            // We only *really* have the last WFT if the events go all the way up to at least the
            // WFT started event id. Otherwise we somehow still have partial history.
            let no_more = matches!(self.next_page_token, NextPageToken::Done) && seen_enough_events;
            let (update, extra) = HistoryUpdate::from_events(
                current_events,
                self.previous_wft_started_id,
                self.wft_started_event_id,
                no_more,
                self.has_pending_speculative_updates,
                self.has_wft_chunking_v2,
            );

            // If there are potentially more events and we haven't extracted two WFTs yet, keep
            // trying.
            if !matches!(self.next_page_token, NextPageToken::Done) && update.wft_count < 2 {
                // Unwrap the update and stuff it all back in the queue
                self.event_queue.extend(update.events);
                self.event_queue.extend(extra);
                continue;
            }

            let extra_eid_same = extra
                .first()
                .map(|e| e.event_id == first_event_id)
                .unwrap_or_default();
            // If there are some events at the end of the fetched events which represent only a
            // portion of a complete WFT, retain them to be used in the next extraction.
            self.event_queue = extra.into();
            if !no_more && extra_eid_same {
                // There was not a meaningful WFT in the whole page. We must fetch more.
                continue;
            }
            self.id_of_last_event_in_last_extracted_update =
                update.events.last().map(|e| e.event_id);
            #[cfg(debug_assertions)]
            update.assert_contiguous();
            return Ok(update);
        }
    }

    /// Fetches the next page and adds it to the internal queue.
    /// Returns true if we still have a next page token after fetching.
    async fn get_next_page(&mut self) -> Result<bool, tonic::Status> {
        let history = loop {
            let npt = match mem::replace(&mut self.next_page_token, NextPageToken::Done) {
                // If the last page token we got was empty, we're done.
                NextPageToken::Done => break None,
                NextPageToken::FetchFromStart => vec![],
                NextPageToken::Next(v) => v,
            };
            debug!(run_id=%self.run_id, "Fetching new history page");
            let fetch_res = self
                .client
                .get_workflow_execution_history(self.wf_id.clone(), Some(self.run_id.clone()), npt)
                .instrument(span!(tracing::Level::TRACE, "fetch_history_in_paginator"))
                .await?;

            self.next_page_token = fetch_res.next_page_token.into();

            let history_is_empty = fetch_res
                .history
                .as_ref()
                .map(|h| h.events.is_empty())
                .unwrap_or(true);
            if history_is_empty && matches!(&self.next_page_token, NextPageToken::Next(_)) {
                // If the fetch returned an empty history, but there *was* a next page token,
                // immediately try to get that.
                continue;
            }
            // Async doesn't love recursion so we do this instead.
            break fetch_res.history;
        };

        let queue_back_id = self
            .event_queue
            .back()
            .map(|e| e.event_id)
            .unwrap_or_default();
        self.event_queue.extend(
            history
                .map(|h| h.events)
                .unwrap_or_default()
                .into_iter()
                .skip_while(|e| e.event_id <= queue_back_id),
        );
        if matches!(&self.next_page_token, NextPageToken::Done) {
            // If finished, we need to extend the queue with the final events, skipping any
            // which are already present.
            if let Some(last_event_id) = self.event_queue.back().map(|e| e.event_id) {
                let final_events = mem::take(&mut self.final_events);
                self.event_queue.extend(
                    final_events
                        .into_iter()
                        .skip_while(|e2| e2.event_id <= last_event_id),
                );
            }
        };
        Ok(!matches!(&self.next_page_token, NextPageToken::Done))
    }
}

#[pin_project::pin_project]
struct StreamingHistoryPaginator {
    inner: HistoryPaginator,
    #[pin]
    open_history_request: Option<BoxFuture<'static, Result<(), tonic::Status>>>,
}

impl StreamingHistoryPaginator {
    // Kept since can be used for history downloading
    #[cfg(test)]
    fn new(inner: HistoryPaginator) -> Self {
        Self {
            inner,
            open_history_request: None,
        }
    }
}

impl Stream for StreamingHistoryPaginator {
    type Item = Result<HistoryEvent, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(e) = this.inner.event_queue.pop_front() {
            return Poll::Ready(Some(Ok(e)));
        }
        if this.open_history_request.is_none() {
            // SAFETY: This is safe because the inner paginator cannot be dropped before the future,
            //   and the future won't be moved from out of this struct.
            this.open_history_request.set(Some(unsafe {
                transmute::<
                    BoxFuture<'_, Result<(), tonic::Status>>,
                    BoxFuture<'static, Result<(), tonic::Status>>,
                >(this.inner.get_next_page().map_ok(|_| ()).boxed())
            }));
        }
        let history_req = this.open_history_request.as_mut().as_pin_mut().unwrap();

        match Future::poll(history_req, cx) {
            Poll::Ready(resp) => {
                this.open_history_request.set(None);
                match resp {
                    Err(neterr) => Poll::Ready(Some(Err(neterr))),
                    Ok(_) => Poll::Ready(this.inner.event_queue.pop_front().map(Ok)),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl HistoryUpdate {
    /// Sometimes it's useful to take an update out of something without needing to use an option
    /// field. Use this to replace the field with an empty update.
    pub(crate) fn dummy() -> Self {
        Self {
            events: vec![],
            previous_wft_started_id: -1,
            wft_started_id: -1,
            has_last_wft: false,
            wft_count: 0,
            has_pending_speculative_updates: false,
            has_wft_chunking_v2: false,
        }
    }

    pub(crate) fn is_real(&self) -> bool {
        self.previous_wft_started_id >= 0
    }

    pub(crate) fn first_event_id(&self) -> Option<i64> {
        self.events.first().map(|e| e.event_id)
    }

    #[cfg(debug_assertions)]
    fn assert_contiguous(&self) -> bool {
        use crate::abstractions::dbg_panic;

        for win in self.events.as_slice().windows(2) {
            if let &[e1, e2] = &win
                && e2.event_id != e1.event_id + 1
            {
                dbg_panic!("HistoryUpdate isn't contiguous! {:?} -> {:?}", e1, e2);
            }
        }
        true
    }

    /// Create an instance of an update directly from events. If the passed in event iterator has a
    /// partial WFT sequence at the end, all events after the last complete WFT sequence (ending
    /// with WFT started) are returned back to the caller, since the history update only works in
    /// terms of complete WFT sequences.
    pub(crate) fn from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
        wft_started_id: i64,
        has_last_wft: bool,
        has_pending_speculative_updates: bool,
        has_wft_chunking_v2: bool,
    ) -> (Self, Vec<HistoryEvent>)
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        let all_events: Vec<_> = events.into_iter().collect();

        Self::from_events_apply(
            all_events,
            previous_wft_started_id,
            wft_started_id,
            has_last_wft,
            has_pending_speculative_updates,
            has_wft_chunking_v2,
        )
    }

    fn from_events_apply(
        mut all_events: Vec<HistoryEvent>,
        previous_wft_started_id: i64,
        wft_started_id: i64,
        has_last_wft: bool,
        has_pending_speculative_updates: bool,
        has_wft_chunking_v2: bool,
    ) -> (Self, Vec<HistoryEvent>) {
        let mut last_end = find_end_index_of_next_wft_seq(
            all_events.as_slice(),
            previous_wft_started_id,
            has_last_wft,
            has_pending_speculative_updates,
            has_wft_chunking_v2,
        );

        if matches!(
            last_end,
            NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail
        ) {
            return if has_last_wft {
                (
                    Self {
                        events: all_events,
                        previous_wft_started_id,
                        wft_started_id,
                        has_last_wft,
                        wft_count: 1,
                        has_pending_speculative_updates,
                        has_wft_chunking_v2,
                    },
                    vec![],
                )
            } else {
                (
                    Self {
                        events: vec![],
                        previous_wft_started_id,
                        wft_started_id,
                        has_last_wft,
                        wft_count: 0,
                        has_pending_speculative_updates,
                        has_wft_chunking_v2,
                    },
                    all_events,
                )
            };
        }

        let mut wft_count = 0;
        while let NextWFTSeqEndIndex::Complete(next_end_ix) = last_end {
            wft_count += 1;
            let next_end_eid = all_events[next_end_ix].event_id;
            let next_end = find_end_index_of_next_wft_seq(
                &all_events[next_end_ix..],
                next_end_eid,
                has_last_wft,
                has_pending_speculative_updates,
                has_wft_chunking_v2,
            )
            .add(next_end_ix);
            if matches!(
                next_end,
                NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail
            ) {
                break;
            }
            last_end = next_end;
        }

        let remaining_events = if all_events.is_empty() || has_last_wft {
            vec![]
        } else {
            all_events.split_off(last_end.end_index_in_slice(all_events.len()) + 1)
        };

        (
            Self {
                events: all_events,
                previous_wft_started_id,
                wft_started_id,
                has_last_wft,
                wft_count,
                has_pending_speculative_updates,
                has_wft_chunking_v2,
            },
            remaining_events,
        )
    }

    /// Create an instance of an update directly from events. The passed in events *must* consist
    /// of one or more complete WFT sequences. IE: The event iterator must not end in the middle
    /// of a WFT sequence.
    #[cfg(test)]
    fn new_from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
        wft_started_id: i64,
        has_last_wft: bool,
        has_pending_speculative_updates: bool,
        has_wft_chunking_v2: bool,
    ) -> Self
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        Self {
            events: events.into_iter().collect(),
            previous_wft_started_id,
            wft_started_id,
            has_last_wft,
            wft_count: 0,
            has_pending_speculative_updates,
            has_wft_chunking_v2,
        }
    }

    /// Given a workflow task started id, return all events starting at that number (exclusive) to
    /// the next WFT started event (inclusive).
    ///
    /// Events are *consumed* by this process, to keep things efficient in workflow machines.
    ///
    /// If we are out of WFT sequences that can be yielded by this update, it will return an empty
    /// vec, indicating more pages will need to be fetched.
    pub(crate) fn take_next_wft_sequence(&mut self, from_wft_started_id: i64) -> NextWFT {
        // First, drop any events from the queue which are earlier than the passed-in id.
        if let Some(ix_first_relevant) =
            starting_index_after_skipping(&self.events, from_wft_started_id)
        {
            self.events.drain(0..ix_first_relevant);
        }

        let chunk = find_end_index_of_next_wft_seq(
            &self.events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
            self.has_wft_chunking_v2,
        );

        match chunk {
            NextWFTSeqEndIndex::NeedMore => NextWFT::NeedFetch,
            NextWFTSeqEndIndex::Tail => {
                if !self.has_last_wft {
                    // We don't have the full history yet; what looks like tail events may
                    // just be the end of the current page. Fetch more.
                    NextWFT::NeedFetch
                } else if self.events.is_empty() {
                    NextWFT::ReplayOver
                } else {
                    // Remaining events are trailing matter (e.g. terminal events, WFTCompleted
                    // + commands after the last WFTStarted). Include them all so the caller
                    // can process them (e.g. to set have_seen_terminal_event).
                    self.build_next_wft(self.events.len() - 1)
                }
            }
            NextWFTSeqEndIndex::Complete(next_wft_ix) => self.build_next_wft(next_wft_ix),
        }
    }

    fn build_next_wft(&mut self, drain_this_much: usize) -> NextWFT {
        NextWFT::WFT(
            self.events.drain(0..=drain_this_much).collect(),
            self.events.is_empty() && self.has_last_wft,
        )
    }

    /// Lets the caller peek ahead at the next WFT sequence that will be returned by
    /// [take_next_wft_sequence]. Will always return the first available WFT sequence if that has
    /// not been called first. May also return an empty iterator or incomplete sequence if we are at
    /// the end of history.
    pub(crate) fn peek_next_wft_sequence(&self, from_wft_started_id: i64) -> &[HistoryEvent] {
        let ix_first_relevant =
            starting_index_after_skipping(&self.events, from_wft_started_id).unwrap_or_default();

        let relevant_events = &self.events[ix_first_relevant..];
        if relevant_events.is_empty() {
            return relevant_events;
        }

        let ix_end = find_end_index_of_next_wft_seq(
            relevant_events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
            self.has_wft_chunking_v2,
        )
        .end_index_in_slice(relevant_events.len());

        &relevant_events[0..=ix_end]
    }

    /// Returns true if this update has the next needed WFT sequence, false if events will need to
    /// be fetched in order to create a complete update with the entire next WFT sequence.
    pub(crate) fn can_take_next_wft_sequence(&self, from_wft_started_id: i64) -> bool {
        let next_wft_ix = find_end_index_of_next_wft_seq(
            &self.events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
            self.has_wft_chunking_v2,
        );
        match next_wft_ix {
            NextWFTSeqEndIndex::NeedMore => false,
            NextWFTSeqEndIndex::Tail => self.has_last_wft,
            NextWFTSeqEndIndex::Complete(_) => true,
        }
    }

    /// Returns the next WFT completed event attributes, if any, starting at (inclusive) the
    /// `from_id`
    pub(crate) fn peek_next_wft_completed(
        &self,
        from_id: i64,
    ) -> Option<&WorkflowTaskCompletedEventAttributes> {
        self.events
            .iter()
            .skip_while(|e| e.event_id < from_id)
            .find_map(|e| match &e.attributes {
                Some(Attributes::WorkflowTaskCompletedEventAttributes(a)) => Some(a),
                _ => None,
            })
    }
}

fn starting_index_after_skipping(
    events: &[HistoryEvent],
    from_wft_started_id: i64,
) -> Option<usize> {
    events
        .iter()
        .find_position(|e| e.event_id > from_wft_started_id)
        .map(|(ix, _)| ix)
}

/// Returns true if any WFTCompleted event in the given events carries the
/// `WftChunkingV2` flag.
fn events_have_wft_chunking_v2(events: &[HistoryEvent]) -> bool {
    let flag_value = CoreInternalFlags::WftChunkingV2 as u32;
    events.iter().any(|e| {
        if let Some(Attributes::WorkflowTaskCompletedEventAttributes(ref attr)) = e.attributes
            && let Some(ref metadata) = attr.sdk_metadata
        {
            metadata.core_used_flags.contains(&flag_value)
        } else {
            false
        }
    })
}

/// Dispatches to v1 (legacy) or v2 chunking based on the `has_wft_chunking_v2` flag.
fn find_end_index_of_next_wft_seq(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
    has_pending_speculative_updates: bool,
    has_wft_chunking_v2: bool,
) -> NextWFTSeqEndIndex {
    if has_wft_chunking_v2 {
        find_end_index_of_next_wft_seq_v2(
            events,
            from_event_id,
            has_last_wft,
            has_pending_speculative_updates,
        )
    } else {
        find_end_index_of_next_wft_seq_v1(events, from_event_id, has_last_wft)
    }
}

/// Legacy chunking algorithm. Used for workflows that were started before the
/// `WftChunkingV2` flag was introduced.
fn find_end_index_of_next_wft_seq_v1(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
) -> NextWFTSeqEndIndex {
    if events.is_empty() {
        return if has_last_wft {
            NextWFTSeqEndIndex::Tail
        } else {
            NextWFTSeqEndIndex::NeedMore
        };
    }
    let mut last_index;
    let mut saw_command_or_started = false;
    let mut saw_command = false;
    let mut wft_started_event_id_to_index = vec![];
    for (ix, e) in events.iter().enumerate() {
        last_index = ix;

        if e.event_id <= from_event_id {
            continue;
        }

        if e.is_command_event() {
            saw_command = true;
            saw_command_or_started = true;
        }
        if e.event_type() == EventType::WorkflowExecutionStarted {
            saw_command_or_started = true;
        }
        if e.is_final_wf_execution_event() {
            return NextWFTSeqEndIndex::Complete(last_index);
        }

        if e.event_type() == EventType::WorkflowTaskStarted {
            wft_started_event_id_to_index.push((e.event_id, ix));
            if let Some(next_event) = events.get(ix + 1) {
                let next_event_type = next_event.event_type();
                if matches!(
                    next_event_type,
                    EventType::WorkflowTaskFailed
                        | EventType::WorkflowTaskTimedOut
                        | EventType::WorkflowExecutionTimedOut
                        | EventType::WorkflowExecutionTerminated
                        | EventType::WorkflowExecutionCanceled
                ) {
                    wft_started_event_id_to_index.pop();
                    continue;
                } else if next_event_type == EventType::WorkflowTaskCompleted {
                    if let Some(next_next_event) = events.get(ix + 2) {
                        if !saw_command
                            && next_next_event.event_type() == EventType::WorkflowTaskScheduled
                        {
                            continue;
                        } else {
                            if let Some(Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(
                                ref attr,
                            )) = next_next_event.attributes
                                && let Some(ret_ix) = wft_started_event_id_to_index
                                    .iter()
                                    .rev()
                                    .find_map(|(eid, ix)| {
                                        if *eid < attr.accepted_request_sequencing_event_id {
                                            return Some(*ix);
                                        }
                                        None
                                    })
                            {
                                return NextWFTSeqEndIndex::Complete(ret_ix);
                            }
                            return NextWFTSeqEndIndex::Complete(ix);
                        }
                    } else if !has_last_wft && !saw_command_or_started {
                        continue;
                    }
                }
            } else if !has_last_wft && !saw_command_or_started {
                continue;
            }
            if saw_command_or_started {
                return NextWFTSeqEndIndex::Complete(ix);
            }
        }
    }

    // Legacy: Incomplete maps to NeedMore when !has_last_wft; the caller handles
    // has_last_wft by treating all remaining events as a single WFT.
    if has_last_wft {
        NextWFTSeqEndIndex::Tail
    } else {
        NextWFTSeqEndIndex::NeedMore
    }
}

#[derive(Debug, Copy, Clone)]
enum NextWFTSeqEndIndex {
    /// The next Logical WFT sequence is completely contained within the passed-in slice.
    /// The index corresponds to the index of the last `WorkflowTaskStarted` event.
    Complete(usize),

    /// Not enough events in the slice to positively determine the next WFT boundary.
    /// The caller should fetch more events before attempting to chunk again.
    NeedMore,

    /// No more WFT boundaries exist in this slice. Any remaining events are trailing matter
    /// after the last WFT (e.g. terminal `WorkflowExecution*` events, `WorkflowTaskCompleted`
    /// with its commands). These events still need to be processed by the caller.
    Tail,
}

impl NextWFTSeqEndIndex {
    /// Last event index within a slice of length `slice_len` that this result refers to.
    fn end_index_in_slice(self, slice_len: usize) -> usize {
        match self {
            NextWFTSeqEndIndex::Complete(ix) => ix,
            NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail => slice_len.saturating_sub(1),
        }
    }

    fn add(self, val: usize) -> Self {
        match self {
            NextWFTSeqEndIndex::Complete(ix) => NextWFTSeqEndIndex::Complete(ix + val),
            NextWFTSeqEndIndex::NeedMore => NextWFTSeqEndIndex::NeedMore,
            NextWFTSeqEndIndex::Tail => NextWFTSeqEndIndex::Tail,
        }
    }
}

/// Return the event _index_ (not ID!) of the last event of the logical workflow task starting
/// at event ID `from_event_id`. The logical WFT is guaranteed to be "complete", meaning that all
/// events required to process that logical WFT are contained in the provided slice.
///
/// Returns one of three variants:
///
/// - `Complete(ix)` — the WFT boundary is at the `WorkflowTaskStarted` event at index `ix`.
///   All events required to process the LWFT are present in the slice.
/// - `NeedMore` — not enough events to determine the boundary; the caller should fetch more
///   history pages before retrying. This can happen when the slice ends at a point where
///   look-ahead is required (e.g. `WFTStarted → WFTCompleted → EOS` with `!has_last_wft`).
/// - `Tail` — no more WFT boundaries exist in the remaining events. Any events still in the
///   slice are trailing matter after the last WFT (e.g. terminal `WorkflowExecution*` events,
///   `WorkflowTaskCompleted` + commands). The caller must still process these events (e.g. to
///   set `have_seen_terminal_event`).
///
/// When `has_last_wft` is true, the slice is the full history for this update: a trailing
/// `WorkflowTaskStarted` with no following event (open task) **is** a `Complete` boundary at
/// that started event—there is no further history to page in that could change the decision.
///
/// The index returned by `Complete(x)` always corresponds to the event index of a
/// `WorkflowTaskStarted` event.
///
/// A logical WFT may span multiple real WFTs in history, in the following cases:
///
/// - Empty Workflow Tasks sequences, like those resulting from WFT heartbeats;
/// - WFT attempts that failed or timed out.
///
/// In both cases, the ignored wft is swallowed by the _following_ workflow task,
/// resulting in a single logical workflow task.
fn find_end_index_of_next_wft_seq_v2(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
    has_pending_speculative_updates: bool,
) -> NextWFTSeqEndIndex {
    use EventType::*;
    use NextWFTSeqEndIndex::*;

    if events.is_empty() {
        return if has_last_wft { Tail } else { NeedMore };
    }

    // It's possible to have gotten a new history update without eviction (ex: unhandled
    // command on completion), where we may need to skip events we already handled.
    let mut ix = starting_index_after_skipping(events, from_event_id).unwrap_or(events.len());

    // Set to true if we've seen any event that prevents extending the present LWFT past the next `WFTStarted` event.
    let mut prevent_heartbeat = false;

    // Skip the initial `WFExecutionStarted` event, if present.
    //
    // Consume `WFExecutionStarted?`
    if let Some(WorkflowExecutionStarted) = events.get(ix).map(|e| e.event_type()) {
        ix += 1;
        prevent_heartbeat = true;
    }

    // We're at the begining of a LWFT. Any command here results from the _previous_ WFT,
    // and therefore shouldn't affect chunking of the present LWFT, besides
    //
    // Consume `(WFTCompleted -> Command*)?`
    if let Some(WorkflowTaskCompleted) = events.get(ix).map(|e| e.event_type()) {
        ix += 1; // WFTCompleted

        while ix < events.len() {
            if !events[ix].is_command_event() {
                break;
            }

            prevent_heartbeat = true;
            ix += 1; // Command
        }
    }

    // From this point on, there should be:
    // `InboundEvent* -> WFTScheduled -> WFTStarted -> WFTCompleted -> Command*`
    while ix < events.len() {
        // let ahead = &events[ix + 1..events.len().min(ix + 6)];
        // let ahead: Vec<_> = ahead.iter().map(|e| e.event_type()).collect();

        let e0 = &events[ix];
        let e1 = events.get(ix + 1);
        let e2 = events.get(ix + 2);
        let e3 = events.get(ix + 3);
        let e4 = events.get(ix + 4);
        let e5 = events.get(ix + 5);

        match e0.event_type() {
            // WFTStarted -> ...
            EventType::WorkflowTaskStarted => {
                match e1.map(|e| e.event_type()) {
                    // WFTStarted -> EOH
                    None if has_last_wft => {
                        // History ends on this WFTStarted.
                        // Conclusion is safe and replay is over after this LWFT.
                        return NextWFTSeqEndIndex::Complete(ix);
                    }

                    // WFTStarted -> (unknown)
                    None /* !has_last_wft */ => {
                        // Can't conclude yet: unknown could be a WFTCompleted, WFTFailed, or WFTTimedOut event.
                        return NextWFTSeqEndIndex::NeedMore;
                    }

                    // WFTStarted -> WFTCompleted -> ...
                    Some(EventType::WorkflowTaskCompleted) => {
                        match e2.map(|e| e.event_type()) {
                            // WFTStarted -> WFTCompleted -> EOH
                            None if has_last_wft => {
                                // There's no more event to look ahead.
                                // It is safe to conclude the LWFT at the current WFTStarted event.
                                return NextWFTSeqEndIndex::Complete(ix);
                            }

                            // WFTStarted -> WFTCompleted -> (unknown)
                            None /* !has_last_wft */ => {
                                // Can't conclcude yet, as unknown could be a WFTScheduled or UpdateAccepted event.
                                // Note that we are not making an exception for prevent_heartbeat=true here,
                                // because we'd still need to if there's an UpdateAccepted event ahead.
                                return NextWFTSeqEndIndex::NeedMore;
                            }

                            // WFTStarted -> WFTCompleted -> WFTScheduled -> ...
                            Some(EventType::WorkflowTaskScheduled) => {
                                if prevent_heartbeat {
                                    // For some reason (e.g. we saw a command preceding this WFTStarted), we know
                                    // that we can't collapse the current WFT with the one ahead, and we've seen
                                    // one event that can't belong to the current WFT (the WFTScheduled), so it
                                    // is safe to conclude a Complete LWFT at the current WFTStarted event.
                                    return NextWFTSeqEndIndex::Complete(ix);
                                }

                                match e3.map(|e| e.event_type()) {
                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> EOH
                                    None if has_last_wft => {
                                            // History ends on this WFTScheduled. That's somewhat unexpected,
                                            // but still means there can't be nothing affecting decision on the
                                            // present LWFT, so it is safe to conclude a Complete LWFT
                                            // at the current WFTStarted event.
                                            return NextWFTSeqEndIndex::Complete(ix);
                                    }

                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> (unknown)
                                    None /* !has_last_wft */ => {
                                        // There might be more events ahead that would affect the conclusion,
                                        // e.g. a `WFTScheduled -> WFTStarted` sequence that would make this
                                        // a heartbeat. Delay the conclusion until we see more events.
                                        return NextWFTSeqEndIndex::NeedMore;
                                    }

                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> ...
                                    Some(EventType::WorkflowTaskStarted) => {
                                        match e4.map(|e| e.event_type()) {
                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> EOH
                                            None if has_last_wft => {
                                                if has_pending_speculative_updates {
                                                    // There's a pending speculative update, which necessarily affects
                                                    // the last WFTStarted event, which is the one we're looking ahead
                                                    // to. We therefore can't collapse the current WFT (WFTStarted at ix)
                                                    // with the one ahead (WFTStarted at ix + 3).
                                                    return NextWFTSeqEndIndex::Complete(ix);
                                                } else {
                                                    // We got a full noop WFT sequence. Collapse the current WFT
                                                    // (WFTStarted at ix) with the one ahead (WFTStarted at ix + 3),
                                                    // and return that as this is the final event in history.
                                                    return NextWFTSeqEndIndex::Complete(ix + 3);
                                                }
                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)
                                            None /* !has_last_wft */ => {
                                                // Can't conclude yet: unknown could be a WFTCompleted, WFTFailed, or WFTTimedOut.
                                                return NextWFTSeqEndIndex::NeedMore;
                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> ...
                                            Some(EventType::WorkflowTaskCompleted) => {
                                                match e5.map(|e| e.event_type()) {
                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> EOH
                                                    None if has_last_wft => {
                                                        assert!(!has_pending_speculative_updates);

                                                        // We got a full noop WFT sequence. Collapse the current WFT
                                                        // (WFTStarted at ix) with the one ahead (WFTStarted at ix + 3),
                                                        // and return that as this is the final event in history.
                                                        return NextWFTSeqEndIndex::Complete(ix + 3);
                                                    }

                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)
                                                    None /* !has_last_wft */ => {
                                                        // Can't conclude yet, as unknown could be a WFTStarted, WFTFailed, or WFTTimedOut event.
                                                        return NextWFTSeqEndIndex::NeedMore;
                                                    }

                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> UpdateAccepted -> ...
                                                    Some(EventType::WorkflowExecutionUpdateAccepted) => {
                                                        // Found an UpdateAccepted event, which must affect the WFTStarted at ix + 3.
                                                        // That means we can't collapse the current WFT (WFTStarted at ix) with the
                                                        // one ahead (WFTStarted at ix + 3). Conclude the current WFTStarted event.
                                                        return NextWFTSeqEndIndex::Complete(ix);
                                                    }

                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> <something else>
                                                    Some(_) => {
                                                        // We found a full noop WFT sequence (ix..ix+3), and we've looked
                                                        // ahead far enough to be sure that we won't need to walk back on
                                                        // previous WFTStarted events. Jump ahead to the next WFTStarted
                                                        // event, and continue the loop.
                                                        ix += 3; // WFTStarted + WFTCompleted + WFTScheduled
                                                        continue;
                                                    }
                                                }

                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> <something else>
                                            Some(_) => {
                                                return NextWFTSeqEndIndex::Complete(ix);
                                            }
                                        }
                                    }

                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> <something else>
                                    Some(_) => {
                                        return NextWFTSeqEndIndex::Complete(ix);
                                    }
                                }
                            }

                            // WFTStarted -> WFTCompleted -> <something else>
                            Some(_) => {
                                return NextWFTSeqEndIndex::Complete(ix);
                            }
                        }
                    }

                    // WFTStarted -> WFT(Failed|TimedOut) -> ...
                    Some(EventType::WorkflowTaskFailed) | Some(EventType::WorkflowTaskTimedOut) => {
                        // Failed WFT. Skip over it.
                        ix += 2; // Started + Failed/TimedOut
                        continue;
                    }

                    // Workflow execution terminates after WFTStarted without WFTCompleted.
                    // Complete points at the WFTStarted; the terminal event is left as
                    // trailing matter (will be returned as `Tail` on the next call).
                    // `WFTStarted -> WFExecution(Terminated|TimedOut|...)`
                    Some(_) if e1.is_some_and(|e| e.is_final_wf_execution_event()) => {
                        return NextWFTSeqEndIndex::Complete(ix);
                    }

                    // `WFTStarted -> <something else>`
                    Some(_) => {
                        panic!(
                            "Unexpected event type: {:?} after WorkflowTaskStarted event, {:?}",
                            e0.event_type(),
                            events
                        );
                    }
                }
            }

            // Sudden workflow execution termination. That's the end of history,
            // but we still don't have a "complete" LWFT. The terminal event is trailing
            // matter that the caller must still process (to set have_seen_terminal_event).
            // `WFExecution(Failed|TimedOut|Canceled|Terminated|TimedOut|CAN)`
            _ if e0.is_final_wf_execution_event() => {
                if e1.is_some() || !has_last_wft || has_pending_speculative_updates {
                    panic!(
                        "{:?} event at index {ix} is not the last event in history",
                        e0.event_type()
                    );
                }
                return Tail;
            }

            // Just skip over any other event type.
            _ => {
                if e0.is_command_event() {
                    // This case is theoretically impossible, unless either the workflow
                    // history is malformed or we hit a bug in this chunking logic.
                    dbg_panic!(
                        "Command event at index {ix} is not expected after seeing a non-command event"
                    );
                }

                if e0.is_wft_time_sensitive_event() {
                    prevent_heartbeat = true;
                }

                ix += 1;
                continue;
            }
        }

        #[allow(unreachable_code)]
        {
            panic!("All match arms above must diverge (return/continue/panic)");
        }
    }

    // Fell off the main loop without finding a WFTStarted.
    if has_last_wft {
        // This is the last WFT in history. Any events consumed by the preamble (WFTCompleted + commands)
        // or remaining inbound events are trailing matter.
        NextWFTSeqEndIndex::Tail
    } else {
        // There might be a WFTStarted event ahead, but we'll need to fetch more events to find it.
        NextWFTSeqEndIndex::NeedMore
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        replay::{HistoryInfo, TestHistoryBuilder, canned_histories},
        test_help::{ResponseType, hist_to_poll_resp},
        worker::client::mocks::mock_worker_client,
    };
    use futures_util::TryStreamExt;
    use temporalio_common::protos::temporal::api::{
        common::v1::WorkflowExecution,
        enums::v1::WorkflowTaskFailedCause,
        history::v1::{History, history_event::Attributes},
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, update_activity_options_request,
        },
    };

    impl From<HistoryInfo> for HistoryUpdate {
        fn from(v: HistoryInfo) -> Self {
            let events = v.events().to_vec();
            let has_chunking_v2 = events_have_wft_chunking_v2(&events);
            Self::new_from_events(
                events,
                v.previous_started_event_id(),
                v.workflow_task_started_event_id(),
                true,
                false,
                has_chunking_v2,
            )
        }
    }

    trait TestHBExt {
        fn as_history_update(&self) -> HistoryUpdate;
    }

    impl TestHBExt for TestHistoryBuilder {
        fn as_history_update(&self) -> HistoryUpdate {
            self.get_full_history_info().unwrap().into()
        }
    }

    /// Retroactively sets the `WftChunkingV2` flag on the first
    /// WFTCompleted event in an already-constructed builder (for canned histories).
    fn maybe_set_chunking_v2(t: &mut TestHistoryBuilder, chunking_v2: bool) {
        if chunking_v2 {
            use crate::internal_flags::CoreInternalFlags;
            t.set_flags_first_wft(&[CoreInternalFlags::WftChunkingV2], &[]);
        }
    }

    impl NextWFT {
        fn unwrap_events(self) -> Vec<HistoryEvent> {
            match self {
                NextWFT::WFT(e, _) => e,
                o => panic!("Must be complete WFT: {o:?}"),
            }
        }

        fn is_complete(&self) -> bool {
            match self {
                NextWFT::WFT(_, true) => true,
                _ => false,
            }
        }
    }

    fn next_check_peek(update: &mut HistoryUpdate, from_id: i64) -> Vec<HistoryEvent> {
        let seq_peeked = update.peek_next_wft_sequence(from_id).to_vec();
        let seq = update.take_next_wft_sequence(from_id).unwrap_events();
        assert_eq!(seq, seq_peeked);
        seq
    }

    fn next_check_peek2(update: &mut HistoryUpdate, from_id: i64) -> (usize, bool) {
        let seq_peek = update.peek_next_wft_sequence(from_id).to_vec();
        let next = update.take_next_wft_sequence(from_id);
        let is_complete = next.is_complete();
        let seq_take = next.unwrap_events();
        assert_eq!(seq_take, seq_peek);
        (seq_take.len(), is_complete)
    }

    #[rstest::rstest]
    #[test]
    fn consumes_standard_wft_sequence(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let mut update = timer_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2_peeked = update.peek_next_wft_sequence(0).to_vec();
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2, seq_2_peeked);
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[rstest::rstest]
    #[test]
    fn skips_wft_failed(#[values(false, true)] chunking_v2: bool) {
        let mut failed_hist = canned_histories::workflow_fails_with_reset_after_timer("t", "runid");
        maybe_set_chunking_v2(&mut failed_hist, chunking_v2);
        let mut update = failed_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2.len(), 8);
        assert_eq!(seq_2.last().unwrap().event_id, 11);
    }

    #[rstest::rstest]
    #[test]
    fn skips_wft_timeout(#[values(false, true)] chunking_v2: bool) {
        let mut failed_hist = canned_histories::wft_timeout_repro();
        maybe_set_chunking_v2(&mut failed_hist, chunking_v2);
        let mut update = failed_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2.len(), 11);
        assert_eq!(seq_2.last().unwrap().event_id, 14);
    }

    #[rstest::rstest]
    #[test]
    fn skips_events_before_desired_wft(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let mut update = timer_hist.as_history_update();
        // We haven't processed the first 3 events, but we should still only get the second sequence
        let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[rstest::rstest]
    #[test]
    fn history_ends_abruptly(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        timer_hist.add_workflow_execution_terminated();
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let mut update = timer_hist.as_history_update();
        let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
        if chunking_v2 {
            // New algorithm: terminal event is not part of the WFTStarted LWFT.
            assert_eq!(seq_2.len(), 5);
            assert_eq!(seq_2.last().unwrap().event_id, 8);
            let seq_3 = update.take_next_wft_sequence(8).unwrap_events();
            assert_eq!(seq_3.len(), 1);
            assert!(seq_3[0].is_final_wf_execution_event());
            assert_matches!(update.take_next_wft_sequence(8), NextWFT::ReplayOver);
        } else {
            // Legacy algorithm: terminal event included in the WFTStarted LWFT.
            assert_eq!(seq_2.len(), 6);
            assert_eq!(seq_2.last().unwrap().event_id, 9);
            assert!(seq_2.last().unwrap().is_final_wf_execution_event());
        }
    }

    /// Verifies that non-command terminal events (`WorkflowExecutionTerminated`,
    /// `WorkflowExecutionTimedOut`) following a `WorkflowTaskStarted` are returned as
    /// trailing tail events rather than being silently dropped. This is critical because
    /// callers need to process them to set `have_seen_terminal_event`.
    #[rstest::rstest]
    #[test]
    fn terminal_events_not_dropped_after_wft_started(#[values(false, true)] chunking_v2: bool) {
        // Test both non-command terminal event types that can follow WFTStarted.
        for add_terminal in [
            TestHistoryBuilder::add_workflow_execution_terminated as fn(&mut TestHistoryBuilder),
            TestHistoryBuilder::add_workflow_execution_timed_out,
        ] {
            let mut t = TestHistoryBuilder::default();
            t.add_by_type(EventType::WorkflowExecutionStarted);
            t.add_full_wf_task(); // Sched(2), Started(3), Completed(4)
            t.add_by_type(EventType::TimerStarted); // TimerStarted(5)
            t.add_workflow_task_scheduled_and_started(); // Sched(6), Started(7)
            add_terminal(&mut t); // terminal(8)
            maybe_set_chunking_v2(&mut t, chunking_v2);

            let mut update = t.as_history_update();
            let seq_1 = update.take_next_wft_sequence(0).unwrap_events();
            assert_eq!(seq_1.last().unwrap().event_id, 3);

            if chunking_v2 {
                let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
                assert_eq!(seq_2.last().unwrap().event_id, 7);
                assert_eq!(
                    seq_2.last().unwrap().event_type(),
                    EventType::WorkflowTaskStarted
                );
                let seq_3 = update.take_next_wft_sequence(7).unwrap_events();
                assert_eq!(seq_3.len(), 1);
                assert!(seq_3[0].is_final_wf_execution_event());
                assert_matches!(update.take_next_wft_sequence(7), NextWFT::ReplayOver);
            } else {
                let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
                assert_eq!(seq_2.last().unwrap().event_id, 8);
                assert!(seq_2.last().unwrap().is_final_wf_execution_event());
            }
        }
    }

    #[rstest::rstest]
    #[test]
    fn heartbeats_skipped(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whee", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_history_update();
        if chunking_v2 {
            // v2 treats WFExecutionStarted as time-sensitive,
            // so the first WFT can't be collapsed with the next.
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(seq.len(), 3);
            let seq = next_check_peek(&mut update, 3);
            assert_eq!(seq.len(), 3);
        } else {
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(seq.len(), 6);
        }
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 10);
        assert_eq!(seq.len(), 9);
        let seq = next_check_peek(&mut update, 19);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 23);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 27);
        assert_eq!(seq.len(), 2);
    }

    #[rstest::rstest]
    #[test]
    fn heartbeat_marker_end(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_local_activity_result_marker(1, "1", "done".into());
        t.add_workflow_execution_completed();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 3);
    }

    fn paginator_setup(history: TestHistoryBuilder, chunk_size: usize) -> HistoryPaginator {
        let hinfo = history.get_full_history_info().unwrap();
        let wft_started = hinfo.workflow_task_started_event_id();
        let full_hist = hinfo.into_events();
        let initial_hist = full_hist.chunks(chunk_size).next().unwrap().to_vec();
        let mut mock_client = mock_worker_client();

        let mut npt = 1;
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, passed_npt| {
                assert_eq!(passed_npt, vec![npt]);
                let mut hist_chunks = full_hist.chunks(chunk_size).peekable();
                let next_chunks = hist_chunks.nth(npt.into()).unwrap_or_default();
                npt += 1;
                let next_page_token = if hist_chunks.peek().is_none() {
                    vec![]
                } else {
                    vec![npt]
                };
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History {
                        events: next_chunks.into(),
                    }),
                    raw_history: vec![],
                    next_page_token,
                    archived: false,
                })
            });

        HistoryPaginator::new(
            History {
                events: initial_hist,
            },
            0,
            wft_started,
            "wfid".to_string(),
            "runid".to_string(),
            vec![1],
            Arc::new(mock_client),
            false,
        )
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_extracts_updates(
        #[values(10, 11, 12, 13, 14)] chunk_size: usize,
        #[values(false, true)] chunking_v2: bool,
    ) {
        let wft_count = 100;
        let mut hist = canned_histories::long_sequential_timers(wft_count);
        let expected_final_eid = hist
            .get_full_history_info()
            .unwrap()
            .into_events()
            .last()
            .unwrap()
            .event_id;
        maybe_set_chunking_v2(&mut hist, chunking_v2);

        let mut paginator = paginator_setup(hist, chunk_size);
        let mut update = paginator.extract_next_update().await.unwrap();
        let mut last_id = 0;
        loop {
            let seq = loop {
                match update.take_next_wft_sequence(last_id) {
                    NextWFT::WFT(seq, _) => break seq,
                    NextWFT::NeedFetch => {
                        update = paginator.extract_next_update().await.unwrap();
                    }
                    NextWFT::ReplayOver => {
                        assert_eq!(last_id, expected_final_eid);
                        return;
                    }
                }
            };
            assert!(!seq.is_empty());
            for e in &seq {
                assert!(
                    e.event_id > last_id,
                    "event ids must increase monotonically (last_id={last_id}, got {})",
                    e.event_id
                );
                last_id = e.event_id;
            }
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_streams(#[values(false, true)] chunking_v2: bool) {
        let wft_count = 10;
        let mut hist = canned_histories::long_sequential_timers(wft_count);
        maybe_set_chunking_v2(&mut hist, chunking_v2);
        let paginator = StreamingHistoryPaginator::new(paginator_setup(hist, 10));
        let everything: Vec<_> = paginator.try_collect().await.unwrap();
        assert_eq!(everything.len(), (wft_count + 1) * 5);
        everything.iter().fold(1, |event_id, e| {
            assert_eq!(event_id, e.event_id);
            e.event_id + 1
        });
    }

    fn three_wfts_then_heartbeats() -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        // Start with two complete normal WFTs
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // wft start - 3
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft start - 7
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft start - 11
        for _ in 1..50 {
            // Add a bunch of heartbeats with no commands, which count as one task
            t.add_full_wf_task();
        }
        t.add_workflow_execution_completed();
        t
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn needs_fetch_if_ending_in_middle_of_wft_seq(
        // These values test points truncation could've occurred in the middle of the heartbeat
        #[values(18, 19, 20, 21)] truncate_at: usize,
        #[values(false, true)] chunking_v2: bool,
    ) {
        let mut t = three_wfts_then_heartbeats();
        maybe_set_chunking_v2(&mut t, chunking_v2);
        let mut ends_in_middle_of_seq = t.as_history_update().events;
        ends_in_middle_of_seq.truncate(truncate_at);
        // The update should contain the first three complete WFTs, ending on the 11th event which
        // is WFT started. The remaining events should be returned. False flags means the creator
        // knows there are more events, so we should return need fetch
        let (mut update, remaining) = HistoryUpdate::from_events(
            ends_in_middle_of_seq,
            0,
            t.get_full_history_info()
                .unwrap()
                .workflow_task_started_event_id(),
            false,
            false,
            chunking_v2,
        );
        assert_eq!(remaining[0].event_id, 12);
        assert_eq!(remaining.last().unwrap().event_id, truncate_at as i64);
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        if chunking_v2 {
            // New algorithm: the third logical WFT ends at `WorkflowTaskStarted` (id 11), but
            // the buffer has no following event — `find_end` returns NeedMore until more history
            // exists.
            let next = update.take_next_wft_sequence(7);
            assert_matches!(next, NextWFT::NeedFetch);
        } else {
            // Legacy algorithm: less conservative, yields WFT3 immediately then NeedFetch for
            // the next call.
            let seq = update.take_next_wft_sequence(7).unwrap_events();
            assert_eq!(seq.last().unwrap().event_id, 11);
            let next = update.take_next_wft_sequence(11);
            assert_matches!(next, NextWFT::NeedFetch);
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_works_with_wft_over_multiple_pages(
        #[values(10, 11, 12, 13, 14)] chunk_size: usize,
        #[values(false, true)] chunking_v2: bool,
    ) {
        let mut t = three_wfts_then_heartbeats();
        maybe_set_chunking_v2(&mut t, chunking_v2);
        let mut paginator = paginator_setup(t, chunk_size);
        let mut update = paginator.extract_next_update().await.unwrap();
        let mut last_id = 0;
        loop {
            let seq = update.take_next_wft_sequence(last_id);
            match seq {
                NextWFT::WFT(seq, _) => {
                    last_id = seq.last().unwrap().event_id;
                }
                NextWFT::NeedFetch => {
                    update = paginator.extract_next_update().await.unwrap();
                }
                NextWFT::ReplayOver => break,
            }
        }
        assert_eq!(last_id, 160);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn task_just_before_heartbeat_chain_is_taken(#[values(false, true)] chunking_v2: bool) {
        let mut t = three_wfts_then_heartbeats();
        maybe_set_chunking_v2(&mut t, chunking_v2);
        let mut update = t.as_history_update();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = update.take_next_wft_sequence(7).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 11);
        let seq = update.take_next_wft_sequence(11).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 158);
        let seq = update.take_next_wft_sequence(158).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 160);
        assert_eq!(
            seq.last().unwrap().event_type(),
            EventType::WorkflowExecutionCompleted
        );
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn handles_cache_misses(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let mut history_from_get: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_history_info(2).unwrap().into();
        // Chop off the last event, which is WFT started, which server doesn't return in get
        // history
        history_from_get.history.as_mut().map(|h| h.events.pop());
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(history_from_get.clone()));

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        // We expect if we try to take the first task sequence that the first event is the first
        // event in the sequence.
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq[0].event_id, 1);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        // Verify anything extra (which should only ever be WFT started) was re-appended to the
        // end of the event iteration after fetching the old history.
        assert_eq!(seq.last().unwrap().event_id, 8);
    }

    #[rstest::rstest]
    #[test]
    fn la_marker_chunking(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task(); // started - 7
        t.add_local_activity_result_marker(1, "hi", Default::default());
        let act_s = t.add_activity_task_scheduled("1");
        let act_st = t.add_activity_task_started(act_s);
        t.add_activity_task_completed(act_s, act_st, Default::default());
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_timed_out();
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_timed_out();
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.len(), 13);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn handles_blank_fetch_response(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(Default::default()));

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let err = paginator.extract_next_update().await.unwrap_err();
        assert_matches!(err.code(), tonic::Code::Unknown);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn handles_empty_page_with_next_token(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let full_resp: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_full_history_info().unwrap().into();
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![2],
                    archived: false,
                })
            })
            .times(1);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 8);
        assert_matches!(update.take_next_wft_sequence(8), NextWFT::ReplayOver);
    }

    // TODO: Test we dont re-feed pointless updates if fetching returns <= events we already
    //   processed

    #[rstest::rstest]
    #[tokio::test]
    async fn handles_fetching_page_with_complete_wft_and_page_token_to_empty_page(
        #[values(false, true)] chunking_v2: bool,
    ) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let workflow_task = timer_hist.get_full_history_info().unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();

        let mut full_resp_with_npt: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_full_history_info().unwrap().into();
        full_resp_with_npt.next_page_token = vec![1];

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp_with_npt.clone()))
            .times(1);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![],
                    archived: false,
                })
            })
            .times(1);

        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 8);
        assert_matches!(update.take_next_wft_sequence(8), NextWFT::ReplayOver);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn extreme_pagination_doesnt_drop_wft_events_paginator(
        #[values(false, true)] chunking_v2: bool,
    ) {
        // 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        // 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 3: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- previous_started_event_id
        // 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED

        // 5: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 6: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 7: EVENT_TYPE_WORKFLOW_TASK_STARTED
        // 8: EVENT_TYPE_WORKFLOW_TASK_FAILED

        // 9: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 10: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 11: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 12: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 13: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 14: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 15: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- started_event_id

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::UnhandledCommand,
            Default::default(),
        );

        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut mock_client = mock_worker_client();

        let events: Vec<HistoryEvent> = t.get_full_history_info().unwrap().into_events();
        let first_event = events[0].clone();
        for (i, event) in events.into_iter().enumerate() {
            // Add an empty page
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| {
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History { events: vec![] }),
                        raw_history: vec![],
                        next_page_token: vec![(i * 10) as u8],
                        archived: false,
                    })
                })
                .times(1);

            // Add a page with only event i
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| {
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History {
                            events: vec![event.clone()],
                        }),
                        raw_history: vec![],
                        next_page_token: vec![(i * 10 + 1) as u8],
                        archived: false,
                    })
                })
                .times(1);
        }

        // Add an extra empty page at the end, with no NPT
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![],
                    archived: false,
                })
            })
            .times(1);

        let mut paginator = HistoryPaginator::new(
            History {
                events: vec![first_event],
            },
            3,
            15,
            "wfid".to_string(),
            "runid".to_string(),
            vec![1],
            Arc::new(mock_client),
            false,
        );

        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.first().unwrap().event_id, 1);
        assert_eq!(seq.last().unwrap().event_id, 3);

        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.first().unwrap().event_id, 4);
        assert_eq!(seq.last().unwrap().event_id, 15);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn finding_end_index_with_started_as_last_event(
        #[values(false, true)] chunking_v2: bool,
    ) {
        let wf_id = "fakeid";
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);
        // We need to see more after this - it's not sufficient to end on a started event when
        // we know there might be more

        let workflow_task = t.get_history_info(1).unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();
        let mut wft_resp = workflow_task.as_poll_wft_response();
        wft_resp.workflow_execution = Some(WorkflowExecution {
            workflow_id: wf_id.to_string(),
            run_id: t.get_orig_run_id().to_string(),
        });
        wft_resp.next_page_token = vec![1];

        let mut resp_1: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();
        resp_1.next_page_token = vec![2];

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(resp_1.clone()))
            .times(1);
        // Since there aren't sufficient events, we should try to see another fetch, and that'll
        // say there aren't any
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(Default::default()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        // We're done since the last fetch revealed nothing
        assert_eq!(seq.last().unwrap().event_id, 7);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn just_signal_is_complete_wft(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let workflow_task = t.get_full_history_info().unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();
        let mock_client = mock_worker_client();
        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::Done,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 11);
        assert_eq!(seq.len(), 2);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn heartbeats_then_signal(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        let mut need_fetch_resp =
            hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory).resp;
        need_fetch_resp.next_page_token = vec![1];
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let full_resp: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            need_fetch_resp.history.unwrap(),
            // Pretend we have already processed first WFT
            3,
            6,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::Next(vec![1]),
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        // Starting past first wft
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 9);
        assert_eq!(seq.len(), 4);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn cache_miss_with_only_one_wft_available_orders_properly(
        #[values(false, true)] chunking_v2: bool,
    ) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let incremental_task =
            hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::OneTask(3)).resp;

        let mut mock_client = mock_worker_client();
        let mut one_task_resp: GetWorkflowExecutionHistoryResponse =
            t.get_history_info(1).unwrap().into();
        one_task_resp.next_page_token = vec![1];
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(one_task_resp.clone()))
            .times(1);
        let mut up_to_sched_start: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();
        up_to_sched_start
            .history
            .as_mut()
            .unwrap()
            .events
            .truncate(9);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(up_to_sched_start.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            incremental_task.history.unwrap(),
            6,
            9,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.last().unwrap().event_id, 11);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn wft_fail_on_first_task_with_update(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::Unspecified,
            Default::default(),
        );
        t.add_full_wf_task();
        let accept_id = t.add_update_accepted("1", "upd");
        let timer_id = t.add_timer_started("1".to_string());
        t.add_update_completed(accept_id);
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        // In this case, we expect to see up to the task with update, since the task failure
        // should be skipped. This means that the peek of the _next_ task will include the update
        // and thus properly synthesize the update request with the first activation.
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 7);
    }

    #[rstest::rstest]
    #[test]
    fn update_accepted_after_empty_wft(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        let accept_id = t.add_update_accepted("1", "upd");
        let timer_id = t.add_timer_started("1".to_string());
        t.add_update_completed(accept_id);
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        // unlike the case with a wft failure, here the first task should not extend through to
        // the update, because here the first empty WFT happened with _just_ the workflow init,
        // not also with the update.
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 3);
        //         // Heartbeat: first empty WFT collapses into the second; boundary is the second WFTStarted.
        // assert_eq!(seq.len(), 6);
        // assert_eq!(seq.last().unwrap().event_id, 6);
        // let seq = next_check_peek(&mut update, 6);
        // // Through timer command, next WFTStarted (open until following completion is visible as end index).
        // assert_eq!(seq.len(), 7);
        // assert_eq!(seq.last().unwrap().event_id, 13);
    }

    /// Builds a history with an empty WFT followed by a WFT with an update:
    ///   Event 1:  WorkflowExecutionStarted
    ///   Event 2:  WFTScheduled  ─┐
    ///   Event 3:  WFTStarted    ─┤ WFT1 (empty, no commands)
    ///   Event 4:  WFTCompleted  ─┘
    ///   Event 5:  WFTScheduled  ─┐
    ///   Event 6:  WFTStarted    ─┤ WFT2 (empty, no commands)
    ///   Event 7:  WFTCompleted  ─┘
    ///   Event 8:  WFTScheduled  ─┐
    ///   Event 9:  WFTStarted    ─┤ WFT3 (update + commands follow)
    ///   Event 10: WFTCompleted  ─┘
    ///   Event 11: UpdateAccepted  (sequencing_event_id = 8)
    ///   Event 12: UpdateCompleted
    ///   Event 13: TimerStarted
    ///   Event 14: TimerFired
    ///   Event 15: WFTScheduled  ─┐
    ///   Event 16: WFTStarted    ─┤ WFT4
    ///   Event 17: WFTCompleted  ─┘
    ///   Event 18: WorkflowExecutionCompleted
    fn build_empty_wft_then_update_history(chunking_v2: bool) -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        if chunking_v2 {
            t.set_use_wft_chunking_v2();
        }
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // WFT1: events 2-4 (empty)
        t.add_full_wf_task(); // WFT2: events 5-7 (empty)
        t.add_full_wf_task(); // WFT3: events 8-10
        let accept_id = t.add_update_accepted("upd-1", "startWork"); // 11, seq=8
        t.add_update_completed(accept_id); // 12
        let timer_id = t.add_timer_started("1".to_string()); // 13
        t.add_timer_fired(timer_id, "1".to_string()); // 14
        t.add_full_wf_task(); // WFT4: events 15-17 (command)
        t.add_workflow_execution_completed(); // 18
        t
    }

    /// Empty WFT followed by a speculative WFT with Update request.
    ///
    /// The v1 algorithm may incorrectly collapse the speculative WFT with the preceding WFT,
    /// resulting in an NDE. This test confirms that v2 correctly handles this scenario.
    ///
    /// The legacy, v1 algorithm's behavior is not covered by this test.
    #[rstest::rstest]
    #[test]
    fn empty_wft_then_update_has_last_wft(#[values(false, true)] chunking_v2: bool) {
        if !chunking_v2 {
            // The legacy, v1 algorithm's behavior is not covered by this test.
            return;
        }

        let t = build_empty_wft_then_update_history(chunking_v2);
        let all_events = t.get_full_history_info().unwrap().into_events();

        // 3. Up to WFT1 Started — single WFT visible.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..3].to_vec(),
                0,
                3,
                true,
                false,
                chunking_v2,
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::ReplayOver);
        }

        // 4. Up to WFT1 Completed.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..4].to_vec(),
                0,
                3,
                true,
                false,
                chunking_v2,
            );

            // Can't collapse because of WFExecutionStarted.
            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 3), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(4), NextWFT::ReplayOver);
        }

        // 6. Up to WFT2 Started.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..6].to_vec(),
                0,
                6,
                true,
                false,
                chunking_v2,
            );

            // Can't collapse because of WFExecutionStarted.
            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFT2 is the remaining LWFT.
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(6), NextWFT::ReplayOver);
        }

        // 7. Up to WFT2 Completed.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..7].to_vec(),
                0,
                6,
                true,
                false,
                chunking_v2,
            );

            // Can't collapse because of WFExecutionStarted.
            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 7), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(7), NextWFT::ReplayOver);
        }

        // 9. Up to WFT3 Started, no speculative Update pending.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..9].to_vec(),
                0,
                9,
                true,
                false,
                chunking_v2,
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // It is ok to collapse WFT2+WFT3 in this case, as there is no new event on WFT3.
            // WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (6, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::ReplayOver);
        }

        // 9a. Similar to 9, but WFT3 is a speculative WFT with a pending update.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..9].to_vec(),
                0,
                9,
                true,
                true,
                chunking_v2,
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // Can't collapse because of speculative update affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::ReplayOver);
        }

        // 10. Up to WFT3 Completed.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..10].to_vec(),
                0,
                6,
                true,
                false,
                chunking_v2,
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // It is ok to collapse WFT2+WFT3 in this case, as there is no new event on WFT3.
            // WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (6, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 9), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(10), NextWFT::ReplayOver);
        }

        // 11. Similar to 10, but there's an UpdateAccepted affecting WFT3.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..11].to_vec(),
                0,
                9,
                true,
                false,
                chunking_v2,
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // Can't collapse because of UpdateAccepted affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Tail(WFTCompleted -> UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 9), (2, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(11), NextWFT::ReplayOver);
        }

        // 18: Full history
        {
            let mut update = t.as_history_update();

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // Can't collapse because of UpdateAccepted affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Complete(WFTCompleted -> UpdateAccepted -> UpdateCompleted -> TimerStarted -> TimerFired -> WFTScheduled -> WFTStarted)
            assert_eq!(next_check_peek2(&mut update, 9), (7, false));

            // Complete(WFTCompleted -> WorkflowExecutionCompleted)
            assert_eq!(next_check_peek2(&mut update, 16), (2, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(18), NextWFT::ReplayOver);
        }
    }

    /// Empty WFT followed by WFT with update.
    ///
    /// The v1 algorithm would often return a Complete despite not being not being able to look
    /// ahead far enough (despite has_last_wft=false) to take a positive decision. This test
    /// confirms that v2 correctly returns NeedFetch in those case.
    ///
    /// The legacy, v1 algorithm's behavior is not covered by this test.
    #[rstest::rstest]
    #[test]
    fn empty_wft_then_update_no_last_wft(#[values(false, true)] chunking_v2: bool) {
        if !chunking_v2 {
            // This test encodes behavior that WFT chunking v2 specifically fixes
            // (correct handling of updates after empty WFTs). The legacy algorithm has
            // known buggy behavior in these scenarios — which is the entire motivation
            // for this workspace's work. Skip on legacy.
            return;
        }
        let t = build_empty_wft_then_update_history(chunking_v2);
        let all_events = t.get_full_history_info().unwrap().into_events();

        // 3. Up to WFT1 Started.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..3].to_vec(),
                0,
                3,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> (unknown)

            // Can't decide because unknown could:
            // - be collapsable into WFT1
            // - contain an UpdateAccepted event pointing back to the first WFTStarted
            // - contain a WFTFailed event

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 4. Up to WFT1 Completed.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..4].to_vec(),
                0,
                3,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // Can't decide because unknown could:
            // - be collapsable into WFT1
            // - contain an UpdateAccepted event pointing back to the first WFTStarted

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 4a. Up to WFT1 Completed + a follow up command
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..4].to_vec());
            t.add_timer_started("1".to_string());

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                HistoryUpdate::new_from_events(events, 0, 3, false, false, chunking_v2);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> TimerStarted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide because there are no more WFTStarted in buffer, but unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 5. Up to WFT2 Scheduled.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..5].to_vec(),
                0,
                3,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could:
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 5a. Up to WFT2 Scheduled + some inbound event
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..5].to_vec());
            t.add_we_signaled("whee", vec![]);

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                HistoryUpdate::new_from_events(events, 0, 3, false, false, chunking_v2);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WeSignaled -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted.
            // There can't be any unknown passed the WeSignaled that would affect WFT1
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer, but unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 6. Up to WFT2 Started.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..6].to_vec(),
                0,
                6,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could:
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 6a. Up to WFT2 Started + WFTTimedOut.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..6].to_vec());
            t.add_workflow_task_timed_out();

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                HistoryUpdate::new_from_events(events, 0, 3, false, false, chunking_v2);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTFailed -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because there are no more non-failed WFTStarted in buffer; unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 7. Up to WFT2 Completed.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..7].to_vec(),
                0,
                6,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could contain an UpdateAccepted event pointing
            // back to the second WFTStarted.
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 7a. Up to WFT2 Completed + a follow up command.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..7].to_vec());
            t.add_timer_started("1".to_string());

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                HistoryUpdate::new_from_events(events, 0, 3, false, false, chunking_v2);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> TimerStarted -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // It is safe to return LWFT ending at the second WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            assert_matches!(update.take_next_wft_sequence(6), NextWFT::NeedFetch);
        }

        // 9. Up to WFT3 Started.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..9].to_vec(),
                0,
                9,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could:
            // - allow or prevent collapsing WFT2+WFT3
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 9a. Up to WFT3 Started + WFTTimedOut.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..9].to_vec());
            t.add_workflow_task_timed_out();

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                HistoryUpdate::new_from_events(events, 0, 0, false, false, chunking_v2);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTTimedOut -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // It is safe to return LWFT ending at the second WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // Can't decide further because there are no more non-failed WFTStarted in buffer; unknown could contain some
            assert_matches!(update.take_next_wft_sequence(6), NextWFT::NeedFetch);
        }

        // 10. Up to WFT3 Completed.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..10].to_vec(),
                0,
                9,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could:
            // - allow or prevent collapsing WFT2+WFT3
            // - contain an UpdateAccepted event pointing back to the third WFTStarted
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 11. Up to updateAccepted
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..11].to_vec(),
                0,
                9,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));
            // WFT2 is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));
            // WFT3 is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer; unknown could contain some; UpdateAccepted is not part of any LWFT
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }

        // 12. Up to TimerStarted
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..13].to_vec(),
                0,
                9,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> WFTCompleted -> TimerStarted -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));
            // WFT2 is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));
            // WFT3 is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer; unknown could contain some; UpdateAccepted is not part of any LWFT
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }

        // 16. Up to WFT4 Started.
        {
            let mut update = HistoryUpdate::new_from_events(
                all_events[..16].to_vec(),
                0,
                9,
                false,
                false,
                chunking_v2,
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> WFTCompleted -> TimerStarted -> TimerFired -> WFTScheduled -> WFTStarted -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));
            // WFT2 is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));
            // WFT3 is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because WFT4 Started could be followed by a WFTFailure or noop WFT sequences.
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }
    }

    fn build_heartbeat_then_commands_history(chunking_v2: bool) -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        if chunking_v2 {
            t.set_use_wft_chunking_v2();
        }
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task(); // WFT2: has commands
        let timer_id = t.add_timer_started("1".to_string());
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task(); // WFT3
        t
    }

    /// Heartbeat collapsing: empty WFT followed by WFT with commands.
    ///
    /// Under v1 (legacy), WFT1+WFT2 collapse into a single 6-event LWFT.
    /// Under v2, WFT1 is forced separate (it follows WFExecutionStarted),
    /// and WFT2 becomes its own LWFT.
    #[rstest::rstest]
    #[test]
    fn heartbeat_collapsing(#[values(false, true)] chunking_v2: bool) {
        let t = build_heartbeat_then_commands_history(chunking_v2);

        let mut update = t.as_history_update();
        if chunking_v2 {
            // WFT1 alone (follows WFExecutionStarted).
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(seq.len(), 3, "WFT1 should be separate");
            assert_eq!(seq.last().unwrap().event_id, 3);

            // WFT2 alone (was previously collapsed with WFT1 by v1).
            let seq = next_check_peek(&mut update, 3);
            assert_eq!(seq.len(), 3, "WFT2 is the second LWFT");
            assert_eq!(seq.last().unwrap().event_id, 6);
        } else {
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(seq.len(), 6, "WFT1+WFT2 should be collapsed under v1");
            assert_eq!(seq.last().unwrap().event_id, 6);
        }
    }

    /// When there are pending speculative updates, WFT chunking v2 must NOT
    /// collapse the last WFT in a heartbeat chain, because the update needs
    /// to be delivered in its own activation (matching the original execution).
    /// Earlier (non-last) heartbeats in the chain may still be collapsed together.
    ///
    /// To exercise this independently of the WFExecutionStarted rule (which
    /// already forces WFT1 to be separate), the chain we care about runs from
    /// WFT2 through WFT4.
    ///
    /// History:
    ///   Event 1:   WorkflowExecutionStarted
    ///   Event 2:   WFTScheduled  ─┐
    ///   Event 3:   WFTStarted    ─┤ WFT1 (heartbeat, empty)
    ///   Event 4:   WFTCompleted  ─┘
    ///   Event 5:   WFTScheduled  ─┐
    ///   Event 6:   WFTStarted    ─┤ WFT2 (heartbeat, empty)
    ///   Event 7:   WFTCompleted  ─┘
    ///   Event 8:   WFTScheduled  ─┐
    ///   Event 9:   WFTStarted    ─┤ WFT3 (heartbeat, empty)
    ///   Event 10:  WFTCompleted  ─┘
    ///   Event 11:  WFTScheduled  ─┐
    ///   Event 12:  WFTStarted    ─┘ WFT4 (current task, with pending update)
    #[test]
    fn heartbeat_not_collapsed_when_speculative_updates_pending() {
        let chunking_v2 = true;
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // WFT1: events 2-4
        t.add_full_wf_task(); // WFT2: events 5-7
        t.add_full_wf_task(); // WFT3: events 8-10
        t.add_workflow_task_scheduled_and_started(); // WFT4: events 11-12
        maybe_set_chunking_v2(&mut t, chunking_v2);
        let all_events = t.get_full_history_info().unwrap().into_events();

        // Without speculative updates: WFT2+WFT3+WFT4 all collapse via heartbeat coalescing.
        {
            let (mut update, _) =
                HistoryUpdate::from_events(all_events.clone(), 0, 12, true, false, chunking_v2);

            // WFT1 alone (follows WFExecutionStarted).
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(
                seq.len(),
                3,
                "WFT1 is separate (follows WFExecutionStarted)"
            );
            assert_eq!(seq.last().unwrap().event_id, 3);

            // WFT2+WFT3+WFT4 collapsed.
            let seq = next_check_peek(&mut update, 3);
            assert_eq!(
                seq.len(),
                9,
                "Without speculative updates: WFT2+WFT3+WFT4 collapsed"
            );
            assert_eq!(seq.last().unwrap().event_id, 12);
        }

        // With speculative updates: only the last heartbeat (WFT4) is uncollapsed;
        // the earlier heartbeats (WFT2+WFT3) are still collapsed together.
        {
            let (mut update, _) =
                HistoryUpdate::from_events(all_events.clone(), 0, 12, true, true, chunking_v2);

            // WFT1 alone (follows WFExecutionStarted).
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(
                seq.len(),
                3,
                "WFT1 is separate (follows WFExecutionStarted)"
            );
            assert_eq!(seq.last().unwrap().event_id, 3);

            // WFT2+WFT3 collapsed: intermediate heartbeats can still merge.
            let seq = next_check_peek(&mut update, 3);
            assert_eq!(
                seq.len(),
                6,
                "With speculative updates: WFT2+WFT3 still collapsed (intermediate heartbeats)"
            );
            assert_eq!(seq.last().unwrap().event_id, 9);

            // WFT4 separate: holds the pending speculative update.
            let seq = next_check_peek(&mut update, 9);
            assert_eq!(
                seq.len(),
                3,
                "With speculative updates: WFT4 should be separate (3 events)"
            );
            assert_eq!(seq.last().unwrap().event_id, 12);
        }
    }
}
