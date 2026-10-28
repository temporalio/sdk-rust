mod poll_buffer;

pub(crate) use poll_buffer::{
    ActivityTaskOptions, LongPollBuffer, WorkflowTaskOptions, WorkflowTaskPoller,
};
pub use temporalio_client::{Client, ClientOptions, ClientTlsOptions, RetryOptions, TlsOptions};

use crate::{
    abstractions::{OwnedMeteredSemPermit, TrackedOwnedMeteredSemPermit},
    telemetry::metrics::MetricsContext,
    worker::{ActivitySlotKind, NexusSlotKind, SlotKind, WorkflowSlotKind},
};
use anyhow::{anyhow, bail};
use futures_util::{Stream, stream};
use std::{fmt::Debug, marker::PhantomData};
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    PollActivityTaskQueueResponse, PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
};
use tokio_util::sync::CancellationToken;

#[cfg(any(feature = "test-utilities", test))]
use futures_util::Future;
#[cfg(any(feature = "test-utilities", test))]
pub(crate) use poll_buffer::MockPermittedPollBuffer;

/// Error returned by the poller. Not to be confused with [`crate::worker::PollError`].
#[derive(thiserror::Error, Debug)]
pub(crate) enum PollerError {
    #[error("Poller has stopped, shutting down")]
    PollerStopped,
    #[error(transparent)]
    Tonic(#[from] tonic::Status),
}

pub(crate) type PollerResult<T> = Result<T, PollerError>;

/// A trait for things that long poll the server.
#[cfg_attr(any(feature = "test-utilities", test), mockall::automock)]
#[cfg_attr(any(feature = "test-utilities", test), allow(unused))]
#[async_trait::async_trait]
pub(crate) trait Poller<PollResult>
where
    PollResult: Send + Sync + 'static,
{
    async fn poll(&mut self) -> Option<PollerResult<PollResult>>;
    async fn shutdown(self);
    /// Need a separate shutdown to be able to consume boxes :(
    async fn shutdown_box(self: Box<Self>);
}
pub(crate) type BoxedPoller<T> = Box<dyn Poller<T> + Send + Sync + 'static>;
pub(crate) type BoxedWFPoller = BoxedPoller<(
    PollWorkflowTaskQueueResponse,
    OwnedMeteredSemPermit<WorkflowSlotKind>,
)>;
pub(crate) type BoxedActPoller = BoxedPoller<(
    PollActivityTaskQueueResponse,
    OwnedMeteredSemPermit<ActivitySlotKind>,
)>;
pub(crate) type BoxedNexusPoller = BoxedPoller<(
    PollNexusTaskQueueResponse,
    OwnedMeteredSemPermit<NexusSlotKind>,
)>;

#[async_trait::async_trait]
impl<T> Poller<T> for Box<dyn Poller<T> + Send + Sync>
where
    T: Send + Sync + 'static,
{
    async fn poll(&mut self) -> Option<PollerResult<T>> {
        self.as_mut().poll().await
    }

    async fn shutdown(self) {
        <dyn Poller<T> as Poller<T>>::shutdown_box(self).await
    }

    async fn shutdown_box(self: Box<Self>) {
        self.shutdown().await
    }
}

#[cfg(any(feature = "test-utilities", test))]
mockall::mock! {
    pub ManualPoller<T: Send + Sync + 'static> {}
    #[allow(unused)]
    impl<T: Send + Sync + 'static> Poller<T> for ManualPoller<T> {
        fn poll<'a, 'b>(&mut self)
          -> impl Future<Output = Option<PollerResult<T>>> + Send + 'b
            where 'a: 'b, Self: 'b;
        fn shutdown<'a>(self)
          -> impl Future<Output = ()> + Send + 'a
            where Self: 'a;
        fn shutdown_box<'a>(self: Box<Self>)
          -> impl Future<Output = ()> + Send + 'a
            where Self: 'a;
    }
}

#[derive(Debug)]
pub(crate) struct PermittedTqResp<T: ValidatableTask> {
    pub(crate) permit: OwnedMeteredSemPermit<T::SlotKind>,
    pub(crate) resp: T,
}

#[derive(Debug)]
pub(crate) struct TrackedPermittedTqResp<T: ValidatableTask> {
    pub(crate) permit: TrackedOwnedMeteredSemPermit<T::SlotKind>,
    pub(crate) resp: T,
}

pub(crate) trait ValidatableTask:
    Debug + Default + PartialEq + Send + Sync + 'static
{
    type SlotKind: SlotKind;

    fn validate(&self) -> Result<(), anyhow::Error>;
    fn task_name() -> &'static str;
}

pub(crate) struct TaskPollerStream<P, T>
where
    P: Poller<(T, OwnedMeteredSemPermit<T::SlotKind>)>,
    T: ValidatableTask,
{
    poller: P,
    metrics: MetricsContext,
    metrics_no_task: fn(&MetricsContext),
    shutdown_token: CancellationToken,
    _phantom: PhantomData<T>,
}

impl<P, T> TaskPollerStream<P, T>
where
    P: Poller<(T, OwnedMeteredSemPermit<T::SlotKind>)>,
    T: ValidatableTask,
{
    pub(crate) fn new(
        poller: P,
        metrics: MetricsContext,
        metrics_no_task: fn(&MetricsContext),
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            poller,
            metrics,
            metrics_no_task,
            shutdown_token,
            _phantom: PhantomData,
        }
    }

    fn into_stream(self) -> impl Stream<Item = PollerResult<PermittedTqResp<T>>> {
        stream::unfold(Box::new(self), |mut state| async move {
            loop {
                let result = match state.poller.poll().await {
                    Some(Ok((task, permit))) => {
                        if task == Default::default() {
                            // We get the default proto in the event that the long poll
                            // times out, or when worker is shutting down. We only report
                            // the metric on timeout.
                            if !state.shutdown_token.is_cancelled() {
                                debug!("Poll {} task timeout", T::task_name());
                                (state.metrics_no_task)(&state.metrics);
                            }
                            continue;
                        }

                        if let Err(e) = task.validate() {
                            warn!(
                                "Received invalid {} task ({}): {:?}",
                                T::task_name(),
                                e,
                                &task
                            );
                            Err(tonic::Status::invalid_argument(e.to_string()).into())
                        } else {
                            Ok(PermittedTqResp { resp: task, permit })
                        }
                    }
                    Some(Err(e)) => {
                        if let PollerError::Tonic(e) = &e {
                            warn!(error=?e, "Error while polling for {} tasks", T::task_name());
                        }
                        Err(e)
                    }
                    // If poller returns None, it's dead, thus we also return None to terminate this stream.
                    None => {
                        state.poller.shutdown().await;
                        return None;
                    }
                };
                return Some((result, state));
            }
        })
    }
}

impl ValidatableTask for PollActivityTaskQueueResponse {
    type SlotKind = ActivitySlotKind;

    fn validate(&self) -> Result<(), anyhow::Error> {
        if self.task_token.is_empty() {
            return Err(anyhow!("missing task token"));
        }
        Ok(())
    }

    fn task_name() -> &'static str {
        "activity"
    }
}

pub(crate) fn new_activity_task_poller(
    poller: BoxedActPoller,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
) -> impl Stream<Item = PollerResult<PermittedTqResp<PollActivityTaskQueueResponse>>> {
    TaskPollerStream::new(
        poller,
        metrics,
        MetricsContext::act_poll_timeout,
        shutdown_token,
    )
    .into_stream()
}

impl ValidatableTask for PollNexusTaskQueueResponse {
    type SlotKind = NexusSlotKind;

    fn validate(&self) -> Result<(), anyhow::Error> {
        if self.task_token.is_empty() {
            bail!("missing task token");
        } else if self.request.is_none() {
            bail!("missing request field");
        } else if self
            .request
            .as_ref()
            .expect("just request exists")
            .variant
            .is_none()
        {
            bail!("missing request variant");
        }
        Ok(())
    }

    fn task_name() -> &'static str {
        "nexus"
    }
}

pub(crate) type NexusPollItem = PollerResult<PermittedTqResp<PollNexusTaskQueueResponse>>;
pub(crate) fn new_nexus_task_poller(
    poller: BoxedNexusPoller,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
) -> impl Stream<Item = NexusPollItem> {
    TaskPollerStream::new(
        poller,
        metrics,
        MetricsContext::nexus_poll_timeout,
        shutdown_token,
    )
    .into_stream()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer,
        pollers::MockPermittedPollBuffer,
        test_help::{CounterRecordingMeter, mock_poller},
        worker::ActivitySlotKind,
    };
    use futures_util::{StreamExt, pin_mut};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[rstest::rstest]
    #[tokio::test]
    async fn empty_response_is_retried(#[values(false, true)] is_shutdown: bool) {
        let mut mock_poller = mock_poller();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        mock_poller.expect_poll().returning(move || {
            let n = call_count_clone.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Some(Ok(PollActivityTaskQueueResponse::default()))
            } else {
                None
            }
        });

        let sem = Arc::new(fixed_size_permit_dealer::<ActivitySlotKind>(10));
        let shutdown_token = CancellationToken::new();
        if is_shutdown {
            shutdown_token.cancel();
        }

        let meter = Arc::new(CounterRecordingMeter::default());

        let stream = new_activity_task_poller(
            Box::new(MockPermittedPollBuffer::new(sem, mock_poller)),
            MetricsContext::top_level_with_meter(
                "ns".into(),
                "tq".into(),
                Some(meter.clone().into_temporal_meter()),
            ),
            shutdown_token,
        );
        pin_mut!(stream);

        // Empty responses should be skipped and the stream terminates only when the poller returns
        // None.
        let result = stream.next().await;
        assert!(
            result.is_none(),
            "Stream should end when poller returns None"
        );
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
        assert_eq!(
            meter.counter_total("activity_poll_no_task", &[]),
            if is_shutdown { 0 } else { 2 }
        );
    }
}
