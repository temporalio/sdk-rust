//! The Store that the unit tests of the crate share.

use crate::{
    StreamError, StreamResult, StreamRuntime, StreamStore,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse,
    },
    record::BatchLimits,
};
use parking_lot::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

/// A Store that keeps the requests it got and gives the answers that a test set.
#[derive(Default)]
pub(crate) struct Scripted {
    pub(crate) reads: Mutex<Vec<ReadRecordsRequest>>,
    pub(crate) appends: Mutex<Vec<AppendRecordsRequest>>,
    pub(crate) page: Mutex<ReadRecordsResponse>,
    pub(crate) appended: Mutex<AppendRecordsResponse>,
    pub(crate) described: Mutex<DescribeStreamResponse>,
    /// The answer of a create. It is the request's ref, as created, when it is `None`.
    pub(crate) created: Mutex<Option<CreateStreamResponse>>,
    pub(crate) error: Mutex<Option<StreamError>>,
    /// A read tells `entered`, and then waits for `release` if `hold_reads` is set.
    pub(crate) hold_reads: AtomicBool,
    pub(crate) entered: Notify,
    pub(crate) release: Notify,
    pub(crate) limits: BatchLimits,
}

impl Scripted {
    fn answer<T>(&self, value: T) -> StreamResult<T> {
        match self.error.lock().clone() {
            Some(error) => Err(error),
            None => Ok(value),
        }
    }
}

#[async_trait::async_trait]
impl StreamStore for Scripted {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        let created = self.created.lock().clone();
        self.answer(created.unwrap_or(CreateStreamResponse {
            r#ref: request.stream_ref,
            created: true,
        }))
    }

    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        self.appends.lock().push(request);
        self.answer(self.appended.lock().clone())
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        self.reads.lock().push(request);
        self.entered.notify_one();
        if self.hold_reads.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        self.answer(self.page.lock().clone())
    }

    async fn close_stream(
        &self,
        _request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        self.answer(CloseStreamResponse {})
    }

    async fn describe_stream(
        &self,
        _request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        self.answer(self.described.lock().clone())
    }

    fn limits(&self) -> BatchLimits {
        self.limits
    }
}

/// A runtime on a new [Scripted] Store, and the Store.
pub(crate) fn runtime() -> (Arc<Scripted>, Arc<StreamRuntime>) {
    runtime_with_limits(BatchLimits::default())
}

/// A runtime on a new [Scripted] Store that gives `limits`, and the Store. The runtime reads the
/// limits when it is built, so a test sets them here.
pub(crate) fn runtime_with_limits(limits: BatchLimits) -> (Arc<Scripted>, Arc<StreamRuntime>) {
    let store = Arc::new(Scripted {
        limits,
        ..Default::default()
    });
    let runtime = StreamRuntime::new(store.clone(), None);
    (store, runtime)
}
