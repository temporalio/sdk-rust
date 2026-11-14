//! A Store that lacks a capability refuses every call that needs it, by name, and writes nothing.
//! The tests use the memory Store with one capability masked.

use prost::Message;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    StreamError, StreamResult, StreamRuntime,
    memory::MemoryStore,
    proto::{
        AppendRecord, AppendRecordsRequest, ClientProducer, CloseStreamRequest,
        CreateStreamRequest, DescribeStreamRequest, DescribeStreamResponse, Producer,
        StreamErrorDetails, StreamErrorReason, StreamRef, StreamStatus, producer::Kind,
    },
    store::{Appended, Batch, Capabilities, Closed, Create, Created, Entry, Store, StreamState},
};
use tokio::time::Instant;
use tonic::Code;

/// A memory Store that lacks the capabilities in `without`, as a Store with fewer would.
struct Masked {
    inner: Arc<MemoryStore>,
    without: Capabilities,
}

#[async_trait::async_trait]
impl Store for Masked {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities().without(self.without)
    }

    async fn create(&self, stream_ref: &StreamRef, create: &Create<'_>) -> StreamResult<Created> {
        self.inner.create(stream_ref, create).await
    }

    async fn append(&self, stream_ref: &StreamRef, batch: &Batch<'_>) -> StreamResult<Appended> {
        self.inner.append(stream_ref, batch).await
    }

    async fn read(
        &self,
        stream_ref: &StreamRef,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        self.inner.read(stream_ref, from, count).await
    }

    async fn wait_after(
        &self,
        stream_ref: &StreamRef,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        self.inner.wait_after(stream_ref, after, deadline).await
    }

    // The masked calls still work underneath, so a call that skips the check would succeed,
    // and the tests would see it.
    async fn close(&self, stream_ref: &StreamRef, producer: &Producer) -> StreamResult<Closed> {
        self.inner.close(stream_ref, producer).await
    }
}

const NAMESPACE: &str = "ns";

struct Case {
    ext: Arc<StreamRuntime>,
    memory: Arc<MemoryStore>,
    stream: String,
}

fn case(without: Capabilities) -> Case {
    let memory = Arc::new(MemoryStore::new(
        Duration::from_secs(3600),
        Some(Duration::from_secs(7200)),
    ));
    let store = Arc::new(Masked {
        inner: memory.clone(),
        without,
    });
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Case {
        ext: StreamRuntime::new("", store, None),
        memory,
        stream: format!("wf:run-{unique}"),
    }
}

fn record() -> AppendRecord {
    AppendRecord {
        body: Some(Payload::default()),
    }
}

fn client() -> Producer {
    Producer {
        id: "client:me:p".to_string(),
        kind: Some(Kind::Client(ClientProducer {})),
    }
}

/// The capability an error names, after it checked the code and the reason.
fn refused_capability(error: &StreamError) -> String {
    assert_eq!(error.code(), Code::Unimplemented, "{error:?}");
    let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
    assert_eq!(details.reason(), StreamErrorReason::Unsupported);
    error
        .message
        .rsplit(' ')
        .next()
        .unwrap_or_default()
        .to_string()
}

impl Case {
    fn stream_ref(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        })
    }

    async fn call<Req: Message>(
        &self,
        operation: &str,
        request: Req,
    ) -> Result<Vec<u8>, StreamError> {
        self.ext.call(operation, &request.encode_to_vec()).await
    }

    async fn create(&self) {
        let request = CreateStreamRequest {
            namespace: NAMESPACE.to_string(),
            stream_ref: self.stream_ref(),
            ..Default::default()
        };
        self.call("CreateStream", request).await.unwrap();
    }

    fn log_len(&self) -> usize {
        self.memory.test_log_len(&self.stream)
    }
}

#[tokio::test]
async fn an_append_without_the_fence_is_refused_with_nothing_written() {
    let case = case(Capabilities::PRODUCER_FENCE);
    case.create().await;
    let request = AppendRecordsRequest {
        namespace: NAMESPACE.to_string(),
        r#ref: case.stream_ref(),
        producer: Some(client()),
        sequence: 1,
        records: vec![record()],
    };
    let error = case.call("AppendRecords", request).await.unwrap_err();
    assert_eq!(refused_capability(&error), "producer_fence");
    assert_eq!(case.log_len(), 0);
}

#[tokio::test]
async fn an_append_with_no_sequence_without_the_capability_is_refused_with_nothing_written() {
    let case = case(Capabilities::NEXT_SEQUENCE);
    case.create().await;
    let request = AppendRecordsRequest {
        namespace: NAMESPACE.to_string(),
        r#ref: case.stream_ref(),
        producer: Some(client()),
        records: vec![record()],
        ..Default::default()
    };
    let error = case
        .call("AppendRecords", request.clone())
        .await
        .unwrap_err();
    assert_eq!(refused_capability(&error), "next_sequence");
    assert_eq!(case.log_len(), 0);
    // A batch with its own sequence needs no number from the Store.
    let numbered = AppendRecordsRequest {
        sequence: 1,
        ..request
    };
    case.call("AppendRecords", numbered).await.unwrap();
    assert_eq!(case.log_len(), 1);
}

#[tokio::test]
async fn a_close_without_the_capability_is_refused_and_the_stream_stays_open() {
    let case = case(Capabilities::CLOSE);
    case.create().await;
    let request = CloseStreamRequest {
        namespace: NAMESPACE.to_string(),
        r#ref: case.stream_ref(),
        producer: Some(client()),
    };
    let error = case.call("CloseStream", request).await.unwrap_err();
    assert_eq!(refused_capability(&error), "close");
    assert_eq!(case.log_len(), 0);
    let describe = DescribeStreamRequest {
        namespace: NAMESPACE.to_string(),
        r#ref: case.stream_ref(),
    };
    let bytes = case.call("DescribeStream", describe).await.unwrap();
    let described = DescribeStreamResponse::decode(bytes.as_slice()).unwrap();
    assert_eq!(described.status, StreamStatus::Open as i32);
}
