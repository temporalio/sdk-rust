//! How a read scans the log, seen through a memory Store that can trim under a read.

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    StreamError, StreamResult, StreamRuntime,
    memory::MemoryStore,
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, ClientProducer,
        CreateStreamRequest, CreateStreamResponse, Producer, ReadRecordsRequest,
        ReadRecordsResponse, StreamErrorDetails, StreamErrorReason, StreamRef, producer::Kind,
    },
    store::{Appended, Batch, Capabilities, Create, Created, Entry, Store, StreamState},
};
use tokio::time::Instant;

const NAMESPACE: &str = "ns";
const STREAM: &str = "s";

/// The memory Store. Once armed, its next read that asks for log entries finds its first entry
/// trimmed, as a trim just before that read leaves it. It counts the entries that reads return.
struct Trimming {
    inner: MemoryStore,
    trim_next: AtomicBool,
    pulled: AtomicUsize,
}

#[async_trait::async_trait]
impl Store for Trimming {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
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
        let (mut state, mut entries) = self.inner.read(stream_ref, from, count).await?;
        if count > 0 && !entries.is_empty() && self.trim_next.swap(false, Ordering::Relaxed) {
            state.trimmed = entries.remove(0).offset;
        }
        self.pulled.fetch_add(entries.len(), Ordering::Relaxed);
        Ok((state, entries))
    }

    async fn wait_after(
        &self,
        stream_ref: &StreamRef,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        self.inner.wait_after(stream_ref, after, deadline).await
    }
}

fn trimming() -> (Arc<Trimming>, Arc<StreamRuntime>) {
    let store = Arc::new(Trimming {
        inner: MemoryStore::new(Duration::from_secs(3600), Some(Duration::from_secs(7200))),
        trim_next: AtomicBool::new(false),
        pulled: AtomicUsize::new(0),
    });
    let ext = StreamRuntime::new("", store.clone(), None);
    (store, ext)
}

async fn call<Req: Message, Resp: Message + Default>(
    ext: &StreamRuntime,
    operation: &str,
    request: Req,
) -> Resp {
    try_call(ext, operation, request).await.unwrap()
}

async fn try_call<Req: Message, Resp: Message + Default>(
    ext: &StreamRuntime,
    operation: &str,
    request: Req,
) -> Result<Resp, StreamError> {
    let bytes = ext.call(operation, &request.encode_to_vec()).await?;
    Ok(Resp::decode(bytes.as_slice()).unwrap())
}

async fn create(ext: &StreamRuntime) {
    let _: CreateStreamResponse = call(
        ext,
        "CreateStream",
        CreateStreamRequest {
            namespace: NAMESPACE.to_string(),
            stream_ref: stream_ref(),
            ..Default::default()
        },
    )
    .await;
}

fn client() -> Producer {
    Producer {
        id: "client:me:p".to_string(),
        kind: Some(Kind::Client(ClientProducer {})),
    }
}

fn stream_ref() -> Option<StreamRef> {
    Some(StreamRef {
        id: STREAM.to_string(),
        ..Default::default()
    })
}

async fn append(ext: &StreamRuntime, sequence: i64, count: usize) -> AppendRecordsResponse {
    append_sized(ext, sequence, count, 0).await
}

/// Appends `count` Records, each with `size` bytes after its number.
async fn append_sized(
    ext: &StreamRuntime,
    sequence: i64,
    count: usize,
    size: usize,
) -> AppendRecordsResponse {
    let records = (0..count)
        .map(|i| AppendRecord {
            body: Some(Payload {
                data: format!("{i}{}", "x".repeat(size)).into_bytes(),
                ..Default::default()
            }),
        })
        .collect();
    call(
        ext,
        "AppendRecords",
        AppendRecordsRequest {
            namespace: NAMESPACE.to_string(),
            r#ref: stream_ref(),
            producer: Some(client()),
            sequence,
            records,
        },
    )
    .await
}

/// A trim between a read's state and its log read fails a read from an offset as expired, so that
/// the Reader always sees the gap. A read from the start skips the trimmed Records.
#[tokio::test]
async fn a_trim_under_a_read_from_an_offset_fails_it_as_expired() {
    let (store, ext) = trimming();
    create(&ext).await;
    let first = append(&ext, 1, 3).await;
    let read = |from_offset| ReadRecordsRequest {
        namespace: NAMESPACE.to_string(),
        r#ref: stream_ref(),
        from_offset,
        ..Default::default()
    };
    store.trim_next.store(true, Ordering::Relaxed);
    let error = try_call::<_, ReadRecordsResponse>(&ext, "ReadRecords", read(first.first_offset))
        .await
        .unwrap_err();
    let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
    assert_eq!(details.reason(), StreamErrorReason::Expired);
    store.trim_next.store(true, Ordering::Relaxed);
    let from_start: ReadRecordsResponse = call(&ext, "ReadRecords", read(vec![])).await;
    assert_eq!(from_start.records.len(), 2);
}

/// A read with a byte cap reads about the entries that its page holds from the Store, so a small
/// cap on large Records doesn't read a whole page of them.
#[tokio::test]
async fn a_capped_read_pulls_about_its_page_from_the_store() {
    let (store, ext) = trimming();
    create(&ext).await;
    append_sized(&ext, 1, 50, 1000).await;
    store.pulled.store(0, Ordering::Relaxed);
    let page: ReadRecordsResponse = call(
        &ext,
        "ReadRecords",
        ReadRecordsRequest {
            namespace: NAMESPACE.to_string(),
            r#ref: stream_ref(),
            max_bytes: 2500,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(page.records.len(), 2);
    let pulled = store.pulled.load(Ordering::Relaxed);
    assert!(pulled <= 4, "{pulled}");
}
