//! A Store that answers whole Stream operations, such as a Store that a user of a lang SDK writes
//! in that language. Such a Store keeps its own log and offsets, so the runtime can't call the log
//! calls of [crate::store::Store] on it.

use crate::{
    StreamError, StreamResult,
    ids::MAX_OFFSET_BYTES,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse,
    },
    read::{MAX_RECORDS, max_bytes},
    runtime::duration,
};
use prost::Message;

/// The five operations of the Stream service, each answered whole. A runtime that
/// [crate::StreamRuntime::with_operations] builds checks each request as it does for a
/// [crate::store::Store] before the call, and each answer after it.
///
/// The Store applies the rules of the Stream service itself. An append fences a lower epoch of its
/// Producer and takes a resend of the same Producer, epoch and `sequence` once. A read waits up to
/// its `wait` for a Record when it has none to give, and a close wakes it. An offset is opaque bytes
/// that compare in order. A failed call gives a [StreamError] with its reason. A Store that can't
/// reach its storage gives [crate::Reason::StorageUnavailable], which a Worker tries again.
#[async_trait::async_trait]
pub trait OperationStore: Send + Sync + 'static {
    /// Creates a Stream, or gets the open one when the conflict policy allows it.
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse>;

    /// Appends one batch as one Producer.
    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse>;

    /// Reads one page after `from_offset`, and waits up to `wait` for a Record when there is none.
    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse>;

    /// Closes a Stream.
    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse>;

    /// Gives the status and the newest offset of a Stream.
    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse>;
}

/// Refuses an offset that a caller gives with more than [MAX_OFFSET_BYTES].
fn caller_offset(offset: &[u8]) -> StreamResult<()> {
    if offset.len() > MAX_OFFSET_BYTES {
        return Err(StreamError::invalid(format!(
            "The offset has {} bytes, more than {MAX_OFFSET_BYTES}",
            offset.len()
        )));
    }
    Ok(())
}

/// Refuses an offset that a Store gives, empty or with more than [MAX_OFFSET_BYTES]. A Worker
/// records the offsets in History and resumes a read from one, and an empty one is the start.
fn store_offset(offset: &[u8]) -> StreamResult<()> {
    if offset.is_empty() {
        return Err(StreamError::storage_corrupt(
            "The Store gave a Record with no offset",
        ));
    }
    if offset.len() > MAX_OFFSET_BYTES {
        return Err(StreamError::storage_corrupt(format!(
            "The Store gave an offset of {} bytes, more than {MAX_OFFSET_BYTES}",
            offset.len()
        )));
    }
    Ok(())
}

/// Reads one page, with the caps and the longest wait of a read on a [crate::store::Store].
pub(crate) async fn read(
    store: &dyn OperationStore,
    mut request: ReadRecordsRequest,
) -> StreamResult<ReadRecordsResponse> {
    caller_offset(&request.from_offset)?;
    let max_bytes = max_bytes(request.max_bytes)?;
    let max = match request.max_records {
        n if n <= 0 => MAX_RECORDS,
        n => (n as usize).min(MAX_RECORDS),
    };
    request.max_records = max as i32;
    request.wait = duration(request.wait)
        .map(|wait| wait.min(crate::MAX_WAIT))
        .and_then(|wait| prost_types::Duration::try_from(wait).ok());
    let response = store.read_records(request).await?;
    fit_page(response, max, max_bytes)
}

/// The page cut to `max` Records and `max_bytes`, with its first Record always. A Worker bounds
/// what one read holds by these caps, so a Store that gives more must not pass them. A cut page
/// ends at its last Record and is not done.
fn fit_page(
    mut page: ReadRecordsResponse,
    max: usize,
    max_bytes: usize,
) -> StreamResult<ReadRecordsResponse> {
    for record in &page.records {
        store_offset(&record.offset)?;
    }
    if page.last_offset.len() > MAX_OFFSET_BYTES {
        return Err(StreamError::storage_corrupt(format!(
            "The Store gave a last offset of {} bytes, more than {MAX_OFFSET_BYTES}",
            page.last_offset.len()
        )));
    }
    let mut bytes = 0;
    let mut keep = 0;
    for record in &page.records {
        let size = record.encoded_len();
        if keep >= max || (keep > 0 && bytes + size > max_bytes) {
            break;
        }
        bytes += size;
        keep += 1;
    }
    if keep < page.records.len() {
        page.records.truncate(keep);
        page.last_offset = page.records[keep - 1].offset.clone();
        page.done = false;
    }
    Ok(page)
}

/// Appends a batch that the runtime checked, and checks the offsets of the answer.
pub(crate) async fn append(
    store: &dyn OperationStore,
    request: AppendRecordsRequest,
) -> StreamResult<AppendRecordsResponse> {
    let empty = request.records.is_empty();
    let response = store.append_records(request).await?;
    if !empty {
        store_offset(&response.first_offset)?;
        store_offset(&response.last_offset)?;
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Reason, StreamRuntime,
        proto::{
            AppendRecord, Producer, StreamRecord, StreamRef, WorkflowProducer, WorkflowTaskEpoch,
            producer::Kind,
        },
    };
    use parking_lot::Mutex;
    use std::sync::Arc;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    /// A Store that keeps the requests it got and gives the answers that a test set.
    #[derive(Default)]
    struct Scripted {
        reads: Mutex<Vec<ReadRecordsRequest>>,
        appends: Mutex<Vec<AppendRecordsRequest>>,
        page: Mutex<ReadRecordsResponse>,
        appended: Mutex<AppendRecordsResponse>,
        error: Mutex<Option<StreamError>>,
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
    impl OperationStore for Scripted {
        async fn create_stream(
            &self,
            request: CreateStreamRequest,
        ) -> StreamResult<CreateStreamResponse> {
            self.answer(CreateStreamResponse {
                r#ref: request.stream_ref,
                created: true,
            })
        }

        async fn append_records(
            &self,
            request: AppendRecordsRequest,
        ) -> StreamResult<AppendRecordsResponse> {
            self.appends.lock().push(request);
            self.answer(self.appended.lock().clone())
        }

        async fn read_records(
            &self,
            request: ReadRecordsRequest,
        ) -> StreamResult<ReadRecordsResponse> {
            self.reads.lock().push(request);
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
            self.answer(DescribeStreamResponse::default())
        }
    }

    fn runtime() -> (Arc<Scripted>, Arc<StreamRuntime>) {
        let store = Arc::new(Scripted::default());
        let runtime = StreamRuntime::with_operations("py", store.clone(), None);
        (store, runtime)
    }

    fn stream_ref() -> Option<StreamRef> {
        Some(StreamRef {
            id: "s".to_string(),
            store: "py".to_string(),
        })
    }

    fn record(offset: &[u8], bytes: usize) -> StreamRecord {
        StreamRecord {
            body: Some(Payload {
                data: vec![0; bytes],
                ..Default::default()
            }),
            offset: offset.to_vec(),
            ..Default::default()
        }
    }

    fn workflow(epoch: Option<WorkflowTaskEpoch>) -> Option<Producer> {
        Some(Producer {
            id: "wf:r".to_string(),
            kind: Some(Kind::Workflow(WorkflowProducer {
                run_id: "r".to_string(),
                epoch,
            })),
        })
    }

    fn append(producer: Option<Producer>) -> AppendRecordsRequest {
        AppendRecordsRequest {
            r#ref: stream_ref(),
            producer,
            sequence: 1,
            records: vec![AppendRecord {
                body: Some(Payload::default()),
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_read_goes_to_the_store_with_its_caps_and_the_longest_wait() {
        let (store, runtime) = runtime();
        *store.page.lock() = ReadRecordsResponse {
            records: vec![record(b"a", 1)],
            last_offset: b"a".to_vec(),
            done: true,
        };
        let page = runtime
            .read_records(ReadRecordsRequest {
                r#ref: stream_ref(),
                from_offset: vec![9; 20],
                wait: Some(prost_types::Duration {
                    seconds: 3600,
                    nanos: 0,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(page.done);
        let sent = store.reads.lock()[0].clone();
        // An offset is opaque to the runtime, so one of any length up to the bound passes.
        assert_eq!(sent.from_offset, vec![9; 20]);
        assert_eq!(sent.max_records, MAX_RECORDS as i32);
        assert_eq!(duration(sent.wait), Some(crate::MAX_WAIT));
    }

    #[tokio::test]
    async fn a_page_over_its_caps_is_cut_and_not_done() {
        let (store, runtime) = runtime();
        *store.page.lock() = ReadRecordsResponse {
            records: vec![record(b"a", 10), record(b"b", 10), record(b"c", 10)],
            last_offset: b"c".to_vec(),
            done: true,
        };
        let read = |max_records, max_bytes| ReadRecordsRequest {
            r#ref: stream_ref(),
            max_records,
            max_bytes,
            ..Default::default()
        };
        let page = runtime.read_records(read(2, 0)).await.unwrap();
        assert_eq!((page.records.len(), page.done), (2, false));
        assert_eq!(page.last_offset, b"b".to_vec());
        // The first Record comes over the byte cap too, so a read always moves on.
        let page = runtime.read_records(read(0, 1)).await.unwrap();
        assert_eq!((page.records.len(), page.done), (1, false));
        assert_eq!(page.last_offset, b"a".to_vec());
        let page = runtime.read_records(read(0, 0)).await.unwrap();
        assert_eq!((page.records.len(), page.done), (3, true));
    }

    #[tokio::test]
    async fn a_bad_offset_of_the_store_is_corrupt() {
        let (store, runtime) = runtime();
        for offset in [vec![], vec![1; MAX_OFFSET_BYTES + 1]] {
            *store.page.lock() = ReadRecordsResponse {
                records: vec![record(&offset, 1)],
                ..Default::default()
            };
            let request = ReadRecordsRequest {
                r#ref: stream_ref(),
                ..Default::default()
            };
            let error = runtime.read_records(request).await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            *store.appended.lock() = AppendRecordsResponse {
                first_offset: offset.clone(),
                last_offset: offset,
            };
            let epoch = WorkflowTaskEpoch {
                started_event_id: 3,
                attempt: 1,
                ..Default::default()
            };
            let error = runtime
                .append_records(append(workflow(Some(epoch))))
                .await
                .unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
        }
    }

    #[tokio::test]
    async fn a_bad_request_never_reaches_the_store() {
        let (store, runtime) = runtime();
        let bad_reads = [
            ReadRecordsRequest::default(),
            ReadRecordsRequest {
                r#ref: stream_ref(),
                from_offset: vec![1; MAX_OFFSET_BYTES + 1],
                ..Default::default()
            },
            ReadRecordsRequest {
                r#ref: stream_ref(),
                max_bytes: -1,
                ..Default::default()
            },
        ];
        for request in bad_reads {
            let error = runtime.read_records(request).await.unwrap_err();
            assert_eq!(error.reason, Reason::Invalid);
        }
        // A Workflow writes with its Workflow Task as the epoch, so the fence orders its writes.
        let error = runtime
            .append_records(append(workflow(None)))
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        assert!(store.reads.lock().is_empty());
        assert!(store.appends.lock().is_empty());
    }

    #[tokio::test]
    async fn an_append_gets_the_producer_with_its_epoch_and_the_error_of_the_store() {
        let (store, runtime) = runtime();
        let epoch = WorkflowTaskEpoch {
            started_event_id: 3,
            attempt: 1,
            ..Default::default()
        };
        *store.appended.lock() = AppendRecordsResponse {
            first_offset: b"a".to_vec(),
            last_offset: b"a".to_vec(),
        };
        let appended = runtime
            .append_records(append(workflow(Some(epoch))))
            .await
            .unwrap();
        assert_eq!(appended.last_offset, b"a".to_vec());
        assert_eq!(store.appends.lock()[0].producer, workflow(Some(epoch)));
        *store.error.lock() = Some(StreamError::new(Reason::Closed, "closed"));
        let error = runtime
            .append_records(append(workflow(Some(epoch))))
            .await
            .unwrap_err();
        assert_eq!(
            (error.reason, error.message.as_str()),
            (Reason::Closed, "closed")
        );
    }
}
