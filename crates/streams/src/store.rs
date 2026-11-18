//! The Store of the Stream runtime.

use crate::{
    StreamError, StreamResult,
    ids::MAX_OFFSET_BYTES,
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse,
    },
};
use prost::Message;
use std::time::Duration;

/// The longest wait of one read for new Records.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The maximum number of Records in one read.
pub const MAX_RECORDS: usize = 1000;

/// A Stream Store. It answers each operation of the Stream service whole: `create_stream`,
/// `append_records`, `read_records`, `close_stream` and `describe_stream`. It takes and gives the
/// messages of the Stream service. The Store applies the rules of the service itself.
///
/// - An append fences a lower epoch of its Producer, and writes a batch that comes again with the
///   same Producer, epoch and sequence once.
/// - A read waits up to its `wait` for a Record when it has none to give, and a close wakes it.
/// - An offset is opaque bytes, at most [MAX_OFFSET_BYTES], and the offsets of one Stream compare
///   in order.
/// - A failed call gives a [StreamError] with its reason.
///   [StorageUnavailable](crate::Reason::StorageUnavailable) is the one reason that a Worker tries
///   again.
///
/// A lang bridge implements the trait for a Store that is written in that language. The Stores that
/// Temporal provides, and the shared rules that a Store in Rust builds on, are in
/// [temporal-community/sdk-streams](https://github.com/temporal-community/sdk-streams).
#[async_trait::async_trait]
pub trait StreamStore: Send + Sync + 'static {
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

/// Refuses an offset that a Store gives with more than [MAX_OFFSET_BYTES]. An empty one passes.
/// `what` is the offset's place in the message of the error.
pub(crate) fn offset_bound(offset: &[u8], what: &str) -> StreamResult<()> {
    if offset.len() > MAX_OFFSET_BYTES {
        return Err(StreamError::storage_corrupt(format!(
            "The Store gave {what} of {} bytes, more than {MAX_OFFSET_BYTES}",
            offset.len()
        )));
    }
    Ok(())
}

/// Refuses an offset that a Store gives for a Record, empty or with more than [MAX_OFFSET_BYTES].
/// A Worker records the offsets in History and resumes a read from one, and an empty one is the
/// start.
fn store_offset(offset: &[u8]) -> StreamResult<()> {
    if offset.is_empty() {
        return Err(StreamError::storage_corrupt(
            "The Store gave a Record with no offset",
        ));
    }
    offset_bound(offset, "an offset")
}

/// A proto Duration as a `Duration`, `None` when it is unset, zero or negative.
fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// The byte cap of a page, from a request's `max_bytes`. Zero is no cap.
fn max_bytes(value: i64) -> StreamResult<usize> {
    match value {
        0 => Ok(usize::MAX),
        n if n < 0 => Err(StreamError::invalid("max_bytes must be 0 or more")),
        n => Ok(usize::try_from(n).unwrap_or(usize::MAX)),
    }
}

/// Reads one page, with the caps and the longest wait of a read.
pub(crate) async fn read(
    store: &dyn StreamStore,
    metrics: &Metrics,
    mut request: ReadRecordsRequest,
) -> StreamResult<ReadRecordsResponse> {
    caller_offset(&request.from_offset)?;
    let from_offset = request.from_offset.clone();
    let max_bytes = max_bytes(request.max_bytes)?;
    let max = match request.max_records {
        n if n <= 0 => MAX_RECORDS,
        n => (n as usize).min(MAX_RECORDS),
    };
    request.max_records = max as i32;
    request.wait = duration(request.wait)
        .map(|wait| wait.min(MAX_WAIT))
        .and_then(|wait| prost_types::Duration::try_from(wait).ok());
    let response = {
        // The Store holds a read for up to its `wait`. The guard counts the read down again when
        // the Store answers and when the read's future is dropped.
        let _waiting = metrics.read_waiting();
        store.read_records(request).await?
    };
    fit_page(response, &from_offset, max, max_bytes)
}

/// The page cut to `max` Records and `max_bytes`, with its first Record always. A Worker bounds
/// what one read holds by these caps, so a Store that gives more must not pass them. A cut page
/// ends at its last Record and is not done.
fn fit_page(
    mut page: ReadRecordsResponse,
    from_offset: &[u8],
    max: usize,
    max_bytes: usize,
) -> StreamResult<ReadRecordsResponse> {
    for record in &page.records {
        store_offset(&record.offset)?;
    }
    offset_bound(&page.last_offset, "a last offset")?;
    check_cursor(&page, from_offset)?;
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

/// Refuses a page whose offsets don't rise after `from_offset`, or whose `last_offset` is lower
/// than its last Record. Offsets compare as bytes, and an empty `from_offset` is the start. A
/// Worker resumes a read from `last_offset`, so a bad cursor makes a read that never moves on.
/// A `last_offset` above the last Record is fine, since a Store may skip entries that are not
/// Records, and the page that ends a closed Stream may cover them.
fn check_cursor(page: &ReadRecordsResponse, from_offset: &[u8]) -> StreamResult<()> {
    let mut newest = from_offset;
    for (at, record) in page.records.iter().enumerate() {
        if record.offset.as_slice() <= newest {
            return Err(StreamError::storage_corrupt(if at == 0 {
                "The Store gave a first Record with an offset that is not higher than the \
                 from_offset of the read"
                    .to_string()
            } else {
                format!(
                    "The Store gave Record {at} with an offset that is not higher than the one \
                     before it"
                )
            }));
        }
        newest = &record.offset;
    }
    if page.last_offset.as_slice() < newest {
        return Err(StreamError::storage_corrupt(if page.records.is_empty() {
            "The Store gave an empty page with a last_offset that is lower than the from_offset \
             of the read"
        } else {
            "The Store gave a last_offset that is lower than the offset of the last Record"
        }));
    }
    Ok(())
}

/// Appends a batch that the runtime checked, and checks the offsets of the answer.
pub(crate) async fn append(
    store: &dyn StreamStore,
    request: AppendRecordsRequest,
) -> StreamResult<AppendRecordsResponse> {
    let empty = request.records.is_empty();
    let response = store.append_records(request).await?;
    if empty {
        // A Store may give empty offsets for an empty batch, but not long ones.
        offset_bound(&response.first_offset, "an offset")?;
        offset_bound(&response.last_offset, "an offset")?;
    } else {
        store_offset(&response.first_offset)?;
        store_offset(&response.last_offset)?;
        if response.first_offset > response.last_offset {
            return Err(StreamError::storage_corrupt(
                "The Store gave a first offset that is higher than the last offset of the batch",
            ));
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Reason,
        proto::{
            AppendRecord, Producer, StreamRecord, StreamRef, WorkflowProducer, WorkflowTaskEpoch,
            producer::Kind,
        },
        test_store::runtime,
    };
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    #[test]
    fn a_zero_or_negative_duration_is_unset_and_a_huge_one_stays() {
        let proto = |seconds, nanos| Some(prost_types::Duration { seconds, nanos });
        assert_eq!(duration(None), None);
        assert_eq!(duration(proto(0, 0)), None);
        assert_eq!(duration(proto(-5, 0)), None);
        assert_eq!(duration(proto(0, -5)), None);
        assert_eq!(
            duration(proto(i64::MAX, 999_999_999)),
            Some(Duration::new(i64::MAX as u64, 999_999_999))
        );
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

    fn epoch() -> WorkflowTaskEpoch {
        WorkflowTaskEpoch {
            started_event_id: 3,
            attempt: 1,
            ..Default::default()
        }
    }

    /// A page of Records with these offsets, as a Store gives it.
    fn page(offsets: &[&str], last_offset: &str, done: bool) -> ReadRecordsResponse {
        ReadRecordsResponse {
            records: offsets
                .iter()
                .map(|offset| record(offset.as_bytes(), 1))
                .collect(),
            last_offset: last_offset.as_bytes().to_vec(),
            done,
        }
    }

    /// What the runtime gives for a read after `from_offset` when the Store answers `answer`.
    async fn read_after(
        from_offset: &str,
        answer: ReadRecordsResponse,
    ) -> StreamResult<ReadRecordsResponse> {
        let (store, runtime) = runtime();
        *store.page.lock() = answer;
        runtime
            .read_records(ReadRecordsRequest {
                r#ref: stream_ref(),
                from_offset: from_offset.as_bytes().to_vec(),
                ..Default::default()
            })
            .await
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
        assert_eq!(duration(sent.wait), Some(MAX_WAIT));
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

    #[tokio::test]
    async fn a_page_with_a_bad_cursor_is_corrupt() {
        let first = "The Store gave a first Record with an offset that is not higher than the \
                     from_offset of the read";
        let record_at = |at| {
            format!(
                "The Store gave Record {at} with an offset that is not higher than the one \
                 before it"
            )
        };
        let last = "The Store gave a last_offset that is lower than the offset of the last Record";
        let empty = "The Store gave an empty page with a last_offset that is lower than the \
                     from_offset of the read";
        let cases = [
            // The Records are not after the offset of the read, and the cursor is empty.
            ("m", page(&["b", "a", "a"], "", false), first.to_string()),
            ("a", page(&["a"], "a", false), first.to_string()),
            ("b", page(&["a", "c"], "c", false), first.to_string()),
            ("", page(&["b", "a"], "b", false), record_at(1)),
            ("", page(&["a", "a"], "a", false), record_at(1)),
            ("", page(&["a", "b", "b"], "b", false), record_at(2)),
            ("a", page(&["b"], "", false), last.to_string()),
            ("", page(&["b", "c"], "b", true), last.to_string()),
            ("m", page(&[], "", false), empty.to_string()),
            ("m", page(&[], "a", true), empty.to_string()),
        ];
        for (from_offset, answer, message) in cases {
            let error = read_after(from_offset, answer.clone()).await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt, "{answer:?}");
            assert_eq!(error.message, message, "after {from_offset:?}, {answer:?}");
        }
    }

    #[tokio::test]
    async fn a_page_with_a_good_cursor_passes_as_it_is() {
        let cases = [
            ("", page(&["a", "b", "c"], "c", false)),
            ("a", page(&["b", "c"], "c", false)),
            // A Store may skip entries that are not Records, so the page that ends a closed Stream
            // may cover them. Its `last_offset` is then above its last Record.
            ("a", page(&["b"], "z", true)),
            ("", page(&[], "", false)),
            ("m", page(&[], "m", false)),
            ("m", page(&[], "z", true)),
        ];
        for (from_offset, answer) in cases {
            let page = read_after(from_offset, answer.clone()).await.unwrap();
            assert_eq!(page, answer, "after {from_offset:?}");
        }
    }

    #[tokio::test]
    async fn an_append_answer_with_a_first_offset_above_the_last_is_corrupt() {
        let (store, runtime) = runtime();
        for (first, last, good) in [("a", "a", true), ("a", "c", true), ("c", "a", false)] {
            *store.appended.lock() = AppendRecordsResponse {
                first_offset: first.as_bytes().to_vec(),
                last_offset: last.as_bytes().to_vec(),
            };
            let result = runtime
                .append_records(append(workflow(Some(epoch()))))
                .await;
            if good {
                assert_eq!(result.unwrap().last_offset, last.as_bytes().to_vec());
                continue;
            }
            let error = result.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            assert_eq!(
                error.message,
                "The Store gave a first offset that is higher than the last offset of the batch"
            );
        }
    }

    #[tokio::test]
    async fn the_offsets_of_an_empty_batch_stay_within_the_bound() {
        let (store, runtime) = runtime();
        let empty = || AppendRecordsRequest {
            records: vec![],
            ..append(workflow(Some(epoch())))
        };
        for (first, last) in [(0, 0), (MAX_OFFSET_BYTES, MAX_OFFSET_BYTES)] {
            *store.appended.lock() = AppendRecordsResponse {
                first_offset: vec![1; first],
                last_offset: vec![1; last],
            };
            runtime.append_records(empty()).await.unwrap();
        }
        let too_long = MAX_OFFSET_BYTES + 1;
        for (first, last) in [(too_long, 0), (0, too_long)] {
            *store.appended.lock() = AppendRecordsResponse {
                first_offset: vec![1; first],
                last_offset: vec![1; last],
            };
            let error = runtime.append_records(empty()).await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            assert_eq!(
                error.message,
                format!("The Store gave an offset of {too_long} bytes, more than 1024")
            );
        }
    }
}
