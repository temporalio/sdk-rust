use crate::{
    Reason, StreamError, StreamResult, StreamStore,
    ids::{check_offset, check_producer, check_stream_id},
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse, StreamIdConflictPolicy, StreamRef, StreamStatus,
    },
    record::{BatchLimits, MAX_RECORD_BYTES, producer_len, stored_len},
};
use prost::Message;
use std::{fmt, sync::Arc, time::Duration};

/// The highest batch `sequence`. It's the largest integer that a 64-bit float holds exactly, for a
/// Store that keeps its numbers as floats.
pub const MAX_SEQUENCE: i64 = (1 << 53) - 1;

/// The maximum number of Records in a page, and the page size when a read doesn't set one.
pub const MAX_RECORDS: usize = 1000;

/// The longest that a read may wait for new Records. A longer `wait` is capped to it.
pub const MAX_WAIT: Duration = Duration::from_secs(60);

/// Wraps a [`StreamStore`] and enforces the rules that the trait documents. It validates every
/// request before the Store sees it, and every response before the caller does. A response that
/// breaks a rule fails with [`Reason::StorageCorrupt`]. The runtime has no timeout of its own, so a
/// Store that never responds blocks the call.
pub struct StreamRuntime {
    store: Arc<dyn StreamStore>,
    limits: BatchLimits,
}

impl fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamRuntime")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl StreamRuntime {
    /// Creates a runtime for `store` that rejects a batch over `limits`.
    pub fn new(store: Arc<dyn StreamStore>, limits: BatchLimits) -> Self {
        Self { store, limits }
    }

    /// Creates a Stream, or returns the existing open one if the conflict policy allows it. If this
    /// fails with [`Reason::StorageCorrupt`], the Stream may still have been created.
    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        let id = check_ref(request.stream_ref.as_ref())?.id.clone();
        let policy = request.id_conflict_policy;
        if StreamIdConflictPolicy::try_from(policy).is_err() {
            return Err(StreamError::invalid(format!(
                "The conflict policy {policy} is unknown"
            )));
        }
        let created = self.store.create_stream(request).await?;
        // Stores ignore the Store name of a ref, so only the id has to match.
        match created.r#ref.as_ref().map(|stream| stream.id == id) {
            Some(true) => Ok(created),
            Some(false) => Err(StreamError::storage_corrupt(
                "The Store answered a create with the ref of another Stream",
            )),
            None => Err(StreamError::storage_corrupt(
                "The Store answered a create with no ref",
            )),
        }
    }

    /// Appends one batch from one Producer. If this fails with [`Reason::StorageCorrupt`], the
    /// batch may still be in the Stream, because the runtime can only check the response after the
    /// write.
    pub async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        check_ref(request.r#ref.as_ref())?;
        let producer = check_producer(request.producer.as_ref())?;
        if request.sequence < 0 || request.sequence > MAX_SEQUENCE {
            return Err(StreamError::invalid(
                "The sequence must be from 1 to 2^53 - 1, or zero for the next one",
            ));
        }
        if let Some(at) = request
            .records
            .iter()
            .position(|record| record.body.is_none())
        {
            return Err(StreamError::invalid(format!(
                "Record {at} of the batch has no body"
            )));
        }
        self.limits.check(producer, &request.records)?;
        let empty = request.records.is_empty();
        let appended = self.store.append_records(request).await?;
        for offset in [&appended.first_offset, &appended.last_offset] {
            if offset.is_empty() && !empty {
                return Err(StreamError::storage_corrupt(
                    "The Store answered an append with no offset",
                ));
            }
            check_offset(offset, "An offset from the Store", Reason::StorageCorrupt)?;
        }
        Ok(appended)
    }

    /// Reads one page of Records after `from_offset`. If the Store returns more than the read asked
    /// for, the page is trimmed and is no longer `done`. The first Record is always kept, so a read
    /// makes progress whenever the Store has something to return.
    pub async fn read_records(
        &self,
        mut request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        check_ref(request.r#ref.as_ref())?;
        check_offset(&request.from_offset, "The offset", Reason::Invalid)?;
        if request.max_records < 0 {
            return Err(StreamError::invalid("max_records must be 0 or more"));
        }
        if request.max_bytes < 0 {
            return Err(StreamError::invalid("max_bytes must be 0 or more"));
        }
        let max_records = match request.max_records {
            0 => MAX_RECORDS,
            max => (max as usize).min(MAX_RECORDS),
        };
        let max_bytes = match request.max_bytes {
            0 => usize::MAX,
            max => usize::try_from(max).unwrap_or(usize::MAX),
        };
        request.max_records = max_records as i32;
        request.wait = match request.wait {
            Some(wait) if wait.seconds < 0 || wait.nanos < 0 => {
                return Err(StreamError::invalid("wait must be 0 or more"));
            }
            Some(wait) => {
                let wait = Duration::new(wait.seconds as u64, wait.nanos as u32).min(MAX_WAIT);
                prost_types::Duration::try_from(wait)
                    .ok()
                    .filter(|_| !wait.is_zero())
            }
            None => None,
        };
        let from_offset = request.from_offset.clone();
        let page = self.store.read_records(request).await?;
        fit_page(page, &from_offset, max_records, max_bytes)
    }

    /// Closes a Stream.
    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        check_ref(request.r#ref.as_ref())?;
        check_producer(request.producer.as_ref())?;
        self.store.close_stream(request).await
    }

    /// Returns the status and the newest offset of a Stream.
    pub async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        check_ref(request.r#ref.as_ref())?;
        let described = self.store.describe_stream(request).await?;
        check_offset(
            &described.last_offset,
            "The last offset from the Store",
            Reason::StorageCorrupt,
        )?;
        // A newer Store may return a status that this build doesn't know. The caller decides what
        // to do with it.
        if described.status == StreamStatus::Unspecified as i32 {
            return Err(StreamError::storage_corrupt(
                "The Store answered a describe with no status",
            ));
        }
        Ok(described)
    }
}

fn check_ref(stream: Option<&StreamRef>) -> StreamResult<&StreamRef> {
    let stream = stream.ok_or_else(|| StreamError::invalid("The request has no Stream ref"))?;
    check_stream_id(&stream.id)?;
    Ok(stream)
}

fn fit_page(
    mut page: ReadRecordsResponse,
    from_offset: &[u8],
    max_records: usize,
    max_bytes: usize,
) -> StreamResult<ReadRecordsResponse> {
    let mut newest = from_offset;
    for (at, record) in page.records.iter().enumerate() {
        if record.offset.is_empty() {
            return Err(StreamError::storage_corrupt(
                "The Store gave a Record with no offset",
            ));
        }
        check_offset(
            &record.offset,
            "An offset from the Store",
            Reason::StorageCorrupt,
        )?;
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
        // Readers tell a Producer's attempts apart by the Producer of each Record, so it has to be
        // there.
        let producer = record.producer.as_ref();
        let producer = producer.filter(|p| p.kind.is_some() && !p.id.is_empty());
        let (Some(producer), Some(body)) = (producer, record.body.as_ref()) else {
            return Err(StreamError::storage_corrupt(format!(
                "The Store gave Record {at} with no Producer id or kind, or no body"
            )));
        };
        let size = stored_len(producer_len(producer), Some(body));
        if size > MAX_RECORD_BYTES {
            return Err(StreamError::storage_corrupt(format!(
                "The Store gave Record {at} with {size} bytes, more than {MAX_RECORD_BYTES}"
            )));
        }
        newest = &record.offset;
    }
    check_offset(
        &page.last_offset,
        "The last offset from the Store",
        Reason::StorageCorrupt,
    )?;
    if page.last_offset.as_slice() < newest {
        return Err(StreamError::storage_corrupt(if page.records.is_empty() {
            "The Store gave an empty page with a last_offset that is lower than the from_offset \
             of the read"
        } else {
            "The Store gave a last_offset that is lower than the offset of the last Record"
        }));
    }
    let mut bytes = 0;
    let mut keep = 0;
    for record in page.records.iter().take(max_records) {
        bytes += record.encoded_len();
        if keep > 0 && bytes > max_bytes {
            break;
        }
        keep += 1;
    }
    if keep < page.records.len() {
        page.records.truncate(keep);
        page.last_offset = page.records[keep - 1].offset.clone();
        page.done = false;
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::{AppendRecord, StreamRecord},
        record::data_record,
        test_store::{Request, Scripted, client, runtime, task, workflow},
    };
    use std::fmt::Debug;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    fn stream() -> Option<StreamRef> {
        Some(StreamRef {
            id: "s".to_string(),
            store: "redis".to_string(),
        })
    }

    fn record() -> AppendRecord {
        AppendRecord {
            body: Some(Payload::default()),
        }
    }

    fn valid_create() -> CreateStreamRequest {
        CreateStreamRequest {
            namespace: "ns".to_string(),
            stream_ref: stream(),
            id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
            request_id: "r".to_string(),
        }
    }

    fn valid_append() -> AppendRecordsRequest {
        AppendRecordsRequest {
            namespace: "ns".to_string(),
            r#ref: stream(),
            producer: Some(workflow(Some(task(3, 1, 0)))),
            sequence: 1,
            records: vec![record(); 2],
        }
    }

    fn valid_read() -> ReadRecordsRequest {
        ReadRecordsRequest {
            r#ref: stream(),
            ..Default::default()
        }
    }

    fn valid_close() -> CloseStreamRequest {
        CloseStreamRequest {
            namespace: "ns".to_string(),
            r#ref: stream(),
            producer: Some(client()),
        }
    }

    fn valid_describe() -> DescribeStreamRequest {
        DescribeStreamRequest {
            namespace: "ns".to_string(),
            r#ref: stream(),
        }
    }

    fn seconds(seconds: i64) -> Option<prost_types::Duration> {
        Some(prost_types::Duration { seconds, nanos: 0 })
    }

    fn page(offsets: &[&str], last_offset: &str, done: bool) -> ReadRecordsResponse {
        let record = |offset: &&str| StreamRecord {
            producer: Some(client()),
            body: Some(Payload::default()),
            offset: offset.as_bytes().to_vec(),
        };
        ReadRecordsResponse {
            records: offsets.iter().map(record).collect(),
            last_offset: last_offset.as_bytes().to_vec(),
            done,
        }
    }

    fn expect<T: PartialEq + Debug>(result: StreamResult<T>, answer: T, corrupt: Option<&str>) {
        match corrupt {
            None => assert_eq!(result, Ok(answer)),
            Some(message) => assert_eq!(result, Err(StreamError::storage_corrupt(message))),
        }
    }

    #[tokio::test]
    async fn a_bad_request_fails_before_the_store() {
        let store = Arc::new(Scripted::default());
        let limits = BatchLimits::default().with_max_batch_records(2).unwrap();
        let runtime = StreamRuntime::new(store.clone(), limits);
        let create = async |change: fn(&mut CreateStreamRequest)| {
            let mut request = valid_create();
            change(&mut request);
            runtime.create_stream(request).await.map(drop)
        };
        let append = async |change: fn(&mut AppendRecordsRequest)| {
            let mut request = valid_append();
            change(&mut request);
            runtime.append_records(request).await.map(drop)
        };
        let read = async |change: fn(&mut ReadRecordsRequest)| {
            let mut request = valid_read();
            change(&mut request);
            runtime.read_records(request).await.map(drop)
        };
        let close = async |change: fn(&mut CloseStreamRequest)| {
            let mut request = valid_close();
            change(&mut request);
            runtime.close_stream(request).await.map(drop)
        };
        let describe = async |change: fn(&mut DescribeStreamRequest)| {
            let mut request = valid_describe();
            change(&mut request);
            runtime.describe_stream(request).await.map(drop)
        };
        let invalid = StreamError::invalid;
        let no_ref = || invalid("The request has no Stream ref");
        let sequence =
            || invalid("The sequence must be from 1 to 2^53 - 1, or zero for the next one");
        let cases = [
            (create(|r| r.stream_ref = None).await, no_ref()),
            (
                create(|r| r.id_conflict_policy = 99).await,
                invalid("The conflict policy 99 is unknown"),
            ),
            (append(|r| r.r#ref = None).await, no_ref()),
            (
                append(|r| r.producer = None).await,
                invalid("The request has no Producer"),
            ),
            (
                append(|r| r.producer = Some(workflow(None))).await,
                invalid("The Producer has no Workflow Task as its epoch"),
            ),
            (append(|r| r.sequence = -1).await, sequence()),
            (append(|r| r.sequence = MAX_SEQUENCE + 1).await, sequence()),
            (
                append(|r| r.records[1].body = None).await,
                invalid("Record 1 of the batch has no body"),
            ),
            (
                append(|r| r.records.push(record())).await,
                StreamError::new(Reason::TooLarge, "The batch has 3 Records, more than 2"),
            ),
            (read(|r| r.r#ref = None).await, no_ref()),
            (
                read(|r| r.from_offset = vec![0; 1025]).await,
                invalid("The offset has 1025 bytes, more than 1024"),
            ),
            (
                read(|r| r.max_records = -1).await,
                invalid("max_records must be 0 or more"),
            ),
            (
                read(|r| r.max_bytes = -1).await,
                invalid("max_bytes must be 0 or more"),
            ),
            (
                read(|r| r.wait = seconds(-1)).await,
                invalid("wait must be 0 or more"),
            ),
            (close(|r| r.r#ref = None).await, no_ref()),
            (
                close(|r| r.producer = None).await,
                invalid("The request has no Producer"),
            ),
            (describe(|r| r.r#ref = None).await, no_ref()),
        ];
        for (result, error) in cases {
            assert_eq!(result, Err(error));
        }
        assert_eq!(store.requests(), Vec::<Request>::new());
    }

    #[tokio::test]
    async fn a_valid_request_reaches_the_store_unchanged() {
        let (store, runtime) = runtime();
        {
            let mut answers = store.answers.lock();
            answers.created.r#ref = stream();
            answers.appended.first_offset = b"a".to_vec();
            answers.appended.last_offset = b"b".to_vec();
            answers.described.status = StreamStatus::Open as i32;
        }
        let appends = [0, MAX_SEQUENCE].map(|sequence| AppendRecordsRequest {
            sequence,
            ..valid_append()
        });
        runtime.create_stream(valid_create()).await.unwrap();
        for append in &appends {
            runtime.append_records(append.clone()).await.unwrap();
        }
        runtime.close_stream(valid_close()).await.unwrap();
        runtime.describe_stream(valid_describe()).await.unwrap();
        let [lowest, highest] = appends;
        let sent = [
            Request::Create(valid_create()),
            Request::Append(lowest),
            Request::Append(highest),
            Request::Close(valid_close()),
            Request::Describe(valid_describe()),
        ];
        assert_eq!(store.requests(), sent);
    }

    #[tokio::test]
    async fn a_read_reaches_the_store_with_its_caps_and_wait() {
        let (store, runtime) = runtime();
        store.answers.lock().page = page(&[], "a", false);
        let short = Some(prost_types::Duration {
            seconds: 1,
            nanos: 5,
        });
        let rows = [
            (0, None, 1000, None),
            (1, seconds(0), 1, None),
            (7, short, 7, short),
            (1001, seconds(3600), 1000, seconds(60)),
        ];
        for (max_records, wait, sent_max_records, sent_wait) in rows {
            let read = ReadRecordsRequest {
                from_offset: b"a".to_vec(),
                max_records,
                wait,
                max_bytes: 10,
                ..valid_read()
            };
            runtime.read_records(read.clone()).await.unwrap();
            let sent = ReadRecordsRequest {
                max_records: sent_max_records,
                wait: sent_wait,
                ..read
            };
            assert_eq!(store.requests().last(), Some(&Request::Read(sent)));
        }
    }

    #[tokio::test]
    async fn a_page_over_its_caps_is_cut_and_not_done() {
        let (store, runtime) = runtime();
        let answer = page(&["a", "b", "c"], "c", true);
        store.answers.lock().page = answer.clone();
        let size = answer.records[0].encoded_len() as i64;
        let first_two = page(&["a", "b"], "b", false);
        let rows = [
            (2, 0, first_two.clone()),
            (0, 2 * size, first_two),
            (0, 2 * size - 1, page(&["a"], "a", false)),
            (0, 1, page(&["a"], "a", false)),
            (0, 0, answer),
        ];
        for (max_records, max_bytes, cut) in rows {
            let read = ReadRecordsRequest {
                max_records,
                max_bytes,
                ..valid_read()
            };
            assert_eq!(runtime.read_records(read).await, Ok(cut));
        }
    }

    #[tokio::test]
    async fn an_answer_that_breaks_the_contract_is_corrupt() {
        let (store, runtime) = runtime();
        let longest = "a".repeat(1024);
        let long = "a".repeat(1025);
        let long_offset = "An offset from the Store has 1025 bytes, more than 1024";
        let long_last = "The last offset from the Store has 1025 bytes, more than 1024";
        let first = "The Store gave a first Record with an offset that is not higher than the \
                     from_offset of the read";
        let second = "The Store gave Record 1 with an offset that is not higher than the one \
                      before it";
        let below_record =
            "The Store gave a last_offset that is lower than the offset of the last Record";
        let below_from = "The Store gave an empty page with a last_offset that is lower than the \
                          from_offset of the read";
        let incomplete = "The Store gave Record 1 with no Producer id or kind, or no body";
        let broken = |change: fn(&mut StreamRecord)| {
            let mut answer = page(&["a", "b"], "b", false);
            change(&mut answer.records[1]);
            answer
        };
        let large = broken(|r| r.body.as_mut().unwrap().data = vec![0; MAX_RECORD_BYTES]);
        let body = large.records[1].body.clone();
        let size = data_record(&client(), AppendRecord { body }).len();
        let too_large = format!("The Store gave Record 1 with {size} bytes, more than 1572864");
        let reads = [
            ("", page(&["a", "b"], "b", false), None),
            ("a", page(&["b"], "z", true), None),
            ("m", page(&[], "m", false), None),
            ("m", page(&[], "z", true), None),
            ("", page(&[&longest], &longest, false), None),
            (
                "",
                page(&[""], "", false),
                Some("The Store gave a Record with no offset"),
            ),
            ("", page(&[&long], &long, false), Some(long_offset)),
            ("", page(&["a"], &long, false), Some(long_last)),
            ("a", page(&["a"], "a", false), Some(first)),
            ("", page(&["b", "a"], "b", false), Some(second)),
            ("", page(&["a", "a"], "a", false), Some(second)),
            ("a", page(&["b"], "", false), Some(below_record)),
            ("m", page(&[], "a", false), Some(below_from)),
            ("", broken(|r| r.producer = None), Some(incomplete)),
            (
                "",
                broken(|r| r.producer.as_mut().unwrap().kind = None),
                Some(incomplete),
            ),
            (
                "",
                broken(|r| r.producer.as_mut().unwrap().id.clear()),
                Some(incomplete),
            ),
            ("", broken(|r| r.body = None), Some(incomplete)),
            ("", large, Some(too_large.as_str())),
        ];
        for (from_offset, answer, corrupt) in reads {
            store.answers.lock().page = answer.clone();
            let read = ReadRecordsRequest {
                from_offset: from_offset.as_bytes().to_vec(),
                ..valid_read()
            };
            expect(runtime.read_records(read).await, answer, corrupt);
        }
        let no_offset = "The Store answered an append with no offset";
        let appends = [
            (1, "a", "b", None),
            (1, longest.as_str(), longest.as_str(), None),
            (0, "", "", None),
            (1, "", "b", Some(no_offset)),
            (1, "a", "", Some(no_offset)),
            (1, long.as_str(), "b", Some(long_offset)),
            (1, "a", long.as_str(), Some(long_offset)),
            (0, long.as_str(), "", Some(long_offset)),
        ];
        for (records, first_offset, last_offset, corrupt) in appends {
            let answer = AppendRecordsResponse {
                first_offset: first_offset.as_bytes().to_vec(),
                last_offset: last_offset.as_bytes().to_vec(),
            };
            store.answers.lock().appended = answer.clone();
            let append = AppendRecordsRequest {
                records: vec![record(); records],
                ..valid_append()
            };
            expect(runtime.append_records(append).await, answer, corrupt);
        }
        let named = |id: &str, store: &str| StreamRef {
            id: id.to_string(),
            store: store.to_string(),
        };
        let creates = [
            (Some(named("s", "memory")), None),
            (None, Some("The Store answered a create with no ref")),
            (
                Some(named("other", "redis")),
                Some("The Store answered a create with the ref of another Stream"),
            ),
        ];
        for (r#ref, corrupt) in creates {
            let answer = CreateStreamResponse {
                r#ref,
                created: false,
            };
            store.answers.lock().created = answer.clone();
            expect(runtime.create_stream(valid_create()).await, answer, corrupt);
        }
        let open = StreamStatus::Open as i32;
        let describes = [
            (99, "", None),
            (open, longest.as_str(), None),
            (
                StreamStatus::Unspecified as i32,
                "",
                Some("The Store answered a describe with no status"),
            ),
            (open, long.as_str(), Some(long_last)),
        ];
        for (status, last_offset, corrupt) in describes {
            let answer = DescribeStreamResponse {
                status,
                last_offset: last_offset.as_bytes().to_vec(),
            };
            store.answers.lock().described = answer.clone();
            expect(
                runtime.describe_stream(valid_describe()).await,
                answer,
                corrupt,
            );
        }
    }
}
