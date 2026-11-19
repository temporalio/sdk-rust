use crate::{
    AnyReason, AppendError, CloseError, CreateError, DescribeError, ReadError, StreamError,
    StreamStore,
    ids::{check_offset, check_producer, check_stream_id},
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse, StreamIdConflictPolicy, StreamStatus,
    },
    record::{StoreLimits, field_len, producer_len, stored_len},
};
use prost::Message;
use std::{fmt, sync::Arc};

/// The checks around one [`StreamStore`]. The runtime validates every request before the Store
/// sees it, and every response before the caller does. The docs of [`StreamStore`] say which rules
/// it checks. The runtime has no timeout of its own, so a Store that never responds blocks the
/// call.
///
/// The runtime refuses a batch over its [`StoreLimits`], and fails a read that gives a Record over
/// their Record size. The limits are the caller's. An SDK builds the runtime of each Store with
/// the limits of its Stream config, and a Store that runs a runtime around itself builds it with
/// [`StoreLimits::UNBOUNDED`].
pub struct StreamRuntime {
    store: Arc<dyn StreamStore>,
    limits: StoreLimits,
}

impl fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamRuntime")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl StreamRuntime {
    /// Creates a runtime that calls `store`, and that refuses a batch or a Record over `limits`.
    pub fn new(store: Arc<dyn StreamStore>, limits: StoreLimits) -> Self {
        Self { store, limits }
    }

    /// Creates a Stream. If an open Stream already has the id, the create fails, or it returns
    /// `created` false with the `UseExisting` policy.
    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, CreateError> {
        check_stream_id(&request.stream_id)?;
        let policy = request.id_conflict_policy;
        if StreamIdConflictPolicy::try_from(policy).is_err() {
            return Err(StreamError::invalid_argument(format!(
                "The conflict policy {policy} is unknown"
            )));
        }
        self.store.create_stream(request).await
    }

    /// Appends one batch of at least one Record from one Producer. If this fails with
    /// [`AnyReason::StoreCorrupt`], the batch may still be in the Stream, because the runtime can
    /// only check the response after the write.
    pub async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> Result<AppendRecordsResponse, AppendError> {
        self.limits.check_append(&request)?;
        let appended = self.store.append_records(request).await?;
        for offset in [&appended.first_offset, &appended.last_offset] {
            if offset.is_empty() {
                return Err(StreamError::store_corrupt(
                    "The Store answered an append with no offset",
                ));
            }
            check_offset(offset, "An offset from the Store", AnyReason::StoreCorrupt)?;
        }
        if appended.first_offset > appended.last_offset {
            return Err(StreamError::store_corrupt(
                "The Store answered an append with a first offset that is higher than its last \
                 offset",
            ));
        }
        Ok(appended)
    }

    /// Reads one page of Records after `from_offset`. The Store gets `max_records` and `wait` as
    /// the caller set them, and a `max_records` of 0 leaves the page size to the Store. If the
    /// Store returns more than the read asked for, the page is trimmed and is no longer `done`. The
    /// first Record is always kept, so a read makes progress whenever the Store has something to
    /// return. `max_bytes` counts each Record as the page encodes it, with the tag and the length
    /// of its field. The runtime checks only the Records that are left after the trim.
    pub async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> Result<ReadRecordsResponse, ReadError> {
        check_stream_id(&request.stream_id)?;
        check_offset(
            &request.from_offset,
            "The from_offset of the read",
            AnyReason::InvalidArgument,
        )?;
        if request.max_records < 0 {
            return Err(StreamError::invalid_argument(
                "max_records must be 0 or more",
            ));
        }
        if request.max_bytes < 0 {
            return Err(StreamError::invalid_argument("max_bytes must be 0 or more"));
        }
        if (request.wait.as_ref()).is_some_and(|wait| wait.seconds < 0 || wait.nanos < 0) {
            return Err(StreamError::invalid_argument("wait must be 0 or more"));
        }
        let max_records = match request.max_records {
            0 => usize::MAX,
            max => max as usize,
        };
        let max_bytes = match request.max_bytes {
            0 => usize::MAX,
            max => usize::try_from(max).unwrap_or(usize::MAX),
        };
        let from_offset = request.from_offset.clone();
        let page = self.store.read_records(request).await?;
        let max_record_bytes = self.limits.max_record_bytes;
        fit_page(page, &from_offset, max_records, max_bytes, max_record_bytes)
    }

    /// Closes a Stream.
    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> Result<CloseStreamResponse, CloseError> {
        check_stream_id(&request.stream_id)?;
        check_producer(request.producer.as_ref())?;
        self.store.close_stream(request).await
    }

    /// Returns the status of a Stream, and the offset of its newest Record as `last_offset`.
    pub async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<DescribeStreamResponse, DescribeError> {
        check_stream_id(&request.stream_id)?;
        let described = self.store.describe_stream(request).await?;
        check_offset(
            &described.last_offset,
            "The last offset from the Store",
            AnyReason::StoreCorrupt,
        )?;
        // A newer Store may return a status that this build doesn't know. The caller decides what
        // to do with it.
        if described.status == StreamStatus::Unspecified as i32 {
            return Err(StreamError::store_corrupt(
                "The Store answered a describe with no status",
            ));
        }
        Ok(described)
    }
}

fn fit_page(
    mut page: ReadRecordsResponse,
    from_offset: &[u8],
    max_records: usize,
    max_bytes: usize,
    max_record_bytes: usize,
) -> Result<ReadRecordsResponse, ReadError> {
    // The trim comes first, so a Record that the caller doesn't get can't fail the read.
    let mut bytes = 0;
    let mut keep = 0;
    for record in page.records.iter().take(max_records) {
        bytes += field_len(record.encoded_len());
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
    let mut newest = from_offset;
    for (at, record) in page.records.iter().enumerate() {
        if record.offset.is_empty() {
            return Err(StreamError::store_corrupt(
                "The Store gave a Record with no offset",
            ));
        }
        check_offset(
            &record.offset,
            "An offset from the Store",
            AnyReason::StoreCorrupt,
        )?;
        if record.offset.as_slice() <= newest {
            return Err(StreamError::store_corrupt(if at == 0 {
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
        // A later version of the API can add a kind of Producer, which this build decodes as a
        // Producer with no kind. So only a Record with no Producer at all breaks the contract.
        let (Some(producer), Some(body)) = (record.producer.as_ref(), record.body.as_ref()) else {
            return Err(StreamError::store_corrupt(format!(
                "The Store gave Record {at} with no Producer or no body"
            )));
        };
        // The limit is this runtime's, so another runtime of the Store may take the Record.
        let size = stored_len(producer_len(producer), Some(body));
        if size > max_record_bytes {
            return Err(StreamError::new(
                AnyReason::Unsupported,
                format!(
                    "The Store gave Record {at} with {size} bytes, and this runtime takes at most \
                     {max_record_bytes}"
                ),
            ));
        }
        newest = &record.offset;
    }
    check_offset(
        &page.last_offset,
        "The last offset from the Store",
        AnyReason::StoreCorrupt,
    )?;
    if page.last_offset.as_slice() < newest {
        return Err(StreamError::store_corrupt(if page.records.is_empty() {
            "The Store gave an empty page with a last_offset that is lower than the from_offset \
             of the read"
        } else {
            "The Store gave a last_offset that is lower than the offset of the last Record"
        }));
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AnyError, AppendReason,
        proto::{AppendRecord, StreamRecord},
        record::DEFAULT_MAX_RECORD_BYTES,
        test_store::{Scripted, client, runtime, task, workflow},
    };
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::fmt::Debug;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    fn record() -> AppendRecord {
        AppendRecord {
            body: Some(Payload::default()),
        }
    }

    fn valid_create() -> CreateStreamRequest {
        CreateStreamRequest {
            stream_id: "s".to_string(),
            id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
            request_id: "r".to_string(),
        }
    }

    fn valid_append() -> AppendRecordsRequest {
        AppendRecordsRequest {
            stream_id: "s".to_string(),
            producer: Some(workflow(Some(task(3, 1, 0)))),
            request_id: "r".to_string(),
            records: vec![record(); 2],
        }
    }

    fn valid_read() -> ReadRecordsRequest {
        ReadRecordsRequest {
            stream_id: "s".to_string(),
            ..Default::default()
        }
    }

    fn valid_close() -> CloseStreamRequest {
        CloseStreamRequest {
            stream_id: "s".to_string(),
            producer: Some(client()),
        }
    }

    fn valid_describe() -> DescribeStreamRequest {
        DescribeStreamRequest {
            stream_id: "s".to_string(),
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

    fn expect<T, R>(result: Result<T, StreamError<R>>, answer: T, corrupt: Option<&str>)
    where
        T: PartialEq + Debug,
        R: From<AnyReason> + PartialEq + Debug,
    {
        match corrupt {
            None => assert_eq!(result, Ok(answer)),
            Some(message) => assert_eq!(result, Err(StreamError::store_corrupt(message))),
        }
    }

    #[tokio::test]
    async fn a_bad_request_fails_before_the_store() {
        let store = Arc::new(Scripted::default());
        let limits = StoreLimits::default().with_max_batch_records(2);
        let runtime = StreamRuntime::new(store.clone(), limits.unwrap());
        let create = async |change: fn(&mut CreateStreamRequest)| {
            let mut request = valid_create();
            change(&mut request);
            runtime
                .create_stream(request)
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        };
        let append = async |change: fn(&mut AppendRecordsRequest)| {
            let mut request = valid_append();
            change(&mut request);
            runtime
                .append_records(request)
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        };
        let read = async |change: fn(&mut ReadRecordsRequest)| {
            let mut request = valid_read();
            change(&mut request);
            runtime
                .read_records(request)
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        };
        let close = async |change: fn(&mut CloseStreamRequest)| {
            let mut request = valid_close();
            change(&mut request);
            runtime
                .close_stream(request)
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        };
        let describe = async |change: fn(&mut DescribeStreamRequest)| {
            let mut request = valid_describe();
            change(&mut request);
            runtime
                .describe_stream(request)
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        };
        let invalid = |message: &str| AnyError::invalid_argument(message).to_string();
        let no_id = || invalid("The Stream id is empty");
        let cases = [
            (create(|r| r.stream_id.clear()).await, no_id()),
            (
                create(|r| r.id_conflict_policy = 99).await,
                invalid("The conflict policy 99 is unknown"),
            ),
            (append(|r| r.stream_id.clear()).await, no_id()),
            (
                append(|r| r.producer = None).await,
                invalid("The request has no Producer"),
            ),
            (
                append(|r| r.producer = Some(workflow(None))).await,
                invalid("The Producer has no Workflow Task attempt"),
            ),
            (
                append(|r| r.records.clear()).await,
                invalid("The batch has no Record"),
            ),
            (
                append(|r| r.records[1].body = None).await,
                invalid("Record 1 of the batch has no body"),
            ),
            (
                append(|r| r.records.push(record())).await,
                AppendError::operation(
                    AppendReason::TooLarge,
                    "The batch has 3 Records, more than 2",
                )
                .to_string(),
            ),
            (read(|r| r.stream_id.clear()).await, no_id()),
            (
                read(|r| r.from_offset = vec![0; 1025]).await,
                invalid("The from_offset of the read has 1025 bytes, more than 1024"),
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
            (close(|r| r.stream_id.clear()).await, no_id()),
            (
                close(|r| r.producer = None).await,
                invalid("The request has no Producer"),
            ),
            (describe(|r| r.stream_id.clear()).await, no_id()),
        ];
        for (result, error) in cases {
            assert_eq!(result, Err(error));
        }
    }

    #[tokio::test]
    async fn a_page_is_trimmed_to_the_caps_of_its_read_and_only_then_checked() {
        let (store, runtime) = runtime();
        let offsets = ["a", "b", "c", "d", "e"];
        let mut spared = 0;
        for seed in 0..400 {
            let mut rng = StdRng::seed_from_u64(seed);
            let count = rng.random_range(0..=offsets.len());
            let last_offset = match rng.random_bool(0.5) {
                true => "z",
                false => offsets[..count].last().copied().unwrap_or(""),
            };
            let mut answer = page(&offsets[..count], last_offset, rng.random_bool(0.5));
            for record in &mut answer.records {
                record.body.as_mut().unwrap().data = vec![0; rng.random_range(0..200)];
            }
            // In some seeds one Record has no body, which breaks the contract of a Store.
            let broken = (count > 0 && rng.random_bool(0.3)).then(|| rng.random_range(0..count));
            if let Some(at) = broken {
                answer.records[at].body = None;
            }
            // The size of each Record as a page encodes it, with the tag and the length of its
            // field.
            let size = |record: &StreamRecord| {
                let records = vec![record.clone()];
                ReadRecordsResponse {
                    records,
                    ..Default::default()
                }
                .encoded_len()
            };
            let sizes: Vec<usize> = answer.records.iter().map(size).collect();
            let max_records = rng.random_range(0..=count + 1);
            // The byte cap is the size of some first Records, or one byte off it. Zero is no cap.
            let first: usize = sizes[..rng.random_range(0..=count)].iter().sum();
            let max_bytes = (first + rng.random_range(0..3)).saturating_sub(1);
            let fits = |keep: usize| {
                (max_records == 0 || keep <= max_records)
                    && (max_bytes == 0 || sizes[..keep].iter().sum::<usize>() <= max_bytes)
            };
            // A page keeps its first Record, and then each Record that still fits both caps.
            let keep = (1..=count)
                .take_while(|keep| *keep == 1 || fits(*keep))
                .count();
            let mut expected = answer.clone();
            if keep < count {
                expected.records.truncate(keep);
                expected.last_offset = expected.records[keep - 1].offset.clone();
                expected.done = false;
            }
            store.answers.lock().page = answer;
            let read = ReadRecordsRequest {
                max_records: max_records as i32,
                max_bytes: max_bytes as i64,
                ..valid_read()
            };
            let result = runtime.read_records(read).await;
            // A Record that the trim removed can't fail the read.
            match broken.filter(|at| *at < keep) {
                Some(at) => {
                    let message = format!("The Store gave Record {at} with no Producer or no body");
                    let corrupt = StreamError::store_corrupt(message);
                    assert_eq!(result, Err(corrupt), "seed {seed}");
                }
                None => assert_eq!(result, Ok(expected), "seed {seed}"),
            }
            spared += usize::from(broken.is_some_and(|at| at >= keep));
        }
        assert!(spared > 0, "No trim removed a Record with no body");
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
        let incomplete = "The Store gave Record 1 with no Producer or no body";
        let broken = |change: fn(&mut StreamRecord)| {
            let mut answer = page(&["a", "b"], "b", false);
            change(&mut answer.records[1]);
            answer
        };
        let large = broken(|r| r.body.as_mut().unwrap().data = vec![0; DEFAULT_MAX_RECORD_BYTES]);
        let stored = StreamRecord {
            offset: vec![],
            ..large.records[1].clone()
        };
        let size = stored.encoded_len();
        let reads = [
            ("", page(&["a", "b"], "b", false), None),
            ("a", page(&["b"], "z", true), None),
            ("m", page(&[], "m", false), None),
            ("m", page(&[], "z", true), None),
            ("", page(&[&longest], &longest, false), None),
            (
                "",
                broken(|r| r.producer.as_mut().unwrap().kind = None),
                None,
            ),
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
            ("", broken(|r| r.body = None), Some(incomplete)),
        ];
        for (from_offset, answer, corrupt) in reads {
            store.answers.lock().page = answer.clone();
            let read = ReadRecordsRequest {
                from_offset: from_offset.as_bytes().to_vec(),
                ..valid_read()
            };
            expect(runtime.read_records(read).await, answer, corrupt);
        }
        // The Record size is the limit of the runtime, so a Record over the default is one that
        // this runtime can't take, and a larger limit takes it.
        store.answers.lock().page = large.clone();
        let too_large = format!(
            "The Store gave Record 1 with {size} bytes, and this runtime takes at most 1572864"
        );
        let refused = StreamError::new(AnyReason::Unsupported, too_large);
        assert_eq!(runtime.read_records(valid_read()).await, Err(refused));
        let larger = StoreLimits::default().with_max_record_bytes(size).unwrap();
        let larger = StreamRuntime::new(store.clone(), larger);
        assert_eq!(larger.read_records(valid_read()).await, Ok(large));
        let no_offset = "The Store answered an append with no offset";
        let reversed =
            "The Store answered an append with a first offset that is higher than its last offset";
        let appends = [
            ("a", "b", None),
            (longest.as_str(), longest.as_str(), None),
            ("", "b", Some(no_offset)),
            ("a", "", Some(no_offset)),
            (long.as_str(), "b", Some(long_offset)),
            ("a", long.as_str(), Some(long_offset)),
            ("b", "a", Some(reversed)),
        ];
        for (first_offset, last_offset, corrupt) in appends {
            let answer = AppendRecordsResponse {
                first_offset: first_offset.as_bytes().to_vec(),
                last_offset: last_offset.as_bytes().to_vec(),
            };
            store.answers.lock().appended = answer.clone();
            expect(
                runtime.append_records(valid_append()).await,
                answer,
                corrupt,
            );
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
