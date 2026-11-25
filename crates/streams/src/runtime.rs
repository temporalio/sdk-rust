use crate::{
    AnyReason, AppendError, CreateError, DescribeError, ReadError, StreamError, StreamStore,
    ids::{MAX_OFFSET_BYTES, validate_offset, validate_stream_id},
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CreateStreamRequest, DescribeStreamRequest,
        DescribeStreamResponse, ReadRecordsRequest, ReadRecordsResponse, StreamIdConflictPolicy,
        sdk,
    },
    record::{StoreLimits, field_len, producer_len, stored_len},
};
use prost::Message;
use std::{fmt, sync::Arc};

/// Wraps a [`StreamStore`] and checks each call. It takes the request of an SDK, with the ref of a
/// Stream, and calls the Store with only the Stream id of the ref. It validates the request before
/// the Store gets it, and the response before the caller gets it. The [`StreamStore`] docs describe
/// the contract that it checks. It has no timeout of its own, so a Store that never responds blocks
/// the call.
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

    /// Creates a Stream. If a Stream already has the id, the create fails, or it returns
    /// `created` false with the `UseExisting` policy. Every try of one create passes the same
    /// `request_id`, so a retry after a lost response returns `created` true. The response has the
    /// ref of the request, since the Store gets only its id. The runtime doesn't know the name of
    /// its Store, so a ref with no Store name has none in the response either.
    pub async fn create_stream(
        &self,
        request: sdk::CreateStreamRequest,
        request_id: String,
    ) -> Result<sdk::CreateStreamResponse, CreateError> {
        let sdk::CreateStreamRequest {
            stream_ref,
            id_conflict_policy: policy,
        } = request;
        let stream_ref = stream_ref.unwrap_or_default();
        validate_stream_id(&stream_ref.id)?;
        if StreamIdConflictPolicy::try_from(policy).is_err() {
            return Err(StreamError::invalid_argument(format!(
                "The conflict policy {policy} is unknown"
            )));
        }
        let request = CreateStreamRequest {
            stream_id: stream_ref.id.clone(),
            id_conflict_policy: policy,
        };
        let created = self.store.create_stream(request, request_id).await?.created;
        Ok(sdk::CreateStreamResponse {
            r#ref: Some(stream_ref),
            created,
        })
    }

    /// Appends one batch of at least one Record from one Producer. Every try of one append passes
    /// the same `request_id`, so the Store writes the batch once while it remembers the id. If this
    /// fails with [`AnyReason::StoreCorrupt`], the batch may still be in the Stream, because the
    /// runtime can only check the response after the write.
    pub async fn append_records(
        &self,
        request: sdk::AppendRecordsRequest,
        request_id: String,
    ) -> Result<AppendRecordsResponse, AppendError> {
        let sdk::AppendRecordsRequest {
            r#ref,
            producer,
            records,
        } = request;
        let request = AppendRecordsRequest {
            stream_id: stream_id(r#ref),
            producer,
            records,
        };
        self.limits.validate_append(&request)?;
        let appended = self.store.append_records(request, request_id).await?;
        for offset in [&appended.first_offset, &appended.last_offset] {
            if offset.is_empty() {
                return Err(StreamError::store_corrupt(
                    "The Store answered an append with no offset",
                ));
            }
            validate_offset(offset, "An offset from the Store", AnyReason::StoreCorrupt)?;
        }
        if appended.first_offset > appended.last_offset {
            return Err(StreamError::store_corrupt(
                "The Store answered an append with a first offset that is higher than its last \
                 offset",
            ));
        }
        Ok(appended)
    }

    /// Reads one page of Records after `from_offset`. A `max_records` or `max_bytes` of 0 leaves
    /// that size to the Store. The Store gets caps that take one Record more than the page holds,
    /// and the runtime trims the answer to the caps of the read. So [`ReadPage::more`] is true only
    /// when the Store returned a Record after the page. The first Record is always kept, so a read
    /// makes progress whenever the Store has something to return. `max_bytes` counts each Record
    /// as the page encodes it, with the tag and the length of its field. The runtime checks only
    /// the Records that it keeps.
    pub async fn read_records(
        &self,
        request: sdk::ReadRecordsRequest,
    ) -> Result<ReadPage, ReadError> {
        let sdk::ReadRecordsRequest {
            r#ref,
            from_offset,
            max_records,
            max_bytes,
            wait,
        } = request;
        let request = ReadRecordsRequest {
            stream_id: stream_id(r#ref),
            from_offset,
            max_records,
            max_bytes,
            wait,
        };
        validate_stream_id(&request.stream_id)?;
        validate_offset(
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
        let max_record_bytes = self.limits.max_record_bytes;
        let request = ReadRecordsRequest {
            max_records: one_more_record(request.max_records),
            max_bytes: one_more_record_bytes(request.max_bytes, max_record_bytes),
            ..request
        };
        let page = self.store.read_records(request).await?;
        fit_page(page, &from_offset, max_records, max_bytes, max_record_bytes)
    }

    /// Returns the offset of the newest Record of a Stream as `last_offset`.
    pub async fn describe_stream(
        &self,
        request: sdk::DescribeStreamRequest,
    ) -> Result<DescribeStreamResponse, DescribeError> {
        let sdk::DescribeStreamRequest { r#ref } = request;
        let request = DescribeStreamRequest {
            stream_id: stream_id(r#ref),
        };
        validate_stream_id(&request.stream_id)?;
        let described = self.store.describe_stream(request).await?;
        validate_offset(
            &described.last_offset,
            "The last offset from the Store",
            AnyReason::StoreCorrupt,
        )?;
        Ok(described)
    }
}

/// Returns the Stream id of a ref, which is all that a Store gets of it. A request with no ref has
/// an empty id, which the id check refuses.
fn stream_id(stream_ref: Option<sdk::StreamRef>) -> String {
    stream_ref
        .map(|stream_ref| stream_ref.id)
        .unwrap_or_default()
}

/// A page of Records that a read gives its caller.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadPage {
    /// The Store's answer, trimmed to the caps of the read and checked.
    pub response: ReadRecordsResponse,
    /// True if the Store returned a Record after the page, which the trim removed. So the Stream
    /// has more Records after `response.last_offset`. False says nothing about the Stream.
    pub more: bool,
}

/// Returns the `max_records` that the Store gets for a read with `max_records`.
fn one_more_record(max_records: i32) -> i32 {
    match max_records {
        0 => 0,
        max => max.saturating_add(1),
    }
}

/// Returns the `max_bytes` that the Store gets for a read with `max_bytes`. It adds the largest
/// Record that the runtime takes, as a page encodes it with its offset. A sum over `i64` is no cap.
fn one_more_record_bytes(max_bytes: i64, max_record_bytes: usize) -> i64 {
    if max_bytes == 0 {
        return 0;
    }
    let record = max_record_bytes
        .checked_add(field_len(MAX_OFFSET_BYTES))
        .map(field_len);
    let extra = record.and_then(|record| i64::try_from(record).ok());
    extra
        .and_then(|extra| max_bytes.checked_add(extra))
        .unwrap_or(0)
}

fn fit_page(
    mut page: ReadRecordsResponse,
    from_offset: &[u8],
    max_records: usize,
    max_bytes: usize,
    max_record_bytes: usize,
) -> Result<ReadPage, ReadError> {
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
    let more = keep < page.records.len();
    if more {
        page.records.truncate(keep);
        page.last_offset = page.records[keep - 1].offset.clone();
    }
    let mut newest = from_offset;
    for (at, record) in page.records.iter().enumerate() {
        if record.offset.is_empty() {
            return Err(StreamError::store_corrupt(
                "The Store gave a Record with no offset",
            ));
        }
        validate_offset(
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
    // The page shows where it ended, so a Store doesn't have to say it.
    if page.last_offset.is_empty() {
        page.last_offset = newest.to_vec();
    }
    validate_offset(
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
    Ok(ReadPage {
        response: page,
        more,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AnyError, AppendReason,
        proto::{AppendRecord, CreateStreamResponse, StreamRecord},
        record::{DEFAULT_MAX_RECORD_BYTES, producer_len, stored_len},
        test_store::{Received, Scripted, client, runtime, task, workflow},
    };
    use futures_util::future::BoxFuture;
    use parking_lot::Mutex;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::fmt::Debug;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    fn record() -> AppendRecord {
        AppendRecord {
            body: Some(Payload::default()),
        }
    }

    fn stream_ref(id: &str, store: &str) -> Option<sdk::StreamRef> {
        Some(sdk::StreamRef {
            id: id.to_string(),
            store: store.to_string(),
        })
    }

    fn valid_create() -> sdk::CreateStreamRequest {
        sdk::CreateStreamRequest {
            stream_ref: stream_ref("s", "x"),
            id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
        }
    }

    fn valid_append() -> sdk::AppendRecordsRequest {
        sdk::AppendRecordsRequest {
            r#ref: stream_ref("s", "x"),
            producer: Some(workflow(Some(task(3, 1, 0)))),
            records: vec![record(); 2],
        }
    }

    fn valid_read() -> sdk::ReadRecordsRequest {
        sdk::ReadRecordsRequest {
            r#ref: stream_ref("s", "x"),
            ..Default::default()
        }
    }

    fn valid_describe() -> sdk::DescribeStreamRequest {
        sdk::DescribeStreamRequest {
            r#ref: stream_ref("s", "x"),
        }
    }

    fn seconds(seconds: i64) -> Option<prost_types::Duration> {
        Some(prost_types::Duration { seconds, nanos: 0 })
    }

    fn page(offsets: &[&str], last_offset: &str) -> ReadRecordsResponse {
        let record = |offset: &&str| StreamRecord {
            producer: Some(client()),
            body: Some(Payload::default()),
            offset: offset.as_bytes().to_vec(),
        };
        ReadRecordsResponse {
            records: offsets.iter().map(record).collect(),
            last_offset: last_offset.as_bytes().to_vec(),
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
        let create = async |change: fn(&mut sdk::CreateStreamRequest)| {
            let mut request = valid_create();
            change(&mut request);
            runtime
                .create_stream(request, "r".to_string())
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        };
        let append = async |change: fn(&mut sdk::AppendRecordsRequest)| {
            let mut request = valid_append();
            change(&mut request);
            runtime
                .append_records(request, "r".to_string())
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        };
        let read = async |change: fn(&mut sdk::ReadRecordsRequest)| {
            let mut request = valid_read();
            change(&mut request);
            runtime
                .read_records(request)
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        };
        let describe = async |change: fn(&mut sdk::DescribeStreamRequest)| {
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
            (create(|r| r.stream_ref = None).await, no_id()),
            (
                create(|r| r.id_conflict_policy = 99).await,
                invalid("The conflict policy 99 is unknown"),
            ),
            (append(|r| r.r#ref = None).await, no_id()),
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
            (read(|r| r.r#ref = None).await, no_id()),
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
            (describe(|r| r.r#ref = None).await, no_id()),
        ];
        for (result, error) in cases {
            assert_eq!(result, Err(error));
        }
    }

    #[tokio::test]
    async fn a_create_answers_with_the_ref_of_its_request() {
        // The Store gets only the id, so the `store` of the ref comes from the request.
        let (store, runtime) = runtime();
        for created in [true, false] {
            store.answers.lock().created = CreateStreamResponse { created };
            let answer = sdk::CreateStreamResponse {
                r#ref: stream_ref("s", "x"),
                created,
            };
            let response = runtime.create_stream(valid_create(), "r".to_string());
            assert_eq!(response.await, Ok(answer));
        }
    }

    #[tokio::test]
    async fn the_store_gets_each_request_with_only_the_stream_id_of_its_ref() {
        let (store, runtime) = runtime();
        store.answers.lock().appended = AppendRecordsResponse {
            first_offset: b"a".to_vec(),
            last_offset: b"a".to_vec(),
        };
        let (create, append) = (valid_create(), valid_append());
        let read = sdk::ReadRecordsRequest {
            from_offset: b"a".to_vec(),
            max_records: 3,
            max_bytes: 7,
            wait: seconds(2),
            ..valid_read()
        };
        let created = runtime.create_stream(create.clone(), "c".to_string());
        created.await.unwrap();
        let appended = runtime.append_records(append.clone(), "a".to_string());
        appended.await.unwrap();
        runtime.read_records(read.clone()).await.unwrap();
        runtime.describe_stream(valid_describe()).await.unwrap();
        let id = || "s".to_string();
        let expected = [
            Received::Create(
                CreateStreamRequest {
                    stream_id: id(),
                    id_conflict_policy: create.id_conflict_policy,
                },
                "c".to_string(),
            ),
            Received::Append(
                AppendRecordsRequest {
                    stream_id: id(),
                    producer: append.producer,
                    records: append.records,
                },
                "a".to_string(),
            ),
            // The caps take one Record more, which has at most 1572864 bytes and its offset.
            Received::Read(ReadRecordsRequest {
                stream_id: id(),
                from_offset: read.from_offset,
                max_records: 4,
                max_bytes: 7 + 1573895,
                wait: read.wait,
            }),
            Received::Describe(DescribeStreamRequest { stream_id: id() }),
        ];
        assert_eq!(*store.received.lock(), expected);
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
            let mut answer = page(&offsets[..count], last_offset);
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
            }
            let expected = ReadPage {
                response: expected,
                more: keep < count,
            };
            store.answers.lock().page = answer;
            let read = sdk::ReadRecordsRequest {
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

    /// A Store that keeps the contract. It answers with the first Records after `from_offset`
    /// that fit the caps of the request.
    #[derive(Default)]
    struct Stream(Mutex<Vec<StreamRecord>>);

    impl StreamStore for Stream {
        fn create_stream(
            &self,
            _: CreateStreamRequest,
            _: String,
        ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
            unreachable!("The test only reads")
        }

        fn append_records(
            &self,
            _: AppendRecordsRequest,
            _: String,
        ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
            unreachable!("The test only reads")
        }

        fn read_records(
            &self,
            request: ReadRecordsRequest,
        ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
            let max_records = match request.max_records {
                0 => usize::MAX,
                max => max as usize,
            };
            let max_bytes = match request.max_bytes {
                0 => usize::MAX,
                max => max as usize,
            };
            let mut bytes = 0;
            let records: Vec<StreamRecord> = (self.0.lock().iter())
                .filter(|record| record.offset > request.from_offset)
                .take(max_records)
                .take_while(|record| {
                    let first = bytes == 0;
                    bytes += field_len(record.encoded_len());
                    first || bytes <= max_bytes
                })
                .cloned()
                .collect();
            let page = ReadRecordsResponse {
                records,
                ..Default::default()
            };
            Box::pin(async move { Ok(page) })
        }

        fn describe_stream(
            &self,
            _: DescribeStreamRequest,
        ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
            unreachable!("The test only reads")
        }
    }

    #[tokio::test]
    async fn more_says_that_the_stream_has_a_record_after_the_page() {
        const MAX_RECORD: usize = 400;
        let store = Arc::new(Stream::default());
        let limits = StoreLimits::default().with_max_record_bytes(MAX_RECORD);
        let runtime = StreamRuntime::new(store.clone(), limits.unwrap());
        let (mut told, mut over_size_spared) = (0, 0);
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            // Some Records are over the Record size of the runtime.
            let records: Vec<StreamRecord> = (0..rng.random_range(0..8_u8))
                .map(|at| StreamRecord {
                    producer: Some(client()),
                    body: Some(Payload {
                        data: vec![0; rng.random_range(0..MAX_RECORD + 100)],
                        ..Default::default()
                    }),
                    offset: vec![b'a' + at],
                })
                .collect();
            let size = |record: &StreamRecord| field_len(record.encoded_len());
            let over_size = |record: &StreamRecord| {
                let stored = stored_len(producer_len(&client()), record.body.as_ref());
                stored > MAX_RECORD
            };
            *store.0.lock() = records.clone();
            let max_records = rng.random_range(0..4);
            let max_bytes = match rng.random_bool(0.3) {
                true => 0,
                false => rng.random_range(1..1500),
            };
            let read = sdk::ReadRecordsRequest {
                max_records: max_records as i32,
                max_bytes: max_bytes as i64,
                ..valid_read()
            };
            let result = runtime.read_records(read).await;
            // The page has the first Records that fit the caps of the read, and at least one.
            let (mut keep, mut bytes) = (0, 0);
            for record in &records {
                bytes += size(record);
                let fits = (max_records == 0 || keep < max_records)
                    && (max_bytes == 0 || bytes <= max_bytes);
                if keep > 0 && !fits {
                    break;
                }
                keep += 1;
            }
            let page = &records[..keep];
            if let Some(at) = page.iter().position(over_size) {
                let Err(error) = result else {
                    panic!("seed {seed}: a Record over the size of the runtime passed");
                };
                assert_eq!(error.reason, AnyReason::Unsupported.into(), "seed {seed}");
                assert!(
                    error
                        .message
                        .starts_with(&format!("The Store gave Record {at} "))
                );
                continue;
            }
            let ReadPage { response, more } = result.unwrap();
            assert_eq!(response.records, page, "seed {seed}");
            let next = records.get(keep);
            over_size_spared += usize::from(more && next.is_some_and(over_size));
            // `more` is never a guess.
            assert!(!more || next.is_some(), "seed {seed}");
            // With a page within its byte cap, the extra cap has room for any Record that the
            // runtime takes, so the Store returns it.
            let page_bytes: usize = page.iter().map(size).sum();
            let room = max_bytes == 0 || page_bytes <= max_bytes;
            if next.is_some_and(|next| !over_size(next)) && room {
                assert!(more, "seed {seed}");
            }
            told += usize::from(more);
        }
        assert!(told > 100, "Only {told} pages had more");
        assert!(
            over_size_spared > 0,
            "No trim removed a Record over the size"
        );
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
            let mut answer = page(&["a", "b"], "b");
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
            ("", page(&["a", "b"], "b"), None),
            ("a", page(&["b"], "z"), None),
            ("m", page(&[], "m"), None),
            ("m", page(&[], "z"), None),
            ("", page(&[&longest], &longest), None),
            (
                "",
                broken(|r| r.producer.as_mut().unwrap().kind = None),
                None,
            ),
            (
                "",
                page(&[""], ""),
                Some("The Store gave a Record with no offset"),
            ),
            ("", page(&[&long], &long), Some(long_offset)),
            ("", page(&["a"], &long), Some(long_last)),
            ("a", page(&["a"], "a"), Some(first)),
            ("", page(&["b", "a"], "b"), Some(second)),
            ("", page(&["a", "a"], "a"), Some(second)),
            ("", page(&["a", "b"], "a"), Some(below_record)),
            ("m", page(&[], "a"), Some(below_from)),
            ("", broken(|r| r.producer = None), Some(incomplete)),
            ("", broken(|r| r.body = None), Some(incomplete)),
        ];
        for (from_offset, answer, corrupt) in reads {
            store.answers.lock().page = answer.clone();
            let read = sdk::ReadRecordsRequest {
                from_offset: from_offset.as_bytes().to_vec(),
                ..valid_read()
            };
            let read = runtime.read_records(read).await;
            expect(read.map(|read| read.response), answer, corrupt);
        }
        // A Store can leave last_offset empty. The page then ends at its last Record, or where its
        // read started.
        for (offsets, ends_at) in [(&[][..], "m"), (&["n", "o"][..], "o")] {
            store.answers.lock().page = page(offsets, "");
            let read = sdk::ReadRecordsRequest {
                from_offset: b"m".to_vec(),
                ..valid_read()
            };
            let filled = page(offsets, ends_at);
            let read = runtime.read_records(read).await;
            assert_eq!(read.map(|read| read.response), Ok(filled));
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
        let read = larger.read_records(valid_read()).await;
        assert_eq!(read.map(|read| read.response), Ok(large));
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
                runtime
                    .append_records(valid_append(), "r".to_string())
                    .await,
                answer,
                corrupt,
            );
        }
        let describes = [
            ("", None),
            (longest.as_str(), None),
            (long.as_str(), Some(long_last)),
        ];
        for (last_offset, corrupt) in describes {
            let answer = DescribeStreamResponse {
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
