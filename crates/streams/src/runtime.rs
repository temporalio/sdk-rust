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

/// Adapts SDK requests to [`StreamStore`]. The Store receives the Stream id from the ref.
/// The runtime validates requests and individual responses against the Store contract and
/// the caller's [`StoreLimits`]. Set a deadline at the call site if needed.
///
/// An SDK uses the limits from its Stream config. A Store can wrap itself in a runtime with
/// [`StoreLimits::UNBOUNDED`] to validate requests independently of the caller's config.
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

    /// Create a Stream under the Store's [conflict and retry rules](StreamStore::create_stream).
    /// The response contains the caller's original ref, including its Store name.
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
        if !created && policy != StreamIdConflictPolicy::UseExisting as i32 {
            return Err(StreamError::store_corrupt(
                "The Store returned an existing Stream without the UseExisting policy",
            ));
        }
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
    /// that size to the Store. If the Store returns more than the read asked for, the runtime trims
    /// the page to the caps of the read. The first Record is always kept, so a read makes progress
    /// whenever the Store has something to return. `max_bytes` counts each Record as the page
    /// encodes it, with the tag and the length of its field. The runtime checks only the Records
    /// that it keeps.
    pub async fn read_records(
        &self,
        request: sdk::ReadRecordsRequest,
    ) -> Result<ReadRecordsResponse, ReadError> {
        let page = self.read(request, false).await?;
        Ok(page.response)
    }

    /// Read a page and check for a Record beyond its limits. The runtime asks the Store for
    /// one extra Record. [`ReadPage::more`] is true only if the runtime trimmed a Record.
    /// Use [`Self::read_records`] when the caller does not need this information.
    pub async fn read_records_with_more(
        &self,
        request: sdk::ReadRecordsRequest,
    ) -> Result<ReadPage, ReadError> {
        self.read(request, true).await
    }

    async fn read(
        &self,
        request: sdk::ReadRecordsRequest,
        one_more: bool,
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
        if let Some(wait) = request.wait {
            if wait.seconds < 0 || wait.nanos < 0 {
                return Err(StreamError::invalid_argument("wait must be 0 or more"));
            }
            if wait.seconds > 315_576_000_000 || wait.nanos >= 1_000_000_000 {
                return Err(StreamError::invalid_argument(
                    "wait exceeds the protobuf Duration range",
                ));
            }
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
        let request = match one_more {
            true => ReadRecordsRequest {
                max_records: match request.max_records {
                    0 => 0,
                    max => max.saturating_add(1),
                },
                max_bytes: one_more_record_bytes(request.max_bytes, max_record_bytes),
                ..request
            },
            false => request,
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

// The extra budget lets a full page include one more Record.
// Saturation avoids zero, which would remove the Store's limit.
fn one_more_record_bytes(max_bytes: i64, max_record_bytes: usize) -> i64 {
    if max_bytes == 0 {
        return 0;
    }
    let record = max_record_bytes.saturating_add(field_len(MAX_OFFSET_BYTES));
    let field = record.saturating_add(1 + prost::length_delimiter_len(record));
    max_bytes.saturating_add(i64::try_from(field).unwrap_or(i64::MAX))
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
        record::DEFAULT_MAX_RECORD_BYTES,
        test_store::{Received, Scripted, client, runtime, task, workflow},
    };
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
            (
                read(|r| {
                    r.wait = Some(prost_types::Duration {
                        seconds: 0,
                        nanos: 1_000_000_000,
                    })
                })
                .await,
                invalid("wait exceeds the protobuf Duration range"),
            ),
            (
                read(|r| r.wait = seconds(315_576_000_001)).await,
                invalid("wait exceeds the protobuf Duration range"),
            ),
            (describe(|r| r.r#ref = None).await, no_id()),
        ];
        for (result, error) in cases {
            assert_eq!(result, Err(error));
        }
        assert!(store.received.lock().is_empty());

        let longest_wait = sdk::ReadRecordsRequest {
            wait: Some(prost_types::Duration {
                seconds: 315_576_000_000,
                nanos: 999_999_999,
            }),
            ..valid_read()
        };
        runtime.read_records(longest_wait).await.unwrap();
    }

    #[tokio::test]
    async fn only_use_existing_can_return_an_existing_stream() {
        let (store, runtime) = runtime();
        for (policy, created, valid) in [
            (StreamIdConflictPolicy::Unspecified, true, true),
            (StreamIdConflictPolicy::Unspecified, false, false),
            (StreamIdConflictPolicy::Fail, true, true),
            (StreamIdConflictPolicy::Fail, false, false),
            (StreamIdConflictPolicy::UseExisting, true, true),
            (StreamIdConflictPolicy::UseExisting, false, true),
        ] {
            store.answers.lock().created = CreateStreamResponse { created };
            let request = sdk::CreateStreamRequest {
                id_conflict_policy: policy.into(),
                ..valid_create()
            };
            let result = runtime.create_stream(request, "r".into()).await;
            if valid {
                assert_eq!(
                    result.unwrap(),
                    sdk::CreateStreamResponse {
                        r#ref: stream_ref("s", "x"),
                        created,
                    }
                );
            } else {
                assert_eq!(result.unwrap_err().reason, AnyReason::StoreCorrupt.into());
            }
        }
    }

    #[tokio::test]
    async fn a_lookahead_adds_one_record_without_overflowing_the_caps() {
        let store = Arc::new(Scripted::default());
        let runtime = StreamRuntime::new(store.clone(), StoreLimits::UNBOUNDED);
        let bounded = StreamRuntime::new(store.clone(), StoreLimits::default());
        for (runtime, max_records, max_bytes, store_records, store_bytes) in [
            // The lookahead must fit the largest Record, including its offset.
            (&bounded, 3, 7, 4, 7 + 1_573_895),
            (&runtime, 0, 0, 0, 0),
            (&runtime, 0, 1, 0, i64::MAX),
            (&runtime, 1, 0, 2, 0),
            (&runtime, i32::MAX - 1, i64::MAX - 1, i32::MAX, i64::MAX),
            (&runtime, i32::MAX, i64::MAX, i32::MAX, i64::MAX),
        ] {
            let read = sdk::ReadRecordsRequest {
                max_records,
                max_bytes,
                ..valid_read()
            };
            runtime.read_records_with_more(read).await.unwrap();
            let Some(Received::Read(sent)) = store.received.lock().pop() else {
                panic!("expected a Store read");
            };
            assert_eq!(
                (sent.max_records, sent.max_bytes),
                (store_records, store_bytes)
            );
        }
    }

    #[tokio::test]
    async fn a_page_keeps_its_first_record_and_ends_at_the_first_cap_it_reaches() {
        let (store, runtime) = runtime();
        let mut answer = page(&["a", "b", "c"], "z");
        answer.records[1].body.as_mut().unwrap().data = vec![0; 128];
        let first_two = ReadRecordsResponse {
            records: answer.records[..2].to_vec(),
            last_offset: vec![],
        };
        let bytes = first_two.encoded_len() as i64;
        store.answers.lock().page = answer.clone();
        for (max_records, max_bytes, count, last_offset, more) in [
            (0, 0, 3, "z", false),
            (3, 0, 3, "z", false),
            (0, 1, 1, "a", true),
            (3, bytes - 1, 1, "a", true),
            (3, bytes, 2, "b", true),
            (3, bytes + 1, 2, "b", true),
            (1, bytes, 1, "a", true),
        ] {
            let read = sdk::ReadRecordsRequest {
                max_records,
                max_bytes,
                ..valid_read()
            };
            let response = ReadRecordsResponse {
                records: answer.records[..count].to_vec(),
                last_offset: last_offset.as_bytes().to_vec(),
            };
            assert_eq!(
                runtime.read_records(read.clone()).await,
                Ok(response.clone())
            );
            assert_eq!(
                runtime.read_records_with_more(read).await,
                Ok(ReadPage { response, more })
            );
        }

        store.answers.lock().page = page(&[], "z");
        let empty = runtime.read_records_with_more(valid_read()).await.unwrap();
        assert_eq!(
            empty,
            ReadPage {
                response: page(&[], "z"),
                more: false
            }
        );
    }

    #[tokio::test]
    async fn a_discarded_record_cannot_fail_a_read() {
        let (store, runtime) = runtime();
        let read = sdk::ReadRecordsRequest {
            max_records: 1,
            ..valid_read()
        };
        for body in [
            None,
            Some(Payload {
                data: vec![0; DEFAULT_MAX_RECORD_BYTES],
                ..Default::default()
            }),
        ] {
            let mut answer = page(&["a", "b"], "z");
            answer.records[1].body = body;
            store.answers.lock().page = answer;
            let response = page(&["a"], "a");
            assert_eq!(
                runtime.read_records(read.clone()).await,
                Ok(response.clone())
            );
            assert_eq!(
                runtime.read_records_with_more(read.clone()).await,
                Ok(ReadPage {
                    response,
                    more: true
                })
            );
            assert!(runtime.read_records_with_more(valid_read()).await.is_err());
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
            expect(runtime.read_records(read).await, answer, corrupt);
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
            assert_eq!(runtime.read_records(read).await, Ok(filled));
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
