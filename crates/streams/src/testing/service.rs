//! The Stream service operations through the runtime's typed calls.
//!
//! Every Store runs the suite through its own runtime.

use super::{RuntimeConfig, case, support::Needs};

use crate::{
    StreamError, StreamRuntime,
    ids::MAX_PRODUCER_ID_BYTES,
    proto::{
        ActivityEpoch, ActivityProducer, AppendRecord, AppendRecordsRequest, AppendRecordsResponse,
        ClientProducer, CloseStreamRequest, CloseStreamResponse, CreateStreamRequest,
        CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse, LocalActivityEpoch,
        LocalActivityProducer, Producer, ReadRecordsRequest, ReadRecordsResponse,
        StreamErrorDetails, StreamIdConflictPolicy, StreamRef, StreamStatus, WorkflowProducer,
        WorkflowTaskEpoch, producer::Kind,
    },
};
use prost::Message;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use tonic::Code;

struct Case {
    ext: Arc<StreamRuntime>,
    namespace: String,
    stream: String,
}

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn unique() -> String {
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{now}-{n}")
}

/// The Retention of the cases that let a closed Stream expire.
const SHORT_RETENTION: Duration = Duration::from_millis(300);

/// A fresh Stream on each Store that gives `needs`.
async fn cases_for(needs: Needs) -> Vec<Case> {
    cases_with(needs, &RuntimeConfig::default()).await
}

/// A fresh Stream on each Store that gives short times, on a runtime whose Retention is
/// [SHORT_RETENTION].
async fn short_cases() -> Vec<Case> {
    let config = RuntimeConfig {
        retention: Some(SHORT_RETENTION),
        ..Default::default()
    };
    cases_with(Needs::SHORT_TIMES, &config).await
}

/// A fresh Stream on each Store that gives `needs`, on a runtime with `config`.
async fn cases_with(needs: Needs, config: &RuntimeConfig) -> Vec<Case> {
    super::support::runtimes(needs, config)
        .await
        .into_iter()
        .map(|runtime| {
            let unique = unique();
            Case {
                ext: runtime.ext,
                namespace: runtime.namespace.unwrap_or(format!("ns-{unique}")),
                stream: format!("s-{unique}"),
            }
        })
        .collect()
}

fn reason(error: &StreamError) -> String {
    let details = StreamErrorDetails::decode(error.details().as_slice()).unwrap();
    assert_eq!(details.reason(), error.reason.proto());
    error.reason.token().to_string()
}

/// The internal offset of a wire value, for the suite's assertions.
fn off(wire: &[u8]) -> i64 {
    crate::offset::offset_from_wire(wire).unwrap()
}

/// The wire value of an internal offset.
fn wire(offset: i64) -> Vec<u8> {
    crate::offset::offset_to_wire(offset)
}

fn client() -> Producer {
    Producer {
        id: "client:me:p".to_string(),
        kind: Some(Kind::Client(ClientProducer {})),
    }
}

/// The Producer of run `run` in its first Workflow Task, as its close Activity sends it.
fn workflow(run: &str) -> Producer {
    Producer {
        id: format!("wf:{run}"),
        kind: Some(Kind::Workflow(WorkflowProducer {
            run_id: run.to_string(),
            epoch: Some(WorkflowTaskEpoch {
                started_event_id: 3,
                attempt: 1,
                started_time: None,
            }),
        })),
    }
}

/// The fields of a create that uses an existing Stream.
fn use_existing() -> CreateStreamRequest {
    CreateStreamRequest {
        id_conflict_policy: StreamIdConflictPolicy::UseExisting as i32,
        ..Default::default()
    }
}

fn record(text: &str) -> AppendRecord {
    AppendRecord {
        body: Some(Payload {
            data: text.as_bytes().to_vec(),
            ..Default::default()
        }),
    }
}

fn millis(ms: u64) -> Option<prost_types::Duration> {
    let duration = Duration::from_millis(ms);
    Some(prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    })
}

impl Case {
    fn stream_ref(&self) -> Option<StreamRef> {
        Some(StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        })
    }

    async fn create(&self) -> Result<CreateStreamResponse, StreamError> {
        self.create_with(CreateStreamRequest::default()).await
    }

    /// Creates the Stream with the fields of `request`, in the case's namespace and ref.
    async fn create_with(
        &self,
        request: CreateStreamRequest,
    ) -> Result<CreateStreamResponse, StreamError> {
        let request = CreateStreamRequest {
            namespace: self.namespace.clone(),
            stream_ref: self.stream_ref(),
            ..request
        };
        self.ext.create_stream(request).await
    }

    async fn append(
        &self,
        producer: Producer,
        sequence: i64,
        records: Vec<AppendRecord>,
    ) -> Result<AppendRecordsResponse, StreamError> {
        self.ext
            .append_records(AppendRecordsRequest {
                namespace: self.namespace.clone(),
                r#ref: self.stream_ref(),
                producer: Some(producer),
                sequence,
                records,
            })
            .await
    }

    async fn read(&self, request: ReadRecordsRequest) -> Result<ReadRecordsResponse, StreamError> {
        self.ext
            .read_records(ReadRecordsRequest {
                namespace: self.namespace.clone(),
                r#ref: self.stream_ref(),
                ..request
            })
            .await
    }

    async fn describe(&self) -> Result<DescribeStreamResponse, StreamError> {
        self.ext
            .describe_stream(DescribeStreamRequest {
                namespace: self.namespace.clone(),
                r#ref: self.stream_ref(),
            })
            .await
    }

    async fn close(&self, producer: Producer) -> Result<CloseStreamResponse, StreamError> {
        self.ext
            .close_stream(CloseStreamRequest {
                namespace: self.namespace.clone(),
                r#ref: self.stream_ref(),
                producer: Some(producer),
            })
            .await
    }
}

fn texts(response: &ReadRecordsResponse) -> Vec<String> {
    response
        .records
        .iter()
        .map(|record| String::from_utf8(record.body.clone().unwrap().data).unwrap())
        .collect()
}

/// A create of an open Stream fails as `already_exists`, unless it uses the Stream or it is a
/// retry of the create that made the Stream. Such a retry has the same request id, and it gets
/// `created` true with either policy. A create needs a ref with an id.
async fn a_create_of_an_open_stream_fails_unless_it_uses_it_or_retries() {
    for case in cases_for(Needs::CALLS).await {
        let first = CreateStreamRequest {
            request_id: "create-1".to_string(),
            ..Default::default()
        };
        assert!(case.create_with(first.clone()).await.unwrap().created);
        let other = CreateStreamRequest {
            request_id: "create-2".to_string(),
            ..Default::default()
        };
        for request in [other.clone(), CreateStreamRequest::default()] {
            let error = case.create_with(request).await.unwrap_err();
            assert_eq!(
                (error.code(), reason(&error).as_str()),
                (Code::AlreadyExists, "stream_already_exists")
            );
        }
        let used = CreateStreamRequest {
            request_id: other.request_id,
            ..use_existing()
        };
        assert!(!case.create_with(used).await.unwrap().created);
        let retry = CreateStreamRequest {
            request_id: first.request_id.clone(),
            ..use_existing()
        };
        for request in [first, retry] {
            assert!(case.create_with(request).await.unwrap().created);
        }
        for stream_ref in [None, Some(StreamRef::default())] {
            let request = CreateStreamRequest {
                namespace: case.namespace.clone(),
                stream_ref,
                ..Default::default()
            };
            let error = case.ext.create_stream(request).await.unwrap_err();
            assert_eq!(reason(&error), "stream_invalid");
        }
    }
}

/// A create of a closed Stream fails as closed, before its conflict policy and before a retry of
/// the create that made the Stream.
async fn a_create_of_a_closed_stream_fails() {
    for case in cases_for(Needs::CALLS).await {
        let first = CreateStreamRequest {
            request_id: "create-1".to_string(),
            ..Default::default()
        };
        case.create_with(first.clone()).await.unwrap();
        case.close(client()).await.unwrap();
        for request in [first, CreateStreamRequest::default()] {
            let error = case.create_with(request).await.unwrap_err();
            assert_eq!(
                (error.code(), reason(&error).as_str()),
                (Code::FailedPrecondition, "stream_closed")
            );
        }
    }
}

/// A close writes no Record, so the describe's last offset stays at the newest Record. A second
/// close of the closed Stream succeeds, and a later append fails as closed.
async fn a_close_writes_no_record_and_a_second_close_succeeds() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let written = case.append(client(), 1, vec![record("a")]).await.unwrap();
        case.close(client()).await.unwrap();
        case.close(client()).await.unwrap();
        let error = case
            .append(client(), 2, vec![record("b")])
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_closed");
        let describe = DescribeStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: case.stream_ref(),
        };
        let described = case.ext.describe_stream(describe).await.unwrap();
        assert_eq!(described.status, StreamStatus::Closed as i32);
        assert_eq!(described.last_offset, written.last_offset);
    }
}

/// A create of a closed Stream whose Retention passed fails as expired while its Tombstone is
/// there, before the closed check and before `already_exists`.
async fn a_create_of_an_expired_stream_fails() {
    for case in short_cases().await {
        case.create().await.unwrap();
        case.close(client()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let error = case.create().await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::NotFound, "stream_expired")
        );
    }
}

/// A Producer has a kind, a Workflow appends only through a Local Activity, and a Local Activity's
/// epoch names the Workflow Task that dispatched it.
async fn a_producer_has_a_kind_and_is_not_a_workflow() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let no_kind = Producer {
            kind: None,
            ..client()
        };
        let error = case
            .append(no_kind, 1, vec![record("x")])
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::InvalidArgument, "stream_invalid")
        );
        let error = case.append(workflow("r1"), 1, vec![]).await.unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        let no_task = Producer {
            id: "la:r1:call-1".to_string(),
            kind: Some(Kind::LocalActivity(LocalActivityProducer {
                run_id: "r1".to_string(),
                epoch: Some(LocalActivityEpoch {
                    task: None,
                    attempt: 1,
                }),
            })),
        };
        let error = case
            .append(no_task, 1, vec![record("x")])
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}

/// A Producer id is opaque bytes. The runtime refuses an empty one and one over the limit, and
/// takes any other, also with separators in it.
async fn a_producer_id_is_opaque_but_not_empty_or_too_long() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        for id in [String::new(), "x".repeat(MAX_PRODUCER_ID_BYTES + 1)] {
            let wrong = Producer { id, ..client() };
            let error = case.append(wrong, 1, vec![record("x")]).await.unwrap_err();
            assert_eq!(
                (error.code(), reason(&error).as_str()),
                (Code::InvalidArgument, "stream_invalid")
            );
        }
        let odd = Producer {
            id: "ingest|eu:1 é".to_string(),
            ..client()
        };
        case.append(odd.clone(), 1, vec![record("a")])
            .await
            .unwrap();
        case.close(odd).await.unwrap();
    }
}

/// A close takes a Producer of any kind, an Activity's too. Only an append refuses a Workflow
/// Producer.
async fn a_close_takes_a_producer_of_any_kind() {
    for case in cases_for(Needs::UNCHECKED).await {
        case.create().await.unwrap();
        let activity = Producer {
            id: "act:r1:a:s".to_string(),
            kind: Some(Kind::WorkflowActivity(ActivityProducer {
                run_id: "r1".to_string(),
                epoch: Some(ActivityEpoch {
                    attempt_scheduled_time: Some(prost_types::Timestamp::default()),
                    attempt: 1,
                }),
            })),
        };
        case.close(activity).await.unwrap();
        assert_eq!(
            case.describe().await.unwrap().status,
            StreamStatus::Closed as i32
        );
    }
}

/// An append and a close of a Producer that lacks a required field fail as invalid before the
/// Store, so the Stream gets no Record and stays open.
async fn a_write_with_a_missing_required_field_is_invalid() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let no_epoch = Producer {
            id: "la:r1:call-1".to_string(),
            kind: Some(Kind::LocalActivity(LocalActivityProducer {
                run_id: "r1".to_string(),
                epoch: None,
            })),
        };
        let error = case
            .append(no_epoch.clone(), 1, vec![record("x")])
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::InvalidArgument, "stream_invalid")
        );
        let error = case.close(no_epoch).await.unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
        // A Workflow Producer may close with no epoch, but one that it sends must order.
        let mut no_task = workflow("r1");
        let Some(Kind::Workflow(kind)) = no_task.kind.as_mut() else {
            unreachable!()
        };
        kind.epoch = Some(WorkflowTaskEpoch::default());
        let error = case.close(no_task).await.unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
        let describe = case.describe().await.unwrap();
        assert_eq!(describe.status, StreamStatus::Open as i32);
        assert!(describe.last_offset.is_empty());
    }
}

/// An append with a Record that has no body fails as invalid before the Store, so no Record of the
/// batch is written.
async fn an_append_with_a_record_that_has_no_body_is_invalid() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let records = vec![record("a"), AppendRecord::default()];
        let error = case.append(client(), 1, records).await.unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::InvalidArgument, "stream_invalid")
        );
        assert!(case.describe().await.unwrap().last_offset.is_empty());
    }
}

/// An append with no sequence takes the next one of its Producer's epoch, as a call with no writer
/// state sends it. Nothing dedupes it, so its retry writes the batch again. A negative sequence
/// fails.
async fn an_append_with_no_sequence_takes_the_next_one_of_its_epoch() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let first = case.append(client(), 0, vec![record("a")]).await.unwrap();
        let retry = case.append(client(), 0, vec![record("a")]).await.unwrap();
        assert!(
            retry.first_offset > first.last_offset,
            "{first:?} {retry:?}"
        );
        let resend = case.append(client(), 2, vec![record("a")]).await.unwrap();
        assert_eq!(resend, retry);
        let error = case
            .append(client(), -1, vec![record("b")])
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
    }
}

/// A read with no offset starts at the beginning, and a read is exclusive from its offset. A read
/// after the newest offset gives no Record, so a describe and a read give only new Records. An
/// offset past the newest fails with `stream_offset_too_high`, and one that decodes below zero or
/// has more than 8 bytes is invalid.
async fn a_read_is_exclusive_from_its_offset_and_get_offset_reads_only_new_records() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let first = case
            .append(client(), 1, vec![record("1"), record("2")])
            .await
            .unwrap();
        let all = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&all), ["1", "2"]);
        assert_eq!(all.records[0].offset, first.first_offset);
        assert_eq!(all.last_offset, first.last_offset);
        let from = case
            .read(ReadRecordsRequest {
                from_offset: first.first_offset.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&from), ["2"]);
        let newest = case.describe().await.unwrap().last_offset;
        let none = case
            .read(ReadRecordsRequest {
                from_offset: newest.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(none.records.is_empty());
        assert_eq!(none.last_offset, newest);
        let error = case
            .read(ReadRecordsRequest {
                from_offset: wire(off(&newest) + 1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::OutOfRange, "stream_offset_too_high")
        );
        let error = case
            .read(ReadRecordsRequest {
                // An 8-byte value with the top bit set decodes to a negative offset.
                from_offset: vec![0x80, 0, 0, 0, 0, 0, 0, 0],
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
        let error = case
            .read(ReadRecordsRequest {
                from_offset: vec![0; 9],
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_invalid");
        // The largest offset has no next one, and it is past the newest Record too.
        let error = case
            .read(ReadRecordsRequest {
                from_offset: wire(i64::MAX),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_offset_too_high");
    }
}

/// A read of a Stream that nobody created fails at once with `stream_not_found`, also with a long
/// wait.
async fn a_read_of_an_uncreated_stream_fails_at_once() {
    for case in cases_for(Needs::CALLS).await {
        let started = Instant::now();
        let error = case
            .read(ReadRecordsRequest {
                wait: millis(5000),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::NotFound, "stream_not_found")
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}

/// A quiet read ends empty after its wait, and a long poll wakes on an append.
async fn a_quiet_read_ends_empty_after_its_wait_and_a_long_poll_wakes_on_append() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let first = case.append(client(), 1, vec![record("1")]).await.unwrap();
        let from = first.last_offset;
        let started = Instant::now();
        let quiet = case
            .read(ReadRecordsRequest {
                from_offset: from.clone(),
                wait: millis(200),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(quiet.records.is_empty());
        assert_eq!(quiet.last_offset, from);
        assert!(!quiet.done);
        let ext = case.ext.clone();
        let (namespace, stream) = (case.namespace.clone(), case.stream.clone());
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let request = AppendRecordsRequest {
                namespace,
                r#ref: Some(StreamRef {
                    id: stream,
                    ..Default::default()
                }),
                producer: Some(client()),
                sequence: 2,
                records: vec![record("2")],
            };
            ext.append_records(request).await
        });
        let woke = case
            .read(ReadRecordsRequest {
                from_offset: from,
                wait: millis(5000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&woke), ["2"]);
        writer.await.unwrap().unwrap();
    }
}

/// A read that gives the last Record of a closed Stream is done, and so is every later read, which
/// gives no Record. A close refuses later appends.
async fn a_close_ends_every_read_with_done() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        case.append(client(), 1, vec![record("1")]).await.unwrap();
        case.close(client()).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["1"]);
        assert!(read.done);
        let again = case
            .read(ReadRecordsRequest {
                from_offset: read.last_offset.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(again.records.is_empty() && again.done);
        assert_eq!(again.last_offset, read.last_offset);
        assert_eq!(
            case.describe().await.unwrap().status,
            StreamStatus::Closed as i32
        );
        let error = case
            .append(client(), 2, vec![record("2")])
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), reason(&error).as_str()),
            (Code::FailedPrecondition, "stream_closed")
        );
    }
}

/// A read of a closed Stream whose Retention passed fails as expired, from any offset, instead
/// of waiting out its wait.
async fn a_read_of_an_expired_closed_stream_fails_instead_of_waiting() {
    for case in short_cases().await {
        case.create().await.unwrap();
        let first = case.append(client(), 1, vec![record("1")]).await.unwrap();
        case.close(client()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let describe = case.describe().await.unwrap();
        assert_eq!(describe.status, StreamStatus::Expired as i32);
        for request in [
            ReadRecordsRequest::default(),
            ReadRecordsRequest {
                from_offset: first.first_offset,
                ..Default::default()
            },
            ReadRecordsRequest {
                from_offset: describe.last_offset,
                ..Default::default()
            },
        ] {
            let started = Instant::now();
            let error = case
                .read(ReadRecordsRequest {
                    wait: millis(1500),
                    ..request
                })
                .await
                .unwrap_err();
            assert_eq!(reason(&error), "stream_expired");
            assert!(started.elapsed() < Duration::from_secs(1));
        }
    }
}

/// An open Stream keeps its Records past its Retention, which counts from the close. So a read
/// gets every Record until one Retention after the close, and then the Stream is expired. The
/// Retention is the Store's, since a create takes none.
async fn the_retention_counts_from_the_close() {
    for case in short_cases().await {
        case.create().await.unwrap();
        let first = case
            .append(client(), 1, vec![record("a"), record("b")])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let after_first = ReadRecordsRequest {
            from_offset: first.first_offset.clone(),
            ..Default::default()
        };
        let read = case.read(after_first.clone()).await.unwrap();
        assert_eq!(texts(&read), ["b"]);
        case.append(client(), 2, vec![record("c")]).await.unwrap();
        case.close(client()).await.unwrap();
        let read = case.read(ReadRecordsRequest::default()).await.unwrap();
        assert_eq!(texts(&read), ["a", "b", "c"]);
        let describe = case.describe().await.unwrap();
        assert_eq!(describe.status, StreamStatus::Closed as i32);
        tokio::time::sleep(Duration::from_millis(400)).await;
        let describe = case.describe().await.unwrap();
        assert_eq!(describe.status, StreamStatus::Expired as i32);
        let error = case.read(after_first).await.unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
    }
}

/// With a last-write TTL in the Store config, an open Stream expires that long after its last
/// write. Each write starts the TTL again.
async fn an_open_stream_expires_its_last_write_ttl_after_its_last_write() {
    let config = RuntimeConfig {
        last_write_ttl: Some(Duration::from_millis(500)),
        ..Default::default()
    };
    for case in cases_with(Needs::SHORT_TIMES, &config).await {
        case.create().await.unwrap();
        case.append(client(), 1, vec![record("a")]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        case.append(client(), 2, vec![record("b")]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let describe = case.describe().await.unwrap();
        assert_eq!(describe.status, StreamStatus::Open as i32);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let describe = case.describe().await.unwrap();
        assert_eq!(describe.status, StreamStatus::Expired as i32);
        let error = case
            .append(client(), 3, vec![record("c")])
            .await
            .unwrap_err();
        assert_eq!(reason(&error), "stream_expired");
    }
}

/// A create answer names the configured Store, and the Store never reads the name.
async fn a_create_answer_names_the_store_and_the_store_never_reads_it() {
    for ext in super::support::named_runtimes("redis-a") {
        let unique = unique();
        let case = Case {
            ext,
            namespace: format!("ns-{unique}"),
            stream: format!("s-{unique}"),
        };
        let created = case.create().await.unwrap();
        let stream = created.r#ref.unwrap();
        assert_eq!(stream.id, case.stream);
        assert_eq!(stream.store, "redis-a");
        // A ref with any Store name, or none, reaches the same Stream.
        let named = |store: &str| {
            Some(StreamRef {
                id: case.stream.clone(),
                store: store.to_string(),
            })
        };
        for (sequence, stream) in [(1, named("redis-a")), (2, named("")), (3, named("other"))] {
            let request = AppendRecordsRequest {
                namespace: case.namespace.clone(),
                r#ref: stream,
                producer: Some(client()),
                sequence,
                records: vec![record(&sequence.to_string())],
            };
            case.ext.append_records(request).await.unwrap();
        }
        let read = ReadRecordsRequest {
            namespace: case.namespace.clone(),
            r#ref: named("other"),
            ..Default::default()
        };
        let read = case.ext.read_records(read).await.unwrap();
        assert_eq!(texts(&read), ["1", "2", "3"]);
    }
    // With no Store name in the config, the ref has none, and lang reads it as the default.
    for case in cases_for(Needs::OWN_CONFIG).await {
        let created = case.create().await.unwrap();
        assert_eq!(created.r#ref.unwrap().store, "");
    }
}

/// A read of a closed Stream gives `done` only with its last Record. With no Record left, a read
/// answers at once with `done`, even with a long wait.
async fn a_read_of_a_closed_stream_with_no_record_left_answers_at_once() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let records = vec![record("1"), record("2")];
        let written = case.append(client(), 1, records).await.unwrap();
        case.close(client()).await.unwrap();
        let first = case
            .read(ReadRecordsRequest {
                max_records: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&first), ["1"]);
        assert!(!first.done);
        let rest = case
            .read(ReadRecordsRequest {
                from_offset: first.last_offset,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(texts(&rest), ["2"]);
        assert!(rest.done);
        let started = Instant::now();
        let read = case
            .read(ReadRecordsRequest {
                from_offset: written.last_offset.clone(),
                wait: millis(5000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(read.records.is_empty() && read.done);
        assert_eq!(read.last_offset, written.last_offset);
    }
}

/// A long poll at the end of an open Stream wakes on its close and answers with `done`.
async fn a_waiting_read_wakes_with_done_on_a_close() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let written = case.append(client(), 1, vec![record("1")]).await.unwrap();
        let ext = case.ext.clone();
        let close = CloseStreamRequest {
            namespace: case.namespace.clone(),
            r#ref: case.stream_ref(),
            producer: Some(client()),
        };
        let closer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            ext.close_stream(close).await
        });
        let started = Instant::now();
        let woke = case
            .read(ReadRecordsRequest {
                from_offset: written.last_offset,
                wait: millis(5000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(4));
        assert!(woke.records.is_empty() && woke.done);
        closer.await.unwrap().unwrap();
    }
}

/// A page stays under `max_bytes`, and a larger Record is still returned alone.
async fn a_page_keeps_under_max_bytes_and_a_larger_record_comes_alone() {
    for case in cases_for(Needs::CALLS).await {
        case.create().await.unwrap();
        let big = "x".repeat(1000);
        let records = vec![record("a"), record("b"), record(&big), record("c")];
        case.append(client(), 1, records).await.unwrap();
        let capped = |from_offset| ReadRecordsRequest {
            from_offset,
            max_bytes: 200,
            ..Default::default()
        };
        let first = case.read(capped(vec![])).await.unwrap();
        assert_eq!(texts(&first), ["a", "b"]);
        let second = case.read(capped(first.last_offset)).await.unwrap();
        assert_eq!(texts(&second), [big]);
        let third = case.read(capped(second.last_offset)).await.unwrap();
        assert_eq!(texts(&third), ["c"]);
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(a_create_of_an_open_stream_fails_unless_it_uses_it_or_retries),
    case!(a_create_of_a_closed_stream_fails),
    case!(a_close_writes_no_record_and_a_second_close_succeeds),
    case!(a_create_of_an_expired_stream_fails),
    case!(a_producer_has_a_kind_and_is_not_a_workflow),
    case!(a_producer_id_is_opaque_but_not_empty_or_too_long),
    case!(a_close_takes_a_producer_of_any_kind),
    case!(a_write_with_a_missing_required_field_is_invalid),
    case!(an_append_with_a_record_that_has_no_body_is_invalid),
    case!(an_append_with_no_sequence_takes_the_next_one_of_its_epoch),
    case!(a_read_is_exclusive_from_its_offset_and_get_offset_reads_only_new_records),
    case!(a_read_of_an_uncreated_stream_fails_at_once),
    case!(a_quiet_read_ends_empty_after_its_wait_and_a_long_poll_wakes_on_append),
    case!(a_close_ends_every_read_with_done),
    case!(a_read_of_an_expired_closed_stream_fails_instead_of_waiting),
    case!(the_retention_counts_from_the_close),
    case!(an_open_stream_expires_its_last_write_ttl_after_its_last_write),
    case!(a_create_answer_names_the_store_and_the_store_never_reads_it),
    case!(a_read_of_a_closed_stream_with_no_record_left_answers_at_once),
    case!(a_waiting_read_wakes_with_done_on_a_close),
    case!(a_page_keeps_under_max_bytes_and_a_larger_record_comes_alone),
];
