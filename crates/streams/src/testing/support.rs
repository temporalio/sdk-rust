//! The helpers that the suites share. They give a new Stream per case on each Store under test,
//! and the Producers of a Workflow run.

#![allow(dead_code)]

use super::{RawKeys, test_stores};
use crate::{
    StreamResult,
    digest::batch_digest,
    proto::{
        AppendRecord, LocalActivityEpoch, LocalActivityProducer, Producer, StreamRecord, StreamRef,
        WorkflowTaskEpoch, producer::Kind,
    },
    store::{Appended, Batch, Closed, Create, Created, Entry, Store},
};
use prost::Message;
use prost_types::Timestamp;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

/// What a case needs beyond the Store's calls on its own Stream. A case runs on every Store that
/// has all that it needs. Each case's comment says its guarantee and its needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Needs(u8);

impl Needs {
    /// Only the Store's calls on the Stream and reads of its Records.
    pub const CALLS: Self = Self(0);
    /// The Store's own results beyond the API. These are a create's stored Retention, `resend`,
    /// `flagged`, and a close's offset and `already`.
    pub const STORE_ANSWERS: Self = Self(1);
    /// The keys under the calls. These are the meta fields, TTLs and raw log entries, and the
    /// Store's log calls `read` and `wait_after`.
    pub const RAW_KEYS: Self = Self(1 << 1);
    /// Producers and runs as sent. The server checks them against its executions, so a
    /// case with made-up runs can't run there.
    pub const UNCHECKED: Self = Self(1 << 2);
    /// A Retention of a few hundred ms.
    pub const SHORT_TIMES: Self = Self(1 << 3);
    /// The runtime built from the crate's own options, such as a Store name.
    pub const OWN_CONFIG: Self = Self(1 << 4);
    /// Every need. Redis and memory have all of them.
    pub const ALL: Self = Self(u8::MAX);

    /// Both needs.
    pub const fn and(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Whether this set gives every need of `needs`.
    pub const fn gives(self, needs: Self) -> bool {
        self.0 & needs.0 == needs.0
    }
}

/// One Stream on one server, fresh for each case.
pub(crate) struct Target {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) raw: Arc<dyn RawKeys>,
    pub(crate) stream: String,
    /// What the Store gives. A case checks it before a step that needs more than it asked for.
    pub(crate) gives: Needs,
}

fn unique() -> String {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}-{n}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// A new Stream on each Store under test that gives `needs`.
pub(crate) async fn stores_for(needs: Needs) -> Vec<Target> {
    let mut targets = vec![];
    for test_store in test_stores() {
        if !test_store.gives().gives(needs) {
            continue;
        }
        let (store, raw) = test_store.connect().await;
        let unique = unique();
        targets.push(Target {
            store,
            raw,
            stream: format!("wf:{unique}"),
            gives: test_store.gives(),
        });
    }
    targets
}

/// The Producers of one Workflow run.
pub(crate) struct Run {
    pub(crate) id: String,
}

fn at(seconds: i64) -> Option<Timestamp> {
    Some(Timestamp { seconds, nanos: 0 })
}

impl Run {
    pub(crate) fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }

    pub(crate) fn local_activity(
        &self,
        call: &str,
        started: i64,
        task_attempt: i32,
        attempt: i32,
    ) -> Producer {
        let task = WorkflowTaskEpoch {
            started_event_id: started,
            attempt: task_attempt,
            started_time: at(100),
        };
        Producer {
            id: format!("la:{}:{call}", self.id),
            kind: Some(Kind::LocalActivity(LocalActivityProducer {
                run_id: self.id.clone(),
                epoch: Some(LocalActivityEpoch {
                    task: Some(task),
                    attempt,
                }),
            })),
        }
    }
}

pub(crate) fn record(producer: &Producer, text: &str) -> Vec<u8> {
    StreamRecord {
        producer: Some(producer.clone()),
        body: Some(
            temporalio_common::protos::temporal::api::common::v1::Payload {
                data: text.as_bytes().to_vec(),
                ..Default::default()
            },
        ),
        ..Default::default()
    }
    .encode_to_vec()
}

/// The bytes of a hex text, as the vectors give binary values.
pub(crate) fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
        .collect()
}

/// The digest of the batch that [record] makes of each text.
pub(crate) fn digest(texts: &[&str]) -> [u8; crate::digest::DIGEST_LEN] {
    let records: Vec<_> = texts
        .iter()
        .map(|text| AppendRecord {
            body: Some(
                temporalio_common::protos::temporal::api::common::v1::Payload {
                    data: text.as_bytes().to_vec(),
                    ..Default::default()
                },
            ),
        })
        .collect();
    batch_digest(&records)
}

/// The data texts of the entries, in log order.
pub(crate) fn records_of(entries: &[Entry]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|entry| entry.record.as_ref())
        .map(|bytes| StreamRecord::decode(bytes.as_slice()).unwrap())
        .map(|record| String::from_utf8(record.body.unwrap().data).unwrap())
        .collect()
}

impl Target {
    pub(crate) fn stream_ref(&self) -> StreamRef {
        StreamRef {
            id: self.stream.clone(),
            ..Default::default()
        }
    }

    pub(crate) async fn create(&self) {
        self.create_with(&Create::default()).await.unwrap();
    }

    /// Creates the Stream, and gives the Store's answer.
    pub(crate) async fn create_with(&self, create: &Create<'_>) -> StreamResult<Created> {
        self.store.create(&self.stream_ref(), create).await
    }

    /// Sets one meta field under the Store's calls.
    pub(crate) async fn set_meta(&self, field: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.raw
            .set_meta(&self.stream, field.as_ref(), value.as_ref())
            .await
    }

    /// One meta field.
    pub(crate) async fn meta(&self, field: impl AsRef<[u8]>) -> Option<Vec<u8>> {
        self.raw.meta(&self.stream, field.as_ref()).await
    }

    /// The ms left on the log and on the meta, as `PTTL` gives them.
    pub(crate) async fn ttls(&self) -> (i64, i64) {
        self.raw.ttls(&self.stream).await
    }

    /// Adds a log entry with an explicit id, as `XADD <id>`.
    pub(crate) async fn add_raw_entry(&self, id: &str) {
        self.raw.add_raw_entry(&self.stream, id).await
    }

    /// The number of log entries.
    pub(crate) async fn log_len(&self) -> usize {
        self.raw.log_len(&self.stream).await
    }

    /// Deletes the log, as an eviction would.
    pub(crate) async fn drop_log(&self) {
        self.raw.drop_log(&self.stream).await
    }

    /// Every entry of the log.
    pub(crate) async fn entries(&self) -> Vec<Entry> {
        let (_, entries) = self
            .store
            .read(&self.stream_ref(), 0, 10_000)
            .await
            .unwrap();
        entries
    }

    /// The data texts in the log.
    pub(crate) async fn texts(&self) -> Vec<String> {
        records_of(&self.entries().await)
    }

    /// Appends a batch of `texts` as `producer` with `sequence`.
    pub(crate) async fn batch(
        &self,
        producer: &Producer,
        sequence: i64,
        texts: &[&str],
    ) -> StreamResult<Appended> {
        let records: Vec<_> = texts.iter().map(|text| record(producer, text)).collect();
        self.store
            .append(
                &self.stream_ref(),
                &Batch {
                    producer,
                    sequence,
                    records: &records,
                    digest: digest(texts),
                },
            )
            .await
    }

    pub(crate) async fn close(&self, producer: &Producer) -> StreamResult<Closed> {
        self.store.close(&self.stream_ref(), producer).await
    }
}
