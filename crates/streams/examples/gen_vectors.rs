//! Writes the golden vectors of the epoch keys and of the digest of a read to `testdata/`.
//!
//! This is a second implementation of both. It calls none of the crate's own code, so the crate's
//! tests check `epoch_key` and `records_digest` against values they did not compute. Run it from
//! any directory:
//!
//! ```text
//! cargo run -p temporalio-streams --example gen_vectors
//! ```

use sha2::{Digest, Sha256};
use std::{fs, path::Path};

/// A JSON value that keeps the order of its keys, so the file does not depend on a map type.
enum Json {
    Str(String),
    Int(i128),
    Arr(Vec<Json>),
    Obj(Vec<(&'static str, Json)>),
}

impl Json {
    fn str(text: impl Into<String>) -> Json {
        Json::Str(text.into())
    }

    fn int(value: impl Into<i128>) -> Json {
        Json::Int(value.into())
    }

    /// Prints with two-space indent and a final newline, one array item to a line.
    fn pretty(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, 0);
        out.push('\n');
        out
    }

    fn write(&self, out: &mut String, depth: usize) {
        match self {
            Json::Str(text) => quote_json(out, text),
            Json::Int(value) => out.push_str(&value.to_string()),
            Json::Arr(items) if items.is_empty() => out.push_str("[]"),
            Json::Obj(fields) if fields.is_empty() => out.push_str("{}"),
            Json::Arr(items) => {
                out.push_str("[\n");
                for (index, item) in items.iter().enumerate() {
                    indent(out, depth + 1);
                    item.write(out, depth + 1);
                    out.push_str(if index + 1 < items.len() { ",\n" } else { "\n" });
                }
                indent(out, depth);
                out.push(']');
            }
            Json::Obj(fields) => {
                out.push_str("{\n");
                for (index, (key, value)) in fields.iter().enumerate() {
                    indent(out, depth + 1);
                    quote_json(out, key);
                    out.push_str(": ");
                    value.write(out, depth + 1);
                    out.push_str(if index + 1 < fields.len() {
                        ",\n"
                    } else {
                        "\n"
                    });
                }
                indent(out, depth);
                out.push('}');
            }
        }
    }
}

fn indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push_str("  ");
    }
}

/// Quotes a string. Every string of the files is a key, a kind, a case name or hex, so none needs
/// an escape.
fn quote_json(out: &mut String, text: &str) {
    out.push('"');
    out.push_str(text);
    out.push('"');
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The fields of an epoch that a case sets. The rest are zero.
#[derive(Default)]
struct Epoch {
    task_started_event_id: Option<u64>,
    task_attempt: Option<u32>,
    task_started_time: Option<(u64, u32)>,
    attempt_scheduled_time: Option<(u64, u32)>,
    attempt: Option<u32>,
}

impl Epoch {
    fn json(&self) -> Json {
        let time =
            |(seconds, nanos): (u64, u32)| Json::Arr(vec![Json::int(seconds), Json::int(nanos)]);
        let mut fields = Vec::new();
        if let Some(value) = self.task_started_event_id {
            fields.push(("task_started_event_id", Json::int(value)));
        }
        if let Some(value) = self.task_attempt {
            fields.push(("task_attempt", Json::int(value)));
        }
        if let Some(value) = self.task_started_time {
            fields.push(("task_started_time", time(value)));
        }
        if let Some(value) = self.attempt_scheduled_time {
            fields.push(("attempt_scheduled_time", time(value)));
        }
        if let Some(value) = self.attempt {
            fields.push(("attempt", Json::int(value)));
        }
        Json::Obj(fields)
    }

    /// 24 bytes, which compare as bytes in the order of the fields.
    fn task_key(&self) -> Vec<u8> {
        let (seconds, nanos) = self.task_started_time.unwrap_or((0, 0));
        let mut key = Vec::with_capacity(24);
        key.extend(self.task_started_event_id.unwrap_or(0).to_be_bytes());
        key.extend(self.task_attempt.unwrap_or(0).to_be_bytes());
        key.extend(seconds.to_be_bytes());
        key.extend(nanos.to_be_bytes());
        key
    }

    /// 40 bytes, the task key and then the scheduled time and the attempt.
    fn key(&self) -> Vec<u8> {
        let (seconds, nanos) = self.attempt_scheduled_time.unwrap_or((0, 0));
        let mut key = self.task_key();
        key.extend(seconds.to_be_bytes());
        key.extend(nanos.to_be_bytes());
        key.extend(self.attempt.unwrap_or(0).to_be_bytes());
        key
    }
}

fn epochs() -> Json {
    let cases = [
        (
            "workflow",
            Epoch {
                task_started_event_id: Some(8),
                task_attempt: Some(2),
                task_started_time: Some((1760000000, 5)),
                ..Epoch::default()
            },
        ),
        (
            "local_activity",
            Epoch {
                task_started_event_id: Some(8),
                task_attempt: Some(1),
                task_started_time: Some((1760000000, 5)),
                attempt: Some(3),
                ..Epoch::default()
            },
        ),
        (
            "activity",
            Epoch {
                attempt_scheduled_time: Some((1760000001, 999999999)),
                attempt: Some(2),
                ..Epoch::default()
            },
        ),
        ("client", Epoch::default()),
        ("empty", Epoch::default()),
    ];
    Json::Arr(
        cases
            .iter()
            .map(|(kind, epoch)| {
                Json::Obj(vec![
                    ("kind", Json::str(*kind)),
                    ("epoch", epoch.json()),
                    ("key", Json::str(hex(&epoch.key()))),
                    ("task_key", Json::str(hex(&epoch.task_key()))),
                ])
            })
            .collect(),
    )
}

/// The body of a Record. A key that a case leaves out is `None`, which is not the same as empty.
#[derive(Default)]
struct Record {
    metadata: Option<Vec<(&'static str, Vec<u8>)>>,
    data: Option<Vec<u8>>,
    external_payloads: Option<Vec<i64>>,
}

impl Record {
    fn json(&self) -> Json {
        let mut fields = Vec::new();
        if let Some(metadata) = &self.metadata {
            let entries = metadata
                .iter()
                .map(|(key, value)| (*key, Json::str(hex(value))))
                .collect();
            fields.push(("metadata", Json::Obj(entries)));
        }
        if let Some(data) = &self.data {
            fields.push(("data", Json::str(hex(data))));
        }
        if let Some(sizes) = &self.external_payloads {
            let sizes = sizes.iter().map(|size| Json::int(*size)).collect();
            fields.push(("external_payloads", Json::Arr(sizes)));
        }
        Json::Obj(fields)
    }
}

fn hash_part(out: &mut Sha256, data: &[u8]) {
    out.update((data.len() as u64).to_be_bytes());
    out.update(data);
}

/// The body part of a Record, as the digest hashes it.
fn hash_body(out: &mut Sha256, record: &Record) {
    // A Record with none of these keys has no body.
    if record.metadata.is_none() && record.data.is_none() && record.external_payloads.is_none() {
        out.update([0u8]);
        return;
    }
    out.update([1u8]);
    let mut metadata: Vec<(&[u8], &[u8])> = record
        .metadata
        .iter()
        .flatten()
        .map(|(key, value)| (key.as_bytes(), value.as_slice()))
        .collect();
    metadata.sort();
    out.update((metadata.len() as u64).to_be_bytes());
    for (key, value) in metadata {
        hash_part(out, key);
        hash_part(out, value);
    }
    hash_part(out, record.data.as_deref().unwrap_or(&[]));
    let external = record.external_payloads.as_deref().unwrap_or(&[]);
    out.update((external.len() as u64).to_be_bytes());
    for size in external {
        out.update(size.to_be_bytes());
    }
}

/// A Record that a read gave, with its wire offset and its encoded Producer.
struct ReadRecord {
    offset: Vec<u8>,
    producer: Option<Vec<u8>>,
    body: Record,
}

/// The digest of the Records of a read, as the first 16 bytes of the SHA-256. A tag byte `R`
/// starts it, then each Record gives its offset, its Producer and its body.
fn records_digest(records: &[ReadRecord]) -> Vec<u8> {
    let mut out = Sha256::new();
    out.update(b"R");
    for record in records {
        hash_part(&mut out, &record.offset);
        match &record.producer {
            Some(producer) => {
                out.update([1u8]);
                hash_part(&mut out, producer);
            }
            None => out.update([0u8]),
        }
        hash_body(&mut out, &record.body);
    }
    out.finalize()[..16].to_vec()
}

/// One protobuf field, with a one-byte length, which is all these short values need.
fn proto_field(number: u8, wire_type: u8, value: &[u8]) -> Vec<u8> {
    let mut out = vec![(number << 3) | wire_type];
    if wire_type == 2 {
        out.push(value.len() as u8);
    }
    out.extend_from_slice(value);
    out
}

/// The encoded Producer `wf:<run>` of kind Workflow, in Workflow Task `started_event_id`,
/// attempt 1.
fn workflow_producer(run: &str, started_event_id: u8) -> Vec<u8> {
    let epoch = [
        proto_field(1, 0, &[started_event_id]),
        proto_field(2, 0, &[1]),
    ]
    .concat();
    let workflow = [proto_field(1, 2, run.as_bytes()), proto_field(2, 2, &epoch)].concat();
    [
        proto_field(1, 2, format!("wf:{run}").as_bytes()),
        proto_field(2, 2, &workflow),
    ]
    .concat()
}

fn record_digests() -> Json {
    let offset = |value: u64| value.to_be_bytes().to_vec();
    let hello = |offset: Vec<u8>| ReadRecord {
        offset,
        producer: Some(workflow_producer("r1", 3)),
        body: Record {
            data: Some(b"hello".to_vec()),
            ..Record::default()
        },
    };
    let bare = ReadRecord {
        offset: offset(2),
        producer: None,
        body: Record::default(),
    };
    let other_task = ReadRecord {
        producer: Some(workflow_producer("r1", 9)),
        ..hello(offset(1))
    };
    let cases = [
        ("no records", vec![]),
        ("one record", vec![hello(offset(1))]),
        ("one record at another offset", vec![hello(offset(2))]),
        ("one record of another task", vec![other_task]),
        ("no Producer and no body", vec![bare]),
        (
            "two records",
            vec![
                hello(offset(1)),
                ReadRecord {
                    offset: offset(1 << 21),
                    producer: Some(workflow_producer("r1", 3)),
                    body: Record {
                        metadata: Some(vec![("encoding", b"binary/plain".to_vec())]),
                        data: Some(vec![]),
                        ..Record::default()
                    },
                },
            ],
        ),
    ];
    let sums: Vec<Vec<u8>> = cases
        .iter()
        .map(|(_, records)| records_digest(records))
        .collect();
    assert_ne!(sums[1], sums[2]);
    assert_ne!(sums[1], sums[3]);
    Json::Arr(
        cases
            .iter()
            .zip(&sums)
            .map(|((name, records), sum)| {
                let records = records
                    .iter()
                    .map(|record| {
                        let mut fields = vec![("offset", Json::str(hex(&record.offset)))];
                        if let Some(producer) = &record.producer {
                            fields.push(("producer", Json::str(hex(producer))));
                        }
                        fields.push(("body", record.body.json()));
                        Json::Obj(fields)
                    })
                    .collect();
                Json::Obj(vec![
                    ("name", Json::str(*name)),
                    ("records", Json::Arr(records)),
                    ("digest", Json::str(hex(sum))),
                ])
            })
            .collect(),
    )
}

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    for (name, value) in [
        ("epochs.json", epochs()),
        ("record_digests.json", record_digests()),
    ] {
        fs::write(dir.join(name), value.pretty()).unwrap();
    }
}
