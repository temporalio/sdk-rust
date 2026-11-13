//! Writes the shared test vectors of the Stream runtime to `testdata/`.
//!
//! This is a second implementation of the layout. It calls none of the crate's own code for ids,
//! offsets, epochs, fields or digests, so the crate's tests check that code against values it did
//! not compute. The crate's README says which side reads each file. Run it from any directory:
//!
//! ```text
//! cargo run -p temporalio-streams --example gen_vectors
//! ```

use sha2::{Digest, Sha256};
use std::{fs, path::Path};

const SEQ_BITS: u32 = 20;

/// A JSON value that keeps the order of its keys, so the files do not depend on a map type.
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

/// Escapes as the readers of the files need, and leaves non-ASCII text as it is.
fn quote_json(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Percent-encodes every byte but ASCII letters, digits and `_.-~`.
fn part(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"_.-~".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn ids() -> Json {
    let cases = [
        ("temporal-streams", "agent-1:run-1:turn-3"),
        ("p:x", "a%3Ab%2Fc:r%251:%C3%A9%20name"),
        ("temporal-streams", "{x}:a|b é"),
    ];
    // A Stream id is opaque, so the keys take its bytes, whatever they hold.
    let streams = cases
        .iter()
        .map(|(prefix, stream_id)| {
            let base = format!("{}:{{{}}}", part(prefix), part(stream_id));
            Json::Obj(vec![
                ("prefix", Json::str(*prefix)),
                ("stream_id", Json::str(*stream_id)),
                ("meta", Json::str(format!("{base}:meta"))),
                ("log", Json::str(format!("{base}:log"))),
                ("closed", Json::str(format!("{base}:closed"))),
            ])
        })
        .collect();
    Json::Obj(vec![("streams", Json::Arr(streams))])
}

fn offsets() -> Json {
    let ids = [
        "0-1",
        "1-0",
        "1700000000000-0",
        "1700000000000-7",
        "1760000000123-1048575",
    ];
    Json::Arr(
        ids.iter()
            .map(|entry_id| {
                let (ms, seq) = split_entry_id(entry_id);
                Json::Obj(vec![
                    ("entry_id", Json::str(*entry_id)),
                    ("offset", Json::int((ms << SEQ_BITS) | seq)),
                ])
            })
            .collect(),
    )
}

fn split_entry_id(text: &str) -> (u64, u64) {
    let (ms, seq) = text.split_once('-').expect("an entry id has a dash");
    (ms.parse().unwrap(), seq.parse().unwrap())
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

/// An entry id in a Producer field, the ms in 8 bytes and the seq in 4, zero for none.
fn entry_id(text: &str) -> Vec<u8> {
    if text.is_empty() {
        return vec![0; 12];
    }
    let (ms, seq) = split_entry_id(text);
    let mut out = ms.to_be_bytes().to_vec();
    out.extend((seq as u32).to_be_bytes());
    out
}

fn producer_values() -> Json {
    let with_records = Epoch {
        attempt_scheduled_time: Some((1760000001, 999999999)),
        attempt: Some(2),
        ..Epoch::default()
    };
    let empty = Epoch {
        attempt_scheduled_time: Some((1760000002, 0)),
        attempt: Some(3),
        ..Epoch::default()
    };
    let cases = [
        (
            "a batch with Records",
            with_records.key(),
            3u64,
            "1760000000123-4",
            "1760000000123-6",
            vec![Record {
                data: Some(vec![1]),
                ..Record::default()
            }],
        ),
        (
            "an empty batch that wrote no entry",
            empty.key(),
            1,
            "",
            "",
            vec![],
        ),
    ];
    Json::Arr(
        cases
            .iter()
            .map(|(name, fence, sequence, first, last, records)| {
                let batch_digest = digest(records);
                // The 88 bytes of a `p|` field.
                let mut value = fence.clone();
                value.extend(sequence.to_be_bytes());
                value.extend(entry_id(first));
                value.extend(entry_id(last));
                value.extend(&batch_digest);
                Json::Obj(vec![
                    ("name", Json::str(*name)),
                    ("fence", Json::str(hex(fence))),
                    ("sequence", Json::int(*sequence)),
                    ("first", Json::str(*first)),
                    ("last", Json::str(*last)),
                    ("digest", Json::str(hex(&batch_digest))),
                    ("value", Json::str(hex(&value))),
                ])
            })
            .collect(),
    )
}

fn fields() -> Json {
    // A Producer id is opaque, so its field name takes the bytes, also a separator in them.
    let producer = "ingest|eu:1 é";
    Json::Obj(vec![
        ("kind", Json::int(3)),
        ("producer_id", Json::str(producer)),
        ("producer", Json::str(format!("p|3|{producer}"))),
        ("producer_values", producer_values()),
    ])
}

/// A Record of a batch. A key that a case leaves out is `None`, which is not the same as empty.
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

/// The content digest of a batch, which a dedupe entry keeps, as the first 16 bytes of the
/// SHA-256.
fn digest(records: &[Record]) -> Vec<u8> {
    let mut out = Sha256::new();
    let part = |out: &mut Sha256, data: &[u8]| {
        out.update((data.len() as u64).to_be_bytes());
        out.update(data);
    };
    for record in records {
        // A Record with none of these keys has no body.
        if record.metadata.is_none() && record.data.is_none() && record.external_payloads.is_none()
        {
            out.update([0u8]);
            continue;
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
            part(&mut out, key);
            part(&mut out, value);
        }
        part(&mut out, record.data.as_deref().unwrap_or(&[]));
        let external = record.external_payloads.as_deref().unwrap_or(&[]);
        out.update((external.len() as u64).to_be_bytes());
        for size in external {
            out.update(size.to_be_bytes());
        }
    }
    out.finalize()[..16].to_vec()
}

fn digests() -> Json {
    let data = |bytes: &[u8]| Record {
        data: Some(bytes.to_vec()),
        ..Record::default()
    };
    let cases = [
        ("empty batch", vec![]),
        ("one record, no body", vec![Record::default()]),
        ("one record, an empty body", vec![data(b"")]),
        ("one record", vec![data(b"hello")]),
        (
            "metadata in key order",
            vec![Record {
                metadata: Some(vec![
                    ("encoding", b"json/plain".to_vec()),
                    ("a", vec![0x00, 0xff]),
                ]),
                data: Some(br#"{"x":1}"#.to_vec()),
                ..Record::default()
            }],
        ),
        ("two records", vec![data(&[1]), data(&[2, 3])]),
        (
            "external payloads",
            vec![Record {
                data: Some(vec![1]),
                external_payloads: Some(vec![10, 20]),
                ..Record::default()
            }],
        ),
        // The length prefixes give these two different digests, though their joined bytes are equal.
        (
            "metadata key and value split one way",
            vec![Record {
                metadata: Some(vec![("ab", b"c".to_vec())]),
                ..Record::default()
            }],
        ),
        (
            "metadata key and value split another way",
            vec![Record {
                metadata: Some(vec![("a", b"bc".to_vec())]),
                ..Record::default()
            }],
        ),
    ];
    let sums: Vec<Vec<u8>> = cases.iter().map(|(_, records)| digest(records)).collect();
    assert_ne!(sums[sums.len() - 1], sums[sums.len() - 2]);
    assert_ne!(sums[1], sums[2]);
    Json::Arr(
        cases
            .iter()
            .zip(&sums)
            .map(|((name, records), sum)| {
                Json::Obj(vec![
                    ("name", Json::str(*name)),
                    (
                        "records",
                        Json::Arr(records.iter().map(Record::json).collect()),
                    ),
                    ("digest", Json::str(hex(sum))),
                ])
            })
            .collect(),
    )
}

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    for (name, value) in [
        ("ids.json", ids()),
        ("offsets.json", offsets()),
        ("epochs.json", epochs()),
        ("fields.json", fields()),
        ("digests.json", digests()),
    ] {
        fs::write(dir.join(name), value.pretty()).unwrap();
    }
}
