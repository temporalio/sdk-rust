//! Writes the golden vectors of the epoch keys to `testdata/epochs.json`.
//!
//! This is a second implementation of the key layout. It calls none of the crate's own code, so
//! the crate's test checks `epoch_key` against values it did not compute. Run it from any
//! directory:
//!
//! ```text
//! cargo run -p temporalio-streams --example gen_vectors
//! ```

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

/// Escapes as the readers of the file need, and leaves non-ASCII text as it is.
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

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
    fs::write(dir.join("epochs.json"), epochs().pretty()).unwrap();
}
