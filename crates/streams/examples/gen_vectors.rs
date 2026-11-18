//! Writes the epoch key vectors to `testdata/epochs.json`.
//!
//! This computes the keys without the crate's code, so `tests/vectors.rs` checks `epoch_key`
//! against values from a second implementation.
//!
//! ```text
//! cargo run -p temporalio-streams --example gen_vectors
//! ```

use serde::Serialize;
use std::{fs, path::Path};

/// The epoch fields that a case sets. Unset fields give zero.
#[derive(Default, Serialize)]
struct Epoch {
    #[serde(skip_serializing_if = "Option::is_none")]
    task_started_event_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_attempt: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_started_time: Option<(u64, u32)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_scheduled_time: Option<(u64, u32)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt: Option<u32>,
}

impl Epoch {
    fn task_key(&self) -> Vec<u8> {
        let (seconds, nanos) = self.task_started_time.unwrap_or_default();
        let mut key = Vec::with_capacity(24);
        key.extend(self.task_started_event_id.unwrap_or_default().to_be_bytes());
        key.extend(self.task_attempt.unwrap_or_default().to_be_bytes());
        key.extend(seconds.to_be_bytes());
        key.extend(nanos.to_be_bytes());
        key
    }

    fn key(&self) -> Vec<u8> {
        let (seconds, nanos) = self.attempt_scheduled_time.unwrap_or_default();
        let mut key = self.task_key();
        key.extend(seconds.to_be_bytes());
        key.extend(nanos.to_be_bytes());
        key.extend(self.attempt.unwrap_or_default().to_be_bytes());
        key
    }
}

#[derive(Serialize)]
struct Case {
    kind: &'static str,
    epoch: Epoch,
    key: String,
    task_key: String,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn main() {
    let cases: Vec<Case> = [
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
    ]
    .into_iter()
    .map(|(kind, epoch)| Case {
        kind,
        key: hex(&epoch.key()),
        task_key: hex(&epoch.task_key()),
        epoch,
    })
    .collect();
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/epochs.json");
    fs::write(path, serde_json::to_string_pretty(&cases).unwrap() + "\n").unwrap();
}
