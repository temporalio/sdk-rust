//! The digest of the Records of a page that a read gave Workflow code. A read marker keeps it in
//! place of the Records, and a replay checks the Records that it reads again against it.

use sha2::{Digest as _, Sha256};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::{
    attempt::{attempt_key, producer_key},
    proto::{StreamRecord, WorkflowTaskAttempt, producer::Kind},
};

/// Number of hash bytes in a read marker, excluding the version prefix.
const DIGEST_LEN: usize = 16;

/// The first byte of the hashed input, so the digest differs from other hashes of the same bytes.
/// It's a fact of the digest's format.
const RECORDS_TAG: u8 = b'R';

/// Original markers contain just the 16-byte hash. New markers prefix it with a version
/// so that replay can still interpret both the old attempt keys and future encodings.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DigestFormat {
    Legacy,
    V1,
}

impl DigestFormat {
    pub(super) fn from_marker(bytes: &[u8]) -> Option<Self> {
        match bytes.len() {
            DIGEST_LEN => Some(Self::Legacy),
            n if n == DIGEST_LEN + 1 && bytes[0] == 1 => Some(Self::V1),
            _ => None,
        }
    }
}

pub(crate) fn digest(records: &[StreamRecord]) -> Vec<u8> {
    DigestFormat::V1.hash(records)
}

impl DigestFormat {
    /// Lengths and counts are big-endian u64s. Offsets and payloads have the
    /// same encoding in both versions. V1 preserves the presence and signed fields of the
    /// Producer's attempt, which its fencing key deliberately normalizes.
    pub(super) fn hash(self, records: &[StreamRecord]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update([RECORDS_TAG]);
        for record in records {
            part(&mut hasher, &record.offset);
            match &record.producer {
                Some(producer) => {
                    hasher.update([1]);
                    match self {
                        Self::Legacy => {
                            part(&mut hasher, &producer_key(producer).unwrap_or_default());
                            hasher.update(attempt_key(producer.kind.as_ref()));
                        }
                        Self::V1 => producer_fields(&mut hasher, producer.kind.as_ref()),
                    }
                }
                None => hasher.update([0]),
            }
            body(&mut hasher, record.body.as_ref());
        }
        let mut digest = match self {
            Self::Legacy => vec![],
            Self::V1 => vec![1],
        };
        digest.extend_from_slice(&hasher.finalize()[..DIGEST_LEN]);
        digest
    }
}

fn optional<T>(hasher: &mut Sha256, value: Option<T>, write: impl FnOnce(&mut Sha256, T)) {
    hasher.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        write(hasher, value);
    }
}

fn timestamp(hasher: &mut Sha256, value: prost_types::Timestamp) {
    hasher.update(value.seconds.to_be_bytes());
    hasher.update(value.nanos.to_be_bytes());
}

fn task(hasher: &mut Sha256, task: WorkflowTaskAttempt) {
    hasher.update(task.started_event_id.to_be_bytes());
    hasher.update(task.attempt.to_be_bytes());
    optional(hasher, task.started_time, timestamp);
}

// A future Producer field must not change a V1 digest, even if the Store's fencing key
// starts using it. Hash only the fields explicitly defined by this marker format.
fn producer_fields(h: &mut Sha256, kind: Option<&Kind>) {
    let Some(kind) = kind else {
        h.update([0]);
        return;
    };
    let activity = match kind {
        Kind::Workflow(w) => {
            h.update([1]);
            for id in [&w.namespace, &w.workflow_id, &w.run_id] {
                part(h, id.as_bytes());
            }
            return optional(h, w.attempt, task);
        }
        Kind::LocalActivity(a) => {
            h.update([2]);
            for id in [&a.namespace, &a.workflow_id, &a.run_id, &a.activity_id] {
                part(h, id.as_bytes());
            }
            return optional(h, a.attempt, |h, a| {
                optional(h, a.task, task);
                h.update(a.attempt.to_be_bytes());
            });
        }
        Kind::WorkflowActivity(a) => {
            h.update([3]);
            for id in [&a.namespace, &a.workflow_id, &a.run_id, &a.activity_id] {
                part(h, id.as_bytes());
            }
            h.update(a.scheduled_event_id.to_be_bytes());
            a.attempt
        }
        Kind::StandaloneActivity(a) => {
            h.update([4]);
            for id in [&a.namespace, &a.activity_id, &a.run_id] {
                part(h, id.as_bytes());
            }
            a.attempt
        }
        Kind::Client(c) => {
            h.update([5]);
            part(h, c.namespace.as_bytes());
            part(h, c.id.as_bytes());
            return;
        }
    };
    optional(h, activity, |h, a| {
        optional(h, a.attempt_scheduled_time, timestamp);
        h.update(a.attempt.to_be_bytes());
    });
}

fn body(hasher: &mut Sha256, body: Option<&Payload>) {
    // A Record with no body and one with an empty body are two different stored Records.
    let Some(body) = body else {
        hasher.update([0]);
        return;
    };
    hasher.update([1]);
    let mut metadata: Vec<_> = body.metadata.iter().collect();
    metadata.sort();
    hasher.update((metadata.len() as u64).to_be_bytes());
    for (key, value) in metadata {
        part(hasher, key.as_bytes());
        part(hasher, value);
    }
    part(hasher, &body.data);
    hasher.update((body.external_payloads.len() as u64).to_be_bytes());
    for external in &body.external_payloads {
        hasher.update(external.size_bytes.to_be_bytes());
    }
}

fn part(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::{
        common::v1::payload::ExternalPayloadDetails,
        stream::v1::{Producer, WorkflowProducer, WorkflowTaskAttempt, producer::Kind},
    };

    use temporalio_streams::proto::{
        ActivityAttempt, LocalActivityAttempt, LocalActivityProducer, StandaloneActivityProducer,
        WorkflowActivityProducer,
    };

    fn record(offset: u8, started_event_id: Option<i64>, body: Option<Payload>) -> StreamRecord {
        let workflow = |started_event_id| Producer {
            kind: Some(Kind::Workflow(WorkflowProducer {
                namespace: "n".to_string(),
                workflow_id: "w".to_string(),
                run_id: "r".to_string(),
                attempt: Some(WorkflowTaskAttempt {
                    started_event_id,
                    attempt: 1,
                    started_time: None,
                }),
            })),
        };
        StreamRecord {
            offset: vec![offset],
            producer: started_event_id.map(workflow),
            body,
        }
    }

    #[test]
    fn attempts_preserve_signed_values_and_presence_for_each_kind() {
        let time = |seconds| Some(prost_types::Timestamp { seconds, nanos: 0 });
        let mut hashes = std::collections::HashSet::new();
        for stamp in [None, time(0), time(-1), time(-2)] {
            let task = WorkflowTaskAttempt {
                started_event_id: -3,
                attempt: -1,
                started_time: stamp,
            };
            let activity = Some(ActivityAttempt {
                attempt: -1,
                attempt_scheduled_time: stamp,
            });
            for kind in [
                Kind::Workflow(WorkflowProducer {
                    attempt: Some(task),
                    ..Default::default()
                }),
                Kind::LocalActivity(LocalActivityProducer {
                    attempt: Some(LocalActivityAttempt {
                        task: Some(task),
                        attempt: -2,
                    }),
                    ..Default::default()
                }),
                Kind::WorkflowActivity(WorkflowActivityProducer {
                    attempt: activity,
                    ..Default::default()
                }),
                Kind::StandaloneActivity(StandaloneActivityProducer {
                    attempt: activity,
                    ..Default::default()
                }),
            ] {
                let record = StreamRecord {
                    producer: Some(Producer { kind: Some(kind) }),
                    ..Default::default()
                };
                assert!(hashes.insert(digest(&[record])));
            }
        }
        for attempt in [
            None,
            Some(LocalActivityAttempt::default()),
            Some(LocalActivityAttempt {
                task: Some(WorkflowTaskAttempt::default()),
                attempt: 0,
            }),
        ] {
            let kind = Kind::LocalActivity(LocalActivityProducer {
                attempt,
                ..Default::default()
            });
            let record = StreamRecord {
                producer: Some(Producer { kind: Some(kind) }),
                ..Default::default()
            };
            assert!(hashes.insert(digest(&[record])));
        }
    }

    // History keeps these digests, so a change to them breaks the replay of existing Workflows.
    #[test]
    fn the_digest_of_known_records_never_changes() {
        let hello = Some(Payload {
            data: b"hello".to_vec(),
            ..Default::default()
        });
        // Prost keeps metadata in a `HashMap`, which is unlikely to give six keys in key order.
        let six_keys = Some(Payload {
            metadata: [
                ("type", "Greeting"),
                ("encoding", "json/plain"),
                ("zone", "eu"),
                ("app", "shop"),
                ("lang", "en"),
                ("id", "42"),
            ]
            .map(|(key, value)| (key.to_string(), value.as_bytes().to_vec()))
            .into(),
            data: b"hi".to_vec(),
            external_payloads: vec![ExternalPayloadDetails { size_bytes: 1024 }],
        });
        // A Producer of a kind that this build doesn't know.
        let no_kind = StreamRecord {
            producer: Some(Default::default()),
            ..record(2, None, None)
        };
        let cases = [
            (vec![], "8c2574892063f995fdf756bce07f46c1"),
            (
                vec![record(1, Some(3), hello.clone())],
                "feb9724c35dca60543545e64b0f6c73c",
            ),
            (
                vec![record(2, Some(3), hello.clone())],
                "ed95e938a53f4e89df137036522ae391",
            ),
            (
                vec![record(1, Some(9), hello.clone())],
                "13f76d43b5d109bce524ef39de005d99",
            ),
            (
                vec![record(2, None, None)],
                "fdc49188bf705cbb99846b2b09d1d243",
            ),
            (vec![no_kind], "72e5fac1ea4ba4577bc9d8033cbda804"),
            (
                vec![record(1, Some(3), six_keys.clone())],
                "9c27fb0c7c87d6e67206cc2b6fe20084",
            ),
            (
                vec![record(1, Some(3), hello), record(9, Some(3), six_keys)],
                "51bf77c0e966964ed24b858959250f2e",
            ),
        ];
        let versioned: Vec<u8> = vec![
            1, 0xc8, 0x5e, 0x3b, 0xfa, 0x92, 0x65, 0xb9, 0x91, 0x67, 0xef, 0x81, 0xe3, 0xcb, 0xef,
            0x06, 0x79,
        ];
        assert_eq!(digest(&cases[1].0), versioned);
        for (records, expected) in cases {
            let hex: String = (DigestFormat::Legacy.hash(&records).iter())
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(hex, expected, "{records:?}");
        }
    }
}
