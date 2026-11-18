//! The digest of the Records that a read returns. Core keeps it in History when a Workflow reads a
//! Stream, and a replay checks the Records it reads again against it.

use crate::{
    epoch::{epoch_key, kind_of},
    proto::StreamRecord,
};
use sha2::{Digest, Sha256};
use temporalio_common::protos::temporal::api::common::v1::Payload;

/// The length of a [`records_digest`] in bytes.
pub const DIGEST_LEN: usize = 16;

/// The first byte of the hashed input, so the digest differs from other hashes of the same bytes.
const RECORDS_TAG: u8 = b'R';

/// The first 16 bytes of a SHA-256 over the Records that a read returned, in order. History keeps
/// the digest for good, so it hashes a fixed list of fields in an encoding that this crate owns. A
/// field that a later version adds to a message doesn't change an old digest.
///
/// The input starts with the byte `R`. Each Record adds its wire offset, its Producer and its body.
///
/// - A missing Producer is the byte 0. Any other is the byte 1, the
///   [`ProducerKind`](crate::epoch::ProducerKind) number as one byte (0 for no kind), the Producer
///   id, and its 40-byte [`epoch_key`].
/// - A missing body is the byte 0. Any other is the byte 1, the metadata in key order, the data,
///   and the sizes of the external payloads.
///
/// An offset, an id, each metadata key and value, and the data have their length before them. The
/// metadata and the external payloads have their count before them. Lengths and counts are
/// big-endian `u64`, and payload sizes are big-endian `i64`.
pub fn records_digest(records: &[StreamRecord]) -> [u8; DIGEST_LEN] {
    let mut hasher = Sha256::new();
    hasher.update([RECORDS_TAG]);
    for record in records {
        part(&mut hasher, &record.offset);
        match &record.producer {
            Some(producer) => {
                hasher.update([1]);
                hasher.update([kind_of(producer).map_or(0, |kind| kind as u8)]);
                part(&mut hasher, producer.id.as_bytes());
                hasher.update(epoch_key(producer.kind.as_ref()));
            }
            None => hasher.update([0]),
        }
        body(&mut hasher, record.body.as_ref());
    }
    finish(hasher)
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

fn finish(hasher: Sha256) -> [u8; DIGEST_LEN] {
    let mut digest = [0; DIGEST_LEN];
    digest.copy_from_slice(&hasher.finalize()[..DIGEST_LEN]);
    digest
}

fn part(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        ActivityEpoch, ActivityProducer, ClientProducer, LocalActivityEpoch, LocalActivityProducer,
        Producer, WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    };
    use prost_types::Timestamp;
    use temporalio_common::protos::temporal::api::common::v1::payload::ExternalPayloadDetails;

    fn record_of(
        offset: u64,
        producer: Option<(&str, Kind)>,
        body: Option<Payload>,
    ) -> StreamRecord {
        StreamRecord {
            offset: offset.to_be_bytes().to_vec(),
            producer: producer.map(|(id, kind)| Producer {
                id: id.to_string(),
                kind: Some(kind),
            }),
            body,
        }
    }

    /// A Record of the Workflow Producer `wf:r1` in the Workflow Task that started at
    /// `started_event_id`, or a Record with no Producer.
    fn record(offset: u64, started_event_id: Option<i64>, body: Option<Payload>) -> StreamRecord {
        let workflow = |started_event_id| {
            Kind::Workflow(WorkflowProducer {
                run_id: "r1".to_string(),
                epoch: Some(WorkflowTaskEpoch {
                    started_event_id,
                    attempt: 1,
                    started_time: None,
                }),
            })
        };
        record_of(
            offset,
            started_event_id.map(|id| ("wf:r1", workflow(id))),
            body,
        )
    }

    /// One Record of each other kind, each with a non-zero epoch, so the test pins their kind
    /// numbers and epoch keys. One body has an external payload and six metadata keys, so a
    /// `HashMap` that iterates them in sorted order by chance is unlikely.
    fn every_other_kind() -> Vec<StreamRecord> {
        let at = |seconds, nanos| Some(Timestamp { seconds, nanos });
        let activity = |seconds, nanos, attempt| ActivityProducer {
            run_id: "r1".to_string(),
            epoch: Some(ActivityEpoch {
                attempt_scheduled_time: at(seconds, nanos),
                attempt,
            }),
        };
        let local = Kind::LocalActivity(LocalActivityProducer {
            run_id: "r1".to_string(),
            epoch: Some(LocalActivityEpoch {
                task: Some(WorkflowTaskEpoch {
                    started_event_id: 3,
                    attempt: 2,
                    started_time: at(1_760_000_000, 5),
                }),
                attempt: 4,
            }),
        });
        let standalone = ActivityProducer {
            run_id: String::new(),
            ..activity(1_760_000_002, 0, 3)
        };
        let full = Payload {
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
        };
        vec![
            record_of(1, Some(("la:r1:1", local)), Some(full)),
            record_of(
                2,
                Some((
                    "act:r1:1",
                    Kind::WorkflowActivity(activity(1_760_000_001, 7, 2)),
                )),
                None,
            ),
            record_of(
                3,
                Some(("sa:1", Kind::StandaloneActivity(standalone))),
                None,
            ),
            record_of(
                4,
                Some(("client:1", Kind::Client(ClientProducer {}))),
                Some(Payload::default()),
            ),
        ]
    }

    // History keeps these digests, so a change to them breaks the replay of existing Workflows.
    #[test]
    fn the_digest_of_known_records_never_changes() {
        let hello = || {
            Some(Payload {
                data: b"hello".to_vec(),
                ..Default::default()
            })
        };
        let encoded = Some(Payload {
            metadata: [("encoding".to_string(), b"binary/plain".to_vec())].into(),
            ..Default::default()
        });
        let cases = [
            (vec![], "8c2574892063f995fdf756bce07f46c1"),
            (
                vec![record(1, Some(3), hello())],
                "545864e4fef7b6261e6cf2f2c6991243",
            ),
            (
                vec![record(2, Some(3), hello())],
                "881e87ad079a423ec937f227441636a2",
            ),
            (
                vec![record(1, Some(9), hello())],
                "34244dd10338b15ebaea09c98aaefeee",
            ),
            (
                vec![record(2, None, None)],
                "b886d25c6cc809cecbbeb169467ca01a",
            ),
            (
                vec![
                    record(1, Some(3), hello()),
                    record(1 << 21, Some(3), encoded),
                ],
                "8f3a15193645a044783cd77f6a1a1575",
            ),
            (every_other_kind(), "2db099d737839de8997ae7b843b303ad"),
        ];
        for (records, digest) in cases {
            let hex: String = records_digest(&records)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(hex, digest, "{records:?}");
        }
    }
}
