//! The stored Records of a batch, and the bounds that measure them. The runtime runs the check of a
//! batch's Records and these bounds before a Store sees a request.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{AppendRecord, Producer, StreamRecord},
};
use prost::{Message, encoding};

/// The maximum size of a stored Record, in bytes.
pub const MAX_RECORD_BYTES: usize = 1536 * 1024;
/// The maximum size of a stored append, in bytes.
pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// The maximum number of Records in an append. Many small Records fit under [MAX_BATCH_BYTES], but
/// they keep a Store busy for long.
pub const MAX_BATCH_RECORDS: usize = 1000;

/// One stored data Record. It has no offset, since the Store gives the entry its offset.
pub fn data_record(producer: &Producer, record: AppendRecord) -> Vec<u8> {
    StreamRecord {
        producer: Some(producer.clone()),
        body: record.body,
        ..Default::default()
    }
    .encode_to_vec()
}

/// Refuses a batch with a Record that has no body, before the Store.
pub(crate) fn check_records(records: &[AppendRecord]) -> StreamResult<()> {
    match records.iter().position(|record| record.body.is_none()) {
        Some(at) => Err(StreamError::invalid(format!(
            "Record {at} of the batch has no body"
        ))),
        None => Ok(()),
    }
}

/// The stored size of one data Record in bytes, which is `data_record(producer, record).len()`.
/// It measures the Record and allocates nothing.
pub fn data_record_len(producer: &Producer, record: &AppendRecord) -> usize {
    // The tags are those of `producer` and `body` in `StreamRecord`. A test checks the sum against
    // the encoded Record.
    encoding::message::encoded_len(1, producer)
        + record
            .body
            .as_ref()
            .map_or(0, |body| encoding::message::encoded_len(2, body))
}

/// Refuses a batch over [MAX_BATCH_RECORDS], [MAX_RECORD_BYTES] or [MAX_BATCH_BYTES] with
/// [Reason::TooLarge]. It measures the stored Records of the batch, and encodes and clones nothing.
pub fn check_bounds(producer: &Producer, records: &[AppendRecord]) -> StreamResult<()> {
    if records.len() > MAX_BATCH_RECORDS {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!(
                "The batch has {} Records, more than {MAX_BATCH_RECORDS}",
                records.len()
            ),
        ));
    }
    let mut total = 0;
    for record in records {
        let size = data_record_len(producer, record);
        if size > MAX_RECORD_BYTES {
            return Err(StreamError::new(
                Reason::TooLarge,
                format!("A Record has {size} bytes, more than {MAX_RECORD_BYTES}"),
            ));
        }
        total += size;
    }
    if total > MAX_BATCH_BYTES {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!("The batch has {total} bytes, more than {MAX_BATCH_BYTES}"),
        ));
    }
    Ok(())
}

/// The stored Records of one batch. A batch over [MAX_BATCH_RECORDS], [MAX_RECORD_BYTES] or
/// [MAX_BATCH_BYTES] fails with [Reason::TooLarge].
pub fn data_records(producer: &Producer, records: Vec<AppendRecord>) -> StreamResult<Vec<Vec<u8>>> {
    check_bounds(producer, &records)?;
    Ok(records
        .into_iter()
        .map(|record| data_record(producer, record))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        ActivityEpoch, ActivityProducer, ClientProducer, LocalActivityEpoch, LocalActivityProducer,
        WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    };
    use prost_types::Timestamp;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    #[test]
    fn a_record_with_no_body_is_invalid() {
        let body = AppendRecord {
            body: Some(Payload::default()),
        };
        check_records(std::slice::from_ref(&body)).unwrap();
        let error = check_records(&[body, AppendRecord::default()]).unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        assert!(error.message.contains("Record 1"), "{}", error.message);
    }

    #[test]
    fn a_batch_over_the_limits_fails() {
        let producer = Producer::default();
        let record = |bytes: usize| AppendRecord {
            body: Some(Payload {
                data: vec![0; bytes],
                ..Default::default()
            }),
        };
        let error = data_records(&producer, vec![record(MAX_RECORD_BYTES)]).unwrap_err();
        assert_eq!(error.reason, Reason::TooLarge);
        let many = (0..4)
            .map(|_| record(MAX_RECORD_BYTES / 2 + 1))
            .collect::<Vec<_>>();
        let many = [many.clone(), many].concat();
        assert_eq!(
            data_records(&producer, many).unwrap_err().reason,
            Reason::TooLarge
        );
        assert_eq!(data_records(&producer, vec![record(10)]).unwrap().len(), 1);
        let small = vec![record(1); MAX_BATCH_RECORDS + 1];
        assert_eq!(
            data_records(&producer, small).unwrap_err().reason,
            Reason::TooLarge
        );
        let small = vec![record(1); MAX_BATCH_RECORDS];
        assert_eq!(
            data_records(&producer, small).unwrap().len(),
            MAX_BATCH_RECORDS
        );
    }

    /// One Producer of each kind, with every field set, and one with no kind.
    fn producers() -> Vec<Producer> {
        let task = WorkflowTaskEpoch {
            started_event_id: 300,
            attempt: 2,
            started_time: Some(Timestamp {
                seconds: 1_760_000_000,
                nanos: 5,
            }),
        };
        let activity = ActivityProducer {
            run_id: "run".repeat(100),
            epoch: Some(ActivityEpoch {
                attempt_scheduled_time: Some(Timestamp {
                    seconds: 1_760_000_001,
                    nanos: 999_999_999,
                }),
                attempt: 3,
            }),
        };
        [
            Kind::Workflow(WorkflowProducer {
                run_id: "run".repeat(100),
                epoch: Some(task),
            }),
            Kind::LocalActivity(LocalActivityProducer {
                run_id: "run".repeat(100),
                epoch: Some(LocalActivityEpoch {
                    task: Some(task),
                    attempt: 4,
                }),
            }),
            Kind::WorkflowActivity(activity.clone()),
            Kind::StandaloneActivity(activity),
            Kind::Client(ClientProducer {}),
        ]
        .into_iter()
        .map(|kind| Producer {
            id: "id".repeat(200),
            kind: Some(kind),
        })
        .chain([Producer::default()])
        .collect()
    }

    #[test]
    fn the_measured_size_of_a_record_is_its_stored_size() {
        let body = |data: usize, metadata: &[(&str, &[u8])]| AppendRecord {
            body: Some(Payload {
                metadata: metadata
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_vec()))
                    .collect(),
                data: vec![7; data],
                ..Default::default()
            }),
        };
        let records = [
            body(MAX_RECORD_BYTES / 2, &[]),
            body(200, &[("encoding", b"json/plain"), ("type", b"Greeting")]),
            body(10, &[]),
            body(0, &[]),
            AppendRecord::default(),
        ];
        for producer in producers() {
            for record in &records {
                assert_eq!(
                    data_record_len(&producer, record),
                    data_record(&producer, record.clone()).len(),
                    "{producer:?}, {record:?}"
                );
            }
        }
    }

    #[test]
    fn the_bounds_check_gives_the_errors_of_the_stored_records() {
        let producer = &producers()[0];
        let record = |bytes: usize| AppendRecord {
            body: Some(Payload {
                data: vec![0; bytes],
                ..Default::default()
            }),
        };
        let big = data_record(producer, record(MAX_RECORD_BYTES)).len();
        let half = vec![record(MAX_RECORD_BYTES / 2 + 1); 8];
        let total: usize = half
            .iter()
            .map(|record| data_record(producer, record.clone()).len())
            .sum();
        let cases = [
            (
                vec![record(MAX_RECORD_BYTES)],
                format!("A Record has {big} bytes, more than {MAX_RECORD_BYTES}"),
            ),
            (
                half,
                format!("The batch has {total} bytes, more than {MAX_BATCH_BYTES}"),
            ),
            (
                vec![record(1); MAX_BATCH_RECORDS + 1],
                format!(
                    "The batch has {} Records, more than {MAX_BATCH_RECORDS}",
                    MAX_BATCH_RECORDS + 1
                ),
            ),
        ];
        for (records, message) in cases {
            let error = check_bounds(producer, &records).unwrap_err();
            assert_eq!(error.reason, Reason::TooLarge);
            assert_eq!(error.message, message);
            assert_eq!(data_records(producer, records).unwrap_err(), error);
        }
        // A batch at the bounds passes, and its stored Records are the ones that were measured.
        let fits = vec![record(1); MAX_BATCH_RECORDS];
        check_bounds(producer, &fits).unwrap();
        assert_eq!(
            data_records(producer, fits).unwrap().len(),
            MAX_BATCH_RECORDS
        );
        check_bounds(producer, &[]).unwrap();
    }
}
