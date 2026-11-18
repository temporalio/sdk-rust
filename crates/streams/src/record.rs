//! The stored form of a Record, and the size limits of a batch. The runtime checks each batch
//! against these limits before the Store sees it.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{AppendRecord, Producer, StreamRecord},
};
use prost::{Message, encoding};

/// The largest stored Record, in bytes, that a Store may allow. A Record can go into Workflow
/// History, where the server refuses a payload over 2 MiB by default.
pub const MAX_RECORD_BYTES: usize = 1536 * 1024;
/// The largest stored batch, in bytes, that a Store may allow.
pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// The most Records that a Store may allow in one batch. Many small Records fit under
/// [`MAX_BATCH_BYTES`], but they can keep a Store busy for a long time.
pub const MAX_BATCH_RECORDS: usize = 1000;

/// The size limits of one batch on a Store. The defaults are the `MAX_*` constants, which are also
/// the upper bounds. A Store can set lower limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    max_record_bytes: usize,
    max_batch_bytes: usize,
    max_batch_records: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: MAX_RECORD_BYTES,
            max_batch_bytes: MAX_BATCH_BYTES,
            max_batch_records: MAX_BATCH_RECORDS,
        }
    }
}

fn limit(option: &str, value: usize, most: usize) -> StreamResult<usize> {
    if (1..=most).contains(&value) {
        return Ok(value);
    }
    Err(StreamError::invalid(format!(
        "{option} must be from 1 to {most}, not {value}"
    )))
}

impl BatchLimits {
    /// Sets the maximum stored size of one Record, from 1 to [`MAX_RECORD_BYTES`].
    pub fn with_max_record_bytes(self, bytes: usize) -> StreamResult<Self> {
        let max_record_bytes = limit("max_record_bytes", bytes, MAX_RECORD_BYTES)?;
        Ok(Self {
            max_record_bytes,
            ..self
        })
    }

    /// Sets the maximum stored size of one batch, from 1 to [`MAX_BATCH_BYTES`].
    pub fn with_max_batch_bytes(self, bytes: usize) -> StreamResult<Self> {
        let max_batch_bytes = limit("max_batch_bytes", bytes, MAX_BATCH_BYTES)?;
        Ok(Self {
            max_batch_bytes,
            ..self
        })
    }

    /// Sets the maximum number of Records in one batch, from 1 to [`MAX_BATCH_RECORDS`].
    pub fn with_max_batch_records(self, records: usize) -> StreamResult<Self> {
        let max_batch_records = limit("max_batch_records", records, MAX_BATCH_RECORDS)?;
        Ok(Self {
            max_batch_records,
            ..self
        })
    }

    /// The maximum stored size of one Record, in bytes.
    pub fn max_record_bytes(&self) -> usize {
        self.max_record_bytes
    }

    /// The maximum stored size of one batch, in bytes.
    pub fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    /// The maximum number of Records in one batch.
    pub fn max_batch_records(&self) -> usize {
        self.max_batch_records
    }

    /// Fails with [`Reason::TooLarge`] if the batch is over a limit. It measures the stored size
    /// of each Record without encoding or cloning it.
    pub fn check(&self, producer: &Producer, records: &[AppendRecord]) -> StreamResult<()> {
        let Self {
            max_record_bytes,
            max_batch_bytes,
            max_batch_records,
        } = *self;
        if records.len() > max_batch_records {
            return Err(StreamError::new(
                Reason::TooLarge,
                format!(
                    "The batch has {} Records, more than {max_batch_records}",
                    records.len()
                ),
            ));
        }
        let mut total = 0;
        for record in records {
            let size = data_record_len(producer, record);
            if size > max_record_bytes {
                return Err(StreamError::new(
                    Reason::TooLarge,
                    format!("A Record has {size} bytes, more than {max_record_bytes}"),
                ));
            }
            total += size;
        }
        if total > max_batch_bytes {
            return Err(StreamError::new(
                Reason::TooLarge,
                format!("The batch has {total} bytes, more than {max_batch_bytes}"),
            ));
        }
        Ok(())
    }
}

/// Encodes the stored form of one data Record. It has no offset, since the Store assigns it.
pub fn data_record(producer: &Producer, record: AppendRecord) -> Vec<u8> {
    StreamRecord {
        producer: Some(producer.clone()),
        body: record.body,
        ..Default::default()
    }
    .encode_to_vec()
}

/// Fails if a Record of the batch has no body.
pub(crate) fn check_records(records: &[AppendRecord]) -> StreamResult<()> {
    match records.iter().position(|record| record.body.is_none()) {
        Some(at) => Err(StreamError::invalid(format!(
            "Record {at} of the batch has no body"
        ))),
        None => Ok(()),
    }
}

/// The length of the [`data_record`] output, without allocating.
pub fn data_record_len(producer: &Producer, record: &AppendRecord) -> usize {
    // This must match the fields that `data_record` sets, `producer` (tag 1) and `body` (tag 2).
    encoding::message::encoded_len(1, producer)
        + record
            .body
            .as_ref()
            .map_or(0, |body| encoding::message::encoded_len(2, body))
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

    /// One Producer of each kind with every field set, and one Producer with no kind.
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
    fn a_limit_is_from_one_to_its_constant() {
        type Set = fn(BatchLimits, usize) -> StreamResult<BatchLimits>;
        type Get = fn(&BatchLimits) -> usize;
        let setters: [(&str, usize, Set, Get); 3] = [
            (
                "max_record_bytes",
                MAX_RECORD_BYTES,
                BatchLimits::with_max_record_bytes,
                BatchLimits::max_record_bytes,
            ),
            (
                "max_batch_bytes",
                MAX_BATCH_BYTES,
                BatchLimits::with_max_batch_bytes,
                BatchLimits::max_batch_bytes,
            ),
            (
                "max_batch_records",
                MAX_BATCH_RECORDS,
                BatchLimits::with_max_batch_records,
                BatchLimits::max_batch_records,
            ),
        ];
        for (option, most, set, get) in setters {
            for value in [0, most + 1] {
                let error = set(BatchLimits::default(), value).unwrap_err();
                assert_eq!(error.reason, Reason::Invalid);
                assert_eq!(
                    error.message,
                    format!("{option} must be from 1 to {most}, not {value}")
                );
            }
            for value in [1, most] {
                let changed = set(BatchLimits::default(), value).unwrap();
                assert_eq!(get(&changed), value);
            }
        }
    }

    #[test]
    fn smaller_limits_refuse_a_batch_that_the_default_takes() {
        let producer = &producers()[0];
        let record = |bytes: usize| AppendRecord {
            body: Some(Payload {
                data: vec![0; bytes],
                ..Default::default()
            }),
        };
        // A Record of 11 bytes is one stored byte larger than one of 10.
        let small = data_record_len(producer, &record(10));
        let larger = data_record_len(producer, &record(11));
        assert_eq!(larger, small + 1);
        let default = BatchLimits::default();
        let cases = [
            (
                default.with_max_batch_records(2).unwrap(),
                vec![record(10); 3],
                "The batch has 3 Records, more than 2".to_string(),
                vec![record(10); 2],
            ),
            (
                default.with_max_record_bytes(small).unwrap(),
                vec![record(11)],
                format!("A Record has {larger} bytes, more than {small}"),
                vec![record(10)],
            ),
            (
                default.with_max_batch_bytes(2 * small).unwrap(),
                vec![record(10), record(11)],
                format!(
                    "The batch has {} bytes, more than {}",
                    small + larger,
                    2 * small
                ),
                vec![record(10); 2],
            ),
        ];
        for (limits, over, message, at) in cases {
            let error = limits.check(producer, &over).unwrap_err();
            assert_eq!(error.reason, Reason::TooLarge);
            assert_eq!(error.message, message);
            default.check(producer, &over).unwrap();
            limits.check(producer, &at).unwrap();
        }
    }
}
