//! The stored form of a Record, and the size limits of a batch.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{AppendRecord, Producer, StreamRecord},
};
use prost::{Message, length_delimiter_len};
use temporalio_common::protos::temporal::api::common::v1::Payload;

/// The largest stored Record that a Store may accept, in bytes. A Record can end up in a marker in
/// Workflow History, and the server rejects a payload over 2 MiB by default.
pub const MAX_RECORD_BYTES: usize = 1536 * 1024;

/// The largest batch that a Store may accept, in bytes of stored Records.
pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// The maximum number of Records that a Store may accept in one batch.
pub const MAX_BATCH_RECORDS: usize = 1000;

/// Size limits for a batch, measured on the stored form of its Records. The defaults are the
/// `MAX_*` constants, which are also the upper bounds, so a Store can only lower them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl BatchLimits {
    /// Sets the maximum size of one stored Record, in bytes. It must be between 1 and
    /// [`MAX_RECORD_BYTES`].
    pub fn with_max_record_bytes(self, bytes: usize) -> StreamResult<Self> {
        Ok(Self {
            max_record_bytes: limit("max_record_bytes", bytes, MAX_RECORD_BYTES)?,
            ..self
        })
    }

    /// Sets the maximum size of one stored batch, in bytes. It must be between 1 and
    /// [`MAX_BATCH_BYTES`].
    pub fn with_max_batch_bytes(self, bytes: usize) -> StreamResult<Self> {
        Ok(Self {
            max_batch_bytes: limit("max_batch_bytes", bytes, MAX_BATCH_BYTES)?,
            ..self
        })
    }

    /// Sets the maximum number of Records in one batch. It must be between 1 and
    /// [`MAX_BATCH_RECORDS`].
    pub fn with_max_batch_records(self, records: usize) -> StreamResult<Self> {
        Ok(Self {
            max_batch_records: limit("max_batch_records", records, MAX_BATCH_RECORDS)?,
            ..self
        })
    }

    /// Checks a batch against the limits. It computes the stored size of each Record without
    /// encoding it, since a batch can be up to 4 MiB.
    pub(crate) fn check(&self, producer: &Producer, records: &[AppendRecord]) -> StreamResult<()> {
        if records.len() > self.max_batch_records {
            return Err(too_large(format!(
                "The batch has {} Records, more than {}",
                records.len(),
                self.max_batch_records
            )));
        }
        let producer_len = producer_len(producer);
        let mut total = 0;
        for record in records {
            let size = stored_len(producer_len, record.body.as_ref());
            if size > self.max_record_bytes {
                return Err(too_large(format!(
                    "A Record has {size} bytes, more than {}",
                    self.max_record_bytes
                )));
            }
            total += size;
        }
        if total > self.max_batch_bytes {
            return Err(too_large(format!(
                "The batch has {total} bytes, more than {}",
                self.max_batch_bytes
            )));
        }
        Ok(())
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

fn too_large(message: String) -> StreamError {
    StreamError::new(Reason::TooLarge, message)
}

/// Encodes a Record in its stored form, which is what [`BatchLimits`] measures. The offset stays
/// empty, because the Store assigns it.
pub fn data_record(producer: &Producer, record: AppendRecord) -> Vec<u8> {
    StreamRecord {
        producer: Some(producer.clone()),
        body: record.body,
        offset: Vec::new(),
    }
    .encode_to_vec()
}

/// The encoded size of a length-delimited field. Every field number of [`StreamRecord`] is below
/// 16, so its tag is one byte.
fn field_len(len: usize) -> usize {
    1 + length_delimiter_len(len) + len
}

pub(crate) fn producer_len(producer: &Producer) -> usize {
    field_len(producer.encoded_len())
}

pub(crate) fn stored_len(producer_len: usize, body: Option<&Payload>) -> usize {
    producer_len + body.map(Message::encoded_len).map_or(0, field_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_store::{
        activity, client, local_activity, standalone_activity, task, workflow,
    };

    fn record(data: usize) -> AppendRecord {
        AppendRecord {
            body: Some(Payload {
                data: vec![7; data],
                ..Default::default()
            }),
        }
    }

    #[test]
    fn the_measured_size_of_a_record_is_its_stored_size() {
        let task = task(300, 2, 1_760_000_000);
        // A 400-byte id makes the length prefix of the Producer two bytes long.
        let producers = [
            workflow(Some(task)),
            local_activity(task, 4),
            activity(1_760_000_001, 999_999_999, 3),
            standalone_activity(1_760_000_001, 999_999_999, 3),
            client(),
            Producer::default(),
        ]
        .map(|producer| Producer {
            id: "id".repeat(200),
            ..producer
        });
        let with_metadata = AppendRecord {
            body: Some(Payload {
                metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
                data: vec![7; 200],
                ..Default::default()
            }),
        };
        let records = [
            record(MAX_RECORD_BYTES / 2),
            with_metadata,
            record(10),
            record(0),
            AppendRecord::default(),
        ];
        for producer in &producers {
            for (at, record) in records.iter().enumerate() {
                let stored = data_record(producer, record.clone()).len();
                let measured = stored_len(producer_len(producer), record.body.as_ref());
                assert_eq!(measured, stored, "Record {at} of {producer:?}");
            }
        }
    }

    #[test]
    fn a_limit_is_from_1_to_its_bound() {
        type Set = fn(BatchLimits, usize) -> StreamResult<BatchLimits>;
        let setters: [(&str, usize, Set); 3] = [
            (
                "max_record_bytes",
                MAX_RECORD_BYTES,
                BatchLimits::with_max_record_bytes,
            ),
            (
                "max_batch_bytes",
                MAX_BATCH_BYTES,
                BatchLimits::with_max_batch_bytes,
            ),
            (
                "max_batch_records",
                MAX_BATCH_RECORDS,
                BatchLimits::with_max_batch_records,
            ),
        ];
        for (option, most, set) in setters {
            for value in [1, most] {
                set(BatchLimits::default(), value).unwrap();
            }
            for value in [0, most + 1] {
                let error = format!("{option} must be from 1 to {most}, not {value}");
                let error = StreamError::invalid(error);
                assert_eq!(set(BatchLimits::default(), value), Err(error));
            }
        }
    }

    #[test]
    fn a_smaller_limit_refuses_a_batch_that_the_default_takes() {
        let producer = client();
        let size = |data| data_record(&producer, record(data)).len();
        let default = BatchLimits::default();
        let cases = [
            (
                default.with_max_batch_records(2),
                vec![record(10); 2],
                vec![record(10); 3],
                "The batch has 3 Records, more than 2".to_string(),
            ),
            (
                default.with_max_record_bytes(size(10)),
                vec![record(10)],
                vec![record(11)],
                format!("A Record has {} bytes, more than {}", size(11), size(10)),
            ),
            (
                default.with_max_batch_bytes(2 * size(10)),
                vec![record(10); 2],
                vec![record(10), record(11)],
                format!(
                    "The batch has {} bytes, more than {}",
                    size(10) + size(11),
                    2 * size(10)
                ),
            ),
        ];
        for (limits, at, over, message) in cases {
            let limits = limits.unwrap();
            assert_eq!(limits.check(&producer, &at), Ok(()));
            assert_eq!(default.check(&producer, &over), Ok(()));
            let error = StreamError::new(Reason::TooLarge, message);
            assert_eq!(limits.check(&producer, &over), Err(error));
        }
    }
}
