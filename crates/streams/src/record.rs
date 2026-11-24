//! The stored form of a Record, and the limits of a batch. The stored form is the encoded
//! [`StreamRecord`](crate::proto::StreamRecord) of the Record with an empty offset.

use crate::{
    AnyError, AppendError, AppendReason, StreamError,
    ids::{validate_producer, validate_ref},
    proto::{AppendRecordsRequest, Producer},
};
use prost::{Message, length_delimiter_len};
use temporalio_common::protos::temporal::api::common::v1::Payload;

pub(crate) const DEFAULT_MAX_RECORD_BYTES: usize = 1536 * 1024;
const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_BATCH_RECORDS: usize = 1000;

/// The limits of a caller on the size of a batch, and on the size of a Record that a read gives.
/// They're settings of the Stream config of an SDK, which builds the
/// [`StreamRuntime`](crate::StreamRuntime) of each Store with them. A size is measured on the
/// stored form of a Record, which counts its Producer. The defaults are:
///
/// - the size of one stored Record, 1.5 MiB
/// - the size of one stored batch, 4 MiB
/// - the number of Records in one batch, 1000
///
/// A Store doesn't know the config of its callers. So a Store with a lower limit fails the batch
/// itself, with [`AppendReason::TooLarge`], and a runtime that a Store runs around itself takes
/// [`StoreLimits::UNBOUNDED`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreLimits {
    pub(crate) max_record_bytes: usize,
    max_batch_bytes: usize,
    max_batch_records: usize,
}

impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_batch_records: DEFAULT_MAX_BATCH_RECORDS,
        }
    }
}

impl StoreLimits {
    /// The limits that bound nothing, for a runtime that a Store runs around itself. The SDK that
    /// calls the Store checks each batch against the limits of its Stream config.
    pub const UNBOUNDED: Self = Self {
        max_record_bytes: usize::MAX,
        max_batch_bytes: usize::MAX,
        max_batch_records: usize::MAX,
    };

    /// Sets the maximum size of one stored Record, in bytes. It must be 1 or more, and the default
    /// is 1.5 MiB. The size of a Record counts its Producer, so a Producer with longer ids leaves
    /// less room for the body. Core holds no Record over the default in a marker, so a read of
    /// Workflow code that keeps its Records in History fails for a larger one.
    pub fn with_max_record_bytes(self, bytes: usize) -> Result<Self, AnyError> {
        Ok(Self {
            max_record_bytes: at_least_one("max_record_bytes", bytes)?,
            ..self
        })
    }

    /// Sets the maximum size of one stored batch, in bytes. It must be 1 or more, and the default
    /// is 4 MiB.
    pub fn with_max_batch_bytes(self, bytes: usize) -> Result<Self, AnyError> {
        Ok(Self {
            max_batch_bytes: at_least_one("max_batch_bytes", bytes)?,
            ..self
        })
    }

    /// Sets the maximum number of Records in one batch. It must be 1 or more, and the default is
    /// 1000.
    pub fn with_max_batch_records(self, records: usize) -> Result<Self, AnyError> {
        Ok(Self {
            max_batch_records: at_least_one("max_batch_records", records)?,
            ..self
        })
    }

    /// Checks the ref, the Producer and the Records of an append, and its sizes against these
    /// limits, with no Store call. The runtime checks each append with it before the Store gets the
    /// append. A lang bridge calls it for an append of client code, which reaches its Store through
    /// no runtime with these limits. It computes the stored size of each Record without encoding
    /// it, since a batch can be megabytes long.
    pub fn validate_append(&self, request: &AppendRecordsRequest) -> Result<(), AppendError> {
        validate_ref(request.r#ref.as_ref())?;
        let producer = validate_producer(request.producer.as_ref())?;
        let records = &request.records;
        if records.is_empty() {
            return Err(StreamError::invalid_argument("The batch has no Record"));
        }
        if let Some(at) = records.iter().position(|record| record.body.is_none()) {
            return Err(StreamError::invalid_argument(format!(
                "Record {at} of the batch has no body"
            )));
        }
        if records.len() > self.max_batch_records {
            return Err(too_large(format!(
                "The batch has {} Records, more than {}",
                records.len(),
                self.max_batch_records
            )));
        }
        let producer_len = producer_len(producer);
        let mut total: usize = 0;
        for (at, record) in records.iter().enumerate() {
            let size = stored_len(producer_len, record.body.as_ref());
            if size > self.max_record_bytes {
                return Err(too_large(format!(
                    "Record {at} of the batch has {size} bytes, more than {}",
                    self.max_record_bytes
                )));
            }
            total = total.checked_add(size).ok_or_else(|| {
                too_large(format!(
                    "The batch has more than {} bytes",
                    self.max_batch_bytes
                ))
            })?;
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

fn at_least_one(option: &str, value: usize) -> Result<usize, AnyError> {
    if value == 0 {
        return Err(AnyError::invalid_argument(format!(
            "{option} must be 1 or more"
        )));
    }
    Ok(value)
}

fn too_large(message: String) -> AppendError {
    AppendError::operation(AppendReason::TooLarge, message)
}

/// Returns the encoded size of a length-delimited field whose content has `len` bytes. The field
/// number must be below 16, so that its tag is one byte.
pub fn field_len(len: usize) -> usize {
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
    use crate::{
        proto::{AppendRecord, ClientProducer, StreamRecord, StreamRef, producer::Kind},
        test_store::{activity, client, local_activity, standalone_activity, task, workflow},
    };

    fn data_record(producer: &Producer, record: AppendRecord) -> Vec<u8> {
        StreamRecord {
            producer: Some(producer.clone()),
            body: record.body,
            offset: Vec::new(),
        }
        .encode_to_vec()
    }

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
        let long_id = Kind::Client(ClientProducer {
            id: "id".repeat(200),
            ..Default::default()
        });
        let producers = [
            workflow(Some(task)),
            local_activity(task, 4),
            activity(1_760_000_001, 999_999_999, 3),
            standalone_activity(1_760_000_001, 999_999_999, 3),
            client(),
            Producer {
                kind: Some(long_id),
            },
            Producer::default(),
        ];
        let with_metadata = AppendRecord {
            body: Some(Payload {
                metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
                data: vec![7; 200],
                ..Default::default()
            }),
        };
        let records = [
            record(DEFAULT_MAX_RECORD_BYTES / 2),
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
    fn a_store_limit_decides_which_batches_it_takes() {
        let producer = client();
        let check = |limits: StoreLimits, records: &[AppendRecord]| {
            limits.validate_append(&AppendRecordsRequest {
                r#ref: Some(StreamRef {
                    id: "s".to_string(),
                    ..Default::default()
                }),
                producer: Some(producer.clone()),
                records: records.to_vec(),
            })
        };
        let over_default = vec![record(0); DEFAULT_MAX_BATCH_RECORDS + 1];
        let default = StoreLimits::default();
        assert!(check(default, &over_default).is_err());
        assert_eq!(check(StoreLimits::UNBOUNDED, &over_default), Ok(()));
        let size = |data| data_record(&producer, record(data)).len();
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
                vec![record(10), record(11)],
                format!(
                    "Record 1 of the batch has {} bytes, more than {}",
                    size(11),
                    size(10)
                ),
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
            assert_eq!(check(limits, &at), Ok(()));
            assert_eq!(check(default, &over), Ok(()));
            let error = AppendError::operation(AppendReason::TooLarge, message);
            assert_eq!(check(limits, &over), Err(error));
        }
    }
}
