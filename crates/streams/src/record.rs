//! The stored Records of a batch.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{AppendRecord, Producer, StreamRecord},
    store::{MAX_BATCH_BYTES, MAX_BATCH_RECORDS, MAX_RECORD_BYTES},
};
use prost::Message;

/// One stored data Record. It has no offset, since the Store gives the entry its offset.
pub(crate) fn data_record(producer: &Producer, record: AppendRecord) -> Vec<u8> {
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

/// The stored Records of one batch from outside Workflow code. A batch over the Store's limits
/// fails, since the crate doesn't split a batch it didn't build.
pub(crate) fn data_records(
    producer: &Producer,
    records: Vec<AppendRecord>,
) -> StreamResult<Vec<Vec<u8>>> {
    if records.len() > MAX_BATCH_RECORDS {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!(
                "The batch has {} Records, more than {MAX_BATCH_RECORDS}",
                records.len()
            ),
        ));
    }
    let records: Vec<_> = records
        .into_iter()
        .map(|record| data_record(producer, record))
        .collect();
    if let Some(big) = records
        .iter()
        .find(|record| record.len() > MAX_RECORD_BYTES)
    {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!(
                "A Record has {} bytes, more than {MAX_RECORD_BYTES}",
                big.len()
            ),
        ));
    }
    let total: usize = records.iter().map(Vec::len).sum();
    if total > MAX_BATCH_BYTES {
        return Err(StreamError::new(
            Reason::TooLarge,
            format!("The batch has {total} bytes, more than {MAX_BATCH_BYTES}"),
        ));
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
