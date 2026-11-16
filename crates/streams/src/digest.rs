//! The digest of the Records that a read gave. Core records it when a Workflow reads a Stream, and
//! checks it when a replay reads the same Records again.

use crate::{
    epoch::{epoch_key, kind_of},
    proto::StreamRecord,
};
use sha2::{Digest, Sha256};
use temporalio_common::protos::temporal::api::common::v1::Payload;

/// The length of a [records_digest].
pub const DIGEST_LEN: usize = 16;

/// The first byte of a [records_digest]. It keeps the digest apart from any other hash over the
/// same bytes.
const RECORDS_TAG: u8 = b'R';

/// The first 16 bytes of a SHA-256 over Records that a read gave, in order. History keeps the
/// digest for good, so it hashes a fixed list of fields in an encoding that this crate owns. A
/// field that a later version adds to a message then changes no old digest.
///
/// The input starts with the byte `R`. Each Record adds its wire offset. A Record with no Producer
/// adds the byte 0. Any other adds the byte 1, the kind number of the Producer as one byte (0 for
/// none), the Producer id, and the 40 bytes of its [epoch_key]. Last comes the body. No body is
/// the byte 0. Any other body is the byte 1, its metadata in key order, its data, and the sizes of
/// its external payloads. An offset, an id, a metadata key or value and the data each have their
/// length before them, as a big-endian `u64`. The tests `the_numbers_of_the_kinds_never_change` and
/// `the_bytes_of_a_key_never_change` pin the kind numbers and the epoch keys that it hashes.
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
