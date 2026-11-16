//! The content digests of the crate. A dedupe entry stores the digest of its batch, so that a resend
//! with the same `sequence` and other content fails instead of being dropped without an error. The
//! Store compares the digest that [batch_digest] gives, and `testdata/digests.json` has its expected
//! values. Core records the digest of the Records that a read gave, and checks it when it reads
//! them again in a replay. `testdata/record_digests.json` has the values of [records_digest].

use crate::{
    epoch::{epoch_key, kind_of},
    proto::{AppendRecord, StreamRecord},
};
use sha2::{Digest, Sha256};
use temporalio_common::protos::temporal::api::common::v1::Payload;

/// The length of a [batch_digest] and of a [records_digest].
pub const DIGEST_LEN: usize = 16;

/// The first byte of a [records_digest], so that it never equals a [batch_digest] of other input.
const RECORDS_TAG: u8 = b'R';

/// The first 16 bytes of a SHA-256 over the Records in order. Each part has its length before it,
/// so that two different batches never hash the same bytes. The metadata is hashed in key order,
/// since a map has no order on the wire.
pub fn batch_digest(records: &[AppendRecord]) -> [u8; DIGEST_LEN] {
    let mut hasher = Sha256::new();
    for record in records {
        body(&mut hasher, record.body.as_ref());
    }
    finish(hasher)
}

/// The first 16 bytes of a SHA-256 over Records that a read gave, in order. History keeps the
/// digest for good, so it hashes a fixed list of fields in an encoding that this crate owns. A
/// field that a later version adds to a message then changes no old digest.
///
/// The input is the byte `R`, then for each Record: its wire offset; the byte 0 for no Producer,
/// or the byte 1, the Producer's kind number as one byte (0 for none), its id, and the 40 bytes of
/// its [epoch_key]; and then its body, as [batch_digest] hashes it. An offset and an id each have
/// their length before them, as a big-endian `u64`.
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
