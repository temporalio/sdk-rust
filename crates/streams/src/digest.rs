//! The digest of the Records that a read gave. Core records it when a Workflow reads a Stream, and
//! checks it when a replay reads the same Records again.

use crate::proto::StreamRecord;
use prost::Message;
use sha2::{Digest, Sha256};
use temporalio_common::protos::temporal::api::common::v1::Payload;

/// The length of a [records_digest].
pub const DIGEST_LEN: usize = 16;

/// The first byte of a [records_digest], so that it never equals the hash of other input.
const RECORDS_TAG: u8 = b'R';

/// The first 16 bytes of a SHA-256 over Records that a read gave, in order. It covers each Record's
/// wire offset, its encoded Producer and its body. A Producer has no map field, so its encoding is
/// the same each time. Each part has its length before it, so that two different lists of Records
/// never hash the same bytes. The metadata is hashed in key order, since a map has no order on the
/// wire.
pub fn records_digest(records: &[StreamRecord]) -> [u8; DIGEST_LEN] {
    let mut hasher = Sha256::new();
    hasher.update([RECORDS_TAG]);
    for record in records {
        part(&mut hasher, &record.offset);
        match &record.producer {
            Some(producer) => {
                hasher.update([1]);
                part(&mut hasher, &producer.encode_to_vec());
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
