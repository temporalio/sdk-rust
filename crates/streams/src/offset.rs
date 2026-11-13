//! Offsets. An offset is the Redis entry id `<ms>-<seq>` packed as `ms << 20 | seq` in an `i64`.
//! It is ordered and unique in one Stream, and sparse. Zero is never an offset.
//!
//! On the wire the offset is opaque `bytes`. Lang only compares two offsets and resumes a read with
//! one, so the crate keeps the `i64` form inside and converts at the proto boundary with
//! [offset_to_wire] and [offset_from_wire].

use crate::{StreamError, StreamResult};

/// The number of bits of the entry id's sequence part.
pub const SEQ_BITS: u32 = 20;

/// The wire form of an offset, for the read and append fields that lang reads as opaque `bytes`.
///
/// Zero, which is "no offset", is the empty string, so it compares below every real offset. A real
/// offset is 8-byte big-endian, which keeps the order of the positive `i64` values.
pub fn offset_to_wire(offset: i64) -> Vec<u8> {
    if offset <= 0 {
        return Vec::new();
    }
    (offset as u64).to_be_bytes().to_vec()
}

/// The offset of a wire value that [offset_to_wire] wrote. The empty string is "no offset", zero.
///
/// A value longer than 8 bytes is a bad request, since no offset has more.
pub fn offset_from_wire(bytes: &[u8]) -> StreamResult<i64> {
    if bytes.is_empty() {
        return Ok(0);
    }
    if bytes.len() > 8 {
        return Err(StreamError::invalid(format!(
            "A Stream offset has {} bytes, more than 8",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 8];
    buf[8 - bytes.len()..].copy_from_slice(bytes);
    Ok(u64::from_be_bytes(buf) as i64)
}

const SEQ_MASK: i64 = (1 << SEQ_BITS) - 1;
/// The largest millisecond part that fits, which is past the year 2200.
const MAX_MS: u64 = (i64::MAX >> SEQ_BITS) as u64;

/// Packs a Redis entry id.
pub fn offset_of_entry_id(id: &str) -> StreamResult<i64> {
    let bad = || StreamError::storage_corrupt(format!("The Store gave the entry id {id:?}"));
    let (ms, seq) = id.split_once('-').ok_or_else(bad)?;
    let ms: u64 = ms.parse().map_err(|_| bad())?;
    let seq: u64 = seq.parse().map_err(|_| bad())?;
    if ms > MAX_MS || seq > SEQ_MASK as u64 {
        return Err(StreamError::storage_corrupt(format!(
            "The entry id {id:?} doesn't fit in an offset"
        )));
    }
    Ok(((ms as i64) << SEQ_BITS) | seq as i64)
}

/// Unpacks an offset into its Redis entry id. The offset must be above zero.
pub fn entry_id_of_offset(offset: i64) -> String {
    debug_assert!(offset > 0);
    format!("{}-{}", offset >> SEQ_BITS, offset & SEQ_MASK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wire_offset_keeps_the_order_and_empty_is_none() {
        assert_eq!(offset_to_wire(0), Vec::<u8>::new());
        assert_eq!(offset_from_wire(&[]).unwrap(), 0);
        for offset in [1, 42, (1 << SEQ_BITS) | 7, i64::MAX] {
            assert_eq!(offset_from_wire(&offset_to_wire(offset)).unwrap(), offset);
        }
        // The byte order matches the i64 order, so lang compares two offsets as bytes.
        assert!(offset_to_wire(1) > offset_to_wire(0));
        assert!(offset_to_wire(256) > offset_to_wire(255));
        // A right-aligned short value reads as the same number a full one would.
        assert_eq!(offset_from_wire(&[1]).unwrap(), 1);
        assert_eq!(
            offset_from_wire(&[0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap_err()
                .reason,
            crate::Reason::Invalid
        );
    }

    #[test]
    fn an_entry_id_that_doesnt_fit_is_refused() {
        for id in ["1-1048576", &format!("{}-0", MAX_MS + 1), "x"] {
            let error = offset_of_entry_id(id).unwrap_err();
            assert_eq!(error.reason, crate::Reason::StorageCorrupt, "{id}");
        }
        assert_eq!(
            offset_of_entry_id(&format!("{MAX_MS}-{SEQ_MASK}")).unwrap(),
            i64::MAX
        );
    }
}
