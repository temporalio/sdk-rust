//! The limits of Stream ids, Producer ids and offsets.

/// The longest Stream id, in bytes.
pub const MAX_STREAM_ID_BYTES: usize = 4096;

/// The longest offset, in bytes, that a caller may give or a Store may give back. An offset is
/// opaque to a caller, and a Worker records the offsets of its calls in History, so an offset of
/// any size could make a History event of any size.
pub const MAX_OFFSET_BYTES: usize = 1024;

/// The longest Producer id, in bytes. A caller builds the id from names that Temporal limits to
/// 1000 characters, so an id with two of them still fits. A Store keeps the id of each Producer for
/// as long as the Stream lives. Each Record of a batch holds its Producer, so the size check counts
/// the Producer once for each Record against
/// [MAX_BATCH_BYTES](crate::record::MAX_BATCH_BYTES).
pub const MAX_PRODUCER_ID_BYTES: usize = 4096;
