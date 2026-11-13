//! The calls that every Store gives the Stream service. The Redis Store of
//! `temporalio-streams-redis` is for production, and the memory Store is for tests. Both apply the
//! same rules.

use crate::{
    Reason, StreamError, StreamResult,
    proto::{Producer, StreamRef},
};
use std::time::Duration;
use tokio::time::Instant;

mod lazy;

pub use lazy::LazyStore;

/// The Retention of a Stream whose create sets none, when a Store's options set no other.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The format version this crate writes. A Stream with a higher one refuses writes.
pub const FORMAT_VERSION: u32 = 1;
/// The maximum size of a stored Record, in bytes.
pub const MAX_RECORD_BYTES: usize = 1536 * 1024;
/// The maximum size of a stored append, in bytes.
pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// The maximum number of Records in an append. Many small Records fit under [MAX_BATCH_BYTES], but
/// they keep a Store busy for long.
pub const MAX_BATCH_RECORDS: usize = 1000;
/// The highest batch `sequence`. A Lua number is exact for integers only up to this value.
pub const MAX_SEQUENCE: i64 = (1 << 53) - 1;
/// How long the meta key stays after the log key expires, so that a read can tell an expired Stream
/// from a missing one.
pub const TOMBSTONE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// The longest Retention or last-write TTL, which is the largest proto Duration. Its ms plus the
/// time of a write stay exact in a Lua number.
pub const MAX_TTL: Duration = Duration::from_secs(315_576_000_000);

/// What a create does when its Stream exists and is open, and the create isn't a retry of the
/// create that made the Stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// The create fails with [Reason::AlreadyExists].
    #[default]
    Fail,
    /// The create gets the open Stream, which keeps its own Retention.
    UseExisting,
}

/// A create of a Stream.
#[derive(Debug, Clone, Default)]
pub struct Create<'a> {
    /// The Retention, `None` for the Store default. The service always sends `None`, since the
    /// Store config holds the Retention. A test may set a short one.
    pub retention: Option<Duration>,
    /// What the create does when the Stream exists.
    pub conflict: ConflictPolicy,
    /// Unique for each create call, and the same on its retries. Empty for none. The Store keeps
    /// the request id of the create that made the Stream, so a retry after a lost answer succeeds.
    pub request_id: &'a str,
}

/// What a create found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    /// The create wrote the key, or it is a retry of the create that wrote it.
    Created,
    /// The Stream exists and is open, and the create uses it. The create doesn't change its
    /// Retention.
    Exists,
}

/// The result of a create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Created {
    /// What the create found.
    pub outcome: CreateOutcome,
    /// The Retention of the Stream, the stored one if it existed.
    pub retention: Duration,
}

/// One batch of one Producer.
#[derive(Debug, Clone)]
pub struct Batch<'a> {
    /// The Producer, with its epoch.
    pub producer: &'a Producer,
    /// The batch number in this epoch. Zero takes the next one of the epoch, which needs
    /// [Capabilities::NEXT_SEQUENCE]. Such a batch has no dedupe, so its retry writes it again.
    pub sequence: i64,
    /// The encoded Records.
    pub records: &'a [Vec<u8>],
    /// The [crate::digest::batch_digest] of the batch, which the dedupe entry keeps.
    pub digest: [u8; crate::digest::DIGEST_LEN],
}

/// The result of an append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// The offsets of the first and the last entry, `None` for an empty batch.
    pub offsets: Option<(i64, i64)>,
    /// The Store already had the batch, so the offsets are those of the first result.
    pub resend: bool,
}

/// The result of a close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    /// The Stream was already closed.
    pub already: bool,
}

/// The state of a Stream as one read saw it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamState {
    /// The meta key exists, live or as a Tombstone.
    pub exists: bool,
    /// The format version.
    pub version: u32,
    /// The Stream is closed. Every later append fails, so `newest` is final.
    pub closed: bool,
    /// The offset of the newest entry written, zero if none.
    pub newest: i64,
    /// The newest offset when a write found the log gone, zero if none.
    pub trimmed: i64,
    /// When the Stream stops taking writes, in ms since 1970. Zero for an open Stream with no
    /// last-write TTL, which stays until its close.
    pub expires_ms: i64,
    /// The server time of the read, in ms since 1970.
    pub now_ms: i64,
}

impl StreamState {
    /// The Stream is a Tombstone.
    pub fn expired(&self) -> bool {
        self.exists && self.expires_ms > 0 && self.expires_ms <= self.now_ms
    }

    /// The offset below which the log lost the Records. The Retention counts from the close, so
    /// it hides no Record of a Stream that hasn't expired.
    pub fn retained_from(&self) -> i64 {
        self.trimmed + 1
    }
}

/// One log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Its offset.
    pub offset: i64,
    /// The encoded Record.
    pub record: Option<Vec<u8>>,
}

/// The rules that a Store applies atomically beyond the log. The service checks a capability before
/// a call that needs it. So a Store that lacks the capability refuses the call, and doesn't give
/// weaker guarantees without notice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities(u8);

impl Capabilities {
    /// Each append checks the Producer's epoch and its last `sequence` before it writes. The Store
    /// keeps both for as long as the Stream exists, also for a Producer with no recent append.
    pub const PRODUCER_FENCE: Self = Self(1);
    /// A close marks the Stream closed and wakes the waiting reads. Every later append is
    /// refused.
    pub const CLOSE: Self = Self(1 << 1);
    /// An append with sequence zero takes the next one of the Producer's epoch, in the same atomic
    /// step as the dedupe check. So two such batches never get the same number.
    pub const NEXT_SEQUENCE: Self = Self(1 << 2);
    /// Every capability.
    pub const ALL: Self = Self((1 << 3) - 1);

    const NAMES: [(Self, &'static str); 3] = [
        (Self::PRODUCER_FENCE, "producer_fence"),
        (Self::CLOSE, "close"),
        (Self::NEXT_SEQUENCE, "next_sequence"),
    ];

    /// Both sets.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// This set without the other one.
    pub const fn without(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// Whether every capability of `other` is in this set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// The names of the capabilities in this set.
    pub fn names(self) -> Vec<&'static str> {
        Self::NAMES
            .iter()
            .filter(|(capability, _)| self.contains(*capability))
            .map(|(_, name)| *name)
            .collect()
    }

    /// Refuses a call that needs a capability this set lacks, and names the missing ones.
    pub fn require(self, needed: Self) -> StreamResult<()> {
        let missing = needed.without(self);
        if missing == Self::default() {
            return Ok(());
        }
        Err(unsupported(&missing.names().join(", ")))
    }
}

fn unsupported(capability: &str) -> StreamError {
    StreamError::new(
        Reason::Unsupported,
        format!("The Store lacks the capability {capability}"),
    )
}

/// The calls of a Store. Each one is atomic, as one Lua script is on Redis.
///
/// The required calls make the log contract. They are a create key, an atomic batch append, an
/// ordered read with the Stream's state, stable offsets, a Retention that counts from the close and
/// a wait for the next entry. An append applies the rules of the capabilities that the Store
/// declares. The optional calls fail by default.
#[async_trait::async_trait]
pub trait Store: Send + Sync + 'static {
    /// What the Store does atomically beyond the log. It has no default, so a Store that wraps
    /// another one must say what it forwards.
    fn capabilities(&self) -> Capabilities;

    /// Creates a Stream.
    ///
    /// The create of an existing Stream fails with [Reason::Expired] or [Reason::Closed], in this
    /// order. Then a retry of the create that made the Stream, with the same request id, gets
    /// [CreateOutcome::Created] and changes nothing. Any other create applies its [ConflictPolicy].
    async fn create(&self, stream_ref: &StreamRef, create: &Create<'_>) -> StreamResult<Created>;

    /// Appends the whole batch, or nothing of it. The batch's Records get consecutive offsets that
    /// end at the last offset of [Appended::offsets], so a caller knows the offset of each one.
    async fn append(&self, stream_ref: &StreamRef, batch: &Batch<'_>) -> StreamResult<Appended>;

    /// Reads the Stream's state and up to `count` entries from `from`, inclusive. Zero reads
    /// from the start of the log.
    async fn read(
        &self,
        stream_ref: &StreamRef,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)>;

    /// Waits until the log has an entry after `after`, the Stream is closed, or `deadline` passes.
    /// Returns whether an entry came or the Stream is closed. It may return `false` before the
    /// deadline, and then the caller reads again.
    async fn wait_after(
        &self,
        stream_ref: &StreamRef,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool>;

    /// Closes a Stream. A close writes no log entry, and a close of a closed Stream changes
    /// nothing. Needs [Capabilities::CLOSE].
    async fn close(&self, _stream_ref: &StreamRef, _producer: &Producer) -> StreamResult<Closed> {
        Err(unsupported("close"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_capability_is_refused_by_name() {
        let some = Capabilities::PRODUCER_FENCE.union(Capabilities::CLOSE);
        some.require(Capabilities::CLOSE).unwrap();
        let error = some
            .require(Capabilities::CLOSE.union(Capabilities::NEXT_SEQUENCE))
            .unwrap_err();
        assert_eq!(error.reason, Reason::Unsupported);
        assert!(
            error.message.ends_with("next_sequence"),
            "{}",
            error.message
        );
    }
}
