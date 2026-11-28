use temporalio_common::protos::coresdk::external_data::{
    LocalActivityMarkerData, StreamAppendResult, StreamReadResult,
};

/// A marker entry that records the index, within its Workflow Task, of the activation that gave
/// its result. Local Activity and Stream results count activations with one index, so that a
/// replay gives both kinds of result in the order of the first run.
pub(crate) trait HasActivationIndex {
    /// Returns the index. A Local Activity marker of an older Core has none, and a replay keeps the
    /// batching of that Core for it.
    fn activation(&self) -> Option<u64>;

    /// Returns true if the first run gave this result in an activation before the one of `other`.
    fn is_before(&self, other: &dyn HasActivationIndex) -> bool {
        (self.activation())
            .zip(other.activation())
            .is_some_and(|(this, other)| this < other)
    }

    /// Advances `current` to the activation of this result. It does nothing when the entry has no
    /// index or `current` is already past it.
    fn advance(&self, current: &mut u64) {
        if let Some(index) = self.activation() {
            *current = (*current).max(index);
        }
    }
}

impl HasActivationIndex for LocalActivityMarkerData {
    fn activation(&self) -> Option<u64> {
        self.activation_index
    }
}

impl HasActivationIndex for StreamAppendResult {
    fn activation(&self) -> Option<u64> {
        Some(self.activation_index)
    }
}

impl HasActivationIndex for StreamReadResult {
    fn activation(&self) -> Option<u64> {
        Some(self.activation_index)
    }
}
