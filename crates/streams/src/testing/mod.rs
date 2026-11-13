//! The shared suites of the Stream rules. Each Store crate runs them on its own Stores, so that
//! every Store keeps the same rules. This crate runs them on the memory Store.

mod support;

pub mod fields;
pub mod store;

pub use support::Needs;

use crate::{
    memory::MemoryStore,
    store::{DEFAULT_RETENTION, Store},
};
use futures_util::{FutureExt, future::join_all};
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc};

/// The state under a Store's calls, which some cases read or change.
#[async_trait::async_trait]
pub trait RawKeys: Send + Sync {
    /// Sets one meta field of a Stream. Field names and values are bytes, as Redis keeps them.
    async fn set_meta(&self, stream: &str, field: &[u8], value: &[u8]);
    /// One meta field of a Stream.
    async fn meta(&self, stream: &str, field: &[u8]) -> Option<Vec<u8>>;
    /// The ms left on the log and on the meta, as `PTTL` gives them.
    async fn ttls(&self, stream: &str) -> (i64, i64);
    /// Adds a log entry with an explicit id, as `XADD <id>`.
    async fn add_raw_entry(&self, stream: &str, id: &str);
    /// The number of log entries.
    async fn log_len(&self, stream: &str) -> usize;
    /// Deletes the log, as an eviction would.
    async fn drop_log(&self, stream: &str);
}

/// A Store that the suites run on, with the server under it.
#[async_trait::async_trait]
pub trait TestStore: Send + Sync {
    /// What the Store gives. A case that needs more doesn't run on it.
    fn gives(&self) -> Needs;
    /// A Store for one case, and the state under it.
    async fn connect(&self) -> (Arc<dyn Store>, Arc<dyn RawKeys>);
}

/// The memory Store, with a new Store for each case.
#[derive(Debug, Default)]
pub struct MemoryTestStore;

#[async_trait::async_trait]
impl TestStore for MemoryTestStore {
    fn gives(&self) -> Needs {
        Needs::ALL
    }

    async fn connect(&self) -> (Arc<dyn Store>, Arc<dyn RawKeys>) {
        let store = Arc::new(MemoryStore::new(DEFAULT_RETENTION, None));
        (store.clone(), store)
    }
}

#[async_trait::async_trait]
impl RawKeys for MemoryStore {
    async fn set_meta(&self, stream: &str, field: &[u8], value: &[u8]) {
        self.test_set_meta(stream, field, value)
    }

    async fn meta(&self, stream: &str, field: &[u8]) -> Option<Vec<u8>> {
        self.test_meta(stream, field)
    }

    async fn ttls(&self, stream: &str) -> (i64, i64) {
        self.test_ttls(stream)
    }

    async fn add_raw_entry(&self, stream: &str, id: &str) {
        self.test_add_entry(stream, id)
    }

    async fn log_len(&self, stream: &str) -> usize {
        self.test_log_len(stream)
    }

    async fn drop_log(&self, stream: &str) {
        self.test_drop_log(stream)
    }
}

/// One case of a suite.
pub struct Case {
    /// The name of the case, for the report.
    pub name: &'static str,
    /// Runs the case on each Store under test that gives what it needs.
    pub run: fn() -> Pin<Box<dyn Future<Output = ()>>>,
}

macro_rules! case {
    ($name:ident) => {
        $crate::testing::Case {
            name: stringify!($name),
            run: || Box::pin($name()),
        }
    };
}
pub(crate) use case;

tokio::task_local! {
    static TEST_STORES: Vec<Arc<dyn TestStore>>;
}

/// The Stores of the running suite.
pub(crate) fn test_stores() -> Vec<Arc<dyn TestStore>> {
    TEST_STORES.with(Clone::clone)
}

/// Runs `cases` on `stores`, all at once, and then fails with the names of the cases that
/// failed. A case that fails prints its panic as it goes.
pub async fn run(stores: Vec<Arc<dyn TestStore>>, cases: &[Case]) {
    let outcomes = TEST_STORES
        .scope(
            stores,
            join_all(cases.iter().map(|case| async move {
                let passed = AssertUnwindSafe((case.run)()).catch_unwind().await.is_ok();
                (case.name, passed)
            })),
        )
        .await;
    let failed: Vec<_> = outcomes
        .into_iter()
        .filter(|(_, passed)| !passed)
        .map(|(name, _)| name)
        .collect();
    assert!(failed.is_empty(), "these cases failed: {failed:?}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_store_suite_passes_on_the_memory_store() {
        run(vec![Arc::new(MemoryTestStore)], store::CASES).await;
    }
}
