//! A Store that connects on its first call.

use crate::{
    StreamError, StreamResult,
    proto::{Producer, StreamRef},
    store::{Appended, Batch, Capabilities, Closed, Create, Created, Entry, Store, StreamState},
};
use std::{future::Future, pin::Pin};
use tokio::{sync::OnceCell, time::Instant};

type Connect<S> =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = StreamResult<S>> + Send>> + Send + Sync>;

/// A Store that connects its inner Store on the first call, so a bridge can build the runtime with
/// no async runtime, and a Workflow that never publishes never connects. A failed connect is tried
/// again on the next call. An inner Store that lacks a declared capability is refused once, and
/// every later call fails with that refusal.
pub struct LazyStore<S: Store> {
    capabilities: Capabilities,
    connect: Connect<S>,
    store: OnceCell<StreamResult<S>>,
}

impl<S: Store> std::fmt::Debug for LazyStore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyStore")
            .field("capabilities", &self.capabilities)
            .field("connected", &matches!(self.store.get(), Some(Ok(_))))
            .finish_non_exhaustive()
    }
}

impl<S: Store> LazyStore<S> {
    /// A Store that `connect` connects on its first call. The runtime asks for the capabilities
    /// before any call connects, so the Store declares `capabilities` when it is built, and a
    /// connect fails if the inner Store lacks one of them.
    pub fn new<F, Fut, E>(capabilities: Capabilities, connect: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        E: Into<StreamError>,
    {
        Self {
            capabilities,
            connect: Box::new(move || {
                let connecting = connect();
                Box::pin(async move { connecting.await.map_err(Into::into) })
            }),
            store: OnceCell::new(),
        }
    }

    async fn store(&self) -> StreamResult<&S> {
        let connected = self
            .store
            .get_or_try_init(|| async {
                let store = (self.connect)().await?;
                // The same Store lacks the capability on the next connect too.
                Ok::<_, StreamError>(
                    store
                        .capabilities()
                        .require(self.capabilities)
                        .map(|()| store),
                )
            })
            .await?;
        connected.as_ref().map_err(Clone::clone)
    }
}

#[async_trait::async_trait]
impl<S: Store> Store for LazyStore<S> {
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    async fn create(&self, stream_ref: &StreamRef, create: &Create<'_>) -> StreamResult<Created> {
        self.store().await?.create(stream_ref, create).await
    }

    async fn append(&self, stream_ref: &StreamRef, batch: &Batch<'_>) -> StreamResult<Appended> {
        self.store().await?.append(stream_ref, batch).await
    }

    async fn read(
        &self,
        stream_ref: &StreamRef,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        self.store().await?.read(stream_ref, from, count).await
    }

    async fn wait_after(
        &self,
        stream_ref: &StreamRef,
        after: i64,
        deadline: Instant,
    ) -> StreamResult<bool> {
        self.store()
            .await?
            .wait_after(stream_ref, after, deadline)
            .await
    }

    async fn close(&self, stream_ref: &StreamRef, producer: &Producer) -> StreamResult<Closed> {
        self.store().await?.close(stream_ref, producer).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Reason, memory::MemoryStore, store::DEFAULT_RETENTION};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn stream() -> StreamRef {
        StreamRef {
            id: "s".to_string(),
            ..Default::default()
        }
    }

    fn memory() -> MemoryStore {
        MemoryStore::new(DEFAULT_RETENTION, None)
    }

    #[tokio::test]
    async fn a_failed_connect_is_tried_again_on_the_next_call() {
        let connects = Arc::new(AtomicUsize::new(0));
        let counted = connects.clone();
        let lazy = LazyStore::new(Capabilities::ALL, move || {
            let first = counted.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    return Err(StreamError::storage_unavailable("the Store is down"));
                }
                Ok(memory())
            }
        });
        assert_eq!(lazy.capabilities(), Capabilities::ALL);
        assert_eq!(connects.load(Ordering::SeqCst), 0);
        let error = lazy
            .create(&stream(), &Create::default())
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::StorageUnavailable);
        lazy.create(&stream(), &Create::default()).await.unwrap();
        let (state, _) = lazy.read(&stream(), 0, 0).await.unwrap();
        assert!(state.exists);
        assert_eq!(connects.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_connect_refuses_a_store_that_lacks_a_declared_capability_once() {
        let connects = Arc::new(AtomicUsize::new(0));
        let counted = connects.clone();
        let lazy = LazyStore::new(Capabilities::ALL, move || {
            counted.fetch_add(1, Ordering::SeqCst);
            async {
                let fewer = LazyStore::new(Capabilities::CLOSE, || async {
                    Ok::<_, StreamError>(memory())
                });
                Ok::<_, StreamError>(fewer)
            }
        });
        for _ in 0..2 {
            let error = lazy.read(&stream(), 0, 0).await.unwrap_err();
            assert_eq!(error.reason, Reason::Unsupported);
            assert!(
                error.message.contains("producer_fence"),
                "{}",
                error.message
            );
        }
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }
}
