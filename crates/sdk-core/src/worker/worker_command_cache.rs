use crate::{TaskToken, telemetry::metrics::WorkerCommandCacheMetrics};
use lru::LruCache;
use parking_lot::Mutex;
use std::{hash::Hash, num::NonZeroUsize, sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;
use tokio::time::Instant;

const ACTIVITY_CANCEL_CAPACITY: usize = 1024;
const ACTIVITY_CANCEL_TTL: Duration = Duration::from_secs(60);

pub(super) struct WorkerCommandCache {
    activity_cancellations: Mutex<ExpiringLru<TaskToken, ()>>,
}

#[derive(Default)]
pub(super) struct ActivityCommandActions {
    pub(super) cancel: bool,
}

struct ExpiringLru<K, V> {
    entries: LruCache<K, (V, Instant)>,
    ttl: Duration,
    metrics: WorkerCommandCacheMetrics,
}

impl<K: Hash + Eq, V> ExpiringLru<K, V> {
    fn new(capacity: NonZeroUsize, ttl: Duration, metrics: WorkerCommandCacheMetrics) -> Self {
        Self {
            entries: LruCache::new(capacity),
            ttl,
            metrics,
        }
    }

    fn insert(&mut self, key: K, value: V) {
        let mut expired = 0;
        while self
            .entries
            .peek_lru()
            .is_some_and(|(_, (_, time))| time.elapsed() >= self.ttl)
        {
            self.entries.pop_lru();
            expired += 1;
        }
        self.metrics.expired(expired);
        let is_new = !self.entries.contains(&key);
        let displaced = self.entries.push(key, (value, Instant::now()));
        if is_new {
            self.metrics.buffered();
            if displaced.is_some() {
                self.metrics.evicted();
            }
        }
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let (value, time) = self.entries.pop(key)?;
        if time.elapsed() >= self.ttl {
            self.metrics.expired(1);
            return None;
        }
        Some(value)
    }
}

impl WorkerCommandCache {
    pub(super) fn new(namespace: &str, meter: Option<TemporalMeter>) -> Arc<Self> {
        let metrics = WorkerCommandCacheMetrics::new(namespace, "activity_cancel", meter);
        Arc::new(Self {
            activity_cancellations: Mutex::new(ExpiringLru::new(
                NonZeroUsize::new(ACTIVITY_CANCEL_CAPACITY).unwrap(),
                ACTIVITY_CANCEL_TTL,
                metrics,
            )),
        })
    }

    pub(super) fn cancel_activity(
        &self,
        token: TaskToken,
        try_cancel: impl FnOnce(&TaskToken) -> bool,
    ) {
        // Keep lookup and buffering atomic with registration to avoid losing cancellation.
        let mut activity_cancellations = self.activity_cancellations.lock();
        if try_cancel(&token) {
            return;
        }
        activity_cancellations.insert(token, ());
    }

    pub(super) fn register_activity(
        &self,
        token: &TaskToken,
        register: impl FnOnce(),
    ) -> ActivityCommandActions {
        let mut activity_cancellations = self.activity_cancellations.lock();
        register();
        ActivityCommandActions {
            cancel: activity_cancellations.remove(token).is_some(),
        }
    }
}

impl temporalio_client::worker::WorkerCommandCache for WorkerCommandCache {
    fn register_activity(&self, token: &TaskToken, register: &mut dyn FnMut()) -> bool {
        self.register_activity(token, register).cancel
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::CounterRecordingMeter;
    use std::sync::Barrier;

    #[tokio::test(start_paused = true)]
    async fn metrics_count_entry_lifecycle_without_duplicate_refreshes() {
        let meter = Arc::new(CounterRecordingMeter::default());
        let cache = WorkerCommandCache::new("ns", Some(meter.clone().into_temporal_meter()));
        cache
            .activity_cancellations
            .lock()
            .entries
            .resize(NonZeroUsize::new(2).unwrap());
        let total = |event| {
            meter.counter_total(
                "worker_command_cache_events",
                &[
                    ("namespace", "ns"),
                    ("command_type", "activity_cancel"),
                    ("event", event),
                ],
            )
        };
        let a: TaskToken = vec![1].into();
        let b: TaskToken = vec![2].into();
        let c: TaskToken = vec![3].into();

        cache.cancel_activity(a.clone(), |_| true);
        assert_eq!(total("buffered"), 0);
        for token in [&a, &b, &a, &c] {
            cache.cancel_activity(token.clone(), |_| false);
        }
        assert_eq!(total("buffered"), 3);
        assert_eq!(total("evicted"), 1);
        assert!(!cache.register_activity(&b, || {}).cancel);
        assert!(cache.register_activity(&a, || {}).cancel);
        assert!(!cache.register_activity(&a, || {}).cancel);

        tokio::time::advance(ACTIVITY_CANCEL_TTL).await;
        assert!(!cache.register_activity(&c, || {}).cancel);
        assert!(!cache.register_activity(&c, || {}).cancel);
        assert_eq!(total("expired"), 1);

        cache.cancel_activity(a.clone(), |_| false);
        cache.cancel_activity(b.clone(), |_| false);
        tokio::time::advance(ACTIVITY_CANCEL_TTL).await;
        cache.cancel_activity(a.clone(), |_| false);
        assert_eq!(total("buffered"), 6);
        assert_eq!(total("expired"), 3);
        assert_eq!(total("evicted"), 1);
        assert!(cache.register_activity(&a, || {}).cancel);
    }

    #[tokio::test(start_paused = true)]
    async fn command_caches_have_independent_capacity_expiry_and_payloads() {
        let meter = Arc::new(CounterRecordingMeter::default());
        let mut cancellations = ExpiringLru::new(
            NonZeroUsize::new(1).unwrap(),
            ACTIVITY_CANCEL_TTL,
            WorkerCommandCacheMetrics::new(
                "ns",
                "activity_cancel",
                Some(meter.clone().into_temporal_meter()),
            ),
        );
        let mut other_commands = ExpiringLru::new(
            NonZeroUsize::new(2).unwrap(),
            Duration::from_secs(120),
            WorkerCommandCacheMetrics::new(
                "ns",
                "other_command",
                Some(meter.clone().into_temporal_meter()),
            ),
        );
        cancellations.insert(1, ());
        other_commands.insert(1, "first payload");
        other_commands.insert(2, "second payload");
        other_commands.insert(3, "third payload");
        assert!(other_commands.remove(&1).is_none());
        assert!(cancellations.remove(&1).is_some());

        cancellations.insert(2, ());
        tokio::time::advance(ACTIVITY_CANCEL_TTL).await;
        assert!(cancellations.remove(&2).is_none());
        assert_eq!(other_commands.remove(&2), Some("second payload"));
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(other_commands.remove(&3).is_none());
        for (command_type, buffered, evicted) in
            [("activity_cancel", 2, 0), ("other_command", 3, 1)]
        {
            for (event, expected) in [("buffered", buffered), ("expired", 1), ("evicted", evicted)]
            {
                assert_eq!(
                    meter.counter_total(
                        "worker_command_cache_events",
                        &[
                            ("namespace", "ns"),
                            ("command_type", command_type),
                            ("event", event),
                        ],
                    ),
                    expected,
                    "{command_type} {event}",
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expires_and_refreshes_duplicate_commands() {
        let cache = WorkerCommandCache::new("ns", None);
        let token: TaskToken = vec![1].into();
        cache.cancel_activity(token.clone(), |_| false);
        tokio::time::advance(Duration::from_secs(59)).await;
        cache.cancel_activity(token.clone(), |_| false);
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(cache.register_activity(&token, || {}).cancel);
        assert!(!cache.register_activity(&token, || {}).cancel);

        cache.cancel_activity(token.clone(), |_| false);
        tokio::time::advance(ACTIVITY_CANCEL_TTL).await;
        assert!(!cache.register_activity(&token, || {}).cancel);
    }

    #[test]
    fn evicts_least_recent_command_and_matches_exact_tokens() {
        let cache = WorkerCommandCache::new("ns", None);
        cache
            .activity_cancellations
            .lock()
            .entries
            .resize(NonZeroUsize::new(2).unwrap());
        let a: TaskToken = vec![1].into();
        let b: TaskToken = vec![2].into();
        let c: TaskToken = vec![3].into();
        for token in [&a, &b, &a, &c] {
            cache.cancel_activity(token.clone(), |_| false);
        }
        assert!(!cache.register_activity(&b, || {}).cancel);
        assert!(cache.register_activity(&a, || {}).cancel);
        assert!(cache.register_activity(&c, || {}).cancel);
        assert!(!cache.register_activity(&vec![4].into(), || {}).cancel);
    }

    #[test]
    fn concurrent_cancellation_and_registration_do_not_lose_handoff() {
        for _ in 0..100 {
            let cache = WorkerCommandCache::new("ns", None);
            let registered = Mutex::new(false);
            let barrier = Barrier::new(2);
            let token: TaskToken = vec![1].into();
            std::thread::scope(|scope| {
                let cancel = scope.spawn(|| {
                    barrier.wait();
                    let mut cancelled = false;
                    cache.cancel_activity(token.clone(), |_| {
                        cancelled = *registered.lock();
                        cancelled
                    });
                    cancelled
                });
                barrier.wait();
                let pending = cache
                    .register_activity(&token, || *registered.lock() = true)
                    .cancel;
                assert_ne!(cancel.join().unwrap(), pending);
                assert!(!cache.register_activity(&token, || {}).cancel);
            });
        }
    }
}
