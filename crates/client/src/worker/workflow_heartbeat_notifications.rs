use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use temporalio_common::protos::{
    TaskToken, temporal::api::worker::v1::workflow_task_heartbeat_notification_result::Outcome,
};

#[derive(Default)]
/// Accepts notifications without waiting for workflow execution or the completion RPC.
pub struct WorkflowHeartbeatNotifications {
    tasks: Mutex<HashMap<TaskToken, Notification>>,
}

struct Notification {
    active: Arc<AtomicBool>,
    received: bool,
    notify: Box<dyn Fn() -> bool + Send + Sync>,
}

#[derive(Debug)]
/// Retires routing when its workflow task is destroyed, including on error paths.
pub struct WorkflowHeartbeatRegistration {
    router: Arc<WorkflowHeartbeatNotifications>,
    token: TaskToken,
    active: Arc<AtomicBool>,
}

impl std::fmt::Debug for WorkflowHeartbeatNotifications {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHeartbeatNotifications")
            .finish_non_exhaustive()
    }
}

impl WorkflowHeartbeatNotifications {
    /// The callback must enqueue without blocking and must not reenter this router.
    pub fn register(
        self: &Arc<Self>,
        token: TaskToken,
        notify: impl Fn() -> bool + Send + Sync + 'static,
    ) -> WorkflowHeartbeatRegistration {
        let active = Arc::new(AtomicBool::new(true));
        if let Some(old) = self.tasks.lock().insert(
            token.clone(),
            Notification {
                active: active.clone(),
                received: false,
                notify: Box::new(notify),
            },
        ) {
            old.active.store(false, Ordering::Release);
        }
        WorkflowHeartbeatRegistration {
            router: self.clone(),
            token,
            active,
        }
    }

    /// Acceptance acknowledges intent, even if normal completion later makes a heartbeat unnecessary.
    pub fn notify(&self, token: &TaskToken) -> Outcome {
        let mut tasks = self.tasks.lock();
        if tasks.is_empty() {
            return Outcome::NoWorkflowTask;
        }
        let Some(task) = tasks.get_mut(token) else {
            return Outcome::TaskTokenMismatch;
        };
        if task.received {
            return Outcome::AlreadyReceived;
        }
        if !(task.notify)() {
            if let Some(task) = tasks.remove(token) {
                task.active.store(false, Ordering::Release);
            }
            return if tasks.is_empty() {
                Outcome::NoWorkflowTask
            } else {
                Outcome::TaskTokenMismatch
            };
        }
        task.received = true;
        Outcome::Handled
    }

    /// Stop accepting before reporting starts, while the outstanding task still owns its permit.
    pub fn retire(&self, token: &TaskToken) {
        if let Some(task) = self.tasks.lock().remove(token) {
            task.active.store(false, Ordering::Release);
        }
    }
}

impl WorkflowHeartbeatRegistration {
    /// Reject queued events after retirement, even before their task is destroyed.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
}

impl Drop for WorkflowHeartbeatRegistration {
    fn drop(&mut self) {
        let mut tasks = self.router.tasks.lock();
        if tasks
            .get(&self.token)
            .is_some_and(|task| Arc::ptr_eq(&task.active, &self.active))
        {
            tasks.remove(&self.token);
        }
        self.active.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, atomic::AtomicUsize};

    #[test]
    fn routing_deduplication_and_registration_lifetime() {
        let router = Arc::new(WorkflowHeartbeatNotifications::default());
        let token: TaskToken = vec![1].into();
        let other: TaskToken = vec![2].into();
        assert_eq!(router.notify(&token), Outcome::NoWorkflowTask);
        let received = Arc::new(AtomicUsize::new(0));
        let count = received.clone();
        let old = router.register(token.clone(), move || {
            count.fetch_add(1, Ordering::Relaxed);
            true
        });
        assert_eq!(router.notify(&other), Outcome::TaskTokenMismatch);
        assert_eq!(router.notify(&token), Outcome::Handled);
        assert_eq!(router.notify(&token), Outcome::AlreadyReceived);
        assert_eq!(received.load(Ordering::Relaxed), 1);
        router.retire(&token);
        assert!(!old.is_active());
        let replacement = router.register(token.clone(), || true);
        drop(old);
        assert_eq!(router.notify(&token), Outcome::Handled);
        drop(replacement);
        assert_eq!(router.notify(&token), Outcome::NoWorkflowTask);
        let closed = router.register(token.clone(), || false);
        assert_eq!(router.notify(&token), Outcome::NoWorkflowTask);
        assert!(!closed.is_active());
    }

    #[test]
    fn retirement_races_acceptance_without_leaving_a_registration() {
        for _ in 0..100 {
            let router = Arc::new(WorkflowHeartbeatNotifications::default());
            let token: TaskToken = vec![1].into();
            let registration = router.register(token.clone(), || true);
            let barrier = Arc::new(Barrier::new(2));
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    barrier.wait();
                    router.notify(&token);
                });
                barrier.wait();
                drop(registration);
            });
            assert_eq!(router.notify(&token), Outcome::NoWorkflowTask);
        }
    }
}
