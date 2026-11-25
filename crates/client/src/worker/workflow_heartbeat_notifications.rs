use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use temporalio_common::protos::{
    TaskToken,
    temporal::api::{
        common::v1::WorkflowExecution,
        worker::v1::{
            WorkflowHeartbeatNotificationCommand, WorkflowHeartbeatNotificationResult,
            workflow_heartbeat_notification_result::Outcome,
        },
    },
};

#[derive(Default)]
/// Accepts notifications without waiting for workflow execution or the completion RPC.
pub struct WorkflowHeartbeatNotifications {
    tasks: Mutex<HashMap<String, Notification>>,
}

struct Notification {
    token: TaskToken,
    workflow_id: String,
    active: Arc<AtomicBool>,
    received: bool,
    notify: Box<dyn Fn() -> bool + Send + Sync>,
}

#[derive(Debug)]
/// Retires routing when its workflow task is destroyed, including on error paths.
pub struct WorkflowHeartbeatRegistration {
    router: Arc<WorkflowHeartbeatNotifications>,
    run_id: String,
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
        execution: WorkflowExecution,
        notify: impl Fn() -> bool + Send + Sync + 'static,
    ) -> WorkflowHeartbeatRegistration {
        let active = Arc::new(AtomicBool::new(true));
        if let Some(old) = self.tasks.lock().insert(
            execution.run_id.clone(),
            Notification {
                token,
                workflow_id: execution.workflow_id,
                active: active.clone(),
                received: false,
                notify: Box::new(notify),
            },
        ) {
            old.active.store(false, Ordering::Release);
        }
        WorkflowHeartbeatRegistration {
            router: self.clone(),
            run_id: execution.run_id,
            active,
        }
    }

    /// Acceptance acknowledges intent, even if normal completion later makes a heartbeat unnecessary.
    pub fn notify(
        &self,
        command: &WorkflowHeartbeatNotificationCommand,
    ) -> WorkflowHeartbeatNotificationResult {
        let mut tasks = self.tasks.lock();
        let mismatch = WorkflowHeartbeatNotificationResult {
            outcome: Outcome::TaskTokenMismatch.into(),
            current_task_token: vec![],
        };
        let Some(execution) = command.execution.as_ref() else {
            return mismatch;
        };
        let Some(task) = tasks.get_mut(&execution.run_id) else {
            return mismatch;
        };
        if task.workflow_id != execution.workflow_id {
            return mismatch;
        }
        if task.token != command.task_token.clone().into() {
            return WorkflowHeartbeatNotificationResult {
                current_task_token: task.token.clone().into_inner(),
                ..mismatch
            };
        }
        if !task.received && !(task.notify)() {
            if let Some(task) = tasks.remove(&execution.run_id) {
                task.active.store(false, Ordering::Release);
            }
            return mismatch;
        }
        task.received = true;
        WorkflowHeartbeatNotificationResult {
            outcome: Outcome::Accepted.into(),
            current_task_token: vec![],
        }
    }

    /// Stop accepting before reporting starts, while the outstanding task still owns its permit.
    pub fn retire(&self, run_id: &str, token: &TaskToken) {
        let mut tasks = self.tasks.lock();
        if tasks.get(run_id).is_some_and(|task| &task.token == token)
            && let Some(task) = tasks.remove(run_id)
        {
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
            .get(&self.run_id)
            .is_some_and(|task| Arc::ptr_eq(&task.active, &self.active))
        {
            tasks.remove(&self.run_id);
        }
        self.active.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, atomic::AtomicUsize};

    fn command(token: u8) -> WorkflowHeartbeatNotificationCommand {
        WorkflowHeartbeatNotificationCommand {
            task_token: vec![token],
            execution: Some(WorkflowExecution {
                workflow_id: "workflow".to_owned(),
                run_id: "run".to_owned(),
            }),
        }
    }

    #[test]
    fn routing_deduplication_and_registration_lifetime() {
        let router = Arc::new(WorkflowHeartbeatNotifications::default());
        let first = command(1);
        let execution = first.execution.clone().unwrap();
        let token: TaskToken = first.task_token.clone().into();
        assert_eq!(router.notify(&first).outcome(), Outcome::TaskTokenMismatch);
        let received = Arc::new(AtomicUsize::new(0));
        let count = received.clone();
        let old = router.register(token.clone(), execution.clone(), move || {
            count.fetch_add(1, Ordering::Relaxed);
            true
        });
        let mismatch = router.notify(&command(2));
        assert_eq!(mismatch.outcome(), Outcome::TaskTokenMismatch);
        assert_eq!(mismatch.current_task_token, vec![1]);
        let mut other_execution = first.clone();
        other_execution.execution.as_mut().unwrap().workflow_id = "other-workflow".to_owned();
        let foreign = router.notify(&other_execution);
        assert_eq!(foreign.outcome(), Outcome::TaskTokenMismatch);
        assert!(foreign.current_task_token.is_empty());
        other_execution = first.clone();
        other_execution.execution.as_mut().unwrap().run_id = "other-run".to_owned();
        assert!(
            router
                .notify(&other_execution)
                .current_task_token
                .is_empty()
        );
        assert_eq!(
            router.notify(&other_execution).outcome(),
            Outcome::TaskTokenMismatch
        );
        other_execution.execution = None;
        assert_eq!(
            router.notify(&other_execution).outcome(),
            Outcome::TaskTokenMismatch
        );
        assert_eq!(router.notify(&first).outcome(), Outcome::Accepted);
        assert_eq!(router.notify(&first).outcome(), Outcome::Accepted);
        assert_eq!(received.load(Ordering::Relaxed), 1);
        let replacement = router.register(vec![2].into(), execution.clone(), || true);
        assert!(!old.is_active());
        router.retire(&execution.run_id, &token);
        drop(old);
        let stale = router.notify(&first);
        assert_eq!(stale.outcome(), Outcome::TaskTokenMismatch);
        assert_eq!(stale.current_task_token, vec![2]);
        assert_eq!(router.notify(&command(2)).outcome(), Outcome::Accepted);
        router.retire(&execution.run_id, &vec![2].into());
        assert!(!replacement.is_active());
        drop(replacement);
        let absent = router.notify(&command(2));
        assert_eq!(absent.outcome(), Outcome::TaskTokenMismatch);
        assert!(absent.current_task_token.is_empty());
        let closed = router.register(token, execution, || false);
        assert_eq!(router.notify(&first).outcome(), Outcome::TaskTokenMismatch);
        assert!(!closed.is_active());
    }

    #[test]
    fn retirement_races_acceptance_without_leaving_a_registration() {
        for _ in 0..100 {
            let router = Arc::new(WorkflowHeartbeatNotifications::default());
            let first = command(1);
            let registration = router.register(
                first.task_token.clone().into(),
                first.execution.clone().unwrap(),
                || true,
            );
            let barrier = Arc::new(Barrier::new(2));
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    barrier.wait();
                    router.notify(&first);
                });
                barrier.wait();
                drop(registration);
            });
            assert_eq!(router.notify(&first).outcome(), Outcome::TaskTokenMismatch);
        }
    }
}
