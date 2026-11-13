use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        ResponseType, WorkflowCachingPolicy::NonSticky, build_fake_worker, gen_assert_and_reply,
        poll_and_reply, start_timer_cmd,
    },
};
use rstest::rstest;
use std::time::Duration;
use temporalio_common::protos::{
    coresdk::{
        nexus::{NexusOperationCancellationType, NexusOperationResult, nexus_operation_result},
        workflow_activation::{
            ResolveNexusOperation, ResolveNexusOperationStart, WorkflowActivation,
            WorkflowActivationJob, resolve_nexus_operation_start, workflow_activation_job,
        },
        workflow_commands::{
            CompleteWorkflowExecution, RequestCancelNexusOperation, ScheduleNexusOperation,
        },
    },
    temporal::api::{
        enums::v1::EventType,
        failure::v1::{Failure, failure::FailureInfo},
        history::v1::{
            NexusOperationCancelRequestCompletedEventAttributes,
            NexusOperationCancelRequestFailedEventAttributes,
            NexusOperationCancelRequestedEventAttributes, NexusOperationCanceledEventAttributes,
            NexusOperationCompletedEventAttributes, NexusOperationFailedEventAttributes,
            NexusOperationScheduledEventAttributes, NexusOperationStartedEventAttributes,
            NexusOperationTimedOutEventAttributes,
        },
    },
};

const SEQ: u32 = 1;
const TIMER_SEQ: u32 = 1;

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Completed,
    Failed,
    Canceled,
    TimedOut,
}

fn add_outcome(t: &mut TestHistoryBuilder, scheduled_event_id: i64, outcome: Outcome) {
    let failure = Some(Failure {
        message: format!("{outcome:?} after cancellation"),
        ..Default::default()
    });
    match outcome {
        Outcome::Completed => t.add(NexusOperationCompletedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        }),
        Outcome::Failed => t.add(NexusOperationFailedEventAttributes {
            scheduled_event_id,
            failure,
            ..Default::default()
        }),
        Outcome::Canceled => t.add(NexusOperationCanceledEventAttributes {
            scheduled_event_id,
            failure,
            ..Default::default()
        }),
        Outcome::TimedOut => t.add(NexusOperationTimedOutEventAttributes {
            scheduled_event_id,
            failure,
            ..Default::default()
        }),
    };
}

fn add_scheduled(t: &mut TestHistoryBuilder) -> i64 {
    t.add(NexusOperationScheduledEventAttributes {
        endpoint: "endpoint".to_owned(),
        service: "service".to_owned(),
        operation: "operation".to_owned(),
        ..Default::default()
    })
}

fn schedule_cmd(cancellation_type: NexusOperationCancellationType) -> ScheduleNexusOperation {
    ScheduleNexusOperation {
        seq: SEQ,
        endpoint: "endpoint".to_owned(),
        service: "service".to_owned(),
        operation: "operation".to_owned(),
        cancellation_type: cancellation_type as i32,
        ..Default::default()
    }
}

fn response_batches(num_wfts: usize, replay: bool) -> Vec<ResponseType> {
    if replay {
        vec![ResponseType::AllHistory]
    } else {
        (1..=num_wfts).map(ResponseType::ToTaskNum).collect()
    }
}

fn assert_only_job(
    act: &WorkflowActivation,
    check: impl FnOnce(&workflow_activation_job::Variant),
) {
    match act.jobs.as_slice() {
        [WorkflowActivationJob { variant: Some(v) }] => check(v),
        jobs => panic!("expected exactly one job, got {jobs:?}"),
    }
}

fn is_initialize(act: &WorkflowActivation) {
    assert_only_job(act, |v| {
        assert!(
            matches!(v, workflow_activation_job::Variant::InitializeWorkflow(_)),
            "{v:?}"
        )
    });
}

fn is_signal(act: &WorkflowActivation) {
    assert_only_job(act, |v| {
        assert!(
            matches!(v, workflow_activation_job::Variant::SignalWorkflow(_)),
            "{v:?}"
        )
    });
}

fn is_fire_timer(act: &WorkflowActivation) {
    assert_only_job(act, |v| {
        assert!(
            matches!(v, workflow_activation_job::Variant::FireTimer(_)),
            "{v:?}"
        )
    });
}

fn is_async_start(act: &WorkflowActivation) {
    assert_only_job(act, |v| {
        assert!(
            matches!(
                v,
                workflow_activation_job::Variant::ResolveNexusOperationStart(
                    ResolveNexusOperationStart {
                        status: Some(resolve_nexus_operation_start::Status::OperationToken(_)),
                        ..
                    }
                )
            ),
            "{v:?}"
        )
    });
}

fn is_resolved_cancelled(act: &WorkflowActivation) {
    assert_only_job(act, |v| {
        assert!(
            matches!(
                v,
                workflow_activation_job::Variant::ResolveNexusOperation(ResolveNexusOperation {
                    result: Some(NexusOperationResult {
                        status: Some(nexus_operation_result::Status::Cancelled(_))
                    }),
                    ..
                })
            ),
            "{v:?}"
        )
    });
}

fn is_resolved(act: &WorkflowActivation) {
    assert_only_job(act, |v| {
        assert!(
            matches!(
                v,
                workflow_activation_job::Variant::ResolveNexusOperation(_)
            ),
            "{v:?}"
        )
    });
}

// Lang must learn the outcome of a Nexus operation exactly once. Cancellation types that resolve
// lang when cancellation is requested (or, for WaitCancellationRequested, when the request is
// delivered) leave the operation running on the server, so its eventual outcome still lands in
// history and must then be ignored.
#[rstest]
#[tokio::test]
async fn cancel_after_start_resolves_once(
    #[values(
        NexusOperationCancellationType::Abandon,
        NexusOperationCancellationType::TryCancel,
        NexusOperationCancellationType::WaitCancellationRequested,
        NexusOperationCancellationType::WaitCancellationCompleted
    )]
    cancellation_type: NexusOperationCancellationType,
    #[values(
        Outcome::Completed,
        Outcome::Failed,
        Outcome::Canceled,
        Outcome::TimedOut
    )]
    outcome: Outcome,
    #[values(false, true)] replay: bool,
) {
    let sends_cancel_request = cancellation_type != NexusOperationCancellationType::Abandon;

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = add_scheduled(&mut t);
    t.add(NexusOperationStartedEventAttributes {
        scheduled_event_id,
        operation_token: "token".to_owned(),
        ..Default::default()
    });
    t.add_full_wf_task();
    let mut num_wfts = 2;
    if sends_cancel_request {
        t.add(NexusOperationCancelRequestedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        });
    }
    let timer_started_event_id = t.add_timer_started(TIMER_SEQ.to_string());
    if sends_cancel_request {
        t.add(NexusOperationCancelRequestCompletedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        });
        t.add_full_wf_task();
        num_wfts += 1;
    }
    add_outcome(&mut t, scheduled_event_id, outcome);
    t.add_full_wf_task();
    t.add_timer_fired(timer_started_event_id, TIMER_SEQ.to_string());
    t.add_full_wf_task();
    num_wfts += 2;
    t.add_workflow_execution_completed();

    let core = build_fake_worker("fake_wf_id", t, response_batches(num_wfts, replay));

    let mut steps = vec![
        gen_assert_and_reply(&is_initialize, vec![schedule_cmd(cancellation_type).into()]),
        gen_assert_and_reply(
            &is_async_start,
            vec![
                RequestCancelNexusOperation { seq: SEQ }.into(),
                start_timer_cmd(TIMER_SEQ, Duration::from_secs(1)),
            ],
        ),
    ];
    match cancellation_type {
        NexusOperationCancellationType::Abandon
        | NexusOperationCancellationType::TryCancel
        | NexusOperationCancellationType::WaitCancellationRequested => {
            steps.push(gen_assert_and_reply(&is_resolved_cancelled, vec![]));
        }
        NexusOperationCancellationType::WaitCancellationCompleted => {
            steps.push(gen_assert_and_reply(&is_resolved, vec![]));
        }
    }
    steps.push(gen_assert_and_reply(
        &is_fire_timer,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ));
    poll_and_reply(&core, NonSticky, &steps).await;
}

// WaitCancellationRequested resolves lang with a failure when the cancel request fails, but the
// operation keeps running and its eventual outcome must not resolve lang again.
#[rstest]
#[tokio::test]
async fn failed_cancel_request_resolves_once(
    #[values(
        Outcome::Completed,
        Outcome::Failed,
        Outcome::Canceled,
        Outcome::TimedOut
    )]
    outcome: Outcome,
    #[values(false, true)] replay: bool,
) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = add_scheduled(&mut t);
    t.add(NexusOperationStartedEventAttributes {
        scheduled_event_id,
        operation_token: "token".to_owned(),
        ..Default::default()
    });
    t.add_full_wf_task();
    t.add(NexusOperationCancelRequestedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    let timer_started_event_id = t.add_timer_started(TIMER_SEQ.to_string());
    t.add(NexusOperationCancelRequestFailedEventAttributes {
        scheduled_event_id,
        failure: Some(Failure {
            message: "cancel request rejected".to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    });
    t.add_full_wf_task();
    add_outcome(&mut t, scheduled_event_id, outcome);
    t.add_full_wf_task();
    t.add_timer_fired(timer_started_event_id, TIMER_SEQ.to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let core = build_fake_worker("fake_wf_id", t, response_batches(5, replay));

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &is_initialize,
                vec![
                    schedule_cmd(NexusOperationCancellationType::WaitCancellationRequested).into(),
                ],
            ),
            gen_assert_and_reply(
                &is_async_start,
                vec![
                    RequestCancelNexusOperation { seq: SEQ }.into(),
                    start_timer_cmd(TIMER_SEQ, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(&is_resolved_failed, vec![]),
            gen_assert_and_reply(
                &is_fire_timer,
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

fn is_resolved_failed(act: &WorkflowActivation) {
    assert_only_job(act, |v| {
        assert!(
            matches!(
                v,
                workflow_activation_job::Variant::ResolveNexusOperation(ResolveNexusOperation {
                    result: Some(NexusOperationResult {
                        status: Some(nexus_operation_result::Status::Failed(_))
                    }),
                    ..
                })
            ),
            "{v:?}"
        )
    });
}

#[derive(Clone, Copy, Debug)]
enum BeforeStartFollowUp {
    StartedThenCompleted,
    StartedThenCanceled,
    SyncCompleted,
    Failed,
    Canceled,
    TimedOut,
}

// Abandon and TryCancel resolve lang as soon as cancellation is requested. When that happens
// before the operation started, the server still starts (or fails, or times out) the operation, and
// none of that may reach lang a second time.
#[rstest]
#[tokio::test]
async fn cancel_before_start_resolves_once(
    #[values(
        NexusOperationCancellationType::Abandon,
        NexusOperationCancellationType::TryCancel
    )]
    cancellation_type: NexusOperationCancellationType,
    #[values(
        BeforeStartFollowUp::StartedThenCompleted,
        BeforeStartFollowUp::StartedThenCanceled,
        BeforeStartFollowUp::SyncCompleted,
        BeforeStartFollowUp::Failed,
        BeforeStartFollowUp::Canceled,
        BeforeStartFollowUp::TimedOut
    )]
    follow_up: BeforeStartFollowUp,
    #[values(false, true)] replay: bool,
) {
    let sends_cancel_request = cancellation_type != NexusOperationCancellationType::Abandon;

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = add_scheduled(&mut t);
    t.add_we_signaled("cancel", vec![]);
    t.add_full_wf_task();
    if sends_cancel_request {
        t.add(NexusOperationCancelRequestedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        });
    }
    let timer_started_event_id = t.add_timer_started(TIMER_SEQ.to_string());
    let started = |t: &mut TestHistoryBuilder| {
        t.add(NexusOperationStartedEventAttributes {
            scheduled_event_id,
            operation_token: "token".to_owned(),
            ..Default::default()
        });
        // The server only sends a cancel request once the operation has started.
        if sends_cancel_request {
            t.add(NexusOperationCancelRequestCompletedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            });
        }
    };
    match follow_up {
        BeforeStartFollowUp::StartedThenCompleted => {
            started(&mut t);
            add_outcome(&mut t, scheduled_event_id, Outcome::Completed);
        }
        BeforeStartFollowUp::StartedThenCanceled => {
            started(&mut t);
            add_outcome(&mut t, scheduled_event_id, Outcome::Canceled);
        }
        BeforeStartFollowUp::SyncCompleted => {
            add_outcome(&mut t, scheduled_event_id, Outcome::Completed)
        }
        BeforeStartFollowUp::Failed => add_outcome(&mut t, scheduled_event_id, Outcome::Failed),
        BeforeStartFollowUp::Canceled => add_outcome(&mut t, scheduled_event_id, Outcome::Canceled),
        BeforeStartFollowUp::TimedOut => add_outcome(&mut t, scheduled_event_id, Outcome::TimedOut),
    }
    t.add_full_wf_task();
    t.add_timer_fired(timer_started_event_id, TIMER_SEQ.to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let core = build_fake_worker("fake_wf_id", t, response_batches(4, replay));

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(&is_initialize, vec![schedule_cmd(cancellation_type).into()]),
            gen_assert_and_reply(
                &is_signal,
                vec![
                    RequestCancelNexusOperation { seq: SEQ }.into(),
                    start_timer_cmd(TIMER_SEQ, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(&is_cancelled_before_start, vec![]),
            gen_assert_and_reply(
                &is_fire_timer,
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

// The protocol reports an operation cancelled before it started as a failed start, after which no
// `ResolveNexusOperation` follows. Lang SDKs that only resolve their start future from
// `ResolveNexusOperationStart` would otherwise never see the cancellation.
fn is_cancelled_before_start(act: &WorkflowActivation) {
    assert_only_job(act, |v| match v {
        workflow_activation_job::Variant::ResolveNexusOperationStart(
            ResolveNexusOperationStart {
                status: Some(resolve_nexus_operation_start::Status::Failed(failure)),
                ..
            },
        ) => assert!(
            matches!(
                failure.cause.as_deref(),
                Some(Failure {
                    failure_info: Some(FailureInfo::CanceledFailureInfo(_)),
                    ..
                })
            ),
            "{failure:?}"
        ),
        v => panic!("expected a failed start, got {v:?}"),
    });
}
