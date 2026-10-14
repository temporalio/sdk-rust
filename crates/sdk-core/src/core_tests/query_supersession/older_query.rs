use super::*;
#[tokio::test]
async fn older_query_uses_current_cached_state_without_replaying_history() {
    let (_, replaced, mut query, replacement) = fixtures::task_sequence();
    let mut initial = replaced.clone();
    initial.task_token = b"held-second-task".to_vec();
    query.previous_started_event_id = 0;
    query.history = Some(Default::default());
    let replies = Arc::new(Mutex::new(Vec::new()));
    let recorded_replies = replies.clone();
    let reports = Arc::new(Mutex::new(Vec::new()));
    let recorded_reports = reports.clone();
    let mut client = mock_worker_client();
    client
        .expect_complete_workflow_task()
        .returning(move |completion, _shutdown_token| {
            recorded_reports.lock().unwrap().push(completion.task_token);
            Ok(Default::default())
        });
    client
        .expect_respond_legacy_query()
        .returning(move |token, result| {
            assert!(matches!(result, LegacyQueryResult::Succeeded(_)));
            recorded_replies.lock().unwrap().push(token);
            Ok(Default::default())
        });
    let slots = Arc::new(ObservedSlots::default());
    let worker = mock_worker(fixtures::mocks_with_slots(
        client,
        stream::iter([initial, replaced, query, replacement]),
        slots.clone(),
        10,
    ));
    let replay = worker.poll_workflow_activation().await.unwrap();
    assert!(replay.is_replaying);
    assert!(matches!(
        replay.jobs[0].variant,
        Some(Variant::InitializeWorkflow(_))
    ));
    fixtures::complete_empty(&worker, replay.run_id).await;
    let active = worker.poll_workflow_activation().await.unwrap();
    assert!(!active.is_replaying);
    assert!(
        matches!(active.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::SignalWorkflow(_))))
    );
    let current_clock = active.timestamp;
    tokio::time::timeout(std::time::Duration::from_secs(5), slots.replaced.notified())
        .await
        .unwrap();
    fixtures::complete_empty(&worker, active.run_id).await;
    let query = worker.poll_workflow_activation().await.unwrap();
    assert!(
        matches!(query.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::QueryWorkflow(_))))
    );
    assert_eq!(query.timestamp, current_clock);
    fixtures::answer_query(&worker, query.run_id, "current state").await;
    fixtures::drain_allowed_activations_until_shutdown(&worker, |job| {
        matches!(job.variant, Some(Variant::RemoveFromCache(_)))
    })
    .await;
    worker.shutdown().await;
    assert_eq!(
        replies.lock().unwrap().as_slice(),
        &[b"live-query".to_vec().into()],
        "every consumed Query must receive a response on its original token"
    );
    assert_eq!(
        reports.lock().unwrap().as_slice(),
        &[
            b"held-second-task".to_vec().into(),
            b"replacement-task".to_vec().into(),
        ]
    );
    slots.assert_released_once(4);
}
