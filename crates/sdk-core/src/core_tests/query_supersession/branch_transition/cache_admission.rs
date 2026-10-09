use super::*;

#[tokio::test]
async fn worker_preserves_completion_source_when_cache_fills_during_history_fetch() {
    tokio::time::timeout(Duration::from_secs(5), exercise_cache_admission())
        .await
        .unwrap();
}

async fn exercise_cache_admission() {
    let histories::BranchTasks {
        initial,
        losing,
        losing_retry,
        mut winning,
    } = histories::branch_tasks(true);
    let full_history = winning.history.clone();
    let history_start = winning.previous_started_event_id + 1;
    winning
        .history
        .as_mut()
        .unwrap()
        .events
        .retain(|event| event.event_id >= history_start);
    let mut others = Vec::new();
    for index in 0..3 {
        let mut history = TestHistoryBuilder::default();
        history.add_by_type(EventType::WorkflowExecutionStarted);
        history.add_full_wf_task();
        let mut task = hist_to_poll_resp(&history, "other-run", 1.into()).resp;
        task.task_token = format!("other-run-{index}").into_bytes();
        others.push(task);
    }
    let other_runs: Vec<_> = others
        .iter()
        .map(|task| task.workflow_execution.as_ref().unwrap().run_id.clone())
        .collect();
    let other_tokens: Vec<_> = others
        .iter()
        .map(|task| task.task_token.clone().into())
        .collect();
    let run_id = initial.workflow_execution.as_ref().unwrap().run_id.clone();
    assert!(other_runs.iter().all(|other| *other != run_id));
    let (completion_started_tx, completion_started_rx) = oneshot::channel();
    let (release_completion_tx, release_completion_rx) = oneshot::channel();
    let (fetch_started_tx, fetch_started_rx) = oneshot::channel();
    let (release_fetch_tx, release_fetch_rx) = oneshot::channel();
    let mut completion_started_tx = Some(completion_started_tx);
    let mut release_completion_rx = Some(release_completion_rx);
    let mut fetch_started_tx = Some(fetch_started_tx);
    let mut release_fetch_rx = Some(release_fetch_rx);
    let reports = Arc::new(Mutex::new(Vec::new()));
    let recorded_reports = reports.clone();
    let mut client = mock_manual_worker_client();
    client
        .expect_complete_workflow_task()
        .returning(move |completion, _| {
            let token = completion.task_token;
            recorded_reports.lock().unwrap().push(token.clone());
            if token == b"initial-task".to_vec().into() {
                completion_started_tx.take().unwrap().send(()).unwrap();
                let release = release_completion_rx.take().unwrap();
                let response = RespondWorkflowTaskCompletedResponse {
                    workflow_task: Some(winning.clone()),
                    ..Default::default()
                };
                async move {
                    release.await.unwrap();
                    Ok(response)
                }
                .boxed()
            } else if token == b"losing-branch-retry".to_vec().into() {
                assert!(
                    recorded_reports
                        .lock()
                        .unwrap()
                        .contains(&b"winning-branch-task".to_vec().into())
                );
                async { Err(tonic::Status::not_found("losing branch task expired")) }.boxed()
            } else {
                assert!(
                    token == b"winning-branch-task".to_vec().into()
                        || other_tokens.contains(&token)
                );
                async { Ok(Default::default()) }.boxed()
            }
        });
    let failed_reports = reports.clone();
    client
        .expect_fail_workflow_task()
        .returning(move |token, _, _| {
            assert_eq!(token, b"losing-branch-retry".to_vec().into());
            let mut reports = failed_reports.lock().unwrap();
            assert!(reports.contains(&b"winning-branch-task".to_vec().into()));
            reports.push(token);
            async { Err(tonic::Status::not_found("losing branch task expired")) }.boxed()
        });
    client
        .expect_get_workflow_execution_history()
        .times(1)
        .returning(move |_, _, _| {
            fetch_started_tx.take().unwrap().send(()).unwrap();
            let release = release_fetch_rx.take().unwrap();
            let history = full_history.clone();
            async move {
                release.await.unwrap();
                Ok(GetWorkflowExecutionHistoryResponse {
                    history,
                    ..Default::default()
                })
            }
            .boxed()
        });
    let responses = stream::iter([initial])
        .chain(stream::once(async move {
            completion_started_rx.await.unwrap();
            losing
        }))
        .chain(stream::iter([losing_retry]))
        .chain(
            stream::once(async move {
                fetch_started_rx.await.unwrap();
                stream::iter(others)
            })
            .flatten(),
        )
        .chain(stream::pending());
    let slots = Arc::new(ObservedSlots::default());
    let mut mocks = fixtures::mocks_with_slots(client, responses, slots.clone(), 3);
    mocks.worker_cfg(|config| config.ignore_evicts_on_shutdown = false);
    let worker = mock_worker(mocks);
    let initial = worker.poll_workflow_activation().await.unwrap();
    tokio::join!(fixtures::complete_empty(&worker, initial.run_id), async {
        slots.replaced.notified().await;
        worker.request_workflow_eviction(&run_id);
        release_completion_tx.send(()).unwrap();
    },);
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(eviction.run_id, run_id);
    assert!(
        matches!(eviction.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::RemoveFromCache(_))))
    );
    fixtures::complete_empty(&worker, eviction.run_id).await;
    for run in &other_runs {
        let other = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(&other.run_id, run);
        assert!(
            matches!(other.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::InitializeWorkflow(_))))
        );
        fixtures::complete_empty(&worker, other.run_id).await;
    }
    // Three cache slots allow the two retained tasks and a poll for another run.
    // All cache slots are now occupied before the history response can arrive.
    release_fetch_tx.send(()).unwrap();
    let eviction = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(eviction.run_id, other_runs[0]);
    assert!(
        matches!(eviction.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::RemoveFromCache(_))))
    );
    fixtures::complete_empty(&worker, eviction.run_id).await;
    let replay = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(replay.run_id, run_id);
    assert!(
        matches!(replay.jobs.as_slice(), [job] if matches!(job.variant, Some(Variant::InitializeWorkflow(_))))
    );
    fixtures::complete_empty(&worker, replay.run_id).await;
    let winning = worker.poll_workflow_activation().await.unwrap();
    assert!(
        matches!(winning.jobs.as_slice(), [job] if matches!(&job.variant, Some(Variant::SignalWorkflow(signal)) if signal.signal_name == "winning"))
    );
    fixtures::complete_empty(&worker, winning.run_id).await;
    worker.initiate_shutdown();
    fixtures::drain_allowed_activations_until_shutdown(&worker, |job| {
        matches!(
            &job.variant,
            Some(Variant::RemoveFromCache(_)) | Some(Variant::SignalWorkflow(_))
        )
    })
    .await;
    worker.shutdown().await;
    assert_eq!(
        reports.lock().unwrap().as_slice(),
        &[
            b"initial-task".to_vec().into(),
            b"other-run-0".to_vec().into(),
            b"other-run-1".to_vec().into(),
            b"other-run-2".to_vec().into(),
            b"winning-branch-task".to_vec().into(),
            b"losing-branch-retry".to_vec().into(),
        ]
    );
    slots.assert_released_once(6);
}
