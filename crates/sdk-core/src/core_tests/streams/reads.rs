//! Direct reads of a Stream, on a test Store. A test adds Records to the Store between its polls.

use super::*;
use crate::{
    test_help::{FakeWfResponses, PollWFTRespExt, schedule_local_activity_cmd},
    worker::{MAX_MARKER_RECORD_BYTES, STREAM_PAGE_MAX_RECORDS},
};
use std::time::SystemTime;
use temporalio_common::protos::coresdk::{
    ActivityTaskCompletion,
    activity_result::ActivityExecutionResult,
    workflow_commands::{
        ActivityCancellationType, CloseStreamRead, StartTimer, UpdateResponse, update_response,
    },
};
use temporalio_streams::ids::MAX_OFFSET_BYTES;

fn stop(read: &ResolveStreamRead) -> StreamReadStop {
    read.end.as_ref().unwrap().stop()
}

/// A read runs through a heartbeat and a task that a timer completes early, and ends with its
/// Stream. Each task records one marker of what Workflow code got in it.
///
/// A test drives a read over a heartbeat like this. History starts with a short Workflow Task
/// timeout, and each task after the first starts with the marker event of the task before. The
/// test adds Records to the Store, polls the page, and completes with no command. It then polls
/// until Core completes the task as a heartbeat at 80 % of the task timeout, and adds the next
/// Records, which come in the next task.
#[tokio::test]
async fn read_spans_three_tasks() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(500));
    t.add_workflow_task_scheduled_and_started();
    let run = served(t, runtimes(&runtime), None, |_| {}, |_| {});
    let (worker, sent) = (run.worker, run.sent);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    complete(&worker, &page, vec![]).await;

    // The heartbeat comes at 400 ms, and completes the first task with the marker of its page.
    let page = poll_after_heartbeat(&worker, &sent, || put(&runtime, "s", &["b", "c"])).await;
    assert_eq!(texts(&reads(&page)[0].records), ["b", "c"]);
    let timer = StartTimer {
        seq: 1,
        start_to_fire_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
    };
    complete(&worker, &page, vec![timer.into()]).await;

    put(&runtime, "s", &["d"]).await;
    close(&runtime, "s").await;
    let mut got = vec![];
    let last = loop {
        let page = worker.poll_workflow_activation().await.unwrap();
        let read = reads(&page).pop().unwrap();
        got.extend(texts(&read.records));
        if read.end.is_some() {
            assert_eq!(stop(&read), StreamReadStop::Done);
            assert!(read.end.as_ref().unwrap().done);
            break page;
        }
        complete(&worker, &page, vec![]).await;
    };
    assert_eq!(got, ["d"]);
    complete(
        &worker,
        &last,
        vec![CompleteWorkflowExecution { result: None }.into()],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;

    let records = all_records(&runtime, "s").await;
    let offset = |at: usize| records[at].offset.clone();
    let sent = sent.lock();
    assert_eq!(sent.len(), 3);
    assert!(sent[0].force_create_new_workflow_task);
    assert!(sent[1].force_create_new_workflow_task);
    assert!(!sent[2].force_create_new_workflow_task);
    assert_eq!(command_types(&sent[0]), [CommandType::RecordMarker]);
    assert_eq!(
        command_types(&sent[1]),
        [CommandType::StartTimer, CommandType::RecordMarker]
    );
    assert_eq!(
        command_types(&sent[2]),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );

    let data = marker(&sent[0]).unwrap();
    let entry = &data.reads[0];
    assert_eq!(data.streams, [stream("s")]);
    assert_eq!((entry.seq, entry.stream_index, entry.count), (1, 0, 1));
    assert!(entry.from_offset.is_empty() && entry.after_offset.is_empty());
    assert_eq!(entry.last_offset, offset(0));
    assert_eq!(entry.digest, records_digest(&records[..1]).to_vec());
    assert!(entry.records.is_empty());
    // The page came in the second activation of the task, after the one that started the read.
    assert_eq!(
        (entry.page_counts.clone(), entry.page_activations.clone()),
        (vec![1], vec![1])
    );
    assert!(entry.end.is_none());

    let entry = &marker(&sent[1]).unwrap().reads[0];
    assert_eq!(
        (entry.after_offset.clone(), entry.last_offset.clone()),
        (offset(0), offset(2))
    );
    assert_eq!(entry.digest, records_digest(&records[1..3]).to_vec());
    assert_eq!(
        (entry.page_counts.clone(), entry.page_activations.clone()),
        (vec![2], vec![0])
    );

    let entry = &marker(&sent[2]).unwrap().reads[0];
    assert_eq!((entry.after_offset.clone(), entry.count), (offset(2), 1));
    let end = entry.end.as_ref().unwrap();
    assert_eq!(
        (end.last_offset.clone(), end.stop(), end.done),
        (offset(3), StreamReadStop::Done, true)
    );
}

/// A Worker with no Local Activities still heartbeats a task that a read holds open.
#[tokio::test]
async fn heartbeat_runs_without_local_activities() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(300));
    t.add_workflow_task_scheduled_and_started();
    let workflow_only = |wc: &mut WorkerConfig| wc.task_types = WorkerTaskTypes::workflow_only();
    let run = served(t, runtimes(&runtime), None, |_| {}, workflow_only);
    let (worker, sent) = (run.worker, run.sent);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &page, vec![]).await;
    let page = poll_after_heartbeat(&worker, &sent, || put(&runtime, "s", &["b"])).await;
    assert_eq!(texts(&reads(&page)[0].records), ["b"]);
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(sent.len(), 2);
    assert!(sent[0].force_create_new_workflow_task);
    assert_eq!(marker(&sent[0]).unwrap().reads[0].count, 1);
    assert_eq!(marker(&sent[1]).unwrap().reads[0].count, 1);
}

/// With its Records in History, a read's marker holds them and no digest.
#[tokio::test]
async fn marker_with_records_in_history_has_no_digest() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b", "c"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        max_records: 2,
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    let read = &reads(&page)[0];
    assert_eq!(texts(&read.records), ["a", "b"]);
    assert_eq!(stop(read), StreamReadStop::MaxRecords);
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    let entry = &marker(&sent[0]).unwrap().reads[0];
    assert_eq!(texts(&entry.records), ["a", "b"]);
    assert!(entry.digest.is_empty());
    assert_eq!(entry.count, 2);
    assert_eq!(
        entry.end.as_ref().unwrap().last_offset,
        read.records[1].offset
    );
}

/// Two reads give their pages in one activation, by seq, and the marker has an entry for each.
#[tokio::test]
async fn two_reads_keep_their_pages_and_activations() {
    let runtime = runtime();
    for id in ["x", "y"] {
        create(&runtime, id).await;
    }
    put(&runtime, "y", &["y1"]).await;
    put(&runtime, "x", &["x1"]).await;
    close(&runtime, "x").await;
    close(&runtime, "y").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![read_cmd(2, "y").into(), read_cmd(1, "x").into()],
    )
    .await;
    // Each read may end in its own activation, but the jobs of one activation go by seq.
    let mut ended = vec![];
    while ended.len() < 2 {
        let page = worker.poll_workflow_activation().await.unwrap();
        let seqs: Vec<_> = reads(&page).iter().map(|read| read.seq).collect();
        let mut sorted = seqs.clone();
        sorted.sort();
        assert_eq!(seqs, sorted);
        for read in reads(&page) {
            assert_eq!(stop(&read), StreamReadStop::Done);
            ended.push((read.seq, texts(&read.records)));
        }
        if ended.len() < 2 {
            complete(&worker, &page, vec![]).await;
        } else {
            worker.complete_execution(&page.run_id).await;
        }
    }
    ended.sort();
    assert_eq!(
        ended,
        [(1, vec!["x1".to_string()]), (2, vec!["y1".to_string()])]
    );
    worker.drain_pollers_and_shutdown().await;

    let data = marker(&sent.lock()[0]).unwrap();
    let mut entries: Vec<_> = data
        .reads
        .iter()
        .map(|entry| {
            let stream = &data.streams[entry.stream_index as usize];
            (entry.seq, stream.id.clone(), entry.count)
        })
        .collect();
    entries.sort();
    assert_eq!(entries, [(1, "x".to_string(), 1), (2, "y".to_string(), 1)]);
}

/// A read next to a Local Activity in one task. The Local Activity marker comes first, and the
/// Stream marker records the activation that got the page.
#[tokio::test]
async fn read_next_to_local_activity_keeps_activations() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let la = schedule_local_activity_cmd(
        1,
        "1",
        ActivityCancellationType::TryCancel,
        Duration::from_secs(60),
    );
    complete(&worker, &first, vec![la, read_cmd(1, "s").into()]).await;
    let task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    put(&runtime, "s", &["a"]).await;
    let (mut la_at, mut page_at) = (None, None);
    let mut activation = 0;
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        activation += 1;
        for job in &next.jobs {
            match &job.variant {
                Some(workflow_activation_job::Variant::ResolveActivity(_)) => {
                    la_at = Some(activation)
                }
                Some(workflow_activation_job::Variant::ResolveStreamRead(_)) => {
                    page_at = Some(activation)
                }
                _ => {}
            }
        }
        if la_at.is_some() && page_at.is_some() {
            worker.complete_execution(&next.run_id).await;
            break;
        }
        complete(&worker, &next, vec![]).await;
    }
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        command_types(&sent[0]),
        [
            CommandType::RecordMarker,
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert!(marker(&sent[0]).is_some());
    let entry = &marker(&sent[0]).unwrap().reads[0];
    assert_eq!(entry.page_activations, [page_at.unwrap()]);
}

/// A command for the server completes the task at once while a read runs. The read goes on, and
/// its next page waits for the next task.
#[tokio::test]
async fn server_command_completes_the_task_while_a_read_runs() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_by_type(EventType::TimerStarted);
    t.add_workflow_task_scheduled_and_started();
    let responses = [1, 2].map(ResponseType::ToTaskNum);
    let (worker, sent) = worker(t, responses, &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let timer = StartTimer {
        seq: 1,
        start_to_fire_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
    };
    complete(&worker, &first, vec![read_cmd(1, "s").into(), timer.into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(command_types(&sent[0]), [CommandType::StartTimer]);
    assert!(sent[0].force_create_new_workflow_task);
    // The page is the first activation of the second task.
    let entry = &marker(&sent[1]).unwrap().reads[0];
    assert_eq!(entry.page_activations, [0]);
}

/// A failed task writes no marker and stops its read. The next attempt reads again from the start.
#[tokio::test]
async fn failed_task_drops_read_results() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let responses = [1, 1].map(ResponseType::ToTaskNum);
    let (worker, sent) = worker(t, responses, &runtime, 1);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::fail(
            page.run_id.clone(),
            Failure {
                message: "Workflow code failed".to_string(),
                ..Default::default()
            },
            None,
        ))
        .await
        .unwrap();
    put(&runtime, "s", &["b"]).await;
    let evict = worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        evict.jobs[0].variant,
        Some(workflow_activation_job::Variant::RemoveFromCache(_))
    ));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evict.run_id))
        .await
        .unwrap();

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a", "b"]);
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(sent.len(), 1);
    let entry = &marker(&sent[0]).unwrap().reads[0];
    assert!(entry.after_offset.is_empty());
    assert_eq!(entry.count, 2);
}

/// A close stops a read. Workflow code gets no result for it, and the marker has no end.
#[tokio::test]
async fn close_stops_the_read() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    // The close stops the read, so the task completes with nothing outstanding.
    complete(
        &worker,
        &page,
        vec![
            CloseStreamRead { seq: 1 }.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    )
    .await;
    put(&runtime, "s", &["b"]).await;
    worker.drain_pollers_and_shutdown().await;

    let entry = &marker(&sent.lock()[0]).unwrap().reads[0];
    assert_eq!(entry.count, 1);
    assert!(entry.end.is_none());
}

/// A read ends at the first of its stops.
#[tokio::test]
async fn read_stops_at_each_cap() {
    let runtime = runtime();
    for id in ["done", "records", "bytes", "idle", "timeout"] {
        create(&runtime, id).await;
    }
    put(&runtime, "done", &["a"]).await;
    close(&runtime, "done").await;
    put(&runtime, "records", &["a", "b", "c"]).await;
    put(&runtime, "bytes", &["a", "b"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let short = Some(Duration::from_millis(200).try_into().unwrap());
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![
            read_cmd(1, "done").into(),
            StartStreamRead {
                max_records: 2,
                ..read_cmd(2, "records")
            }
            .into(),
            // The first Record is over the cap, and the read still takes it.
            StartStreamRead {
                max_bytes: 1,
                ..read_cmd(3, "bytes")
            }
            .into(),
            StartStreamRead {
                idle_timeout: short,
                ..read_cmd(4, "idle")
            }
            .into(),
            StartStreamRead {
                timeout: short,
                ..read_cmd(5, "timeout")
            }
            .into(),
        ],
    )
    .await;
    let mut ends = vec![];
    loop {
        let page = worker.poll_workflow_activation().await.unwrap();
        for read in reads(&page) {
            if read.end.is_some() {
                ends.push((read.seq, stop(&read), texts(&read.records)));
            }
        }
        if ends.len() == 5 {
            worker.complete_execution(&page.run_id).await;
            break;
        }
        complete(&worker, &page, vec![]).await;
    }
    worker.drain_pollers_and_shutdown().await;
    ends.sort_by_key(|end| end.0);
    let owned = |texts: &[&str]| {
        texts
            .iter()
            .map(|text| text.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ends,
        [
            (1, StreamReadStop::Done, owned(&["a"])),
            (2, StreamReadStop::MaxRecords, owned(&["a", "b"])),
            (3, StreamReadStop::MaxBytes, owned(&["a"])),
            (4, StreamReadStop::Idle, owned(&[])),
            (5, StreamReadStop::Timeout, owned(&[])),
        ]
    );
}

/// A call on a Store with no runtime fails the Workflow Task, and writes nothing.
#[tokio::test]
async fn call_on_unknown_store_fails() {
    let runtime = runtime();
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let mut cfg = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    cfg.num_expected_fails = 1;
    cfg.expect_fail_wft_matcher = Box::new(|_, cause, failure| {
        matches!(
            cause,
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        ) && failure.as_ref().is_some_and(|failure| {
            failure.message == "This Worker has no Stream runtime for the Store `elsewhere`"
        })
    });
    let mut mock = build_mock_pollers(cfg);
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime)]);
    mock.worker_cfg(|wc| wc.stream_runtimes = runtimes);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        stream: Some(StreamRef {
            id: "s".to_string(),
            store: "elsewhere".to_string(),
        }),
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    worker.drain_pollers_and_shutdown().await;
}

/// A replay reads the Records that the marker counts from the Store, and gives them in the
/// activation that the marker has.
#[tokio::test]
async fn replay_reads_records_from_the_store() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    close(&runtime, "s").await;
    let records = all_records(&runtime, "s").await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    let read = &reads(&replayed)[0];
    assert_eq!(texts(&read.records), ["a", "b"]);
    assert_eq!(read.records, records);
    assert_eq!(stop(read), StreamReadStop::Done);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&live) && reads(&live).is_empty());
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(marker(&run.sent.lock()[0]).is_none());
}

/// A marker with its Records needs no Store, so a Worker with no runtime replays it.
#[tokio::test]
async fn replay_from_history_calls_no_store() {
    let records: Vec<_> = ["a", "b"]
        .iter()
        .enumerate()
        .map(|(at, text)| StreamRecord {
            producer: Some(client()),
            body: Some(body(text)),
            offset: (at as u64 + 1).to_be_bytes().to_vec(),
        })
        .collect();
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), true);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run = start(
        t,
        [ResponseType::AllHistory],
        StreamRuntimes::default(),
        1,
        0,
    );

    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(reads(&replayed)[0].records, records);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
}

/// Replays a marker of read 1 of Stream `s` on `runtimes`, and returns the message of the failed
/// Workflow Task.
async fn failed_replay(data: StreamMarkerData, runtimes: StreamRuntimes) -> String {
    let t = replayed_read_history(&data);
    // The Records are read after the poll, so the poller must not end before the task fails.
    let run = start_with(t, [ResponseType::AllHistory], runtimes, 1, 1, true);
    // The task fails before any activation, so the poll waits while the failure comes in.
    tokio::select! {
        _ = until("the failed task", || run.failed.lock().len() == 1) => {}
        polled = run.worker.poll_workflow_activation() => {
            panic!("The task should fail before any activation: {polled:?}")
        }
    }
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].0,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
    );
    failed[0].1.clone()
}

/// The Records that a marker counts are gone after the Retention of their closed Stream.
#[tokio::test]
async fn replay_fails_when_records_are_gone() {
    let store = TestStreamStore::with_retention(Duration::from_millis(100));
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    close(&runtime, "s").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let message = failed_replay(data, runtimes(&runtime)).await;
    assert!(
        message.starts_with(&format!("StreamReplayError: {range}: stream_expired")),
        "{message}"
    );
}

#[tokio::test]
async fn replay_fails_on_digest_mismatch() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (mut data, range) = one_page(&all_records(&runtime, "s").await);
    data.reads[0].digest[0] ^= 1;
    let message = failed_replay(data, runtimes(&runtime)).await;
    assert_eq!(
        message,
        format!("StreamReplayError: {range}: the digest of the Records differs")
    );
}

#[tokio::test]
async fn replay_fails_with_no_runtime() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    let message = failed_replay(data, StreamRuntimes::default()).await;
    assert_eq!(
        message,
        format!(
            "StreamReplayError: {range}: this Worker has no Stream runtime for the Store `mem`"
        )
    );
}

/// A read that history leaves open goes on live from its last offset, with what is left of its
/// caps.
#[tokio::test]
async fn open_read_resumes_after_replay() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let records = all_records(&runtime, "s").await;
    put(&runtime, "s", &["c", "d"]).await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        max_records: 3,
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&replayed)[0].records), ["a", "b"]);
    complete(&run.worker, &replayed, vec![]).await;
    let resumed = loop {
        let live = run.worker.poll_workflow_activation().await.unwrap();
        if let Some(read) = reads(&live).pop() {
            break (live, read);
        }
        complete(&run.worker, &live, vec![]).await;
    };
    assert_eq!(texts(&resumed.1.records), ["c"]);
    assert_eq!(stop(&resumed.1), StreamReadStop::MaxRecords);
    run.worker.complete_execution(&resumed.0.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;

    let entry = &marker(&run.sent.lock()[0]).unwrap().reads[0];
    assert_eq!(entry.after_offset, records[1].offset);
    assert_eq!(entry.count, 1);
}

/// A resumed read keeps the deadline of its start, which history dates, so a read that started
/// an hour ago with a timeout of a minute ends at once.
#[tokio::test]
async fn resumed_read_keeps_its_deadline() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let records = all_records(&runtime, "s").await;
    let page = Page {
        records: &records[..1],
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, false);
    let mut t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let an_hour_ago = SystemTime::now() - Duration::from_secs(3600);
    for event_id in 1..=5 {
        t.modify_event(event_id, |event| {
            event.event_time = Some(an_hour_ago.into())
        });
    }
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        timeout: Some(Duration::from_secs(60).try_into().unwrap()),
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&replayed)[0].records), ["a"]);
    complete(&run.worker, &replayed, vec![]).await;
    let ended = loop {
        let live = run.worker.poll_workflow_activation().await.unwrap();
        if let Some(read) = reads(&live).pop() {
            break (live, read);
        }
        complete(&run.worker, &live, vec![]).await;
    };
    assert!(ended.1.records.is_empty());
    assert_eq!(stop(&ended.1), StreamReadStop::Timeout);
    assert_eq!(ended.1.end.as_ref().unwrap().last_offset, records[0].offset);
    run.worker.complete_execution(&ended.0.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
}

/// A close in a replay ends the read, so it doesn't go on live.
#[tokio::test]
async fn close_ends_a_read_in_replay() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let records = all_records(&runtime, "s").await;
    put(&runtime, "s", &["b"]).await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&replayed)[0].records), ["a"]);
    complete(
        &run.worker,
        &replayed,
        vec![CloseStreamRead { seq: 1 }.into()],
    )
    .await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&live) && reads(&live).is_empty());
    // A read that went on would hold the task open, and the completion would wait for it.
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    let sent = run.sent.lock();
    assert_eq!(sent.len(), 1);
    assert!(marker(&sent[0]).is_none());
}

/// A read of another Stream than history has is a non-determinism error.
#[tokio::test]
async fn other_stream_id_is_nondeterministic() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let t = replayed_read_history(&data);
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "other").into()]).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed[0].0, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        failed[0].1.contains("Stream read 1 reads Stream `other`"),
        "{}",
        failed[0].1
    );
}

/// A result in history of a call that Workflow code never makes is a non-determinism error.
#[tokio::test]
async fn entry_with_no_call_is_nondeterministic() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let t = replayed_read_history(&data);
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![]).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed[0].0, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        failed[0]
            .1
            .contains("History has a Stream result of read 1 that Workflow code never got"),
        "{}",
        failed[0].1
    );
}

/// A Worker with no cache replays every earlier Workflow Task, and still runs a read and an
/// append to the end of the Workflow. The append is written once.
#[tokio::test]
async fn worker_with_no_cache_runs_reads_and_appends() {
    let runtime = runtime();
    create(&runtime, "s").await;
    create(&runtime, "out").await;
    put(&runtime, "s", &["a", "b"]).await;
    close(&runtime, "s").await;
    let records = all_records(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer = t.add_timer_started("1".to_string());
    t.add_full_wf_task();
    t.add_timer_started("2".to_string());
    let page = Page {
        records: &records,
        activation: 0,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 0)), false);
    stream_marker_event(
        &mut t,
        8,
        &StreamMarkerData {
            reads: vec![entry],
            appends: vec![],
            closes: vec![],
            streams: vec![stream("s")],
        },
    );
    t.add_timer_fired(timer, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let responses = [1, 2, 3].map(ResponseType::ToTaskNum);
    let run = start(t, responses, runtimes(&runtime), 0, 0);

    let timer = |seq| StartTimer {
        seq,
        start_to_fire_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
    };
    loop {
        let next = run.worker.poll_workflow_activation().await.unwrap();
        let mut commands: Vec<workflow_command::Variant> = vec![];
        let mut done = false;
        for job in &next.jobs {
            match &job.variant {
                Some(workflow_activation_job::Variant::InitializeWorkflow(_)) => {
                    commands.extend([read_cmd(1, "s").into(), timer(1).into()]);
                }
                Some(workflow_activation_job::Variant::ResolveStreamRead(read)) => {
                    assert_eq!(texts(&read.records), ["a", "b"]);
                    commands.push(timer(2).into());
                }
                Some(workflow_activation_job::Variant::FireTimer(_)) => {
                    commands.push(append_cmd(1, "out", &run_id, &["a", "b"]).into());
                }
                Some(workflow_activation_job::Variant::ResolveStreamAppend(_)) => {
                    commands.push(CompleteWorkflowExecution { result: None }.into());
                    done = true;
                }
                _ => {}
            }
        }
        complete(&run.worker, &next, commands).await;
        if done {
            break;
        }
    }
    run.worker.drain_pollers_and_shutdown().await;

    assert_eq!(texts(&all_records(&runtime, "out").await), ["a", "b"]);
    let sent = run.sent.lock();
    assert_eq!(sent.len(), 3);
    assert_eq!(command_types(&sent[0]), [CommandType::StartTimer]);
    assert_eq!(
        command_types(&sent[1]),
        [CommandType::StartTimer, CommandType::RecordMarker]
    );
    assert_eq!(marker(&sent[1]).unwrap().reads[0], {
        let page = Page {
            records: &records,
            activation: 0,
        };
        read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 0)), false)
    });
    assert_eq!(
        command_types(&sent[2]),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
}

/// The state of the Workflow of [replay_matches_live_activations], which lang would keep.
#[derive(Default)]
struct TwoReads {
    appends: u32,
    open_appends: u32,
    ended_reads: u32,
    local_activity_done: bool,
}

impl TwoReads {
    /// The commands for an activation. Each page of a read gets an append of its Records.
    fn activate(
        &mut self,
        activation: &WorkflowActivation,
        run_id: &str,
    ) -> (Vec<workflow_command::Variant>, bool) {
        let mut commands = vec![];
        for job in &activation.jobs {
            match &job.variant {
                Some(workflow_activation_job::Variant::InitializeWorkflow(_)) => {
                    commands.push(schedule_local_activity_cmd(
                        1,
                        "1",
                        ActivityCancellationType::TryCancel,
                        Duration::from_secs(60),
                    ));
                    commands.push(read_cmd(1, "x").into());
                    commands.push(read_cmd(2, "y").into());
                }
                Some(workflow_activation_job::Variant::ResolveStreamRead(read)) => {
                    if !read.records.is_empty() {
                        self.appends += 1;
                        self.open_appends += 1;
                        let texts = texts(&read.records);
                        let texts: Vec<_> = texts.iter().map(String::as_str).collect();
                        commands.push(append_cmd(self.appends, "out", run_id, &texts).into());
                    }
                    if read.end.is_some() {
                        self.ended_reads += 1;
                    }
                }
                Some(workflow_activation_job::Variant::ResolveStreamAppend(_)) => {
                    self.open_appends -= 1;
                }
                Some(workflow_activation_job::Variant::ResolveActivity(_)) => {
                    self.local_activity_done = true;
                }
                _ => {}
            }
        }
        let done = self.ended_reads == 2 && self.open_appends == 0 && self.local_activity_done;
        if done {
            commands.push(CompleteWorkflowExecution { result: None }.into());
        }
        (commands, done)
    }
}

/// A replay gives every page, end and append result in the activation of its first run, next to
/// a Local Activity result, and writes nothing.
#[tokio::test]
async fn replay_matches_live_activations() {
    let runtime = runtime();
    for id in ["x", "y", "out"] {
        create(&runtime, id).await;
    }
    put(&runtime, "x", &["x1", "x2"]).await;
    put(&runtime, "y", &["y1"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let replay = t.clone();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let mut live = vec![];
    let mut workflow = TwoReads::default();
    let first = worker.poll_workflow_activation().await.unwrap();
    let (commands, _) = workflow.activate(&first, &run_id);
    live.push(first.jobs.clone());
    complete(&worker, &first, commands).await;
    let task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    // A second page of `x` comes while the first one is out with Workflow code.
    put(&runtime, "x", &["x3"]).await;
    close(&runtime, "x").await;
    close(&runtime, "y").await;
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        let (commands, done) = workflow.activate(&next, &run_id);
        live.push(next.jobs.clone());
        complete(&worker, &next, commands).await;
        if done {
            break;
        }
    }
    worker.drain_pollers_and_shutdown().await;
    let written = all_records(&runtime, "out").await.len();
    assert_eq!(written, 4);

    let mut replay = replay;
    replay.add_workflow_task_completed();
    let completion = sent.lock()[0].clone();
    for command in completion.commands {
        match command.attributes {
            Some(Attributes::RecordMarkerCommandAttributes(marker)) => {
                replay.add(MarkerRecordedEventAttributes {
                    marker_name: marker.marker_name,
                    details: marker.details,
                    workflow_task_completed_event_id: 4,
                    header: marker.header,
                    failure: marker.failure,
                });
            }
            Some(Attributes::CompleteWorkflowExecutionCommandAttributes(_)) => {
                replay.add_workflow_execution_completed();
            }
            other => panic!("The task sent an unexpected command: {other:?}"),
        }
    }
    let run = start(replay, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);
    let mut replayed = vec![];
    let mut workflow = TwoReads::default();
    loop {
        let next = run.worker.poll_workflow_activation().await.unwrap();
        assert!(next.is_replaying);
        let (commands, done) = workflow.activate(&next, &run_id);
        replayed.push(next.jobs.clone());
        complete(&run.worker, &next, commands).await;
        if done {
            break;
        }
    }
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(replayed, live);
    assert_eq!(all_records(&runtime, "out").await.len(), written);
}

/// Replays a history whose only Workflow Task has a page of read 1 of Stream `s` and completes the
/// Workflow. Workflow code sends `commands` and the completion in its first activation. Returns the
/// cause and message of the failed Workflow Task.
async fn replay_last_task(
    commands: Vec<workflow_command::Variant>,
) -> (WorkflowTaskFailedCause, String) {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    stream_marker_event(&mut t, 4, &data);
    t.add_workflow_execution_completed();
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let mut commands = commands;
    commands.push(CompleteWorkflowExecution { result: None }.into());
    complete(&run.worker, &first, commands).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1, "the replay should fail");
    failed[0].clone()
}

/// A result in history with no call is a non-determinism error, also in the task that completes
/// the Workflow.
#[tokio::test]
async fn a_result_with_no_call_in_the_last_task_is_nondeterministic() {
    let (cause, message) = replay_last_task(vec![]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        message.contains("History has a Stream result of read 1 that Workflow code never got"),
        "{message}"
    );
}

/// A read of another Stream, or from another offset, is a non-determinism error, also in the task
/// that completes the Workflow.
#[tokio::test]
async fn a_changed_read_in_the_last_task_is_nondeterministic() {
    let (cause, message) = replay_last_task(vec![read_cmd(1, "other").into()]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        message.contains("Stream read 1 reads Stream `other`"),
        "{message}"
    );
    let later = StartStreamRead {
        from_offset: 1u64.to_be_bytes().to_vec(),
        ..read_cmd(1, "s")
    };
    let (cause, message) = replay_last_task(vec![later.into()]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(message.contains("or from another offset"), "{message}");
}

/// With non-determinism set to fail the Workflow, a changed read in the last task fails the
/// Workflow. The results that history holds don't fail that forced completion again.
#[tokio::test]
async fn a_changed_read_can_fail_the_workflow() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_completed();
    stream_marker_event(&mut t, 4, &data);
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = runtimes(&runtime);
        wc.workflow_failure_errors = [WorkflowErrorType::Nondeterminism].into();
    });
    let worker = mock_worker(mock);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "other").into()]).await;
    worker.drain_pollers_and_shutdown().await;
    let sent = sent.lock();
    assert_eq!(
        command_types(&sent[0]),
        [CommandType::FailWorkflowExecution]
    );
}

/// A read whose cap falls on the last Record of a closed Stream ends `Done`, so that Workflow code
/// knows that the Stream is finished.
#[tokio::test]
async fn a_cap_on_the_last_record_of_a_closed_stream_ends_done() {
    let runtime = runtime();
    for id in ["records", "bytes"] {
        create(&runtime, id).await;
        put(&runtime, id, &["a", "b"]).await;
        close(&runtime, id).await;
    }
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    let records = StartStreamRead {
        max_records: 2,
        ..read_cmd(1, "records")
    };
    // Room for both Records and not a byte more.
    let both: u64 = all_records(&runtime, "bytes")
        .await
        .iter()
        .map(|record| record.encoded_len() as u64)
        .sum();
    let bytes = StartStreamRead {
        max_bytes: both,
        ..read_cmd(2, "bytes")
    };
    complete(&worker, &first, vec![records.into(), bytes.into()]).await;
    let mut ends = vec![];
    while ends.len() < 2 {
        let next = worker.poll_workflow_activation().await.unwrap();
        for read in reads(&next) {
            if let Some(end) = read.end {
                ends.push((read.seq, end.stop(), end.done));
            }
        }
        if ends.len() == 2 {
            worker.complete_execution(&next.run_id).await;
        } else {
            complete(&worker, &next, vec![]).await;
        }
    }
    worker.drain_pollers_and_shutdown().await;
    ends.sort_by_key(|end| end.0);
    assert_eq!(
        ends,
        [
            (1, StreamReadStop::Done, true),
            (2, StreamReadStop::Done, true)
        ]
    );
}

fn read_with_idle(seq: u32, id: &str, idle: Duration) -> StartStreamRead {
    StartStreamRead {
        idle_timeout: Some(idle.try_into().unwrap()),
        ..read_cmd(seq, id)
    }
}

fn timer_cmd(seq: u32) -> workflow_command::Variant {
    StartTimer {
        seq,
        start_to_fire_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
    }
    .into()
}

/// A read with no idle timeout ends after 1 minute with no Record. On a quiet Stream it holds its
/// task open past a heartbeat, and then ends with `Idle`. Tokio's paused clock moves the minute.
#[tokio::test]
async fn a_read_with_no_idle_timeout_ends_after_a_minute() {
    let waits = Arc::new(AtomicUsize::new(0));
    let store = WatchedStore {
        inner: TestStreamStore::default(),
        gave: Default::default(),
        waits: waits.clone(),
        closes: Default::default(),
    };
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    // The heartbeats come at 40 s and 80 s, so the minute ends between them.
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(50));
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, [1, 2].map(ResponseType::ToTaskNum));
    cfg.make_poll_stream_interminable = true;
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    // With no Local Activity manager, the Stream manager keeps the heartbeat on Tokio's clock.
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.task_types = WorkerTaskTypes::workflow_only();
        wc.stream_runtimes = runtimes(&runtime);
    });
    let worker = mock_worker(mock);
    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        idle_timeout: None,
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    // The read's minute counts on this test's clock from its first wait on the Store.
    until("the read's wait on the Store", || {
        waits.load(Ordering::Acquire) == 1
    })
    .await;
    let ended = {
        let next = worker.poll_workflow_activation();
        tokio::pin!(next);
        tokio::select! {
            _ = async {
                move_clock(Duration::from_secs(40)).await;
                until("the heartbeat", || sent.lock().len() == 1).await;
            } => {}
            early = &mut next => panic!("an activation came before the heartbeat: {early:?}"),
        }
        assert!(sent.lock()[0].force_create_new_workflow_task);
        let ended = tokio::select! {
            ended = tokio::time::timeout(Duration::from_secs(30), &mut next) => ended,
            _ = move_clock(Duration::from_secs(20)) => {
                tokio::time::timeout(Duration::from_secs(30), &mut next).await
            }
        };
        ended
    };
    let ended = ended
        .expect("the read didn't end after its minute")
        .unwrap();
    let read = &reads(&ended)[0];
    assert!(read.records.is_empty());
    assert_eq!(stop(read), StreamReadStop::Idle);
    worker.complete_execution(&ended.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

/// A read whose timeout passed before Core could start it still looks at the Store once, and
/// gives the Records that are there. The history dates its Workflow Task an hour ago.
#[tokio::test]
async fn a_read_past_its_deadline_gives_what_is_there() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(30));
    t.add_workflow_task_scheduled_and_started();
    let an_hour_ago = SystemTime::now() - Duration::from_secs(3600);
    for event_id in 1..=3 {
        t.modify_event(event_id, |event| {
            event.event_time = Some(an_hour_ago.into())
        });
    }
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        timeout: Some(Duration::from_secs(60).try_into().unwrap()),
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    let mut got = vec![];
    let end = loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        let read = reads(&next).pop().unwrap();
        got.extend(texts(&read.records));
        if let Some(end) = read.end.clone() {
            worker.complete_execution(&next.run_id).await;
            break end;
        }
        complete(&worker, &next, vec![]).await;
    };
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(got, ["a", "b"]);
    assert_eq!(end.stop(), StreamReadStop::Timeout);
    assert!(!end.done);
}

/// A read with a zero idle timeout doesn't wait for the Store. On an empty Stream it ends with
/// `Idle` immediately, and on a Stream that has Records it gives them.
#[tokio::test]
async fn a_read_with_a_zero_idle_timeout_gives_what_is_there() {
    let runtime = runtime();
    create(&runtime, "empty").await;
    create(&runtime, "full").await;
    put(&runtime, "full", &["a", "b"]).await;
    for (id, expected) in [("empty", vec![]), ("full", vec!["a", "b"])] {
        let mut t = TestHistoryBuilder::default();
        t.add_wfe_started_with_wft_timeout(Duration::from_secs(30));
        t.add_workflow_task_scheduled_and_started();
        let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
        let first = worker.poll_workflow_activation().await.unwrap();
        complete(
            &worker,
            &first,
            vec![read_with_idle(1, id, Duration::ZERO).into()],
        )
        .await;
        let mut got = vec![];
        let end = loop {
            let next = worker.poll_workflow_activation().await.unwrap();
            let read = reads(&next).pop().unwrap();
            got.extend(texts(&read.records));
            if let Some(end) = read.end.clone() {
                worker.complete_execution(&next.run_id).await;
                break end;
            }
            complete(&worker, &next, vec![]).await;
        };
        worker.drain_pollers_and_shutdown().await;
        assert_eq!(got, expected, "{id}");
        assert_eq!(end.stop(), StreamReadStop::Idle, "{id}");
        assert!(!end.done, "{id}");
    }
}

/// The idle time doesn't count while a read pauses for Workflow code to take its Records. The pause
/// lasts twice the idle time, and the read finds nothing new after it. So it must wait on the
/// Store for its idle time before it ends with `Idle`. A read that counted the pause would end
/// with no wait on the Store.
#[tokio::test]
async fn idle_time_skips_a_paused_read() {
    let gave = Arc::new(Mutex::new(vec![]));
    let waits = Arc::new(AtomicUsize::new(0));
    let store = WatchedStore {
        inner: TestStreamStore::default(),
        gave: gave.clone(),
        waits: waits.clone(),
        closes: Default::default(),
    };
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    t.add_workflow_task_scheduled_and_started();
    let runtimes = runtimes(&runtime).with_read_ahead_bytes(SMALL_READ_AHEAD);
    let worker = start(t, [ResponseType::AllHistory], runtimes, 1, 0).worker;
    let first = worker.poll_workflow_activation().await.unwrap();
    let idle = Duration::from_millis(500);
    complete(&worker, &first, vec![read_with_idle(1, "s", idle).into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    // One Record passes the bound alone. The read takes it and pauses in the same poll on this
    // thread, so it waits for room by the time the Store gave the Record.
    let big = "x".repeat(5 << 10);
    put(&runtime, "s", &[big.as_str()]).await;
    let given = || gave.lock().iter().map(|(_, count)| count).sum::<usize>();
    until("the read of the large Record", || given() == 2).await;
    let before = waits.load(Ordering::Acquire);
    // Workflow code holds the page for at least twice the idle time. Only a lower bound matters,
    // so a slow machine can't fail the test.
    tokio::time::sleep(idle * 2).await;
    complete(&worker, &page, vec![]).await;
    let held = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(reads(&held)[0].records.len(), 1);
    complete(&worker, &held, vec![]).await;
    until("the read's wait on the Store after its pause", || {
        waits.load(Ordering::Acquire) > before
    })
    .await;
    let ended = worker.poll_workflow_activation().await.unwrap();
    let read = &reads(&ended)[0];
    assert!(read.records.is_empty());
    assert_eq!(stop(read), StreamReadStop::Idle);
    worker.complete_execution(&ended.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

/// The idle time doesn't count while a page waits for the next Workflow Task. Records that come
/// in that wait join the page.
#[tokio::test]
async fn idle_time_skips_a_page_that_waits_for_a_task() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_timer_started("1".to_string());
    t.add_workflow_task_scheduled_and_started();
    // The second task comes only after "b" is in, so the page can't go out with "a" alone.
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let next_task = async move {
        released.await.ok();
    }
    .boxed();
    let responses = [
        ResponseType::ToTaskNum(1),
        ResponseType::UntilResolved(next_task, 2),
    ];
    let (worker, sent) = worker(t, responses, &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![
            read_with_idle(1, "s", Duration::from_millis(300)).into(),
            timer_cmd(1),
        ],
    )
    .await;
    put(&runtime, "s", &["a"]).await;
    // Longer than the idle time, which must not end a read whose page waits for a task.
    tokio::time::sleep(Duration::from_millis(700)).await;
    put(&runtime, "s", &["b"]).await;
    release.send(()).unwrap();
    let page = worker.poll_workflow_activation().await.unwrap();
    let read = &reads(&page)[0];
    assert_eq!(texts(&read.records), ["a", "b"]);
    assert!(read.end.is_none(), "the read ended while its page waited");
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(sent.lock()[0].force_create_new_workflow_task);
}

/// No read task outlives its Worker, also one that paused because its run holds its
/// `read_ahead_bytes` that no Workflow Task took.
#[tokio::test]
async fn no_read_outlives_its_worker() {
    let (runtime, served) = counted_runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let runtimes = runtimes(&runtime).with_read_ahead_bytes(SMALL_READ_AHEAD);
    // The mock poller stays alive, or its end would shut the Worker down before the read pauses.
    let run = start_with(t, [ResponseType::AllHistory], runtimes, 1, 0, true);
    let (worker, sent) = (run.worker, run.sent);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![read_with_idle(1, "s", Duration::from_secs(1)).into()],
    )
    .await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    // The timer completes the task immediately, so the read's later pages wait for a task that
    // never comes.
    complete(&worker, &page, vec![timer_cmd(1)]).await;
    assert_eq!(sent.lock().len(), 1);
    let big = "x".repeat(1 << 10);
    for _ in 0..5 {
        put(&runtime, "s", &[big.as_str()]).await;
    }
    // Four Records of 1 KiB pass the bound, so the read pauses once the Store gave them.
    until("the read of four Records", || {
        served.load(Ordering::Acquire) >= 5
    })
    .await;
    worker.drain_pollers_and_shutdown().await;
    // A paused read would hold the runtime for good, so this wait would not end.
    until("the end of every Stream task", || {
        Arc::strong_count(&runtime) == 1
    })
    .await;
}

/// A replay Worker needs no runtime for a read that history leaves open, when its marker holds
/// the Records.
#[tokio::test]
async fn a_replay_worker_needs_no_runtime_for_an_open_read() {
    let records = vec![StreamRecord {
        producer: Some(client()),
        body: Some(body("a")),
        offset: 1u64.to_be_bytes().to_vec(),
    }];
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, true);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let worker = replay_worker(t, StreamRuntimes::default());
    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        jobs.iter().any(|job| job == "ResolveStreamRead(1)"),
        "{jobs:?}"
    );
    assert!(
        jobs.iter().any(|job| job.starts_with("SignalWorkflow")),
        "{jobs:?}"
    );
    assert!(!has_failure_eviction(&jobs), "{jobs:?}");
}

/// A replay Worker still reads the Records of a marker that has none from the Store.
#[tokio::test]
async fn a_replay_worker_reads_a_marker_from_the_store() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let records = all_records(&runtime, "s").await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let worker = replay_worker(t, runtimes(&runtime));
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(reads(&replayed)[0].records, records);
    complete(&worker, &replayed, vec![]).await;
    let _ = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
}

/// A cached run that gets a full history doesn't read the markers that it has already. Here the
/// marker in history names a Store that the Worker has no runtime for, so a read of it would fail
/// the task.
#[tokio::test]
async fn a_cached_run_reads_no_marker_of_a_full_history() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let elsewhere = read_entry(
        1,
        vec![],
        &[Page {
            records: &[StreamRecord {
                offset: 1u64.to_be_bytes().to_vec(),
                ..Default::default()
            }],
            activation: 1,
        }],
        None,
        false,
    );
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(500));
    t.add_full_wf_task();
    stream_marker_event(
        &mut t,
        4,
        &StreamMarkerData {
            reads: vec![elsewhere],
            appends: vec![],
            closes: vec![],
            streams: vec![StreamRef {
                id: "s".to_string(),
                store: "elsewhere".to_string(),
            }],
        },
    );
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    // Both tasks come as a full history, as from the normal queue after a sticky timeout.
    let responses = [1, 2].map(ResponseType::ToTaskNum);
    let run = start(t, responses, runtimes(&runtime), 1, 0);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &page, vec![]).await;
    // The heartbeat completes the first task, and the second one comes.
    let signal = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&signal));
    run.worker.complete_execution(&signal.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(run.failed.lock().is_empty(), "{:?}", run.failed.lock());
    assert_eq!(run.sent.lock().len(), 2);
}

/// Replays the history of a marker of read 1 of Stream `s` on a replay Worker, and returns the
/// message of the eviction that ends it. The replay Worker must end the history, and not feed it
/// again.
async fn replay_worker_eviction(data: StreamMarkerData, runtimes: StreamRuntimes) -> String {
    let worker = replay_worker(replayed_read_history(&data), runtimes);
    let evicted = tokio::time::timeout(Duration::from_secs(60), worker.poll_workflow_activation())
        .await
        .expect("the replay Worker gave no activation")
        .unwrap();
    let Some(workflow_activation_job::Variant::RemoveFromCache(evict)) =
        evicted.jobs.first().and_then(|job| job.variant.clone())
    else {
        panic!("Expected an eviction, got {:?}", evicted.jobs);
    };
    assert_eq!(evicted.jobs.len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evicted.run_id))
        .await
        .unwrap();
    let next = tokio::time::timeout(Duration::from_secs(60), worker.poll_workflow_activation())
        .await
        .expect("the replay Worker didn't end the history");
    assert!(
        next.is_err(),
        "the replay Worker fed the history again: {next:?}"
    );
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    evict.message
}

/// A replay Worker with no runtime for the Store of a marker evicts the run with the
/// `StreamReplayError`, and ends the history.
#[tokio::test]
async fn a_replay_worker_with_no_runtime_ends_the_history() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    let message = replay_worker_eviction(data, StreamRuntimes::default()).await;
    assert!(
        message.contains(&format!(
            "StreamReplayError: {range}: this Worker has no Stream runtime for the Store `mem`"
        )),
        "{message}"
    );
}

/// A replay Worker whose Store lost the Records of a marker evicts the run with the
/// `StreamReplayError`, and ends the history.
#[tokio::test]
async fn a_replay_worker_whose_records_are_gone_ends_the_history() {
    let store = TestStreamStore::with_retention(Duration::from_millis(100));
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    close(&runtime, "s").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let message = replay_worker_eviction(data, runtimes(&runtime)).await;
    assert!(
        message.contains(&format!("StreamReplayError: {range}: stream_expired")),
        "{message}"
    );
}

/// In a replay, a read with no Store name takes the Store of its entry in history, so a replay
/// Worker with no runtime replays a marker that holds its Records.
#[tokio::test]
async fn a_replayed_read_with_no_store_name_takes_the_store_of_history() {
    let records = vec![StreamRecord {
        producer: Some(client()),
        body: Some(body("a")),
        offset: 1u64.to_be_bytes().to_vec(),
    }];
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), true);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let worker = replay_worker(t, StreamRuntimes::default());
    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        stream: unnamed("s"),
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        jobs.iter().any(|job| job == "ResolveStreamRead(1)"),
        "{jobs:?}"
    );
    assert!(
        jobs.iter().any(|job| job.starts_with("SignalWorkflow")),
        "{jobs:?}"
    );
    assert!(!has_failure_eviction(&jobs), "{jobs:?}");
}

/// A replayed call that names another Store than history is still a non-determinism error.
#[tokio::test]
async fn a_replayed_read_of_another_store_is_nondeterministic() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let t = replayed_read_history(&data);
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        stream: Some(StreamRef {
            id: "s".to_string(),
            store: "other".to_string(),
        }),
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed[0].0, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(failed[0].1.contains("in Store `other`"), "{}", failed[0].1);
}

/// A read counts once when Core starts it, and counts each Record that Workflow code got.
#[tokio::test]
async fn a_read_counts_its_start_and_its_records() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    close(&runtime, "s").await;
    let (run, meter) = metered(
        one_task_history(),
        [ResponseType::AllHistory],
        runtimes(&runtime),
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let last = until_resolved(&run.worker, 1, 0).await;
    run.worker.complete_execution(&last.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(
            &meter,
            &[
                "stream_read_total",
                "stream_read_records",
                "stream_read_failed"
            ]
        ),
        [1, 2, 0]
    );
}

/// A replay that reads the Records of a marker from the Store counts the entry and its latency,
/// and counts no read.
#[tokio::test]
async fn a_replay_counts_its_read_of_a_marker_entry() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let (run, meter) = metered(
        replayed_read_history(&data),
        [ResponseType::AllHistory],
        runtimes(&runtime),
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&replayed)[0].records), ["a", "b"]);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    // The read goes on live, which counts as a start, with no Record yet.
    complete(&run.worker, &live, vec![]).await;
    put(&runtime, "s", &["c"]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["c"]);
    run.worker.complete_execution(&page.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(
            &meter,
            &[
                "stream_replay_read_total",
                "stream_replay_read_failed",
                "stream_read_total",
                "stream_read_records",
            ]
        ),
        [1, 0, 1, 1]
    );
    assert_eq!(meter.duration_count("stream_replay_read_latency", MEM), 1);
}

/// A `read_ahead_bytes` that four Records of 1 KiB pass, so that a test pauses a read with little
/// memory.
const SMALL_READ_AHEAD: usize = 4 << 10;

/// A test Store that counts the Records that its reads gave.
struct CountedStore {
    inner: TestStreamStore,
    served: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl StreamStore for CountedStore {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        self.inner.create_stream(request).await
    }

    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        self.inner.append_records(request).await
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        let page = self.inner.read_records(request).await?;
        self.served.fetch_add(page.records.len(), Ordering::AcqRel);
        Ok(page)
    }

    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        self.inner.close_stream(request).await
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        self.inner.describe_stream(request).await
    }
}

/// A runtime on a [CountedStore], and the count of the Records that its reads gave.
fn counted_runtime() -> (Arc<StreamRuntime>, Arc<AtomicUsize>) {
    let served = Arc::new(AtomicUsize::new(0));
    let store = CountedStore {
        inner: TestStreamStore::default(),
        served: served.clone(),
    };
    (StreamRuntime::new(Arc::new(store), None), served)
}

/// A read with a small `read_ahead_bytes` stops asking the Store while that many bytes of its
/// Records wait for Workflow code, and goes on once an activation takes them. Its first page
/// always comes.
#[tokio::test]
async fn a_read_pauses_at_its_read_ahead_bytes() {
    let (runtime, served) = counted_runtime();
    create(&runtime, "s").await;
    // One byte, so that each page is past the bound until an activation takes it.
    let runtimes = runtimes(&runtime).with_read_ahead_bytes(1);
    let run = start_with(
        one_task_history(),
        [ResponseType::AllHistory],
        runtimes,
        1,
        0,
        true,
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    // Workflow code holds the page. The read gives "b" to the run, and then waits, so "c" stays
    // in the Store until an activation takes "b".
    put(&runtime, "s", &["b"]).await;
    until("the read of b", || served.load(Ordering::Acquire) >= 2).await;
    put(&runtime, "s", &["c"]).await;
    assert_eq!(served.load(Ordering::Acquire), 2);
    complete(&run.worker, &page, vec![]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["b"]);
    complete(&run.worker, &page, vec![]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["c"]);
    run.worker.complete_execution(&page.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
}

/// A Worker whose `read_ahead_bytes` is zero is refused when its config is built.
#[test]
fn a_zero_read_ahead_bytes_is_refused() {
    let runtimes = runtimes(&runtime()).with_read_ahead_bytes(0);
    let Err(refused) = test_worker_cfg().stream_runtimes(runtimes).build() else {
        panic!("a zero `read_ahead_bytes` was taken");
    };
    assert_eq!(
        refused,
        "`read_ahead_bytes` of `stream_runtimes` must be > 0"
    );
}

/// A Worker on a [WatchedStore] with Streams `s` and `t`, whose first Workflow Task is out. Its
/// task timeout is long, so that no heartbeat comes in a test.
struct Watching {
    runtime: Arc<StreamRuntime>,
    gave: Arc<Mutex<Vec<(tokio::time::Instant, usize)>>>,
    waits: Arc<AtomicUsize>,
    run: Run,
    first: WorkflowActivation,
}

impl Watching {
    async fn new() -> Self {
        Self::with(|runtimes| runtimes).await
    }

    /// A Worker whose Stream runtimes `settings` gives from the default ones.
    async fn with(settings: impl FnOnce(StreamRuntimes) -> StreamRuntimes) -> Self {
        let gave = Arc::new(Mutex::new(vec![]));
        let waits = Arc::new(AtomicUsize::new(0));
        let store = WatchedStore {
            inner: TestStreamStore::default(),
            gave: gave.clone(),
            waits: waits.clone(),
            closes: Default::default(),
        };
        let runtime = StreamRuntime::new(Arc::new(store), None);
        create(&runtime, "s").await;
        create(&runtime, "t").await;
        let mut t = TestHistoryBuilder::default();
        t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
        t.add_workflow_task_scheduled_and_started();
        let run = start_with(
            t,
            [ResponseType::AllHistory],
            settings(runtimes(&runtime)),
            1,
            0,
            true,
        );
        let first = run.worker.poll_workflow_activation().await.unwrap();
        Self {
            runtime,
            gave,
            waits,
            run,
            first,
        }
    }

    /// The next activation. No heartbeat comes in the bound, so a page that waits for one fails.
    async fn next(&self) -> WorkflowActivation {
        tokio::time::timeout(
            Duration::from_secs(30),
            self.run.worker.poll_workflow_activation(),
        )
        .await
        .expect("no activation came")
        .unwrap()
    }

    /// The Records that the Store gave to reads.
    fn given(&self) -> usize {
        self.gave.lock().iter().map(|(_, count)| count).sum()
    }

    async fn finish(self, last: &WorkflowActivation) {
        self.run.worker.complete_execution(&last.run_id).await;
        self.run.worker.drain_pollers_and_shutdown().await;
    }
}

/// A read whose idle timeout is far past the length of a test.
fn long_read(seq: u32, id: &str) -> StartStreamRead {
    StartStreamRead {
        idle_timeout: Some(Duration::from_secs(3600).try_into().unwrap()),
        ..read_cmd(seq, id)
    }
}

/// Moves Tokio's clock by `by`, so that a long timeout passes with no real wait.
async fn move_clock(by: Duration) {
    tokio::time::pause();
    tokio::time::advance(by).await;
    tokio::time::resume();
}

/// Appends `count` Records to Stream `id`, whose texts are their numbers from `from`.
async fn put_numbered(runtime: &StreamRuntime, id: &str, from: usize, count: usize) {
    let texts: Vec<String> = (from..from + count).map(|at| at.to_string()).collect();
    for batch in texts.chunks(250) {
        let batch: Vec<&str> = batch.iter().map(String::as_str).collect();
        put(runtime, id, &batch).await;
    }
}

fn numbered(from: usize, count: usize) -> Vec<String> {
    (from..from + count).map(|at| at.to_string()).collect()
}

/// A read of 1000 Records that wait in the Store gets them in one Store answer. Workflow code gets
/// them in pages of at most 256, in back to back activations, each Record once and in order.
#[tokio::test]
async fn a_backlog_comes_in_pages_of_at_most_256_records() {
    let w = Watching::new().await;
    put_numbered(&w.runtime, "s", 0, 1000).await;
    complete(&w.run.worker, &w.first, vec![long_read(1, "s").into()]).await;
    let (mut sizes, mut got) = (vec![], vec![]);
    let last = loop {
        let page = w.next().await;
        let read = reads(&page).pop().unwrap();
        sizes.push(read.records.len());
        got.extend(texts(&read.records));
        if got.len() >= 1000 {
            break page;
        }
        complete(&w.run.worker, &page, vec![]).await;
    };
    assert_eq!(STREAM_PAGE_MAX_RECORDS, 256);
    assert_eq!(sizes, [256, 256, 256, 232]);
    assert_eq!(got, numbered(0, 1000));
    // The Records past the first page came from Core and not from a new answer of the Store.
    assert_eq!(w.given(), 1000);
    let sent = w.run.sent.clone();
    w.finish(&last).await;
    // The marker has each page in its own activation, which a replay gives as it is.
    let entry = marker(&sent.lock()[0]).unwrap().reads.remove(0);
    assert_eq!(entry.page_counts, [256, 256, 256, 232]);
    assert_eq!(entry.page_activations, [1, 1, 1, 1]);
}

/// A read whose next activation holds 256 Records takes a later page in full past the cap. Once the
/// Store has nothing new, the Records past the cap still come, in the activation after.
#[tokio::test]
async fn a_capped_read_gives_the_rest_while_the_store_is_quiet() {
    let w = Watching::new().await;
    complete(&w.run.worker, &w.first, vec![long_read(1, "s").into()]).await;
    until("the read's wait on the Store", || {
        w.waits.load(Ordering::Acquire) == 1
    })
    .await;
    put_numbered(&w.runtime, "s", 0, 5).await;
    let first = w.next().await;
    assert_eq!(texts(&reads(&first)[0].records), numbered(0, 5));
    // The pages come while an activation is out. The first fills the next activation, and all of
    // the second is over the cap.
    put_numbered(&w.runtime, "s", 5, 300).await;
    until("the Store's answer of 300", || w.given() == 305).await;
    put_numbered(&w.runtime, "s", 305, 10).await;
    until("the Store's answer of 10", || w.given() == 315).await;
    complete(&w.run.worker, &first, vec![]).await;
    let full = w.next().await;
    assert_eq!(texts(&reads(&full)[0].records), numbered(5, 256));
    complete(&w.run.worker, &full, vec![]).await;
    let rest = w.next().await;
    assert_eq!(texts(&reads(&rest)[0].records), numbered(261, 54));
    assert_eq!(w.given(), 315);
    w.finish(&rest).await;
}

/// The cap holds for each read. Two reads of a run each give up to 256 Records in one activation.
#[tokio::test]
async fn two_reads_each_give_up_to_256_records_in_one_activation() {
    let w = Watching::new().await;
    put_numbered(&w.runtime, "s", 0, 1000).await;
    put_numbered(&w.runtime, "t", 0, 1000).await;
    let reads_both = vec![long_read(1, "s").into(), long_read(2, "t").into()];
    complete(&w.run.worker, &w.first, reads_both).await;
    until("the Store's answers", || w.given() == 2000).await;
    let mut got: HashMap<u32, Vec<String>> = HashMap::new();
    let mut both_full = false;
    let last = loop {
        let page = w.next().await;
        let pages = reads(&page);
        for read in &pages {
            assert!(read.records.len() <= STREAM_PAGE_MAX_RECORDS);
            got.entry(read.seq)
                .or_default()
                .extend(texts(&read.records));
        }
        both_full |= pages.len() == 2 && pages.iter().all(|read| read.records.len() == 256);
        if got.values().map(Vec::len).sum::<usize>() >= 2000 {
            break page;
        }
        complete(&w.run.worker, &page, vec![]).await;
    };
    assert!(both_full, "no activation had 256 Records of each read");
    assert_eq!(got[&1], numbered(0, 1000));
    assert_eq!(got[&2], numbered(0, 1000));
    w.finish(&last).await;
}

/// Replay gives a page of History as it is, also one of more than 256 Records, which a Worker
/// wrote before the cap.
#[tokio::test]
async fn a_replayed_page_over_the_cap_comes_as_recorded() {
    let records: Vec<_> = (0..300u64)
        .map(|at| StreamRecord {
            producer: Some(client()),
            body: Some(body(&at.to_string())),
            offset: (at + 1).to_be_bytes().to_vec(),
        })
        .collect();
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), true);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        streams: vec![stream("s")],
        ..Default::default()
    });
    let run = start(
        t,
        [ResponseType::AllHistory],
        StreamRuntimes::default(),
        1,
        0,
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    let read = reads(&replayed).pop().unwrap();
    assert_eq!(read.records, records);
    assert_eq!(stop(&read), StreamReadStop::Done);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&live));
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(run.failed.lock().is_empty(), "{:?}", run.failed.lock());
}

/// Gives the pages of read 1 until Workflow code got `count` Records, and the last activation.
async fn pages_until(w: &Watching, count: usize) -> (Vec<usize>, Vec<String>, WorkflowActivation) {
    let (mut sizes, mut got) = (vec![], vec![]);
    loop {
        let page = w.next().await;
        let read = reads(&page).pop().unwrap();
        sizes.push(read.records.len());
        got.extend(texts(&read.records));
        if got.len() >= count {
            return (sizes, got, page);
        }
        complete(&w.run.worker, &page, vec![]).await;
    }
}

/// A Worker with a `read_page_records` of 10 gives a backlog of 25 Records in pages of at most 10,
/// from one Store answer, each Record once and in order.
#[tokio::test]
async fn a_page_cap_of_ten_gives_pages_of_at_most_ten() {
    let w = Watching::with(|runtimes| runtimes.with_read_page_records(10)).await;
    put_numbered(&w.runtime, "s", 0, 25).await;
    complete(&w.run.worker, &w.first, vec![long_read(1, "s").into()]).await;
    let (sizes, got, last) = pages_until(&w, 25).await;
    assert_eq!(sizes, [10, 10, 5]);
    assert_eq!(got, numbered(0, 25));
    assert_eq!(w.given(), 25);
    let sent = w.run.sent.clone();
    w.finish(&last).await;
    let entry = marker(&sent.lock()[0]).unwrap().reads.remove(0);
    assert_eq!(entry.page_counts, [10, 10, 5]);
}

/// A Worker whose `read_page_records` is under 1 or over 1000 is refused when its config is built.
#[rstest::rstest]
#[case::zero(0)]
#[case::over_the_bound(1001)]
fn a_page_cap_out_of_its_bounds_is_refused(#[case] records: usize) {
    let runtimes = runtimes(&runtime()).with_read_page_records(records);
    let Err(refused) = test_worker_cfg().stream_runtimes(runtimes).build() else {
        panic!("a `read_page_records` of {records} was taken");
    };
    assert_eq!(
        refused,
        "`read_page_records` of `stream_runtimes` must be 1 to 1000"
    );
}

/// The bounds themselves are taken.
#[test]
fn a_page_cap_of_one_or_1000_is_taken() {
    for records in [1, 1000] {
        let runtimes = runtimes(&runtime()).with_read_page_records(records);
        test_worker_cfg()
            .stream_runtimes(runtimes)
            .build()
            .expect("a `read_page_records` in its bounds was refused");
    }
}

/// The bound of `read_min_gap`.
const GAP: Duration = Duration::from_secs(1);
/// A gap that real time doesn't reach in a test, so that only [move_clock] ends it. The test
/// harness sets the runtimes past the check of the config build, which refuses a gap over [GAP].
const LONG_GAP: Duration = Duration::from_secs(60);

/// A Worker with a gap of [LONG_GAP], whose read 1 of Stream `s` waits on the Store.
async fn gapped_read(settings: impl FnOnce(StreamRuntimes) -> StreamRuntimes) -> Watching {
    let w = Watching::with(|runtimes| settings(runtimes.with_read_min_gap(LONG_GAP))).await;
    complete(&w.run.worker, &w.first, vec![long_read(1, "s").into()]).await;
    until("the read's wait on the Store", || {
        w.waits.load(Ordering::Acquire) == 1
    })
    .await;
    w
}

/// True if the Store gave no Record to a read in the gap after its first answer.
fn no_answer_in_the_gap(w: &Watching) -> bool {
    let gave = w.gave.lock();
    gave.iter()
        .all(|(at, _)| *at == gave[0].0 || *at - gave[0].0 >= LONG_GAP)
}

/// With a gap, a read gives its first Record with no wait, since [Watching::next] gives up long
/// before the gap ends. Single appends that come in the gap after that batch come in one page once
/// the gap passed, so Workflow code gets 2 pages for 10 Records.
#[tokio::test]
async fn a_gap_gives_single_appends_in_fewer_pages() {
    let w = gapped_read(|runtimes| runtimes).await;
    put(&w.runtime, "s", &["0"]).await;
    let first = w.next().await;
    assert_eq!(texts(&reads(&first)[0].records), ["0"]);
    complete(&w.run.worker, &first, vec![]).await;
    let page = {
        let next = w.run.worker.poll_workflow_activation();
        tokio::pin!(next);
        for at in 1..10 {
            put(&w.runtime, "s", &[at.to_string().as_str()]).await;
            // The read runs on this thread, so a short real wait lets it look at the Store. With
            // no gap, each of these Records would make its own page.
            tokio::select! {
                early = &mut next => panic!("a page came in the gap: {early:?}"),
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
        move_clock(LONG_GAP).await;
        tokio::time::timeout(Duration::from_secs(30), &mut next).await
    };
    let last = page.expect("no page came after the gap").unwrap();
    assert_eq!(texts(&reads(&last)[0].records), numbered(1, 9));
    assert_eq!(w.given(), 10);
    assert!(
        no_answer_in_the_gap(&w),
        "the read asked the Store in its gap"
    );
    w.finish(&last).await;
}

/// The end of a read waits at most one gap too. A Stream closed in the gap after a batch ends the
/// read with `Done` once that gap has passed, and not before.
#[tokio::test]
async fn a_gap_holds_the_end_of_a_read_for_at_most_one_gap() {
    let w = gapped_read(|runtimes| runtimes).await;
    put(&w.runtime, "s", &["a"]).await;
    let page = w.next().await;
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    complete(&w.run.worker, &page, vec![]).await;
    close(&w.runtime, "s").await;
    let ended = {
        let next = w.run.worker.poll_workflow_activation();
        tokio::pin!(next);
        tokio::select! {
            early = &mut next => panic!("the end came in the gap: {early:?}"),
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        move_clock(LONG_GAP).await;
        tokio::time::timeout(Duration::from_secs(30), &mut next).await
    };
    let ended = ended.expect("the end didn't come after the gap").unwrap();
    let read = &reads(&ended)[0];
    assert!(read.records.is_empty(), "{:?}", texts(&read.records));
    assert_eq!(stop(read), StreamReadStop::Done);
    w.finish(&ended).await;
}

/// The gap applies to new batches from the Store, and not to Records that the page cap held back.
/// With a cap of 10, the 15 Records of one Store answer after the gap come as 10 and then 5, and
/// the 5 come with no wait for another gap.
#[tokio::test]
async fn records_over_the_cap_come_with_no_wait_for_the_gap() {
    let w = gapped_read(|runtimes| runtimes.with_read_page_records(10)).await;
    put(&w.runtime, "s", &["0"]).await;
    let first = w.next().await;
    assert_eq!(texts(&reads(&first)[0].records), ["0"]);
    for at in 1..16 {
        put(&w.runtime, "s", &[at.to_string().as_str()]).await;
    }
    complete(&w.run.worker, &first, vec![]).await;
    move_clock(LONG_GAP).await;
    let full = w.next().await;
    assert_eq!(texts(&reads(&full)[0].records), numbered(1, 10));
    complete(&w.run.worker, &full, vec![]).await;
    // No clock move, so a page that waited for a gap would not come.
    let rest = w.next().await;
    assert_eq!(texts(&reads(&rest)[0].records), numbered(11, 5));
    assert_eq!(w.given(), 16);
    assert!(
        no_answer_in_the_gap(&w),
        "the read asked the Store in its gap"
    );
    w.finish(&rest).await;
}

/// A Worker whose `read_min_gap` is over 1 s is refused when its config is built, and one of
/// 1 s or zero is taken.
#[test]
fn a_gap_over_one_second_is_refused() {
    let settings = runtimes(&runtime()).with_read_min_gap(GAP + Duration::from_nanos(1));
    let Err(refused) = test_worker_cfg().stream_runtimes(settings).build() else {
        panic!("a `read_min_gap` over 1 s was taken");
    };
    assert_eq!(
        refused,
        "`read_min_gap` of `stream_runtimes` must be at most 1s"
    );
    for gap in [GAP, Duration::ZERO] {
        let settings = runtimes(&runtime()).with_read_min_gap(gap);
        test_worker_cfg()
            .stream_runtimes(settings)
            .build()
            .expect("a `read_min_gap` in its bound was refused");
    }
}

/// Replay gives a page of History as it is, also one over the Worker's `read_page_records`, and
/// with no wait for its `read_min_gap`.
#[tokio::test]
async fn a_replay_ignores_the_read_settings() {
    let records: Vec<_> = (0..30u64)
        .map(|at| StreamRecord {
            producer: Some(client()),
            body: Some(body(&at.to_string())),
            offset: (at + 1).to_be_bytes().to_vec(),
        })
        .collect();
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), true);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        streams: vec![stream("s")],
        ..Default::default()
    });
    let settings = StreamRuntimes::default()
        .with_read_page_records(10)
        .with_read_min_gap(GAP);
    let run = start(t, [ResponseType::AllHistory], settings, 1, 0);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    let read = reads(&replayed).pop().unwrap();
    assert_eq!(read.records, records);
    assert_eq!(stop(&read), StreamReadStop::Done);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&live));
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(run.failed.lock().is_empty(), "{:?}", run.failed.lock());
}

/// A task that a read holds open until its heartbeat counts that hold apart from its duration, so
/// that the duration warning leaves it out.
#[tokio::test]
async fn a_read_holds_its_task_apart_from_the_duration_warning() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(500));
    t.add_workflow_task_scheduled_and_started();
    let run = served(t, runtimes(&runtime), None, |_| {}, |_| {});
    let (worker, sent) = (run.worker, run.sent);
    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &page, vec![]).await;
    let page = poll_after_heartbeat(&worker, &sent, || put(&runtime, "s", &["b"])).await;
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    let tasks = crate::worker::TASK_DURATIONS
        .lock()
        .remove(&run_id)
        .expect("the run recorded its tasks");
    let (duration, held) = tasks[0];
    assert!(held > Duration::ZERO, "the read's hold wasn't counted");
    assert!(held <= duration, "{held:?} of {duration:?}");
}

/// A history in which the first Workflow Task, on this Worker, started read 1 and a timer, and a
/// second one, on another Worker, recorded `marker` if it is given. A signal starts each later
/// task, and the third one is the task that comes back to this Worker.
fn foreign_task_history(marker: Option<&StreamMarkerData>) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    t.add_full_wf_task();
    t.add_timer_started("1".to_string());
    t.add_we_signaled("go", vec![]);
    t.add_full_wf_task();
    if let Some(marker) = marker {
        stream_marker_event(&mut t, 9, marker);
    }
    t.add_we_signaled("again", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// A runtime on a [WatchedStore], and the reads of it that gave Records.
type Watched = (
    Arc<StreamRuntime>,
    Arc<Mutex<Vec<(tokio::time::Instant, usize)>>>,
);

/// A runtime on a [WatchedStore] with Stream `s` and one Record, "a", and the reads that gave
/// Records.
async fn watched_with_a() -> Watched {
    let gave = Arc::new(Mutex::new(vec![]));
    let store = WatchedStore {
        inner: TestStreamStore::default(),
        gave: gave.clone(),
        waits: Arc::new(AtomicUsize::new(0)),
        closes: Default::default(),
    };
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    gave.lock().clear();
    (runtime, gave)
}

/// What Workflow code got from a run that came back to its Worker after another Worker ran a task.
#[derive(Debug, Default)]
struct BackAgain {
    /// Each read page, with whether its activation replayed.
    pages: Vec<(bool, Vec<String>)>,
    evictions: Vec<String>,
}

/// Runs the first task of `t` on a Worker whose read takes the Record in the Store with no task
/// in hand, and then gives it the third task. Workflow code starts read 1 and a timer, and ends
/// the Workflow once the second signal came live and it got one Record.
async fn back_again(
    t: TestHistoryBuilder,
    (runtime, gave): Watched,
    records_in_history: bool,
) -> BackAgain {
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let gate = async move {
        released.await.ok();
    }
    .boxed();
    let responses = [
        ResponseType::ToTaskNum(1),
        ResponseType::UntilResolved(gate, 3),
    ];
    let run = start_with(t, responses, runtimes(&runtime), 1, 0, true);
    let read = StartStreamRead {
        records_in_history,
        idle_timeout: None,
        ..read_cmd(1, "s")
    };
    let mut seen = BackAgain::default();
    let mut gate = Some(release);
    let (mut got, mut again) = (0, false);
    loop {
        let next = tokio::time::timeout(
            Duration::from_secs(30),
            run.worker.poll_workflow_activation(),
        )
        .await
        .unwrap_or_else(|_| panic!("no activation came: {seen:?}"))
        .unwrap();
        let mut first = false;
        for job in next.jobs.iter().filter_map(|job| job.variant.as_ref()) {
            match job {
                workflow_activation_job::Variant::InitializeWorkflow(_) => first = true,
                workflow_activation_job::Variant::RemoveFromCache(evict) => {
                    seen.evictions.push(evict.message.clone());
                }
                workflow_activation_job::Variant::ResolveStreamRead(page) => {
                    got += page.records.len();
                    seen.pages.push((next.is_replaying, texts(&page.records)));
                }
                workflow_activation_job::Variant::SignalWorkflow(signal) => {
                    again |= signal.signal_name == "again" && !next.is_replaying;
                }
                _ => {}
            }
        }
        assert!(
            run.failed.lock().is_empty(),
            "{:?} after {seen:?}",
            run.failed.lock()
        );
        if first {
            complete(&run.worker, &next, vec![read.clone().into(), timer_cmd(1)]).await;
            // The read takes the Record with no task in hand. Its page is in the run by the time
            // the Store gave the Record, since the read sends it in the same poll on this thread.
            if let Some(release) = gate.take() {
                until("the read of the Record", || !gave.lock().is_empty()).await;
                release.send(()).unwrap();
            }
        } else if again && got >= 1 {
            run.worker.complete_execution(&next.run_id).await;
            break;
        } else {
            complete(&run.worker, &next, vec![]).await;
        }
    }
    run.worker.drain_pollers_and_shutdown().await;
    seen
}

/// The marker of a page of `records` that read 1 gave in the second activation of a task.
fn one_page_marker(records: &[StreamRecord], records_in_history: bool) -> StreamMarkerData {
    let page = Page {
        records,
        activation: 1,
    };
    StreamMarkerData {
        reads: vec![read_entry(1, vec![], &[page], None, records_in_history)],
        streams: vec![stream("s")],
        ..Default::default()
    }
}

/// A cached run gets a task with a page that another Worker recorded, with its Records in the
/// Store. Its replay reads them, so Workflow code gets the Record once, in the replayed task.
#[tokio::test]
async fn a_cached_run_replays_a_page_that_another_worker_recorded() {
    let watched = watched_with_a().await;
    let marker = one_page_marker(&all_records(&watched.0, "s").await, false);
    let seen = back_again(foreign_task_history(Some(&marker)), watched, false).await;
    assert_eq!(seen.pages, [(true, vec!["a".to_string()])], "{seen:?}");
}

/// The same with the Records in History, while the cached run's own read took the Record with no
/// task in hand. Workflow code gets it once, and not also from that read.
#[tokio::test]
async fn a_cached_run_drops_its_own_page_for_one_in_history() {
    let watched = watched_with_a().await;
    let marker = one_page_marker(&all_records(&watched.0, "s").await, true);
    let seen = back_again(foreign_task_history(Some(&marker)), watched, true).await;
    assert_eq!(seen.pages, [(true, vec!["a".to_string()])], "{seen:?}");
}

/// Another Worker's task gave read 1 no page. The cached run's own read took the Record with no
/// task in hand, and it must not reach Workflow code in that replayed task, since no marker has
/// it. It comes in a live task instead.
#[tokio::test]
async fn a_cached_run_gives_no_page_that_history_lacks() {
    let seen = back_again(foreign_task_history(None), watched_with_a().await, false).await;
    assert_eq!(seen.pages, [(false, vec!["a".to_string()])], "{seen:?}");
}

/// A graceful shutdown ends a Workflow Task that only a read holds open, also when the server
/// returns the next task inline after each heartbeat, as a real server does. The task completes
/// and forces a new one for the task queue, with no task inline and no sticky queue, so that the
/// Worker that takes it resumes the read from History.
#[tokio::test]
async fn a_shutdown_ends_a_task_that_only_a_read_holds() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(300));
    for _ in 0..400 {
        t.add_full_wf_task();
    }
    t.add_workflow_task_scheduled_and_started();
    let inline = t.clone();
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, [ResponseType::ToTaskNum(1)]);
    cfg.make_poll_stream_interminable = true;
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        let mut kept = kept.lock();
        kept.push(completion.clone());
        // The heartbeat forces a task, which the server returns inline.
        let workflow_task = (completion.force_create_new_workflow_task
            && completion.return_new_workflow_task)
            .then(|| {
                let next = ResponseType::ToTaskNum(kept.len() + 1);
                hist_to_poll_resp(&inline, "fake_wf_id", next).resp
            });
        Ok(RespondWorkflowTaskCompletedResponse {
            workflow_task,
            ..Default::default()
        })
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = runtimes(&runtime);
    });
    let worker = mock_worker(mock);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    // The guard is long next to the heartbeat at 240 ms, so a shutdown that waits on the inline
    // tasks shows here.
    let shutdown =
        tokio::time::timeout(Duration::from_secs(10), worker.drain_pollers_and_shutdown());
    if shutdown.await.is_err() {
        panic!(
            "the shutdown didn't end after 10 s, with {} heartbeat completions",
            sent.lock().len()
        );
    }
    let sent = sent.lock();
    let last = sent.last().unwrap();
    assert!(last.force_create_new_workflow_task && !last.return_new_workflow_task);
    assert!(last.sticky_attributes.is_none());
}

/// The task that a shutdown forced comes back to the same Worker with a Signal, through a poll
/// that was still open while a second Workflow kept the Worker busy. Workflow code starts a second
/// read there, or still waits on the first one. No read starts, the task goes on to another Worker
/// again, and the shutdown ends.
#[rstest::rstest]
#[case::with_a_new_read(true)]
#[case::with_no_command(false)]
#[tokio::test]
async fn a_shutdown_ends_when_its_forced_task_comes_back_to_it(#[case] new_read: bool) {
    let waits = Arc::new(AtomicUsize::new(0));
    let store = WatchedStore {
        inner: TestStreamStore::default(),
        gave: Default::default(),
        waits: waits.clone(),
        closes: Default::default(),
    };
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    let mut reader = TestHistoryBuilder::default();
    reader.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    reader.add_full_wf_task();
    reader.add_we_signaled("back", vec![]);
    reader.add_workflow_task_scheduled_and_started();
    let mut busy = TestHistoryBuilder::default();
    busy.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    busy.add_workflow_task_scheduled_and_started();
    let (forced, comes_back) = tokio::sync::oneshot::channel::<()>();
    let forced = Mutex::new(Some(forced));
    let gate = async move {
        comes_back.await.ok();
    }
    .boxed();
    // The mock poller hands out tasks in the order of the Workflow ids.
    let hists = [
        FakeWfResponses {
            wf_id: "a-busy".to_string(),
            hist: busy,
            response_batches: vec![ResponseType::ToTaskNum(1)],
        },
        FakeWfResponses {
            wf_id: "b-reader".to_string(),
            hist: reader,
            response_batches: vec![
                ResponseType::ToTaskNum(1),
                ResponseType::UntilResolved(gate, 2),
            ],
        },
    ];
    let mut cfg = MockPollCfg::new(hists.into(), true, 0);
    cfg.make_poll_stream_interminable = true;
    cfg.num_expected_completions = Some((0..).into());
    let sent = Arc::new(Mutex::new(vec![]));
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        if completion.resource_id == "workflow:b-reader" {
            kept.lock().push(completion.clone());
        }
        // The forced task reaches the poll of this Worker that waits.
        if completion.force_create_new_workflow_task
            && let Some(forced) = forced.lock().take()
        {
            let _ = forced.send(());
        }
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 2;
        wc.stream_runtimes = runtimes(&runtime);
    });
    let meter = Arc::new(CounterRecordingMeter::default());
    mock.set_temporal_meter(meter.clone().into_temporal_meter());
    let worker = mock_worker(mock);
    let mut busy_first = None;
    for _ in 0..2 {
        let first = worker.poll_workflow_activation().await.unwrap();
        let reads = first.jobs.iter().any(|job| {
            matches!(&job.variant,
                Some(workflow_activation_job::Variant::InitializeWorkflow(init))
                    if init.workflow_id == "b-reader")
        });
        if reads {
            complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
        } else {
            busy_first = Some(first);
        }
    }
    until("the wait of read 1 on the Store", || {
        waits.load(Ordering::Acquire) == 1
    })
    .await;
    worker.initiate_shutdown();
    let back = tokio::time::timeout(Duration::from_secs(10), worker.poll_workflow_activation())
        .await
        .expect("the forced task didn't come back")
        .unwrap();
    assert!(signaled(&back), "{back:?}");
    let commands = if new_read {
        vec![read_cmd(2, "s").into()]
    } else {
        vec![]
    };
    complete(&worker, &back, commands).await;
    // Core completes the task of the reader only while lang polls.
    let busy_ends = async {
        until("the second completion of the reader", || {
            sent.lock().len() == 2
        })
        .await;
        worker
            .complete_execution(&busy_first.expect("the busy run started").run_id)
            .await;
    };
    let drained = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(busy_ends, worker.drain_workflow_poller())
    });
    assert!(drained.await.is_ok(), "the shutdown didn't end");
    worker.shutdown().await;
    let sent = sent.lock();
    assert_eq!(sent.len(), 2, "{sent:?}");
    for completion in sent.iter() {
        assert!(completion.force_create_new_workflow_task);
        assert!(!completion.return_new_workflow_task);
        assert!(completion.sticky_attributes.is_none());
    }
    // Core counts a read when it starts it, so read 2 never started.
    assert_eq!(totals(&meter, &["stream_read_total"]), [1]);
}

/// An Update rejection is a message for the server and no command. With only a read open, the
/// task completes immediately with it, as for a command, and doesn't wait for the heartbeat.
#[tokio::test]
async fn an_update_rejection_next_to_a_read_completes_the_task() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(2));
    t.add_workflow_task_scheduled_and_started();
    let mut task = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::AllHistory).resp;
    task.add_update_request("upd-1", 1);
    let run = start(t, [ResponseType::Raw(task)], runtimes(&runtime), 1, 0);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let rejection = UpdateResponse {
        protocol_instance_id: "upd-1".to_string(),
        response: Some(update_response::Response::Rejected(Default::default())),
    };
    let started = std::time::Instant::now();
    complete(
        &run.worker,
        &first,
        vec![read_cmd(1, "s").into(), rejection.into()],
    )
    .await;
    if run.sent.lock().is_empty() {
        // Core sends the heartbeat only while a poll waits.
        let next = run.worker.poll_workflow_activation();
        tokio::pin!(next);
        tokio::select! {
            _ = until("the rejection", || !run.sent.lock().is_empty()) => {}
            early = &mut next => panic!("an activation came first: {early:?}"),
        }
        panic!(
            "the rejection reached the server after {:?}, at the heartbeat",
            started.elapsed()
        );
    }
    let sent = run.sent.lock()[0].clone();
    assert_eq!(sent.messages.len(), 1);
    assert!(sent.commands.is_empty());
    run.worker.drain_pollers_and_shutdown().await;
}

/// A read with its Records in History keeps the Records of one marker under the bound. The task
/// that reaches it completes with a new one forced, and the read goes on there. The first Record
/// of a task always comes.
#[tokio::test]
async fn a_marker_with_records_in_history_stays_under_its_bound() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let body = "x".repeat(100 << 10);
    for _ in 0..30 {
        put(&runtime, "s", &[body.as_str()]).await;
    }
    close(&runtime, "s").await;
    let records = all_records(&runtime, "s").await;
    let largest = records.iter().map(Message::encoded_len).max().unwrap();
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    t.add_workflow_task_scheduled_and_started();
    let run = served(t, runtimes(&runtime), None, |_| {}, |_| {});
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let mut got = 0;
    let last = loop {
        let next = run.worker.poll_workflow_activation().await.unwrap();
        let read = reads(&next).pop().unwrap();
        got += read.records.len();
        if read.end.is_some() {
            break next;
        }
        complete(&run.worker, &next, vec![]).await;
    };
    run.worker.complete_execution(&last.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(got, records.len());
    let sent = run.sent.lock();
    let sizes: Vec<usize> = sent
        .iter()
        .filter_map(marker)
        .map(|data| data.reads[0].records.iter().map(Message::encoded_len).sum())
        .collect();
    assert!(sizes.len() >= 3, "{sizes:?}");
    assert!(
        sizes
            .iter()
            .all(|size| *size <= MAX_MARKER_RECORD_BYTES + largest),
        "{sizes:?}"
    );
}

/// A read asks the Store for no more bytes than the room left under its `read_ahead_bytes`, so
/// one page can't pass that bound by more than one Record.
#[tokio::test]
async fn a_page_fits_the_room_under_read_ahead_bytes() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let body = "x".repeat(1 << 10);
    put(&runtime, "s", &[body.as_str(); 10]).await;
    let runtimes = runtimes(&runtime).with_read_ahead_bytes(SMALL_READ_AHEAD);
    let run = start(
        one_task_history(),
        [ResponseType::AllHistory],
        runtimes,
        1,
        0,
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    let records = &reads(&page)[0].records;
    let bytes: usize = records.iter().map(Message::encoded_len).sum();
    let largest = records.iter().map(Message::encoded_len).max().unwrap();
    assert!(
        bytes <= SMALL_READ_AHEAD + largest,
        "the first page has {} Records, {bytes} bytes",
        records.len()
    );
    run.worker.complete_execution(&page.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
}

/// A language SDK can send a call that Core can't run as it says. Each such call fails the
/// Workflow Task with one message: a read with a `timeout` of zero or less, and an append or a
/// close whose Producer has another run.
#[tokio::test]
async fn a_call_that_lang_got_wrong_fails_the_task() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let zero = StartStreamRead {
        timeout: Some(Duration::ZERO.try_into().unwrap()),
        ..read_cmd(1, "s")
    };
    let negative = StartStreamRead {
        timeout: Some(prost_types::Duration {
            seconds: -1,
            nanos: 0,
        }),
        ..read_cmd(1, "s")
    };
    let cases: [(workflow_command::Variant, &str); 4] = [
        (
            zero.into(),
            "Stream read 1 has a timeout of zero or less. Leave it unset for no bound",
        ),
        (
            negative.into(),
            "Stream read 1 has a timeout of zero or less. Leave it unset for no bound",
        ),
        (
            append_cmd(1, "s", "another-run", &["a"]).into(),
            "Stream append 1 has the Producer of run `another-run`, but its run is",
        ),
        (
            close_cmd(1, "s", "another-run").into(),
            "Stream close 1 has the Producer of run `another-run`, but its run is",
        ),
    ];
    for (command, expected) in cases {
        let run = start(
            one_task_history(),
            [ResponseType::AllHistory],
            runtimes(&runtime),
            1,
            1,
        );
        let first = run.worker.poll_workflow_activation().await.unwrap();
        complete(&run.worker, &first, vec![command]).await;
        run.worker.drain_pollers_and_shutdown().await;
        let failed = run.failed.lock();
        assert_eq!(failed.len(), 1, "{expected}");
        assert!(failed[0].1.contains(expected), "{}", failed[0].1);
    }
    assert!(all_records(&runtime, "s").await.is_empty());
    assert!(!stream_is_closed(&runtime, "s"));
}

/// A marker of read 1 whose Records the Worker's Store doesn't have, and that Store.
async fn records_gone() -> (StreamMarkerData, Arc<StreamRuntime>) {
    let first = runtime();
    create(&first, "s").await;
    put(&first, "s", &["a", "b"]).await;
    let (data, _) = one_page(&all_records(&first, "s").await);
    let emptied = runtime();
    create(&emptied, "s").await;
    (data, emptied)
}

/// The Workflow failure of a completion, if it has one.
fn workflow_failure(completion: &WorkflowTaskCompletion) -> Option<Failure> {
    completion
        .commands
        .iter()
        .find_map(|command| match &command.attributes {
            Some(Attributes::FailWorkflowExecutionCommandAttributes(attrs)) => {
                attrs.failure.clone()
            }
            _ => None,
        })
}

/// With `StreamReplay` in `workflow_failure_errors`, a replay whose Records are gone fails the
/// Workflow with the text of the task failure, and the type `StreamReplayError`.
#[tokio::test]
async fn a_replay_with_its_records_gone_can_fail_the_workflow() {
    let (data, emptied) = records_gone().await;
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(replayed_read_history(&data), [ResponseType::AllHistory]);
    cfg.make_poll_stream_interminable = true;
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = runtimes(&emptied);
        wc.workflow_failure_errors = [WorkflowErrorType::StreamReplay].into();
    });
    let worker = mock_worker(mock);
    let evicted = worker.poll_workflow_activation().await.unwrap();
    assert!(evicted.is_only_eviction(), "{:?}", evicted.jobs);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evicted.run_id))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
    let sent = sent.lock();
    let failure = workflow_failure(&sent[0]).expect("the Workflow didn't fail");
    assert!(
        failure.message.starts_with("StreamReplayError: "),
        "{}",
        failure.message
    );
    assert!(failure.message.contains("the Store has 0 of the 2 Records"));
    let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
        panic!("the failure has no type");
    };
    assert_eq!(info.r#type, "StreamReplayError");
}

/// A replay Worker with `StreamReplay` in `workflow_failure_errors` reports a replay whose Records
/// are gone as it reports a non-determinism failure: it fails the Workflow, and evicts the run
/// with the same text.
#[tokio::test]
async fn a_replay_worker_fails_the_workflow_for_records_that_are_gone() {
    let (data, emptied) = records_gone().await;
    let sent = Arc::new(Mutex::new(vec![]));
    let kept = sent.clone();
    let mut client = crate::worker::client::mocks::mock_manual_worker_client();
    client
        .expect_complete_workflow_task()
        .returning(move |completion, _| {
            kept.lock().push(completion);
            async { Ok(Default::default()) }.boxed()
        });
    let cfg = test_worker_cfg()
        .stream_runtimes(runtimes(&emptied))
        .workflow_failure_errors([WorkflowErrorType::StreamReplay].into())
        .build()
        .unwrap();
    let mut input = ReplayWorkerInput::new(
        cfg,
        futures_util::stream::iter([HistoryForReplay::from(replayed_read_history(&data))]),
    );
    input.client_override = Some(client);
    let worker = crate::init_replay_worker(input).unwrap();
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        jobs.iter().any(|job| job.starts_with("RemoveFromCache")),
        "{jobs:?}"
    );
    let sent = sent.lock();
    let failure = sent
        .iter()
        .find_map(workflow_failure)
        .expect("the Workflow didn't fail");
    assert!(
        failure.message.starts_with("StreamReplayError: "),
        "{}",
        failure.message
    );
}

/// A read whose Store fails twice gives its Records once the Store answers. An outage in the
/// middle of its long poll makes it go on from its last offset, with no Record twice and none
/// lost.
#[tokio::test]
async fn a_read_retries_a_store_outage() {
    let (runtime, flaky) = flaky_runtime().await;
    put(&runtime, "s", &["a", "b"]).await;
    flaky.fail_reads.store(2, Ordering::Release);
    let (worker, sent) = worker(one_task_history(), [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a", "b"]);
    assert_eq!(flaky.fail_reads.load(Ordering::Acquire), 0);
    complete(&worker, &page, vec![]).await;
    flaky.fail_reads.store(2, Ordering::Release);
    put(&runtime, "s", &["c"]).await;
    let next = worker.poll_workflow_activation().await.unwrap();
    let read = &reads(&next)[0];
    assert_eq!(texts(&read.records), ["c"]);
    assert!(read.end.is_none());
    assert_eq!(flaky.fail_reads.load(Ordering::Acquire), 0);
    complete(
        &worker,
        &next,
        vec![
            CloseStreamRead { seq: 1 }.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;
    let entry = &marker(&sent.lock()[0]).unwrap().reads[0];
    assert_eq!(entry.count, 3);
    assert_eq!(entry.page_counts, [2, 1]);
}

/// A replay whose Store fails once reads the Records on its next try, and gives them.
#[tokio::test]
async fn a_replay_retries_a_store_that_fails_once() {
    let (runtime, flaky) = flaky_runtime().await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    flaky.fail_reads.store(1, Ordering::Release);
    let run = start(
        replayed_read_history(&data),
        [ResponseType::AllHistory],
        runtimes(&runtime),
        1,
        0,
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(texts(&reads(&replayed)[0].records), ["a", "b"]);
    assert_eq!(flaky.fail_reads.load(Ordering::Acquire), 0);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(run.failed.lock().is_empty());
}

/// A shorter bound of a replay's retry. A replay reads the Store on the Worker's own thread, whose
/// clock a test can't pause.
const SHORT_REPLAY_BOUND: Duration = Duration::from_millis(500);

/// A replay whose Store stays down past the bound fails only its task, with
/// `StreamReplayUnavailable`, also on a Worker that fails the Workflow for a `StreamReplayError`.
#[tokio::test]
async fn a_replay_whose_store_stays_down_fails_only_the_task() {
    let (runtime, flaky) = flaky_runtime().await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    flaky.fail_reads.store(usize::MAX, Ordering::Release);
    let run = start_configured(
        replayed_read_history(&data),
        [ResponseType::AllHistory],
        runtimes(&runtime),
        (1, 1, true),
        None,
        |wc| {
            wc.workflow_failure_errors = [WorkflowErrorType::StreamReplay].into();
            wc.stream_test_hooks = StreamTestHooks {
                replay_retry_bound: Some(SHORT_REPLAY_BOUND),
                ..Default::default()
            };
        },
    );
    let started = tokio::time::Instant::now();
    tokio::select! {
        _ = until("the failed task", || run.failed.lock().len() == 1) => {}
        polled = run.worker.poll_workflow_activation() => {
            panic!("The task should fail before any activation: {polled:?}")
        }
    }
    assert!(
        started.elapsed() >= SHORT_REPLAY_BOUND,
        "{:?}",
        started.elapsed()
    );
    // A try after each wait of 100, 200 and 200 ms, and one before them.
    assert!(flaky.fail_reads.load(Ordering::Acquire) <= usize::MAX - 4);
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].0,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
    );
    assert!(
        failed[0].1.starts_with(&format!(
            "StreamReplayUnavailable: {range}: stream_storage_unavailable"
        )),
        "{}",
        failed[0].1
    );
    assert!(
        run.sent
            .lock()
            .iter()
            .all(|sent| workflow_failure(sent).is_none()),
        "the Workflow failed"
    );
}

/// A replay Worker whose Store stays down evicts the run with `StreamReplayUnavailable` and ends
/// the history, and doesn't fail the Workflow for it.
#[tokio::test]
async fn a_replay_worker_whose_store_stays_down_ends_the_history() {
    let (runtime, flaky) = flaky_runtime().await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    flaky.fail_reads.store(usize::MAX, Ordering::Release);
    let sent = Arc::new(Mutex::new(vec![]));
    let kept = sent.clone();
    let mut client = crate::worker::client::mocks::mock_manual_worker_client();
    client
        .expect_complete_workflow_task()
        .returning(move |completion, _| {
            kept.lock().push(completion);
            async { Ok(Default::default()) }.boxed()
        });
    client
        .expect_fail_workflow_task()
        .returning(|_, _, _| async { Ok(Default::default()) }.boxed());
    let mut cfg = test_worker_cfg()
        .stream_runtimes(runtimes(&runtime))
        .workflow_failure_errors([WorkflowErrorType::StreamReplay].into())
        .build()
        .unwrap();
    cfg.stream_test_hooks = StreamTestHooks {
        replay_retry_bound: Some(SHORT_REPLAY_BOUND),
        ..Default::default()
    };
    let mut input = ReplayWorkerInput::new(
        cfg,
        futures_util::stream::iter([HistoryForReplay::from(replayed_read_history(&data))]),
    );
    input.client_override = Some(client);
    let worker = crate::init_replay_worker(input).unwrap();
    let evicted = worker.poll_workflow_activation().await.unwrap();
    let Some(workflow_activation_job::Variant::RemoveFromCache(evict)) =
        evicted.jobs.first().and_then(|job| job.variant.clone())
    else {
        panic!("Expected an eviction, got {:?}", evicted.jobs);
    };
    assert!(
        evict.message.contains(&format!(
            "StreamReplayUnavailable: {range}: stream_storage_unavailable"
        )),
        "{}",
        evict.message
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evicted.run_id))
        .await
        .unwrap();
    let next = worker.poll_workflow_activation().await;
    assert!(
        next.is_err(),
        "the replay Worker fed the history again: {next:?}"
    );
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        sent.lock()
            .iter()
            .all(|sent| workflow_failure(sent).is_none())
    );
}

/// A read from an offset over its limit is refused, and its end gives back that offset.
#[tokio::test]
async fn a_from_offset_over_its_limit_is_refused() {
    let long = vec![7; MAX_OFFSET_BYTES + 1];
    let (refused, sent, calls) = refused_calls(|_| {
        vec![
            StartStreamRead {
                from_offset: long.clone(),
                ..read_cmd(1, "s")
            }
            .into(),
        ]
    })
    .await;
    let end = reads(&refused).pop().unwrap().end.unwrap();
    assert_eq!(end.last_offset, long);
    assert_refused(
        end.failure.as_ref(),
        "The offset `from_offset`",
        MAX_OFFSET_BYTES,
    );
    assert!(marker(&sent).is_none(), "{:?}", marker(&sent));
    assert_eq!(calls, [0, 0, 0]);
}

/// A replay refuses the same read again, so a History with no marker of the refusal replays.
#[tokio::test]
async fn a_replay_refuses_a_read_over_its_limit_again() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let run = start(
        t,
        [ResponseType::AllHistory],
        StreamRuntimes::default(),
        1,
        0,
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        from_offset: vec![7; MAX_OFFSET_BYTES + 1],
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let refused = run.worker.poll_workflow_activation().await.unwrap();
    assert!(refused.is_replaying);
    let end = reads(&refused).pop().unwrap().end.unwrap();
    assert_refused(
        end.failure.as_ref(),
        "The offset `from_offset`",
        MAX_OFFSET_BYTES,
    );
    run.worker.complete_execution(&refused.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(run.failed.lock().is_empty(), "{:?}", run.failed.lock());
}
