//! The Store suite. Every Store runs it, so that each one keeps the same rules.
//!
//! A case says its guarantee and what it needs from a Store in its comment.

use super::{
    case, fields,
    support::{Needs, Run, Target, digest, record, records_of, stores_for},
};
use crate::{
    Reason, StreamResult,
    epoch::{ProducerKind, epoch_key},
    memory::{EntryId, ProducerField},
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{ActivityEpoch, ActivityProducer, ClientProducer, Producer, producer::Kind},
    store::{Batch, ConflictPolicy, Create, CreateOutcome, Created},
};
use futures_util::future::join_all;
use prost_types::Timestamp;
use std::time::Duration;

fn activity(run: &str, attempt: i32, scheduled: i64) -> Producer {
    Producer {
        id: format!("act:{run}:a:s"),
        kind: Some(Kind::WorkflowActivity(ActivityProducer {
            run_id: run.to_string(),
            epoch: Some(ActivityEpoch {
                attempt_scheduled_time: Some(Timestamp {
                    seconds: scheduled,
                    nanos: 0,
                }),
                attempt,
            }),
        })),
    }
}

fn client() -> Producer {
    Producer {
        id: "client:me:p1".to_string(),
        kind: Some(Kind::Client(ClientProducer {})),
    }
}

/// A second client Producer, which every Store accepts as sent.
fn other_client() -> Producer {
    Producer {
        id: "client:other:p2".to_string(),
        ..client()
    }
}

async fn append(
    target: &Target,
    producer: &Producer,
    sequence: i64,
    texts: &[&str],
) -> Result<crate::store::Appended, crate::StreamError> {
    let records: Vec<_> = texts.iter().map(|text| record(producer, text)).collect();
    target
        .store
        .append(
            &target.stream_ref(),
            &Batch {
                producer,
                sequence,
                records: &records,
                digest: digest(texts),
            },
        )
        .await
}

/// The create of a Stream that uses an existing one.
fn use_existing() -> Create<'static> {
    Create {
        conflict: ConflictPolicy::UseExisting,
        ..Default::default()
    }
}

/// A create writes the key once. A create with another Retention finds the Stream and keeps the
/// stored Retention. A create with no request id keeps none.
async fn a_create_writes_the_key_once_and_keeps_its_retention() {
    for target in stores_for(Needs::STORE_ANSWERS).await {
        assert_eq!(
            append(&target, &client(), 1, &["x"])
                .await
                .unwrap_err()
                .reason,
            Reason::NotFound
        );
        let day = Create {
            retention: Some(Duration::from_secs(86_400)),
            ..use_existing()
        };
        let created = target.create_with(&day).await.unwrap();
        assert_eq!(created.outcome, CreateOutcome::Created);
        let again = target.create_with(&day).await.unwrap();
        assert_eq!(again.outcome, CreateOutcome::Exists);
        let other = target.create_with(&use_existing()).await.unwrap();
        assert_eq!(other.outcome, CreateOutcome::Exists);
        assert_eq!(other.retention, Duration::from_secs(86_400));
        if target.gives.gives(Needs::RAW_KEYS) {
            assert_eq!(target.meta("rid").await, None);
        }
    }
}

/// A create of an open Stream fails as `already_exists`, unless it uses the Stream or it is a
/// retry of the create that made the Stream. Such a retry has the same request id, and it succeeds
/// as a create with either policy. The Store keeps that request id as `rid`.
async fn a_create_of_an_open_stream_fails_unless_it_uses_it_or_retries() {
    for target in stores_for(Needs::CALLS).await {
        let first = Create {
            request_id: "create-1",
            ..Default::default()
        };
        let outcome = |created: StreamResult<Created>| created.unwrap().outcome;
        let reason = |created: StreamResult<Created>| created.unwrap_err().reason;
        assert_eq!(
            outcome(target.create_with(&first).await),
            CreateOutcome::Created
        );
        let other = Create {
            request_id: "create-2",
            ..Default::default()
        };
        for create in [&other, &Create::default()] {
            assert_eq!(
                reason(target.create_with(create).await),
                Reason::AlreadyExists
            );
        }
        let used = Create {
            request_id: "create-2",
            ..use_existing()
        };
        assert_eq!(
            outcome(target.create_with(&used).await),
            CreateOutcome::Exists
        );
        let retry = Create {
            request_id: "create-1",
            ..use_existing()
        };
        for create in [&first, &retry] {
            assert_eq!(
                outcome(target.create_with(create).await),
                CreateOutcome::Created
            );
        }
        if target.gives.gives(Needs::RAW_KEYS) {
            assert_eq!(target.meta("rid").await.as_deref(), Some(&b"create-1"[..]));
        }
    }
}

/// A create of an expired or a closed Stream fails as `expired` or `closed`, in this order. These
/// come before the conflict policy and before a retry of the create that made the Stream.
async fn closed_and_expired_come_before_already_exists() {
    for target in stores_for(Needs::RAW_KEYS).await {
        let first = Create {
            request_id: "create-1",
            ..Default::default()
        };
        target.create_with(&first).await.unwrap();
        let reason = |created: StreamResult<Created>| created.unwrap_err().reason;
        target.close(&client()).await.unwrap();
        for create in [&first, &Create::default()] {
            assert_eq!(reason(target.create_with(create).await), Reason::Closed);
        }
        target.set_meta("exp", "1").await;
        assert_eq!(reason(target.create_with(&first).await), Reason::Expired);
    }
}

/// A resend with the last sequence gets the offsets of its first result, even after another
/// Producer wrote, and a lower sequence is refused.
async fn a_resend_gets_its_first_positions_and_a_lower_sequence_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create().await;
        let p = client();
        let first = append(&target, &p, 1, &["p1", "p2"]).await.unwrap();
        // Another Producer writes between the batch and its resend.
        append(&target, &other_client(), 1, &["q1"]).await.unwrap();
        let resend = append(&target, &p, 1, &["p1", "p2"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        if target.gives.gives(Needs::STORE_ANSWERS) {
            assert!(!first.resend && resend.resend);
        }
        append(&target, &p, 2, &["p3"]).await.unwrap();
        assert_eq!(
            append(&target, &p, 1, &["p1"]).await.unwrap_err().reason,
            Reason::SequenceTooLow
        );
        let texts = target.texts().await;
        assert_eq!(texts, ["p1", "p2", "q1", "p3"]);
    }
}

/// A Producer id is bytes. An id with the field separator and non-ASCII text keeps its own dedupe
/// state, apart from an id that it starts with.
async fn a_producer_id_is_bytes_with_its_own_state() {
    for target in stores_for(Needs::CALLS).await {
        target.create().await;
        let odd = Producer {
            id: "ingest|eu:1 é".to_string(),
            ..client()
        };
        let plain = Producer {
            id: "ingest".to_string(),
            ..client()
        };
        let first = append(&target, &odd, 1, &["a"]).await.unwrap();
        append(&target, &plain, 1, &["b"]).await.unwrap();
        let resend = append(&target, &odd, 1, &["a"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        assert_eq!(target.texts().await, ["a", "b"]);
    }
}

/// A resend with the last sequence and other content fails as `seq_divergent` and writes nothing.
async fn a_resend_with_other_content_fails_as_divergent() {
    for target in stores_for(Needs::CALLS).await {
        target.create().await;
        let p = client();
        append(&target, &p, 1, &["p1"]).await.unwrap();
        let divergent = append(&target, &p, 1, &["other"]).await.unwrap_err();
        assert_eq!(divergent.reason, Reason::SeqDivergent);
        assert_eq!(target.texts().await, ["p1"]);
    }
}

/// A lower epoch is fenced for each Producer kind that has an epoch. A client has none, so two
/// writes of one client Producer are never fenced.
async fn a_lower_epoch_is_fenced_for_each_producer_kind() {
    for target in stores_for(Needs::UNCHECKED).await {
        target.create().await;
        let run = Run::new("r1");
        let la = run.local_activity("k", 8, 1, 2);
        target.batch(&la, 1, &["la2"]).await.unwrap();
        let stale = target
            .batch(&run.local_activity("k", 8, 1, 1), 1, &["la1"])
            .await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        append(&target, &activity("r1", 2, 20), 1, &["a2"])
            .await
            .unwrap();
        let stale = append(&target, &activity("r1", 3, 15), 1, &["a3"]).await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        append(&target, &client(), 1, &["c1"]).await.unwrap();
        append(&target, &client(), 2, &["c2"]).await.unwrap();
        assert_eq!(target.texts().await, ["la2", "a2", "c1", "c2"]);
    }
}

/// A close refuses later appends but not a resend of an earlier batch, and a create of the Stream
/// fails as closed. With `STORE_ANSWERS`, a second close returns `already`.
async fn a_close_stops_appends_but_not_a_resend() {
    for target in stores_for(Needs::CALLS).await {
        target.create().await;
        let p = client();
        let first = append(&target, &p, 1, &["a"]).await.unwrap();
        let closed = target.close(&p).await.unwrap();
        assert_eq!(
            append(&target, &p, 2, &["b"]).await.unwrap_err().reason,
            Reason::Closed
        );
        assert_eq!(
            append(&target, &p, 1, &["a"]).await.unwrap().offsets,
            first.offsets
        );
        let again = target.close(&p).await.unwrap();
        if target.gives.gives(Needs::STORE_ANSWERS) {
            assert!(!closed.already && again.already);
        }
        let created = target.create_with(&use_existing()).await;
        assert_eq!(created.unwrap_err().reason, Reason::Closed);
    }
}

/// A Stream with a newer format version refuses every write. A write checks the version before it
/// reads the Producer's `p|` field, since a newer format can change that field.
async fn a_newer_format_refuses_every_write() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        let p = client();
        append(&target, &p, 1, &["a"]).await.unwrap();
        target.set_meta(fields::VERSION, "2").await;
        let field = fields::producer(ProducerKind::Client as i32, &p.id);
        target.set_meta(&field, "a field of a newer format").await;
        let error = append(&target, &p, 2, &["b"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::FormatTooNew);
        assert_eq!(
            target.close(&p).await.unwrap_err().reason,
            Reason::FormatTooNew
        );
        let create = target.create_with(&use_existing()).await;
        assert_eq!(create.unwrap_err().reason, Reason::FormatTooNew);
    }
}

/// An expired Stream refuses writes and its create.
async fn an_expired_stream_refuses_writes_and_its_create() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        target.set_meta("exp", "1").await;
        let error = append(&target, &client(), 1, &["a"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Expired);
        let created = target.create_with(&use_existing()).await;
        assert_eq!(created.unwrap_err().reason, Reason::Expired);
    }
}

/// With no last-write TTL, an open Stream's meta and log never expire, and the meta has no `exp`.
/// Its close gives both the Retention, and the meta the Tombstone after it.
async fn an_open_stream_with_no_last_write_ttl_never_expires_until_the_close() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        append(&target, &client(), 1, &["a"]).await.unwrap();
        let week = Duration::from_secs(7 * 86_400).as_millis() as i64;
        assert_eq!(target.ttls().await, (-1, -1));
        assert_eq!(target.meta("exp").await, None);
        target.close(&client()).await.unwrap();
        let (ttl, meta) = target.ttls().await;
        assert!(0 < ttl && ttl <= week, "{ttl}");
        assert!(
            meta > Duration::from_secs(36 * 86_400).as_millis() as i64,
            "{meta}"
        );
    }
}

/// An open Stream keeps every Record past its Retention, while it is idle and after its next
/// append. No append trims a Record and no read hides one, since the Retention counts from the
/// close.
async fn an_open_stream_keeps_its_records_past_its_retention() {
    for target in stores_for(Needs::RAW_KEYS.and(Needs::SHORT_TIMES)).await {
        let retention = Duration::from_millis(300);
        target
            .create_with(&Create {
                retention: Some(retention),
                ..Default::default()
            })
            .await
            .unwrap();
        let old = append(&target, &client(), 1, &["old"]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(target.texts().await, ["old"]);
        append(&target, &client(), 2, &["new"]).await.unwrap();
        let (state, entries) = target
            .store
            .read(&target.stream_ref(), 0, 10)
            .await
            .unwrap();
        assert_eq!(state.trimmed, 0);
        assert!(state.retained_from() <= old.offsets.unwrap().0);
        assert_eq!(records_of(&entries), ["old", "new"]);
        // A Store with a last-write TTL gives the log that TTL, and one with none gives no TTL.
        let (ttl, _) = target.ttls().await;
        assert!(ttl == -1 || ttl > retention.as_millis() as i64, "{ttl}");
    }
}

/// A write that finds the log gone, as after an eviction, raises the trimmed offset to the meta's
/// newest one. Its offsets go above it, also with the clock behind it.
async fn offsets_go_on_after_the_log_is_gone() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        append(&target, &client(), 1, &["old"]).await.unwrap();
        // A newest id one minute ahead, as after the clock steps back.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let ahead = format!("{}-3", now + 60_000);
        target.set_meta("newest", &ahead).await;
        target.drop_log().await;
        assert_eq!(target.ttls().await.0, -2);
        let new = append(&target, &client(), 2, &["new"]).await.unwrap();
        let ahead = offset_of_entry_id(&ahead).unwrap();
        assert!(new.offsets.unwrap().0 > ahead);
        let (state, _) = target.store.read(&target.stream_ref(), 0, 0).await.unwrap();
        assert_eq!(state.trimmed, ahead);
        assert_eq!(target.texts().await, ["new"]);
    }
}

/// A wait for the next entry wakes on an append and ends at its deadline.
async fn a_blocked_read_wakes_on_an_append_and_ends_at_its_deadline() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        let first = append(&target, &client(), 1, &["a"]).await.unwrap();
        let newest = first.offsets.unwrap().1;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let woke = target
            .store
            .wait_after(&target.stream_ref(), newest, deadline)
            .await
            .unwrap();
        assert!(!woke);
        let store = target.store.clone();
        let stream = target.stream_ref();
        let waiter = tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            store.wait_after(&stream, newest, deadline).await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        append(&target, &client(), 2, &["b"]).await.unwrap();
        assert!(waiter.await.unwrap().unwrap());
    }
}

/// A close writes no log entry. It ends a wait for the next entry, and a wait on a closed Stream
/// ends at once, so a waiting read sees the close.
async fn a_close_writes_no_entry_and_ends_a_wait() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        let first = append(&target, &client(), 1, &["a"]).await.unwrap();
        let newest = first.offsets.unwrap().1;
        let store = target.store.clone();
        let stream = target.stream_ref();
        let waiter = tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            store.wait_after(&stream, newest, deadline).await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        target.close(&client()).await.unwrap();
        assert!(waiter.await.unwrap().unwrap());
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_secs(5);
        let woke = target
            .store
            .wait_after(&target.stream_ref(), newest, deadline)
            .await
            .unwrap();
        assert!(woke && started.elapsed() < Duration::from_secs(1));
        let (state, entries) = target
            .store
            .read(&target.stream_ref(), 0, 10)
            .await
            .unwrap();
        assert!(state.closed);
        assert_eq!((state.newest, entries.len()), (newest, 1));
    }
}

/// The Records of one batch get consecutive offsets that end at the batch's last offset, so a
/// caller knows the offset of each one.
async fn a_batch_gets_consecutive_offsets_that_end_at_its_last_offset() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        append(&target, &client(), 1, &["before"]).await.unwrap();
        // Enough Records that `XADD *` on Redis 7.0 would cross a millisecond.
        let texts: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
        let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
        let appended = append(&target, &client(), 2, &texts).await.unwrap();
        let offsets: Vec<i64> = target.entries().await[1..]
            .iter()
            .map(|entry| entry.offset)
            .collect();
        assert_eq!(offsets.len(), texts.len());
        assert_eq!(
            appended.offsets,
            Some((offsets[0], offsets[offsets.len() - 1]))
        );
        assert!(
            offsets.windows(2).all(|pair| pair[1] == pair[0] + 1),
            "{offsets:?}"
        );
    }
}

/// A negative sequence is refused.
async fn a_negative_sequence_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create().await;
        let error = append(&target, &client(), -1, &["x"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        assert!(target.texts().await.is_empty());
    }
}

/// A batch with no sequence takes the next one of its Producer's epoch, and a newer epoch starts
/// at one. Such a batch has no dedupe, so its retry writes it again. A batch that sends the taken
/// number is a resend.
async fn a_batch_with_no_sequence_takes_the_next_one_of_its_epoch() {
    for target in stores_for(Needs::UNCHECKED).await {
        target.create().await;
        let p = activity("r1", 1, 10);
        append(&target, &p, 0, &["a"]).await.unwrap();
        let retry = append(&target, &p, 0, &["a"]).await.unwrap();
        let resend = append(&target, &p, 2, &["a"]).await.unwrap();
        assert_eq!(resend.offsets, retry.offsets);
        let lower = append(&target, &p, 1, &["a"]).await.unwrap_err();
        assert_eq!(lower.reason, Reason::SequenceTooLow);
        append(&target, &p, 7, &["b"]).await.unwrap();
        let next = append(&target, &p, 0, &["c"]).await.unwrap();
        let resend = append(&target, &p, 8, &["c"]).await.unwrap();
        assert_eq!(resend.offsets, next.offsets);
        let newer = activity("r1", 2, 10);
        let first = append(&target, &newer, 0, &["d"]).await.unwrap();
        let resend = append(&target, &newer, 1, &["d"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        assert_eq!(target.texts().await, ["a", "a", "b", "c", "d"]);
    }
}

/// Batches with no sequence that one Producer epoch sends at once each take their own number, so
/// the Store writes all of them.
async fn batches_with_no_sequence_at_once_each_take_their_own_number() {
    for target in stores_for(Needs::CALLS).await {
        target.create().await;
        let p = client();
        let texts = ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"];
        let appends = texts
            .iter()
            .map(|text| append(&target, &p, 0, std::slice::from_ref(text)));
        for appended in join_all(appends).await {
            appended.unwrap();
        }
        let mut written = target.texts().await;
        written.sort();
        assert_eq!(written, texts);
        // The batches took 1 to 8, so 8 holds one of them.
        let taken = append(&target, &p, 8, &["other"]).await.unwrap_err();
        assert_eq!(taken.reason, Reason::SeqDivergent);
    }
}

/// A large sequence keeps its dedupe entry, so a resend gets its first answer and a lower or
/// divergent batch is refused. A sequence past 2^53 - 1 is refused.
async fn a_large_sequence_dedupes_and_one_past_2_53_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create().await;
        let p = client();
        let large = 100_000_000_000_000;
        let first = append(&target, &p, large, &["x"]).await.unwrap();
        let resend = append(&target, &p, large, &["x"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        let divergent = append(&target, &p, large, &["other"]).await;
        assert_eq!(divergent.unwrap_err().reason, Reason::SeqDivergent);
        let lower = append(&target, &p, large - 1, &["lower"]).await;
        assert_eq!(lower.unwrap_err().reason, Reason::SequenceTooLow);
        assert_eq!(target.texts().await, ["x"]);
        let max = (1 << 53) - 1;
        append(&target, &p, max, &["max"]).await.unwrap();
        let past = append(&target, &p, max + 1, &["past"]).await;
        assert_eq!(past.unwrap_err().reason, Reason::Invalid);
    }
}

/// An open Stream keeps a Producer's dedupe past the Retention. A resend of the last batch of a
/// Producer with no append for longer than the Retention gets the first answer, and the Stream
/// has one copy of the batch.
async fn a_silent_producer_resends_its_last_batch_once_past_the_retention() {
    for target in stores_for(Needs::SHORT_TIMES).await {
        target
            .create_with(&Create {
                retention: Some(Duration::from_millis(300)),
                ..Default::default()
            })
            .await
            .unwrap();
        let p = client();
        let first = append(&target, &p, 1, &["old"]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        // A Store that dropped a silent Producer's state would do it on another Producer's append.
        append(&target, &other_client(), 1, &["new"]).await.unwrap();
        let resend = append(&target, &p, 1, &["old"]).await.unwrap();
        assert_eq!(resend.offsets, first.offsets);
        if target.gives.gives(Needs::STORE_ANSWERS) {
            assert!(resend.resend);
        }
        assert_eq!(target.texts().await, ["old", "new"]);
    }
}

/// An open Stream keeps a Producer's fence past the Retention, so a lower epoch of a Producer with
/// no append for longer than the Retention is still fenced.
async fn a_silent_producer_stays_fenced_past_the_retention() {
    for target in stores_for(Needs::SHORT_TIMES.and(Needs::UNCHECKED)).await {
        target
            .create_with(&Create {
                retention: Some(Duration::from_millis(300)),
                ..Default::default()
            })
            .await
            .unwrap();
        append(&target, &activity("r1", 2, 10), 1, &["new"])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        append(&target, &other_client(), 1, &["other"])
            .await
            .unwrap();
        let stale = append(&target, &activity("r1", 1, 10), 1, &["stale"]).await;
        assert_eq!(stale.unwrap_err().reason, Reason::Fenced);
        assert_eq!(target.texts().await, ["new", "other"]);
    }
}

/// A Producer with a lower epoch can't close the Stream.
async fn a_stale_producer_cant_close_the_stream() {
    for target in stores_for(Needs::UNCHECKED).await {
        target.create().await;
        append(&target, &activity("r1", 2, 10), 1, &["a"])
            .await
            .unwrap();
        assert_eq!(
            target
                .close(&activity("r1", 1, 10))
                .await
                .unwrap_err()
                .reason,
            Reason::Fenced
        );
        target.close(&activity("r1", 2, 10)).await.unwrap();
    }
}

/// A Store refuses an append or a close of a Producer with no epoch, or with an epoch that has no
/// case, since the case gives the kind of the Producer.
async fn a_producer_with_no_epoch_case_is_refused() {
    for target in stores_for(Needs::CALLS).await {
        target.create().await;
        let producer = Producer {
            kind: None,
            ..client()
        };
        let error = append(&target, &producer, 1, &["a"]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        let error = target.close(&producer).await.unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        assert!(target.texts().await.is_empty());
    }
}

/// An append whose entry ids would not fit an offset is refused before it writes.
async fn an_append_whose_ids_would_not_fit_an_offset_is_refused_before_it_writes() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        // An entry a minute ahead of the clock, with nearly the last 20-bit sequence.
        let ahead = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 60_000;
        target.add_raw_entry(&format!("{ahead}-1048570")).await;
        let error = append(&target, &client(), 1, &["a"; 10]).await.unwrap_err();
        assert_eq!(error.reason, Reason::StorageUnavailable);
        assert_eq!(target.log_len().await, 1);
    }
}

/// The entry ids of an append's offsets.
fn entry_ids(offsets: Option<(i64, i64)>) -> Option<(EntryId, EntryId)> {
    let id = |offset: i64| {
        let text = entry_id_of_offset(offset);
        let (ms, seq) = text.split_once('-').unwrap();
        (ms.parse().unwrap(), seq.parse().unwrap())
    };
    offsets.map(|(first, last)| (id(first), id(last)))
}

/// A Producer's `p|` field has the same 88 bytes on every Store. These are the fence, and the last
/// batch's sequence, entry ids and digest.
async fn a_producer_field_has_the_shared_layout() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        let kind = ProducerKind::Activity as i32;
        let field = |producer: &Producer, offsets, texts: &[&str]| {
            ProducerField {
                fence: epoch_key(producer.kind.as_ref()),
                sequence: 1,
                entries: entry_ids(offsets),
                digest: digest(texts),
            }
            .encode()
        };
        let a1 = activity("r1", 1, 10);
        let stored = || async { target.meta(fields::producer(kind, &a1.id)).await.unwrap() };
        let written = append(&target, &a1, 1, &["x", "y"]).await.unwrap();
        assert_eq!(stored().await, field(&a1, written.offsets, &["x", "y"]));
        // A newer attempt's empty batch sets its fence but writes no entry.
        let a2 = activity("r1", 2, 20);
        let empty = append(&target, &a2, 1, &[]).await.unwrap();
        assert_eq!(empty.offsets, None);
        assert_eq!(stored().await, field(&a2, None, &[]));
    }
}

/// A Store refuses a write of a Producer whose `p|` field has another length, which a later format
/// may write.
async fn a_producer_field_of_another_format_is_refused() {
    for target in stores_for(Needs::RAW_KEYS).await {
        target.create().await;
        let p = client();
        append(&target, &p, 1, &["a"]).await.unwrap();
        let field = fields::producer(ProducerKind::Client as i32, &p.id);
        let value = target.meta(&field).await.unwrap();
        let longer = [value.clone(), vec![0]].concat();
        for other in [longer, value[..value.len() - 1].to_vec()] {
            target.set_meta(&field, &other).await;
            let error = append(&target, &p, 2, &["b"]).await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            let error = target.close(&p).await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
        }
        assert_eq!(target.texts().await, ["a"]);
    }
}

/// The cases of the suite, in file order.
pub const CASES: &[super::Case] = &[
    case!(a_create_writes_the_key_once_and_keeps_its_retention),
    case!(a_create_of_an_open_stream_fails_unless_it_uses_it_or_retries),
    case!(closed_and_expired_come_before_already_exists),
    case!(a_resend_gets_its_first_positions_and_a_lower_sequence_is_refused),
    case!(a_resend_with_other_content_fails_as_divergent),
    case!(a_producer_id_is_bytes_with_its_own_state),
    case!(a_lower_epoch_is_fenced_for_each_producer_kind),
    case!(a_close_stops_appends_but_not_a_resend),
    case!(a_newer_format_refuses_every_write),
    case!(an_expired_stream_refuses_writes_and_its_create),
    case!(an_open_stream_with_no_last_write_ttl_never_expires_until_the_close),
    case!(an_open_stream_keeps_its_records_past_its_retention),
    case!(offsets_go_on_after_the_log_is_gone),
    case!(a_blocked_read_wakes_on_an_append_and_ends_at_its_deadline),
    case!(a_close_writes_no_entry_and_ends_a_wait),
    case!(a_batch_gets_consecutive_offsets_that_end_at_its_last_offset),
    case!(a_negative_sequence_is_refused),
    case!(a_batch_with_no_sequence_takes_the_next_one_of_its_epoch),
    case!(batches_with_no_sequence_at_once_each_take_their_own_number),
    case!(a_large_sequence_dedupes_and_one_past_2_53_is_refused),
    case!(a_silent_producer_resends_its_last_batch_once_past_the_retention),
    case!(a_silent_producer_stays_fenced_past_the_retention),
    case!(a_stale_producer_cant_close_the_stream),
    case!(a_producer_with_no_epoch_case_is_refused),
    case!(an_append_whose_ids_would_not_fit_an_offset_is_refused_before_it_writes),
    case!(a_producer_field_has_the_shared_layout),
    case!(a_producer_field_of_another_format_is_refused),
];
