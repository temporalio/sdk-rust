use crate::internal_flags::CoreInternalFlags;
use temporalio_common::protos::temporal::api::{
    enums::v1::EventType,
    history::v1::{HistoryEvent, history_event::Attributes},
};

#[derive(Default, Debug)]
pub(super) struct RunVersion(Option<bool>);

#[derive(Debug, thiserror::Error)]
#[error("Unknown Core flag {flag} on first completion event {event_id}")]
pub(super) struct UnknownFlag {
    flag: u32,
    event_id: i64,
}

impl RunVersion {
    pub(super) fn observe(&mut self, events: &[HistoryEvent]) -> Result<(), UnknownFlag> {
        if self.0.is_none()
            && let Some(event) = events
                .iter()
                .find(|event| event.event_type() == EventType::WorkflowTaskCompleted)
        {
            let flags = match &event.attributes {
                Some(Attributes::WorkflowTaskCompletedEventAttributes(attrs)) => attrs
                    .sdk_metadata
                    .as_ref()
                    .map(|metadata| metadata.core_used_flags.as_slice())
                    .unwrap_or_default(),
                _ => &[],
            };
            if let Some(flag) = flags
                .iter()
                .find(|flag| CoreInternalFlags::from_u32(**flag) == CoreInternalFlags::TooHigh)
            {
                return Err(UnknownFlag {
                    flag: *flag,
                    event_id: event.event_id,
                });
            }
            self.0 = Some(flags.contains(&(CoreInternalFlags::WftChunkingV2 as u32)));
        }
        Ok(())
    }

    pub(super) fn selected(&self) -> Option<bool> {
        self.0
    }
}

pub(super) fn command_event(event: &HistoryEvent) -> bool {
    event.is_command_event()
        || (event.event_type() == EventType::Unspecified && event.is_ignorable())
}

pub(super) fn boundary(
    events: &[HistoryEvent],
    after: i64,
    finished: bool,
    updates: &[i64],
) -> Option<usize> {
    let next_update = updates.iter().copied().filter(|id| *id > after).min();
    let mut meaningful = false;
    let mut starts = Vec::new();
    let mut cursor = events.partition_point(|event| event.event_id <= after);
    while let Some(event) = events.get(cursor) {
        let kind = event.event_type();
        let update_reached = next_update.is_some_and(|id| id <= event.event_id)
            || kind == EventType::WorkflowExecutionUpdateAdmitted;
        if update_reached && let Some((_, index)) = starts.last() {
            return Some(*index);
        }
        meaningful |= update_reached;
        if event.is_final_wf_execution_event() {
            return Some(cursor);
        }
        if kind != EventType::WorkflowTaskStarted {
            meaningful |= !matches!(
                kind,
                EventType::WorkflowTaskScheduled | EventType::WorkflowTaskCompleted
            );
            cursor += 1;
            continue;
        }
        let outcome = events.get(cursor + 1).map(HistoryEvent::event_type);
        if matches!(
            outcome,
            Some(EventType::WorkflowTaskFailed | EventType::WorkflowTaskTimedOut)
        ) {
            cursor += 2;
            continue;
        }
        starts.push((event.event_id, cursor));
        if outcome != Some(EventType::WorkflowTaskCompleted) {
            return (finished || outcome.is_some()).then_some(cursor);
        }
        let commands_begin = cursor + 2;
        let commands_end = commands_begin
            + events[commands_begin..]
                .iter()
                .take_while(|event| command_event(event))
                .count();
        if commands_end == events.len() && !finished {
            return None;
        }
        let required = events[commands_begin..commands_end]
            .iter()
            .filter_map(|event| {
                let Some(Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(attrs)) =
                    &event.attributes
                else {
                    return None;
                };
                starts
                    .iter()
                    .rev()
                    .find(|(id, _)| *id < attrs.accepted_request_sequencing_event_id)
                    .map(|(_, index)| *index)
            })
            .min();
        if let Some(required) = required {
            return Some(required);
        }
        if meaningful || commands_end > commands_begin {
            return Some(cursor);
        }
        let scheduled = events.get(commands_end).map(HistoryEvent::event_type);
        let successor = commands_end + 1;
        if scheduled != Some(EventType::WorkflowTaskScheduled)
            || events.get(successor).map(HistoryEvent::event_type)
                != Some(EventType::WorkflowTaskStarted)
        {
            return if finished || events.len() > successor {
                Some(cursor)
            } else {
                None
            };
        }
        let successor_outcome = events.get(successor + 1).map(HistoryEvent::event_type);
        if successor_outcome.is_none() {
            if !finished {
                return None;
            }
            return Some(if next_update.is_some_and(|id| id > event.event_id) {
                cursor
            } else {
                successor
            });
        }
        if successor_outcome != Some(EventType::WorkflowTaskCompleted) {
            return Some(cursor);
        }
        let batch_begin = successor + 2;
        let batch_end = batch_begin
            + events[batch_begin..]
                .iter()
                .take_while(|event| command_event(event))
                .count();
        if batch_end == events.len() && !finished {
            return None;
        }
        if events[batch_begin..batch_end].iter().any(|event| {
            matches!(
                event.event_type(),
                EventType::WorkflowExecutionUpdateAccepted | EventType::Unspecified
            )
        }) {
            return Some(cursor);
        }
        cursor = successor;
    }
    None
}
