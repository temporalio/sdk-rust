//! User-facing Event Groups and conversion to protocol markers.

use std::collections::HashMap;
use temporalio_common_wasm::{
    data_converters::{
        GenericPayloadConverter, PayloadConverter, SerializationContext, SerializationContextData,
        WorkflowSerializationContext,
    },
    protos::temporal::api::{
        common::v1::Payload,
        sdk::v1::{
            EventGroupMarker,
            event_group_marker::{InboundEvent, InboundUpdate, Label, Variant},
        },
    },
};

/// A token that associates workflow commands (and the history events they produce) with a logical
/// group for UI and observability.
///
/// Create a group with [`EventGroup::new`]. Attach it to specific commands via `event_groups` on
/// command options, or to every command produced through a derived context via
/// [`crate::WorkflowContext::with_event_group`].
///
/// **EXPERIMENTAL:** Event Groups is an experimental API and may change without notice.
#[cfg_attr(docsrs, doc(cfg(feature = "experimental")))]
#[derive(Clone, Debug)]
pub struct EventGroup {
    inner: EventGroupInner,
}

#[derive(Clone, Debug)]
enum EventGroupInner {
    Label { id: String, label: Option<String> },
    InboundEvent { event_id: i64 },
    InboundUpdate { update_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum EventGroupKey {
    Label(String),
    InboundEvent(i64),
    InboundUpdate(String),
}

/// Ambient Event Groups carried by a workflow context.
///
/// Implicit groups (signal / update handlers) replace any enclosing explicit scope. Explicit groups
/// nest and compose.
#[derive(Clone, Debug, Default)]
pub(crate) struct ActiveEventGroups {
    implicit: Option<EventGroup>,
    explicit: Vec<EventGroup>,
}

impl EventGroup {
    /// Create an Event Group with the given opaque identifier and no display label.
    ///
    /// Commands with the same `id` belong to the same group. The identifier is stored as plaintext
    /// in workflow history and should not contain sensitive information.
    ///
    /// # Panics
    ///
    /// Panics if `id` is empty.
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        assert!(!id.is_empty(), "Event group id must not be empty");
        Self {
            inner: EventGroupInner::Label { id, label: None },
        }
    }

    /// Set a display label for this Event Group.
    ///
    /// The label is persisted as a `json/plain` payload and then codec-encoded if a payload codec is
    /// configured. If omitted, the Marker carries no label payload and the UI should display the ID
    /// instead.
    ///
    /// # Panics
    ///
    /// Panics if `label` is empty.
    pub fn with_label(self, label: impl Into<String>) -> Self {
        let label = label.into();
        assert!(!label.is_empty(), "Event group label must not be empty");
        let EventGroupInner::Label { id, .. } = self.inner else {
            unreachable!("with_label is only valid on explicitly created Event Groups");
        };
        Self {
            inner: EventGroupInner::Label {
                id,
                label: Some(label),
            },
        }
    }

    pub(crate) fn inbound_event(event_id: i64) -> Option<Self> {
        (event_id > 0).then_some(Self {
            inner: EventGroupInner::InboundEvent { event_id },
        })
    }

    pub(crate) fn inbound_update(update_id: impl Into<String>) -> Self {
        Self {
            inner: EventGroupInner::InboundUpdate {
                update_id: update_id.into(),
            },
        }
    }

    fn key(&self) -> EventGroupKey {
        match &self.inner {
            EventGroupInner::Label { id, .. } => EventGroupKey::Label(id.clone()),
            EventGroupInner::InboundEvent { event_id } => EventGroupKey::InboundEvent(*event_id),
            EventGroupInner::InboundUpdate { update_id } => {
                EventGroupKey::InboundUpdate(update_id.clone())
            }
        }
    }

    pub(crate) fn to_marker(&self) -> EventGroupMarker {
        EventGroupMarker {
            variant: Some(match &self.inner {
                EventGroupInner::Label { id, label } => Variant::Label(Label {
                    id: id.clone(),
                    label: label.as_deref().map(label_payload),
                }),
                EventGroupInner::InboundEvent { event_id } => Variant::InboundEvent(InboundEvent {
                    inbound_event_id: *event_id,
                }),
                EventGroupInner::InboundUpdate { update_id } => {
                    Variant::InboundUpdate(InboundUpdate {
                        inbound_update_id: update_id.clone(),
                    })
                }
            }),
        }
    }

    pub(crate) fn to_markers(groups: impl IntoIterator<Item = Self>) -> Vec<EventGroupMarker> {
        groups.into_iter().map(|group| group.to_marker()).collect()
    }
}

impl ActiveEventGroups {
    pub(crate) fn with_explicit(&self, groups: impl IntoIterator<Item = EventGroup>) -> Self {
        let mut explicit = self.explicit.clone();
        for group in groups {
            upsert_group(&mut explicit, group);
        }
        Self {
            implicit: self.implicit.clone(),
            explicit,
        }
    }

    pub(crate) fn with_implicit(&self, implicit: EventGroup) -> Self {
        Self {
            implicit: Some(implicit),
            explicit: Vec::new(),
        }
    }

    fn to_markers(&self) -> Vec<EventGroupMarker> {
        let mut groups = Vec::new();
        if let Some(implicit) = &self.implicit {
            groups.push(implicit.clone());
        }
        groups.extend(self.explicit.iter().cloned());
        EventGroup::to_markers(groups)
    }
}

/// Merge ambient context groups with markers directly attached to a command.
pub(crate) fn merge_event_group_markers(
    ambient: &ActiveEventGroups,
    direct: Vec<EventGroupMarker>,
) -> Vec<EventGroupMarker> {
    let mut by_key = HashMap::new();
    let mut order = Vec::new();
    for marker in ambient
        .to_markers()
        .into_iter()
        .chain(direct)
        .filter(|marker| marker.variant.is_some())
    {
        if let Some(key) = marker_key(&marker)
            && by_key.insert(key.clone(), marker).is_none()
        {
            order.push(key);
        }
    }
    order
        .into_iter()
        .filter_map(|key| by_key.remove(&key))
        .collect()
}

fn label_payload(label: &str) -> Payload {
    let converter = PayloadConverter::default();
    let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
    let context = SerializationContext::new(&context_data, &converter);
    let label = label.to_owned();
    converter
        .to_payload(&context, &label)
        .expect("encoding an Event Group label as json/plain is infallible")
}

fn upsert_group(groups: &mut Vec<EventGroup>, group: EventGroup) {
    if let Some(existing) = groups
        .iter_mut()
        .find(|existing| existing.key() == group.key())
    {
        *existing = group;
    } else {
        groups.push(group);
    }
}

fn marker_key(marker: &EventGroupMarker) -> Option<EventGroupKey> {
    match marker.variant.as_ref()? {
        Variant::Label(label) => Some(EventGroupKey::Label(label.id.clone())),
        Variant::InboundEvent(event) => Some(EventGroupKey::InboundEvent(event.inbound_event_id)),
        Variant::InboundUpdate(update) => Some(EventGroupKey::InboundUpdate(
            update.inbound_update_id.clone(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_provided_id_is_used_verbatim() {
        let group = EventGroup::new("c-id").with_label("ccc");
        match group.to_marker().variant {
            Some(Variant::Label(label)) => {
                assert_eq!(label.id, "c-id");
                assert_eq!(
                    label
                        .label
                        .as_ref()
                        .unwrap()
                        .metadata
                        .get("encoding")
                        .unwrap(),
                    b"json/plain"
                );
                assert_eq!(label.label.as_ref().unwrap().data, b"\"ccc\"");
            }
            other => panic!("expected label marker, got {other:?}"),
        }
    }

    #[test]
    fn omitted_label_emits_no_payload() {
        let group = EventGroup::new("aaa");
        match group.to_marker().variant {
            Some(Variant::Label(label)) => {
                assert_eq!(label.id, "aaa");
                assert!(label.label.is_none());
            }
            other => panic!("expected label marker, got {other:?}"),
        }
    }

    #[test]
    fn merge_collapses_duplicate_ids_and_keeps_direct_label() {
        let ambient = ActiveEventGroups::default().with_explicit([
            EventGroup::new("a-id").with_label("aaa"),
            EventGroup::new("b-id").with_label("bbb"),
        ]);
        let extra = vec![EventGroup::new("a-id").with_label("aaa-direct").to_marker()];
        let merged = merge_event_group_markers(&ambient, extra);
        assert_eq!(merged.len(), 2);
        let a = merged
            .iter()
            .find_map(|marker| match marker.variant.as_ref()? {
                Variant::Label(label) if label.id == "a-id" => Some(label),
                _ => None,
            })
            .unwrap();
        assert_eq!(a.label.as_ref().unwrap().data, b"\"aaa-direct\"");
    }

    #[test]
    #[should_panic(expected = "Event group label must not be empty")]
    fn empty_label_panics() {
        let _ = EventGroup::new("id").with_label("");
    }

    #[test]
    #[should_panic(expected = "Event group id must not be empty")]
    fn empty_id_panics() {
        let _ = EventGroup::new("");
    }
}
