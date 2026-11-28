use base64::{Engine, prelude::BASE64_STANDARD};
use prost::Message;
use std::{
    borrow::Borrow,
    fmt::{Debug, Display, Formatter},
};

static LOCAL_ACT_TASK_TOKEN_PREFIX: &[u8] = b"local_act_";

#[derive(
    Hash,
    Eq,
    PartialEq,
    Clone,
    derive_more::From,
    derive_more::Into,
    serde::Serialize,
    serde::Deserialize,
)]
/// Type-safe wrapper for task token bytes
pub struct TaskToken(Vec<u8>);

impl TaskToken {
    /// Consumes this token and returns its underlying bytes.
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    /// Task tokens for local activities are always prefixed with a special sigil so they can
    /// be identified easily
    pub fn new_local_activity_token(unique_data: impl IntoIterator<Item = u8>) -> Self {
        let mut bytes = LOCAL_ACT_TASK_TOKEN_PREFIX.to_vec();
        bytes.extend(unique_data);
        TaskToken(bytes)
    }

    /// Returns true if the task token is for a local activity
    pub fn is_local_activity_task(&self) -> bool {
        self.0.starts_with(LOCAL_ACT_TASK_TOKEN_PREFIX)
    }
}

impl Display for TaskToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format_task_token(&self.0))
    }
}

impl Debug for TaskToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("TaskToken({})", format_task_token(&self.0)))
    }
}

impl Borrow<[u8]> for TaskToken {
    fn borrow(&self) -> &[u8] {
        self.0.as_slice()
    }
}

pub(crate) fn format_task_token(tt: &[u8]) -> String {
    BASE64_STANDARD.encode(tt)
}

#[derive(Clone, PartialEq, Message)]
struct ServerActivityToken {
    #[prost(int64, tag = "4")]
    scheduled_event_id: i64,
}

/// The server exposes the scheduling event ID only in its internal task token.
/// An unreadable token must not prevent Activity task delivery.
pub(crate) fn scheduled_event_id(task_token: &[u8]) -> i64 {
    ServerActivityToken::decode(task_token).map_or(0, |token| token.scheduled_event_id.max(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_scheduled_event_id_from_field_four() {
        // The scheduling event ID must remain distinct from the started event ID.
        assert_eq!(scheduled_event_id(&[0x0a, 1, b'n', 0x20, 5, 0x50, 9]), 5);
        assert_eq!(scheduled_event_id(&[0x0a, 1, b'n', 0x50, 9]), 0);
        assert_eq!(scheduled_event_id(&[0xff]), 0);
        assert_eq!(scheduled_event_id(&[0x22, 1, 0]), 0);
        let negative = ServerActivityToken {
            scheduled_event_id: -1,
        };
        assert_eq!(scheduled_event_id(&negative.encode_to_vec()), 0);
    }
}
