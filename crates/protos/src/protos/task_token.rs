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

/// The one field of the server's activity task token that Core reads.
#[derive(Clone, PartialEq, Message)]
struct ServerActivityToken {
    #[prost(int64, tag = "4")]
    scheduled_event_id: i64,
}

/// Returns the event id of the `ActivityTaskScheduled` event of an Activity, from the token of its
/// task, or 0.
pub(crate) fn scheduled_event_id(task_token: &[u8]) -> i64 {
    // The server has no API field for the id. The token is a serialized message of the server that
    // has it, but its format is internal to the server and can change, so a token that doesn't
    // decode gives 0.
    ServerActivityToken::decode(task_token).map_or(0, |token| token.scheduled_event_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The server's activity task token, with the field numbers and the wire types of its message.
    /// Another message takes the place of the clock.
    #[derive(Clone, PartialEq, Message)]
    struct ServerTask {
        #[prost(string, tag = "1")]
        namespace_id: String,
        #[prost(string, tag = "2")]
        workflow_id: String,
        #[prost(string, tag = "3")]
        run_id: String,
        #[prost(int64, tag = "4")]
        scheduled_event_id: i64,
        #[prost(int32, tag = "5")]
        attempt: i32,
        #[prost(string, tag = "6")]
        activity_id: String,
        #[prost(string, tag = "7")]
        workflow_type: String,
        #[prost(string, tag = "8")]
        activity_type: String,
        #[prost(message, optional, tag = "9")]
        clock: Option<prost_types::Duration>,
        #[prost(int64, tag = "10")]
        started_event_id: i64,
        #[prost(int64, tag = "11")]
        version: i64,
        #[prost(message, optional, tag = "12")]
        started_time: Option<prost_types::Timestamp>,
        #[prost(int64, tag = "13")]
        start_version: i64,
        #[prost(bytes = "vec", tag = "14")]
        component_ref: Vec<u8>,
        #[prost(int32, tag = "15")]
        activity_attempt_stamp: i32,
    }

    #[test]
    fn the_scheduled_event_id_is_field_4_of_the_token_or_0() {
        let token = ServerTask {
            namespace_id: "n".to_string(),
            workflow_id: "w".to_string(),
            run_id: "r".to_string(),
            scheduled_event_id: 5,
            attempt: 2,
            activity_id: "a".to_string(),
            workflow_type: "wt".to_string(),
            activity_type: "t".to_string(),
            clock: Some(prost_types::Duration {
                seconds: 1,
                nanos: 2,
            }),
            started_event_id: 9,
            version: 3,
            started_time: Some(prost_types::Timestamp {
                seconds: 4,
                nanos: 5,
            }),
            start_version: 10,
            component_ref: vec![6, 7],
            activity_attempt_stamp: 8,
        };
        assert_eq!(scheduled_event_id(&token.encode_to_vec()), 5);
        // A varint that the bytes end in the middle of isn't a message.
        assert_eq!(scheduled_event_id(&[0xff]), 0);
        // Field 4 with the length-delimited wire type.
        assert_eq!(scheduled_event_id(&[0x22, 0x01, 0x00]), 0);
    }
}
