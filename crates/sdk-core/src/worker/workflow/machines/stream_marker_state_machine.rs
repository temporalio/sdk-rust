//! The marker `core_stream` that Core records in a Workflow Task in which Workflow code got a
//! Stream result. The machine exists so that the marker command takes its place among the other
//! commands, and matches its event in the next Workflow Task of a cached run.

use super::{
    EventInfo, HistEventData, StateMachine, TransitionResult, WFMachinesAdapter, fsm,
    workflow_machines::MachineResponse,
};
use crate::worker::workflow::{WFMachinesError, nondeterminism};
use prost::Message;
use std::collections::HashMap;
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::external_data::StreamMarkerData,
    temporal::api::{
        command::v1::{RecordMarkerCommandAttributes, command},
        common::v1::{Payload, Payloads},
        enums::v1::{CommandType, EventType},
        history::v1::{HistoryEvent, MarkerRecordedEventAttributes, history_event},
    },
};

/// The marker detail that holds the encoded [StreamMarkerData].
pub(crate) const STREAM_MARKER_DATA_KEY: &str = "data";

fsm! {
    pub(super) name StreamMarkerMachine;
    command StreamMarkerCommand;
    error WFMachinesError;
    shared_state ();

    Created --(CommandRecordMarker) --> CommandSent;
    CommandSent --(MarkerRecorded) --> Recorded;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum StreamMarkerCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandSent {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Recorded {}

impl From<Created> for CommandSent {
    fn from(_: Created) -> Self {
        Self {}
    }
}

impl From<CommandSent> for Recorded {
    fn from(_: CommandSent) -> Self {
        Self {}
    }
}

/// A new marker machine and the attributes of its command, which hold `data`.
pub(super) fn stream_marker(data: &StreamMarkerData) -> (StreamMarkerMachine, command::Attributes) {
    let payload = Payload {
        metadata: HashMap::from([
            ("encoding".to_string(), b"binary/protobuf".to_vec()),
            (
                "messageType".to_string(),
                b"coresdk.external_data.StreamMarkerData".to_vec(),
            ),
        ]),
        data: data.encode_to_vec(),
        ..Default::default()
    };
    let attributes = RecordMarkerCommandAttributes {
        marker_name: STREAM_MARKER_NAME.to_string(),
        details: HashMap::from([(
            STREAM_MARKER_DATA_KEY.to_string(),
            Payloads {
                payloads: vec![payload],
            },
        )]),
        header: None,
        failure: None,
    };
    (
        StreamMarkerMachine::from_parts(Created {}.into(), ()),
        attributes.into(),
    )
}

/// True if the event is a `core_stream` marker.
pub(super) fn is_stream_marker(event: &HistoryEvent) -> bool {
    matches!(
        &event.attributes,
        Some(history_event::Attributes::MarkerRecordedEventAttributes(
            MarkerRecordedEventAttributes { marker_name, .. }
        )) if marker_name == STREAM_MARKER_NAME
    )
}

impl WFMachinesAdapter for StreamMarkerMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }
}

impl TryFrom<CommandType> for StreamMarkerMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::RecordMarker => Ok(Self::CommandRecordMarker),
            _ => Err(()),
        }
    }
}

impl TryFrom<HistEventData> for StreamMarkerMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        if e.event_type() == EventType::MarkerRecorded && is_stream_marker(&e) {
            Ok(Self::MarkerRecorded)
        } else {
            Err(nondeterminism!(
                "Core sent a Stream marker, but history has this event in its place: {e}"
            ))
        }
    }
}
