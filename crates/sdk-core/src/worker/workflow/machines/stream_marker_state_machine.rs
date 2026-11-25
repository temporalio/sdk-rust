//! The machine of a Stream marker, which an activation records when Workflow code gets the result
//! of an append or a read, or a completion records with no entry when it leaves a call open. A
//! cached run matches the marker event to its command, as it does for any other command.

use super::{
    EventInfo, HistEventData, NewMachineWithCommand, StateMachine, TransitionResult,
    WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    nondeterminism,
    streams::{PeekedMarker, is_stream_marker, marker_details},
};
use temporalio_common::protos::{
    constants::{STREAM_APPEND_MARKER_NAME, STREAM_READ_MARKER_NAME},
    temporal::api::{command::v1::RecordMarkerCommandAttributes, enums::v1::CommandType},
};

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

#[derive(Default, Clone)]
pub(super) struct Created {}

#[derive(Default, Clone)]
pub(super) struct CommandSent {}

impl From<Created> for CommandSent {
    fn from(_: Created) -> Self {
        Self {}
    }
}

#[derive(Default, Clone)]
pub(super) struct Recorded {}

impl From<CommandSent> for Recorded {
    fn from(_: CommandSent) -> Self {
        Self {}
    }
}

/// Creates the marker command that records `data`, with its machine.
pub(super) fn stream_marker(data: &PeekedMarker) -> NewMachineWithCommand {
    let (marker_name, details) = match data {
        PeekedMarker::Append(data) => (STREAM_APPEND_MARKER_NAME, marker_details(data)),
        PeekedMarker::Read(data) => (STREAM_READ_MARKER_NAME, marker_details(data)),
    };
    let command = RecordMarkerCommandAttributes {
        marker_name: marker_name.to_string(),
        details,
        header: None,
        failure: None,
    };
    NewMachineWithCommand {
        command: command.into(),
        machine: StreamMarkerMachine::from_parts(Created {}.into(), ()).into(),
    }
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
        if is_stream_marker(&e.event) {
            Ok(Self::MarkerRecorded)
        } else {
            Err(nondeterminism!(
                "The run sent a Stream marker, and History has this event in its place: {}",
                e.event
            ))
        }
    }
}
