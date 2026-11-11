//! Writers for Activities and clients. The bridge holds each writer as an object, and dropping it
//! releases it.

use crate::{
    StreamResult,
    proto::{CloseWriterResponse, WriteRecordsRequest, WriteRecordsResponse},
};

/// Has no value, so no writer exists until the runtime opens one.
#[derive(Debug)]
enum Never {}

/// A writer of one Producer epoch on one Stream, for an Activity, a Local Activity or a client.
/// The bridge holds it, and dropping it releases it.
#[derive(Debug)]
pub struct StreamWriter(Never);

impl StreamWriter {
    /// Appends Records at once and answers with the offsets of the first and the last one.
    pub async fn write(&self, request: WriteRecordsRequest) -> StreamResult<WriteRecordsResponse> {
        let _ = request;
        match self.0 {}
    }

    /// Closes the writer. It waits for a write in flight, then answers with the offset of the
    /// newest Record that the writer wrote. It doesn't close the Stream.
    pub async fn close(&self) -> StreamResult<CloseWriterResponse> {
        match self.0 {}
    }
}
