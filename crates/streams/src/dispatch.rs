//! The byte form of the runtime's calls, for bridges. A bridge passes the encoded request and gets
//! the encoded response, so it decodes no proto.

use crate::{Reason, StreamError, StreamResult, StreamRuntime};
use prost::Message;

fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}

impl StreamRuntime {
    /// Runs one of the five Stream service operations on an encoded request, by its name, and
    /// returns the encoded response. The names are `CreateStream`, `AppendRecords`, `CloseStream`,
    /// `ReadRecords` and `DescribeStream`. The bridge raises an error with [StreamError::code] and
    /// [StreamError::details].
    pub async fn call(&self, operation: &str, request: &[u8]) -> StreamResult<Vec<u8>> {
        let response = match operation {
            "CreateStream" => self.create_stream(decode(request)?).await?.encode_to_vec(),
            "AppendRecords" => self.append_records(decode(request)?).await?.encode_to_vec(),
            "CloseStream" => self.close_stream(decode(request)?).await?.encode_to_vec(),
            "ReadRecords" => self.read_records(decode(request)?).await?.encode_to_vec(),
            "DescribeStream" => self
                .describe_stream(decode(request)?)
                .await?
                .encode_to_vec(),
            _ => {
                return Err(StreamError::new(
                    Reason::Unsupported,
                    format!("Unknown operation {operation}"),
                ));
            }
        };
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryStreamOptions, MemoryStreamRuntime,
        proto::{
            AppendRecord, AppendRecordsRequest, AppendRecordsResponse, ClientProducer,
            CloseStreamRequest, CloseStreamResponse, CreateStreamRequest, CreateStreamResponse,
            DescribeStreamRequest, DescribeStreamResponse, Producer, ReadRecordsRequest,
            ReadRecordsResponse, StreamRef, StreamStatus, producer::Kind,
        },
    };
    use std::sync::Arc;
    use temporalio_common::protos::temporal::api::common::v1::Payload;
    use tonic::Code;

    const OPERATIONS: [&str; 5] = [
        "CreateStream",
        "AppendRecords",
        "CloseStream",
        "ReadRecords",
        "DescribeStream",
    ];

    fn runtime() -> Arc<StreamRuntime> {
        MemoryStreamRuntime::new("b", MemoryStreamOptions::default(), None)
    }

    fn stream_ref() -> Option<StreamRef> {
        Some(StreamRef {
            id: "s".to_string(),
            ..Default::default()
        })
    }

    fn client() -> Option<Producer> {
        Some(Producer {
            id: "client:me:p".to_string(),
            kind: Some(Kind::Client(ClientProducer {})),
        })
    }

    fn records() -> Vec<AppendRecord> {
        vec![AppendRecord {
            body: Some(Payload::default()),
        }]
    }

    async fn call<Req: Message, Resp: Message + Default>(
        runtime: &StreamRuntime,
        operation: &str,
        request: Req,
    ) -> Resp {
        let bytes = runtime
            .call(operation, &request.encode_to_vec())
            .await
            .unwrap();
        Resp::decode(bytes.as_slice()).unwrap()
    }

    async fn create(runtime: &StreamRuntime) -> CreateStreamResponse {
        let request = CreateStreamRequest {
            stream_ref: stream_ref(),
            ..Default::default()
        };
        call(runtime, "CreateStream", request).await
    }

    // A typo in an operation name would make it unknown, which is unsupported, so the loop also
    // checks the names that a bridge calls.
    #[tokio::test]
    async fn bad_bytes_are_invalid_and_an_unknown_operation_is_unsupported() {
        let runtime = runtime();
        for operation in OPERATIONS {
            let error = runtime.call(operation, &[0xff]).await.unwrap_err();
            assert_eq!(error.reason, Reason::Invalid, "{operation}");
        }
        let error = runtime.call("Nope", &[]).await.unwrap_err();
        assert_eq!(
            (error.reason, error.code(), error.message.as_str()),
            (
                Reason::Unsupported,
                Code::Unimplemented,
                "Unknown operation Nope"
            )
        );
    }

    #[tokio::test]
    async fn each_stream_operation_round_trips_its_bytes() {
        let runtime = runtime();
        assert_eq!(create(&runtime).await.r#ref.unwrap().store, "b");
        let appended: AppendRecordsResponse = call(
            &runtime,
            "AppendRecords",
            AppendRecordsRequest {
                r#ref: stream_ref(),
                producer: client(),
                sequence: 1,
                records: records(),
                ..Default::default()
            },
        )
        .await;
        let read: ReadRecordsResponse = call(
            &runtime,
            "ReadRecords",
            ReadRecordsRequest {
                r#ref: stream_ref(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(read.last_offset, appended.last_offset);
        let _: CloseStreamResponse = call(
            &runtime,
            "CloseStream",
            CloseStreamRequest {
                r#ref: stream_ref(),
                producer: client(),
                ..Default::default()
            },
        )
        .await;
        let described: DescribeStreamResponse = call(
            &runtime,
            "DescribeStream",
            DescribeStreamRequest {
                r#ref: stream_ref(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(described.status, StreamStatus::Closed as i32);
    }
}
