//! The byte form of the runtime's and the writer's calls, for bridges. A bridge passes the encoded
//! request and gets the encoded response, so it decodes no proto.

use crate::{
    Reason, StreamError, StreamResult, StreamRuntime, StreamWriter, proto::WriteRecordsRequest,
};
use prost::Message;

pub(crate) fn decode<M: Message + Default>(input: &[u8]) -> StreamResult<M> {
    M::decode(input).map_err(|error| StreamError::invalid(format!("The request is bad: {error}")))
}

impl StreamRuntime {
    /// Runs one of the five Stream service operations on encoded input, by its name, and returns
    /// the encoded output. The bridge raises an error with [StreamError::code] and
    /// [StreamError::details].
    pub async fn call(&self, method: &str, input: &[u8]) -> StreamResult<Vec<u8>> {
        let output = match method {
            "CreateStream" => self.create_stream(decode(input)?).await?.encode_to_vec(),
            "AppendRecords" => self.append_records(decode(input)?).await?.encode_to_vec(),
            "CloseStream" => self.close_stream(decode(input)?).await?.encode_to_vec(),
            "ReadRecords" => self.read_records(decode(input)?).await?.encode_to_vec(),
            "DescribeStream" => self.describe_stream(decode(input)?).await?.encode_to_vec(),
            _ => {
                return Err(StreamError::new(
                    Reason::Unsupported,
                    format!("Unknown method {method}"),
                ));
            }
        };
        Ok(output)
    }

    /// Opens a writer from an encoded `OpenWriterRequest`.
    pub async fn open_writer_bytes(&self, input: &[u8]) -> StreamResult<StreamWriter> {
        self.open_writer(decode(input)?).await
    }
}

impl StreamWriter {
    /// Writes an encoded `WriteRecordsRequest`, and returns the encoded `WriteRecordsResponse`.
    pub async fn write_bytes(&self, input: &[u8]) -> StreamResult<Vec<u8>> {
        let request = decode::<WriteRecordsRequest>(input)?;
        Ok(self.write(request).await?.encode_to_vec())
    }

    /// Closes the writer, and returns the encoded `CloseWriterResponse`.
    pub async fn close_bytes(&self) -> StreamResult<Vec<u8>> {
        Ok(self.close().await?.encode_to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use std::sync::Arc;

    struct NoStore;

    impl Store for NoStore {}

    const METHODS: [&str; 5] = [
        "CreateStream",
        "AppendRecords",
        "CloseStream",
        "ReadRecords",
        "DescribeStream",
    ];

    fn runtime() -> Arc<StreamRuntime> {
        StreamRuntime::new("test", "", Arc::new(NoStore), None)
    }

    // Each method decodes its request before anything else. A typo in a method name would make it
    // unknown, which is unsupported, so the loop also checks the names that a bridge calls.
    #[tokio::test]
    async fn bad_bytes_are_invalid_and_an_unknown_method_is_unsupported() {
        let runtime = runtime();
        for method in METHODS {
            let error = runtime.call(method, &[0xff]).await.unwrap_err();
            assert_eq!(error.reason, Reason::Invalid, "{method}");
        }
        let error = runtime.open_writer_bytes(&[0xff]).await.unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        let error = runtime.call("Nope", &[]).await.unwrap_err();
        assert_eq!(
            (error.reason, error.message.as_str()),
            (Reason::Unsupported, "Unknown method Nope")
        );
    }
}
