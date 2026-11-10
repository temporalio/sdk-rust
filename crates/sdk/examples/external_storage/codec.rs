#![allow(unreachable_pub)]
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use futures_util::{FutureExt, future::BoxFuture};
use prost::Message;
use std::{
    collections::HashMap,
    io::{Read, Write},
};
use temporalio_common::{
    data_converters::{PayloadCodec, PayloadConversionError, SerializationContextData},
    protos::{ENCODING_PAYLOAD_KEY, temporal::api::common::v1::Payload},
};

const ZLIB_ENCODING: &[u8] = b"binary/zlib";

/// Compresses each payload, so the size compared against the external storage threshold is the
/// compressed size.
pub struct ZlibCodec;

impl PayloadCodec for ZlibCodec {
    fn encode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        async move {
            payloads
                .into_iter()
                .map(|payload| {
                    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                    encoder.write_all(&payload.encode_to_vec())?;
                    Ok(Payload {
                        metadata: HashMap::from([(
                            ENCODING_PAYLOAD_KEY.to_owned(),
                            ZLIB_ENCODING.to_vec(),
                        )]),
                        data: encoder.finish()?,
                        ..Default::default()
                    })
                })
                .collect::<std::io::Result<_>>()
                .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
        }
        .boxed()
    }

    fn decode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        async move {
            payloads
                .into_iter()
                .map(|payload| {
                    if payload
                        .metadata
                        .get(ENCODING_PAYLOAD_KEY)
                        .map(Vec::as_slice)
                        != Some(ZLIB_ENCODING)
                    {
                        return Ok(payload);
                    }
                    let mut decompressed = Vec::new();
                    ZlibDecoder::new(payload.data.as_slice()).read_to_end(&mut decompressed)?;
                    Payload::decode(decompressed.as_slice()).map_err(std::io::Error::other)
                })
                .collect::<std::io::Result<_>>()
                .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
        }
        .boxed()
    }
}
