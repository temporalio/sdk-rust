use futures_util::{FutureExt, future::BoxFuture};
use temporalio_common::{
    data_converters::{PayloadCodec, SerializationContextData},
    protos::temporal::api::common::v1::Payload,
};

pub(crate) struct XorCodec;

impl PayloadCodec for XorCodec {
    fn encode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move {
            payloads
                .into_iter()
                .map(|mut payload| {
                    payload.data.iter_mut().for_each(|byte| *byte ^= 0x42);
                    payload
                })
                .collect()
        }
        .boxed()
    }

    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        self.encode(context, payloads)
    }
}
