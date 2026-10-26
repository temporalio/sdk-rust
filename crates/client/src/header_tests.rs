use super::*;
use crate::test_helpers::{FailingCodec, XorCodec};
use temporalio_common::{
    data_converters::{DefaultFailureConverter, PayloadCodec, PayloadConverter},
    protos::temporal::api::common::v1::Payload,
};

type HeaderCodecCall = (SerializationContextData, Vec<Payload>);

pub(crate) struct RecordingHeaderCodec {
    pub calls: Arc<std::sync::Mutex<Vec<HeaderCodecCall>>>,
    pub output_count: usize,
}

impl PayloadCodec for RecordingHeaderCodec {
    fn encode(
        &self,
        context: &SerializationContextData,
        mut payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        self.calls
            .lock()
            .unwrap()
            .push((context.clone(), payloads.clone()));
        let output_count = self.output_count;
        Box::pin(async move {
            for payload in &mut payloads {
                payload.data.push(42);
            }
            Ok(payloads.into_iter().cycle().take(output_count).collect())
        })
    }

    fn decode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        Box::pin(async move { Ok(payloads) })
    }
}

#[derive(Clone)]
struct HeaderClient {
    converter: DataConverter,
}

impl NamespacedClient for HeaderClient {
    fn namespace(&self) -> String {
        "namespace".into()
    }

    fn identity(&self) -> String {
        "identity".into()
    }

    fn data_converter(&self) -> &DataConverter {
        &self.converter
    }
}

impl WorkflowService for HeaderClient {
    fn start_activity_execution(
        &mut self,
        request: tonic::Request<StartActivityExecutionRequest>,
    ) -> BoxFuture<'_, Result<tonic::Response<StartActivityExecutionResponse>, tonic::Status>> {
        assert_eq!(
            request.into_inner().header.unwrap().fields["custom"].data,
            vec![3, 42]
        );
        Box::pin(async {
            Ok(tonic::Response::new(
                StartActivityExecutionResponse::default(),
            ))
        })
    }
}

#[tokio::test]
async fn standalone_activity_headers_use_activity_context() {
    let calls = Arc::default();
    let client = HeaderClient {
        converter: DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            RecordingHeaderCodec {
                calls: Arc::clone(&calls),
                output_count: 1,
            },
        )
        .with_encode_headers(true),
    };
    client
        .start_activity(
            UntypedActivity::new("activity"),
            temporalio_common::data_converters::RawValue::empty(),
            ActivityStartOptions::with_start_to_close_timeout(
                "queue",
                "id",
                Duration::from_secs(10),
            )
            .header(Header {
                fields: [(
                    "custom".into(),
                    Payload {
                        data: vec![3],
                        ..Default::default()
                    },
                )]
                .into(),
            })
            .build(),
        )
        .await
        .unwrap();
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert!(calls.iter().all(|(context, _)| *context
        == SerializationContextData::Activity(ActivitySerializationContext::new())));
    assert_eq!(calls[1].1[0].data, vec![3]);
}

#[tokio::test]
async fn client_headers_codec_policy() {
    let workflow = SerializationContextData::Workflow(WorkflowSerializationContext::new());
    let activity = SerializationContextData::Activity(ActivitySerializationContext::new());
    let payload = DataConverter::default()
        .to_payload(&workflow, &"trace carrier")
        .await
        .unwrap();
    let original = Header {
        fields: ["custom", "_tracer-data"]
            .map(|key| (key.to_owned(), payload.clone()))
            .into(),
    };
    for policy in [None, Some(false), Some(true)] {
        let converter = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            XorCodec,
        );
        let converter = match policy {
            None => converter,
            Some(enabled) => converter.with_encode_headers(enabled),
        };
        let client = HeaderClient { converter };
        let enabled = policy.unwrap_or(false);
        for context in [&workflow, &activity] {
            let mut sent = encode_client_headers(&client, Some(original.clone()), context)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sent == original, !enabled);
            if enabled {
                for value in sent.fields.values() {
                    assert_eq!(value.metadata, payload.metadata);
                    assert_eq!(
                        value.data,
                        payload
                            .data
                            .iter()
                            .map(|byte| byte ^ 0x42)
                            .collect::<Vec<_>>()
                    );
                }
                decode_payloads(&mut sent, client.converter.codec(), context)
                    .await
                    .unwrap();
                assert_eq!(sent, original);
            }
        }
        let sent = build_start_workflow_request(
            &client,
            "workflow".into(),
            None,
            None,
            WorkflowStartOptions::new("queue", "id")
                .header(original.clone())
                .build(),
        )
        .await
        .unwrap();
        assert_eq!(sent.header.as_ref() == Some(&original), !enabled);
    }
    assert_eq!(original.fields["_tracer-data"], payload);
}

#[tokio::test]
async fn client_headers_codec_errors_propagate() {
    let context = SerializationContextData::Workflow(WorkflowSerializationContext::new());
    let header = Header {
        fields: [("header".into(), Default::default())].into(),
    };
    let mut client = HeaderClient {
        converter: DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            FailingCodec,
        ),
    };
    assert_eq!(
        encode_client_headers(&client, Some(header.clone()), &context)
            .await
            .unwrap(),
        Some(header.clone())
    );
    client.converter = client.converter.with_encode_headers(true);
    assert!(
        encode_client_headers(&client, Some(header), &context)
            .await
            .is_err()
    );
}

#[rstest::rstest]
#[case(0)]
#[case(2)]
#[tokio::test]
async fn client_headers_require_one_encoded_payload(#[case] output_count: usize) {
    let client = HeaderClient {
        converter: DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            RecordingHeaderCodec {
                calls: Default::default(),
                output_count,
            },
        )
        .with_encode_headers(true),
    };
    let error = encode_client_headers(
        &client,
        Some(Header {
            fields: [("header".into(), Payload::default())].into(),
        }),
        &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, PayloadConversionError::EncodingError(_)));
    assert!(
        error
            .to_string()
            .contains(&format!("codec returned {output_count}"))
    );
}
