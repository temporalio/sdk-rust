//! Payload visitor infrastructure for applying codecs to proto messages.
//!
//! This module provides a visitor pattern for traversing proto messages and transforming
//! payload fields. It's used to apply `PayloadCodec` encode/decode operations at the
//! boundary between the SDK and Core.

use crate::{
    data_converters::{
        DataConverter, PayloadCodec, PayloadConversionError, SerializationContextData,
    },
    protos::temporal::api::common::v1::{Payload, Payloads},
};
use futures::future::BoxFuture;

/// Represents a payload field in a proto message.
/// Payloads within the same field may be processed together by the codec.
pub struct PayloadField<'a> {
    /// The fully-qualified field path (e.g.,
    /// `coresdk.workflow_commands.ScheduleActivity.arguments`)
    pub path: &'static str,
    /// The payload data
    pub data: PayloadFieldData<'a>,
}

/// The payload data within a field, varying by field type.
pub enum PayloadFieldData<'a> {
    /// A singular [Payload] field
    Single(&'a mut Payload),
    /// A repeated [Payload] field
    Repeated(&'a mut Vec<Payload>),
    /// A [Payloads] message field
    Payloads(&'a mut Payloads),
}

/// Async visitor for transforming payload fields.
pub trait AsyncPayloadVisitor {
    /// Visit a payload field, potentially transforming it. Returning an error stops traversal.
    fn visit<'a>(
        &'a mut self,
        field: PayloadField<'a>,
    ) -> BoxFuture<'a, Result<(), PayloadConversionError>>;
}

/// Trait for messages that contain Payload fields (directly or transitively).
/// Generated via codegen for all relevant proto message types.
pub trait PayloadVisitable: Send {
    /// Visit all payload fields in this message.
    /// The visitor is called once per field, receiving the field's payload(s).
    /// Traversal stops and returns the first visitor error.
    fn visit_payloads_mut<'a>(
        &'a mut self,
        visitor: &'a mut (dyn AsyncPayloadVisitor + Send),
    ) -> BoxFuture<'a, Result<(), PayloadConversionError>>;
}

/// Check if a field path represents search attributes that must not be transformed.
/// Search attributes must remain server-readable for indexing.
fn is_search_attributes_path(path: &str) -> bool {
    // All search attributes go through the SearchAttributes message which has indexed_fields
    path.contains("SearchAttributes.indexed_fields")
}

/// Whether a field at this path may be transformed: encoded, decoded, or offloaded.
pub(crate) fn should_transform(path: &str) -> bool {
    !is_search_attributes_path(path)
}

/// Implement [`AsyncPayloadVisitor`] for a visitor that applies one batch operation to every
/// transformable field, given `|visitor, payloads| body`. The body takes the field's payloads as an
/// owned `Vec<Payload>` and evaluates to the payloads that replace them.
macro_rules! payload_visitor {
    ($visitor:ty, |$this:ident, $payloads:ident| $body:expr) => {
        impl $crate::payload_visitor::AsyncPayloadVisitor for $visitor {
            fn visit<'a>(
                &'a mut self,
                field: $crate::payload_visitor::PayloadField<'a>,
            ) -> ::futures::future::BoxFuture<
                'a,
                Result<(), $crate::data_converters::PayloadConversionError>,
            > {
                Box::pin(async move {
                    if !$crate::payload_visitor::should_transform(field.path) {
                        return Ok(());
                    }
                    let $this = self;
                    match field.data {
                        $crate::payload_visitor::PayloadFieldData::Single(payload) => {
                            let $payloads = vec![::std::mem::take(payload)];
                            if let Some(p) = $body.into_iter().next() {
                                *payload = p;
                            }
                        }
                        $crate::payload_visitor::PayloadFieldData::Repeated(payloads) => {
                            let $payloads = ::std::mem::take(payloads);
                            *payloads = $body;
                        }
                        $crate::payload_visitor::PayloadFieldData::Payloads(message) => {
                            let $payloads = ::std::mem::take(&mut message.payloads);
                            message.payloads = $body;
                        }
                    }
                    Ok(())
                })
            }
        }
    };
}
pub(crate) use payload_visitor;

/// Visitor that encodes payloads using a codec.
pub struct EncodeVisitor<'a> {
    codec: &'a (dyn PayloadCodec + Send + Sync),
    context: &'a SerializationContextData,
}

payload_visitor!(EncodeVisitor<'_>, |this, payloads| {
    this.codec.encode(this.context, payloads).await?
});

/// Visitor that decodes payloads using a codec.
pub struct DecodeVisitor<'a> {
    codec: &'a (dyn PayloadCodec + Send + Sync),
    context: &'a SerializationContextData,
}

payload_visitor!(DecodeVisitor<'_>, |this, payloads| {
    this.codec.decode(this.context, payloads).await?
});

/// Encode all payloads in a message using the given codec.
pub async fn encode_payloads<M: PayloadVisitable + Send>(
    msg: &mut M,
    codec: &(dyn PayloadCodec + Send + Sync),
    context: &SerializationContextData,
) -> Result<(), PayloadConversionError> {
    let mut visitor = EncodeVisitor { codec, context };
    msg.visit_payloads_mut(&mut visitor).await
}

/// Decode all payloads in a message using the given codec.
pub async fn decode_payloads<M: PayloadVisitable + Send>(
    msg: &mut M,
    codec: &(dyn PayloadCodec + Send + Sync),
    context: &SerializationContextData,
) -> Result<(), PayloadConversionError> {
    let mut visitor = DecodeVisitor { codec, context };
    msg.visit_payloads_mut(&mut visitor).await
}

/// Offload every eligible payload in an outbound message to the data converter's external storage.
/// Runs after [`encode_payloads`], since the size threshold applies to encoded payloads. Does
/// nothing without the `experimental` feature.
///
/// On error, reported as [`PayloadConversionError::ExternalStorage`], the message is left partly
/// processed and must not be sent.
#[doc(hidden)]
pub async fn store_external_payloads<M: PayloadVisitable + Send>(
    msg: &mut M,
    data_converter: &DataConverter,
) -> Result<(), PayloadConversionError> {
    // Stored without a target until the worker and client can name the execution a message is for.
    #[cfg(feature = "experimental")]
    crate::external_storage_runner::store_in(data_converter.external_storage(), msg, None).await?;
    #[cfg(not(feature = "experimental"))]
    let _ = (msg, data_converter);
    Ok(())
}

/// Replace every external storage reference in an inbound message with the payload it points at.
/// Runs before [`decode_payloads`], since a driver returns a payload as it was before offloading.
/// Without external storage to retrieve them, which includes every build without the
/// `experimental` feature, a reference is an error instead. Failures are reported as
/// [`PayloadConversionError::ExternalStorage`].
///
/// On error the message is left partly processed and must not be used.
#[doc(hidden)]
pub async fn retrieve_external_payloads<M: PayloadVisitable + Send>(
    msg: &mut M,
    data_converter: &DataConverter,
) -> Result<(), PayloadConversionError> {
    #[cfg(feature = "experimental")]
    crate::external_storage_runner::retrieve_in(data_converter.external_storage(), msg).await?;
    #[cfg(not(feature = "experimental"))]
    {
        let _ = data_converter;
        crate::external_storage_references::reject_references_in(msg).await?;
    }
    Ok(())
}

// Manual impl for Payload - visits itself as a single payload
impl PayloadVisitable for Payload {
    fn visit_payloads_mut<'a>(
        &'a mut self,
        visitor: &'a mut (dyn AsyncPayloadVisitor + Send),
    ) -> BoxFuture<'a, Result<(), PayloadConversionError>> {
        Box::pin(async move {
            visitor
                .visit(PayloadField {
                    path: "temporal.api.common.v1.Payload",
                    data: PayloadFieldData::Single(self),
                })
                .await
        })
    }
}

// Manual impl for Payloads - visits itself as a Payloads field
impl PayloadVisitable for Payloads {
    fn visit_payloads_mut<'a>(
        &'a mut self,
        visitor: &'a mut (dyn AsyncPayloadVisitor + Send),
    ) -> BoxFuture<'a, Result<(), PayloadConversionError>> {
        Box::pin(async move {
            visitor
                .visit(PayloadField {
                    path: "temporal.api.common.v1.Payloads",
                    data: PayloadFieldData::Payloads(self),
                })
                .await
        })
    }
}

// Include the generated PayloadVisitable implementations
include!(concat!(env!("OUT_DIR"), "/payload_visitor_impl.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_converters::{ExternalStorageError, WorkflowSerializationContext},
        protos::{
            coresdk::{
                activity_result::{
                    ActivityResolution, Success, activity_resolution::Status as ActivityStatus,
                },
                workflow_activation::{
                    InitializeWorkflow, ResolveActivity, WorkflowActivation, WorkflowActivationJob,
                    workflow_activation_job::Variant,
                },
                workflow_commands::{
                    ContinueAsNewWorkflowExecution, ScheduleActivity, StartChildWorkflowExecution,
                    UpsertWorkflowSearchAttributes, WorkflowCommand,
                    workflow_command::Variant as CmdVariant,
                },
                workflow_completion::{
                    WorkflowActivationCompletion, workflow_activation_completion::Status,
                },
            },
            temporal::api::{
                common::v1::{Memo, SearchAttributes},
                failure::v1::failure::FailureInfo,
                workflow::v1::WorkflowExecutionInfo,
                workflowservice::v1::DescribeWorkflowExecutionResponse,
            },
        },
    };
    use futures::FutureExt;
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use temporalio_common_wasm::{
        data_converters::{DefaultFailureConverter, FailureConverter, PayloadConverter},
        error::{ApplicationFailure, OutgoingError, OutgoingWorkflowError},
    };

    struct MarkingCodec;
    impl PayloadCodec for MarkingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            async move {
                Ok(payloads
                    .into_iter()
                    .map(|mut p| {
                        p.metadata.insert("encoded".to_string(), b"true".to_vec());
                        p
                    })
                    .collect())
            }
            .boxed()
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            async move {
                Ok(payloads
                    .into_iter()
                    .map(|mut p| {
                        p.metadata.insert("decoded".to_string(), b"true".to_vec());
                        p
                    })
                    .collect())
            }
            .boxed()
        }
    }

    #[derive(Default)]
    struct FailingCodec {
        encode_calls: AtomicUsize,
        decode_calls: AtomicUsize,
    }

    impl PayloadCodec for FailingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            self.encode_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(PayloadConversionError::EncodingError(
                    "visitor encode failed".into(),
                ))
            }
            .boxed()
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            self.decode_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(PayloadConversionError::EncodingError(
                    "visitor decode failed".into(),
                ))
            }
            .boxed()
        }
    }

    struct PathRecordingVisitor {
        visited_paths: Vec<String>,
    }
    impl PathRecordingVisitor {
        fn new() -> Self {
            Self {
                visited_paths: Vec::new(),
            }
        }

        fn paths(&self) -> Vec<String> {
            self.visited_paths.clone()
        }
    }

    impl AsyncPayloadVisitor for PathRecordingVisitor {
        fn visit<'a>(
            &'a mut self,
            field: PayloadField<'a>,
        ) -> BoxFuture<'a, Result<(), PayloadConversionError>> {
            let path = field.path.to_string();
            self.visited_paths.push(path);
            async move { Ok(()) }.boxed()
        }
    }

    fn make_payload(data: &str) -> Payload {
        Payload {
            metadata: HashMap::new(),
            data: data.as_bytes().to_vec(),
            external_payloads: vec![],
        }
    }

    fn is_encoded(p: &Payload) -> bool {
        p.metadata.contains_key("encoded")
    }

    fn is_decoded(p: &Payload) -> bool {
        p.metadata.contains_key("decoded")
    }

    #[tokio::test]
    async fn test_direct_visitor_records_paths() {
        let mut activation = WorkflowActivation {
            run_id: "test-run".to_string(),
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::InitializeWorkflow(InitializeWorkflow {
                    workflow_type: "test-workflow".to_string(),
                    arguments: vec![make_payload("input1")],
                    headers: {
                        let mut h = HashMap::new();
                        h.insert("header-key".to_string(), make_payload("header-value"));
                        h
                    },
                    memo: Some(Memo {
                        fields: {
                            let mut m = HashMap::new();
                            m.insert("memo-key".to_string(), make_payload("memo-value"));
                            m
                        },
                    }),
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        let mut visitor = PathRecordingVisitor::new();
        activation.visit_payloads_mut(&mut visitor).await.unwrap();

        let paths = visitor.paths();
        assert!(
            paths
                .iter()
                .any(|p| p.contains("InitializeWorkflow.arguments")),
            "should visit arguments, got: {:?}",
            paths
        );
        assert!(
            paths
                .iter()
                .any(|p| p.contains("InitializeWorkflow.headers")),
            "should visit headers, got: {:?}",
            paths
        );
        assert!(
            paths.iter().any(|p| p.contains("Memo.fields")),
            "should visit memo fields, got: {:?}",
            paths
        );
    }

    #[tokio::test]
    async fn test_encode_workflow_activation_completion_with_schedule_activity() {
        let mut completion = WorkflowActivationCompletion {
            run_id: "test-run".to_string(),
            status: Some(Status::Successful(
                crate::protos::coresdk::workflow_completion::Success {
                    commands: vec![WorkflowCommand {
                        variant: Some(CmdVariant::ScheduleActivity(ScheduleActivity {
                            activity_id: "act-1".to_string(),
                            activity_type: "test-activity".to_string(),
                            arguments: vec![make_payload("arg1"), make_payload("arg2")],
                            headers: {
                                let mut h = HashMap::new();
                                h.insert("header-key".to_string(), make_payload("header-value"));
                                h
                            },
                            ..Default::default()
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        encode_payloads(
            &mut completion,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        let status = completion.status.as_ref().unwrap();
        let Status::Successful(success) = status else {
            panic!("Expected successful status")
        };
        let cmd = &success.commands[0];
        let CmdVariant::ScheduleActivity(schedule) = cmd.variant.as_ref().unwrap() else {
            panic!("Expected ScheduleActivity")
        };

        assert!(is_encoded(&schedule.arguments[0]), "arg1 should be encoded");
        assert!(is_encoded(&schedule.arguments[1]), "arg2 should be encoded");
        assert!(
            is_encoded(schedule.headers.get("header-key").unwrap()),
            "header should be encoded"
        );
    }

    #[tokio::test]
    async fn test_decode_workflow_activation_with_initialize() {
        let mut activation = WorkflowActivation {
            run_id: "test-run".to_string(),
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::InitializeWorkflow(InitializeWorkflow {
                    workflow_type: "test-workflow".to_string(),
                    arguments: vec![make_payload("input1"), make_payload("input2")],
                    headers: {
                        let mut h = HashMap::new();
                        h.insert("header-key".to_string(), make_payload("header-value"));
                        h
                    },
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        decode_payloads(
            &mut activation,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        let job = &activation.jobs[0];
        let Variant::InitializeWorkflow(init) = job.variant.as_ref().unwrap() else {
            panic!("Expected InitializeWorkflow")
        };

        assert!(is_decoded(&init.arguments[0]), "arg1 should be decoded");
        assert!(is_decoded(&init.arguments[1]), "arg2 should be decoded");
        assert!(
            is_decoded(init.headers.get("header-key").unwrap()),
            "header should be decoded"
        );
    }

    #[tokio::test]
    async fn test_decode_workflow_activation_with_resolve_activity() {
        let mut activation = WorkflowActivation {
            run_id: "test-run".to_string(),
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::ResolveActivity(ResolveActivity {
                    seq: 1,
                    result: Some(ActivityResolution {
                        status: Some(ActivityStatus::Completed(Success {
                            result: Some(make_payload("activity-result")),
                        })),
                    }),
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        decode_payloads(
            &mut activation,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        let job = &activation.jobs[0];
        let Variant::ResolveActivity(resolve) = job.variant.as_ref().unwrap() else {
            panic!("Expected ResolveActivity")
        };
        let ActivityStatus::Completed(success) =
            resolve.result.as_ref().unwrap().status.as_ref().unwrap()
        else {
            panic!("Expected Completed status")
        };

        assert!(
            is_decoded(success.result.as_ref().unwrap()),
            "activity result should be decoded"
        );
    }

    #[tokio::test]
    async fn test_search_attributes_skipped_on_encode() {
        // Test that search attributes are NOT encoded (they must remain server-readable)
        let mut completion = WorkflowActivationCompletion {
            run_id: "test-run".to_string(),
            status: Some(Status::Successful(
                crate::protos::coresdk::workflow_completion::Success {
                    commands: vec![
                        // UpsertWorkflowSearchAttributes command
                        WorkflowCommand {
                            variant: Some(CmdVariant::UpsertWorkflowSearchAttributes(
                                UpsertWorkflowSearchAttributes {
                                    search_attributes: Some(SearchAttributes {
                                        indexed_fields: {
                                            let mut sa = HashMap::new();
                                            sa.insert(
                                                "CustomField".to_string(),
                                                make_payload("search-value"),
                                            );
                                            sa
                                        },
                                    }),
                                },
                            )),
                            ..Default::default()
                        },
                        // ContinueAsNewWorkflowExecution command
                        WorkflowCommand {
                            variant: Some(CmdVariant::ContinueAsNewWorkflowExecution(
                                ContinueAsNewWorkflowExecution {
                                    arguments: vec![make_payload("continue-arg")],
                                    search_attributes: Some(SearchAttributes {
                                        indexed_fields: {
                                            let mut sa = HashMap::new();
                                            sa.insert(
                                                "CustomField".to_string(),
                                                make_payload("continue-search-value"),
                                            );
                                            sa
                                        },
                                    }),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        },
                        // StartChildWorkflowExecution command
                        WorkflowCommand {
                            variant: Some(CmdVariant::StartChildWorkflowExecution(
                                StartChildWorkflowExecution {
                                    seq: 1,
                                    workflow_type: "child-workflow".to_string(),
                                    input: vec![make_payload("child-arg")],
                                    search_attributes: Some(SearchAttributes {
                                        indexed_fields: {
                                            let mut sa = HashMap::new();
                                            sa.insert(
                                                "CustomField".to_string(),
                                                make_payload("child-search-value"),
                                            );
                                            sa
                                        },
                                    }),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        encode_payloads(
            &mut completion,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        let status = completion.status.as_ref().unwrap();
        let Status::Successful(success) = status else {
            panic!("Expected successful status")
        };

        // UpsertWorkflowSearchAttributes - search attributes should NOT be encoded
        let CmdVariant::UpsertWorkflowSearchAttributes(upsert) =
            success.commands[0].variant.as_ref().unwrap()
        else {
            panic!("Expected UpsertWorkflowSearchAttributes")
        };
        let sa = upsert.search_attributes.as_ref().unwrap();
        assert!(
            !is_encoded(sa.indexed_fields.get("CustomField").unwrap()),
            "search attributes should NOT be encoded"
        );

        // ContinueAsNewWorkflowExecution - arguments encoded, search attributes NOT
        let CmdVariant::ContinueAsNewWorkflowExecution(continue_as_new) =
            success.commands[1].variant.as_ref().unwrap()
        else {
            panic!("Expected ContinueAsNewWorkflowExecution")
        };
        assert!(
            is_encoded(&continue_as_new.arguments[0]),
            "arguments should be encoded"
        );
        let sa = continue_as_new.search_attributes.as_ref().unwrap();
        assert!(
            !is_encoded(sa.indexed_fields.get("CustomField").unwrap()),
            "search attributes should NOT be encoded"
        );

        // StartChildWorkflowExecution - input encoded, search attributes NOT
        let CmdVariant::StartChildWorkflowExecution(start_child) =
            success.commands[2].variant.as_ref().unwrap()
        else {
            panic!("Expected StartChildWorkflowExecution")
        };
        assert!(is_encoded(&start_child.input[0]), "input should be encoded");
        let sa = start_child.search_attributes.as_ref().unwrap();
        assert!(
            !is_encoded(sa.indexed_fields.get("CustomField").unwrap()),
            "search attributes should NOT be encoded"
        );
    }

    #[tokio::test]
    async fn test_search_attributes_skipped_on_decode() {
        let mut response = DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(WorkflowExecutionInfo {
                memo: Some(Memo {
                    fields: {
                        let mut memo = HashMap::new();
                        memo.insert("tracked".to_string(), make_payload("memo-value"));
                        memo
                    },
                }),
                search_attributes: Some(SearchAttributes {
                    indexed_fields: {
                        let mut sa = HashMap::new();
                        sa.insert("CustomField".to_string(), make_payload("search-value"));
                        sa
                    },
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        decode_payloads(
            &mut response,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        let info = response.workflow_execution_info.as_ref().unwrap();
        assert!(
            is_decoded(info.memo.as_ref().unwrap().fields.get("tracked").unwrap()),
            "memo should be decoded"
        );
        assert!(
            !is_decoded(
                info.search_attributes
                    .as_ref()
                    .unwrap()
                    .indexed_fields
                    .get("CustomField")
                    .unwrap()
            ),
            "search attributes should NOT be decoded"
        );
    }

    #[tokio::test]
    async fn test_encode_single_payload() {
        let mut payload = make_payload("test-data");

        encode_payloads(
            &mut payload,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        assert!(is_encoded(&payload), "single payload should be encoded");
    }

    #[tokio::test]
    async fn test_decode_single_payload() {
        let mut payload = make_payload("test-data");

        decode_payloads(
            &mut payload,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        assert!(is_decoded(&payload), "single payload should be decoded");
    }

    #[tokio::test]
    async fn test_encode_payloads_message() {
        let mut payloads = Payloads {
            payloads: vec![make_payload("p1"), make_payload("p2"), make_payload("p3")],
        };

        encode_payloads(
            &mut payloads,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        for (i, p) in payloads.payloads.iter().enumerate() {
            assert!(is_encoded(p), "payload {} should be encoded", i);
        }
    }

    #[tokio::test]
    async fn test_codec_errors_stop_subsequent_codec_invocations() {
        let codec = FailingCodec::default();
        let mut activation = WorkflowActivation {
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::InitializeWorkflow(InitializeWorkflow {
                    arguments: vec![make_payload("input")],
                    headers: HashMap::from([("header".to_string(), make_payload("value"))]),
                    memo: Some(Memo {
                        fields: HashMap::from([("memo".to_string(), make_payload("memo-value"))]),
                    }),
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        let err = decode_payloads(
            &mut activation,
            &codec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), "Encoding error: visitor decode failed");
        assert_eq!(codec.decode_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_encode_failure_encodes_application_failure_details() {
        let mut failure = DefaultFailureConverter::default().to_failure(
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::builder(anyhow::anyhow!("app boom"))
                    .details(crate::data_converters::RawValue::new(vec![make_payload(
                        "detail",
                    )]))
                    .build(),
            ))),
            &PayloadConverter::default(),
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        );

        encode_payloads(
            &mut failure,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await
        .unwrap();

        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info")
        };
        assert!(is_encoded(&info.details.unwrap().payloads[0]));
    }

    #[tokio::test]
    async fn unretrievable_reference_is_a_payload_conversion_error() {
        let reference = crate::external_storage_references::create_reference_payload(
            "mem",
            HashMap::from([("key".to_owned(), "a".to_owned())]),
            99,
        )
        .unwrap();
        let mut message = Payloads {
            payloads: vec![reference],
        };

        let err = retrieve_external_payloads(&mut message, &DataConverter::default())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            PayloadConversionError::ExternalStorage(ExternalStorageError::NotConfigured)
        ));
        assert!(err.to_string().contains("TMPRL1105"));
    }
}
