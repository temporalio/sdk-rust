//! Experimental, typed Temporal-managed Nexus executions outside workflows.
//!
//! These APIs call Temporal's standalone Nexus execution RPCs, not Nexus HTTP endpoints.
//! They require a server with standalone Nexus execution support and a registered endpoint.
//! Dropping a future or handle does not cancel a durable execution.
//!
//! ```
//! use nexus_rpc::service;
//! use temporalio_client::{Client, nexus::*};
//! #[service]
//! trait Payments {
//!     #[operation]
//!     fn charge(input: String) -> String;
//! }
//! async fn charge(client: &Client) -> Result<String, NexusClientError> {
//!     let payments = client.nexus_client(Payments, "payments-endpoint");
//!     let handle = payments
//!         .start(
//!             Payments::charge,
//!             "order".into(),
//!             NexusOperationStartOptions::new("order-123").build(),
//!         )
//!         .await?;
//!     // Persist both IDs to reconnect to this exact execution after a process restart.
//!     let resumed = client.get_nexus_operation_handle::<String>(
//!         handle.operation_id(),
//!         handle.run_id().map(str::to_owned),
//!     )?;
//!     resumed.result().await
//! }
//! # fn main() {}
//! ```

pub use nexus_rpc::{NexusOperation, NexusService};
pub use temporalio_common::error::NexusOperationError;

use crate::{Client, NamespacedClient, Next, RpcOptions, grpc::WorkflowService, interceptors};
use futures_util::{
    FutureExt, StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use std::{marker::PhantomData, time::Duration};
use temporalio_common::{
    data_converters::{
        GenericPayloadConverter, NexusOperationDecodeHint, NexusSerializationContext,
        PayloadConversionError, RawValue, SerializationContext, SerializationContextData,
        TemporalDeserializable, TemporalSerializable,
    },
    external_storage::{retrieve_external_payloads, store_external_payloads},
    payload_visitor::decode_payloads,
    protos::{
        temporal::api::{
            enums::v1::{
                NexusOperationIdConflictPolicy, NexusOperationIdReusePolicy,
                NexusOperationWaitStage,
            },
            errordetails::v1::NexusOperationExecutionAlreadyStartedFailure,
            nexus::v1::NexusOperationExecutionListInfo,
            sdk::v1::UserMetadata,
            workflowservice::v1::{
                CountNexusOperationExecutionsRequest, CountNexusOperationExecutionsResponse,
                DescribeNexusOperationExecutionRequest, DescribeNexusOperationExecutionResponse,
                ListNexusOperationExecutionsRequest, PollNexusOperationExecutionRequest,
                PollNexusOperationExecutionResponse, RequestCancelNexusOperationExecutionRequest,
                StartNexusOperationExecutionRequest, TerminateNexusOperationExecutionRequest,
                poll_nexus_operation_execution_response::Outcome,
            },
        },
        utilities::decode_status_detail,
    },
    search_attributes::SearchAttributes,
};
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// Errors from the standalone Nexus API.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NexusClientError {
    /// A Temporal RPC failed.
    #[error("Nexus RPC failed: {0}")]
    Rpc(#[from] tonic::Status),
    /// An existing execution has the same ID and the reuse or conflict policy prevents starting.
    #[error("Nexus operation {operation_id:?} already started with run ID: {run_id:?}")]
    AlreadyStarted {
        /// Caller-assigned operation ID.
        operation_id: String,
        /// Run ID of the conflicting execution, if reported by the server.
        run_id: Option<String>,
        /// The original gRPC status.
        #[source]
        source: tonic::Status,
    },
    /// Input, output, or failure payload conversion failed.
    #[error("Nexus payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
    /// The operation completed unsuccessfully.
    #[error("{source} (operation ID: {operation_id}, run ID: {run_id})")]
    Operation {
        /// Caller-assigned operation ID.
        operation_id: String,
        /// Run ID of the failed execution.
        run_id: String,
        /// The operation failure and its decoded cause.
        #[source]
        source: NexusOperationError,
    },
    /// The server returned an inconsistent or incomplete response.
    #[error("Malformed Nexus response: {0}")]
    MalformedResponse(&'static str),
    /// The operation ID or an explicitly supplied run ID is empty.
    #[error("Nexus operation ID and any supplied run ID must not be empty")]
    InvalidExecutionId,
    /// A supplied timeout cannot be represented in the Temporal API.
    #[error("Nexus {0} timeout is out of range")]
    InvalidTimeout(&'static str),
}

/// Options for starting a standalone Nexus execution.
#[derive(Debug, Clone, bon::Builder)]
#[builder(start_fn = new, on(String, into))]
#[non_exhaustive]
pub struct NexusOperationStartOptions {
    /// Caller-assigned ID, unique in the namespace subject to reuse/conflict policies.
    #[builder(start_fn)]
    pub operation_id: String,
    /// Maximum total execution time, including retries.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Maximum time until the handler starts or synchronously completes.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum time after an asynchronous handler starts.
    pub start_to_close_timeout: Option<Duration>,
    /// Policy for a previously closed operation with the same ID. Defaults to allowing duplicates.
    #[builder(default = NexusOperationIdReusePolicy::AllowDuplicate)]
    pub id_reuse_policy: NexusOperationIdReusePolicy,
    /// Policy for an already running operation with the same ID. Defaults to failing the start.
    #[builder(default = NexusOperationIdConflictPolicy::Fail)]
    pub id_conflict_policy: NexusOperationIdConflictPolicy,
    /// Indexed attributes. These are not passed through payload codecs.
    pub search_attributes: Option<SearchAttributes>,
    /// Fixed UI summary, encoded with the default payload converter.
    pub static_summary: Option<String>,
    /// Fixed UI details, encoded with the default payload converter.
    pub static_details: Option<String>,
    /// Controls for the start RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// Options for listing standalone Nexus executions.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct NexusOperationListOptions {
    /// Maximum number of executions requested per page, or zero to use the server default.
    #[builder(default)]
    pub page_size: i32,
    /// Controls for each list page RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// Options for counting standalone Nexus executions.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct NexusOperationCountOptions {
    /// Controls for the count RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// A typed client for a single Nexus service.
///
/// An operation from another service is rejected:
///
/// ```compile_fail
/// use nexus_rpc::service;
/// use temporalio_client::{Client, nexus::*};
/// #[service]
/// trait Payments { #[operation] fn charge(input: String) -> String; }
/// #[service]
/// trait Shipping { #[operation] fn ship(input: String) -> String; }
/// async fn wrong(client: &Client) {
///     let payments = client.nexus_client(Payments, "ep");
///     payments.execute(Shipping::ship, String::new(), NexusOperationStartOptions::new("id").build()).await;
/// }
/// # fn main() {}
/// ```
///
/// Inputs and outputs are determined by the operation:
///
/// ```compile_fail
/// use nexus_rpc::service;
/// use temporalio_client::{Client, nexus::*};
/// #[service]
/// trait Payments { #[operation] fn charge(input: String) -> String; }
/// async fn wrong(client: &Client) {
///     let payments = client.nexus_client(Payments, "ep");
///     payments.execute(Payments::charge, 42u32, NexusOperationStartOptions::new("id").build()).await;
/// }
/// # fn main() {}
/// ```
#[derive(Debug)]
pub struct NexusClient<S> {
    client: Client,
    endpoint: String,
    service: PhantomData<fn() -> S>,
}

impl Client {
    /// Bind this namespace client to a typed Nexus service and registered endpoint.
    pub fn nexus_client<S: NexusService>(
        &self,
        _service: S,
        endpoint: impl Into<String>,
    ) -> NexusClient<S> {
        NexusClient {
            client: self.clone(),
            endpoint: endpoint.into(),
            service: PhantomData,
        }
    }

    /// Get a handle to an existing standalone Nexus operation without making an RPC.
    ///
    /// Supply a run ID to target that exact run, or `None` to target the latest run. Specify
    /// the expected result type as `Output`, or use [`RawValue`] if the type is unknown.
    pub fn get_nexus_operation_handle<Output>(
        &self,
        operation_id: impl Into<String>,
        run_id: Option<String>,
    ) -> Result<NexusOperationHandle<Output>, NexusClientError> {
        let operation_id = operation_id.into();
        if operation_id.is_empty() || run_id.as_ref().is_some_and(String::is_empty) {
            return Err(NexusClientError::InvalidExecutionId);
        }
        Ok(NexusOperationHandle {
            client: self.clone(),
            operation_id,
            run_id,
            output: PhantomData,
        })
    }

    /// List standalone Nexus operations in this namespace that match a visibility query.
    ///
    /// The stream fetches pages lazily. An empty query matches all visible operations.
    pub fn list_nexus_operations(
        &self,
        query: impl Into<String>,
        options: NexusOperationListOptions,
    ) -> BoxStream<'static, Result<NexusOperationExecutionListInfo, NexusClientError>> {
        let client = self.clone();
        let query = query.into();
        stream::try_unfold(Some(Vec::new()), move |next_page_token| {
            let client = client.clone();
            let query = query.clone();
            let options = options.clone();
            async move {
                let Some(next_page_token) = next_page_token else {
                    return Ok(None);
                };
                let response = interceptors::call_list_nexus_operations_page(
                    client.client_interceptors(),
                    interceptors::ListNexusOperationsPageInput {
                        request: ListNexusOperationExecutionsRequest {
                            namespace: client.namespace(),
                            query,
                            page_size: options.page_size,
                            next_page_token,
                        },
                        rpc_options: options.rpc_options,
                    },
                    Next::new(|input: interceptors::ListNexusOperationsPageInput| {
                        let mut client = client.clone();
                        async move {
                            let mut request = tonic::Request::new(input.request);
                            input.rpc_options.apply_to(&mut request);
                            Ok(client
                                .list_nexus_operation_executions(request)
                                .await?
                                .into_inner())
                        }
                        .boxed()
                    }),
                )
                .await?;
                let token =
                    (!response.next_page_token.is_empty()).then_some(response.next_page_token);
                Ok::<_, NexusClientError>(Some((
                    stream::iter(response.operations.into_iter().map(Ok)),
                    token,
                )))
            }
        })
        .try_flatten()
        .boxed()
    }

    /// Count standalone Nexus operations in this namespace that match a visibility query.
    ///
    /// A `GROUP BY` clause returns individual group counts in addition to the approximate total.
    pub async fn count_nexus_operations(
        &self,
        query: impl Into<String>,
        options: NexusOperationCountOptions,
    ) -> Result<CountNexusOperationExecutionsResponse, NexusClientError> {
        interceptors::call_count_nexus_operations(
            self.client_interceptors(),
            interceptors::CountNexusOperationsInput {
                request: CountNexusOperationExecutionsRequest {
                    namespace: self.namespace(),
                    query: query.into(),
                },
                rpc_options: options.rpc_options,
            },
            Next::new(|input: interceptors::CountNexusOperationsInput| {
                async move {
                    let mut request = tonic::Request::new(input.request);
                    input.rpc_options.apply_to(&mut request);
                    Ok(self
                        .clone()
                        .count_nexus_operation_executions(request)
                        .await?
                        .into_inner())
                }
                .boxed()
            }),
        )
        .await
    }
}

impl<S> Clone for NexusClient<S> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
            service: PhantomData,
        }
    }
}

fn context() -> SerializationContextData {
    SerializationContextData::Nexus(NexusSerializationContext::new())
}

impl<S: NexusService> NexusClient<S> {
    /// Schedule a durable execution. This returns after Temporal accepts it, without waiting
    /// for the handler. Use [`NexusOperationHandle::operation_token`] to wait for handler start.
    pub async fn start<O>(
        &self,
        operation: O,
        input: O::Input,
        options: NexusOperationStartOptions,
    ) -> Result<NexusOperationHandle<O::Output>, NexusClientError>
    where
        O: NexusOperation<Service = S>,
        O::Input: TemporalSerializable,
        O::Output: TemporalDeserializable + 'static,
    {
        let operation_id = options.operation_id;
        if operation_id.is_empty() {
            return Err(NexusClientError::InvalidExecutionId);
        }
        let converter = self.client.data_converter();
        let context = context();
        let payload = converter.payload_converter().to_payload(
            &SerializationContext::new(&context, converter.payload_converter()),
            &input,
        )?;
        let mut payloads = converter.codec().encode(&context, vec![payload]).await?;
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding.into());
        }
        let metadata_converter = temporalio_common::data_converters::PayloadConverter::default();
        let metadata_context = SerializationContext::new(&context, &metadata_converter);
        let request = StartNexusOperationExecutionRequest {
            namespace: self.client.namespace(),
            identity: self.client.identity(),
            request_id: Uuid::new_v4().to_string(),
            operation_id: operation_id.clone(),
            endpoint: self.endpoint.clone(),
            service: S::name().to_owned(),
            operation: operation.name().to_owned(),
            input: payloads.pop(),
            schedule_to_close_timeout: options
                .schedule_to_close_timeout
                .map(TryInto::try_into)
                .transpose()
                .map_err(|_| NexusClientError::InvalidTimeout("schedule-to-close"))?,
            schedule_to_start_timeout: options
                .schedule_to_start_timeout
                .map(TryInto::try_into)
                .transpose()
                .map_err(|_| NexusClientError::InvalidTimeout("schedule-to-start"))?,
            start_to_close_timeout: options
                .start_to_close_timeout
                .map(TryInto::try_into)
                .transpose()
                .map_err(|_| NexusClientError::InvalidTimeout("start-to-close"))?,
            id_reuse_policy: options.id_reuse_policy as i32,
            id_conflict_policy: options.id_conflict_policy as i32,
            search_attributes: options.search_attributes.map(SearchAttributes::into_proto),
            user_metadata: Some(UserMetadata {
                summary: options
                    .static_summary
                    .map(|s| metadata_converter.to_payload(&metadata_context, &s))
                    .transpose()?,
                details: options
                    .static_details
                    .map(|s| metadata_converter.to_payload(&metadata_context, &s))
                    .transpose()?,
            }),
            ..Default::default()
        };
        let response = interceptors::call_start_nexus_operation(
            self.client.client_interceptors(),
            interceptors::StartNexusOperationInput {
                request,
                rpc_options: options.rpc_options,
            },
            Next::new(|input: interceptors::StartNexusOperationInput| {
                async move {
                    let mut request = input.request;
                    let user_metadata = request.user_metadata.take();
                    store_external_payloads(
                        &mut request,
                        self.client.data_converter(),
                        &self.client.namespace(),
                        None,
                    )
                    .await?;
                    request.user_metadata = user_metadata;
                    let mut request = tonic::Request::new(request);
                    input.rpc_options.apply_to(&mut request);
                    Ok(self
                        .client
                        .clone()
                        .start_nexus_operation_execution(request)
                        .await?
                        .into_inner())
                }
                .boxed()
            }),
        )
        .await
        .map_err(|error| match error {
            NexusClientError::Rpc(source) if source.code() == tonic::Code::AlreadyExists => {
                match decode_status_detail::<NexusOperationExecutionAlreadyStartedFailure>(
                    source.details(),
                ) {
                    Some(detail) => NexusClientError::AlreadyStarted {
                        operation_id: operation_id.clone(),
                        run_id: (!detail.run_id.is_empty()).then_some(detail.run_id),
                        source,
                    },
                    None => NexusClientError::Rpc(source),
                }
            }
            other => other,
        })?;
        if response.run_id.is_empty() {
            return Err(NexusClientError::MalformedResponse(
                "start returned an empty run ID",
            ));
        }
        self.client
            .get_nexus_operation_handle(operation_id, Some(response.run_id))
    }

    /// Schedule an execution and await its typed result.
    pub async fn execute<O>(
        &self,
        operation: O,
        input: O::Input,
        options: NexusOperationStartOptions,
    ) -> Result<O::Output, NexusClientError>
    where
        O: NexusOperation<Service = S>,
        O::Input: TemporalSerializable,
        O::Output: TemporalDeserializable + 'static,
    {
        self.start(operation, input, options).await?.result().await
    }
}

/// A durable execution reference. Dropping it does not cancel the operation.
#[derive(Debug)]
pub struct NexusOperationHandle<Output = RawValue> {
    client: Client,
    operation_id: String,
    run_id: Option<String>,
    output: PhantomData<fn() -> Output>,
}

impl<Output> Clone for NexusOperationHandle<Output> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            operation_id: self.operation_id.clone(),
            run_id: self.run_id.clone(),
            output: PhantomData,
        }
    }
}

impl<Output> NexusOperationHandle<Output> {
    /// Caller-assigned operation ID.
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }
    /// Exact server-assigned run ID, or `None` if this handle targets the latest run.
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    async fn poll(
        &self,
        stage: NexusOperationWaitStage,
        options: RpcOptions,
    ) -> Result<PollNexusOperationExecutionResponse, NexusClientError> {
        let mut run_id = self.run_id.clone().unwrap_or_default();
        loop {
            let response = interceptors::call_poll_nexus_operation(
                self.client.client_interceptors(),
                interceptors::PollNexusOperationInput {
                    request: PollNexusOperationExecutionRequest {
                        namespace: self.client.namespace(),
                        operation_id: self.operation_id.clone(),
                        run_id: run_id.clone(),
                        wait_stage: stage as i32,
                    },
                    rpc_options: options.clone(),
                },
                Next::new(|input: interceptors::PollNexusOperationInput| {
                    async move {
                        let mut request = tonic::Request::new(input.request);
                        input.rpc_options.apply_to(&mut request);
                        Ok(self
                            .client
                            .clone()
                            .poll_nexus_operation_execution(request)
                            .await?
                            .into_inner())
                    }
                    .boxed()
                }),
            )
            .await?;
            if response.run_id.is_empty() || (!run_id.is_empty() && response.run_id != run_id) {
                return Err(NexusClientError::MalformedResponse(
                    "poll returned a different run ID",
                ));
            }
            let actual = NexusOperationWaitStage::try_from(response.wait_stage)
                .map_err(|_| NexusClientError::MalformedResponse("unknown wait stage"))?;
            if actual as i32 >= stage as i32 {
                return Ok(response);
            }
            // An ID can be reused while waiting; keep polling the run selected by the first RPC.
            run_id = response.run_id;
        }
    }

    /// Wait for handler start and return its token, or `None` for a synchronous operation.
    /// A closed unsuccessful operation is returned as an operation error.
    pub async fn operation_token(&self) -> Result<Option<String>, NexusClientError> {
        self.operation_token_with_options(RpcOptions::default())
            .await
    }

    /// Wait for handler start with per-poll RPC controls.
    pub async fn operation_token_with_options(
        &self,
        options: RpcOptions,
    ) -> Result<Option<String>, NexusClientError> {
        let mut response = self.poll(NexusOperationWaitStage::Started, options).await?;
        self.decode_failure(&mut response).await?;
        Ok((!response.operation_token.is_empty()).then_some(response.operation_token))
    }

    async fn decode_failure(
        &self,
        response: &mut PollNexusOperationExecutionResponse,
    ) -> Result<(), NexusClientError> {
        if matches!(response.outcome, Some(Outcome::Failure(_))) {
            let converter = self.client.data_converter();
            retrieve_external_payloads(response, converter).await?;
            decode_payloads(response, converter.codec(), &context()).await?;
            if let Some(Outcome::Failure(failure)) = response.outcome.take() {
                return Err(NexusClientError::Operation {
                    operation_id: self.operation_id.clone(),
                    run_id: response.run_id.clone(),
                    source: converter.to_error(
                        &context(),
                        failure,
                        NexusOperationDecodeHint::Failed,
                    )?,
                });
            }
        }
        Ok(())
    }

    /// Request cancellation. Completion remains observable through `result`.
    pub async fn cancel(&self) -> Result<(), NexusClientError> {
        self.cancel_with_options("", RpcOptions::default()).await
    }

    /// Request cancellation with a recorded reason and per-call RPC controls.
    pub async fn cancel_with_options(
        &self,
        reason: impl Into<String>,
        options: RpcOptions,
    ) -> Result<(), NexusClientError> {
        interceptors::call_cancel_nexus_operation(
            self.client.client_interceptors(),
            interceptors::CancelNexusOperationInput {
                request: RequestCancelNexusOperationExecutionRequest {
                    namespace: self.client.namespace(),
                    operation_id: self.operation_id.clone(),
                    run_id: self.run_id.clone().unwrap_or_default(),
                    identity: self.client.identity(),
                    request_id: Uuid::new_v4().to_string(),
                    reason: reason.into(),
                },
                rpc_options: options,
            },
            Next::new(|input: interceptors::CancelNexusOperationInput| {
                async move {
                    let mut request = tonic::Request::new(input.request);
                    input.rpc_options.apply_to(&mut request);
                    Ok(self
                        .client
                        .clone()
                        .request_cancel_nexus_operation_execution(request)
                        .await?
                        .into_inner())
                }
                .boxed()
            }),
        )
        .await?;
        Ok(())
    }

    /// Terminate the execution without waiting for the Nexus handler to finish.
    pub async fn terminate(&self) -> Result<(), NexusClientError> {
        self.terminate_with_options("", RpcOptions::default()).await
    }

    /// Terminate the execution with a recorded reason and per-call RPC controls.
    pub async fn terminate_with_options(
        &self,
        reason: impl Into<String>,
        options: RpcOptions,
    ) -> Result<(), NexusClientError> {
        interceptors::call_terminate_nexus_operation(
            self.client.client_interceptors(),
            interceptors::TerminateNexusOperationInput {
                request: TerminateNexusOperationExecutionRequest {
                    namespace: self.client.namespace(),
                    operation_id: self.operation_id.clone(),
                    run_id: self.run_id.clone().unwrap_or_default(),
                    identity: self.client.identity(),
                    request_id: Uuid::new_v4().to_string(),
                    reason: reason.into(),
                },
                rpc_options: options,
            },
            Next::new(|input: interceptors::TerminateNexusOperationInput| {
                async move {
                    let mut request = tonic::Request::new(input.request);
                    input.rpc_options.apply_to(&mut request);
                    Ok(self
                        .client
                        .clone()
                        .terminate_nexus_operation_execution(request)
                        .await?
                        .into_inner())
                }
                .boxed()
            }),
        )
        .await?;
        Ok(())
    }

    /// Describe the execution, including codec-decoded input and outcome.
    pub async fn describe(
        &self,
    ) -> Result<DescribeNexusOperationExecutionResponse, NexusClientError> {
        self.describe_with_options(RpcOptions::default()).await
    }

    /// Describe the execution with per-call RPC controls.
    pub async fn describe_with_options(
        &self,
        options: RpcOptions,
    ) -> Result<DescribeNexusOperationExecutionResponse, NexusClientError> {
        let mut response = interceptors::call_describe_nexus_operation(
            self.client.client_interceptors(),
            interceptors::DescribeNexusOperationInput {
                request: DescribeNexusOperationExecutionRequest {
                    namespace: self.client.namespace(),
                    operation_id: self.operation_id.clone(),
                    run_id: self.run_id.clone().unwrap_or_default(),
                    include_input: true,
                    include_outcome: true,
                    ..Default::default()
                },
                rpc_options: options,
            },
            Next::new(|input: interceptors::DescribeNexusOperationInput| {
                async move {
                    let mut request = tonic::Request::new(input.request);
                    input.rpc_options.apply_to(&mut request);
                    Ok(self
                        .client
                        .clone()
                        .describe_nexus_operation_execution(request)
                        .await?
                        .into_inner())
                }
                .boxed()
            }),
        )
        .await?;
        if response.run_id.is_empty()
            || self
                .run_id
                .as_ref()
                .is_some_and(|id| id != &response.run_id)
        {
            return Err(NexusClientError::MalformedResponse(
                "describe returned a different run ID",
            ));
        }
        // Fixed UI metadata uses the default converter without a codec so the server can read it.
        let user_metadata = response
            .info
            .as_mut()
            .and_then(|info| info.user_metadata.take());
        retrieve_external_payloads(&mut response, self.client.data_converter()).await?;
        decode_payloads(
            &mut response,
            self.client.data_converter().codec(),
            &context(),
        )
        .await?;
        if let Some(info) = response.info.as_mut() {
            info.user_metadata = user_metadata;
        }
        Ok(response)
    }
}

impl<Output: TemporalDeserializable + 'static> NexusOperationHandle<Output> {
    /// Wait for completion and decode the result with the configured data converter.
    pub async fn result(&self) -> Result<Output, NexusClientError> {
        self.result_with_options(RpcOptions::default()).await
    }

    /// Wait for completion with per-poll RPC controls.
    pub async fn result_with_options(
        &self,
        options: RpcOptions,
    ) -> Result<Output, NexusClientError> {
        let mut response = self.poll(NexusOperationWaitStage::Closed, options).await?;
        self.decode_failure(&mut response).await?;
        let converter = self.client.data_converter();
        retrieve_external_payloads(&mut response, converter).await?;
        decode_payloads(&mut response, converter.codec(), &context()).await?;
        match response.outcome {
            Some(Outcome::Result(payload)) => Ok(converter.payload_converter().from_payload(
                &SerializationContext::new(&context(), converter.payload_converter()),
                payload,
            )?),
            _ => Err(NexusClientError::MalformedResponse(
                "closed execution has no outcome",
            )),
        }
    }

    /// Alias for [`Self::result`].
    pub async fn get_result(&self) -> Result<Output, NexusClientError> {
        self.result().await
    }
}
