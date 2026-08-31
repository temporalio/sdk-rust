use crate::{
    CancelWorkflowInput, DescribeWorkflowInput, DescribeWorkflowOutput,
    FetchWorkflowHistoryPageInput, FetchWorkflowHistoryPageOutput, NamespacedClient, Next,
    PollWorkflowUpdateInput, PollWorkflowUpdateOutput, QueryWorkflowInput, QueryWorkflowOutput,
    RpcOptions, SignalWorkflowInput, StartWorkflowUpdateInput, StartWorkflowUpdateOutput,
    TerminateWorkflowInput, WorkflowCancelOptions, WorkflowDescribeOptions,
    WorkflowExecuteUpdateOptions, WorkflowExecutionStatus, WorkflowFetchHistoryOptions,
    WorkflowGetResultOptions, WorkflowQueryOptions, WorkflowSignalOptions,
    WorkflowStartUpdateOptions, WorkflowTerminateOptions,
    errors::{
        WorkflowGetResultError, WorkflowInteractionError, WorkflowQueryError, WorkflowUpdateError,
    },
    grpc::WorkflowService,
    interceptors,
};
use futures_util::{TryStreamExt, future::BoxFuture, stream, stream::Stream};
use std::{
    collections::VecDeque,
    fmt::Debug,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
pub use temporalio_common::UntypedWorkflow;
use temporalio_common::{
    HasWorkflowDefinition, QueryDefinition, SignalDefinition, UpdateDefinition, WorkflowDefinition,
    data_converters::{
        DataConverter, DecodablePayloads, GenericPayloadConverter, PayloadConversionError,
        PayloadConverter, RawValue, SerializationContext, SerializationContextData,
        WorkflowSerializationContext,
    },
    error::IncomingError,
    payload_visitor::decode_payloads,
    protos::{
        coresdk::FromPayloadsExt,
        proto_ts_to_system_time,
        temporal::api::{
            common::v1::{Header, Payload, Payloads, WorkflowExecution as ProtoWorkflowExecution},
            enums::v1::{HistoryEventFilterType, UpdateWorkflowExecutionLifecycleStage},
            history::{
                self,
                v1::{History, HistoryEvent, history_event::Attributes},
            },
            query::v1::WorkflowQuery,
            sdk::v1::UserMetadata,
            update::{self, v1::WaitPolicy},
            workflow::v1 as workflow,
            workflowservice::v1::{
                DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
                GetWorkflowExecutionHistoryRequest, PollWorkflowExecutionUpdateRequest,
                QueryWorkflowRequest, RequestCancelWorkflowExecutionRequest,
                SignalWorkflowExecutionRequest, TerminateWorkflowExecutionRequest,
                UpdateWorkflowExecutionRequest,
            },
        },
    },
    search_attributes::SearchAttributes,
};
use tonic::IntoRequest;
use uuid::Uuid;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DecodedUserMetadata {
    summary: Option<String>,
    details: Option<String>,
}

fn decode_user_metadata(
    context: &SerializationContextData,
    user_metadata: Option<UserMetadata>,
) -> Result<DecodedUserMetadata, PayloadConversionError> {
    let payload_converter = PayloadConverter::default();
    let context = SerializationContext::new(context, &payload_converter);
    let (summary, details) = user_metadata
        .map(|metadata| (metadata.summary, metadata.details))
        .unwrap_or_default();
    Ok(DecodedUserMetadata {
        summary: match summary {
            Some(payload) => Some(payload_converter.from_payload(&context, payload)?),
            None => None,
        },
        details: match details {
            Some(payload) => Some(payload_converter.from_payload(&context, payload)?),
            None => None,
        },
    })
}

/// Details attached to a cancelled or terminated workflow result.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WorkflowResultDetails {
    payloads: DecodablePayloads,
}

impl WorkflowResultDetails {
    async fn new(
        payloads: Vec<Payload>,
        data_converter: &DataConverter,
    ) -> Result<Self, PayloadConversionError> {
        let payloads = data_converter
            .codec()
            .decode(
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                payloads,
            )
            .await?;
        Ok(Self {
            payloads: DecodablePayloads::new(
                payloads,
                data_converter.payload_converter().clone(),
                SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ),
        })
    }

    /// Deserialize the details into a typed value using the client's payload converter.
    pub fn deserialize<T: temporalio_common::data_converters::TemporalDeserializable + 'static>(
        &self,
    ) -> Result<T, PayloadConversionError> {
        self.payloads.deserialize()
    }

    /// Returns the codec-decoded payloads.
    pub fn raw(&self) -> &[Payload] {
        self.payloads.raw()
    }

    /// Consume these details and return their codec-decoded payloads.
    pub fn into_raw(self) -> RawValue {
        self.payloads.into_raw()
    }
}

/// Enumerates terminal states for a particular workflow execution
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum WorkflowExecutionResult<T> {
    /// The workflow finished successfully
    Succeeded(T),
    /// The workflow finished in failure
    Failed(IncomingError),
    /// The workflow was cancelled
    Cancelled {
        /// Details provided at cancellation time
        details: WorkflowResultDetails,
    },
    /// The workflow was terminated
    Terminated {
        /// Details provided at termination time
        details: WorkflowResultDetails,
    },
    /// The workflow timed out
    TimedOut,
    /// The workflow continued as new
    ContinuedAsNew,
}

/// Description of a workflow execution returned by `WorkflowHandle::describe`.
///
/// Access to the underlying Protobuf message is provided by [`raw`](Self::raw).
#[derive(Debug, Clone)]
pub struct WorkflowExecutionDescription {
    /// The raw proto response from the server.
    pub raw_description: DescribeWorkflowExecutionResponse,
    history_length: usize,
    static_summary: Option<String>,
    static_details: Option<String>,
    data_converter: DataConverter,
}

impl WorkflowExecutionDescription {
    async fn new(
        mut raw_description: DescribeWorkflowExecutionResponse,
        data_converter: &DataConverter,
    ) -> Result<Self, PayloadConversionError> {
        let raw_user_metadata = raw_description
            .execution_config
            .as_ref()
            .and_then(|cfg| cfg.user_metadata.clone());
        decode_payloads(
            &mut raw_description,
            data_converter.codec(),
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
        .await?;
        let decoded_metadata = decode_user_metadata(
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            raw_user_metadata,
        )?;
        let history_length_raw = raw_description
            .workflow_execution_info
            .as_ref()
            .map(|info| info.history_length)
            .unwrap_or(0);
        let history_length = history_length_raw.try_into().map_err(|_| {
            PayloadConversionError::EncodingError(
                format!("workflow history_length must be non-negative, got {history_length_raw}")
                    .into(),
            )
        })?;
        Ok(Self {
            raw_description,
            history_length,
            static_summary: decoded_metadata.summary,
            static_details: decoded_metadata.details,
            data_converter: data_converter.clone(),
        })
    }

    /// The workflow ID.
    pub fn id(&self) -> &str {
        self.execution().workflow_id.as_str()
    }

    /// The run ID.
    pub fn run_id(&self) -> &str {
        self.execution().run_id.as_str()
    }

    /// The workflow type name.
    pub fn workflow_type(&self) -> &str {
        self.workflow_type_info().name.as_str()
    }

    /// The current status of the workflow execution.
    pub fn status(&self) -> WorkflowExecutionStatus {
        WorkflowExecutionStatus::from_raw(self.workflow_info().status)
    }

    /// When the workflow was created.
    pub fn start_time(&self) -> Option<std::time::SystemTime> {
        self.workflow_info()
            .start_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// When the workflow run started or should start.
    pub fn execution_time(&self) -> Option<std::time::SystemTime> {
        self.workflow_info()
            .execution_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// When the workflow was closed, if closed.
    pub fn close_time(&self) -> Option<std::time::SystemTime> {
        self.workflow_info()
            .close_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// The task queue the workflow runs on.
    pub fn task_queue(&self) -> &str {
        self.workflow_info().task_queue.as_str()
    }

    /// Number of events in history.
    pub fn history_length(&self) -> usize {
        self.history_length
    }

    /// Workflow memo decoded with the client's payload converter.
    pub fn memo(&self) -> crate::Memo {
        crate::Memo::from_raw(
            self.workflow_info().memo.clone(),
            self.data_converter.payload_converter().clone(),
            SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        )
    }

    /// Parent workflow ID, if this is a child workflow.
    pub fn parent_id(&self) -> Option<&str> {
        self.workflow_info()
            .parent_execution
            .as_ref()
            .map(|e| e.workflow_id.as_str())
    }

    /// Parent run ID, if this is a child workflow.
    pub fn parent_run_id(&self) -> Option<&str> {
        self.workflow_info()
            .parent_execution
            .as_ref()
            .map(|e| e.run_id.as_str())
    }

    /// Search attributes on the workflow.
    pub fn search_attributes(&self) -> SearchAttributes {
        self.workflow_info()
            .search_attributes
            .as_ref()
            .map(SearchAttributes::from_proto)
            .unwrap_or_default()
    }

    /// Static summary configured on the workflow, if present.
    pub fn static_summary(&self) -> Option<&str> {
        self.static_summary.as_deref()
    }

    /// Static details configured on the workflow, if present.
    pub fn static_details(&self) -> Option<&str> {
        self.static_details.as_deref()
    }

    /// Access the raw proto for additional fields not exposed via accessors.
    pub fn raw(&self) -> &DescribeWorkflowExecutionResponse {
        &self.raw_description
    }

    /// Consume the wrapper and return the raw proto.
    pub fn into_raw(self) -> DescribeWorkflowExecutionResponse {
        self.raw_description
    }

    fn workflow_info(&self) -> &workflow::WorkflowExecutionInfo {
        self.raw_description
            .workflow_execution_info
            .as_ref()
            .expect("describe response missing workflow_execution_info")
    }

    fn execution(&self) -> &ProtoWorkflowExecution {
        self.workflow_info()
            .execution
            .as_ref()
            .expect("describe response missing workflow_execution_info.execution")
    }

    fn workflow_type_info(
        &self,
    ) -> &temporalio_common::protos::temporal::api::common::v1::WorkflowType {
        self.workflow_info()
            .r#type
            .as_ref()
            .expect("describe response missing workflow_execution_info.type")
    }
}

/// Workflow execution history returned by [`WorkflowHandle::fetch_history`].
///
/// Events and their containing pages are fetched lazily as this stream is polled. Use
/// [`into_events`](Self::into_events) to fetch and collect all events at once.
#[derive(derive_more::Debug)]
pub struct WorkflowHistory {
    #[debug(skip)]
    inner: Pin<Box<dyn Stream<Item = Result<HistoryEvent, WorkflowInteractionError>> + Send>>,
    workflow_id: Option<String>,
}

impl From<history::v1::History> for WorkflowHistory {
    fn from(history: history::v1::History) -> Self {
        let workflow_id =
            history
                .events
                .first()
                .and_then(|event| match event.attributes.as_ref() {
                    Some(Attributes::WorkflowExecutionStartedEventAttributes(attributes))
                        if !attributes.workflow_id.is_empty() =>
                    {
                        Some(attributes.workflow_id.clone())
                    }
                    _ => None,
                });
        Self {
            inner: Box::pin(stream::iter(history.events.into_iter().map(Ok))),
            workflow_id,
        }
    }
}

impl Stream for WorkflowHistory {
    type Item = Result<HistoryEvent, WorkflowInteractionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Error fetching or converting a workflow history.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowHistoryError {
    /// Fetching the workflow history failed.
    #[error("failed to fetch workflow history: {0}")]
    Fetch(#[from] WorkflowInteractionError),
    /// Converting the workflow history JSON failed.
    #[error("failed to convert workflow history JSON: {0}")]
    Json(#[from] serde_json::Error),
}

impl WorkflowHistory {
    /// Decode a workflow history from JSON bytes.
    pub fn from_json(bytes: &[u8]) -> Result<Self, WorkflowHistoryError> {
        let history: History = serde_json::from_slice(bytes)?;
        Ok(history.into())
    }

    /// Fetch all remaining events and encode this workflow history as JSON bytes.
    pub async fn to_json(self) -> Result<Vec<u8>, WorkflowHistoryError> {
        Ok(serde_json::to_vec(&History {
            events: self.into_events().await?,
        })?)
    }

    /// Return the workflow ID when it is known.
    pub fn workflow_id(&self) -> Option<&str> {
        self.workflow_id.as_deref()
    }

    /// Fetch all remaining history pages and collect their events.
    pub async fn into_events(self) -> Result<Vec<HistoryEvent>, WorkflowInteractionError> {
        self.inner.try_collect().await
    }
}

/// A workflow handle which can refer to a specific workflow run, or a chain of workflow runs with
/// the same workflow id.
#[derive(Clone)]
pub struct WorkflowHandle<ClientT, W> {
    client: ClientT,
    info: WorkflowExecutionInfo,

    _wf_type: PhantomData<W>,
}

impl<CT, W> WorkflowHandle<CT, W> {
    /// Return the run id of the Workflow Execution pointed at by this handle, if there is one.
    pub fn run_id(&self) -> Option<&str> {
        self.info.run_id.as_deref()
    }
}

/// Holds needed information to refer to a specific workflow run, or workflow execution chain
#[derive(Debug, Clone, bon::Builder)]
#[builder(on(String, into), state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct WorkflowExecutionInfo {
    /// Namespace the workflow lives in.
    pub namespace: String,
    /// The workflow's id.
    pub workflow_id: String,
    /// If set, target this specific run of the workflow.
    pub run_id: Option<String>,
    /// Run ID used for cancellation and termination to ensure they happen on a workflow starting
    /// with this run ID. This can be set when getting a workflow handle. When starting a workflow,
    /// this is set as the resulting run ID if no start signal was provided.
    pub first_execution_run_id: Option<String>,
}

impl WorkflowExecutionInfo {
    /// Bind the workflow info to a specific client, turning it into a workflow handle
    pub fn bind_untyped<CT>(self, client: CT) -> UntypedWorkflowHandle<CT>
    where
        CT: WorkflowService + Clone,
    {
        UntypedWorkflowHandle::new(client, self)
    }
}

/// A workflow handle to a workflow with unknown types. Uses single argument raw payloads for input
/// and output.
pub type UntypedWorkflowHandle<CT> = WorkflowHandle<CT, UntypedWorkflow>;

/// Marker type for sending untyped signals. Stores the signal name for runtime lookup.
///
/// Use with `handle.signal(UntypedSignal::new("signal_name"), raw_payload)`.
pub struct UntypedSignal<W> {
    name: String,
    _wf: PhantomData<W>,
}

impl<W> UntypedSignal<W> {
    /// Create a new `UntypedSignal` with the given signal name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _wf: PhantomData,
        }
    }
}

impl<W: WorkflowDefinition> SignalDefinition for UntypedSignal<W> {
    type Workflow = W;
    type Input = RawValue;

    fn name(&self) -> &str {
        &self.name
    }
}

/// Marker type for sending untyped queries. Stores the query name for runtime lookup.
///
/// Use with `handle.query(UntypedQuery::new("query_name"), raw_payload)`.
pub struct UntypedQuery<W> {
    name: String,
    _wf: PhantomData<W>,
}

impl<W> UntypedQuery<W> {
    /// Create a new `UntypedQuery` with the given query name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _wf: PhantomData,
        }
    }
}

impl<W: WorkflowDefinition> QueryDefinition for UntypedQuery<W> {
    type Workflow = W;
    type Input = RawValue;
    type Output = RawValue;

    fn name(&self) -> &str {
        &self.name
    }
}

/// Marker type for sending untyped updates. Stores the update name for runtime lookup.
///
/// Use with `handle.update(UntypedUpdate::new("update_name"), raw_payload)`.
pub struct UntypedUpdate<W> {
    name: String,
    _wf: PhantomData<W>,
}

impl<W> UntypedUpdate<W> {
    /// Create a new `UntypedUpdate` with the given update name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _wf: PhantomData,
        }
    }
}

impl<W: WorkflowDefinition> UpdateDefinition for UntypedUpdate<W> {
    type Workflow = W;
    type Input = RawValue;
    type Output = RawValue;

    fn name(&self) -> &str {
        &self.name
    }
}

/// Shared by [WorkflowHandle::start_update] and the client's update-with-start, which sends the
/// same update request as one of its operations. Update starts always wait for the update to be
/// accepted; results are waited on separately via the update handle.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_update_workflow_request(
    namespace: String,
    identity: String,
    workflow_id: String,
    run_id: String,
    update_id: String,
    update_name: String,
    header: Option<Header>,
    payloads: Vec<Payload>,
) -> UpdateWorkflowExecutionRequest {
    UpdateWorkflowExecutionRequest {
        namespace,
        workflow_execution: Some(ProtoWorkflowExecution {
            workflow_id,
            run_id,
        }),
        wait_policy: Some(WaitPolicy {
            lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Accepted.into(),
        }),
        request: Some(update::v1::Request {
            meta: Some(update::v1::Meta {
                update_id,
                identity,
            }),
            input: Some(update::v1::Input {
                header,
                name: update_name,
                args: Some(Payloads { payloads }),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

impl<CT, W> WorkflowHandle<CT, W>
where
    CT: WorkflowService + Clone,
    W: HasWorkflowDefinition,
{
    /// Create a workflow handle from a client and identifying information.
    pub fn new(client: CT, info: WorkflowExecutionInfo) -> Self {
        Self {
            client,
            info,
            _wf_type: PhantomData::<W>,
        }
    }

    /// Get the workflow execution info
    pub fn info(&self) -> &WorkflowExecutionInfo {
        &self.info
    }

    /// Get the client attached to this handle
    pub fn client(&self) -> &CT {
        &self.client
    }

    /// Await the result of the workflow execution
    pub async fn get_result(
        &self,
        opts: WorkflowGetResultOptions,
    ) -> Result<W::Output, WorkflowGetResultError>
    where
        CT: WorkflowService + NamespacedClient + Clone + 'static,
    {
        let raw = self.get_result_raw(opts).await?;
        match raw {
            WorkflowExecutionResult::Succeeded(v) => Ok(v),
            WorkflowExecutionResult::Failed(f) => Err(WorkflowGetResultError::Failed(Box::new(f))),
            WorkflowExecutionResult::Cancelled { details } => {
                Err(WorkflowGetResultError::Cancelled { details })
            }
            WorkflowExecutionResult::Terminated { details } => {
                Err(WorkflowGetResultError::Terminated { details })
            }
            WorkflowExecutionResult::TimedOut => Err(WorkflowGetResultError::TimedOut),
            WorkflowExecutionResult::ContinuedAsNew => Err(WorkflowGetResultError::ContinuedAsNew),
        }
    }

    /// Await the result of the workflow execution, returning the full
    /// [`WorkflowExecutionResult`] enum for callers that need to inspect non-success outcomes
    /// directly.
    async fn get_result_raw(
        &self,
        opts: WorkflowGetResultOptions,
    ) -> Result<WorkflowExecutionResult<W::Output>, WorkflowInteractionError>
    where
        CT: WorkflowService + NamespacedClient + Clone + 'static,
    {
        let mut run_id = self.info.run_id.clone().unwrap_or_default();
        let fetch_opts = WorkflowFetchHistoryOptions::builder()
            .skip_archival(true)
            .wait_new_event(true)
            .event_filter_type(HistoryEventFilterType::CloseEvent)
            .rpc_options(opts.rpc_options.clone())
            .build();

        loop {
            let history = self.fetch_history_for_run(&run_id, fetch_opts.clone());
            let mut events = history.into_events().await?;

            if events.is_empty() {
                continue;
            }

            let event_attrs = events.pop().and_then(|ev| ev.attributes);

            macro_rules! follow {
                ($attrs:ident) => {
                    if opts.follow_runs && $attrs.new_execution_run_id != "" {
                        run_id = $attrs.new_execution_run_id;
                        continue;
                    }
                };
            }

            let dc = self.client.data_converter();

            break match event_attrs {
                Some(Attributes::WorkflowExecutionCompletedEventAttributes(attrs)) => {
                    follow!(attrs);
                    let payload = attrs
                        .result
                        .and_then(|p| p.payloads.into_iter().next())
                        .unwrap_or_default();
                    let result: W::Output = dc
                        .from_payload(&SerializationContextData::Workflow(WorkflowSerializationContext::new()), payload)
                        .await?;
                    Ok(WorkflowExecutionResult::Succeeded(result))
                }
                Some(Attributes::WorkflowExecutionFailedEventAttributes(attrs)) => {
                    follow!(attrs);
                    let mut failure = attrs.failure.unwrap_or_default();
                    decode_payloads(
                        &mut failure,
                        dc.codec(),
                        &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                    )
                    .await?;
                    let error = dc.failure_converter().to_error(
                        failure,
                        dc.payload_converter(),
                        &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                    )?;
                    Ok(WorkflowExecutionResult::Failed(error))
                }
                Some(Attributes::WorkflowExecutionCanceledEventAttributes(attrs)) => {
                    Ok(WorkflowExecutionResult::Cancelled {
                        details: WorkflowResultDetails::new(Vec::from_payloads(attrs.details), dc)
                            .await?,
                    })
                }
                Some(Attributes::WorkflowExecutionTimedOutEventAttributes(attrs)) => {
                    follow!(attrs);
                    Ok(WorkflowExecutionResult::TimedOut)
                }
                Some(Attributes::WorkflowExecutionTerminatedEventAttributes(attrs)) => {
                    Ok(WorkflowExecutionResult::Terminated {
                        details: WorkflowResultDetails::new(Vec::from_payloads(attrs.details), dc)
                            .await?,
                    })
                }
                Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attrs)) => {
                    if opts.follow_runs {
                        if !attrs.new_execution_run_id.is_empty() {
                            run_id = attrs.new_execution_run_id;
                            continue;
                        } else {
                            return Err(WorkflowInteractionError::Other(
                                "New execution run id was empty in continue as new event!".into(),
                            ));
                        }
                    } else {
                        Ok(WorkflowExecutionResult::ContinuedAsNew)
                    }
                }
                o => Err(WorkflowInteractionError::Other(
                    format!(
                        "Server returned an event that didn't match the CloseEvent filter. \
                         This is either a server bug or a new event the SDK does not understand. \
                         Event details: {o:?}"
                    )
                    .into(),
                )),
            };
        }
    }

    /// Send a signal to the workflow
    pub async fn signal<S>(
        &self,
        signal: S,
        input: S::Input,
        opts: WorkflowSignalOptions,
    ) -> Result<(), WorkflowInteractionError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
        S: SignalDefinition<Workflow = W::Run>,
        S::Input: Send,
    {
        interceptors::call_signal_workflow(
            self.client.client_interceptors(),
            SignalWorkflowInput::new(
                self.info.workflow_id.clone(),
                self.info.run_id.clone().unwrap_or_default(),
                signal.name().to_string(),
                input,
                opts,
            ),
            Next::new({
                let mut client = self.client.clone();
                move |input: SignalWorkflowInput| -> BoxFuture<
                    '_,
                    Result<(), WorkflowInteractionError>,
                > {
                    Box::pin(async move {
                        let (workflow_id, run_id, signal_name, args, options) =
                            input.into_parts();
                        let data_converter = client.data_converter().clone();
                        let unencoded_payloads = {
                            let payload_converter = data_converter.payload_converter();
                            let context_data = SerializationContextData::Workflow(
                                WorkflowSerializationContext::new(),
                            );
                            let context =
                                SerializationContext::new(&context_data, payload_converter);
                            args.serialize_payloads(&context)
                        };
                        drop(args);
                        let payloads = data_converter
                            .codec()
                            .encode(&SerializationContextData::Workflow(WorkflowSerializationContext::new()), unencoded_payloads?)
                            .await?;
                        let mut request = SignalWorkflowExecutionRequest {
                            namespace: client.namespace(),
                            workflow_execution: Some(ProtoWorkflowExecution {
                                workflow_id,
                                run_id,
                            }),
                            signal_name,
                            input: Some(Payloads { payloads }),
                            identity: client.identity(),
                            request_id: options
                                .request_id
                                .unwrap_or_else(|| Uuid::new_v4().to_string()),
                            header: options.header,
                            ..Default::default()
                        }
                        .into_request();
                        options.rpc_options.apply_to(&mut request);
                        WorkflowService::signal_workflow_execution(&mut client, request)
                            .await
                            .map_err(WorkflowInteractionError::from_status)?;
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Query the workflow
    pub async fn query<Q>(
        &self,
        query: Q,
        input: Q::Input,
        opts: WorkflowQueryOptions,
    ) -> Result<Q::Output, WorkflowQueryError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
        Q: QueryDefinition<Workflow = W::Run>,
        Q::Input: Send,
    {
        let output = interceptors::call_query_workflow(
            self.client.client_interceptors(),
            QueryWorkflowInput::new(
                self.info.workflow_id.clone(),
                self.info.run_id.clone().unwrap_or_default(),
                query.name().to_string(),
                input,
                opts,
            ),
            Next::new({
                let mut client = self.client.clone();
                move |input: QueryWorkflowInput| -> BoxFuture<
                    '_,
                    Result<QueryWorkflowOutput, WorkflowQueryError>,
                > {
                    Box::pin(async move {
                        let (workflow_id, run_id, query_name, args, options) = input.into_parts();
                        let data_converter = client.data_converter().clone();
                        let unencoded_payloads = {
                            let payload_converter = data_converter.payload_converter();
                            let context_data = SerializationContextData::Workflow(
                                WorkflowSerializationContext::new(),
                            );
                            let context =
                                SerializationContext::new(&context_data, payload_converter);
                            args.serialize_payloads(&context)
                        };
                        drop(args);
                        let payloads = data_converter
                            .codec()
                            .encode(&SerializationContextData::Workflow(WorkflowSerializationContext::new()), unencoded_payloads?)
                            .await?;
                        let mut request = QueryWorkflowRequest {
                            namespace: client.namespace(),
                            execution: Some(ProtoWorkflowExecution {
                                workflow_id,
                                run_id,
                            }),
                            query: Some(WorkflowQuery {
                                query_type: query_name,
                                query_args: Some(Payloads { payloads }),
                                header: options.header,
                            }),
                            query_reject_condition: options
                                .reject_condition
                                .map(|condition| condition as i32)
                                .unwrap_or(1),
                        }
                        .into_request();
                        options.rpc_options.apply_to(&mut request);
                        let response = client
                            .query_workflow(request)
                            .await
                            .map_err(WorkflowQueryError::from_status)?
                            .into_inner();
                        Ok(QueryWorkflowOutput::new(response))
                    })
                }
            }),
        )
        .await?;
        let response = output.response;

        if let Some(rejected) = response.query_rejected {
            return Err(WorkflowQueryError::Rejected {
                status: (rejected.status != 0)
                    .then(|| WorkflowExecutionStatus::from_raw(rejected.status)),
            });
        }

        let result_payloads = response
            .query_result
            .map(|p| p.payloads)
            .unwrap_or_default();

        self.client
            .data_converter()
            .from_payloads(
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                result_payloads,
            )
            .await
            .map_err(WorkflowQueryError::from)
    }

    /// Send an update to the workflow and wait for it to complete, returning the result.
    pub async fn execute_update<U>(
        &self,
        update: U,
        input: U::Input,
        options: WorkflowExecuteUpdateOptions,
    ) -> Result<U::Output, WorkflowUpdateError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
        U: UpdateDefinition<Workflow = W::Run>,
        U::Input: Send,
        U::Output: 'static,
    {
        let rpc_options = options.rpc_options.clone();
        let handle = self.start_update(update, input, options.into()).await?;
        handle.get_result(rpc_options).await
    }

    /// Start an update and return a handle without waiting for completion.
    /// Use `execute_update()` if you want to wait for the result immediately.
    pub async fn start_update<U>(
        &self,
        update: U,
        input: U::Input,
        options: WorkflowStartUpdateOptions,
    ) -> Result<WorkflowUpdateHandle<CT, U::Output>, WorkflowUpdateError>
    where
        CT: WorkflowService + NamespacedClient + Clone,
        U: UpdateDefinition<Workflow = W::Run>,
        U::Input: Send,
    {
        let output = interceptors::call_start_workflow_update(
            self.client.client_interceptors(),
            StartWorkflowUpdateInput::new(
                self.info().workflow_id.clone(),
                self.info().run_id.clone().unwrap_or_default(),
                update.name().to_string(),
                input,
                options,
            ),
            Next::new({
                let mut client = self.client.clone();
                move |input: StartWorkflowUpdateInput| -> BoxFuture<
                        '_,
                        Result<StartWorkflowUpdateOutput, WorkflowUpdateError>,
                    > {
                        Box::pin(async move {
                            let (workflow_id, run_id, update_name, args, options) =
                                input.into_parts();
                            let data_converter = client.data_converter().clone();
                            let unencoded_payloads = {
                                let payload_converter = data_converter.payload_converter();
                                let context_data = SerializationContextData::Workflow(
                                    WorkflowSerializationContext::new(),
                                );
                                let context =
                                    SerializationContext::new(&context_data, payload_converter);
                                args.serialize_payloads(&context)
                            };
                            drop(args);
                            let payloads = data_converter
                                .codec()
                                .encode(
                                    &SerializationContextData::Workflow(
                                        WorkflowSerializationContext::new(),
                                    ),
                                    unencoded_payloads?,
                                )
                                .await?;
                            let update_id = options
                                .update_id
                                .unwrap_or_else(|| Uuid::new_v4().to_string());
                            let mut request = build_update_workflow_request(
                                client.namespace(),
                                client.identity(),
                                workflow_id.clone(),
                                run_id,
                                update_id.clone(),
                                update_name,
                                options.header,
                                payloads,
                            )
                            .into_request();
                            options.rpc_options.apply_to(&mut request);
                            let response =
                                WorkflowService::update_workflow_execution(&mut client, request)
                                    .await
                                    .map_err(WorkflowUpdateError::from_status)?
                                    .into_inner();
                            let run_id = response
                                .update_ref
                                .as_ref()
                                .and_then(|reference| reference.workflow_execution.as_ref())
                                .map(|execution| execution.run_id.clone())
                                .filter(|run_id| !run_id.is_empty());
                            Ok(StartWorkflowUpdateOutput::new(
                                update_id,
                                workflow_id,
                                run_id,
                                response.outcome,
                            ))
                        })
                    }
            }),
        )
        .await?;

        Ok(WorkflowUpdateHandle::new(
            self.client.clone(),
            output.update_id,
            output.workflow_id,
            output.run_id.or_else(|| self.info().run_id.clone()),
            output.known_outcome,
        ))
    }

    /// Get a handle to an existing update.
    ///
    /// The update definition determines the result type. The returned handle uses this workflow
    /// handle's workflow and run IDs and does not validate the update ID until
    /// [`get_result`](WorkflowUpdateHandle::get_result) is called.
    pub fn get_update_handle<U>(
        &self,
        update: U,
        update_id: impl Into<String>,
    ) -> WorkflowUpdateHandle<CT, U::Output>
    where
        U: UpdateDefinition<Workflow = W::Run>,
    {
        let _ = update;
        WorkflowUpdateHandle::new(
            self.client.clone(),
            update_id.into(),
            self.info.workflow_id.clone(),
            self.info.run_id.clone(),
            None,
        )
    }

    /// Request cancellation of this workflow.
    pub async fn cancel(&self, opts: WorkflowCancelOptions) -> Result<(), WorkflowInteractionError>
    where
        CT: NamespacedClient,
    {
        interceptors::call_cancel_workflow(
            self.client.client_interceptors(),
            CancelWorkflowInput {
                workflow_id: self.info.workflow_id.clone(),
                run_id: self.info.run_id.clone().unwrap_or_default(),
                first_execution_run_id: self
                    .info
                    .first_execution_run_id
                    .clone()
                    .unwrap_or_default(),
                options: opts,
            },
            Next::new({
                let mut client = self.client.clone();
                move |input: CancelWorkflowInput| -> BoxFuture<
                    '_,
                    Result<(), WorkflowInteractionError>,
                > {
                    Box::pin(async move {
                        let mut request = RequestCancelWorkflowExecutionRequest {
                            namespace: client.namespace(),
                            workflow_execution: Some(ProtoWorkflowExecution {
                                workflow_id: input.workflow_id,
                                run_id: input.run_id,
                            }),
                            identity: client.identity(),
                            request_id: input
                                .options
                                .request_id
                                .clone()
                                .unwrap_or_else(|| Uuid::new_v4().to_string()),
                            first_execution_run_id: input.first_execution_run_id,
                            reason: input.options.reason.clone(),
                            links: vec![],
                        }
                        .into_request();
                        input.options.rpc_options.apply_to(&mut request);
                        WorkflowService::request_cancel_workflow_execution(&mut client, request)
                            .await
                            .map_err(WorkflowInteractionError::from_status)?;
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Terminate this workflow.
    pub async fn terminate(
        &self,
        opts: WorkflowTerminateOptions,
    ) -> Result<(), WorkflowInteractionError>
    where
        CT: NamespacedClient,
    {
        interceptors::call_terminate_workflow(
            self.client.client_interceptors(),
            TerminateWorkflowInput {
                workflow_id: self.info.workflow_id.clone(),
                run_id: self.info.run_id.clone().unwrap_or_default(),
                first_execution_run_id: self
                    .info
                    .first_execution_run_id
                    .clone()
                    .unwrap_or_default(),
                options: opts,
            },
            Next::new({
                let mut client = self.client.clone();
                move |input: TerminateWorkflowInput| -> BoxFuture<
                    '_,
                    Result<(), WorkflowInteractionError>,
                > {
                    Box::pin(async move {
                        let mut request = TerminateWorkflowExecutionRequest {
                            namespace: client.namespace(),
                            workflow_execution: Some(ProtoWorkflowExecution {
                                workflow_id: input.workflow_id,
                                run_id: input.run_id,
                            }),
                            reason: input.options.reason.clone(),
                            details: input.options.details.clone(),
                            identity: client.identity(),
                            first_execution_run_id: input.first_execution_run_id,
                            links: vec![],
                        }
                        .into_request();
                        input.options.rpc_options.apply_to(&mut request);
                        WorkflowService::terminate_workflow_execution(&mut client, request)
                            .await
                            .map_err(WorkflowInteractionError::from_status)?;
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Get workflow execution description/metadata.
    pub async fn describe(
        &self,
        opts: WorkflowDescribeOptions,
    ) -> Result<WorkflowExecutionDescription, WorkflowInteractionError>
    where
        CT: NamespacedClient,
    {
        let output = interceptors::call_describe_workflow(
            self.client.client_interceptors(),
            DescribeWorkflowInput {
                workflow_id: self.info.workflow_id.clone(),
                run_id: self.info.run_id.clone().unwrap_or_default(),
                options: opts,
            },
            Next::new({
                let mut client = self.client.clone();
                move |input: DescribeWorkflowInput| -> BoxFuture<
                        '_,
                        Result<DescribeWorkflowOutput, WorkflowInteractionError>,
                    > {
                        Box::pin(async move {
                            let mut request = DescribeWorkflowExecutionRequest {
                                namespace: client.namespace(),
                                execution: Some(ProtoWorkflowExecution {
                                    workflow_id: input.workflow_id,
                                    run_id: input.run_id,
                                }),
                            }
                            .into_request();
                            input.options.rpc_options.apply_to(&mut request);
                            let response =
                                WorkflowService::describe_workflow_execution(&mut client, request)
                                    .await
                                    .map_err(WorkflowInteractionError::from_status)?
                                    .into_inner();
                            Ok(DescribeWorkflowOutput::new(response))
                        })
                    }
            }),
        )
        .await?;
        WorkflowExecutionDescription::new(output.response, self.client.data_converter())
            .await
            .map_err(WorkflowInteractionError::from)
    }
    /// Fetch workflow execution history as a lazy stream.
    ///
    /// No request is sent until the returned stream is polled.
    pub fn fetch_history(&self, opts: WorkflowFetchHistoryOptions) -> WorkflowHistory
    where
        CT: NamespacedClient + 'static,
    {
        let run_id = self.info.run_id.clone().unwrap_or_default();
        self.fetch_history_for_run(&run_id, opts)
    }

    fn fetch_history_for_run(
        &self,
        run_id: &str,
        opts: WorkflowFetchHistoryOptions,
    ) -> WorkflowHistory
    where
        CT: NamespacedClient + 'static,
    {
        let client = self.client.clone();
        let workflow_id = self.info.workflow_id.clone();
        let history_workflow_id = workflow_id.clone();
        let run_id = run_id.to_string();

        let stream = stream::unfold(
            (Vec::new(), VecDeque::new(), false),
            move |(mut next_page_token, mut buffer, mut exhausted)| {
                let client = client.clone();
                let workflow_id = workflow_id.clone();
                let run_id = run_id.clone();
                let opts = opts.clone();

                async move {
                    loop {
                        if let Some(event) = buffer.pop_front() {
                            return Some((Ok(event), (next_page_token, buffer, exhausted)));
                        }

                        if exhausted {
                            return None;
                        }

                        let output = interceptors::call_fetch_workflow_history_page(
                            client.client_interceptors(),
                            FetchWorkflowHistoryPageInput {
                                workflow_id: workflow_id.clone(),
                                run_id: run_id.clone(),
                                next_page_token: next_page_token.clone(),
                                options: opts.clone(),
                            },
                            Next::new({
                                let mut rpc_client = client.clone();
                                move |input: FetchWorkflowHistoryPageInput| -> BoxFuture<
                                    '_,
                                    Result<
                                        FetchWorkflowHistoryPageOutput,
                                        WorkflowInteractionError,
                                    >,
                                > {
                                    Box::pin(async move {
                                        let mut request = GetWorkflowExecutionHistoryRequest {
                                            namespace: rpc_client.namespace(),
                                            execution: Some(ProtoWorkflowExecution {
                                                workflow_id: input.workflow_id,
                                                run_id: input.run_id,
                                            }),
                                            next_page_token: input.next_page_token,
                                            skip_archival: input.options.skip_archival,
                                            wait_new_event: input.options.wait_new_event,
                                            history_event_filter_type: input
                                                .options
                                                .event_filter_type
                                                as i32,
                                            ..Default::default()
                                        }
                                        .into_request();
                                        input.options.rpc_options.apply_to(&mut request);
                                        let response =
                                            WorkflowService::get_workflow_execution_history(
                                                &mut rpc_client,
                                                request,
                                            )
                                            .await
                                            .map_err(WorkflowInteractionError::from_status)?
                                            .into_inner();
                                        Ok(FetchWorkflowHistoryPageOutput::new(
                                            response
                                                .history
                                                .map(|history| history.events)
                                                .unwrap_or_default(),
                                            response.next_page_token,
                                        ))
                                    })
                                }
                            }),
                        )
                        .await;

                        match output {
                            Ok(output) => {
                                exhausted = output.next_page_token.is_empty();
                                next_page_token = output.next_page_token;
                                buffer = output.events.into();
                            }
                            Err(error) => {
                                return Some((Err(error), (next_page_token, buffer, true)));
                            }
                        }
                    }
                }
            },
        );

        WorkflowHistory {
            inner: Box::pin(stream),
            workflow_id: Some(history_workflow_id),
        }
    }
}

/// Handle to a workflow update that has been started but may not be complete.
///
/// Use [`get_result`](Self::get_result) to wait for the update to complete and retrieve its result.
pub struct WorkflowUpdateHandle<CT, T> {
    client: CT,
    update_id: String,
    workflow_id: String,
    run_id: Option<String>,
    /// If the update was started with `Completed` wait stage, the outcome is already available.
    known_outcome: Option<update::v1::Outcome>,
    _output: PhantomData<T>,
}

impl<CT, T> WorkflowUpdateHandle<CT, T> {
    pub(crate) fn new(
        client: CT,
        update_id: String,
        workflow_id: String,
        run_id: Option<String>,
        known_outcome: Option<update::v1::Outcome>,
    ) -> Self {
        Self {
            client,
            update_id,
            workflow_id,
            run_id,
            known_outcome,
            _output: PhantomData,
        }
    }

    /// Get the update ID.
    pub fn id(&self) -> &str {
        &self.update_id
    }

    /// Get the workflow ID.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Get the workflow run ID, if available.
    pub fn workflow_run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }
}

impl<CT, T: 'static> WorkflowUpdateHandle<CT, T>
where
    CT: WorkflowService + NamespacedClient + Clone,
{
    /// Wait for the update to complete and return the result using the provided RPC controls.
    pub async fn get_result(&self, rpc_options: RpcOptions) -> Result<T, WorkflowUpdateError>
    where
        T: temporalio_common::data_converters::TemporalDeserializable,
    {
        let output = interceptors::call_poll_workflow_update(
            self.client.client_interceptors(),
            PollWorkflowUpdateInput {
                update_id: self.update_id.clone(),
                workflow_id: self.workflow_id.clone(),
                run_id: self.run_id.clone().unwrap_or_default(),
                rpc_options,
            },
            Next::new({
                let mut client = self.client.clone();
                let known_outcome = self.known_outcome.clone();
                move |input: PollWorkflowUpdateInput| -> BoxFuture<
                    '_,
                    Result<PollWorkflowUpdateOutput, WorkflowUpdateError>,
                > {
                    Box::pin(async move {
                        if let Some(outcome) = known_outcome {
                            return Ok(PollWorkflowUpdateOutput::new(outcome));
                        }
                        // The server's internal long-poll timeout (~60s) may expire before the update
                        // completes, returning a response with outcome: None. Keep polling until we
                        // get an actual outcome.
                        loop {
                            let mut request = PollWorkflowExecutionUpdateRequest {
                                namespace: client.namespace(),
                                update_ref: Some(update::v1::UpdateRef {
                                    workflow_execution: Some(ProtoWorkflowExecution {
                                        workflow_id: input.workflow_id.clone(),
                                        run_id: input.run_id.clone(),
                                    }),
                                    update_id: input.update_id.clone(),
                                }),
                                identity: client.identity(),
                                wait_policy: Some(WaitPolicy {
                                    lifecycle_stage:
                                        UpdateWorkflowExecutionLifecycleStage::Completed.into(),
                                }),
                            }
                            .into_request();
                            input.rpc_options.apply_to(&mut request);
                            let response = WorkflowService::poll_workflow_execution_update(
                                &mut client,
                                request,
                            )
                            .await
                            .map_err(WorkflowUpdateError::from_status)?
                            .into_inner();
                            if let Some(outcome) = response.outcome {
                                return Ok(PollWorkflowUpdateOutput::new(outcome));
                            }
                        }
                    })
                }
            }),
        )
        .await?;
        let outcome = output.outcome;

        match outcome.value {
            Some(update::v1::outcome::Value::Success(success)) => self
                .client
                .data_converter()
                .from_payloads(
                    &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                    success.payloads,
                )
                .await
                .map_err(WorkflowUpdateError::from),
            Some(update::v1::outcome::Value::Failure(failure)) => {
                Err(WorkflowUpdateError::Failed(Box::new(failure)))
            }
            None => Err(WorkflowUpdateError::Other(
                "Update returned no outcome value".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientInterceptor, test_helpers::XorCodec};
    use futures_util::{FutureExt, StreamExt};
    use std::{
        collections::{HashMap, VecDeque},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use temporalio_common::{
        data_converters::DefaultFailureConverter,
        protos::temporal::api::{
            common::v1::{Memo, SearchAttributes},
            enums::v1::WorkflowExecutionStatus as ProtoWorkflowExecutionStatus,
            history::v1::WorkflowExecutionStartedEventAttributes,
            sdk::v1::UserMetadata,
            workflow::v1::WorkflowExecutionConfig,
            workflowservice::v1::GetWorkflowExecutionHistoryResponse,
        },
    };
    use tonic::{Request, Response};

    #[tokio::test]
    async fn workflow_history_workflow_id_roundtrips() {
        let event = HistoryEvent {
            event_id: 1,
            attributes: Some(Attributes::WorkflowExecutionStartedEventAttributes(
                WorkflowExecutionStartedEventAttributes {
                    workflow_id: "workflow-id".to_owned(),
                    original_execution_run_id: "run-id".to_owned(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let history = WorkflowHistory {
            inner: Box::pin(stream::iter(std::iter::once(Ok(event)))),
            workflow_id: None,
        };

        let bytes = history.to_json().await.unwrap();

        let decoded = WorkflowHistory::from_json(&bytes).unwrap();
        assert_eq!(decoded.workflow_id(), Some("workflow-id"));
    }

    #[derive(Clone)]
    struct MockHistoryClient {
        responses: Arc<Mutex<VecDeque<Result<GetWorkflowExecutionHistoryResponse, tonic::Status>>>>,
        calls: Arc<AtomicUsize>,
        interceptors: Vec<Arc<dyn ClientInterceptor>>,
    }

    impl NamespacedClient for MockHistoryClient {
        fn namespace(&self) -> String {
            "test-namespace".to_owned()
        }

        fn identity(&self) -> String {
            "test-identity".to_owned()
        }

        fn client_interceptors(&self) -> &[Arc<dyn ClientInterceptor>] {
            &self.interceptors
        }
    }

    impl WorkflowService for MockHistoryClient {
        fn get_workflow_execution_history(
            &mut self,
            _request: Request<GetWorkflowExecutionHistoryRequest>,
        ) -> BoxFuture<'_, Result<Response<GetWorkflowExecutionHistoryResponse>, tonic::Status>>
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let response = self.responses.lock().unwrap().pop_front().unwrap();
            async move { response.map(Response::new) }.boxed()
        }
    }

    struct CountingHistoryInterceptor(Arc<AtomicUsize>);

    impl ClientInterceptor for CountingHistoryInterceptor {
        fn fetch_workflow_history_page<'a>(
            &'a self,
            input: FetchWorkflowHistoryPageInput,
            next: Next<
                'a,
                FetchWorkflowHistoryPageInput,
                BoxFuture<'a, Result<FetchWorkflowHistoryPageOutput, WorkflowInteractionError>>,
            >,
        ) -> BoxFuture<'a, Result<FetchWorkflowHistoryPageOutput, WorkflowInteractionError>>
        {
            self.0.fetch_add(1, Ordering::SeqCst);
            next.run(input)
        }
    }

    fn history_response(
        event_ids: impl IntoIterator<Item = i64>,
        next_page_token: &[u8],
    ) -> GetWorkflowExecutionHistoryResponse {
        GetWorkflowExecutionHistoryResponse {
            history: Some(History {
                events: event_ids
                    .into_iter()
                    .map(|event_id| HistoryEvent {
                        event_id,
                        ..Default::default()
                    })
                    .collect(),
            }),
            next_page_token: next_page_token.to_vec(),
            ..Default::default()
        }
    }

    fn history_handle(
        responses: impl IntoIterator<Item = Result<GetWorkflowExecutionHistoryResponse, tonic::Status>>,
        calls: Arc<AtomicUsize>,
        interceptors: Vec<Arc<dyn ClientInterceptor>>,
    ) -> WorkflowHandle<MockHistoryClient, UntypedWorkflow> {
        WorkflowHandle::new(
            MockHistoryClient {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                calls,
                interceptors,
            },
            WorkflowExecutionInfo {
                namespace: "test-namespace".to_owned(),
                workflow_id: "workflow-id".to_owned(),
                run_id: Some("run-id".to_owned()),
                first_execution_run_id: None,
            },
        )
    }

    #[tokio::test]
    async fn workflow_history_fetches_pages_lazily() {
        let calls = Arc::new(AtomicUsize::new(0));
        let interceptor_calls = Arc::new(AtomicUsize::new(0));
        let handle = history_handle(
            [
                Ok(history_response([], b"second-page")),
                Ok(history_response([1, 2], b"third-page")),
                Ok(history_response([3], b"")),
            ],
            calls.clone(),
            vec![Arc::new(CountingHistoryInterceptor(
                interceptor_calls.clone(),
            ))],
        );

        let mut history = handle.fetch_history(WorkflowFetchHistoryOptions::default());
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        assert_eq!(history.next().await.unwrap().unwrap().event_id, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(history.next().await.unwrap().unwrap().event_id, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(history.next().await.unwrap().unwrap().event_id, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(history.next().await.is_none());
        assert_eq!(interceptor_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn workflow_history_yields_page_error_then_ends() {
        let calls = Arc::new(AtomicUsize::new(0));
        let handle = history_handle(
            [
                Ok(history_response([1], b"second-page")),
                Err(tonic::Status::unavailable("history unavailable")),
            ],
            calls.clone(),
            Vec::new(),
        );
        let mut history = handle.fetch_history(WorkflowFetchHistoryOptions::default());

        assert_eq!(history.next().await.unwrap().unwrap().event_id, 1);
        assert!(matches!(
            history.next().await.unwrap(),
            Err(WorkflowInteractionError::Rpc(status)) if status.code() == tonic::Code::Unavailable
        ));
        assert!(history.next().await.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn workflow_result_details_support_typed_decoding() {
        let converter = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            XorCodec,
        );
        let payloads = converter
            .to_payloads(
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                &"workflow-result-details".to_owned(),
            )
            .await
            .unwrap();
        let details = WorkflowResultDetails::new(payloads.clone(), &converter)
            .await
            .unwrap();

        assert_ne!(details.raw(), payloads);
        let decoded_payloads = details.raw().to_vec();
        assert_eq!(
            details.deserialize::<String>().unwrap(),
            "workflow-result-details"
        );
        assert_eq!(details.into_raw().payloads, decoded_payloads);
    }

    #[tokio::test]
    async fn workflow_result_detail_conversion_errors_are_reported() {
        let details =
            WorkflowResultDetails::new(vec![Payload::default()], &DataConverter::default())
                .await
                .unwrap();

        assert_eq!(details.raw(), &[Payload::default()]);
        assert!(details.deserialize::<String>().is_err());
    }

    #[tokio::test]
    async fn workflow_description_memo_uses_saved_converter() {
        let converter = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            XorCodec,
        );
        let encoded = converter
            .to_payload(
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                &"memo-value".to_owned(),
            )
            .await
            .unwrap();
        let description = WorkflowExecutionDescription::new(
            DescribeWorkflowExecutionResponse {
                workflow_execution_info: Some(workflow::WorkflowExecutionInfo {
                    memo: Some(Memo {
                        fields: HashMap::from([("memo-key".to_owned(), encoded)]),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &converter,
        )
        .await
        .unwrap();
        let memo = description.memo();

        assert_eq!(
            memo.get::<String>("memo-key").unwrap(),
            Some("memo-value".to_owned())
        );
    }

    #[tokio::test]
    async fn workflow_description_accessors_expose_decoded_fields() {
        let converter = DataConverter::default();
        let memo_payload = converter
            .to_payload(
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                &"memo-value",
            )
            .await
            .unwrap();
        let search_attr_payload = converter
            .to_payload(
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                &"search-value",
            )
            .await
            .unwrap();
        let summary_payload = converter
            .to_payload(
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                &"workflow summary",
            )
            .await
            .unwrap();
        let details_payload = converter
            .to_payload(
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                &"workflow details",
            )
            .await
            .unwrap();
        let description = WorkflowExecutionDescription::new(
            DescribeWorkflowExecutionResponse {
                workflow_execution_info: Some(workflow::WorkflowExecutionInfo {
                    execution: Some(ProtoWorkflowExecution {
                        workflow_id: "wf-id".to_string(),
                        run_id: "run-id".to_string(),
                    }),
                    r#type: Some(
                        temporalio_common::protos::temporal::api::common::v1::WorkflowType {
                            name: "wf-type".to_string(),
                        },
                    ),
                    status: ProtoWorkflowExecutionStatus::Completed as i32,
                    task_queue: "task-queue".to_string(),
                    history_length: 42,
                    memo: Some(Memo {
                        fields: HashMap::from([("memo-key".to_string(), memo_payload.clone())]),
                    }),
                    parent_execution: Some(ProtoWorkflowExecution {
                        workflow_id: "parent-id".to_string(),
                        run_id: "parent-run-id".to_string(),
                    }),
                    search_attributes: Some(SearchAttributes {
                        indexed_fields: HashMap::from([(
                            "CustomKeywordField".to_string(),
                            search_attr_payload.clone(),
                        )]),
                    }),
                    ..Default::default()
                }),
                execution_config: Some(WorkflowExecutionConfig {
                    user_metadata: Some(UserMetadata {
                        summary: Some(summary_payload),
                        details: Some(details_payload),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &converter,
        )
        .await
        .unwrap();

        assert_eq!(description.id(), "wf-id");
        assert_eq!(description.run_id(), "run-id");
        assert_eq!(description.workflow_type(), "wf-type");
        assert_eq!(description.status(), WorkflowExecutionStatus::Completed);
        let mut unknown_status_description = description.clone();
        unknown_status_description
            .raw_description
            .workflow_execution_info
            .as_mut()
            .unwrap()
            .status = 123_456;
        assert_eq!(
            unknown_status_description.status(),
            WorkflowExecutionStatus::Unknown
        );
        assert_eq!(description.task_queue(), "task-queue");
        assert_eq!(description.history_length(), 42);
        assert_eq!(description.parent_id(), Some("parent-id"));
        assert_eq!(description.parent_run_id(), Some("parent-run-id"));
        let memo = description.memo();
        assert_eq!(memo.raw_value("memo-key"), Some(&memo_payload));
        assert_eq!(
            memo.get::<String>("memo-key").unwrap(),
            Some("memo-value".to_owned())
        );
        let search_attributes = description.search_attributes();
        assert_eq!(
            search_attributes.raw_payload("CustomKeywordField"),
            Some(&search_attr_payload)
        );
        assert_eq!(description.static_summary(), Some("workflow summary"));
        assert_eq!(description.static_details(), Some("workflow details"));
    }

    #[tokio::test]
    async fn workflow_description_rejects_negative_history_length() {
        let err = WorkflowExecutionDescription::new(
            DescribeWorkflowExecutionResponse {
                workflow_execution_info: Some(workflow::WorkflowExecutionInfo {
                    history_length: -1,
                    ..Default::default()
                }),
                ..Default::default()
            },
            &DataConverter::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Encoding error: workflow history_length must be non-negative, got -1"
        );
    }
}
