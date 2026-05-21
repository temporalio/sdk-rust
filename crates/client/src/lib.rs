#![warn(missing_docs)] // error if there are missing docs

//! This crate contains client implementations that can be used to contact the Temporal service.
//!
//! It implements auto-retry behavior and metrics collection.

#[macro_use]
extern crate tracing;

mod async_activity_handle;
pub mod callback_based;
mod dns;
/// Configuration loading from environment variables and TOML files.
#[cfg(feature = "envconfig")]
pub mod envconfig;
pub mod errors;
pub mod grpc;
mod metrics;
mod options_structs;
/// Visible only for tests
#[doc(hidden)]
pub mod proxy;
mod replaceable;
pub mod request_extensions;
mod retry;
/// Schedule operations: create, describe, update, pause, trigger, backfill, list, and delete.
pub mod schedules;
pub mod worker;
mod workflow_handle;

pub use crate::{
    proxy::HttpConnectProxyOptions,
    retry::{CallType, RETRYABLE_ERROR_CODES},
};
pub use async_activity_handle::{
    ActivityHeartbeatResponse, ActivityIdentifier, AsyncActivityHandle,
};

pub use metrics::{LONG_REQUEST_LATENCY_HISTOGRAM_NAME, REQUEST_LATENCY_HISTOGRAM_NAME};
pub use options_structs::*;
pub use replaceable::SharedReplaceableClient;
pub use retry::RetryOptions;
pub use tonic;
pub use workflow_handle::{
    UntypedQuery, UntypedSignal, UntypedUpdate, UntypedWorkflow, UntypedWorkflowHandle,
    WorkflowExecutionDescription, WorkflowExecutionInfo, WorkflowExecutionResult, WorkflowHandle,
    WorkflowHistory, WorkflowUpdateHandle,
};

use crate::{
    grpc::{
        AttachMetricLabels, CloudService, HealthService, OperatorService, TestService,
        WorkflowService,
    },
    metrics::{ChannelOrGrpcOverride, GrpcMetricSvc, MetricsContext},
    request_extensions::RequestExt,
    worker::ClientWorkerSet,
};
use errors::*;
use futures_util::{stream, stream::Stream};
use http::Uri;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    pin::Pin,
    str::FromStr,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::{Duration, SystemTime},
};
use temporalio_common::{
    HasWorkflowDefinition,
    data_converters::{
        DataConverter, GenericPayloadConverter, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    protos::{
        coresdk::IntoPayloadsExt,
        grpc::health::v1::health_client::HealthClient,
        proto_ts_to_system_time,
        temporal::api::{
            cloud::cloudservice::v1::cloud_service_client::CloudServiceClient,
            common::v1::{Memo, Payload, SearchAttributes, WorkflowType},
            enums::v1::{TaskQueueKind, WorkflowExecutionStatus},
            errordetails::v1::WorkflowExecutionAlreadyStartedFailure,
            operatorservice::v1::operator_service_client::OperatorServiceClient,
            sdk::v1::UserMetadata,
            taskqueue::v1::TaskQueue,
            testservice::v1::test_service_client::TestServiceClient,
            workflow::v1 as workflow,
            workflowservice::v1::{
                count_workflow_executions_response, workflow_service_client::WorkflowServiceClient,
                *,
            },
        },
        utilities::decode_status_detail,
    },
};
use tonic::{
    Code, IntoRequest,
    body::Body,
    client::GrpcService,
    codegen::InterceptedService,
    metadata::{
        AsciiMetadataKey, AsciiMetadataValue, BinaryMetadataKey, BinaryMetadataValue, MetadataMap,
        MetadataValue,
    },
    service::Interceptor,
    transport::{Certificate, Endpoint, Identity},
};
use tower::ServiceBuilder;
use uuid::Uuid;

static CLIENT_NAME_HEADER_KEY: &str = "client-name";
static CLIENT_VERSION_HEADER_KEY: &str = "client-version";
static TEMPORAL_NAMESPACE_HEADER_KEY: &str = "temporal-namespace";

#[doc(hidden)]
/// Key used to communicate when a GRPC message is too large
pub static MESSAGE_TOO_LARGE_KEY: &str = "message-too-large";
#[doc(hidden)]
/// Key used to indicate a error was returned by the retryer because of the short-circuit predicate
pub static ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT: &str = "short-circuit";

/// The server times out polls after 60 seconds. Set our timeout to be slightly beyond that.
const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(70);
const OTHER_CALL_TIMEOUT: Duration = Duration::from_secs(30);
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// A connection to the Temporal service.
///
/// Cloning a connection is cheap (single Arc increment). The underlying connection is shared
/// between clones.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

#[derive(Clone)]
struct ConnectionInner {
    service: TemporalServiceClient,
    retry_options: RetryOptions,
    identity: String,
    headers: Arc<RwLock<ClientHeaders>>,
    client_name: String,
    client_version: String,
    /// Capabilities as read from the `get_system_info` RPC call made on client connection
    capabilities: Option<get_system_info_response::Capabilities>,
    workers: Arc<ClientWorkerSet>,
    _dns_task: Option<Arc<dns::DnsReresolutionHandle>>,
}

impl Connection {
    /// Connect to a Temporal service.
    pub async fn connect(options: ConnectionOptions) -> Result<Self, ClientConnectError> {
        let dns_lb_opts = dns::validate_and_get_dns_lb(&options)?.cloned();
        let (service, dns_task) = if let Some(service_override) = options.service_override {
            (
                GrpcMetricSvc {
                    inner: ChannelOrGrpcOverride::GrpcOverride(service_override),
                    metrics: options.metrics_meter.clone().map(MetricsContext::new),
                    disable_errcode_label: options.disable_error_code_metric_tags,
                },
                None,
            )
        } else if let Some(dns_opts) = &dns_lb_opts {
            let (channel, sender) = dns::create_balanced_channel(&options).await?;
            let handle = dns::spawn_dns_reresolution(
                sender,
                options.target.clone(),
                options.tls_options.clone(),
                options.keep_alive.clone(),
                options.override_origin.clone(),
                dns_opts.resolution_interval,
            );
            (
                ServiceBuilder::new()
                    .layer_fn(move |channel| GrpcMetricSvc {
                        inner: ChannelOrGrpcOverride::Channel(channel),
                        metrics: options.metrics_meter.clone().map(MetricsContext::new),
                        disable_errcode_label: options.disable_error_code_metric_tags,
                    })
                    .service(channel),
                Some(handle),
            )
        } else {
            let channel = Endpoint::from_shared(options.target.to_string())?;
            let channel = add_tls_to_channel(options.tls_options.as_ref(), channel).await?;
            let channel = if let Some(keep_alive) = options.keep_alive.as_ref() {
                channel
                    .keep_alive_while_idle(true)
                    .http2_keep_alive_interval(keep_alive.interval)
                    .keep_alive_timeout(keep_alive.timeout)
            } else {
                channel
            };
            let channel = if let Some(origin) = options.override_origin.clone() {
                channel.origin(origin)
            } else {
                channel
            };
            // If there is a proxy, we have to connect that way
            let channel = if let Some(proxy) = options.http_connect_proxy.as_ref() {
                proxy.connect_endpoint(&channel).await?
            } else {
                channel.connect().await?
            };
            (
                ServiceBuilder::new()
                    .layer_fn(move |channel| GrpcMetricSvc {
                        inner: ChannelOrGrpcOverride::Channel(channel),
                        metrics: options.metrics_meter.clone().map(MetricsContext::new),
                        disable_errcode_label: options.disable_error_code_metric_tags,
                    })
                    .service(channel),
                None,
            )
        };

        let headers = Arc::new(RwLock::new(ClientHeaders {
            user_headers: parse_ascii_headers(options.headers.clone().unwrap_or_default())?,
            user_binary_headers: parse_binary_headers(
                options.binary_headers.clone().unwrap_or_default(),
            )?,
            api_key: options.api_key.clone(),
        }));
        let interceptor = ServiceCallInterceptor {
            client_name: options.client_name.clone(),
            client_version: options.client_version.clone(),
            headers: headers.clone(),
        };
        let svc = InterceptedService::new(service, interceptor);
        let mut svc_client = TemporalServiceClient::new(svc);

        let capabilities = if !options.skip_get_system_info {
            match svc_client
                .get_system_info(GetSystemInfoRequest::default().into_request())
                .await
            {
                Ok(sysinfo) => sysinfo.into_inner().capabilities,
                Err(status) => match status.code() {
                    Code::Unimplemented => None,
                    _ => return Err(ClientConnectError::SystemInfoCallError(status)),
                },
            }
        } else {
            None
        };
        Ok(Self {
            inner: Arc::new(ConnectionInner {
                service: svc_client,
                retry_options: options.retry_options,
                identity: options.identity,
                headers,
                client_name: options.client_name,
                client_version: options.client_version,
                capabilities,
                workers: Arc::new(ClientWorkerSet::new()),
                _dns_task: dns_task,
            }),
        })
    }

    /// Set API key, overwriting any previous one.
    pub fn set_api_key(&self, api_key: Option<String>) {
        self.inner.headers.write().api_key = api_key;
    }

    /// Set HTTP request headers overwriting previous headers.
    ///
    /// This will not affect headers set via [ConnectionOptions::binary_headers].
    ///
    /// # Errors
    ///
    /// Will return an error if any of the provided keys or values are not valid gRPC metadata.
    /// If an error is returned, the previous headers will remain unchanged.
    pub fn set_headers(&self, headers: HashMap<String, String>) -> Result<(), InvalidHeaderError> {
        self.inner.headers.write().user_headers = parse_ascii_headers(headers)?;
        Ok(())
    }

    /// Set binary HTTP request headers overwriting previous headers.
    ///
    /// This will not affect headers set via [ConnectionOptions::headers].
    ///
    /// # Errors
    ///
    /// Will return an error if any of the provided keys are not valid gRPC binary metadata keys.
    /// If an error is returned, the previous headers will remain unchanged.
    pub fn set_binary_headers(
        &self,
        binary_headers: HashMap<String, Vec<u8>>,
    ) -> Result<(), InvalidHeaderError> {
        self.inner.headers.write().user_binary_headers = parse_binary_headers(binary_headers)?;
        Ok(())
    }

    /// Returns the value used for the `client-name` header by this connection.
    pub fn client_name(&self) -> &str {
        &self.inner.client_name
    }

    /// Returns the value used for the `client-version` header by this connection.
    pub fn client_version(&self) -> &str {
        &self.inner.client_version
    }

    /// Returns the server capabilities we (may have) learned about when establishing an initial
    /// connection
    pub fn capabilities(&self) -> Option<&get_system_info_response::Capabilities> {
        self.inner.capabilities.as_ref()
    }

    /// Get a mutable reference to the retry options.
    ///
    /// Note: If this connection has been cloned, this will copy-on-write to avoid
    /// affecting other clones.
    pub fn retry_options_mut(&mut self) -> &mut RetryOptions {
        &mut Arc::make_mut(&mut self.inner).retry_options
    }

    /// Get a reference to the connection identity.
    pub fn identity(&self) -> &str {
        &self.inner.identity
    }

    /// Get a mutable reference to the connection identity.
    ///
    /// Note: If this connection has been cloned, this will copy-on-write to avoid
    /// affecting other clones.
    pub fn identity_mut(&mut self) -> &mut String {
        &mut Arc::make_mut(&mut self.inner).identity
    }

    /// Returns a reference to a registry with workers using this client instance.
    pub fn workers(&self) -> Arc<ClientWorkerSet> {
        self.inner.workers.clone()
    }

    /// Returns the client-wide key.
    pub fn worker_grouping_key(&self) -> Uuid {
        self.inner.workers.worker_grouping_key()
    }

    /// Get the underlying workflow service client for making raw gRPC calls.
    pub fn workflow_service(&self) -> Box<dyn WorkflowService> {
        self.inner.service.workflow_service()
    }

    /// Get the underlying operator service client for making raw gRPC calls.
    pub fn operator_service(&self) -> Box<dyn OperatorService> {
        self.inner.service.operator_service()
    }

    /// Get the underlying cloud service client for making raw gRPC calls.
    pub fn cloud_service(&self) -> Box<dyn CloudService> {
        self.inner.service.cloud_service()
    }

    /// Get the underlying test service client for making raw gRPC calls.
    pub fn test_service(&self) -> Box<dyn TestService> {
        self.inner.service.test_service()
    }

    /// Get the underlying health service client for making raw gRPC calls.
    pub fn health_service(&self) -> Box<dyn HealthService> {
        self.inner.service.health_service()
    }
}

#[derive(Debug)]
struct ClientHeaders {
    user_headers: HashMap<AsciiMetadataKey, AsciiMetadataValue>,
    user_binary_headers: HashMap<BinaryMetadataKey, BinaryMetadataValue>,
    api_key: Option<String>,
}

impl ClientHeaders {
    fn apply_to_metadata(&self, metadata: &mut MetadataMap) {
        for (key, val) in self.user_headers.iter() {
            // Only if not already present
            if !metadata.contains_key(key) {
                metadata.insert(key, val.clone());
            }
        }
        for (key, val) in self.user_binary_headers.iter() {
            // Only if not already present
            if !metadata.contains_key(key) {
                metadata.insert_bin(key, val.clone());
            }
        }
        if let Some(api_key) = &self.api_key {
            // Only if not already present
            if !metadata.contains_key("authorization")
                && let Ok(val) = format!("Bearer {api_key}").parse()
            {
                metadata.insert("authorization", val);
            }
        }
    }
}

/// If TLS is configured, set the appropriate options on the provided channel and return it.
/// Passes it through if TLS options not set.
async fn add_tls_to_channel(
    tls_options: Option<&TlsOptions>,
    mut channel: Endpoint,
) -> Result<Endpoint, ClientConnectError> {
    if let Some(tls_cfg) = tls_options {
        let mut tls = tonic::transport::ClientTlsConfig::new();

        if let Some(root_cert) = &tls_cfg.server_root_ca_cert {
            let server_root_ca_cert = Certificate::from_pem(root_cert);
            tls = tls.ca_certificate(server_root_ca_cert);
        } else {
            tls = tls.with_native_roots();
        }

        if let Some(domain) = &tls_cfg.domain {
            tls = tls.domain_name(domain);

            // This song and dance ultimately is just to make sure the `:authority` header ends
            // up correct on requests while we use TLS. Setting the header directly in our
            // interceptor doesn't work since seemingly it is overridden at some point by
            // something lower level.
            let uri: Uri = format!("https://{domain}").parse()?;
            channel = channel.origin(uri);
        }

        if let Some(client_opts) = &tls_cfg.client_tls_options {
            let client_identity =
                Identity::from_pem(&client_opts.client_cert, &client_opts.client_private_key);
            tls = tls.identity(client_identity);
        }

        return channel.tls_config(tls).map_err(Into::into);
    }
    Ok(channel)
}

fn parse_ascii_headers(
    headers: HashMap<String, String>,
) -> Result<HashMap<AsciiMetadataKey, AsciiMetadataValue>, InvalidHeaderError> {
    let mut parsed_headers = HashMap::with_capacity(headers.len());
    for (k, v) in headers.into_iter() {
        let key = match AsciiMetadataKey::from_str(&k) {
            Ok(key) => key,
            Err(err) => {
                return Err(InvalidHeaderError::InvalidAsciiHeaderKey {
                    key: k,
                    source: err,
                });
            }
        };
        let value = match MetadataValue::from_str(&v) {
            Ok(value) => value,
            Err(err) => {
                return Err(InvalidHeaderError::InvalidAsciiHeaderValue {
                    key: k,
                    value: v,
                    source: err,
                });
            }
        };
        parsed_headers.insert(key, value);
    }

    Ok(parsed_headers)
}

fn parse_binary_headers(
    headers: HashMap<String, Vec<u8>>,
) -> Result<HashMap<BinaryMetadataKey, BinaryMetadataValue>, InvalidHeaderError> {
    let mut parsed_headers = HashMap::with_capacity(headers.len());
    for (k, v) in headers.into_iter() {
        let key = match BinaryMetadataKey::from_str(&k) {
            Ok(key) => key,
            Err(err) => {
                return Err(InvalidHeaderError::InvalidBinaryHeaderKey {
                    key: k,
                    source: err,
                });
            }
        };
        let value = BinaryMetadataValue::from_bytes(&v);
        parsed_headers.insert(key, value);
    }

    Ok(parsed_headers)
}

/// Interceptor which attaches common metadata (like "client-name") to every outgoing call
#[derive(Clone)]
pub struct ServiceCallInterceptor {
    client_name: String,
    client_version: String,
    /// Only accessed as a reader
    headers: Arc<RwLock<ClientHeaders>>,
}

impl Interceptor for ServiceCallInterceptor {
    /// This function will get called on each outbound request. Returning a `Status` here will
    /// cancel the request and have that status returned to the caller.
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let metadata = request.metadata_mut();
        if !metadata.contains_key(CLIENT_NAME_HEADER_KEY) {
            metadata.insert(
                CLIENT_NAME_HEADER_KEY,
                self.client_name
                    .parse()
                    .unwrap_or_else(|_| MetadataValue::from_static("")),
            );
        }
        if !metadata.contains_key(CLIENT_VERSION_HEADER_KEY) {
            metadata.insert(
                CLIENT_VERSION_HEADER_KEY,
                self.client_version
                    .parse()
                    .unwrap_or_else(|_| MetadataValue::from_static("")),
            );
        }
        self.headers.read().apply_to_metadata(metadata);
        request.set_default_timeout(OTHER_CALL_TIMEOUT);

        Ok(request)
    }
}

/// Aggregates various services exposed by the Temporal server
#[derive(Clone)]
pub struct TemporalServiceClient {
    workflow_svc_client: Box<dyn WorkflowService>,
    operator_svc_client: Box<dyn OperatorService>,
    cloud_svc_client: Box<dyn CloudService>,
    test_svc_client: Box<dyn TestService>,
    health_svc_client: Box<dyn HealthService>,
}

/// We up the limit on incoming messages from server from the 4Mb default to 128Mb. If for
/// whatever reason this needs to be changed by the user, we support overriding it via env var.
fn get_decode_max_size() -> usize {
    static _DECODE_MAX_SIZE: OnceLock<usize> = OnceLock::new();
    *_DECODE_MAX_SIZE.get_or_init(|| {
        std::env::var("TEMPORAL_MAX_INCOMING_GRPC_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128 * 1024 * 1024)
    })
}

impl TemporalServiceClient {
    fn new<T>(svc: T) -> Self
    where
        T: GrpcService<Body> + Send + Sync + Clone + 'static,
        T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
        T::Error: Into<tonic::codegen::StdError>,
        <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
        <T as GrpcService<Body>>::Future: Send,
    {
        let workflow_svc_client = Box::new(
            WorkflowServiceClient::new(svc.clone())
                .max_decoding_message_size(get_decode_max_size()),
        );
        let operator_svc_client = Box::new(
            OperatorServiceClient::new(svc.clone())
                .max_decoding_message_size(get_decode_max_size()),
        );
        let cloud_svc_client = Box::new(
            CloudServiceClient::new(svc.clone()).max_decoding_message_size(get_decode_max_size()),
        );
        let test_svc_client = Box::new(
            TestServiceClient::new(svc.clone()).max_decoding_message_size(get_decode_max_size()),
        );
        let health_svc_client = Box::new(
            HealthClient::new(svc.clone()).max_decoding_message_size(get_decode_max_size()),
        );

        Self {
            workflow_svc_client,
            operator_svc_client,
            cloud_svc_client,
            test_svc_client,
            health_svc_client,
        }
    }

    /// Create a service client from implementations of the individual underlying services. Useful
    /// for mocking out service implementations.
    pub fn from_services(
        workflow: Box<dyn WorkflowService>,
        operator: Box<dyn OperatorService>,
        cloud: Box<dyn CloudService>,
        test: Box<dyn TestService>,
        health: Box<dyn HealthService>,
    ) -> Self {
        Self {
            workflow_svc_client: workflow,
            operator_svc_client: operator,
            cloud_svc_client: cloud,
            test_svc_client: test,
            health_svc_client: health,
        }
    }

    /// Get the underlying workflow service client
    pub fn workflow_service(&self) -> Box<dyn WorkflowService> {
        self.workflow_svc_client.clone()
    }
    /// Get the underlying operator service client
    pub fn operator_service(&self) -> Box<dyn OperatorService> {
        self.operator_svc_client.clone()
    }
    /// Get the underlying cloud service client
    pub fn cloud_service(&self) -> Box<dyn CloudService> {
        self.cloud_svc_client.clone()
    }
    /// Get the underlying test service client
    pub fn test_service(&self) -> Box<dyn TestService> {
        self.test_svc_client.clone()
    }
    /// Get the underlying health service client
    pub fn health_service(&self) -> Box<dyn HealthService> {
        self.health_svc_client.clone()
    }
}

/// Contains an instance of a namespace-bound client for interacting with the Temporal server.
/// Cheap to clone.
#[derive(Clone)]
pub struct Client {
    connection: Connection,
    options: Arc<ClientOptions>,
}

impl Client {
    /// Create a new client from a connection and options.
    ///
    /// Currently infallible, but returns a `Result` for future extensibility
    /// (e.g., interceptor or plugin validation).
    pub fn new(connection: Connection, options: ClientOptions) -> Result<Self, ClientNewError> {
        Ok(Client {
            connection,
            options: Arc::new(options),
        })
    }

    /// Return the options this client was initialized with
    pub fn options(&self) -> &ClientOptions {
        &self.options
    }

    /// Return this client's options mutably.
    ///
    /// Note: If this client has been cloned, this will copy-on-write to avoid affecting other
    /// clones.
    pub fn options_mut(&mut self) -> &mut ClientOptions {
        Arc::make_mut(&mut self.options)
    }

    /// Returns a reference to the underlying connection
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Returns a mutable reference to the underlying connection
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }
}

// High-level workflow operations on Client.
// These forward to the internal WorkflowClientTrait blanket impl which is
// available because Client implements WorkflowService + NamespacedClient + Clone.
impl Client {
    /// Start a workflow execution.
    ///
    /// Returns a [`WorkflowHandle`] that can be used to interact with the workflow
    /// (e.g., get its result, send signals, query, etc.).
    pub async fn start_workflow<W>(
        &self,
        workflow: W,
        input: W::Input,
        options: WorkflowStartOptions,
    ) -> Result<WorkflowHandle<Self, W>, WorkflowStartError>
    where
        W: HasWorkflowDefinition,
        W::Input: Send,
    {
        WorkflowClientTrait::start_workflow(self, workflow, input, options).await
    }

    /// Get a handle to an existing workflow.
    ///
    /// For untyped access, use `get_workflow_handle::<UntypedWorkflow>(...)`.
    pub fn get_workflow_handle<W: HasWorkflowDefinition>(
        &self,
        workflow_id: impl Into<String>,
    ) -> WorkflowHandle<Self, W> {
        WorkflowClientTrait::get_workflow_handle(self, workflow_id)
    }

    /// List workflows matching a query.
    ///
    /// Returns a stream that lazily paginates through results.
    /// Use `limit` in options to cap the number of results returned.
    pub fn list_workflows(
        &self,
        query: impl Into<String>,
        opts: WorkflowListOptions,
    ) -> ListWorkflowsStream {
        WorkflowClientTrait::list_workflows(self, query, opts)
    }

    /// Count workflows matching a query.
    pub async fn count_workflows(
        &self,
        query: impl Into<String>,
        opts: WorkflowCountOptions,
    ) -> Result<WorkflowExecutionCount, ClientError> {
        WorkflowClientTrait::count_workflows(self, query, opts).await
    }

    /// Get a handle to complete an activity asynchronously.
    ///
    /// An activity returning `ActivityError::WillCompleteAsync` can be completed with this handle.
    pub fn get_async_activity_handle(
        &self,
        identifier: ActivityIdentifier,
    ) -> AsyncActivityHandle<Self> {
        WorkflowClientTrait::get_async_activity_handle(self, identifier)
    }
}

impl NamespacedClient for Client {
    fn namespace(&self) -> String {
        self.options.namespace.clone()
    }

    fn identity(&self) -> String {
        self.connection.identity().to_owned()
    }

    fn data_converter(&self) -> &DataConverter {
        &self.options.data_converter
    }
}

/// Enum to help reference a namespace by either the namespace name or the namespace id
#[derive(Clone)]
pub enum Namespace {
    /// Namespace name
    Name(String),
    /// Namespace id
    Id(String),
}

impl Namespace {
    /// Convert into grpc request
    pub fn into_describe_namespace_request(self) -> DescribeNamespaceRequest {
        let (namespace, id) = match self {
            Namespace::Name(n) => (n, "".to_owned()),
            Namespace::Id(n) => ("".to_owned(), n),
        };
        DescribeNamespaceRequest { namespace, id }
    }
}

/// This trait provides higher-level friendlier interaction with the server.
/// See the [WorkflowService] trait for a lower-level client.
pub(crate) trait WorkflowClientTrait: NamespacedClient {
    /// Start a workflow execution.
    fn start_workflow<W>(
        &self,
        workflow: W,
        input: W::Input,
        options: WorkflowStartOptions,
    ) -> impl Future<Output = Result<WorkflowHandle<Self, W>, WorkflowStartError>>
    where
        Self: Sized,
        W: HasWorkflowDefinition,
        W::Input: Send;

    /// Get a handle to an existing workflow. `run_id` may be left blank to specify the most recent
    /// execution having the provided `workflow_id`.
    ///
    /// For untyped access, use `get_workflow_handle::<UntypedWorkflow>(...)`.
    ///
    /// See also [WorkflowHandle::new], for specifying namespace or first_execution_run_id.
    fn get_workflow_handle<W: HasWorkflowDefinition>(
        &self,
        workflow_id: impl Into<String>,
    ) -> WorkflowHandle<Self, W>
    where
        Self: Sized;

    /// List workflows matching a query.
    /// Returns a stream that lazily paginates through results.
    /// Use `limit` in options to cap the number of results returned.
    fn list_workflows(
        &self,
        query: impl Into<String>,
        opts: WorkflowListOptions,
    ) -> ListWorkflowsStream;

    /// Count workflows matching a query.
    fn count_workflows(
        &self,
        query: impl Into<String>,
        opts: WorkflowCountOptions,
    ) -> impl Future<Output = Result<WorkflowExecutionCount, ClientError>>;

    /// Get a handle to complete an activity asynchronously.
    ///
    /// An activity returning `ActivityError::WillCompleteAsync` can be completed with this handle.
    fn get_async_activity_handle(
        &self,
        identifier: ActivityIdentifier,
    ) -> AsyncActivityHandle<Self>
    where
        Self: Sized;
}

/// A client that is bound to a namespace
pub trait NamespacedClient {
    /// Returns the namespace this client is bound to
    fn namespace(&self) -> String;
    /// Returns the client identity
    fn identity(&self) -> String;
    /// Returns the data converter for serializing/deserializing payloads.
    /// Default implementation returns a static default converter.
    fn data_converter(&self) -> &DataConverter {
        static DEFAULT: OnceLock<DataConverter> = OnceLock::new();
        DEFAULT.get_or_init(DataConverter::default)
    }
}

/// A workflow execution returned from list operations.
/// This represents information about a workflow present in visibility.
#[derive(Debug, Clone)]
pub struct WorkflowExecution {
    raw: workflow::WorkflowExecutionInfo,
}

impl WorkflowExecution {
    /// Create a new WorkflowExecution from the raw proto.
    pub fn new(raw: workflow::WorkflowExecutionInfo) -> Self {
        Self { raw }
    }

    /// The workflow ID.
    pub fn id(&self) -> &str {
        self.raw
            .execution
            .as_ref()
            .map(|e| e.workflow_id.as_str())
            .unwrap_or("")
    }

    /// The run ID.
    pub fn run_id(&self) -> &str {
        self.raw
            .execution
            .as_ref()
            .map(|e| e.run_id.as_str())
            .unwrap_or("")
    }

    /// The workflow type name.
    pub fn workflow_type(&self) -> &str {
        self.raw
            .r#type
            .as_ref()
            .map(|t| t.name.as_str())
            .unwrap_or("")
    }

    /// The current status of the workflow execution.
    pub fn status(&self) -> WorkflowExecutionStatus {
        self.raw.status()
    }

    /// When the workflow was created.
    pub fn start_time(&self) -> Option<SystemTime> {
        self.raw
            .start_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// When the workflow run started or should start.
    pub fn execution_time(&self) -> Option<SystemTime> {
        self.raw
            .execution_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// When the workflow was closed, if closed.
    pub fn close_time(&self) -> Option<SystemTime> {
        self.raw
            .close_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// The task queue the workflow runs on.
    pub fn task_queue(&self) -> &str {
        &self.raw.task_queue
    }

    /// Number of events in history.
    pub fn history_length(&self) -> i64 {
        self.raw.history_length
    }

    /// Workflow memo.
    pub fn memo(&self) -> Option<&Memo> {
        self.raw.memo.as_ref()
    }

    /// Parent workflow ID, if this is a child workflow.
    pub fn parent_id(&self) -> Option<&str> {
        self.raw
            .parent_execution
            .as_ref()
            .map(|e| e.workflow_id.as_str())
    }

    /// Parent run ID, if this is a child workflow.
    pub fn parent_run_id(&self) -> Option<&str> {
        self.raw
            .parent_execution
            .as_ref()
            .map(|e| e.run_id.as_str())
    }

    /// Search attributes on the workflow.
    pub fn search_attributes(&self) -> Option<&SearchAttributes> {
        self.raw.search_attributes.as_ref()
    }

    /// Access the raw proto for additional fields not exposed via accessors.
    pub fn raw(&self) -> &workflow::WorkflowExecutionInfo {
        &self.raw
    }

    /// Consume the wrapper and return the raw proto.
    pub fn into_raw(self) -> workflow::WorkflowExecutionInfo {
        self.raw
    }
}

impl From<workflow::WorkflowExecutionInfo> for WorkflowExecution {
    fn from(raw: workflow::WorkflowExecutionInfo) -> Self {
        Self::new(raw)
    }
}

/// A stream of workflow executions from a list query.
/// Internally paginates through results from the server.
pub struct ListWorkflowsStream {
    inner: Pin<Box<dyn Stream<Item = Result<WorkflowExecution, ClientError>> + Send>>,
}

impl ListWorkflowsStream {
    fn new(
        inner: Pin<Box<dyn Stream<Item = Result<WorkflowExecution, ClientError>> + Send>>,
    ) -> Self {
        Self { inner }
    }
}

impl Stream for ListWorkflowsStream {
    type Item = Result<WorkflowExecution, ClientError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Result of a workflow count operation.
///
/// If the query includes a group-by clause, `groups` will contain the aggregated
/// counts and `count` will be the sum of all group counts.
#[derive(Debug, Clone)]
pub struct WorkflowExecutionCount {
    count: usize,
    groups: Vec<WorkflowCountAggregationGroup>,
}

impl WorkflowExecutionCount {
    pub(crate) fn from_response(resp: CountWorkflowExecutionsResponse) -> Self {
        Self {
            count: resp.count as usize,
            groups: resp
                .groups
                .into_iter()
                .map(WorkflowCountAggregationGroup::from_proto)
                .collect(),
        }
    }

    /// The approximate number of workflows matching the query.
    /// If grouping was applied, this is the sum of all group counts.
    pub fn count(&self) -> usize {
        self.count
    }

    /// The groups if the query had a group-by clause, or empty if not.
    pub fn groups(&self) -> &[WorkflowCountAggregationGroup] {
        &self.groups
    }
}

/// Aggregation group from a workflow count query with a group-by clause.
#[derive(Debug, Clone)]
pub struct WorkflowCountAggregationGroup {
    group_values: Vec<Payload>,
    count: usize,
}

impl WorkflowCountAggregationGroup {
    fn from_proto(proto: count_workflow_executions_response::AggregationGroup) -> Self {
        Self {
            group_values: proto.group_values,
            count: proto.count as usize,
        }
    }

    /// The search attribute values for this group.
    pub fn group_values(&self) -> &[Payload] {
        &self.group_values
    }

    /// The approximate number of workflows matching for this group.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl<T> WorkflowClientTrait for T
where
    T: WorkflowService + NamespacedClient + Clone + Send + Sync + 'static,
{
    async fn start_workflow<W>(
        &self,
        workflow: W,
        input: W::Input,
        options: WorkflowStartOptions,
    ) -> Result<WorkflowHandle<Self, W>, WorkflowStartError>
    where
        W: HasWorkflowDefinition,
        W::Input: Send,
    {
        let payloads = self
            .data_converter()
            .to_payloads(&SerializationContextData::Workflow, &input)
            .await?;
        let namespace = self.namespace();
        let workflow_id = options.workflow_id.clone();
        let task_queue_name = options.task_queue.clone();

        let user_metadata = if options.static_summary.is_some() || options.static_details.is_some()
        {
            let payload_converter = PayloadConverter::default();
            let context = SerializationContext {
                data: &SerializationContextData::Workflow,
                converter: &payload_converter,
            };
            Some(UserMetadata {
                summary: options.static_summary.map(|s| {
                    payload_converter
                        .to_payload(&context, &s)
                        .expect("String-to-JSON payload serialization is infallible")
                }),
                details: options.static_details.map(|s| {
                    payload_converter
                        .to_payload(&context, &s)
                        .expect("String-to-JSON payload serialization is infallible")
                }),
            })
        } else {
            None
        };

        let run_id = if let Some(start_signal) = options.start_signal {
            // Use signal-with-start when a start_signal is provided
            let res = WorkflowService::signal_with_start_workflow_execution(
                &mut self.clone(),
                SignalWithStartWorkflowExecutionRequest {
                    namespace: namespace.clone(),
                    workflow_id: workflow_id.clone(),
                    workflow_type: Some(WorkflowType {
                        name: workflow.name().to_string(),
                    }),
                    task_queue: Some(TaskQueue {
                        name: task_queue_name,
                        kind: TaskQueueKind::Normal as i32,
                        normal_name: "".to_string(),
                    }),
                    input: payloads.into_payloads(),
                    signal_name: start_signal.signal_name,
                    signal_input: start_signal.input,
                    identity: self.identity(),
                    request_id: Uuid::new_v4().to_string(),
                    workflow_id_reuse_policy: options.id_reuse_policy as i32,
                    workflow_id_conflict_policy: options.id_conflict_policy as i32,
                    workflow_execution_timeout: options
                        .execution_timeout
                        .and_then(|d| d.try_into().ok()),
                    workflow_run_timeout: options.run_timeout.and_then(|d| d.try_into().ok()),
                    workflow_task_timeout: options.task_timeout.and_then(|d| d.try_into().ok()),
                    search_attributes: options.search_attributes.map(|d| d.into()),
                    cron_schedule: options.cron_schedule.unwrap_or_default(),
                    header: options.header.or(start_signal.header),
                    user_metadata,
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner();
            res.run_id
        } else {
            // Normal start workflow
            let res = self
                .clone()
                .start_workflow_execution(
                    StartWorkflowExecutionRequest {
                        namespace: namespace.clone(),
                        input: payloads.into_payloads(),
                        workflow_id: workflow_id.clone(),
                        workflow_type: Some(WorkflowType {
                            name: workflow.name().to_string(),
                        }),
                        task_queue: Some(TaskQueue {
                            name: task_queue_name,
                            kind: TaskQueueKind::Unspecified as i32,
                            normal_name: "".to_string(),
                        }),
                        request_id: Uuid::new_v4().to_string(),
                        workflow_id_reuse_policy: options.id_reuse_policy as i32,
                        workflow_id_conflict_policy: options.id_conflict_policy as i32,
                        workflow_execution_timeout: options
                            .execution_timeout
                            .and_then(|d| d.try_into().ok()),
                        workflow_run_timeout: options.run_timeout.and_then(|d| d.try_into().ok()),
                        workflow_task_timeout: options.task_timeout.and_then(|d| d.try_into().ok()),
                        search_attributes: options.search_attributes.map(|d| d.into()),
                        cron_schedule: options.cron_schedule.unwrap_or_default(),
                        request_eager_execution: options.enable_eager_workflow_start,
                        retry_policy: options.retry_policy,
                        links: options.links,
                        completion_callbacks: options.completion_callbacks,
                        priority: Some(options.priority.into()),
                        header: options.header,
                        user_metadata,
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .map_err(|status| {
                    if status.code() == Code::AlreadyExists {
                        let run_id =
                            decode_status_detail::<WorkflowExecutionAlreadyStartedFailure>(
                                status.details(),
                            )
                            .map(|f| f.run_id);
                        WorkflowStartError::AlreadyStarted {
                            run_id,
                            source: status,
                        }
                    } else {
                        WorkflowStartError::Rpc(status)
                    }
                })?
                .into_inner();
            res.run_id
        };

        Ok(WorkflowHandle::new(
            self.clone(),
            WorkflowExecutionInfo {
                namespace,
                workflow_id,
                run_id: Some(run_id.clone()),
                first_execution_run_id: Some(run_id),
            },
        ))
    }

    fn get_workflow_handle<W: HasWorkflowDefinition>(
        &self,
        workflow_id: impl Into<String>,
    ) -> WorkflowHandle<Self, W>
    where
        Self: Sized,
    {
        WorkflowHandle::new(
            self.clone(),
            WorkflowExecutionInfo {
                namespace: self.namespace(),
                workflow_id: workflow_id.into(),
                run_id: None,
                first_execution_run_id: None,
            },
        )
    }

    fn list_workflows(
        &self,
        query: impl Into<String>,
        opts: WorkflowListOptions,
    ) -> ListWorkflowsStream {
        let client = self.clone();
        let namespace = self.namespace();
        let query = query.into();
        let limit = opts.limit;

        // State: (next_page_token, buffer, yielded_count, exhausted)
        let initial_state = (Vec::new(), VecDeque::new(), 0, false);

        let stream = stream::unfold(
            initial_state,
            move |(next_page_token, mut buffer, mut yielded, exhausted)| {
                let mut client = client.clone();
                let namespace = namespace.clone();
                let query = query.clone();

                async move {
                    if let Some(l) = limit
                        && yielded >= l
                    {
                        return None;
                    }

                    if let Some(exec) = buffer.pop_front() {
                        yielded += 1;
                        return Some((Ok(exec), (next_page_token, buffer, yielded, exhausted)));
                    }

                    if exhausted {
                        return None;
                    }

                    let response = WorkflowService::list_workflow_executions(
                        &mut client,
                        ListWorkflowExecutionsRequest {
                            namespace,
                            page_size: 0, // Use server default
                            next_page_token: next_page_token.clone(),
                            query,
                        }
                        .into_request(),
                    )
                    .await;

                    match response {
                        Ok(resp) => {
                            let resp = resp.into_inner();
                            let new_exhausted = resp.next_page_token.is_empty();
                            let new_token = resp.next_page_token;

                            buffer = resp
                                .executions
                                .into_iter()
                                .map(WorkflowExecution::from)
                                .collect();

                            if let Some(exec) = buffer.pop_front() {
                                yielded += 1;
                                Some((Ok(exec), (new_token, buffer, yielded, new_exhausted)))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some((Err(e.into()), (next_page_token, buffer, yielded, true))),
                    }
                }
            },
        );

        ListWorkflowsStream::new(Box::pin(stream))
    }

    async fn count_workflows(
        &self,
        query: impl Into<String>,
        _opts: WorkflowCountOptions,
    ) -> Result<WorkflowExecutionCount, ClientError> {
        let resp = WorkflowService::count_workflow_executions(
            &mut self.clone(),
            CountWorkflowExecutionsRequest {
                namespace: self.namespace(),
                query: query.into(),
            }
            .into_request(),
        )
        .await?
        .into_inner();

        Ok(WorkflowExecutionCount::from_response(resp))
    }

    fn get_async_activity_handle(&self, identifier: ActivityIdentifier) -> AsyncActivityHandle<Self>
    where
        Self: Sized,
    {
        AsyncActivityHandle::new(self.clone(), identifier)
    }
}

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      use tracing::error;
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
pub(crate) use dbg_panic;

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::Ascii;
    use url::Url;

    #[test]
    fn applies_headers() {
        // Initial header set
        let headers = Arc::new(RwLock::new(ClientHeaders {
            user_headers: HashMap::new(),
            user_binary_headers: HashMap::new(),
            api_key: Some("my-api-key".to_owned()),
        }));
        headers.clone().write().user_headers.insert(
            "my-meta-key".parse().unwrap(),
            "my-meta-val".parse().unwrap(),
        );
        headers.clone().write().user_binary_headers.insert(
            "my-bin-meta-key-bin".parse().unwrap(),
            vec![1, 2, 3].try_into().unwrap(),
        );
        let mut interceptor = ServiceCallInterceptor {
            client_name: "cute-kitty".to_string(),
            client_version: "0.1.0".to_string(),
            headers: headers.clone(),
        };

        // Confirm on metadata
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert_eq!(req.metadata().get("my-meta-key").unwrap(), "my-meta-val");
        assert_eq!(
            req.metadata().get("authorization").unwrap(),
            "Bearer my-api-key"
        );
        assert_eq!(
            req.metadata().get_bin("my-bin-meta-key-bin").unwrap(),
            vec![1, 2, 3].as_slice()
        );

        // Overwrite at request time
        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("my-meta-key", "my-meta-val2".parse().unwrap());
        req.metadata_mut()
            .insert("authorization", "my-api-key2".parse().unwrap());
        req.metadata_mut()
            .insert_bin("my-bin-meta-key-bin", vec![4, 5, 6].try_into().unwrap());
        let req = interceptor.call(req).unwrap();
        assert_eq!(req.metadata().get("my-meta-key").unwrap(), "my-meta-val2");
        assert_eq!(req.metadata().get("authorization").unwrap(), "my-api-key2");
        assert_eq!(
            req.metadata().get_bin("my-bin-meta-key-bin").unwrap(),
            vec![4, 5, 6].as_slice()
        );

        // Overwrite auth on header
        headers.clone().write().user_headers.insert(
            "authorization".parse().unwrap(),
            "my-api-key3".parse().unwrap(),
        );
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert_eq!(req.metadata().get("my-meta-key").unwrap(), "my-meta-val");
        assert_eq!(req.metadata().get("authorization").unwrap(), "my-api-key3");

        // Remove headers and auth and confirm gone
        headers.clone().write().user_headers.clear();
        headers.clone().write().user_binary_headers.clear();
        headers.clone().write().api_key.take();
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert!(!req.metadata().contains_key("my-meta-key"));
        assert!(!req.metadata().contains_key("authorization"));
        assert!(!req.metadata().contains_key("my-bin-meta-key-bin"));

        // Timeout header not overriden
        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("grpc-timeout", "1S".parse().unwrap());
        let req = interceptor.call(req).unwrap();
        assert_eq!(
            req.metadata().get("grpc-timeout").unwrap(),
            "1S".parse::<MetadataValue<Ascii>>().unwrap()
        );
    }

    #[test]
    fn invalid_ascii_header_key() {
        let invalid_headers = {
            let mut h = HashMap::new();
            h.insert("x-binary-key-bin".to_owned(), "value".to_owned());
            h
        };

        let result = parse_ascii_headers(invalid_headers);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid ASCII header key 'x-binary-key-bin': invalid gRPC metadata key name"
        );
    }

    #[test]
    fn invalid_ascii_header_value() {
        let invalid_headers = {
            let mut h = HashMap::new();
            // Nul bytes are valid UTF-8, but not valid ascii gRPC headers:
            h.insert("x-ascii-key".to_owned(), "\x00value".to_owned());
            h
        };

        let result = parse_ascii_headers(invalid_headers);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid ASCII header value for key 'x-ascii-key': failed to parse metadata value"
        );
    }

    #[test]
    fn invalid_binary_header_key() {
        let invalid_headers = {
            let mut h = HashMap::new();
            h.insert("x-ascii-key".to_owned(), vec![1, 2, 3]);
            h
        };

        let result = parse_binary_headers(invalid_headers);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid binary header key 'x-ascii-key': invalid gRPC metadata key name"
        );
    }

    #[test]
    fn keep_alive_defaults() {
        let opts = ConnectionOptions::new(Url::parse("https://smolkitty").unwrap())
            .identity("enchicat".to_string())
            .client_name("cute-kitty".to_string())
            .client_version("0.1.0".to_string())
            .build();
        assert_eq!(
            opts.keep_alive.clone().unwrap().interval,
            ClientKeepAliveOptions::default().interval
        );
        assert_eq!(
            opts.keep_alive.clone().unwrap().timeout,
            ClientKeepAliveOptions::default().timeout
        );

        // Can be explicitly set to None
        let opts = ConnectionOptions::new(Url::parse("https://smolkitty").unwrap())
            .identity("enchicat".to_string())
            .client_name("cute-kitty".to_string())
            .client_version("0.1.0".to_string())
            .keep_alive(None)
            .build();
        dbg!(&opts.keep_alive);
        assert!(opts.keep_alive.is_none());
    }

    mod list_workflows_tests {
        use super::*;
        use futures_util::{FutureExt, StreamExt};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use temporalio_common::protos::temporal::api::common::v1::WorkflowExecution as ProtoWorkflowExecution;
        use tonic::{Request, Response};

        #[derive(Clone)]
        struct MockListWorkflowsClient {
            call_count: Arc<AtomicUsize>,
            // Returns this many workflows per page
            page_size: usize,
            // Total workflows available
            total_workflows: usize,
        }

        impl NamespacedClient for MockListWorkflowsClient {
            fn namespace(&self) -> String {
                "test-namespace".to_string()
            }
            fn identity(&self) -> String {
                "test-identity".to_string()
            }
        }

        impl WorkflowService for MockListWorkflowsClient {
            fn list_workflow_executions(
                &mut self,
                request: Request<ListWorkflowExecutionsRequest>,
            ) -> futures_util::future::BoxFuture<
                '_,
                Result<Response<ListWorkflowExecutionsResponse>, tonic::Status>,
            > {
                self.call_count.fetch_add(1, Ordering::SeqCst);
                let req = request.into_inner();

                // Determine offset from page token
                let offset: usize = if req.next_page_token.is_empty() {
                    0
                } else {
                    String::from_utf8(req.next_page_token)
                        .unwrap()
                        .parse()
                        .unwrap()
                };

                let remaining = self.total_workflows.saturating_sub(offset);
                let count = remaining.min(self.page_size);
                let new_offset = offset + count;

                let executions: Vec<_> = (offset..offset + count)
                    .map(|i| workflow::WorkflowExecutionInfo {
                        execution: Some(ProtoWorkflowExecution {
                            workflow_id: format!("wf-{i}"),
                            run_id: format!("run-{i}"),
                        }),
                        r#type: Some(WorkflowType {
                            name: "TestWorkflow".to_string(),
                        }),
                        task_queue: "test-queue".to_string(),
                        ..Default::default()
                    })
                    .collect();

                let next_page_token = if new_offset < self.total_workflows {
                    new_offset.to_string().into_bytes()
                } else {
                    vec![]
                };

                async move {
                    Ok(Response::new(ListWorkflowExecutionsResponse {
                        executions,
                        next_page_token,
                    }))
                }
                .boxed()
            }
        }

        #[tokio::test]
        async fn list_workflows_paginates_through_all_results() {
            let call_count = Arc::new(AtomicUsize::new(0));
            let client = MockListWorkflowsClient {
                call_count: call_count.clone(),
                page_size: 3,
                total_workflows: 10,
            };

            let stream = client.list_workflows("", WorkflowListOptions::default());
            let results: Vec<_> = stream.collect().await;

            assert_eq!(results.len(), 10);
            for (i, result) in results.iter().enumerate() {
                let wf = result.as_ref().unwrap();
                assert_eq!(wf.id(), format!("wf-{i}"));
                assert_eq!(wf.run_id(), format!("run-{i}"));
            }
            // Should have made 4 calls: pages of 3, 3, 3, 1
            assert_eq!(call_count.load(Ordering::SeqCst), 4);
        }

        #[tokio::test]
        async fn list_workflows_respects_limit() {
            let call_count = Arc::new(AtomicUsize::new(0));
            let client = MockListWorkflowsClient {
                call_count: call_count.clone(),
                page_size: 3,
                total_workflows: 10,
            };

            let opts = WorkflowListOptions::builder().limit(5).build();
            let stream = client.list_workflows("", opts);
            let results: Vec<_> = stream.collect().await;

            assert_eq!(results.len(), 5);
            for (i, result) in results.iter().enumerate() {
                let wf = result.as_ref().unwrap();
                assert_eq!(wf.id(), format!("wf-{i}"));
            }
            // Should have made 2 calls: 1 page of 3, then 2 more from next page
            assert_eq!(call_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn list_workflows_limit_less_than_page_size() {
            let call_count = Arc::new(AtomicUsize::new(0));
            let client = MockListWorkflowsClient {
                call_count: call_count.clone(),
                page_size: 10,
                total_workflows: 100,
            };

            let opts = WorkflowListOptions::builder().limit(3).build();
            let stream = client.list_workflows("", opts);
            let results: Vec<_> = stream.collect().await;

            assert_eq!(results.len(), 3);
            // Only 1 call needed since limit < page_size
            assert_eq!(call_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn list_workflows_empty_results() {
            let call_count = Arc::new(AtomicUsize::new(0));
            let client = MockListWorkflowsClient {
                call_count: call_count.clone(),
                page_size: 10,
                total_workflows: 0,
            };

            let stream = client.list_workflows("", WorkflowListOptions::default());
            let results: Vec<_> = stream.collect().await;

            assert_eq!(results.len(), 0);
            assert_eq!(call_count.load(Ordering::SeqCst), 1);
        }
    }
}
