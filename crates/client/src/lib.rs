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
/// Interceptors for high-level client operations.
pub mod interceptors;
mod metrics;
mod options_structs;
/// Visible only for tests
#[doc(hidden)]
pub mod proxy;
mod replaceable;
pub mod request_extensions;
mod retry;
mod rpc_options;
/// Schedule operations: create, describe, update, pause, trigger, backfill, list, and delete.
pub mod schedules;
#[cfg(test)]
mod test_helpers;
pub mod worker;
mod workflow_handle;
mod workflow_status;

pub use crate::{
    proxy::HttpConnectProxyOptions,
    request_extensions::PayloadErrorLimits,
    retry::{CallType, RETRYABLE_ERROR_CODES},
};
pub use async_activity_handle::{
    ActivityHeartbeatResponse, ActivityIdentifier, AsyncActivityHandle,
};
#[doc(hidden)]
pub use retry::jittered;

pub use interceptors::{
    BackfillScheduleInput, CancelWorkflowInput, ClientInterceptor, CompleteAsyncActivityInput,
    CountWorkflowsInput, CountWorkflowsOutput, CreateScheduleInput, CreateScheduleOutput,
    DeleteScheduleInput, DescribeScheduleInput, DescribeScheduleOutput, DescribeWorkflowInput,
    DescribeWorkflowOutput, FailAsyncActivityInput, FetchWorkflowHistoryPageInput,
    FetchWorkflowHistoryPageOutput, HasArgs, HeartbeatAsyncActivityInput, ListSchedulesPageInput,
    ListSchedulesPageOutput, ListWorkflowsPageInput, ListWorkflowsPageOutput, Next,
    PauseScheduleInput, PollWorkflowUpdateInput, PollWorkflowUpdateOutput, QueryWorkflowInput,
    QueryWorkflowOutput, ReportAsyncActivityCancellationInput, SendScheduleUpdateInput,
    SignalWorkflowInput, StartWorkflowInput, StartWorkflowOutput, StartWorkflowUpdateInput,
    StartWorkflowUpdateOutput, TemporalClientValue, TerminateWorkflowInput, TriggerScheduleInput,
    UnpauseScheduleInput, UpdateScheduleInput,
};
pub use metrics::{LONG_REQUEST_LATENCY_HISTOGRAM_NAME, REQUEST_LATENCY_HISTOGRAM_NAME};
pub use options_structs::*;
pub use replaceable::SharedReplaceableClient;
pub use retry::RetryOptions;
pub use rpc_options::{RpcMetadata, RpcMetadataError, RpcOptions};
pub use temporalio_common::{Memo, RetryPolicy};
pub use url::Url;
/// Potentially dangerous TLS related functionality.
pub mod danger {
    /// Re-export the `ServerCertVerifier` trait so that users can implement custom TLS
    /// server certificate verification without depending on `tokio-rustls` directly,
    /// while explicitly acknowledging the danger in the import path.
    pub use tokio_rustls::rustls::client::danger::ServerCertVerifier;
}
pub use tonic;
pub use workflow_handle::{
    UntypedQuery, UntypedSignal, UntypedUpdate, UntypedWorkflow, UntypedWorkflowHandle,
    WorkflowExecutionDescription, WorkflowExecutionInfo, WorkflowExecutionResult, WorkflowHandle,
    WorkflowHistory, WorkflowResultDetails, WorkflowUpdateHandle,
};
pub use workflow_status::WorkflowExecutionStatus;

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
use futures_util::{future::BoxFuture, stream, stream::Stream};
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
    payload_visitor::decode_payloads,
    protos::{
        coresdk::IntoPayloadsExt,
        grpc::health::v1::health_client::HealthClient,
        proto_ts_to_system_time,
        temporal::api::{
            cloud::cloudservice::v1::cloud_service_client::CloudServiceClient,
            common::v1::WorkflowType,
            enums::v1::TaskQueueKind,
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
    search_attributes::{SearchAttributeError, SearchAttributeValue, SearchAttributes},
};
use tonic::{
    Code, IntoRequest,
    body::Body,
    client::GrpcService,
    codec::CompressionEncoding,
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
/// Returns the violation, if `status` is the client proactively rejecting an outbound request for exceeding a
/// payload/memo error size limit.
pub fn payload_limit_violation_from(
    status: &tonic::Status,
) -> Option<&temporalio_common::payload_limits::PayloadLimitViolation> {
    std::error::Error::source(status).and_then(|src| src.downcast_ref())
}
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
    /// Configured payload/memo size warning thresholds (bytes); `0` disables that warning.
    payloads_warn_size: usize,
    memo_warn_size: usize,
}

/// Resolve a user-configured warning threshold (bytes) into the internal representation. `0`
/// disables the warning (`None`); so does a value that doesn't fit in `usize` on this platform (a
/// threshold larger than any addressable payload could never fire anyway), with a warning logged.
/// `option` names the configured field, for diagnostics.
fn resolve_warn_threshold(option: &'static str, bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or_else(|_| {
        warn!(
            option,
            configured_bytes = bytes,
            "Configured payload size warning threshold exceeds the maximum addressable size on this \
             platform; disabling this warning"
        );
        0
    })
}

impl Connection {
    /// Connect to a Temporal service.
    pub async fn connect(mut options: ConnectionOptions) -> Result<Self, ClientConnectError> {
        if options.service_override.is_some() {
            options.grpc_compression = GrpcCompression::None;
        }

        let first_result = Self::connect_once(&options).await;
        if options.grpc_compression == GrpcCompression::Gzip
            && let Err(ClientConnectError::SystemInfoCallError(status)) = &first_result
            && status.code() == Code::Unimplemented
            && {
                let msg = status.message().to_lowercase();
                msg.contains("decompress")
                    || msg.contains("grpc-encoding")
                    || msg.contains("compressor")
            }
        {
            options.grpc_compression = GrpcCompression::None;
            return Self::connect_once(&options).await;
        }
        first_result
    }

    async fn connect_once(options: &ConnectionOptions) -> Result<Self, ClientConnectError> {
        let dns_lb_opts = dns::validate_and_get_dns_lb(options)?.cloned();
        let (service, dns_task) = if let Some(service_override) = options.service_override.clone() {
            (
                GrpcMetricSvc {
                    inner: ChannelOrGrpcOverride::GrpcOverride(service_override),
                    metrics: options.metrics_meter.clone().map(MetricsContext::new),
                    disable_errcode_label: options.disable_error_code_metric_tags,
                },
                None,
            )
        } else if let Some(dns_opts) = &dns_lb_opts {
            let (channel, sender) = dns::create_balanced_channel(options).await?;
            let handle = dns::spawn_dns_reresolution(
                sender,
                options.target.clone(),
                options.tls_options.clone(),
                options.keep_alive.clone(),
                options.override_origin.clone(),
                dns_opts.resolution_interval,
                options.connect_timeout,
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
            let channel = if let Some(timeout) = options.connect_timeout {
                channel.connect_timeout(timeout)
            } else {
                channel
            };
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
        let mut svc_client = TemporalServiceClient::new(svc, options.grpc_compression);

        let capabilities = if !options.skip_get_system_info {
            match svc_client
                .get_system_info(GetSystemInfoRequest::default().into_request())
                .await
            {
                Ok(sysinfo) => sysinfo.into_inner().capabilities,
                Err(status) => match status.code() {
                    Code::Unimplemented
                        if {
                            let msg = status.message().to_lowercase();
                            msg.contains("unknown method")
                                || msg.contains("unknown service")
                                || msg.contains("method not found")
                                || (msg.contains("getsysteminfo")
                                    && (msg.contains("is unimplemented")
                                        || msg.contains("not implement")))
                        } =>
                    {
                        None
                    }
                    _ => return Err(ClientConnectError::SystemInfoCallError(status)),
                },
            }
        } else {
            None
        };
        Ok(Self {
            inner: Arc::new(ConnectionInner {
                service: svc_client,
                retry_options: options.retry_options.clone(),
                identity: options.identity.clone(),
                headers,
                client_name: options.client_name.clone(),
                client_version: options.client_version.clone(),
                capabilities,
                workers: Arc::new(ClientWorkerSet::new()),
                _dns_task: dns_task,
                payloads_warn_size: resolve_warn_threshold(
                    "payloads_warn_size",
                    options.payload_limits.payloads_warn_size,
                ),
                memo_warn_size: resolve_warn_threshold(
                    "memo_warn_size",
                    options.payload_limits.memo_warn_size,
                ),
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
        if tls_cfg.server_cert_verifier.is_some() && tls_cfg.server_root_ca_cert.is_some() {
            return Err(ClientConnectError::InvalidConfig(
                "Cannot set both `server_root_ca_cert` and `server_cert_verifier`".to_owned(),
            ));
        }

        let mut tls = tonic::transport::ClientTlsConfig::new();

        if tls_cfg.server_cert_verifier.is_none() {
            if let Some(root_cert) = &tls_cfg.server_root_ca_cert {
                let server_root_ca_cert = Certificate::from_pem(root_cert);
                tls = tls.ca_certificate(server_root_ca_cert);
            } else {
                tls = tls.with_native_roots();
            }
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

        return if let Some(verifier) = &tls_cfg.server_cert_verifier {
            channel
                .tls_config_with_verifier(tls, verifier.clone())
                .map_err(Into::into)
        } else {
            channel.tls_config(tls).map_err(Into::into)
        };
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
    fn new<T>(svc: T, compression: GrpcCompression) -> Self
    where
        T: GrpcService<Body> + Send + Sync + Clone + 'static,
        T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
        T::Error: Into<tonic::codegen::StdError>,
        <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
        <T as GrpcService<Body>>::Future: Send,
    {
        // The generated service clients don't share a trait exposing the compression setters, so
        // a macro applies the same configuration to each concrete client type.
        macro_rules! configure {
            ($client:expr) => {{
                let client = $client.max_decoding_message_size(get_decode_max_size());
                match compression {
                    GrpcCompression::Gzip => client
                        .send_compressed(CompressionEncoding::Gzip)
                        .accept_compressed(CompressionEncoding::Gzip),
                    GrpcCompression::None => client,
                }
            }};
        }

        let workflow_svc_client = Box::new(configure!(WorkflowServiceClient::new(svc.clone())));
        let operator_svc_client = Box::new(configure!(OperatorServiceClient::new(svc.clone())));
        let cloud_svc_client = Box::new(configure!(CloudServiceClient::new(svc.clone())));
        let test_svc_client = Box::new(configure!(TestServiceClient::new(svc.clone())));
        let health_svc_client = Box::new(configure!(HealthClient::new(svc.clone())));

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

    fn client_interceptors(&self) -> &[Arc<dyn ClientInterceptor>] {
        &self.options.client_interceptors
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
    /// Returns the interceptors used for high-level client operations.
    ///
    /// # Warning
    ///
    /// This provider exists so SDK-owned client handles can carry interceptor configuration
    /// through the high-level client blanket implementation. Custom client implementations should
    /// normally retain the default empty chain unless they deliberately provide the same plumbing.
    fn client_interceptors(&self) -> &[Arc<dyn ClientInterceptor>] {
        &[]
    }
}

/// A workflow execution returned from list operations.
/// This represents information about a workflow present in visibility.
#[derive(Debug, Clone)]
pub struct WorkflowExecution {
    raw: workflow::WorkflowExecutionInfo,
    data_converter: DataConverter,
}

impl WorkflowExecution {
    fn new_with_data_converter(
        raw: workflow::WorkflowExecutionInfo,
        data_converter: DataConverter,
    ) -> Self {
        Self {
            raw,
            data_converter,
        }
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
        WorkflowExecutionStatus::from_raw(self.raw.status)
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

    /// Workflow memo decoded with the client's payload converter.
    pub fn memo(&self) -> Memo {
        Memo::from_raw(
            self.raw.memo.clone(),
            self.data_converter.payload_converter().clone(),
            SerializationContextData::Workflow,
        )
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
    pub fn search_attributes(&self) -> SearchAttributes {
        self.raw
            .search_attributes
            .as_ref()
            .map(SearchAttributes::from_proto)
            .unwrap_or_default()
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
    raw: count_workflow_executions_response::AggregationGroup,
}

impl WorkflowCountAggregationGroup {
    fn from_proto(proto: count_workflow_executions_response::AggregationGroup) -> Self {
        Self { raw: proto }
    }

    /// Retrieve a typed group value at `index`.
    ///
    ///  Returns `None` if the index is out of bounds or deserialization fails.
    ///  Use [`Self::try_get`] for explicit error handling.
    pub fn get<T: SearchAttributeValue>(&self, index: usize) -> Option<T> {
        self.try_get(index).ok().flatten()
    }

    /// Retrieve a typed group value at `index`, preserving deserialization
    /// errors.
    ///
    /// Returns `Ok(None)` if the index is out of bounds and `Err` if the
    /// payload cannot be deserialized.
    pub fn try_get<T: SearchAttributeValue>(
        &self,
        index: usize,
    ) -> Result<Option<T>, SearchAttributeError> {
        match self.raw.group_values.get(index) {
            Some(payload) => T::from_search_attribute_payload(payload).map(Some),
            None => Ok(None),
        }
    }

    /// The approximate number of workflows matching for this group.
    pub fn count(&self) -> usize {
        self.raw.count as usize
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
        let namespace = self.namespace();
        let interceptor_output = interceptors::call_start_workflow(
            self.client_interceptors(),
            StartWorkflowInput::new(workflow.name().to_owned(), input, options),
            Next::new({
                let client = (*self).clone();
                move |input: StartWorkflowInput| -> BoxFuture<
                    '_,
                    Result<StartWorkflowOutput, WorkflowStartError>,
                > {
                    let mut client = client;
                    Box::pin(async move {
                        let (workflow_type, args, options, rpc_options) = input.into_parts();
                        let data_converter = client.data_converter().clone();
                        let unencoded_payloads = {
                            let payload_converter = data_converter.payload_converter();
                            let context = SerializationContext {
                                data: &SerializationContextData::Workflow,
                                converter: payload_converter,
                            };
                            args.serialize_payloads(&context)
                        };
                        drop(args);

                        let payloads = data_converter
                            .codec()
                            .encode(&SerializationContextData::Workflow, unencoded_payloads?)
                            .await?;
                        let namespace = client.namespace();
                        let workflow_id = options.workflow_id.clone();
                        let task_queue_name = options.task_queue.clone();

                        let user_metadata = if options.static_summary.is_some()
                            || options.static_details.is_some()
                        {
                            let payload_converter = PayloadConverter::default();
                            let context = SerializationContext {
                                data: &SerializationContextData::Workflow,
                                converter: &payload_converter,
                            };
                            Some(UserMetadata {
                                summary: options.static_summary.map(|summary| {
                                    payload_converter.to_payload(&context, &summary).expect(
                                        "String-to-JSON payload serialization is infallible",
                                    )
                                }),
                                details: options.static_details.map(|details| {
                                    payload_converter.to_payload(&context, &details).expect(
                                        "String-to-JSON payload serialization is infallible",
                                    )
                                }),
                            })
                        } else {
                            None
                        };

                        let run_id = if let Some(start_signal) = options.start_signal {
                            let mut request = SignalWithStartWorkflowExecutionRequest {
                                namespace,
                                workflow_id: workflow_id.clone(),
                                workflow_type: Some(WorkflowType {
                                    name: workflow_type,
                                }),
                                task_queue: Some(TaskQueue {
                                    name: task_queue_name,
                                    kind: TaskQueueKind::Normal as i32,
                                    normal_name: String::new(),
                                }),
                                input: payloads.into_payloads(),
                                signal_name: start_signal.signal_name,
                                signal_input: start_signal.input,
                                identity: client.identity(),
                                request_id: Uuid::new_v4().to_string(),
                                workflow_id_reuse_policy: options.id_reuse_policy as i32,
                                workflow_id_conflict_policy: options.id_conflict_policy as i32,
                                workflow_execution_timeout: options
                                    .execution_timeout
                                    .and_then(|duration| duration.try_into().ok()),
                                workflow_run_timeout: options
                                    .run_timeout
                                    .and_then(|duration| duration.try_into().ok()),
                                workflow_task_timeout: options
                                    .task_timeout
                                    .and_then(|duration| duration.try_into().ok()),
                                search_attributes: options
                                    .search_attributes
                                    .map(|attributes| attributes.into_proto()),
                                cron_schedule: options.cron_schedule.unwrap_or_default(),
                                retry_policy: options.retry_policy.map(Into::into),
                                header: options.header.or(start_signal.header),
                                user_metadata,
                                ..Default::default()
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            WorkflowService::signal_with_start_workflow_execution(
                                &mut client,
                                request,
                            )
                            .await?
                            .into_inner()
                            .run_id
                        } else {
                            let mut request = StartWorkflowExecutionRequest {
                                namespace,
                                input: payloads.into_payloads(),
                                workflow_id: workflow_id.clone(),
                                workflow_type: Some(WorkflowType {
                                    name: workflow_type,
                                }),
                                task_queue: Some(TaskQueue {
                                    name: task_queue_name,
                                    kind: TaskQueueKind::Unspecified as i32,
                                    normal_name: String::new(),
                                }),
                                request_id: Uuid::new_v4().to_string(),
                                workflow_id_reuse_policy: options.id_reuse_policy as i32,
                                workflow_id_conflict_policy: options.id_conflict_policy as i32,
                                workflow_execution_timeout: options
                                    .execution_timeout
                                    .and_then(|duration| duration.try_into().ok()),
                                workflow_run_timeout: options
                                    .run_timeout
                                    .and_then(|duration| duration.try_into().ok()),
                                workflow_task_timeout: options
                                    .task_timeout
                                    .and_then(|duration| duration.try_into().ok()),
                                search_attributes: options
                                    .search_attributes
                                    .map(|attributes| attributes.into_proto()),
                                cron_schedule: options.cron_schedule.unwrap_or_default(),
                                request_eager_execution: options.enable_eager_workflow_start,
                                retry_policy: options.retry_policy.map(Into::into),
                                links: options.links,
                                completion_callbacks: options.completion_callbacks,
                                priority: Some(options.priority.into()),
                                header: options.header,
                                user_metadata,
                                ..Default::default()
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            client
                                .start_workflow_execution(request)
                                .await
                                .map_err(|status| {
                                    if status.code() == Code::AlreadyExists {
                                        let run_id = decode_status_detail::<
                                            WorkflowExecutionAlreadyStartedFailure,
                                        >(
                                            status.details()
                                        )
                                        .map(|failure| failure.run_id);
                                        WorkflowStartError::AlreadyStarted {
                                            run_id,
                                            source: status,
                                        }
                                    } else {
                                        WorkflowStartError::Rpc(status)
                                    }
                                })?
                                .into_inner()
                                .run_id
                        };

                        Ok(StartWorkflowOutput::new(workflow_id, run_id))
                    })
                }
            }),
        )
        .await?;
        let StartWorkflowOutput {
            workflow_id,
            run_id,
        } = interceptor_output;

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
        let rpc_options = opts.rpc_options;

        // State: (next_page_token, buffer, yielded_count, exhausted)
        let initial_state = (Vec::new(), VecDeque::new(), 0, false);

        let stream = stream::unfold(
            initial_state,
            move |(next_page_token, mut buffer, mut yielded, exhausted)| {
                let client = client.clone();
                let namespace = namespace.clone();
                let query = query.clone();
                let rpc_options = rpc_options.clone();

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

                    let response = interceptors::call_list_workflows_page(
                        client.client_interceptors(),
                        ListWorkflowsPageInput {
                            query,
                            next_page_token: next_page_token.clone(),
                            rpc_options,
                        },
                        Next::new({
                            let mut rpc_client = client.clone();
                            move |input: ListWorkflowsPageInput| -> BoxFuture<
                                '_,
                                Result<ListWorkflowsPageOutput, ClientError>,
                            > {
                                Box::pin(async move {
                                    let mut request = ListWorkflowExecutionsRequest {
                                        namespace,
                                        page_size: 0,
                                        next_page_token: input.next_page_token,
                                        query: input.query,
                                    }
                                    .into_request();
                                    input.rpc_options.apply_to(&mut request);
                                    let response = WorkflowService::list_workflow_executions(
                                        &mut rpc_client,
                                        request,
                                    )
                                    .await?
                                    .into_inner();
                                    Ok(ListWorkflowsPageOutput::new(
                                        response.executions,
                                        response.next_page_token,
                                    ))
                                })
                            }
                        }),
                    )
                    .await;

                    match response {
                        Ok(mut output) => {
                            let new_exhausted = output.next_page_token.is_empty();
                            let new_token = output.next_page_token;

                            let data_converter = client.data_converter().clone();
                            for execution in &mut output.executions {
                                if let Some(memo) = execution.memo.as_mut()
                                    && let Err(err) = decode_payloads(
                                        memo,
                                        data_converter.codec(),
                                        &SerializationContextData::Workflow,
                                    )
                                    .await
                                {
                                    return Some((
                                        Err(ClientError::from(err)),
                                        (new_token, buffer, yielded, true),
                                    ));
                                }
                            }
                            buffer = output
                                .executions
                                .into_iter()
                                .map(|raw| {
                                    WorkflowExecution::new_with_data_converter(
                                        raw,
                                        data_converter.clone(),
                                    )
                                })
                                .collect();

                            if let Some(exec) = buffer.pop_front() {
                                yielded += 1;
                                Some((Ok(exec), (new_token, buffer, yielded, new_exhausted)))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some((Err(e), (next_page_token, buffer, yielded, true))),
                    }
                }
            },
        );

        ListWorkflowsStream::new(Box::pin(stream))
    }

    async fn count_workflows(
        &self,
        query: impl Into<String>,
        opts: WorkflowCountOptions,
    ) -> Result<WorkflowExecutionCount, ClientError> {
        let output = interceptors::call_count_workflows(
            self.client_interceptors(),
            CountWorkflowsInput {
                query: query.into(),
                options: opts,
            },
            Next::new({
                let mut client = (*self).clone();
                move |input: CountWorkflowsInput| -> BoxFuture<
                    '_,
                    Result<CountWorkflowsOutput, ClientError>,
                > {
                    Box::pin(async move {
                        let mut request = CountWorkflowExecutionsRequest {
                            namespace: client.namespace(),
                            query: input.query,
                        }
                        .into_request();
                        input.options.rpc_options.apply_to(&mut request);
                        let response = WorkflowService::count_workflow_executions(
                            &mut client,
                            request,
                        )
                        .await?
                        .into_inner();
                        Ok(CountWorkflowsOutput::new(response))
                    })
                }
            }),
        )
        .await?;

        Ok(WorkflowExecutionCount::from_response(output.response))
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
    use crate::callback_based::CallbackBasedGrpcService;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use temporalio_common::search_attributes::SearchAttributeKey;
    use tonic::{Status, metadata::Ascii};
    use url::Url;

    #[test]
    fn count_aggregation_group_gets_typed_value() {
        let attrs = SearchAttributes::new([SearchAttributeKey::int("group").value_set(42)]);
        let group = WorkflowCountAggregationGroup {
            raw: count_workflow_executions_response::AggregationGroup {
                group_values: vec![attrs.raw_payload("group").unwrap().clone()],
                count: 1,
            },
        };

        assert_eq!(group.get::<i64>(0), Some(42));
        assert_eq!(group.get::<i64>(1), None);
        assert!(group.try_get::<String>(0).is_err());
        assert_eq!(group.try_get::<i64>(1).unwrap(), None);
    }

    fn connection_options_for_system_info_test(
        service_override: CallbackBasedGrpcService,
    ) -> ConnectionOptions {
        ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
            .service_override(service_override)
            .dns_load_balancing(None)
            .build()
    }

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

    #[rstest::rstest]
    #[case(
        "unknown method GetSystemInfo for service temporal.api.workflowservice.v1.WorkflowService"
    )]
    #[case("Method temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo is unimplemented")]
    #[case(
        "The server does not implement the method /temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
    )]
    #[tokio::test]
    async fn get_system_info_missing_method_falls_back_to_empty_capabilities(
        #[case] message: &'static str,
    ) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let service_override = CallbackBasedGrpcService {
            callback: Arc::new(move |req| {
                let attempts = attempts_clone.clone();
                Box::pin(async move {
                    assert_eq!(req.rpc, "GetSystemInfo");
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(Status::unimplemented(message))
                })
            }),
        };

        let connection =
            Connection::connect(connection_options_for_system_info_test(service_override))
                .await
                .unwrap();

        assert!(connection.capabilities().is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn get_system_info_non_missing_unimplemented_fails_connect() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let service_override = CallbackBasedGrpcService {
            callback: Arc::new(move |req| {
                let attempts = attempts_clone.clone();
                Box::pin(async move {
                    assert_eq!(req.rpc, "GetSystemInfo");
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(Status::unimplemented("backend temporarily unimplemented"))
                })
            }),
        };

        let err =
            match Connection::connect(connection_options_for_system_info_test(service_override))
                .await
            {
                Ok(_) => panic!("connection should fail"),
                Err(err) => err,
            };

        assert!(matches!(
            err,
            ClientConnectError::SystemInfoCallError(status)
                if status.code() == Code::Unimplemented
                    && status.message() == "backend temporarily unimplemented"
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn connect_timeout_bounds_connection_attempt() {
        let url = Url::parse("http://10.255.255.1:7233").unwrap();
        let opts = ConnectionOptions::new(url)
            .connect_timeout(Duration::from_millis(500))
            .build();
        let start = Instant::now();
        let result = Connection::connect(opts).await;
        assert!(result.is_err(), "connection should fail");
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    mod tls_custom_verifier_tests {
        use super::*;
        use tokio_rustls::rustls::{
            DigitallySignedStruct, Error as RustlsError, SignatureScheme,
            client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
            pki_types::{CertificateDer, ServerName, UnixTime},
        };

        /// A minimal mock verifier for testing. In production, users would
        /// implement real certificate pinning or custom validation here.
        #[derive(Debug)]
        struct MockVerifier;

        impl ServerCertVerifier for MockVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &CertificateDer<'_>,
                _intermediates: &[CertificateDer<'_>],
                _server_name: &ServerName<'_>,
                _ocsp_response: &[u8],
                _now: UnixTime,
            ) -> Result<ServerCertVerified, RustlsError> {
                Ok(ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &CertificateDer<'_>,
                _dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, RustlsError> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &CertificateDer<'_>,
                _dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, RustlsError> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
                vec![
                    SignatureScheme::ECDSA_NISTP256_SHA256,
                    SignatureScheme::RSA_PSS_SHA256,
                ]
            }
        }

        #[tokio::test]
        async fn add_tls_to_channel_with_custom_verifier() {
            let tls_opts = TlsOptions::builder()
                .server_cert_verifier(Arc::new(MockVerifier))
                .domain("test.temporal.io".to_string())
                .build();
            let endpoint = tonic::transport::Channel::from_static("https://test.temporal.io:7233");
            let result = add_tls_to_channel(Some(&tls_opts), endpoint).await;
            assert!(
                result.is_ok(),
                "add_tls_to_channel should succeed with a custom verifier: {:?}",
                result.err()
            );
        }

        #[tokio::test]
        async fn add_tls_to_channel_with_verifier_and_ca_cert_fails() {
            // When both server_cert_verifier and server_root_ca_cert are set,
            // add_tls_to_channel should fail with InvalidConfig.
            let tls_opts = TlsOptions::builder()
                .server_root_ca_cert(b"some-ca-cert-bytes".to_vec())
                .server_cert_verifier(Arc::new(MockVerifier))
                .domain("test.temporal.io".to_string())
                .build();
            let endpoint = tonic::transport::Channel::from_static("https://test.temporal.io:7233");
            let result = add_tls_to_channel(Some(&tls_opts), endpoint).await;
            assert!(
                matches!(result, Err(ClientConnectError::InvalidConfig(_))),
                "add_tls_to_channel should fail with InvalidConfig when both CA cert and verifier are set: {:?}",
                result
            );
        }

        #[tokio::test]
        async fn add_tls_to_channel_without_verifier_still_works() {
            // Regression test: the original PEM path must still work.
            let tls_opts = TlsOptions::builder()
                .domain("test.temporal.io".to_string())
                .build();
            let endpoint = tonic::transport::Channel::from_static("https://test.temporal.io:7233");
            let result = add_tls_to_channel(Some(&tls_opts), endpoint).await;
            assert!(
                result.is_ok(),
                "add_tls_to_channel should succeed without a verifier (native roots): {:?}",
                result.err()
            );
        }
    }

    mod start_workflow_interceptor_tests {
        use super::*;
        use crate::request_extensions::RetryConfigForCall;
        use parking_lot::Mutex;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use temporalio_common::{
            HasWorkflowDefinition, WorkflowDefinition,
            data_converters::{
                DefaultFailureConverter, PayloadCodec, PayloadConversionError,
                SerializationContext, SerializationContextData, TemporalSerializable,
            },
            protos::temporal::api::common::v1::Payload,
        };
        use tonic::{Request, Response};

        struct TestWorkflow;

        impl WorkflowDefinition for TestWorkflow {
            type Input = Vec<String>;
            type Output = ();

            fn name(&self) -> &str {
                "test-workflow"
            }
        }

        impl HasWorkflowDefinition for TestWorkflow {
            type Run = Self;
        }

        #[derive(Default)]
        struct RecordedStart {
            calls: usize,
            workflow_type: String,
            payloads: Vec<Payload>,
            ascii_metadata: Option<String>,
            binary_metadata: Option<Vec<u8>>,
            grpc_timeout: Option<String>,
            retry_options: Option<RetryOptions>,
        }

        struct CountingCodec {
            encode_calls: Arc<AtomicUsize>,
        }

        impl PayloadCodec for CountingCodec {
            fn encode(
                &self,
                _context: &SerializationContextData,
                payloads: Vec<Payload>,
            ) -> futures_util::future::BoxFuture<
                'static,
                Result<Vec<Payload>, PayloadConversionError>,
            > {
                self.encode_calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(payloads) })
            }

            fn decode(
                &self,
                _context: &SerializationContextData,
                payloads: Vec<Payload>,
            ) -> futures_util::future::BoxFuture<
                'static,
                Result<Vec<Payload>, PayloadConversionError>,
            > {
                Box::pin(async move { Ok(payloads) })
            }
        }

        #[derive(Clone)]
        struct MockStartWorkflowClient {
            recorded: Arc<Mutex<RecordedStart>>,
            data_converter: DataConverter,
        }

        impl NamespacedClient for MockStartWorkflowClient {
            fn namespace(&self) -> String {
                "test-namespace".to_owned()
            }

            fn identity(&self) -> String {
                "test-identity".to_owned()
            }

            fn data_converter(&self) -> &DataConverter {
                &self.data_converter
            }
        }

        impl WorkflowService for MockStartWorkflowClient {
            fn start_workflow_execution(
                &mut self,
                request: Request<StartWorkflowExecutionRequest>,
            ) -> futures_util::future::BoxFuture<
                '_,
                Result<Response<StartWorkflowExecutionResponse>, tonic::Status>,
            > {
                let ascii_metadata = request
                    .metadata()
                    .get("call-meta")
                    .map(|value| value.to_str().unwrap().to_owned());
                let binary_metadata = request
                    .metadata()
                    .get_bin("call-meta-bin")
                    .map(|value| value.to_bytes().unwrap().to_vec());
                let grpc_timeout = request
                    .metadata()
                    .get("grpc-timeout")
                    .map(|value| value.to_str().unwrap().to_owned());
                let retry_options = request
                    .extensions()
                    .get::<RetryConfigForCall>()
                    .map(|config| config.0.clone());
                let request = request.into_inner();
                let mut recorded = self.recorded.lock();
                recorded.calls += 1;
                recorded.workflow_type = request.workflow_type.unwrap().name;
                recorded.payloads = request.input.unwrap_or_default().payloads;
                recorded.ascii_metadata = ascii_metadata;
                recorded.binary_metadata = binary_metadata;
                recorded.grpc_timeout = grpc_timeout;
                recorded.retry_options = retry_options;

                Box::pin(async {
                    Ok(Response::new(StartWorkflowExecutionResponse {
                        run_id: "server-run-id".to_owned(),
                        ..Default::default()
                    }))
                })
            }

            fn signal_with_start_workflow_execution(
                &mut self,
                request: Request<SignalWithStartWorkflowExecutionRequest>,
            ) -> futures_util::future::BoxFuture<
                '_,
                Result<Response<SignalWithStartWorkflowExecutionResponse>, tonic::Status>,
            > {
                let ascii_metadata = request
                    .metadata()
                    .get("call-meta")
                    .map(|value| value.to_str().unwrap().to_owned());
                let binary_metadata = request
                    .metadata()
                    .get_bin("call-meta-bin")
                    .map(|value| value.to_bytes().unwrap().to_vec());
                let grpc_timeout = request
                    .metadata()
                    .get("grpc-timeout")
                    .map(|value| value.to_str().unwrap().to_owned());
                let retry_options = request
                    .extensions()
                    .get::<RetryConfigForCall>()
                    .map(|config| config.0.clone());
                let request = request.into_inner();
                let mut recorded = self.recorded.lock();
                recorded.calls += 1;
                recorded.workflow_type = request.workflow_type.unwrap().name;
                recorded.payloads = request.input.unwrap_or_default().payloads;
                recorded.ascii_metadata = ascii_metadata;
                recorded.binary_metadata = binary_metadata;
                recorded.grpc_timeout = grpc_timeout;
                recorded.retry_options = retry_options;

                Box::pin(async {
                    Ok(Response::new(SignalWithStartWorkflowExecutionResponse {
                        run_id: "signal-server-run-id".to_owned(),
                        ..Default::default()
                    }))
                })
            }
        }

        #[derive(Clone)]
        struct InterceptedClient {
            inner: MockStartWorkflowClient,
            interceptors: Vec<Arc<dyn ClientInterceptor>>,
        }

        impl NamespacedClient for InterceptedClient {
            fn namespace(&self) -> String {
                self.inner.namespace()
            }

            fn identity(&self) -> String {
                self.inner.identity()
            }

            fn data_converter(&self) -> &DataConverter {
                self.inner.data_converter()
            }

            fn client_interceptors(&self) -> &[Arc<dyn ClientInterceptor>] {
                &self.interceptors
            }
        }

        impl WorkflowService for InterceptedClient {
            fn start_workflow_execution(
                &mut self,
                request: Request<StartWorkflowExecutionRequest>,
            ) -> futures_util::future::BoxFuture<
                '_,
                Result<Response<StartWorkflowExecutionResponse>, tonic::Status>,
            > {
                self.inner.start_workflow_execution(request)
            }

            fn signal_with_start_workflow_execution(
                &mut self,
                request: Request<SignalWithStartWorkflowExecutionRequest>,
            ) -> futures_util::future::BoxFuture<
                '_,
                Result<Response<SignalWithStartWorkflowExecutionResponse>, tonic::Status>,
            > {
                self.inner.signal_with_start_workflow_execution(request)
            }
        }

        struct OrderedInterceptor {
            name: &'static str,
            events: Arc<Mutex<Vec<String>>>,
            encode_calls: Arc<AtomicUsize>,
        }

        impl ClientInterceptor for OrderedInterceptor {
            fn start_workflow<'a>(
                &'a self,
                mut input: StartWorkflowInput,
                next: Next<
                    'a,
                    StartWorkflowInput,
                    BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>,
                >,
            ) -> BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>> {
                Box::pin(async move {
                    assert_eq!(self.encode_calls.load(Ordering::SeqCst), 0);
                    self.events.lock().push(format!("{}-pre", self.name));
                    tokio::task::yield_now().await;
                    if self.name == "outer" {
                        input
                            .args_mut::<Vec<String>>()
                            .unwrap()
                            .push("mutated".to_owned());
                    } else {
                        assert_eq!(
                            input.args_ref::<Vec<String>>().unwrap(),
                            &["initial".to_owned(), "mutated".to_owned()]
                        );
                        input.replace_args("replacement".to_owned());
                        input.workflow_type = "replacement-workflow".to_owned();
                    }
                    let result = next.run(input).await;
                    tokio::task::yield_now().await;
                    self.events.lock().push(format!("{}-post", self.name));
                    result
                })
            }
        }

        struct ShortCircuitInterceptor;

        impl ClientInterceptor for ShortCircuitInterceptor {
            fn start_workflow<'a>(
                &'a self,
                input: StartWorkflowInput,
                _next: Next<
                    'a,
                    StartWorkflowInput,
                    BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>,
                >,
            ) -> BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>> {
                assert_eq!(
                    input.args_ref::<Vec<String>>().unwrap(),
                    &["initial".to_owned()]
                );
                Box::pin(async {
                    Ok(StartWorkflowOutput::new(
                        "short-circuit-workflow-id",
                        "short-circuit-run-id",
                    ))
                })
            }
        }

        struct CountingInput {
            conversion_calls: Arc<AtomicUsize>,
        }

        impl TemporalSerializable for CountingInput {
            fn to_payloads(
                &self,
                _context: &SerializationContext<'_>,
            ) -> Result<Vec<Payload>, PayloadConversionError> {
                self.conversion_calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![Payload::default()])
            }
        }

        struct ConversionTimingInterceptor {
            conversion_calls: Arc<AtomicUsize>,
        }

        impl ClientInterceptor for ConversionTimingInterceptor {
            fn start_workflow<'a>(
                &'a self,
                mut input: StartWorkflowInput,
                next: Next<
                    'a,
                    StartWorkflowInput,
                    BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>,
                >,
            ) -> BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>> {
                input.replace_args(CountingInput {
                    conversion_calls: self.conversion_calls.clone(),
                });
                let future = next.run(input);
                assert_eq!(self.conversion_calls.load(Ordering::SeqCst), 0);
                future
            }
        }

        fn mock_client(
            interceptors: Vec<Arc<dyn ClientInterceptor>>,
            encode_calls: Arc<AtomicUsize>,
        ) -> (InterceptedClient, Arc<Mutex<RecordedStart>>) {
            let recorded = Arc::new(Mutex::new(RecordedStart::default()));
            let data_converter = DataConverter::new(
                PayloadConverter::default(),
                DefaultFailureConverter,
                CountingCodec {
                    encode_calls: encode_calls.clone(),
                },
            );
            (
                InterceptedClient {
                    inner: MockStartWorkflowClient {
                        recorded: recorded.clone(),
                        data_converter,
                    },
                    interceptors,
                },
                recorded,
            )
        }

        #[tokio::test]
        async fn interceptors_order_mutate_replace_and_defer_conversion() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let encode_calls = Arc::new(AtomicUsize::new(0));
            let interceptors: Vec<Arc<dyn ClientInterceptor>> = vec![
                Arc::new(OrderedInterceptor {
                    name: "outer",
                    events: events.clone(),
                    encode_calls: encode_calls.clone(),
                }),
                Arc::new(OrderedInterceptor {
                    name: "inner",
                    events: events.clone(),
                    encode_calls: encode_calls.clone(),
                }),
            ];
            let (client, recorded) = mock_client(interceptors, encode_calls.clone());

            let handle = client
                .start_workflow(
                    TestWorkflow,
                    vec!["initial".to_owned()],
                    WorkflowStartOptions::new("task-queue", "workflow-id").build(),
                )
                .await
                .unwrap();

            assert_eq!(
                events.lock().as_slice(),
                ["outer-pre", "inner-pre", "inner-post", "outer-post"]
            );
            assert_eq!(encode_calls.load(Ordering::SeqCst), 1);
            assert_eq!(handle.run_id(), Some("server-run-id"));
            let payloads = {
                let recorded = recorded.lock();
                assert_eq!(recorded.calls, 1);
                assert_eq!(recorded.workflow_type, "replacement-workflow");
                recorded.payloads.clone()
            };
            let replacement: String = client
                .data_converter()
                .from_payloads(&SerializationContextData::Workflow, payloads)
                .await
                .unwrap();
            assert_eq!(replacement, "replacement");
        }

        #[tokio::test]
        async fn interceptor_can_short_circuit() {
            let encode_calls = Arc::new(AtomicUsize::new(0));
            let (client, recorded) = mock_client(
                vec![Arc::new(ShortCircuitInterceptor)],
                encode_calls.clone(),
            );
            let handle = client
                .start_workflow(
                    TestWorkflow,
                    vec!["initial".to_owned()],
                    WorkflowStartOptions::new("task-queue", "ignored-workflow-id").build(),
                )
                .await
                .unwrap();

            assert_eq!(handle.info().workflow_id, "short-circuit-workflow-id");
            assert_eq!(handle.run_id(), Some("short-circuit-run-id"));
            assert_eq!(recorded.lock().calls, 0);
            assert_eq!(encode_calls.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn payload_conversion_waits_for_next_future_poll() {
            let conversion_calls = Arc::new(AtomicUsize::new(0));
            let encode_calls = Arc::new(AtomicUsize::new(0));
            let recorded = Arc::new(Mutex::new(RecordedStart::default()));
            let data_converter = DataConverter::new(
                PayloadConverter::UseWrappers,
                DefaultFailureConverter,
                CountingCodec {
                    encode_calls: encode_calls.clone(),
                },
            );
            let client = InterceptedClient {
                inner: MockStartWorkflowClient {
                    recorded: recorded.clone(),
                    data_converter,
                },
                interceptors: vec![Arc::new(ConversionTimingInterceptor {
                    conversion_calls: conversion_calls.clone(),
                })],
            };

            client
                .start_workflow(
                    TestWorkflow,
                    vec!["initial".to_owned()],
                    WorkflowStartOptions::new("task-queue", "workflow-id").build(),
                )
                .await
                .unwrap();

            assert_eq!(conversion_calls.load(Ordering::SeqCst), 1);
            assert_eq!(encode_calls.load(Ordering::SeqCst), 1);
            assert_eq!(recorded.lock().calls, 1);
        }

        #[tokio::test]
        async fn custom_client_defaults_to_empty_chain() {
            let recorded = Arc::new(Mutex::new(RecordedStart::default()));
            let client = MockStartWorkflowClient {
                recorded: recorded.clone(),
                data_converter: DataConverter::default(),
            };
            assert!(client.client_interceptors().is_empty());

            client
                .start_workflow(
                    TestWorkflow,
                    vec!["initial".to_owned()],
                    WorkflowStartOptions::new("task-queue", "workflow-id").build(),
                )
                .await
                .unwrap();
            assert_eq!(recorded.lock().calls, 1);
        }

        #[tokio::test]
        async fn rpc_options_reach_the_request() {
            let (client, recorded) = mock_client(Vec::new(), Arc::new(AtomicUsize::new(0)));
            let mut metadata = RpcMetadata::new();
            metadata
                .insert("call-meta", "call-value")
                .unwrap();
            metadata
                .insert_binary("call-meta-bin", vec![0, 255])
                .unwrap();
            let rpc_options = RpcOptions::builder()
                .metadata(metadata)
                .timeout(Duration::from_millis(250))
                .retry_options(RetryOptions::no_retries())
                .build();
            let mut options = WorkflowStartOptions::new("task-queue", "workflow-id").build();
            options.rpc_options = rpc_options.clone();

            client
                .start_workflow(TestWorkflow, vec!["initial".to_owned()], options)
                .await
                .unwrap();

            {
                let recorded = recorded.lock();
                assert_eq!(recorded.ascii_metadata.as_deref(), Some("call-value"));
                assert_eq!(recorded.binary_metadata.as_deref(), Some(&[0, 255][..]));
                assert_eq!(recorded.grpc_timeout.as_deref(), Some("250000u"));
                assert_eq!(recorded.retry_options, Some(RetryOptions::no_retries()));
            }

            let mut options = WorkflowStartOptions::new("task-queue", "signal-workflow-id").build();
            options.start_signal = Some(WorkflowStartSignal::new("signal-name").build());
            options.rpc_options = rpc_options;
            let handle = client
                .start_workflow(TestWorkflow, vec!["initial".to_owned()], options)
                .await
                .unwrap();

            let recorded = recorded.lock();
            assert_eq!(recorded.calls, 2);
            assert_eq!(recorded.ascii_metadata.as_deref(), Some("call-value"));
            assert_eq!(recorded.binary_metadata.as_deref(), Some(&[0, 255][..]));
            assert_eq!(recorded.grpc_timeout.as_deref(), Some("250000u"));
            assert_eq!(recorded.retry_options, Some(RetryOptions::no_retries()));
            assert_eq!(handle.run_id(), Some("signal-server-run-id"));
        }

        #[test]
        fn rpc_metadata_combines_with_and_overrides_connection_defaults() {
            let headers = Arc::new(RwLock::new(ClientHeaders {
                user_headers: HashMap::from([
                    (
                        "shared-meta".parse().unwrap(),
                        "connection-value".parse().unwrap(),
                    ),
                    (
                        "connection-meta".parse().unwrap(),
                        "connection-only".parse().unwrap(),
                    ),
                ]),
                user_binary_headers: HashMap::from([
                    (
                        "shared-meta-bin".parse().unwrap(),
                        BinaryMetadataValue::from_bytes(&[1]),
                    ),
                    (
                        "connection-meta-bin".parse().unwrap(),
                        BinaryMetadataValue::from_bytes(&[2]),
                    ),
                ]),
                api_key: None,
            }));
            let mut service_interceptor = ServiceCallInterceptor {
                client_name: "test-client".to_owned(),
                client_version: "test-version".to_owned(),
                headers,
            };
            let mut rpc_options = RpcOptions::default();
            rpc_options
                .metadata
                .insert("shared-meta", "call-value")
                .unwrap();
            rpc_options
                .metadata
                .insert("call-meta", "call-only")
                .unwrap();
            rpc_options
                .metadata
                .insert_binary("shared-meta-bin", vec![3])
                .unwrap();
            rpc_options
                .metadata
                .insert_binary("call-meta-bin", vec![4])
                .unwrap();
            let mut request = Request::new(());
            rpc_options.apply_to(&mut request);

            let request = service_interceptor.call(request).unwrap();
            assert_eq!(request.metadata().get("shared-meta").unwrap(), "call-value");
            assert_eq!(request.metadata().get("call-meta").unwrap(), "call-only");
            assert_eq!(
                request.metadata().get("connection-meta").unwrap(),
                "connection-only"
            );
            assert_eq!(
                request.metadata().get_bin("shared-meta-bin").unwrap(),
                &[3][..]
            );
            assert_eq!(
                request.metadata().get_bin("call-meta-bin").unwrap(),
                &[4][..]
            );
            assert_eq!(
                request.metadata().get_bin("connection-meta-bin").unwrap(),
                &[2][..]
            );
        }
    }

    mod list_workflows_tests {
        use super::*;
        use crate::test_helpers::{FailingCodec, XorCodec};
        use futures_util::{FutureExt, StreamExt};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use temporalio_common::{
            data_converters::DefaultFailureConverter,
            protos::temporal::api::common::v1::{
                Memo as ProtoMemo, Payload, WorkflowExecution as ProtoWorkflowExecution,
            },
        };
        use tonic::{Request, Response};

        #[derive(Clone)]
        struct MockListWorkflowsClient {
            call_count: Arc<AtomicUsize>,
            // Returns this many workflows per page
            page_size: usize,
            // Total workflows available
            total_workflows: usize,
            data_converter: DataConverter,
            memo_payload: Option<Payload>,
            interceptors: Vec<Arc<dyn ClientInterceptor>>,
        }

        impl NamespacedClient for MockListWorkflowsClient {
            fn namespace(&self) -> String {
                "test-namespace".to_string()
            }
            fn identity(&self) -> String {
                "test-identity".to_string()
            }
            fn data_converter(&self) -> &DataConverter {
                &self.data_converter
            }
            fn client_interceptors(&self) -> &[Arc<dyn ClientInterceptor>] {
                &self.interceptors
            }
        }

        struct CountingListInterceptor {
            calls: Arc<AtomicUsize>,
        }

        impl ClientInterceptor for CountingListInterceptor {
            fn list_workflows_page<'a>(
                &'a self,
                input: ListWorkflowsPageInput,
                next: Next<
                    'a,
                    ListWorkflowsPageInput,
                    BoxFuture<'a, Result<ListWorkflowsPageOutput, ClientError>>,
                >,
            ) -> BoxFuture<'a, Result<ListWorkflowsPageOutput, ClientError>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                next.run(input)
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
                        memo: self.memo_payload.clone().map(|payload| ProtoMemo {
                            fields: HashMap::from([("memo-key".to_owned(), payload)]),
                        }),
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
            let interceptor_calls = Arc::new(AtomicUsize::new(0));
            let client = MockListWorkflowsClient {
                call_count: call_count.clone(),
                page_size: 3,
                total_workflows: 10,
                data_converter: DataConverter::default(),
                memo_payload: None,
                interceptors: vec![Arc::new(CountingListInterceptor {
                    calls: interceptor_calls.clone(),
                })],
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
            assert_eq!(interceptor_calls.load(Ordering::SeqCst), 4);
        }

        #[tokio::test]
        async fn list_workflows_respects_limit() {
            let call_count = Arc::new(AtomicUsize::new(0));
            let client = MockListWorkflowsClient {
                call_count: call_count.clone(),
                page_size: 3,
                total_workflows: 10,
                data_converter: DataConverter::default(),
                memo_payload: None,
                interceptors: Vec::new(),
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
                data_converter: DataConverter::default(),
                memo_payload: None,
                interceptors: Vec::new(),
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
                data_converter: DataConverter::default(),
                memo_payload: None,
                interceptors: Vec::new(),
            };

            let stream = client.list_workflows("", WorkflowListOptions::default());
            let results: Vec<_> = stream.collect().await;

            assert_eq!(results.len(), 0);
            assert_eq!(call_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn list_workflows_exposes_typed_memo() {
            let data_converter = DataConverter::new(
                PayloadConverter::default(),
                DefaultFailureConverter,
                XorCodec,
            );
            let memo_payload = data_converter
                .to_payload(
                    &SerializationContextData::Workflow,
                    &"memo-value".to_owned(),
                )
                .await
                .unwrap();
            let client = MockListWorkflowsClient {
                call_count: Arc::new(AtomicUsize::new(0)),
                page_size: 1,
                total_workflows: 1,
                data_converter,
                memo_payload: Some(memo_payload),
                interceptors: Vec::new(),
            };

            let workflow = client
                .list_workflows("", WorkflowListOptions::default())
                .next()
                .await
                .unwrap()
                .unwrap();

            assert_eq!(
                workflow.memo().get::<String>("memo-key").unwrap(),
                Some("memo-value".to_owned())
            );
        }

        #[tokio::test]
        async fn list_workflows_yields_codec_error_then_ends() {
            let client = MockListWorkflowsClient {
                call_count: Arc::new(AtomicUsize::new(0)),
                page_size: 1,
                total_workflows: 1,
                data_converter: DataConverter::new(
                    PayloadConverter::default(),
                    DefaultFailureConverter,
                    FailingCodec,
                ),
                memo_payload: Some(Payload::default()),
                interceptors: Vec::new(),
            };
            let mut stream = client.list_workflows("", WorkflowListOptions::default());

            let err = stream.next().await.unwrap().unwrap_err();

            assert!(matches!(err, ClientError::PayloadConversion(_)));
            assert!(stream.next().await.is_none());
        }
    }
}
