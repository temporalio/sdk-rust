use crate::{HttpConnectProxyOptions, RetryOptions, VERSION, callback_based};
use http::Uri;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_common::{
    data_converters::DataConverter,
    protos::temporal::api::{
        common::{
            self,
            v1::{Header, Payloads},
        },
        enums::v1::{
            ArchivalState, HistoryEventFilterType, QueryRejectCondition, WorkflowIdConflictPolicy,
            WorkflowIdReusePolicy,
        },
        replication::v1::ClusterReplicationConfig,
        workflowservice::v1::RegisterNamespaceRequest,
    },
    search_attributes::SearchAttributes,
    telemetry::metrics::TemporalMeter,
};
use tokio_rustls::rustls::client::danger::ServerCertVerifier;
use url::Url;

/// Options for [crate::Connection::connect].
#[derive(bon::Builder, Clone, Debug)]
#[non_exhaustive]
#[builder(start_fn = new, on(String, into), state_mod(vis = "pub"))]
pub struct ConnectionOptions {
    /// The server to connect to.
    #[builder(start_fn, into)]
    pub target: Url,
    /// A human-readable string that can identify this process. Defaults to empty string.
    #[builder(default)]
    pub identity: String,
    /// When set, this client will record metrics using the provided meter. The meter can be
    /// obtained from [temporalio_common::telemetry::TelemetryInstance::get_temporal_metric_meter].
    pub metrics_meter: Option<TemporalMeter>,
    /// If specified, use TLS as configured by the [TlsOptions] struct. If this is set core will
    /// attempt to use TLS when connecting to the Temporal server. Lang SDK is expected to pass any
    /// certs or keys as bytes, loading them from disk itself if needed.
    pub tls_options: Option<TlsOptions>,
    /// If set, override the origin used when connecting. May be useful in rare situations where tls
    /// verification needs to use a different name from what should be set as the `:authority`
    /// header. If [TlsOptions::domain] is set, and this is not, this will be set to
    /// `https://<domain>`, effectively making the `:authority` header consistent with the domain
    /// override.
    pub override_origin: Option<Uri>,
    /// An API key to use for auth. If set, TLS will be enabled by default, but without any mTLS
    /// specific settings.
    pub api_key: Option<String>,
    /// Retry configuration for the server client. Default is [RetryOptions::default]
    #[builder(default)]
    pub retry_options: RetryOptions,
    /// If set, HTTP2 gRPC keep alive will be enabled.
    /// To enable with default settings, use `.keep_alive(Some(ClientKeepAliveConfig::default()))`.
    #[builder(required, default = Some(ClientKeepAliveOptions::default()))]
    pub keep_alive: Option<ClientKeepAliveOptions>,
    /// HTTP headers to include on every RPC call.
    ///
    /// These must be valid gRPC metadata keys, and must not be binary metadata keys (ending in
    /// `-bin). To set binary headers, use [ConnectionOptions::binary_headers]. Invalid header keys
    /// or values will cause an error to be returned when connecting.
    pub headers: Option<HashMap<String, String>>,
    /// HTTP headers to include on every RPC call as binary gRPC metadata (encoded as base64).
    ///
    /// These must be valid binary gRPC metadata keys (and end with a `-bin` suffix). Invalid
    /// header keys will cause an error to be returned when connecting.
    pub binary_headers: Option<HashMap<String, Vec<u8>>>,
    /// HTTP CONNECT proxy to use for this client.
    pub http_connect_proxy: Option<HttpConnectProxyOptions>,
    /// If set, DNS-based load balancing is enabled. When the target is a hostname (not an IP
    /// literal), DNS is resolved to all addresses and requests are distributed across them.
    /// Incompatible with `service_override` and `http_connect_proxy`. Setting either in addition
    /// to this field is an error. Set to `None` to disable.
    #[builder(required, default = Some(DnsLoadBalancingOptions::default()))]
    pub dns_load_balancing: Option<DnsLoadBalancingOptions>,
    /// If set true, error code labels will not be included on request failure metrics.
    #[builder(default)]
    pub disable_error_code_metric_tags: bool,
    /// If set, all gRPC calls will be routed through the provided service.
    pub service_override: Option<callback_based::CallbackBasedGrpcService>,
    /// Controls transport-level gRPC compression for the client. Defaults to
    /// [GrpcCompression::Gzip], which compresses outbound request bodies and accepts
    /// compressed responses. Set to [GrpcCompression::None] to opt out.
    /// If service_override is specified, is forced to `None`.
    #[builder(default)]
    pub grpc_compression: GrpcCompression,

    // Internal / Core-based SDK only options below =============================================
    /// If set true, get_system_info will not be called upon connection.
    #[builder(default)]
    #[cfg_attr(feature = "core-based-sdk", builder(setters(vis = "pub")))]
    pub(crate) skip_get_system_info: bool,
    /// The name of the SDK being implemented on top of core. Is set as `client-name` header in
    /// all RPC calls
    #[builder(default = "temporal-rust".to_owned())]
    #[cfg_attr(feature = "core-based-sdk", builder(setters(vis = "pub")))]
    pub(crate) client_name: String,
    // TODO [rust-sdk-branch]: SDK should set this to its version. Doing that probably easiest
    // after adding proper client interceptors.
    /// The version of the SDK being implemented on top of core. Is set as `client-version` header
    /// in all RPC calls. The server decides if the client is supported based on this.
    #[builder(default = VERSION.to_owned())]
    #[cfg_attr(feature = "core-based-sdk", builder(setters(vis = "pub")))]
    pub(crate) client_version: String,
}

// Setters/getters for fields that should only be touched by SDK implementers.
#[cfg(feature = "core-based-sdk")]
impl ConnectionOptions {
    /// Set whether or not get_system_info will be called upon connection.
    pub fn set_skip_get_system_info(&mut self, skip: bool) {
        self.skip_get_system_info = skip;
    }
    /// Get whether or not get_system_info will be called upon connection.
    pub fn get_skip_get_system_info(&self) -> bool {
        self.skip_get_system_info
    }
    /// Get the name of the SDK being implemented on top of core.
    pub fn get_client_name(&self) -> &str {
        &self.client_name
    }
    /// Get the version of the SDK being implemented on top of core.
    pub fn get_client_version(&self) -> &str {
        &self.client_version
    }
}

/// Options for [crate::Client::new].
#[derive(Clone, Debug, bon::Builder)]
#[non_exhaustive]
#[builder(start_fn = new, on(String, into), state_mod(vis = "pub"))]
pub struct ClientOptions {
    /// The namespace this client will be bound to.
    #[builder(start_fn)]
    pub namespace: String,
    /// The data converter used for serializing/deserializing payloads.
    #[builder(default)]
    pub data_converter: DataConverter,
}

/// Selects the transport-level compression used for gRPC calls. See
/// [ConnectionOptions::grpc_compression].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum GrpcCompression {
    /// Do not compress requests or advertise acceptance of compressed responses.
    None,
    /// Gzip-compress outbound requests and accept gzip-compressed responses.
    #[default]
    Gzip,
}

/// Configuration options for TLS
#[derive(Clone, Default)]
pub struct TlsOptions {
    /// Bytes representing the root CA certificate used by the server. If not set, and the server's
    /// cert is issued by someone the operating system trusts, verification will still work (ex:
    /// Cloud offering).
    pub server_root_ca_cert: Option<Vec<u8>>,
    /// Sets the domain name against which to verify the server's TLS certificate. If not provided,
    /// the domain name will be extracted from the URL used to connect.
    pub domain: Option<String>,
    /// TLS info for the client. If specified, core will attempt to use mTLS.
    pub client_tls_options: Option<ClientTlsOptions>,
    /// Optional custom server certificate verifier. When set, this replaces the default
    /// certificate verification and `server_root_ca_cert` is ignored.
    ///
    /// This is useful for:
    /// - Certificate pinning
    /// - Custom trust-domain validation (e.g., SAN-URI extraction)
    /// - Federated root certificate stores
    ///
    /// # WARNING
    /// Implementing a custom `ServerCertVerifier` can lead to severely insecure TLS connections
    /// (e.g., disabling all validation or allowing man-in-the-middle attacks) if not done carefully.
    /// Only use this if you know exactly what you are doing.
    ///
    /// The verifier must implement [`ServerCertVerifier`] from the `rustls` crate.
    /// Note that `domain` is still respected for the `:authority` header / origin override
    /// even when a custom verifier is set.
    pub server_cert_verifier: Option<Arc<dyn ServerCertVerifier>>,
}

impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsOptions")
            .field(
                "server_root_ca_cert",
                &self
                    .server_root_ca_cert
                    .as_ref()
                    .map(|c| format!("{} bytes", c.len())),
            )
            .field("domain", &self.domain)
            .field("client_tls_options", &self.client_tls_options)
            .field(
                "server_cert_verifier",
                &self.server_cert_verifier.as_ref().map(|_| "<custom>"),
            )
            .finish()
    }
}

/// If using mTLS, both the client cert and private key must be specified, this contains them.
#[derive(Clone)]
pub struct ClientTlsOptions {
    /// The certificate for this client, encoded as PEM
    pub client_cert: Vec<u8>,
    /// The private key for this client, encoded as PEM
    pub client_private_key: Vec<u8>,
}

/// Client keep alive configuration.
#[derive(Clone, Debug)]
pub struct ClientKeepAliveOptions {
    /// Interval to send HTTP2 keep alive pings.
    pub interval: Duration,
    /// Timeout that the keep alive must be responded to within or the connection will be closed.
    pub timeout: Duration,
}

impl Default for ClientKeepAliveOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(15),
        }
    }
}

/// Options for DNS-based load balancing.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DnsLoadBalancingOptions {
    /// How often to re-resolve DNS. Defaults to 30 seconds.
    pub resolution_interval: Duration,
}

impl Default for DnsLoadBalancingOptions {
    fn default() -> Self {
        Self {
            resolution_interval: Duration::from_secs(30),
        }
    }
}

impl std::fmt::Debug for ClientTlsOptions {
    // Intentionally omit details here since they could leak a key if ever printed
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientTlsOptions(..)")
    }
}

/// Options for starting a workflow execution.
#[derive(Debug, Clone, bon::Builder)]
#[builder(start_fn = new, on(String, into))]
#[non_exhaustive]
pub struct WorkflowStartOptions {
    /// The task queue to run the workflow on.
    #[builder(start_fn)]
    pub task_queue: String,

    /// The workflow ID.
    #[builder(start_fn)]
    pub workflow_id: String,

    /// Set the policy for reusing the workflow id
    #[builder(default)]
    pub id_reuse_policy: WorkflowIdReusePolicy,

    /// Set the policy for how to resolve conflicts with running policies.
    /// NOTE: This is ignored for child workflows.
    #[builder(default)]
    pub id_conflict_policy: WorkflowIdConflictPolicy,

    /// Optionally set the execution timeout for the workflow
    /// <https://docs.temporal.io/workflows/#workflow-execution-timeout>
    pub execution_timeout: Option<Duration>,

    /// Optionally indicates the default run timeout for a workflow run
    pub run_timeout: Option<Duration>,

    /// Optionally indicates the default task timeout for a workflow run
    pub task_timeout: Option<Duration>,

    /// Optionally set a cron schedule for the workflow
    pub cron_schedule: Option<String>,

    /// Additional search attributes for the workflow.
    pub search_attributes: Option<SearchAttributes>,

    /// Optionally enable Eager Workflow Start, a latency optimization using local workers
    /// NOTE: Experimental
    #[builder(default)]
    pub enable_eager_workflow_start: bool,

    /// Optionally set a retry policy for the workflow
    pub retry_policy: Option<common::v1::RetryPolicy>,

    /// If set, send a signal to the workflow atomically with start.
    /// The workflow will receive this signal before its first task.
    pub start_signal: Option<WorkflowStartSignal>,

    /// Links to associate with the workflow. Ex: References to a nexus operation.
    #[builder(default)]
    pub links: Vec<common::v1::Link>,

    /// Callbacks that will be invoked upon workflow completion. For, ex, completing nexus
    /// operations.
    #[builder(default)]
    pub completion_callbacks: Vec<common::v1::Callback>,

    /// Priority for the workflow. Defaults to all-inherited (empty).
    #[builder(default)]
    pub priority: Priority,

    /// Headers to include with the start request.
    pub header: Option<Header>,

    /// Single-line static summary for the workflow, shown in the Temporal UI.
    pub static_summary: Option<String>,

    /// Multi-line static details for the workflow, shown in the Temporal UI.
    pub static_details: Option<String>,
}

/// A signal to send atomically when starting a workflow.
/// Use with `WorkflowStartOptions::start_signal` to achieve signal-with-start behavior.
#[derive(Debug, Clone, bon::Builder)]
#[builder(start_fn = new, on(String, into))]
#[non_exhaustive]
pub struct WorkflowStartSignal {
    /// Name of the signal to send.
    #[builder(start_fn)]
    pub signal_name: String,
    /// Payload for the signal.
    pub input: Option<Payloads>,
    /// Headers for the signal.
    pub header: Option<Header>,
}

pub use temporalio_common::Priority;

/// Options for fetching workflow results
#[derive(Debug, Clone, Copy, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowGetResultOptions {
    /// If true (the default), follows to the next workflow run in the execution chain while
    /// retrieving results.
    #[builder(default = true)]
    pub follow_runs: bool,
}
impl Default for WorkflowGetResultOptions {
    fn default() -> Self {
        Self { follow_runs: true }
    }
}

/// Options for starting a workflow update.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowExecuteUpdateOptions {
    /// Update ID for idempotency.
    pub update_id: Option<String>,
    /// Headers to include.
    pub header: Option<Header>,
}

/// Options for sending a signal to a workflow.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowSignalOptions {
    /// Request ID for idempotency. If not provided, a UUID will be generated.
    pub request_id: Option<String>,
    /// Headers to include with the signal.
    pub header: Option<Header>,
}

/// Options for querying a workflow.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowQueryOptions {
    /// Query reject condition. Determines when the query should be rejected
    /// based on workflow state.
    pub reject_condition: Option<QueryRejectCondition>,
    /// Headers to include with the query.
    pub header: Option<Header>,
}

/// Options for cancelling a workflow.
#[derive(Debug, Clone, Default, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct WorkflowCancelOptions {
    /// Reason for cancellation.
    #[builder(default)]
    pub reason: String,
    /// Request ID for idempotency. If not provided, a UUID will be generated.
    pub request_id: Option<String>,
}

/// Options for terminating a workflow.
#[derive(Debug, Clone, Default, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct WorkflowTerminateOptions {
    /// Reason for termination.
    #[builder(default)]
    pub reason: String,
    /// Additional details to include with the termination.
    pub details: Option<Payloads>,
}

/// Options for describing a workflow.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowDescribeOptions {}

/// Default workflow execution retention for a Namespace is 3 days
const DEFAULT_WORKFLOW_EXECUTION_RETENTION_PERIOD: Duration = Duration::from_secs(60 * 60 * 24 * 3);

/// Helper struct for `register_namespace`.
#[derive(Clone, Debug, bon::Builder)]
#[builder(on(String, into))]
pub struct RegisterNamespaceOptions {
    /// Name (required)
    pub namespace: String,
    /// Description (required)
    pub description: String,
    /// Owner's email
    #[builder(default)]
    pub owner_email: String,
    /// Workflow execution retention period
    #[builder(default = DEFAULT_WORKFLOW_EXECUTION_RETENTION_PERIOD)]
    pub workflow_execution_retention_period: Duration,
    /// Cluster settings
    #[builder(default)]
    pub clusters: Vec<ClusterReplicationConfig>,
    /// Active cluster name
    #[builder(default)]
    pub active_cluster_name: String,
    /// Custom Data
    #[builder(default)]
    pub data: HashMap<String, String>,
    /// Security Token
    #[builder(default)]
    pub security_token: String,
    /// Global namespace
    #[builder(default)]
    pub is_global_namespace: bool,
    /// History Archival setting
    #[builder(default = ArchivalState::Unspecified)]
    pub history_archival_state: ArchivalState,
    /// History Archival uri
    #[builder(default)]
    pub history_archival_uri: String,
    /// Visibility Archival setting
    #[builder(default = ArchivalState::Unspecified)]
    pub visibility_archival_state: ArchivalState,
    /// Visibility Archival uri
    #[builder(default)]
    pub visibility_archival_uri: String,
}

impl From<RegisterNamespaceOptions> for RegisterNamespaceRequest {
    fn from(val: RegisterNamespaceOptions) -> Self {
        RegisterNamespaceRequest {
            namespace: val.namespace,
            description: val.description,
            owner_email: val.owner_email,
            workflow_execution_retention_period: val
                .workflow_execution_retention_period
                .try_into()
                .ok(),
            clusters: val.clusters,
            active_cluster_name: val.active_cluster_name,
            data: val.data,
            security_token: val.security_token,
            is_global_namespace: val.is_global_namespace,
            history_archival_state: val.history_archival_state as i32,
            history_archival_uri: val.history_archival_uri,
            visibility_archival_state: val.visibility_archival_state as i32,
            visibility_archival_uri: val.visibility_archival_uri,
        }
    }
}

/// Options for fetching workflow history.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowFetchHistoryOptions {
    /// Whether to skip archival.
    #[builder(default)]
    pub skip_archival: bool,
    /// If set true, the fetch will wait for a new event before returning.
    #[builder(default)]
    pub wait_new_event: bool,
    /// Specifies which kind of events will be retrieved. Defaults to all events.
    #[builder(default = HistoryEventFilterType::AllEvent)]
    pub event_filter_type: HistoryEventFilterType,
}

/// Which lifecycle stage to wait for when starting an update.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WorkflowUpdateWaitStage {
    /// This stage is reached when the server receives the update to process.
    /// This is currently an invalid value on start.
    Admitted,
    /// Wait until the update is accepted by the workflow (validator passed).
    #[default]
    Accepted,
    /// Wait until the update has completed.
    Completed,
}

/// Options for starting an update without waiting for completion.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowStartUpdateOptions {
    /// Update ID for idempotency. If not provided, a UUID will be generated.
    pub update_id: Option<String>,
    /// Headers to include with the update.
    pub header: Option<Header>,
    /// The lifecycle stage to wait for before returning the handle.
    #[builder(default)]
    pub wait_for_stage: WorkflowUpdateWaitStage,
}

/// Options for listing workflows.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowListOptions {
    /// Maximum number of workflows to return.
    /// If not specified, returns all matching workflows.
    pub limit: Option<usize>,
}

/// Options for counting workflows.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct WorkflowCountOptions {}
