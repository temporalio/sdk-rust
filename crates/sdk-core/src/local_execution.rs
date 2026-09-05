use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Write as _,
    io,
    net::{Ipv6Addr, TcpListener},
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use temporalio_client::Connection;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::{Child, Command},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const BRIDGE_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
const BRIDGE_BOOTSTRAP_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Experimental options for running a Worker's workflow and Activity traffic through a local
/// Temporal bridge server.
#[derive(Clone, Debug, bon::Builder)]
#[builder(on(PathBuf, into), state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct LocalFirstOptions {
    /// Maximum wall-clock interval between attempts to synchronize local history upstream.
    pub sync_interval: Duration,
    /// Private durable directory containing the bridge identity, execution records, and SQLite.
    pub state_directory: PathBuf,
    /// Path to a Temporal CLI executable that supports `server start-bridge`.
    pub temporal_cli_path: PathBuf,
    /// Maximum number of unsynchronized history events allowed for one local execution.
    #[builder(default = 10_240)]
    pub max_unsynchronized_events: usize,
    /// Maximum serialized size of an unsynchronized history tail for one local execution.
    #[builder(default = 8 << 20)]
    pub max_unsynchronized_bytes: usize,
}

impl LocalFirstOptions {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.sync_interval.is_zero() {
            return Err("local-first sync interval must be positive".to_owned());
        }
        if self.sync_interval.as_millis() > i64::MAX as u128 {
            return Err("local-first sync interval is outside the supported range".to_owned());
        }
        if self.state_directory.as_os_str().is_empty() {
            return Err("local-first state directory is required".to_owned());
        }
        if self.temporal_cli_path.as_os_str().is_empty() {
            return Err("local-first Temporal CLI path is required".to_owned());
        }
        if self.max_unsynchronized_events == 0 {
            return Err("local-first maximum unsynchronized events must be positive".to_owned());
        }
        if self.max_unsynchronized_bytes == 0 {
            return Err("local-first maximum unsynchronized bytes must be positive".to_owned());
        }
        if self.max_unsynchronized_events > i64::MAX as usize
            || self.max_unsynchronized_bytes > i64::MAX as usize
        {
            return Err("local-first history limits are outside the supported range".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalBridgeError {
    #[error("upstream connection option is not supported by bridge mode: {0}")]
    UnsupportedConnection(&'static str),
    #[error("upstream address cannot be forwarded to bridge mode")]
    InvalidUpstreamAddress,
    #[error("upstream TLS material is not UTF-8 PEM: {0}")]
    InvalidTlsMaterial(#[from] std::string::FromUtf8Error),
    #[error("failed to prepare local-first state: {0}")]
    PrepareState(#[source] io::Error),
    #[error("failed to start Temporal CLI bridge: {0}")]
    Spawn(#[source] io::Error),
    #[error("Temporal CLI bridge exited during startup with status {0}")]
    Exited(std::process::ExitStatus),
    #[error("bridge bootstrap timed out")]
    BootstrapTimeout,
    #[error("bridge bootstrap transport failed: {0}")]
    BootstrapIo(#[source] io::Error),
    #[error("bridge bootstrap returned HTTP status {0}")]
    BootstrapStatus(u16),
    #[error("bridge bootstrap returned an invalid HTTP response")]
    InvalidHttpResponse,
    #[error("bridge bootstrap response was invalid: {0}")]
    InvalidBootstrapResponse(#[from] serde_json::Error),
    #[error("bridge bootstrap configuration could not be encoded: {0}")]
    EncodeBootstrap(serde_json::Error),
    #[error("bridge local frontend address was invalid: {0}")]
    InvalidLocalAddress(#[from] url::ParseError),
    #[error("failed to connect to bridge local frontend: {0}")]
    ConnectLocal(#[from] temporalio_client::errors::ClientConnectError),
    #[error("local-first startup was cancelled")]
    Cancelled,
}

pub(crate) struct LocalBridgeProcess {
    child: Child,
}

impl LocalBridgeProcess {
    pub(crate) async fn shutdown(mut self) {
        if self.child.id().is_some() {
            let _ = self.child.kill().await;
        }
    }
}

#[derive(Serialize)]
struct BridgeConfiguration {
    namespace: String,
    upstream: UpstreamConnectionProfile,
    options: BridgeLocalFirstOptions,
    registrations: WorkerRegistrationManifest,
}

#[derive(Serialize)]
struct UpstreamConnectionProfile {
    address: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    identity: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    server_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls: Option<UpstreamTls>,
    #[serde(skip_serializing_if = "String::is_empty")]
    api_key: String,
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    headers: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
struct UpstreamTls {
    #[serde(skip_serializing_if = "String::is_empty")]
    server_root_ca_certificate: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    client_certificate: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    client_private_key: String,
}

#[derive(Serialize)]
struct BridgeLocalFirstOptions {
    sync_interval_milliseconds: i64,
    maximum_unsynchronized_events: i64,
    maximum_unsynchronized_bytes: i64,
}

#[derive(Serialize)]
struct WorkerRegistrationManifest {
    task_queue: String,
    workflow_types: Vec<String>,
    activity_types: Vec<String>,
}

#[derive(Deserialize)]
struct BridgeBootstrapResponse {
    frontend_address: String,
    local_server_id: String,
}

pub(crate) fn supports_local_execution(
    system_capabilities: Option<&temporalio_common::protos::temporal::api::workflowservice::v1::get_system_info_response::Capabilities>,
    namespace_info: &temporalio_common::protos::temporal::api::namespace::v1::NamespaceInfo,
    requested_interval: Duration,
) -> bool {
    if !system_capabilities.is_some_and(|capabilities| capabilities.local_execution)
        || !namespace_info
            .capabilities
            .as_ref()
            .is_some_and(|capabilities| capabilities.local_execution)
    {
        return false;
    }
    let Some(limits) = namespace_info.limits.as_ref() else {
        return false;
    };
    let Ok(minimum) = limits
        .minimum_local_execution_sync_interval
        .as_ref()
        .ok_or(())
        .and_then(|duration| Duration::try_from(*duration).map_err(|_| ()))
    else {
        return false;
    };
    let Ok(maximum) = limits
        .maximum_local_execution_sync_interval
        .as_ref()
        .ok_or(())
        .and_then(|duration| Duration::try_from(*duration).map_err(|_| ()))
    else {
        return false;
    };
    requested_interval >= minimum && requested_interval <= maximum
}

pub(crate) async fn start_local_bridge(
    connection: &Connection,
    namespace: &str,
    task_queue: &str,
    workflow_types: &[String],
    activity_types: &[String],
    options: &LocalFirstOptions,
    cancellation: &CancellationToken,
) -> Result<(LocalBridgeProcess, Connection), LocalBridgeError> {
    let upstream = upstream_profile(connection)?;
    prepare_state_directory(&options.state_directory).await?;
    let bootstrap_port = reserve_loopback_port()?;
    let token = random_token();
    let token_path = options
        .state_directory
        .join(format!(".bootstrap-{}", Uuid::new_v4()));
    write_private_token(&token_path, token.as_bytes()).await?;

    let mut child = Command::new(&options.temporal_cli_path)
        .args([
            "server",
            "start-bridge",
            "--state-dir",
            options.state_directory.to_string_lossy().as_ref(),
            "--bootstrap-token-file",
            token_path.to_string_lossy().as_ref(),
            "--bootstrap-port",
            &bootstrap_port.to_string(),
            "--log-level",
            "warn",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(LocalBridgeError::Spawn)?;

    let configuration = BridgeConfiguration {
        namespace: namespace.to_owned(),
        upstream,
        options: BridgeLocalFirstOptions {
            sync_interval_milliseconds: options.sync_interval.as_millis() as i64,
            maximum_unsynchronized_events: options.max_unsynchronized_events as i64,
            maximum_unsynchronized_bytes: options.max_unsynchronized_bytes as i64,
        },
        registrations: WorkerRegistrationManifest {
            task_queue: task_queue.to_owned(),
            workflow_types: workflow_types.to_vec(),
            activity_types: activity_types.to_vec(),
        },
    };
    let body = serde_json::to_vec(&configuration).map_err(LocalBridgeError::EncodeBootstrap)?;
    let response =
        bootstrap_until_ready(&mut child, bootstrap_port, &token, &body, cancellation).await;
    let _ = tokio::fs::remove_file(&token_path).await;
    let response = response?;
    if response.frontend_address.is_empty() || Uuid::parse_str(&response.local_server_id).is_err() {
        return Err(LocalBridgeError::InvalidHttpResponse);
    }

    let mut local_options = connection.connection_options();
    local_options.target = Url::parse(&format!("http://{}", response.frontend_address))?;
    local_options.tls_options = None;
    local_options.override_origin = None;
    local_options.api_key = None;
    local_options.headers = None;
    local_options.binary_headers = None;
    local_options.http_connect_proxy = None;
    local_options.service_override = None;
    let local_connection = Connection::connect(local_options).await?;
    Ok((LocalBridgeProcess { child }, local_connection))
}

fn upstream_profile(
    connection: &Connection,
) -> Result<UpstreamConnectionProfile, LocalBridgeError> {
    let options = connection.connection_options();
    if options.service_override.is_some() {
        return Err(LocalBridgeError::UnsupportedConnection(
            "custom gRPC service",
        ));
    }
    if options.http_connect_proxy.is_some() {
        return Err(LocalBridgeError::UnsupportedConnection(
            "HTTP CONNECT proxy",
        ));
    }
    if options
        .binary_headers
        .as_ref()
        .is_some_and(|headers| !headers.is_empty())
    {
        return Err(LocalBridgeError::UnsupportedConnection("binary headers"));
    }
    let host = options
        .target
        .host_str()
        .ok_or(LocalBridgeError::InvalidUpstreamAddress)?;
    let port = options.target.port().unwrap_or(7233);
    let address = if host.parse::<Ipv6Addr>().is_ok() {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let mut server_name = options
        .override_origin
        .as_ref()
        .and_then(|origin| origin.host())
        .unwrap_or_default()
        .to_owned();
    let tls = if let Some(tls) = options.tls_options {
        if tls.server_cert_verifier.is_some() {
            return Err(LocalBridgeError::UnsupportedConnection(
                "custom TLS certificate verifier",
            ));
        }
        #[cfg(feature = "dynamic-tls")]
        if tls.client_cert_resolver.is_some() {
            return Err(LocalBridgeError::UnsupportedConnection(
                "dynamic TLS client certificate resolver",
            ));
        }
        if server_name.is_empty() {
            server_name = tls.domain.unwrap_or_default();
        }
        let (client_certificate, client_private_key) = if let Some(client) = tls.client_tls_options
        {
            (
                String::from_utf8(client.client_cert)?,
                String::from_utf8(client.client_private_key)?,
            )
        } else {
            Default::default()
        };
        Some(UpstreamTls {
            server_root_ca_certificate: tls
                .server_root_ca_cert
                .map(String::from_utf8)
                .transpose()?
                .unwrap_or_default(),
            client_certificate,
            client_private_key,
        })
    } else if options.target.scheme() == "https" || options.api_key.is_some() {
        Some(UpstreamTls {
            server_root_ca_certificate: String::new(),
            client_certificate: String::new(),
            client_private_key: String::new(),
        })
    } else {
        None
    };
    Ok(UpstreamConnectionProfile {
        address,
        identity: connection.identity().to_owned(),
        server_name,
        tls,
        api_key: options.api_key.unwrap_or_default(),
        headers: options.headers.unwrap_or_default(),
    })
}

async fn prepare_state_directory(path: &Path) -> Result<(), LocalBridgeError> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(LocalBridgeError::PrepareState)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(LocalBridgeError::PrepareState)?;
    }
    Ok(())
}

fn reserve_loopback_port() -> Result<u16, LocalBridgeError> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(LocalBridgeError::PrepareState)?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(LocalBridgeError::PrepareState)
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut token, "{byte:02x}").expect("writing into a String cannot fail");
    }
    token
}

async fn write_private_token(path: &Path, token: &[u8]) -> Result<(), LocalBridgeError> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .await
        .map_err(LocalBridgeError::PrepareState)?;
    file.write_all(token)
        .await
        .map_err(LocalBridgeError::PrepareState)?;
    file.sync_all()
        .await
        .map_err(LocalBridgeError::PrepareState)
}

async fn bootstrap_until_ready(
    child: &mut Child,
    port: u16,
    token: &str,
    body: &[u8],
    cancellation: &CancellationToken,
) -> Result<BridgeBootstrapResponse, LocalBridgeError> {
    let deadline = Instant::now() + BRIDGE_BOOTSTRAP_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().map_err(LocalBridgeError::Spawn)? {
            return Err(LocalBridgeError::Exited(status));
        }
        match post_bootstrap(port, token, body).await {
            Ok(Some(response)) => return Ok(response),
            Ok(None) | Err(LocalBridgeError::BootstrapIo(_)) => {}
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            return Err(LocalBridgeError::BootstrapTimeout);
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Err(LocalBridgeError::Cancelled),
            _ = tokio::time::sleep(BRIDGE_BOOTSTRAP_RETRY_DELAY) => {}
        }
    }
}

async fn post_bootstrap(
    port: u16,
    token: &str,
    body: &[u8],
) -> Result<Option<BridgeBootstrapResponse>, LocalBridgeError> {
    let mut stream = match TcpStream::connect(("127.0.0.1", port)).await {
        Ok(stream) => stream,
        Err(error) => return Err(LocalBridgeError::BootstrapIo(error)),
    };
    let headers = format!(
        "POST /bootstrap HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream
        .write_all(headers.as_bytes())
        .await
        .map_err(LocalBridgeError::BootstrapIo)?;
    stream
        .write_all(body)
        .await
        .map_err(LocalBridgeError::BootstrapIo)?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(LocalBridgeError::BootstrapIo)?;
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(LocalBridgeError::InvalidHttpResponse)?;
    let status_line_end = response
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or(LocalBridgeError::InvalidHttpResponse)?;
    let status_line = std::str::from_utf8(&response[..status_line_end])
        .map_err(|_| LocalBridgeError::InvalidHttpResponse)?;
    let status = status_line
        .split_ascii_whitespace()
        .nth(1)
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or(LocalBridgeError::InvalidHttpResponse)?;
    if matches!(status, 409 | 503) {
        return Ok(None);
    }
    if status != 200 {
        return Err(LocalBridgeError::BootstrapStatus(status));
    }
    serde_json::from_slice(&response[header_end + 4..])
        .map(Some)
        .map_err(LocalBridgeError::InvalidBootstrapResponse)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::Duration as ProtoDuration;
    use temporalio_common::protos::temporal::api::{
        namespace::v1::{NamespaceInfo, namespace_info},
        workflowservice::v1::get_system_info_response,
    };

    fn supported_namespace() -> NamespaceInfo {
        NamespaceInfo {
            capabilities: Some(namespace_info::Capabilities {
                local_execution: true,
                ..Default::default()
            }),
            limits: Some(namespace_info::Limits {
                minimum_local_execution_sync_interval: Some(ProtoDuration {
                    seconds: 1,
                    nanos: 0,
                }),
                maximum_local_execution_sync_interval: Some(ProtoDuration {
                    seconds: 60,
                    nanos: 0,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn supported_system() -> get_system_info_response::Capabilities {
        get_system_info_response::Capabilities {
            local_execution: true,
            ..Default::default()
        }
    }

    #[test]
    fn local_execution_requires_both_capabilities_and_valid_interval() {
        let system = supported_system();
        let namespace = supported_namespace();
        assert!(supports_local_execution(
            Some(&system),
            &namespace,
            Duration::from_secs(1),
        ));
        assert!(supports_local_execution(
            Some(&system),
            &namespace,
            Duration::from_secs(60),
        ));
        assert!(!supports_local_execution(
            None,
            &namespace,
            Duration::from_secs(10),
        ));

        let mut namespace_without_capability = namespace.clone();
        namespace_without_capability
            .capabilities
            .as_mut()
            .unwrap()
            .local_execution = false;
        assert!(!supports_local_execution(
            Some(&system),
            &namespace_without_capability,
            Duration::from_secs(10),
        ));
        assert!(!supports_local_execution(
            Some(&system),
            &namespace,
            Duration::from_millis(999),
        ));
        assert!(!supports_local_execution(
            Some(&system),
            &namespace,
            Duration::from_secs(61),
        ));
    }

    #[test]
    fn local_execution_rejects_absent_or_invalid_interval_limits() {
        let system = supported_system();
        let mut namespace = supported_namespace();
        namespace.limits = None;
        assert!(!supports_local_execution(
            Some(&system),
            &namespace,
            Duration::from_secs(10),
        ));

        let mut namespace = supported_namespace();
        namespace
            .limits
            .as_mut()
            .unwrap()
            .minimum_local_execution_sync_interval = Some(ProtoDuration {
            seconds: -1,
            nanos: 0,
        });
        assert!(!supports_local_execution(
            Some(&system),
            &namespace,
            Duration::from_secs(10),
        ));
    }
}
