//! Conversion from [`temporalio_common::envconfig::ClientConfigProfile`] to [`ConnectionOptions`] and [`ClientOptions`].
//!
//! This module bridges the environment/file-based configuration in `temporalio-common` with
//! the client connection types.

use std::{collections::HashMap, fs};
use url::Url;

pub use temporalio_common::envconfig::ConfigError;
use temporalio_common::envconfig::{
    self, ClientConfig as CoreClientConfig, ClientConfigCodec as CoreClientConfigCodec,
    ClientConfigProfile as CoreClientConfigProfile, ClientConfigTLS as CoreClientConfigTLS,
    DataSource as CoreDataSource,
    LoadClientConfigProfileOptions as CoreLoadClientConfigProfileOptions,
};

use crate::{ClientOptions, ClientTlsOptions, ConnectionOptions, TlsOptions};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// A source for configuration or TLS certificate/key data.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum DataSource {
    /// A filesystem path to the data.
    Path(String),
    /// The raw data bytes.
    Data(Vec<u8>),
}

impl From<DataSource> for CoreDataSource {
    fn from(value: DataSource) -> Self {
        match value {
            DataSource::Path(path) => Self::Path(path),
            DataSource::Data(data) => Self::Data(data),
        }
    }
}

impl From<CoreDataSource> for DataSource {
    fn from(value: CoreDataSource) -> Self {
        match value {
            CoreDataSource::Path(path) => Self::Path(path),
            CoreDataSource::Data(data) => Self::Data(data),
        }
    }
}

/// A client configuration file.
#[derive(Debug, Clone, PartialEq, Default, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct ClientConfig {
    /// Profiles, keyed by profile name.
    #[builder(default)]
    pub profiles: HashMap<String, ClientConfigProfile>,
}

impl From<ClientConfig> for CoreClientConfig {
    fn from(value: ClientConfig) -> Self {
        Self {
            profiles: value
                .profiles
                .into_iter()
                .map(|(name, profile)| (name, profile.into()))
                .collect(),
        }
    }
}

impl From<CoreClientConfig> for ClientConfig {
    fn from(value: CoreClientConfig) -> Self {
        Self {
            profiles: value
                .profiles
                .into_iter()
                .map(|(name, profile)| (name, profile.into()))
                .collect(),
        }
    }
}

/// A client configuration profile.
#[derive(Debug, Clone, PartialEq, Default, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct ClientConfigProfile {
    /// Client address.
    pub address: Option<String>,
    /// Client namespace.
    pub namespace: Option<String>,
    /// Client API key.
    pub api_key: Option<String>,
    /// Optional client TLS configuration.
    pub tls: Option<ClientConfigTLS>,
    /// Optional client codec configuration.
    pub codec: Option<ClientConfigCodec>,
    /// Client gRPC metadata headers.
    #[builder(default)]
    pub grpc_meta: HashMap<String, String>,
}

impl From<ClientConfigProfile> for CoreClientConfigProfile {
    fn from(value: ClientConfigProfile) -> Self {
        Self {
            address: value.address,
            namespace: value.namespace,
            api_key: value.api_key,
            tls: value.tls.map(Into::into),
            codec: value.codec.map(Into::into),
            grpc_meta: value.grpc_meta,
        }
    }
}

impl From<CoreClientConfigProfile> for ClientConfigProfile {
    fn from(value: CoreClientConfigProfile) -> Self {
        Self {
            address: value.address,
            namespace: value.namespace,
            api_key: value.api_key,
            tls: value.tls.map(Into::into),
            codec: value.codec.map(Into::into),
            grpc_meta: value.grpc_meta,
        }
    }
}

/// TLS configuration for a client profile.
#[derive(Debug, Clone, PartialEq, Default, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct ClientConfigTLS {
    /// Whether TLS is explicitly disabled or enabled.
    pub disabled: Option<bool>,
    /// Client certificate source.
    pub client_cert: Option<DataSource>,
    /// Client key source.
    pub client_key: Option<DataSource>,
    /// Server CA certificate source.
    pub server_ca_cert: Option<DataSource>,
    /// SNI override.
    pub server_name: Option<String>,
    /// Whether host verification should be skipped.
    #[builder(default)]
    pub disable_host_verification: bool,
}

impl From<ClientConfigTLS> for CoreClientConfigTLS {
    fn from(value: ClientConfigTLS) -> Self {
        Self {
            disabled: value.disabled,
            client_cert: value.client_cert.map(Into::into),
            client_key: value.client_key.map(Into::into),
            server_ca_cert: value.server_ca_cert.map(Into::into),
            server_name: value.server_name,
            disable_host_verification: value.disable_host_verification,
        }
    }
}

impl From<CoreClientConfigTLS> for ClientConfigTLS {
    fn from(value: CoreClientConfigTLS) -> Self {
        Self {
            disabled: value.disabled,
            client_cert: value.client_cert.map(Into::into),
            client_key: value.client_key.map(Into::into),
            server_ca_cert: value.server_ca_cert.map(Into::into),
            server_name: value.server_name,
            disable_host_verification: value.disable_host_verification,
        }
    }
}

/// Remote codec configuration for a client profile.
#[derive(Debug, Clone, PartialEq, Default, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct ClientConfigCodec {
    /// Remote endpoint for the codec.
    pub endpoint: Option<String>,
    /// Authorization header for the codec.
    pub auth: Option<String>,
}

impl From<ClientConfigCodec> for CoreClientConfigCodec {
    fn from(value: ClientConfigCodec) -> Self {
        Self {
            endpoint: value.endpoint,
            auth: value.auth,
        }
    }
}

impl From<CoreClientConfigCodec> for ClientConfigCodec {
    fn from(value: CoreClientConfigCodec) -> Self {
        Self {
            endpoint: value.endpoint,
            auth: value.auth,
        }
    }
}

/// Options for loading a client configuration profile.
#[derive(Debug, PartialEq, bon::Builder)]
#[non_exhaustive]
pub struct LoadClientConfigProfileOptions {
    /// Where to load configuration from. If unset, the loader checks environment variables and
    /// the default configuration path.
    pub config_source: Option<DataSource>,
    /// Specific profile to use.
    pub config_file_profile: Option<String>,
    /// Whether to reject unrecognized configuration file keys.
    #[builder(default)]
    pub config_file_strict: bool,
    /// Whether to skip configuration-file loading.
    #[builder(default)]
    pub disable_file: bool,
    /// Whether to skip environment-variable loading.
    #[builder(default)]
    pub disable_env: bool,
}

impl Default for LoadClientConfigProfileOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl From<LoadClientConfigProfileOptions> for CoreLoadClientConfigProfileOptions {
    fn from(value: LoadClientConfigProfileOptions) -> Self {
        Self::builder()
            .maybe_config_source(value.config_source.map(Into::into))
            .maybe_config_file_profile(value.config_file_profile)
            .config_file_strict(value.config_file_strict)
            .disable_file(value.disable_file)
            .disable_env(value.disable_env)
            .build()
    }
}

impl ClientOptions {
    /// Load client and connection options from environment variables and/or a TOML config file.
    pub fn load_from_config(
        options: LoadClientConfigProfileOptions,
    ) -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
        load_from_config_with_env(options, None)
    }
}

// Separate function allows injecting env vars for testing.
fn load_from_config_with_env(
    options: LoadClientConfigProfileOptions,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
    let profile: ClientConfigProfile =
        envconfig::load_client_config_profile(options.into(), env_vars)?.into();
    let namespace = profile
        .namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_NAMESPACE.to_owned());
    let conn_opts = ConnectionOptions::try_from(profile)?;
    let client_opts = ClientOptions::new(namespace).build();
    Ok((conn_opts, client_opts))
}

/// Parse an address string into a [`Url`], prepending a scheme if none is present.
///
/// Other SDKs pass addresses as bare `host:port` strings. Our [`ConnectionOptions`] requires a
/// [`Url`], so we attempt a direct parse first and fall back to prepending a scheme.
/// When the user omits a scheme, we use `https://` if TLS will be enabled, otherwise `http://`.
fn parse_address(address: &str, use_tls: bool) -> Result<Url, ConfigError> {
    // Try parsing as-is. `Url::parse("localhost:7233")` "succeeds" by treating `localhost` as
    // the scheme, so reject parses that have no host — those need a scheme prefix.
    if let Ok(url) = Url::parse(address)
        && url.host().is_some()
    {
        return Ok(url);
    }
    let scheme = if use_tls { "https" } else { "http" };
    Url::parse(&format!("{scheme}://{address}"))
        .map_err(|e| ConfigError::InvalidConfig(format!("Invalid address: {e}")))
}

/// Build [`TlsOptions`] from a [`ClientConfigTLS`] config, resolving any file-based data sources.
fn build_tls_options(tls: ClientConfigTLS) -> Result<TlsOptions, ConfigError> {
    let client_tls_options = match (tls.client_cert, tls.client_key) {
        (Some(cert), Some(key)) => {
            let cert_bytes =
                resolve_datasource(cert).map_err(|e| ConfigError::LoadError(e.into()))?;
            let key_bytes =
                resolve_datasource(key).map_err(|e| ConfigError::LoadError(e.into()))?;
            Some(
                ClientTlsOptions::builder()
                    .client_cert(cert_bytes)
                    .client_private_key(key_bytes)
                    .build(),
            )
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(ConfigError::InvalidConfig(
                "Both client certificate and client key must be provided together".to_string(),
            ));
        }
        (None, None) => None,
    };

    let server_root_ca_cert = tls
        .server_ca_cert
        .map(resolve_datasource)
        .transpose()
        .map_err(|e| ConfigError::LoadError(e.into()))?;

    Ok(TlsOptions::builder()
        .maybe_server_root_ca_cert(server_root_ca_cert)
        .maybe_domain(tls.server_name)
        .maybe_client_tls_options(client_tls_options)
        .build())
}

/// Determine whether TLS should be enabled based on the profile's TLS config and API key.
///
/// TLS is enabled when:
/// - There is a TLS section that is not explicitly disabled, OR
/// - An API key is set and TLS is not explicitly disabled
fn should_enable_tls(tls: &Option<ClientConfigTLS>, has_api_key: bool) -> bool {
    match tls {
        Some(t) => t.disabled != Some(true),
        None => has_api_key,
    }
}

impl TryFrom<ClientConfigProfile> for ConnectionOptions {
    type Error = ConfigError;

    fn try_from(profile: ClientConfigProfile) -> Result<Self, Self::Error> {
        let ClientConfigProfile {
            address,
            namespace: _,
            api_key,
            tls,
            codec: _,
            grpc_meta,
            ..
        } = profile;

        let has_api_key = api_key.is_some();
        let use_tls = should_enable_tls(&tls, has_api_key);
        let target = parse_address(address.as_deref().unwrap_or(DEFAULT_ADDRESS), use_tls)?;

        let tls_options = if use_tls {
            match tls {
                Some(tls_cfg) => Some(build_tls_options(tls_cfg)?),
                None => Some(TlsOptions::default()),
            }
        } else {
            None
        };

        let headers = (!grpc_meta.is_empty()).then_some(grpc_meta);

        Ok(ConnectionOptions::new(target)
            .maybe_api_key(api_key)
            .maybe_tls_options(tls_options)
            .maybe_headers(headers)
            .build())
    }
}

/// Resolve a data source to its raw bytes.
fn resolve_datasource(data_source: DataSource) -> Result<Vec<u8>, std::io::Error> {
    match data_source {
        DataSource::Path(path) => fs::read(path),
        DataSource::Data(data) => Ok(data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Write a TOML config file into a temp directory and return (dir, path).
    /// The `TempDir` handle keeps the directory alive; it is cleaned up on drop.
    #[fixture]
    fn config_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    /// Write `content` to `temporal.toml` inside `dir`, returning the file path.
    fn write_config(dir: &TempDir, content: &str) -> PathBuf {
        let path = dir.path().join("temporal.toml");
        std::fs::write(&path, content).unwrap();
        path
    }

    #[rstest]
    #[case::default(None, false, "http://localhost:7233/")]
    #[case::with_scheme(Some("https://my-server:7233"), false, "https://my-server:7233/")]
    #[case::without_scheme(Some("localhost:7233"), false, "http://localhost:7233/")]
    #[case::without_scheme_tls(Some("localhost:7233"), true, "https://localhost:7233/")]
    #[case::explicit_http_with_tls(Some("http://my-server:7233"), true, "http://my-server:7233/")]
    fn address_parsing(
        #[case] address: Option<&str>,
        #[case] enable_tls: bool,
        #[case] expected: &str,
    ) {
        let tls = enable_tls.then(ClientConfigTLS::default);
        let profile = ClientConfigProfile::builder()
            .maybe_address(address.map(str::to_string))
            .maybe_tls(tls)
            .build();
        let conn: ConnectionOptions = profile.try_into().unwrap();
        assert_eq!(conn.target.as_str(), expected);
    }

    #[test]
    fn invalid_address_errors() {
        let profile = ClientConfigProfile::builder().address("://bad").build();
        assert!(ConnectionOptions::try_from(profile).is_err());
    }

    #[test]
    fn empty_profile_defaults() {
        let env = HashMap::new();
        let opts = LoadClientConfigProfileOptions::builder()
            .disable_file(true)
            .build();
        let (conn, client) = load_from_config_with_env(opts, Some(&env)).unwrap();

        assert_eq!(conn.target.as_str(), "http://localhost:7233/");
        assert_eq!(client.namespace, "default");
        assert!(conn.tls_options.is_none());
        assert!(conn.headers.is_none());
        assert!(conn.api_key.is_none());
    }

    #[test]
    fn namespace_override() {
        let mut env = HashMap::new();
        env.insert("TEMPORAL_NAMESPACE".to_string(), "my-namespace".to_string());
        let opts = LoadClientConfigProfileOptions::builder()
            .disable_file(true)
            .build();
        let (_, client) = load_from_config_with_env(opts, Some(&env)).unwrap();
        assert_eq!(client.namespace, "my-namespace");
    }

    #[test]
    fn grpc_metadata_passthrough() {
        let mut meta = HashMap::new();
        meta.insert("x-custom".to_string(), "value".to_string());
        meta.insert("another".to_string(), "header".to_string());
        let profile = ClientConfigProfile::builder()
            .grpc_meta(meta.clone())
            .build();
        let conn: ConnectionOptions = profile.try_into().unwrap();
        assert_eq!(conn.headers.unwrap(), meta);
    }

    #[test]
    fn api_key_populates_field() {
        let profile = ClientConfigProfile::builder().api_key("my-key").build();
        let conn: ConnectionOptions = profile.try_into().unwrap();
        assert_eq!(conn.api_key.as_deref(), Some("my-key"));
    }

    #[rstest]
    #[case::no_tls_no_key(None, None, false)]
    #[case::no_tls_with_key(None, Some("key"), true)]
    #[case::tls_disabled_false(Some(Some(false)), None, true)]
    #[case::tls_disabled_true(Some(Some(true)), None, false)]
    #[case::tls_disabled_none(Some(None), None, true)]
    #[case::key_with_tls_disabled(Some(Some(true)), Some("key"), false)]
    #[case::key_with_tls_enabled(Some(Some(false)), Some("key"), true)]
    fn tls_enablement(
        #[case] tls_disabled: Option<Option<bool>>,
        #[case] api_key: Option<&str>,
        #[case] expect_tls: bool,
    ) {
        let profile = ClientConfigProfile::builder()
            .maybe_api_key(api_key.map(str::to_string))
            .maybe_tls(
                tls_disabled
                    .map(|disabled| ClientConfigTLS::builder().maybe_disabled(disabled).build()),
            )
            .build();
        let conn: ConnectionOptions = profile.try_into().unwrap();
        assert_eq!(conn.tls_options.is_some(), expect_tls);
    }

    #[test]
    fn data_source_certs() {
        let profile = ClientConfigProfile::builder()
            .tls(
                ClientConfigTLS::builder()
                    .client_cert(DataSource::Data(b"cert-data".to_vec()))
                    .client_key(DataSource::Data(b"key-data".to_vec()))
                    .build(),
            )
            .build();
        let conn: ConnectionOptions = profile.try_into().unwrap();
        let tls = conn.tls_options.unwrap();
        let mtls = tls.client_tls_options.unwrap();
        assert_eq!(mtls.client_cert, b"cert-data");
        assert_eq!(mtls.client_private_key, b"key-data");
    }

    #[rstest]
    fn path_source_certs(config_dir: TempDir) {
        let cert_path = config_dir.path().join("cert.pem");
        let key_path = config_dir.path().join("key.pem");
        std::fs::write(&cert_path, b"file-cert").unwrap();
        std::fs::write(&key_path, b"file-key").unwrap();

        let profile = ClientConfigProfile::builder()
            .tls(
                ClientConfigTLS::builder()
                    .client_cert(DataSource::Path(cert_path.to_str().unwrap().to_string()))
                    .client_key(DataSource::Path(key_path.to_str().unwrap().to_string()))
                    .build(),
            )
            .build();
        let conn: ConnectionOptions = profile.try_into().unwrap();
        let tls = conn.tls_options.unwrap();
        let mtls = tls.client_tls_options.unwrap();
        assert_eq!(mtls.client_cert, b"file-cert");
        assert_eq!(mtls.client_private_key, b"file-key");
    }

    #[test]
    fn server_ca_cert() {
        let profile = ClientConfigProfile::builder()
            .tls(
                ClientConfigTLS::builder()
                    .server_ca_cert(DataSource::Data(b"ca-data".to_vec()))
                    .build(),
            )
            .build();
        let conn: ConnectionOptions = profile.try_into().unwrap();
        let tls = conn.tls_options.unwrap();
        assert_eq!(tls.server_root_ca_cert.unwrap(), b"ca-data");
    }

    #[test]
    fn server_name_sni() {
        let profile = ClientConfigProfile::builder()
            .tls(
                ClientConfigTLS::builder()
                    .server_name("my.server.com")
                    .build(),
            )
            .build();
        let conn: ConnectionOptions = profile.try_into().unwrap();
        let tls = conn.tls_options.unwrap();
        assert_eq!(tls.domain.as_deref(), Some("my.server.com"));
    }

    #[rstest]
    #[case::cert_without_key(Some(DataSource::Data(b"cert".to_vec())), None)]
    #[case::key_without_cert(None, Some(DataSource::Data(b"key".to_vec())))]
    fn partial_tls_errors(
        #[case] client_cert: Option<DataSource>,
        #[case] client_key: Option<DataSource>,
    ) {
        let profile = ClientConfigProfile::builder()
            .tls(
                ClientConfigTLS::builder()
                    .maybe_client_cert(client_cert)
                    .maybe_client_key(client_key)
                    .build(),
            )
            .build();
        assert!(ConnectionOptions::try_from(profile).is_err());
    }

    #[test]
    fn config_wrappers_convert_to_and_from_common() {
        let config = ClientConfig::builder()
            .profiles(HashMap::from([(
                "default".to_string(),
                ClientConfigProfile::builder()
                    .address("localhost:7233")
                    .tls(
                        ClientConfigTLS::builder()
                            .client_cert(DataSource::Data(b"cert".to_vec()))
                            .client_key(DataSource::Data(b"key".to_vec()))
                            .build(),
                    )
                    .codec(
                        ClientConfigCodec::builder()
                            .endpoint("http://localhost:8080")
                            .auth("Bearer token")
                            .build(),
                    )
                    .build(),
            )]))
            .build();

        let common: CoreClientConfig = config.clone().into();
        assert_eq!(ClientConfig::from(common), config);
    }

    #[rstest]
    fn load_from_config_from_toml(config_dir: TempDir) {
        let config_path = write_config(
            &config_dir,
            r#"
[profile.default]
address = "toml-server:7233"
namespace = "toml-ns"
api_key = "toml-key"

[profile.default.grpc_meta]
x-custom = "value"

[profile.custom]
address = "custom-server:9090"
namespace = "custom-ns"
"#,
        );

        // Default profile
        let opts = LoadClientConfigProfileOptions::builder()
            .config_source(DataSource::Path(config_path.to_str().unwrap().to_string()))
            .disable_env(true)
            .build();
        let (conn, client) = ClientOptions::load_from_config(opts).unwrap();
        assert_eq!(conn.target.as_str(), "https://toml-server:7233/");
        assert_eq!(client.namespace, "toml-ns");
        assert_eq!(conn.api_key.as_deref(), Some("toml-key"));
        assert!(conn.tls_options.is_some());
        assert_eq!(
            conn.headers.as_ref().unwrap().get("x-custom").unwrap(),
            "value"
        );

        // Custom profile
        let opts = LoadClientConfigProfileOptions::builder()
            .config_source(DataSource::Path(config_path.to_str().unwrap().to_string()))
            .config_file_profile("custom".to_string())
            .disable_env(true)
            .build();
        let (conn, client) = ClientOptions::load_from_config(opts).unwrap();
        assert_eq!(conn.target.as_str(), "http://custom-server:9090/");
        assert_eq!(client.namespace, "custom-ns");
    }
}
