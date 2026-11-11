//! Conversion from [`temporalio_common::envconfig::ClientConfigProfile`] to [`ConnectionOptions`] and [`ClientOptions`].
//!
//! This module bridges the environment/file-based configuration in `temporalio-common` with
//! the client connection types.
//!
//! ## Extensions
//!
//! With the `experimental` feature, separately packaged extensions can add their own
//! configuration and client behavior by implementing `ClientConfigExtension`. Each extension owns
//! a TOML table under `ext` in each profile:
//!
//! ```toml
//! [profile.default.ext.my_extension]
//! some_field = "value"
//! ```
//!
//! Pass extensions to `ClientOptions::load_from_config_with_extensions`. Each is called in order
//! with the loaded profile and environment, and any client plugin it returns is registered on the
//! returned `ClientOptions`. Pass the returned options to `Client::connect`; building a
//! `Connection` yourself and calling `Client::new` skips plugins' connection hooks:
//!
//! ```rust,ignore
//! let (conn_options, client_options) = ClientOptions::load_from_config_with_extensions(
//!     LoadClientConfigProfileOptions::default(),
//!     &[&MyExtension],
//! )?;
//! let client = Client::connect(conn_options, client_options).await?;
//! ```
//!
//! Extensions are recommended to use a `TEMPORAL_EXT_<NAME>_` prefix for their environment
//! variables. See the `ClientConfigExtension` docs for a complete example.

use std::{collections::HashMap, fs};
use url::Url;

pub use temporalio_common::envconfig::{
    ClientConfigProfile, ConfigError, DataSource, LoadClientConfigProfileOptions, toml,
};
use temporalio_common::envconfig::{ClientConfigTLS, load_client_config_profile};

#[cfg(feature = "experimental")]
use crate::ErasedClientPlugin;
use crate::{ClientOptions, ClientTlsOptions, ConnectionOptions, TlsOptions};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

impl ClientOptions {
    /// Load client and connection options from environment variables and/or a TOML config file.
    pub fn load_from_config(
        options: LoadClientConfigProfileOptions,
    ) -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
        load_from_config_with_env(options, None)
    }

    /// Load client and connection options like [`ClientOptions::load_from_config`], then apply
    /// `extensions`.
    ///
    /// Each extension is called in order with the loaded profile, and any plugin it returns is
    /// registered on the returned [`ClientOptions`]. Extension names must be non-empty and unique.
    /// Errors returned by an extension are wrapped in a [`ClientConfigExtensionError`].
    ///
    /// Pass the returned options to [`Client::connect`](crate::Client::connect) so that plugin
    /// connection option hooks are applied. [`Client::new`](crate::Client::new) only applies
    /// client option hooks.
    ///
    /// **Experimental:** This API may change or be removed.
    #[cfg(feature = "experimental")]
    pub fn load_from_config_with_extensions(
        options: LoadClientConfigProfileOptions,
        extensions: &[&dyn ClientConfigExtension],
    ) -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
        load_from_config_with_extensions_and_env(options, extensions, None)
    }
}

/// Reads extension config from a loaded profile and the environment, and returns a client plugin
/// that applies it. Passed to [`ClientOptions::load_from_config_with_extensions`].
///
/// An extension's TOML config lives in the `profile.<profile>.ext.<name>` table, available as
/// `profile.ext[name]`. Extensions are recommended to use a `TEMPORAL_EXT_<NAME>_` prefix for their
/// environment variables.
///
/// **Experimental:** This API may change or be removed.
#[cfg(feature = "experimental")]
pub trait ClientConfigExtension: Send + Sync {
    /// The extension's table key under `profile.<profile>.ext`. Must be non-empty and unique among
    /// loaded extensions.
    fn name(&self) -> &str;

    /// Read this extension's config from the profile and env, and return a plugin that applies it,
    /// or `None` if there's nothing to apply.
    fn client_plugin(
        &self,
        req: ClientConfigExtensionRequest<'_>,
    ) -> Result<Option<ErasedClientPlugin>, ConfigError>;
}

/// Input to [`ClientConfigExtension::client_plugin`].
///
/// **Experimental:** This API may change or be removed.
#[cfg(feature = "experimental")]
#[derive(Debug, Clone, Copy, bon::Builder)]
#[non_exhaustive]
pub struct ClientConfigExtensionRequest<'a> {
    /// Profile that was loaded, after environment variables have been applied (unless disabled).
    pub profile: &'a ClientConfigProfile,
    /// Environment variables to read extension env vars from, or `None` if env loading is
    /// disabled. This is the env override map if one was given, otherwise the process
    /// environment.
    pub env: Option<ClientConfigEnv<'a>>,
}

/// Environment variables available to a [`ClientConfigExtension`], backed by either an override
/// map or the process environment. Process environment variables are read when requested rather
/// than copied up front.
///
/// **Experimental:** This API may change or be removed.
#[cfg(feature = "experimental")]
#[derive(Debug, Clone, Copy)]
pub struct ClientConfigEnv<'a>(EnvSource<'a>);

#[cfg(feature = "experimental")]
#[derive(Debug, Clone, Copy)]
enum EnvSource<'a> {
    Map(&'a HashMap<String, String>),
    Process,
}

#[cfg(feature = "experimental")]
impl<'a> ClientConfigEnv<'a> {
    /// Environment backed by the given map.
    pub fn from_map(map: &'a HashMap<String, String>) -> Self {
        Self(EnvSource::Map(map))
    }

    /// Environment backed by the process environment.
    pub fn process() -> Self {
        Self(EnvSource::Process)
    }

    /// Get a single environment variable, or `None` if it isn't set. Fails if a process
    /// environment variable's value isn't valid UTF-8.
    pub fn get(&self, key: &str) -> Result<Option<String>, ConfigError> {
        match self.0 {
            EnvSource::Map(map) => Ok(map.get(key).cloned()),
            EnvSource::Process => match std::env::var(key) {
                Ok(v) => Ok(Some(v)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(e) => Err(e.into()),
            },
        }
    }

    /// Iterate over all environment variables, e.g. to find those with a given prefix. Process
    /// environment variables whose name or value isn't valid UTF-8 are skipped.
    pub fn vars(&self) -> Box<dyn Iterator<Item = (String, String)> + 'a> {
        match self.0 {
            EnvSource::Map(map) => Box::new(map.iter().map(|(k, v)| (k.clone(), v.clone()))),
            EnvSource::Process => Box::new(
                std::env::vars_os()
                    .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?))),
            ),
        }
    }
}

/// An error returned by a [`ClientConfigExtension`], wrapped in [`ConfigError::LoadError`].
///
/// **Experimental:** This API may change or be removed.
#[cfg(feature = "experimental")]
#[derive(Debug, thiserror::Error)]
#[error("client config extension '{extension_name}': {source}")]
#[non_exhaustive]
pub struct ClientConfigExtensionError {
    /// Name of the extension that failed.
    pub extension_name: String,
    /// The error returned by the extension.
    #[source]
    pub source: ConfigError,
}

#[cfg(feature = "experimental")]
fn load_from_config_with_extensions_and_env(
    options: LoadClientConfigProfileOptions,
    extensions: &[&dyn ClientConfigExtension],
    env_vars: Option<&HashMap<String, String>>,
) -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
    let mut seen = std::collections::HashSet::with_capacity(extensions.len());
    for (i, ext) in extensions.iter().enumerate() {
        let name = ext.name();
        if name.is_empty() {
            return Err(ConfigError::InvalidConfig(format!(
                "client config extension at index {i} has empty name"
            )));
        }
        if !seen.insert(name) {
            return Err(ConfigError::InvalidConfig(format!(
                "duplicate client config extension name '{name}'"
            )));
        }
    }

    let disable_env = options.disable_env;
    let profile = load_client_config_profile(options, env_vars)?;
    let namespace = profile
        .namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_NAMESPACE.to_owned());
    // Convert before calling extensions so invalid core config fails before any extension work,
    // which may be expensive (e.g. fetching secrets).
    let conn_opts = ConnectionOptions::try_from(profile.clone())?;

    let env = match (disable_env, env_vars) {
        (true, _) => None,
        (false, Some(env_vars)) => Some(ClientConfigEnv::from_map(env_vars)),
        (false, None) => Some(ClientConfigEnv::process()),
    };
    let req = ClientConfigExtensionRequest {
        profile: &profile,
        env,
    };
    let mut plugins = Vec::new();
    for ext in extensions {
        let plugin = ext.client_plugin(req).map_err(|source| {
            ConfigError::LoadError(Box::new(ClientConfigExtensionError {
                extension_name: ext.name().to_owned(),
                source,
            }))
        })?;
        plugins.extend(plugin);
    }

    let client_opts = ClientOptions::new(namespace).plugins(plugins).build();
    Ok((conn_opts, client_opts))
}

// Separate function allows injecting env vars for testing.
fn load_from_config_with_env(
    options: LoadClientConfigProfileOptions,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
    let profile = load_client_config_profile(options, env_vars)?;
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
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "unsupported envconfig data source",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use temporalio_common::envconfig::{ClientConfigTLS, DataSource};

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

    #[cfg(feature = "experimental")]
    mod extensions {
        use super::*;
        use crate::{ClientPlugin, ErasedClientPlugin};
        use std::sync::Mutex;

        type ClientPluginResult = Result<Option<ErasedClientPlugin>, ConfigError>;

        struct TestExtension<F> {
            name: &'static str,
            client_plugin: F,
        }

        impl<F> ClientConfigExtension for TestExtension<F>
        where
            F: Fn(ClientConfigExtensionRequest<'_>) -> ClientPluginResult + Send + Sync,
        {
            fn name(&self) -> &str {
                self.name
            }

            fn client_plugin(&self, req: ClientConfigExtensionRequest<'_>) -> ClientPluginResult {
                (self.client_plugin)(req)
            }
        }

        // Closures in a `TestExtension { .. }` literal would be inferred to take a request with one
        // specific lifetime, rather than any lifetime as the trait impl requires. Passing them
        // through a function with the `Fn` bound gives them the right signature where they're
        // written.
        fn test_ext<F>(name: &'static str, client_plugin: F) -> TestExtension<F>
        where
            F: Fn(ClientConfigExtensionRequest<'_>) -> ClientPluginResult + Send + Sync,
        {
            TestExtension {
                name,
                client_plugin,
            }
        }

        fn plugin_ext(name: &'static str) -> impl ClientConfigExtension {
            test_ext(name, move |_| {
                Ok(Some(ErasedClientPlugin::new(NamedPlugin(name))))
            })
        }

        struct NamedPlugin(&'static str);

        impl ClientPlugin for NamedPlugin {
            fn name(&self) -> &str {
                self.0
            }
        }

        fn env_only_options() -> LoadClientConfigProfileOptions {
            LoadClientConfigProfileOptions::builder()
                .disable_file(true)
                .build()
        }

        #[test]
        fn plugins_appended_in_order() {
            let skipped = test_ext("skipped", |_| Ok(None));
            let env = HashMap::new();
            let (_, client) = load_from_config_with_extensions_and_env(
                env_only_options(),
                &[&plugin_ext("first"), &skipped, &plugin_ext("second")],
                Some(&env),
            )
            .unwrap();
            let names: Vec<_> = client.plugins().iter().map(|p| p.name()).collect();
            assert_eq!(names, ["first", "second"]);
        }

        #[rstest]
        #[case::duplicate(&["foo", "bar", "foo"], "duplicate client config extension name 'foo'")]
        #[case::empty(&["foo", ""], "client config extension at index 1 has empty name")]
        fn invalid_names(#[case] names: &[&'static str], #[case] expected: &str) {
            let called = Mutex::new(false);
            let exts: Vec<_> = names
                .iter()
                .map(|&name| {
                    test_ext(name, |_| {
                        *called.lock().unwrap() = true;
                        Ok(None)
                    })
                })
                .collect();
            let ext_refs: Vec<&dyn ClientConfigExtension> = exts
                .iter()
                .map(|e| e as &dyn ClientConfigExtension)
                .collect();
            let env = HashMap::new();
            let err =
                load_from_config_with_extensions_and_env(env_only_options(), &ext_refs, Some(&env))
                    .unwrap_err();
            assert!(matches!(&err, ConfigError::InvalidConfig(msg) if msg == expected));
            assert!(!*called.lock().unwrap());
        }

        #[test]
        fn extension_error_wrapped_with_name() {
            let after_called = Mutex::new(false);
            let failing = test_ext("failing", |_| {
                Err(ConfigError::InvalidConfig("bad field".to_owned()))
            });
            let after = test_ext("after", |_| {
                *after_called.lock().unwrap() = true;
                Ok(None)
            });
            let env = HashMap::new();
            let err = load_from_config_with_extensions_and_env(
                env_only_options(),
                &[&failing, &after],
                Some(&env),
            )
            .unwrap_err();
            let ConfigError::LoadError(err) = err else {
                panic!("unexpected error: {err}");
            };
            let err = err.downcast_ref::<ClientConfigExtensionError>().unwrap();
            assert_eq!(err.extension_name, "failing");
            assert!(matches!(&err.source, ConfigError::InvalidConfig(msg) if msg == "bad field"));
            assert_eq!(
                err.to_string(),
                "client config extension 'failing': Invalid configuration: bad field"
            );
            assert!(!*after_called.lock().unwrap());
        }

        #[rstest]
        fn extension_receives_profile_and_env(config_dir: TempDir) {
            let config_path = write_config(
                &config_dir,
                r#"
[profile.default]
namespace = "toml-ns"

[profile.default.ext.foo]
some_field = "toml-value"
"#,
            );
            let seen = Mutex::new(None);
            let ext = test_ext("foo", |req| {
                let env = req.env.unwrap();
                *seen.lock().unwrap() = Some((
                    req.profile.clone(),
                    env.get("TEMPORAL_EXT_FOO_SOME_FIELD").unwrap(),
                    env.vars().collect::<HashMap<_, _>>(),
                ));
                Ok(None)
            });
            let env = HashMap::from([
                ("TEMPORAL_NAMESPACE".to_owned(), "env-ns".to_owned()),
                (
                    "TEMPORAL_EXT_FOO_SOME_FIELD".to_owned(),
                    "env-value".to_owned(),
                ),
            ]);
            let opts = LoadClientConfigProfileOptions::builder()
                .config_source(DataSource::Path(config_path.to_str().unwrap().to_owned()))
                .build();
            load_from_config_with_extensions_and_env(opts, &[&ext], Some(&env)).unwrap();

            let (profile, ext_value, ext_vars) = seen.into_inner().unwrap().unwrap();
            // Core env vars are applied to the profile before extensions see it
            assert_eq!(profile.namespace.as_deref(), Some("env-ns"));
            assert_eq!(
                profile.ext["foo"]["some_field"].as_str(),
                Some("toml-value")
            );
            assert_eq!(ext_value.as_deref(), Some("env-value"));
            assert_eq!(ext_vars, env);
        }

        #[test]
        fn extension_env_from_process_or_disabled() {
            let saw_env = Mutex::new(None);
            let ext = test_ext("foo", |req| {
                *saw_env.lock().unwrap() = Some(req.env.map(|env| env.get("PATH").unwrap()));
                Ok(None)
            });

            ClientOptions::load_from_config_with_extensions(env_only_options(), &[&ext]).unwrap();
            assert_eq!(
                *saw_env.lock().unwrap(),
                Some(Some(std::env::var("PATH").ok()))
            );

            let opts = LoadClientConfigProfileOptions::builder()
                .config_source(DataSource::Data(Vec::new()))
                .disable_env(true)
                .build();
            ClientOptions::load_from_config_with_extensions(opts, &[&ext]).unwrap();
            assert_eq!(*saw_env.lock().unwrap(), Some(None));
        }
    }
}
