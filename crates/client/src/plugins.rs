//! Experimental client plugin APIs.

use crate::{ClientOptions, ConnectionOptions};
use std::{any::Any, error::Error, fmt, sync::Arc};

/// The result type returned by plugin configuration hooks.
///
/// **Experimental:** This API may change or be removed.
pub type PluginResult<T = ()> = Result<T, PluginError>;

/// An error returned by a plugin configuration hook.
///
/// **Experimental:** This API may change or be removed.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct PluginError(Box<dyn Error + Send + Sync + 'static>);

impl PluginError {
    /// Wrap an error returned by a plugin.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn new(error: impl Error + Send + Sync + 'static) -> Self {
        Self(Box::new(error))
    }

    /// Create a plugin error from a message.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn message(message: impl Into<String>) -> Self {
        Self(Box::new(PluginMessageError(message.into())))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct PluginMessageError(String);

/// The configuration target being modified when a plugin failed.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PluginTarget {
    /// Connection options.
    Connection,
    /// Namespace-bound client options.
    Client,
    /// Worker options.
    Worker,
}

impl fmt::Display for PluginTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection => f.write_str("connection options"),
            Self::Client => f.write_str("client options"),
            Self::Worker => f.write_str("worker options"),
        }
    }
}

/// An error applying a named plugin to a configuration target.
///
/// **Experimental:** This API may change or be removed.
#[derive(Debug, thiserror::Error)]
#[error("plugin '{plugin_name}' failed to configure {target}: {source}")]
pub struct PluginApplyError {
    /// The plugin name reported by [`ClientPlugin::name`] or its worker equivalent.
    pub plugin_name: String,
    /// The configuration target being modified.
    pub target: PluginTarget,
    /// The error returned by the plugin.
    #[source]
    pub source: PluginError,
}

impl PluginApplyError {
    pub(crate) fn new(
        plugin_name: impl Into<String>,
        target: PluginTarget,
        source: PluginError,
    ) -> Self {
        Self {
            plugin_name: plugin_name.into(),
            target,
            source,
        }
    }
}

/// Configures connection and namespace-bound client options.
///
/// **Experimental:** This API may change or be removed.
pub trait ClientPlugin: Send + Sync + 'static {
    /// Return the stable name used to identify this plugin in diagnostics and worker heartbeats.
    ///
    /// **Experimental:** This API may change or be removed.
    fn name(&self) -> &str;

    /// Configure options before the connection is established.
    ///
    /// **Experimental:** This API may change or be removed.
    fn configure_connection_options(&self, _options: &mut ConnectionOptions) -> PluginResult {
        Ok(())
    }

    /// Configure options before the namespace-bound client is created.
    ///
    /// **Experimental:** This API may change or be removed.
    fn configure_client_options(&self, _options: &mut ClientOptions) -> PluginResult {
        Ok(())
    }
}

/// A type-erased client plugin and worker-plugin propagation data.
///
/// The worker data is intentionally opaque so SDK layers can carry their own registration type
/// through the client crate without creating a dependency from the client to an SDK.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub struct ClientPluginRegistration {
    pub(crate) client: Arc<dyn ClientPlugin>,
    worker_plugins: Vec<Arc<dyn Any + Send + Sync>>,
    pub(crate) instance_id: Arc<()>,
}

impl ClientPluginRegistration {
    /// Type-erase a client plugin for registration on [`ClientOptions`].
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn new<P: ClientPlugin>(plugin: P) -> Self {
        Self {
            client: Arc::new(plugin),
            worker_plugins: Vec::new(),
            instance_id: Arc::new(()),
        }
    }

    /// Attach opaque worker-plugin propagation data.
    ///
    /// This is intended for SDK integrations that define their own worker plugin trait. Values
    /// with types unknown to an SDK are ignored.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn with_worker_plugin<T: Any + Send + Sync>(mut self, plugin: T) -> Self {
        self.worker_plugins.push(Arc::new(plugin));
        self
    }

    /// Iterate over opaque worker-plugin propagation data.
    ///
    /// This is intended for SDK integrations that recognize their own private registration type.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn worker_plugins(&self) -> impl Iterator<Item = &(dyn Any + Send + Sync)> {
        self.worker_plugins.iter().map(AsRef::as_ref)
    }

    /// Return this registration's identity token.
    ///
    /// This is intended for SDK integrations that need to recognize duplicate use of a combined
    /// client and worker plugin registration.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn instance_id(&self) -> Arc<()> {
        self.instance_id.clone()
    }

    pub(crate) fn plugin(&self) -> &dyn ClientPlugin {
        self.client.as_ref()
    }
}

pub(crate) fn apply_connection_plugins(
    client_options: &ClientOptions,
    connection_options: &mut ConnectionOptions,
) -> Result<(), PluginApplyError> {
    if client_options.plugins_applied() {
        return Ok(());
    }
    for registration in client_options.plugins() {
        registration
            .plugin()
            .configure_connection_options(connection_options)
            .map_err(|source| {
                PluginApplyError::new(
                    registration.plugin().name(),
                    PluginTarget::Connection,
                    source,
                )
            })?;
    }
    Ok(())
}

pub(crate) fn apply_client_plugins(options: &mut ClientOptions) -> Result<(), PluginApplyError> {
    if options.plugins_applied() {
        return Ok(());
    }
    let plugins = options.plugins().to_vec();
    for registration in plugins {
        registration
            .plugin()
            .configure_client_options(options)
            .map_err(|source| {
                PluginApplyError::new(registration.plugin().name(), PluginTarget::Client, source)
            })?;
    }
    options.mark_plugins_applied();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use url::Url;

    struct CountingPlugin {
        connection_calls: Arc<AtomicUsize>,
        client_calls: Arc<AtomicUsize>,
    }

    impl ClientPlugin for CountingPlugin {
        fn name(&self) -> &str {
            "counting"
        }

        fn configure_connection_options(&self, options: &mut ConnectionOptions) -> PluginResult {
            self.connection_calls.fetch_add(1, Ordering::Relaxed);
            options.identity.push_str("-configured");
            Ok(())
        }

        fn configure_client_options(&self, options: &mut ClientOptions) -> PluginResult {
            self.client_calls.fetch_add(1, Ordering::Relaxed);
            options.namespace.push_str("-configured");
            Ok(())
        }
    }

    #[test]
    fn plugins_apply_in_each_phase_once() {
        let connection_calls = Arc::new(AtomicUsize::new(0));
        let client_calls = Arc::new(AtomicUsize::new(0));
        let mut client_options = ClientOptions::new("namespace")
            .client_plugin(CountingPlugin {
                connection_calls: connection_calls.clone(),
                client_calls: client_calls.clone(),
            })
            .build();
        let mut connection_options =
            ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
                .identity("identity")
                .build();

        apply_connection_plugins(&client_options, &mut connection_options).unwrap();
        apply_client_plugins(&mut client_options).unwrap();
        apply_connection_plugins(&client_options, &mut connection_options).unwrap();
        apply_client_plugins(&mut client_options).unwrap();

        assert_eq!(connection_calls.load(Ordering::Relaxed), 1);
        assert_eq!(client_calls.load(Ordering::Relaxed), 1);
        assert_eq!(connection_options.identity, "identity-configured");
        assert_eq!(client_options.namespace, "namespace-configured");
    }

    struct FailingPlugin;

    impl ClientPlugin for FailingPlugin {
        fn name(&self) -> &str {
            "failing"
        }

        fn configure_client_options(&self, _options: &mut ClientOptions) -> PluginResult {
            Err(PluginError::message("expected failure"))
        }
    }

    #[test]
    fn application_errors_include_name_target_and_source() {
        let mut options = ClientOptions::new("namespace")
            .client_plugin(FailingPlugin)
            .build();
        let error = apply_client_plugins(&mut options).unwrap_err();

        assert_eq!(error.plugin_name, "failing");
        assert_eq!(error.target, PluginTarget::Client);
        assert_eq!(error.source.to_string(), "expected failure");
    }

    #[test]
    fn worker_plugin_data_is_opaque_and_ordered() {
        let registration = ClientPluginRegistration::new(FailingPlugin)
            .with_worker_plugin(1usize)
            .with_worker_plugin("second");
        let values = registration.worker_plugins().collect::<Vec<_>>();

        assert_eq!(values[0].downcast_ref::<usize>(), Some(&1));
        assert_eq!(values[1].downcast_ref::<&str>(), Some(&"second"));
    }
}
