//! Experimental plugin APIs for configuring workers and clients from reusable values.

#[cfg(feature = "wasm-workflows")]
use crate::WasmWorkflowComponent;
use crate::{
    WorkerOptions, WorkflowRegistrationError,
    activities::{ActivityDefinitions, ActivityImplementer},
    interceptors::{ActivityInboundInterceptor, WorkerInterceptor},
    workflow_interceptors::WorkflowInterceptorConstructor,
    workflow_registry::WorkflowDefinitions,
};
use std::sync::Arc;
use temporalio_client::{
    ClientInterceptor, ClientOptions, ClientPlugin, ClientPluginRegistration, ConnectionOptions,
    PluginApplyError, PluginError, PluginResult, PluginTarget,
};
use temporalio_common::{WorkflowDefinition, data_converters::DataConverter};
use temporalio_workflow::runtime::entry::WorkflowImplementation;

/// Configures worker options before the underlying Core worker is created.
///
/// **Experimental:** This API may change or be removed.
pub trait WorkerPlugin: Send + Sync + 'static {
    /// Return the stable name used to identify this plugin in diagnostics and worker heartbeats.
    ///
    /// **Experimental:** This API may change or be removed.
    fn name(&self) -> &str;

    /// Configure worker options.
    ///
    /// **Experimental:** This API may change or be removed.
    fn configure_worker_options(&self, _options: &mut WorkerOptions) -> PluginResult {
        Ok(())
    }
}

/// A type-erased worker plugin with an identity used to diagnose duplicate registration.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub struct WorkerPluginRegistration {
    worker: Arc<dyn WorkerPlugin>,
    instance_id: Arc<()>,
}

impl WorkerPluginRegistration {
    /// Type-erase a worker plugin for registration on [`WorkerOptions`].
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn new<P: WorkerPlugin>(plugin: P) -> Self {
        Self {
            worker: Arc::new(plugin),
            instance_id: Arc::new(()),
        }
    }

    fn with_instance_id<P: WorkerPlugin>(plugin: P, instance_id: Arc<()>) -> Self {
        Self {
            worker: Arc::new(plugin),
            instance_id,
        }
    }

    pub(crate) fn plugin(&self) -> &dyn WorkerPlugin {
        self.worker.as_ref()
    }
}

#[derive(Clone)]
struct PropagatedWorkerPlugin(WorkerPluginRegistration);

/// A type-erased registration for one value that implements both [`ClientPlugin`] and
/// [`WorkerPlugin`].
///
/// This wrapper is only needed when the same plugin configures both a client and its workers. Use
/// [`ClientPluginRegistration`] or [`WorkerPluginRegistration`] for one-sided plugins.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub struct ClientAndWorkerPlugin {
    client: ClientPluginRegistration,
    worker: WorkerPluginRegistration,
}

impl ClientAndWorkerPlugin {
    /// Type-erase one plugin value for client registration, automatic worker propagation, and
    /// optional explicit worker registration.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn new<P>(plugin: P) -> Self
    where
        P: ClientPlugin + WorkerPlugin,
    {
        let plugin = Arc::new(plugin);
        let mut client = ClientPluginRegistration::new(SharedClientPlugin(plugin.clone()));
        let worker = WorkerPluginRegistration::with_instance_id(
            SharedWorkerPlugin(plugin),
            client.instance_id(),
        );
        client = client.with_worker_plugin(PropagatedWorkerPlugin(worker.clone()));
        Self { client, worker }
    }
}

impl From<ClientAndWorkerPlugin> for ClientPluginRegistration {
    fn from(plugin: ClientAndWorkerPlugin) -> Self {
        plugin.client
    }
}

impl From<ClientAndWorkerPlugin> for WorkerPluginRegistration {
    fn from(plugin: ClientAndWorkerPlugin) -> Self {
        plugin.worker
    }
}

/// A plugin assembled from declarative values and optional fallible configuration callbacks.
///
/// Scalar values replace the corresponding option, while interceptors and definitions append in
/// registration order. Declarative values are applied before configuration callbacks.
///
/// ```
/// use temporalio_client::ClientOptions;
/// use temporalio_sdk::SimplePlugin;
///
/// let plugin = SimplePlugin::new("acme.standard-library")
///     .configure_worker_options(|options| {
///         options.max_cached_workflows = 0;
///         Ok(())
///     })
///     .build();
/// let client_options = ClientOptions::new("default").plugin(plugin).build();
/// # let _ = client_options;
/// ```
///
/// Worker configuration declared by this combined plugin is automatically propagated through the
/// configured client when a worker is created.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub struct SimplePlugin {
    combined: ClientAndWorkerPlugin,
}

impl SimplePlugin {
    /// Begin declaring a simple plugin with the name used in diagnostics and worker heartbeats.
    ///
    /// **Experimental:** This API may change or be removed.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(name: impl Into<String>) -> SimplePluginBuilder {
        SimplePluginBuilder {
            definition: SimplePluginDefinition {
                name: name.into(),
                ..Default::default()
            },
        }
    }
}

impl From<SimplePlugin> for ClientPluginRegistration {
    fn from(plugin: SimplePlugin) -> Self {
        plugin.combined.into()
    }
}

impl From<SimplePlugin> for WorkerPluginRegistration {
    fn from(plugin: SimplePlugin) -> Self {
        plugin.combined.into()
    }
}

type ConnectionCustomizer =
    Arc<dyn Fn(&mut ConnectionOptions) -> PluginResult + Send + Sync + 'static>;
type ClientCustomizer = Arc<dyn Fn(&mut ClientOptions) -> PluginResult + Send + Sync + 'static>;
type WorkerCustomizer = Arc<dyn Fn(&mut WorkerOptions) -> PluginResult + Send + Sync + 'static>;

#[derive(Default)]
struct SimplePluginDefinition {
    name: String,
    data_converter: Option<DataConverter>,
    client_interceptors: Vec<Arc<dyn ClientInterceptor>>,
    worker_interceptors: Vec<Arc<dyn WorkerInterceptor>>,
    activity_inbound_interceptors: Vec<Arc<dyn ActivityInboundInterceptor>>,
    workflow_interceptors: Vec<WorkflowInterceptorConstructor>,
    activities: ActivityDefinitions,
    workflows: WorkflowDefinitions,
    #[cfg(feature = "wasm-workflows")]
    wasm_workflow_components: Vec<WasmWorkflowComponent>,
    connection_customizers: Vec<ConnectionCustomizer>,
    client_customizers: Vec<ClientCustomizer>,
    worker_customizers: Vec<WorkerCustomizer>,
}

impl ClientPlugin for SimplePluginDefinition {
    fn name(&self) -> &str {
        &self.name
    }

    fn configure_connection_options(&self, options: &mut ConnectionOptions) -> PluginResult {
        for configure in &self.connection_customizers {
            configure(options)?;
        }
        Ok(())
    }

    fn configure_client_options(&self, options: &mut ClientOptions) -> PluginResult {
        if let Some(data_converter) = &self.data_converter {
            options.data_converter = data_converter.clone();
        }
        options
            .client_interceptors
            .extend(self.client_interceptors.iter().cloned());
        for configure in &self.client_customizers {
            configure(options)?;
        }
        Ok(())
    }
}

impl WorkerPlugin for SimplePluginDefinition {
    fn name(&self) -> &str {
        &self.name
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> PluginResult {
        options.activities.extend(&self.activities);
        options
            .workflows
            .extend(&self.workflows)
            .map_err(PluginError::new)?;
        options
            .worker_interceptors
            .extend(self.worker_interceptors.iter().cloned());
        options
            .activity_inbound_interceptors
            .extend(self.activity_inbound_interceptors.iter().cloned());
        options
            .workflow_interceptor_constructors
            .extend(self.workflow_interceptors.iter().cloned());
        #[cfg(feature = "wasm-workflows")]
        options
            .wasm_workflow_components
            .extend(self.wasm_workflow_components.iter().cloned());
        for configure in &self.worker_customizers {
            configure(options)?;
        }
        Ok(())
    }
}

/// Builds a [`SimplePlugin`] from common SDK configuration values.
///
/// **Experimental:** This API may change or be removed.
pub struct SimplePluginBuilder {
    definition: SimplePluginDefinition,
}

impl SimplePluginBuilder {
    /// Set the data converter installed on configured clients.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn data_converter(mut self, data_converter: DataConverter) -> Self {
        self.definition.data_converter = Some(data_converter);
        self
    }

    /// Append a client interceptor.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn client_interceptor<I: ClientInterceptor>(mut self, interceptor: I) -> Self {
        self.definition
            .client_interceptors
            .push(Arc::new(interceptor));
        self
    }

    /// Append a worker interceptor.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn worker_interceptor<I: WorkerInterceptor + 'static>(mut self, interceptor: I) -> Self {
        self.definition
            .worker_interceptors
            .push(Arc::new(interceptor));
        self
    }

    /// Append an activity inbound interceptor.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn activity_inbound_interceptor<I: ActivityInboundInterceptor>(
        mut self,
        interceptor: I,
    ) -> Self {
        self.definition
            .activity_inbound_interceptors
            .push(Arc::new(interceptor));
        self
    }

    /// Append a workflow interceptor constructor.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn workflow_interceptor(mut self, constructor: WorkflowInterceptorConstructor) -> Self {
        self.definition.workflow_interceptors.push(constructor);
        self
    }

    /// Append every activity defined by an activity implementer.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn register_activities<AI: ActivityImplementer>(mut self, instance: AI) -> Self {
        self.definition.activities.register_activities(instance);
        self
    }

    /// Append a workflow definition.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn register_workflow<W>(mut self) -> Result<Self, WorkflowRegistrationError>
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
    {
        self.definition.workflows.register_workflow::<W>()?;
        Ok(self)
    }

    /// Append a prebuilt WASM workflow component.
    ///
    /// **Experimental:** This API may change or be removed.
    #[cfg(feature = "wasm-workflows")]
    pub fn register_wasm_workflow(mut self, component: WasmWorkflowComponent) -> Self {
        self.definition.wasm_workflow_components.push(component);
        self
    }

    /// Append a fallible connection-options callback.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn configure_connection_options<F>(mut self, configure: F) -> Self
    where
        F: Fn(&mut ConnectionOptions) -> PluginResult + Send + Sync + 'static,
    {
        self.definition
            .connection_customizers
            .push(Arc::new(configure));
        self
    }

    /// Append a fallible client-options callback.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn configure_client_options<F>(mut self, configure: F) -> Self
    where
        F: Fn(&mut ClientOptions) -> PluginResult + Send + Sync + 'static,
    {
        self.definition.client_customizers.push(Arc::new(configure));
        self
    }

    /// Append a fallible worker-options callback.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn configure_worker_options<F>(mut self, configure: F) -> Self
    where
        F: Fn(&mut WorkerOptions) -> PluginResult + Send + Sync + 'static,
    {
        self.definition.worker_customizers.push(Arc::new(configure));
        self
    }

    /// Build the reusable plugin registration.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn build(self) -> SimplePlugin {
        SimplePlugin {
            combined: ClientAndWorkerPlugin::new(self.definition),
        }
    }
}

struct SharedClientPlugin<P>(Arc<P>);

impl<P> ClientPlugin for SharedClientPlugin<P>
where
    P: ClientPlugin,
{
    fn name(&self) -> &str {
        ClientPlugin::name(self.0.as_ref())
    }

    fn configure_connection_options(&self, options: &mut ConnectionOptions) -> PluginResult {
        self.0.configure_connection_options(options)
    }

    fn configure_client_options(&self, options: &mut ClientOptions) -> PluginResult {
        self.0.configure_client_options(options)
    }
}

struct SharedWorkerPlugin<P>(Arc<P>);

impl<P> WorkerPlugin for SharedWorkerPlugin<P>
where
    P: ClientPlugin + WorkerPlugin,
{
    fn name(&self) -> &str {
        ClientPlugin::name(self.0.as_ref())
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> PluginResult {
        self.0.configure_worker_options(options)
    }
}

pub(crate) fn apply_worker_plugins(
    client_options: &ClientOptions,
    options: &mut WorkerOptions,
) -> Result<(), PluginApplyError> {
    if options.plugins_applied {
        return Ok(());
    }

    let mut plugins = client_options
        .plugins()
        .iter()
        .flat_map(ClientPluginRegistration::worker_plugins)
        .filter_map(|plugin| plugin.downcast_ref::<PropagatedWorkerPlugin>())
        .map(|plugin| plugin.0.clone())
        .collect::<Vec<_>>();
    plugins.append(&mut options.worker_plugins);

    for (index, plugin) in plugins.iter().enumerate() {
        if plugins[index + 1..]
            .iter()
            .any(|other| Arc::ptr_eq(&plugin.instance_id, &other.instance_id))
        {
            warn!(
                plugin = plugin.plugin().name(),
                "The same combined client and worker plugin was registered both through the client and directly on the worker"
            );
        }
    }

    for registration in &plugins {
        registration
            .plugin()
            .configure_worker_options(options)
            .map_err(|source| PluginApplyError {
                plugin_name: registration.plugin().name().to_owned(),
                target: PluginTarget::Worker,
                source,
            })?;
    }
    options.worker_plugins = plugins;
    options.plugins_applied = true;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use temporalio_client::ClientOptions;

    struct RecordingCombinedPlugin {
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    impl ClientPlugin for RecordingCombinedPlugin {
        fn name(&self) -> &str {
            "combined"
        }
    }

    impl WorkerPlugin for RecordingCombinedPlugin {
        fn name(&self) -> &str {
            "combined"
        }

        fn configure_worker_options(&self, _options: &mut WorkerOptions) -> PluginResult {
            self.order.lock().unwrap().push("propagated");
            Ok(())
        }
    }

    struct RecordingWorkerPlugin {
        name: &'static str,
        value: &'static str,
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    impl WorkerPlugin for RecordingWorkerPlugin {
        fn name(&self) -> &str {
            self.name
        }

        fn configure_worker_options(&self, _options: &mut WorkerOptions) -> PluginResult {
            self.order.lock().unwrap().push(self.value);
            Ok(())
        }
    }

    #[test]
    fn propagated_plugins_run_before_local_plugins() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let combined = ClientAndWorkerPlugin::new(RecordingCombinedPlugin {
            order: order.clone(),
        });
        let client_options = ClientOptions::new("namespace").plugin(combined).build();
        let mut worker_options = WorkerOptions::new("queue")
            .worker_plugin(WorkerPluginRegistration::new(RecordingWorkerPlugin {
                name: "local",
                value: "local",
                order: order.clone(),
            }))
            .build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(*order.lock().unwrap(), ["propagated", "local"]);
    }

    #[test]
    fn explicitly_reusing_a_combined_registration_applies_both_registrations() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let combined = ClientAndWorkerPlugin::new(RecordingCombinedPlugin {
            order: order.clone(),
        });
        let client_registration: ClientPluginRegistration = combined.clone().into();
        let worker_registration: WorkerPluginRegistration = combined.into();
        let propagated_registration = client_registration
            .worker_plugins()
            .find_map(|plugin| plugin.downcast_ref::<PropagatedWorkerPlugin>())
            .unwrap();
        assert!(Arc::ptr_eq(
            &propagated_registration.0.instance_id,
            &worker_registration.instance_id
        ));
        let client_options = ClientOptions::new("namespace")
            .plugin(client_registration)
            .build();
        let mut worker_options = WorkerOptions::new("queue")
            .worker_plugin(worker_registration)
            .build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(*order.lock().unwrap(), ["propagated", "propagated"]);
    }

    struct ClientOnlyPlugin;

    impl ClientPlugin for ClientOnlyPlugin {
        fn name(&self) -> &str {
            "client-only"
        }
    }

    #[test]
    fn arbitrary_opaque_data_cannot_impersonate_propagated_plugin() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let client_registration = ClientPluginRegistration::new(ClientOnlyPlugin)
            .with_worker_plugin(WorkerPluginRegistration::new(RecordingWorkerPlugin {
                name: "unrecognized",
                value: "should-not-run",
                order: order.clone(),
            }));
        let client_options = ClientOptions::new("namespace")
            .plugin(client_registration)
            .build();
        let mut worker_options = WorkerOptions::new("queue").build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert!(order.lock().unwrap().is_empty());
    }

    #[test]
    fn heartbeat_plugin_names_are_deduplicated() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let client_options = ClientOptions::new("namespace").build();
        let mut worker_options = WorkerOptions::new("queue")
            .worker_plugins([
                WorkerPluginRegistration::new(RecordingWorkerPlugin {
                    name: "same-name",
                    value: "first",
                    order: order.clone(),
                }),
                WorkerPluginRegistration::new(RecordingWorkerPlugin {
                    name: "same-name",
                    value: "second",
                    order,
                }),
            ])
            .build();
        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        let core_options = worker_options
            .to_core_options("namespace".to_owned(), "identity".to_owned())
            .unwrap();

        assert_eq!(core_options.plugins.len(), 1);
        let plugin = core_options.plugins.iter().next().unwrap();
        assert_eq!(plugin.name, "same-name");
        assert!(plugin.version.is_empty());
    }

    struct EmptyClientInterceptor;

    impl ClientInterceptor for EmptyClientInterceptor {}

    struct EmptyWorkerInterceptor;

    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for EmptyWorkerInterceptor {}

    #[test]
    fn simple_plugin_applies_declarative_values_before_customizers() {
        let client_builder = SimplePlugin::new("simple")
            .client_interceptor(EmptyClientInterceptor)
            .configure_client_options(|options| {
                assert_eq!(options.client_interceptors.len(), 1);
                options.namespace.push_str("-customized");
                Ok(())
            });
        let mut client_options = ClientOptions::new("namespace").build();
        client_builder
            .definition
            .configure_client_options(&mut client_options)
            .unwrap();
        assert_eq!(client_options.namespace, "namespace-customized");

        let plugin = SimplePlugin::new("simple")
            .worker_interceptor(EmptyWorkerInterceptor)
            .configure_worker_options(|options| {
                assert_eq!(options.worker_interceptors.len(), 1);
                options.max_cached_workflows = 0;
                Ok(())
            })
            .build();
        let client_options = ClientOptions::new("namespace").build();
        let mut worker_options = WorkerOptions::new("queue").worker_plugin(plugin).build();
        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(worker_options.max_cached_workflows, 0);
        assert_eq!(worker_options.worker_interceptors.len(), 1);
    }

    struct FailingWorkerPlugin;

    impl WorkerPlugin for FailingWorkerPlugin {
        fn name(&self) -> &str {
            "failing-worker"
        }

        fn configure_worker_options(&self, _options: &mut WorkerOptions) -> PluginResult {
            Err(PluginError::message("worker failure"))
        }
    }

    #[test]
    fn worker_application_errors_include_context() {
        let client_options = ClientOptions::new("namespace").build();
        let mut worker_options = WorkerOptions::new("queue")
            .worker_plugin(WorkerPluginRegistration::new(FailingWorkerPlugin))
            .build();

        let error = apply_worker_plugins(&client_options, &mut worker_options).unwrap_err();

        assert_eq!(error.plugin_name, "failing-worker");
        assert_eq!(error.target, PluginTarget::Worker);
        assert_eq!(error.source.to_string(), "worker failure");
    }
}
