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
use std::{any::Any, sync::Arc};
use temporalio_client::{
    ClientInterceptor, ClientOptions, ClientPlugin, ConnectionOptions, ErasedClientPlugin,
    PluginApplyError, PluginError, PluginTarget, WorkerPluginData,
};
use temporalio_common::{WorkflowDefinition, data_converters::DataConverter};
use temporalio_workflow::runtime::entry::WorkflowImplementation;

/// Configures worker options before the worker is created.
///
/// Use [`ClientAndWorkerPlugin`] for plugins that target both clients and workers.
///
/// **Experimental:** This API may change or be removed.
pub trait WorkerPlugin: Send + Sync + 'static {
    /// Name used to identify this plugin.
    fn name(&self) -> &str;

    /// Configure worker options.
    fn configure_worker_options(&self, _options: &mut WorkerOptions) -> Result<(), PluginError> {
        Ok(())
    }
}

/// A type-erased worker plugin.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub struct ErasedWorkerPlugin {
    worker: Arc<dyn WorkerPlugin>,
}

impl ErasedWorkerPlugin {
    /// Type-erase a worker plugin for registration on [`WorkerOptions`].
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn new<P: WorkerPlugin>(plugin: P) -> Self {
        Self {
            worker: Arc::new(plugin),
        }
    }

    pub(crate) fn plugin(&self) -> &dyn WorkerPlugin {
        self.worker.as_ref()
    }
}

#[derive(Clone)]
struct PropagatedWorkerPlugin(ErasedWorkerPlugin);

impl WorkerPluginData for PropagatedWorkerPlugin {}

/// A container for a plugin that implements both [`ClientPlugin`] and
/// [`WorkerPlugin`].
///
/// This wrapper is only needed when the same plugin configures both a client and workers. Use
/// [`ErasedClientPlugin`] or [`ErasedWorkerPlugin`] for one-sided plugins.
///
/// When registered on [`ClientOptions`], the resulting client carries the worker plugin and
/// automatically applies it to workers created from that client. Do not also register the plugin
/// on [`WorkerOptions`], or its worker configuration will be applied twice.
///
/// ```
/// # use temporalio_client::{ClientOptions, ClientPlugin};
/// # use temporalio_sdk::{ClientAndWorkerPlugin, WorkerPlugin};
/// # struct MyPlugin;
/// # impl ClientPlugin for MyPlugin {
/// #     fn name(&self) -> &str { "my-plugin" }
/// # }
/// # impl WorkerPlugin for MyPlugin {
/// #     fn name(&self) -> &str { "my-plugin" }
/// # }
/// let plugin = ClientAndWorkerPlugin::new(MyPlugin);
/// let client_options = ClientOptions::new("default").plugin(plugin).build();
/// # let _ = client_options;
/// ```
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub struct ClientAndWorkerPlugin {
    client: ErasedClientPlugin,
    worker: ErasedWorkerPlugin,
}

impl ClientAndWorkerPlugin {
    /// Type-erase one plugin value for client registration, automatic worker propagation, and
    /// optional explicit worker registration.
    pub fn new<P>(plugin: P) -> Self
    where
        P: ClientPlugin + WorkerPlugin,
    {
        let plugin = Arc::new(plugin);
        let mut client = ErasedClientPlugin::new(SharedClientPlugin(plugin.clone()));
        let worker = ErasedWorkerPlugin::new(SharedWorkerPlugin(plugin));
        client = client.with_worker_plugin(PropagatedWorkerPlugin(worker.clone()));
        Self { client, worker }
    }
}

impl From<ClientAndWorkerPlugin> for ErasedClientPlugin {
    fn from(plugin: ClientAndWorkerPlugin) -> Self {
        plugin.client
    }
}

impl From<ClientAndWorkerPlugin> for ErasedWorkerPlugin {
    fn from(plugin: ClientAndWorkerPlugin) -> Self {
        plugin.worker
    }
}

/// A plugin assembled from declarative values.
///
/// Scalar values replace the corresponding option, while array values append.
///
/// ```
/// use temporalio_client::ClientOptions;
/// use temporalio_common::data_converters::DataConverter;
/// use temporalio_sdk::SimplePlugin;
///
/// let plugin = SimplePlugin::builder("my-plugin")
///     .data_converter(DataConverter::default())
///     .build();
/// let client_options = ClientOptions::new("default").plugin(plugin).build();
/// # let _ = client_options;
/// ```
///
/// Worker configuration declared by this combined plugin is automatically propagated through the
/// configured client when a worker is created.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone, bon::Builder)]
#[builder(state_mod(vis = "pub"))]
pub struct SimplePlugin {
    #[builder(start_fn, into)]
    name: String,
    #[builder(field)]
    data_converter: Option<DataConverter>,
    #[builder(field)]
    client_interceptors: Vec<Arc<dyn ClientInterceptor>>,
    #[builder(field)]
    worker_interceptors: Vec<Arc<dyn WorkerInterceptor>>,
    #[builder(field)]
    activity_inbound_interceptors: Vec<Arc<dyn ActivityInboundInterceptor>>,
    #[builder(field)]
    workflow_interceptors: Vec<WorkflowInterceptorConstructor>,
    #[builder(field)]
    activities: ActivityDefinitions,
    #[builder(field)]
    workflows: WorkflowDefinitions,
    #[cfg(feature = "wasm-workflows")]
    #[builder(field)]
    wasm_workflow_components: Vec<WasmWorkflowComponent>,
}

impl From<SimplePlugin> for ErasedClientPlugin {
    fn from(plugin: SimplePlugin) -> Self {
        ClientAndWorkerPlugin::new(plugin).into()
    }
}

impl From<SimplePlugin> for ErasedWorkerPlugin {
    fn from(plugin: SimplePlugin) -> Self {
        ErasedWorkerPlugin::new(plugin)
    }
}

impl ClientPlugin for SimplePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn configure_client_options(&self, options: &mut ClientOptions) -> Result<(), PluginError> {
        if let Some(data_converter) = &self.data_converter {
            options.data_converter = data_converter.clone();
        }
        options
            .client_interceptors
            .extend(self.client_interceptors.iter().cloned());
        Ok(())
    }
}

impl WorkerPlugin for SimplePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
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
        Ok(())
    }
}

impl<S: simple_plugin_builder::State> SimplePluginBuilder<S> {
    /// Set the data converter installed on configured clients.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn data_converter(mut self, data_converter: DataConverter) -> Self {
        self.data_converter = Some(data_converter);
        self
    }

    /// Append a client interceptor.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn client_interceptor<I: ClientInterceptor>(mut self, interceptor: I) -> Self {
        self.client_interceptors.push(Arc::new(interceptor));
        self
    }

    /// Append a worker interceptor.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn worker_interceptor<I: WorkerInterceptor + 'static>(mut self, interceptor: I) -> Self {
        self.worker_interceptors.push(Arc::new(interceptor));
        self
    }

    /// Append an activity inbound interceptor.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn activity_inbound_interceptor<I: ActivityInboundInterceptor>(
        mut self,
        interceptor: I,
    ) -> Self {
        self.activity_inbound_interceptors
            .push(Arc::new(interceptor));
        self
    }

    /// Append a workflow interceptor constructor.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn workflow_interceptor(mut self, constructor: WorkflowInterceptorConstructor) -> Self {
        self.workflow_interceptors.push(constructor);
        self
    }

    /// Append every activity defined by an activity implementer.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn register_activities<AI: ActivityImplementer>(mut self, instance: AI) -> Self {
        self.activities.register_activities(instance);
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
        self.workflows.register_workflow::<W>()?;
        Ok(self)
    }

    /// Append a prebuilt WASM workflow component.
    ///
    /// **Experimental:** This API may change or be removed.
    #[cfg(feature = "wasm-workflows")]
    pub fn register_wasm_workflow(mut self, component: WasmWorkflowComponent) -> Self {
        self.wasm_workflow_components.push(component);
        self
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

    fn configure_connection_options(
        &self,
        options: &mut ConnectionOptions,
    ) -> Result<(), PluginError> {
        self.0.configure_connection_options(options)
    }

    fn configure_client_options(&self, options: &mut ClientOptions) -> Result<(), PluginError> {
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

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        self.0.configure_worker_options(options)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct WorkerPluginWarning<'a> {
    plugin_name: &'a str,
    message: &'static str,
}

fn worker_plugin_warnings(
    plugins: &[ErasedWorkerPlugin],
) -> impl Iterator<Item = WorkerPluginWarning<'_>> {
    plugins.iter().enumerate().filter_map(|(index, plugin)| {
        plugins[index + 1..]
            .iter()
            .any(|other| Arc::ptr_eq(&plugin.worker, &other.worker))
            .then_some(WorkerPluginWarning {
                plugin_name: plugin.plugin().name(),
                message: "The same combined client and worker plugin was registered both through the client and directly on the worker",
            })
    })
}

pub(crate) fn apply_worker_plugins(
    client_options: &ClientOptions,
    options: &mut WorkerOptions,
) -> Result<(), PluginApplyError> {
    if options.plugins_applied {
        return Ok(());
    }

    options.client_plugin_names = client_options
        .plugins()
        .iter()
        .map(|plugin| plugin.name().to_owned())
        .collect();
    let mut plugins = client_options
        .plugins()
        .iter()
        .flat_map(ErasedClientPlugin::worker_plugins)
        .filter_map(|plugin| (plugin as &dyn Any).downcast_ref::<PropagatedWorkerPlugin>())
        .map(|plugin| plugin.0.clone())
        .collect::<Vec<_>>();
    plugins.append(&mut options.worker_plugins);

    for warning in worker_plugin_warnings(&plugins) {
        warn!(plugin = warning.plugin_name, "{}", warning.message);
    }

    for registration in &plugins {
        registration
            .plugin()
            .configure_worker_options(options)
            .map_err(|source| {
                PluginApplyError::new(registration.plugin().name(), PluginTarget::Worker, source)
            })?;
    }
    options.worker_plugins = plugins;
    options.plugins_applied = true;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, sync::Mutex};
    use temporalio_client::ClientOptions;
    use temporalio_common::protos::temporal::api::worker::v1::PluginInfo;

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

        fn configure_worker_options(
            &self,
            _options: &mut WorkerOptions,
        ) -> Result<(), PluginError> {
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

        fn configure_worker_options(
            &self,
            _options: &mut WorkerOptions,
        ) -> Result<(), PluginError> {
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
            .worker_plugin(RecordingWorkerPlugin {
                name: "local",
                value: "local",
                order: order.clone(),
            })
            .build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(*order.lock().unwrap(), ["propagated", "local"]);
    }

    #[test]
    fn explicitly_reusing_a_combined_registration_warns_and_applies_both() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let combined = ClientAndWorkerPlugin::new(RecordingCombinedPlugin {
            order: order.clone(),
        });
        let client_options = ClientOptions::new("namespace")
            .plugin(combined.clone())
            .build();
        let mut worker_options = WorkerOptions::new("queue").plugin(combined).build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(
            worker_plugin_warnings(&worker_options.worker_plugins).collect::<Vec<_>>(),
            [WorkerPluginWarning {
                plugin_name: "combined",
                message: "The same combined client and worker plugin was registered both through the client and directly on the worker",
            }]
        );
        assert_eq!(*order.lock().unwrap(), ["propagated", "propagated"]);
    }

    struct ClientOnlyPlugin;

    impl ClientPlugin for ClientOnlyPlugin {
        fn name(&self) -> &str {
            "client-only"
        }
    }

    struct SameNameClientPlugin;

    impl ClientPlugin for SameNameClientPlugin {
        fn name(&self) -> &str {
            "same-name"
        }
    }

    struct UnrecognizedWorkerPluginExtension {
        _registration: ErasedWorkerPlugin,
    }

    impl WorkerPluginData for UnrecognizedWorkerPluginExtension {}

    #[test]
    fn arbitrary_opaque_data_cannot_impersonate_propagated_plugin() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let client_registration = ErasedClientPlugin::new(ClientOnlyPlugin).with_worker_plugin(
            UnrecognizedWorkerPluginExtension {
                _registration: ErasedWorkerPlugin::new(RecordingWorkerPlugin {
                    name: "unrecognized",
                    value: "should-not-run",
                    order: order.clone(),
                }),
            },
        );
        let client_options = ClientOptions::new("namespace")
            .plugin(client_registration)
            .build();
        let mut worker_options = WorkerOptions::new("queue").build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert!(order.lock().unwrap().is_empty());
    }

    #[test]
    fn heartbeat_plugin_names_include_client_plugins_and_are_deduplicated() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let client_options = ClientOptions::new("namespace")
            .client_plugin(ClientOnlyPlugin)
            .client_plugin(SameNameClientPlugin)
            .build();
        let mut worker_options = WorkerOptions::new("queue")
            .worker_plugin(RecordingWorkerPlugin {
                name: "same-name",
                value: "first",
                order: order.clone(),
            })
            .worker_plugin(RecordingWorkerPlugin {
                name: "same-name",
                value: "second",
                order,
            })
            .build();
        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(
            worker_plugin_warnings(&worker_options.worker_plugins).count(),
            0
        );
        let core_options = worker_options
            .to_core_options("namespace".to_owned(), "identity".to_owned())
            .unwrap();

        let expected_plugins: HashSet<_> = vec![
            PluginInfo {
                name: "client-only".into(),
                version: "".into(),
            },
            PluginInfo {
                name: "same-name".into(),
                version: "".into(),
            },
        ]
        .into_iter()
        .collect();
        assert_eq!(core_options.plugins, expected_plugins);
    }

    struct EmptyClientInterceptor;

    impl ClientInterceptor for EmptyClientInterceptor {}

    struct EmptyWorkerInterceptor;

    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for EmptyWorkerInterceptor {}

    #[test]
    fn simple_plugin_applies_declarative_values() {
        let plugin = SimplePlugin::builder("simple")
            .client_interceptor(EmptyClientInterceptor)
            .build();
        let mut client_options = ClientOptions::new("namespace").build();
        plugin
            .configure_client_options(&mut client_options)
            .unwrap();
        assert_eq!(client_options.client_interceptors.len(), 1);

        let plugin = SimplePlugin::builder("simple")
            .worker_interceptor(EmptyWorkerInterceptor)
            .build();
        let client_options = ClientOptions::new("namespace").build();
        let mut worker_options = WorkerOptions::new("queue").plugin(plugin).build();
        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(worker_options.worker_interceptors.len(), 1);
    }
}
