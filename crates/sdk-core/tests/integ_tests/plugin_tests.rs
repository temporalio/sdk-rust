use crate::common::{get_integ_server_options, get_integ_telem_options, integ_namespace};
use futures_util::future::BoxFuture;
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicU8, AtomicUsize,
            Ordering::{self, Relaxed},
        },
    },
    time::Duration,
};
use temporalio_client::{
    Client, ClientInterceptor, ClientOptions, ClientPlugin, ConnectionOptions, NamespacedClient,
    Next, PluginError, StartWorkflowInput, StartWorkflowOutput, WorkflowStartOptions,
    errors::WorkflowStartError,
};
use temporalio_common::{
    data_converters::{
        DataConverter, DefaultFailureConverter, PayloadCodec, PayloadConversionError,
        PayloadConverter, SerializationContextData,
    },
    protos::{
        coresdk::workflow_activation::WorkflowActivation, temporal::api::common::v1::Payload,
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ClientAndWorkerPlugin, Runtime, SimplePlugin, Worker, WorkerOptions,
    WorkerPlugin, WorkflowContext, WorkflowDefinitions, WorkflowResult,
    activities::{ActivityContext, ActivityDefinitions, ActivityError},
    interceptors::WorkerInterceptor,
    runtime::RuntimeOptions,
};
use url::Url;
use uuid::Uuid;

fn new_sdk_runtime() -> Runtime {
    Runtime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(get_integ_telem_options())
            .build()
            .unwrap(),
    )
    .unwrap()
}

#[derive(Clone)]
struct IntegrationPlugin {
    connection_calls: Arc<AtomicU8>,
    client_calls: Arc<AtomicU8>,
    worker_calls: Arc<AtomicU8>,
    target: Url,
}

impl ClientPlugin for IntegrationPlugin {
    fn name(&self) -> &str {
        "integration-plugin"
    }

    fn configure_connection_options(
        &self,
        options: &mut ConnectionOptions,
    ) -> Result<(), PluginError> {
        self.connection_calls.fetch_add(1, Relaxed);
        options.target = self.target.clone();
        options.identity = "integration-plugin-client".to_owned();
        Ok(())
    }

    fn configure_client_options(&self, options: &mut ClientOptions) -> Result<(), PluginError> {
        self.client_calls.fetch_add(1, Relaxed);
        options.namespace = integ_namespace();
        Ok(())
    }
}

impl WorkerPlugin for IntegrationPlugin {
    fn name(&self) -> &str {
        "integration-plugin"
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        self.worker_calls.fetch_add(1, Relaxed);
        options.max_cached_workflows = 0;
        Ok(())
    }
}

#[tokio::test]
async fn plugins_configure_client_and_worker() {
    let runtime = new_sdk_runtime();
    let connection_calls = Arc::new(AtomicU8::new(0));
    let client_calls = Arc::new(AtomicU8::new(0));
    let worker_calls = Arc::new(AtomicU8::new(0));
    let server_options = get_integ_server_options();
    let plugin = ClientAndWorkerPlugin::new(IntegrationPlugin {
        connection_calls: connection_calls.clone(),
        client_calls: client_calls.clone(),
        worker_calls: worker_calls.clone(),
        target: server_options.target,
    });
    let client_options = ClientOptions::new("plugin-replaces-this-namespace")
        .plugin(plugin)
        .build();
    let connection_options =
        ConnectionOptions::new(Url::parse("http://127.0.0.1:1").unwrap()).build();
    let client = Client::connect(connection_options, client_options)
        .await
        .unwrap();
    assert_eq!(client.connection().identity(), "integration-plugin-client");
    assert_eq!(client.namespace(), integ_namespace());
    let worker_options = WorkerOptions::new(format!("plugins-{}", Uuid::new_v4()))
        .register_workflow::<SimplePluginWorkflow>()
        .unwrap()
        .build();
    let _worker = Worker::new(&runtime, client, worker_options).unwrap();

    assert_eq!(connection_calls.load(Relaxed), 1);
    assert_eq!(client_calls.load(Relaxed), 1);
    assert_eq!(worker_calls.load(Relaxed), 1);
}

struct CountingPayloadCodec {
    encode_calls: Arc<AtomicUsize>,
    decode_calls: Arc<AtomicUsize>,
}

impl PayloadCodec for CountingPayloadCodec {
    fn encode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        self.encode_calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move { Ok(payloads) })
    }

    fn decode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        self.decode_calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move { Ok(payloads) })
    }
}

struct CountingClientInterceptor {
    calls: Arc<AtomicUsize>,
}

impl ClientInterceptor for CountingClientInterceptor {
    fn start_workflow<'a>(
        &'a self,
        input: StartWorkflowInput,
        next: Next<
            'a,
            StartWorkflowInput,
            BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>,
        >,
    ) -> BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        next.run(input)
    }
}

struct CountingWorkerInterceptor {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for CountingWorkerInterceptor {
    async fn on_workflow_activation(
        &self,
        _activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct SimplePluginWorkflow;

struct SimplePluginActivities;

#[activities]
impl SimplePluginActivities {
    #[activity]
    async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}

#[workflow_methods]
impl SimplePluginWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        Ok(ctx
            .execute_activity(
                SimplePluginActivities::greet,
                name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?)
    }
}

#[tokio::test]
async fn simple_plugin_configures_working_client_and_worker() {
    let encode_calls = Arc::new(AtomicUsize::new(0));
    let decode_calls = Arc::new(AtomicUsize::new(0));
    let client_interceptor_calls = Arc::new(AtomicUsize::new(0));
    let worker_interceptor_calls = Arc::new(AtomicUsize::new(0));
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter::default(),
        CountingPayloadCodec {
            encode_calls: encode_calls.clone(),
            decode_calls: decode_calls.clone(),
        },
    );
    let mut activities = ActivityDefinitions::default();
    activities.register_activities(SimplePluginActivities);
    let mut workflows = WorkflowDefinitions::new();
    workflows
        .register_workflow::<SimplePluginWorkflow>()
        .unwrap();
    let plugin = SimplePlugin::builder("simple-integration-plugin")
        .data_converter(data_converter)
        .client_interceptors(vec![Arc::new(CountingClientInterceptor {
            calls: client_interceptor_calls.clone(),
        }) as Arc<dyn ClientInterceptor>])
        .worker_interceptors(vec![Arc::new(CountingWorkerInterceptor {
            calls: worker_interceptor_calls.clone(),
        }) as Arc<dyn WorkerInterceptor>])
        .activities(activities)
        .workflows(workflows)
        .build();
    let client_options = ClientOptions::new(integ_namespace()).plugin(plugin).build();
    let client = Client::connect(get_integ_server_options(), client_options)
        .await
        .unwrap();
    let runtime = new_sdk_runtime();
    let task_queue = format!("simple-plugin-{}", Uuid::new_v4());
    let mut worker = Worker::new(
        &runtime,
        client.clone(),
        WorkerOptions::new(task_queue.clone()).build(),
    )
    .unwrap();
    let workflow_id = format!("simple-plugin-{}", Uuid::new_v4());
    let handle = client
        .start_workflow(
            SimplePluginWorkflow::run,
            "Temporal".to_owned(),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .unwrap();

    let shutdown = worker.shutdown_handle();
    let (workflow_result, worker_result) = tokio::join!(
        async {
            let result = handle.get_result(Default::default()).await;
            shutdown();
            result
        },
        worker.run(),
    );
    worker_result.unwrap();
    let workflow_result = workflow_result.unwrap();
    assert_eq!(workflow_result, "Hello, Temporal!");
    assert_eq!(client_interceptor_calls.load(Ordering::Relaxed), 1);
    assert!(worker_interceptor_calls.load(Ordering::Relaxed) > 0);
    assert!(encode_calls.load(Ordering::Relaxed) > 0);
    assert!(decode_calls.load(Ordering::Relaxed) > 0);
}

#[tokio::test]
async fn plugin_errors_surface() {
    let connection_result = Client::connect(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace())
            .client_plugin(FailingConnectionPlugin)
            .build(),
    )
    .await;
    assert_eq!(
        connection_result.unwrap_err().to_string(),
        "plugin 'failing-connection' failed to configure connection options: connection failure"
    );

    let client_result = Client::connect(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace())
            .client_plugin(FailingClientPlugin)
            .build(),
    )
    .await;
    assert_eq!(
        client_result.unwrap_err().to_string(),
        "plugin 'failing-client' failed to configure client options: client failure"
    );

    let client = Client::connect(
        get_integ_server_options(),
        ClientOptions::new(integ_namespace()).build(),
    )
    .await
    .unwrap();
    let worker_result = Worker::new(
        &new_sdk_runtime(),
        client,
        WorkerOptions::new(format!("failing-plugin-{}", Uuid::new_v4()))
            .worker_plugin(FailingWorkerPlugin)
            .build(),
    );
    assert_eq!(
        worker_result.unwrap_err().to_string(),
        "plugin 'failing-worker' failed to configure worker options: worker failure"
    );
}

struct FailingConnectionPlugin;

impl ClientPlugin for FailingConnectionPlugin {
    fn name(&self) -> &str {
        "failing-connection"
    }

    fn configure_connection_options(
        &self,
        _options: &mut ConnectionOptions,
    ) -> Result<(), PluginError> {
        Err(PluginError::new("connection failure"))
    }
}

struct FailingClientPlugin;

impl ClientPlugin for FailingClientPlugin {
    fn name(&self) -> &str {
        "failing-client"
    }

    fn configure_client_options(&self, _options: &mut ClientOptions) -> Result<(), PluginError> {
        Err(PluginError::new("client failure"))
    }
}

struct FailingWorkerPlugin;

impl WorkerPlugin for FailingWorkerPlugin {
    fn name(&self) -> &str {
        "failing-worker"
    }

    fn configure_worker_options(&self, _options: &mut WorkerOptions) -> Result<(), PluginError> {
        Err(PluginError::new("worker failure"))
    }
}
