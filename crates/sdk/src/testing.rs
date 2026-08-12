//! Test environments for running activity code and workflow workers.
//!
//! Activity inputs and outputs stay typed and are passed directly to the activity. The data
//! converter is used only where a worker would use it, such as heartbeat details.
//!
//! ```
//! use std::sync::Arc;
//! use temporalio_macros::activities;
//! use temporalio_sdk::{
//!     activities::{ActivityContext, ActivityError},
//!     testing::ActivityEnvironment,
//! };
//!
//! struct GreetingActivities {
//!     greeting: String,
//! }
//!
//! #[activities]
//! impl GreetingActivities {
//!     #[activity]
//!     async fn greet(
//!         self: Arc<Self>,
//!         _ctx: ActivityContext,
//!         name: String,
//!     ) -> Result<String, ActivityError> {
//!         Ok(format!("{}, {name}!", self.greeting))
//!     }
//! }
//!
//! # async fn example() {
//! let env = ActivityEnvironment::builder()
//!     .register_activities(GreetingActivities {
//!         greeting: "Hello".to_owned(),
//!     })
//!     .build();
//!
//! assert_eq!(
//!     env.run(GreetingActivities::greet, "Temporal".to_owned())
//!         .await
//!         .unwrap(),
//!     "Hello, Temporal!"
//! );
//! # }
//! ```
//!
//! [`WorkflowEnvironment::start_local`] owns a Temporal CLI dev server. Its client can be passed
//! to ordinary workflow starters and workers, while the local-server type state makes shutdown
//! available only on environments that own a server.
//!
//! ```no_run
//! use temporalio_sdk::testing::{LocalWorkflowEnvironmentOptions, WorkflowEnvironment};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let env = WorkflowEnvironment::start_local(LocalWorkflowEnvironmentOptions::default()).await?;
//! let client = env.client().clone();
//! // Construct workflow starters and workers with `client`.
//! # drop(client);
//! env.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use crate::activities::{
    ActivityContext, ActivityError, ActivityImplementer, ActivityInfo, ExecutableActivity,
};
use futures_util::{FutureExt, future::BoxFuture};
use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_client::{
    Client, ClientOptions, ConnectionOptions, Priority, errors::ClientConnectError,
};
use temporalio_common::{
    RetryPolicy, WorkflowExecution,
    data_converters::{
        DataConverter, PayloadConversionError, SerializationContextData, TemporalSerializable,
    },
    protos::temporal::api::common::v1::Payload,
};
use tokio_util::sync::CancellationToken;
use url::Url;

pub use temporalio_sdk_core::ephemeral_server::{
    DevServerLogFormat, DevServerLogLevel, EphemeralExe, EphemeralExeVersion, EphemeralServerError,
};
use temporalio_sdk_core::ephemeral_server::{
    EphemeralServer, TemporalDevServerConfig, default_cached_download,
};

type HeartbeatCallback = Arc<dyn Fn(Vec<Payload>) + Send + Sync>;
type HeartbeatDetailsFactory = Arc<
    dyn Fn(DataConverter) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>>
        + Send
        + Sync,
>;
type ActivityImplementers = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;

struct Defaulted<T>(T);

fn default_workflow_execution() -> WorkflowExecution {
    let mut execution = WorkflowExecution::default();
    execution.set_workflow_id("test").set_run_id("test-run");
    execution
}

/// Configure [`ActivityInfo`] with defaults suitable for an activity test.
#[bon::builder(
    builder_type(name = ActivityInfoBuilder, vis = "pub"),
    start_fn(name = activity_info, vis = "pub"),
    finish_fn(name = build, vis = "pub"),
    state_mod(vis = "pub"),
    on(String, into)
)]
fn build_activity_info(
    #[builder(default = b"test".to_vec())] task_token: Vec<u8>,
    #[builder(default = "test".to_owned())] workflow_type: String,
    #[builder(default = "default".to_owned())] workflow_namespace: String,
    #[builder(
        with = |value: Option<WorkflowExecution>| Defaulted(value),
        default = Defaulted(Some(default_workflow_execution()))
    )]
    workflow_execution: Defaulted<Option<WorkflowExecution>>,
    #[builder(default = "test".to_owned())] activity_id: String,
    #[builder(default = "unknown".to_owned())] activity_type: String,
    #[builder(default = "test".to_owned())] task_queue: String,
    heartbeat_timeout: Option<Duration>,
    #[builder(
        with = |value: Option<SystemTime>| Defaulted(value),
        default = Defaulted(Some(SystemTime::UNIX_EPOCH))
    )]
    scheduled_time: Defaulted<Option<SystemTime>>,
    #[builder(
        with = |value: Option<SystemTime>| Defaulted(value),
        default = Defaulted(Some(SystemTime::UNIX_EPOCH))
    )]
    started_time: Defaulted<Option<SystemTime>>,
    #[builder(
        with = |value: Option<SystemTime>| Defaulted(value),
        default = Defaulted(SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(1)))
    )]
    deadline: Defaulted<Option<SystemTime>>,
    #[builder(default = 1)] attempt: u32,
    #[builder(
        with = |value: Option<SystemTime>| Defaulted(value),
        default = Defaulted(Some(SystemTime::UNIX_EPOCH))
    )]
    current_attempt_scheduled_time: Defaulted<Option<SystemTime>>,
    retry_policy: Option<RetryPolicy>,
    #[builder(default)] is_local: bool,
    #[builder(default)] priority: Priority,
    run_id: Option<String>,
) -> ActivityInfo {
    ActivityInfo {
        task_token,
        workflow_type,
        workflow_namespace,
        workflow_execution: workflow_execution.0,
        activity_id,
        activity_type,
        task_queue,
        heartbeat_timeout,
        scheduled_time: scheduled_time.0,
        started_time: started_time.0,
        deadline: deadline.0,
        attempt,
        current_attempt_scheduled_time: current_attempt_scheduled_time.0,
        retry_policy,
        is_local,
        priority,
        run_id,
    }
}

/// Environment for running activity code with a test [`ActivityContext`].
#[derive(bon::Builder)]
#[builder(state_mod(vis = "pub"))]
pub struct ActivityEnvironment {
    #[builder(field)]
    heartbeat_callback: Option<HeartbeatCallback>,
    #[builder(field)]
    heartbeat_details_factory: Option<HeartbeatDetailsFactory>,
    #[builder(field)]
    implementers: ActivityImplementers,
    #[builder(default = activity_info().build())]
    info: ActivityInfo,
    #[builder(default)]
    headers: HashMap<String, Payload>,
    #[builder(default)]
    data_converter: DataConverter,
    client: Option<Client>,
    #[builder(default = CancellationToken::new())]
    cancellation_token: CancellationToken,
}

impl<S: activity_environment_builder::State> ActivityEnvironmentBuilder<S> {
    /// Register all activities implemented by an instance.
    pub fn register_activities<AI>(mut self, instance: AI) -> Self
    where
        AI: ActivityImplementer,
    {
        self.implementers
            .insert(TypeId::of::<AI>(), Arc::new(instance));
        self
    }

    /// Observe codec-encoded payloads for every heartbeat.
    pub fn on_heartbeat<F>(mut self, callback: F) -> Self
    where
        F: Fn(Vec<Payload>) + Send + Sync + 'static,
    {
        self.heartbeat_callback = Some(Arc::new(callback));
        self
    }

    /// Supply typed heartbeat details from a previous activity attempt.
    pub fn heartbeat_details<T>(mut self, details: T) -> Self
    where
        T: TemporalSerializable + Send + Sync + 'static,
    {
        let details = Arc::new(details);
        self.heartbeat_details_factory = Some(Arc::new(move |data_converter| {
            let details = details.clone();
            async move {
                let encoded = data_converter
                    .to_payloads(&SerializationContextData::Activity, details.as_ref())
                    .await?;
                data_converter
                    .codec()
                    .decode(&SerializationContextData::Activity, encoded)
                    .await
            }
            .boxed()
        }));
        self
    }
}

impl ActivityEnvironment {
    /// Run an activity marker with already-typed input.
    pub async fn run<A>(
        &self,
        activity: A,
        input: A::Input,
    ) -> Result<A::Output, ActivityEnvironmentError>
    where
        A: ExecutableActivity,
    {
        let receiver = if A::REQUIRES_INSTANCE {
            let implementer = self
                .implementers
                .get(&TypeId::of::<A::Implementer>())
                .cloned()
                .and_then(|instance| Arc::downcast::<A::Implementer>(instance).ok())
                .ok_or_else(|| ActivityEnvironmentError::MissingImplementer {
                    activity_type: activity.name().to_owned(),
                    implementer_type: type_name::<A::Implementer>(),
                })?;
            Some(implementer)
        } else {
            None
        };
        let heartbeat_details = match &self.heartbeat_details_factory {
            Some(factory) => factory(self.data_converter.clone())
                .await
                .map_err(|source| ActivityEnvironmentError::PayloadConversion {
                    operation: "previous heartbeat details",
                    source,
                })?,
            None => Vec::new(),
        };
        let context = ActivityContext::new_for_test(
            self.info.clone(),
            self.headers.clone(),
            self.data_converter.clone(),
            self.cancellation_token.clone(),
            heartbeat_details,
            self.client.clone(),
            self.heartbeat_callback.clone(),
        );
        A::execute(receiver, context, input)
            .await
            .map_err(ActivityEnvironmentError::Activity)
    }

    /// Cancel activity contexts created by this environment.
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }
}

/// Errors produced while preparing or running an activity in a test environment.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ActivityEnvironmentError {
    /// An instance activity was run without registering its implementer.
    #[error("activity `{activity_type}` requires an instance of `{implementer_type}`")]
    MissingImplementer {
        /// Activity type that could not be run.
        activity_type: String,
        /// Required implementer type.
        implementer_type: &'static str,
    },
    /// A value needed by the activity context could not be converted to payloads.
    #[error("payload conversion failed for {operation}: {source}")]
    PayloadConversion {
        /// Conversion being performed.
        operation: &'static str,
        /// Underlying payload conversion error.
        #[source]
        source: PayloadConversionError,
    },
    /// The activity returned an error.
    #[error("activity execution failed: {0:?}")]
    Activity(ActivityError),
}

/// Configuration for a local Temporal CLI dev server and its client.
#[derive(Debug, Clone, bon::Builder)]
#[builder(state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct LocalWorkflowEnvironmentOptions {
    /// Options used to create the namespace-bound client.
    #[builder(default = ClientOptions::new("default").build())]
    pub client_options: ClientOptions,
    /// Existing or downloadable Temporal CLI executable.
    #[builder(default = default_cached_download())]
    pub server_executable: EphemeralExe,
    /// Address on which the dev server listens.
    #[builder(default = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub bind_ip: IpAddr,
    /// Fixed frontend port, or an OS-selected port when absent.
    pub port: Option<u16>,
    /// Whether to start the Temporal UI.
    #[builder(default)]
    pub ui: bool,
    /// Fixed UI port, or the server default when absent.
    pub ui_port: Option<u16>,
    /// SQLite database path, or in-memory storage when absent.
    pub database_filename: Option<PathBuf>,
    /// Dev server log format.
    #[builder(default)]
    pub log_format: DevServerLogFormat,
    /// Dev server log level.
    #[builder(default)]
    pub log_level: DevServerLogLevel,
    /// Additional arguments appended to the Temporal CLI invocation.
    #[builder(default)]
    pub extra_args: Vec<String>,
}

impl Default for LocalWorkflowEnvironmentOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// State for a workflow environment backed by an externally managed server.
#[derive(Debug)]
#[non_exhaustive]
pub struct ExternalServer {
    _private: (),
}

/// State for a workflow environment that owns a local dev server.
#[derive(Debug)]
#[non_exhaustive]
pub struct LocalServer {
    server: EphemeralServer,
}

/// Client environment for workflow tests, parameterized by server ownership.
#[derive(Debug)]
#[non_exhaustive]
pub struct WorkflowEnvironment<S> {
    client: Client,
    state: S,
}

impl<S> WorkflowEnvironment<S> {
    /// Return the client used by workflow starters and workers in this environment.
    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl WorkflowEnvironment<ExternalServer> {
    /// Wrap a client connected to an externally managed Temporal server.
    pub fn from_client(client: Client) -> Self {
        Self {
            client,
            state: ExternalServer { _private: () },
        }
    }
}

impl WorkflowEnvironment<LocalServer> {
    /// Start a local Temporal CLI dev server and connect a client to it.
    pub async fn start_local(
        options: LocalWorkflowEnvironmentOptions,
    ) -> Result<Self, WorkflowEnvironmentError> {
        let database_filename = options
            .database_filename
            .map(|path| {
                path.into_os_string().into_string().map_err(|path| {
                    WorkflowEnvironmentError::InvalidDatabasePath {
                        path: PathBuf::from(path),
                    }
                })
            })
            .transpose()?;
        let server_config = TemporalDevServerConfig::builder()
            .exe(options.server_executable)
            .namespace(options.client_options.namespace.clone())
            .ip(options.bind_ip.to_string())
            .maybe_port(options.port)
            .ui(options.ui)
            .maybe_ui_port(options.ui_port)
            .maybe_db_filename(database_filename)
            .log_format(options.log_format)
            .log_level(options.log_level)
            .extra_args(options.extra_args)
            .build();
        let mut server = server_config
            .start_server()
            .await
            .map_err(WorkflowEnvironmentError::ServerStart)?;
        let target = Url::parse(&format!("http://{}", server.target))
            .map_err(WorkflowEnvironmentError::InvalidServerTarget)?;
        let connection_options = ConnectionOptions::new(target)
            .identity("temporalio-sdk-testing".to_owned())
            .client_name("temporalio-sdk".to_owned())
            .client_version(env!("CARGO_PKG_VERSION").to_owned())
            .build();
        let client = match Client::connect(connection_options, options.client_options).await {
            Ok(client) => client,
            Err(connect) => {
                return match server.shutdown().await {
                    Ok(()) => Err(WorkflowEnvironmentError::ClientConnect(connect)),
                    Err(shutdown) => Err(WorkflowEnvironmentError::ClientConnectAndShutdown {
                        connect: Box::new(connect),
                        shutdown: Box::new(shutdown),
                    }),
                };
            }
        };
        Ok(Self {
            client,
            state: LocalServer { server },
        })
    }

    /// Shut down the local server owned by this environment.
    pub async fn shutdown(mut self) -> Result<(), WorkflowEnvironmentError> {
        self.state
            .server
            .shutdown()
            .await
            .map_err(WorkflowEnvironmentError::ServerShutdown)
    }
}

/// Errors produced while creating or shutting down a workflow test environment.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowEnvironmentError {
    /// The local server could not be started.
    #[error("failed to start local Temporal server: {0}")]
    ServerStart(#[source] EphemeralServerError),
    /// A client could not connect to the newly started server.
    #[error("failed to connect client to local Temporal server: {0}")]
    ClientConnect(#[source] ClientConnectError),
    /// Client connection and subsequent server cleanup both failed.
    #[error("failed to connect client ({connect}) and shut down local server ({shutdown})")]
    ClientConnectAndShutdown {
        /// Client connection failure.
        connect: Box<ClientConnectError>,
        /// Server cleanup failure.
        shutdown: Box<EphemeralServerError>,
    },
    /// Explicit local server shutdown failed.
    #[error("failed to shut down local Temporal server: {0}")]
    ServerShutdown(#[source] EphemeralServerError),
    /// The local server target could not be represented as a URL.
    #[error("invalid local Temporal server target: {0}")]
    InvalidServerTarget(#[source] url::ParseError),
    /// The configured database path was not valid UTF-8 for the Temporal CLI.
    #[error("local Temporal database path is not valid UTF-8: {}", path.display())]
    InvalidDatabasePath {
        /// Invalid database path.
        path: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::BoxFuture;
    use std::sync::Mutex;
    use temporalio_common::data_converters::{
        DefaultFailureConverter, PayloadCodec, PayloadConverter,
    };
    use temporalio_macros::activities;

    struct TestActivities {
        prefix: String,
    }

    #[activities]
    impl TestActivities {
        #[activity]
        async fn echo(_ctx: ActivityContext, value: String) -> Result<String, ActivityError> {
            Ok(value)
        }

        #[activity]
        async fn prefixed(
            self: Arc<Self>,
            _ctx: ActivityContext,
            value: String,
        ) -> Result<String, ActivityError> {
            Ok(format!("{}{}", self.prefix, value))
        }

        #[activity]
        async fn heartbeat(ctx: ActivityContext, increment: u32) -> Result<u32, ActivityError> {
            let previous = ctx.heartbeat_details().deserialize::<u32>()?.unwrap_or(0);
            ctx.record_heartbeat(previous + increment).await?;
            Ok(previous)
        }

        #[activity]
        async fn cancellation_state(ctx: ActivityContext) -> Result<bool, ActivityError> {
            Ok(ctx.is_cancelled())
        }
    }

    struct FailingCodec;

    impl PayloadCodec for FailingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            async move {
                Err(PayloadConversionError::EncodingError(
                    "codec must not be called".into(),
                ))
            }
            .boxed()
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            async move {
                Err(PayloadConversionError::EncodingError(
                    "codec must not be called".into(),
                ))
            }
            .boxed()
        }
    }

    fn requires_instance<A: ExecutableActivity>(_: A) -> bool {
        A::REQUIRES_INSTANCE
    }

    #[test]
    fn activity_info_has_test_defaults_and_supports_overrides() {
        let info = activity_info().attempt(3).activity_type("custom").build();

        assert_eq!(info.attempt, 3);
        assert_eq!(info.activity_type, "custom");
        assert_eq!(info.activity_id, "test");
        assert_eq!(info.workflow_namespace, "default");
        assert_eq!(
            info.workflow_execution.as_ref().unwrap().workflow_id(),
            "test"
        );
    }

    #[tokio::test]
    async fn runs_static_and_registered_instance_activities_directly() {
        let data_converter = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            FailingCodec,
        );
        let env = ActivityEnvironment::builder()
            .data_converter(data_converter)
            .register_activities(TestActivities {
                prefix: "second:".to_owned(),
            })
            .register_activities(TestActivities {
                prefix: "latest:".to_owned(),
            })
            .build();

        assert!(!requires_instance(TestActivities::echo));
        assert!(requires_instance(TestActivities::prefixed));
        assert_eq!(
            env.run(TestActivities::echo, "value".to_owned())
                .await
                .unwrap(),
            "value"
        );
        assert_eq!(
            env.run(TestActivities::prefixed, "value".to_owned())
                .await
                .unwrap(),
            "latest:value"
        );
    }

    #[tokio::test]
    async fn missing_instance_is_an_environment_error() {
        let error = ActivityEnvironment::builder()
            .build()
            .run(TestActivities::prefixed, "value".to_owned())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ActivityEnvironmentError::MissingImplementer { .. }
        ));
    }

    #[tokio::test]
    async fn converts_previous_and_outbound_heartbeat_details() {
        let heartbeats = Arc::new(Mutex::new(Vec::new()));
        let env = ActivityEnvironment::builder()
            .heartbeat_details(4_u32)
            .on_heartbeat({
                let heartbeats = heartbeats.clone();
                move |payloads| heartbeats.lock().unwrap().push(payloads)
            })
            .build();

        assert_eq!(env.run(TestActivities::heartbeat, 3).await.unwrap(), 4);
        let payloads = heartbeats.lock().unwrap().pop().unwrap();
        let value: u32 = DataConverter::default()
            .from_payloads(&SerializationContextData::Activity, payloads)
            .await
            .unwrap();
        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn cancel_affects_contexts_created_by_environment() {
        let env = ActivityEnvironment::builder().build();
        env.cancel();

        assert!(
            env.run(TestActivities::cancellation_state, ())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn local_environment_reports_a_missing_executable() {
        let path = std::env::temp_dir().join(format!(
            "temporal-missing-test-server-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let error = WorkflowEnvironment::start_local(
            LocalWorkflowEnvironmentOptions::builder()
                .server_executable(EphemeralExe::ExistingPath(
                    path.to_string_lossy().into_owned(),
                ))
                .build(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            WorkflowEnvironmentError::ServerStart(EphemeralServerError::ExecutableNotFound { .. })
        ));
    }
}
