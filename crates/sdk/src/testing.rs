//! Test environments for running activity code and workflow workers.
//!
//! Activity inputs, outputs, and outbound heartbeat details stay typed. Previous heartbeat details
//! are serialized with the configured [`PayloadConverter`]; payload codecs and failure converters
//! are not used by [`ActivityEnvironment`].
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
    ActivityContext, ActivityDefinitions, ActivityError, ActivityHeartbeatCallback,
    ActivityImplementer, ActivityInfo, ExecutableActivity,
};
use std::{
    any::Any,
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_client::{
    Client, ClientOptions, ConnectionOptions, Priority, errors::ClientConnectError,
};
use temporalio_common::{
    RetryPolicy,
    data_converters::{
        ActivitySerializationContext, GenericPayloadConverter, PayloadConversionError,
        PayloadConverter, SerializationContext, SerializationContextData, TemporalSerializable,
    },
    protos::temporal::api::common::v1::Payload,
};
use tokio_util::sync::CancellationToken;
use url::Url;

use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe as CoreEphemeralExe, EphemeralExeVersion as CoreEphemeralExeVersion,
    EphemeralServer, EphemeralServerError as CoreEphemeralServerError, TemporalDevServerConfig,
};

type ActivityImplementers = HashMap<String, Arc<dyn Any + Send + Sync>>;

/// Where to find the Temporal server executable used by a local workflow environment.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EphemeralExe {
    /// Use an existing executable at this path.
    ExistingPath(String),
    /// Download and cache an executable when necessary.
    CachedDownload {
        /// Version to download.
        version: EphemeralExeVersion,
        /// Cache directory, or the operating system's temporary directory when absent.
        dest_dir: Option<String>,
        /// Maximum cache age, or no expiration when absent.
        ttl: Option<Duration>,
    },
}

impl EphemeralExe {
    fn into_core(self) -> CoreEphemeralExe {
        match self {
            EphemeralExe::ExistingPath(path) => CoreEphemeralExe::ExistingPath(path),
            EphemeralExe::CachedDownload {
                version,
                dest_dir,
                ttl,
            } => CoreEphemeralExe::CachedDownload {
                version: version.into_core(),
                dest_dir,
                ttl,
            },
        }
    }
}

impl Default for EphemeralExe {
    fn default() -> Self {
        EphemeralExe::CachedDownload {
            version: EphemeralExeVersion::Default,
            dest_dir: None,
            ttl: Some(Duration::from_secs(60 * 60 * 24 * 15)),
        }
    }
}

/// Version of a downloadable Temporal server executable.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EphemeralExeVersion {
    /// Resolve the server version selected for this SDK release.
    Default,
    /// Download a specific server version.
    Fixed(String),
}

impl EphemeralExeVersion {
    fn into_core(self) -> CoreEphemeralExeVersion {
        match self {
            EphemeralExeVersion::Default => CoreEphemeralExeVersion::SDKDefault {
                sdk_name: "sdk-rust".to_owned(),
                sdk_version: env!("CARGO_PKG_VERSION").to_owned(),
            },
            EphemeralExeVersion::Fixed(version) => CoreEphemeralExeVersion::Fixed(version),
        }
    }
}

/// Errors encountered while downloading, starting, or stopping a local Temporal server.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct EphemeralServerError(CoreEphemeralServerError);

impl EphemeralServerError {
    fn from_core(error: CoreEphemeralServerError) -> Self {
        Self(error)
    }
}

/// Options for constructing [`ActivityInfo`] with defaults suitable for an activity test.
#[derive(bon::Builder)]
#[builder(
    finish_fn(name = build_internal, vis = ""),
    state_mod(vis = "pub"),
    on(String, into)
)]
pub struct TestActivityInfoOptions {
    #[builder(default = b"test".to_vec())]
    task_token: Vec<u8>,
    #[builder(required, default = Some("test".to_owned()))]
    workflow_type: Option<String>,
    #[builder(default = "default".to_owned())]
    namespace: String,
    #[builder(required, default = Some("test".to_owned()))]
    workflow_id: Option<String>,
    #[builder(required, default = Some("test-run".to_owned()))]
    workflow_run_id: Option<String>,
    #[builder(default = "test".to_owned())]
    activity_id: String,
    #[builder(default = "unknown".to_owned())]
    activity_type: String,
    #[builder(default = "test".to_owned())]
    task_queue: String,
    heartbeat_timeout: Option<Duration>,
    #[builder(required, default = Some(SystemTime::UNIX_EPOCH))]
    scheduled_time: Option<SystemTime>,
    #[builder(required, default = Some(SystemTime::UNIX_EPOCH))]
    started_time: Option<SystemTime>,
    #[builder(
        required,
        default = SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(1))
    )]
    deadline: Option<SystemTime>,
    #[builder(default = 1)]
    attempt: u32,
    #[builder(required, default = Some(SystemTime::UNIX_EPOCH))]
    current_attempt_scheduled_time: Option<SystemTime>,
    retry_policy: Option<RetryPolicy>,
    #[builder(default)]
    is_local: bool,
    #[builder(default)]
    priority: Priority,
    activity_run_id: Option<String>,
}

impl<S: test_activity_info_options_builder::State> TestActivityInfoOptionsBuilder<S> {
    /// Build activity information from these test options.
    pub fn build(self) -> ActivityInfo {
        self.build_internal().into()
    }
}

impl From<TestActivityInfoOptions> for ActivityInfo {
    fn from(options: TestActivityInfoOptions) -> Self {
        Self {
            task_token: options.task_token,
            workflow_type: options.workflow_type,
            namespace: options.namespace,
            workflow_id: options.workflow_id,
            workflow_run_id: options.workflow_run_id,
            activity_id: options.activity_id,
            activity_type: options.activity_type,
            task_queue: options.task_queue,
            heartbeat_timeout: options.heartbeat_timeout,
            scheduled_time: options.scheduled_time,
            started_time: options.started_time,
            deadline: options.deadline,
            attempt: options.attempt,
            current_attempt_scheduled_time: options.current_attempt_scheduled_time,
            retry_policy: options.retry_policy,
            is_local: options.is_local,
            priority: options.priority,
            activity_run_id: options.activity_run_id,
        }
    }
}

/// Environment for running activity code with a test [`ActivityContext`].
#[derive(bon::Builder)]
#[builder(
    start_fn(name = builder_internal, vis = ""),
    state_mod(vis = "pub")
)]
pub struct ActivityEnvironment {
    #[builder(field)]
    heartbeat_callback: Option<ActivityHeartbeatCallback>,
    #[builder(field)]
    heartbeat_details: Vec<Payload>,
    #[builder(field)]
    implementers: ActivityImplementers,
    #[builder(
        default,
        getter(name = payload_converter_ref, vis = ""),
        setters(option_fn(vis = ""))
    )]
    payload_converter: PayloadConverter,
    #[builder(default = TestActivityInfoOptions::builder().build())]
    info: ActivityInfo,
    #[builder(default)]
    headers: HashMap<String, Payload>,
    client: Option<Client>,
    #[builder(default = CancellationToken::new())]
    cancellation_token: CancellationToken,
}

impl<S: activity_environment_builder::State> ActivityEnvironmentBuilder<S> {
    /// Register all activities implemented by an instance.
    pub fn register_activities<AI>(mut self, instance: AI) -> Self
    where
        AI: ActivityImplementer + Send + Sync + 'static,
    {
        let instance = Arc::new(instance);
        let mut definitions = ActivityDefinitions::default();
        AI::register_all(instance.clone(), &mut definitions);
        let instance: Arc<dyn Any + Send + Sync> = instance;
        for activity_type in definitions.names() {
            self.implementers.insert(activity_type, instance.clone());
        }
        self
    }

    /// Observe the typed details supplied to every heartbeat.
    pub fn on_heartbeat<F>(mut self, callback: F) -> Self
    where
        F: Fn(Box<dyn Any>) + Send + Sync + 'static,
    {
        self.heartbeat_callback = Some(Arc::new(callback));
        self
    }
}

impl<S> ActivityEnvironmentBuilder<S>
where
    S: activity_environment_builder::State,
    S::PayloadConverter: activity_environment_builder::IsSet,
{
    /// Supply heartbeat details from an activity attempt.
    ///
    /// Accessible via [`ActivityContext::heartbeat_details`].
    pub fn heartbeat_details<T>(mut self, details: T) -> Result<Self, PayloadConversionError>
    where
        T: TemporalSerializable + 'static,
    {
        let payload_converter = self
            .payload_converter_ref()
            .expect("payload converter must be set in builder state");
        let context_data = SerializationContextData::Activity(ActivitySerializationContext::new());
        let context = SerializationContext::new(&context_data, payload_converter);
        self.heartbeat_details = payload_converter.to_payloads(&context, &details)?;
        Ok(self)
    }
}

impl ActivityEnvironment {
    /// Construct an activity environment builder.
    pub fn builder() -> ActivityEnvironmentBuilder {
        Self::builder_internal()
    }

    /// Construct an activity environment builder using the default payload converter.
    pub fn builder_with_default()
    -> ActivityEnvironmentBuilder<activity_environment_builder::SetPayloadConverter> {
        Self::builder_internal().payload_converter(PayloadConverter::default())
    }

    /// Run an activity.
    pub async fn run<A>(
        &self,
        activity: A,
        input: A::Input,
    ) -> Result<A::Output, ActivityEnvironmentError>
    where
        A: ExecutableActivity,
    {
        let receiver = if A::REQUIRES_INSTANCE {
            let activity_type = activity.name();
            let implementer = self
                .implementers
                .get(activity_type)
                .cloned()
                .and_then(|instance| Arc::downcast::<A::Implementer>(instance).ok())
                .ok_or_else(|| ActivityEnvironmentError::MissingImplementer {
                    activity_type: activity_type.to_owned(),
                })?;
            Some(implementer)
        } else {
            None
        };
        let context = ActivityContext::new_for_test(
            self.info.clone(),
            self.headers.clone(),
            self.payload_converter.clone(),
            self.cancellation_token.clone(),
            self.heartbeat_details.clone(),
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

/// Errors produced while running an activity in a test environment.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ActivityEnvironmentError {
    /// An instance activity was run without registering its implementer.
    #[error("activity `{activity_type}` requires an instance in order to execute")]
    MissingImplementer {
        /// Activity type that could not be run.
        activity_type: String,
    },
    /// The activity returned an error.
    #[error("activity execution failed: {0:?}")]
    Activity(ActivityError),
}

/// Temporal CLI output format for a local workflow environment.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, derive_more::Display)]
#[non_exhaustive]
pub enum DevServerLogFormat {
    /// Human-readable text output.
    #[default]
    #[display("text")]
    Text,
    /// JSON output.
    #[display("json")]
    Json,
}

/// Temporal CLI logging level for a local workflow environment.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, derive_more::Display)]
#[non_exhaustive]
pub enum DevServerLogLevel {
    /// Debug and higher-severity messages.
    #[display("debug")]
    Debug,
    /// Informational and higher-severity messages.
    #[display("info")]
    Info,
    /// Warning and higher-severity messages.
    #[default]
    #[display("warn")]
    Warn,
    /// Error messages only.
    #[display("error")]
    Error,
    /// Disable logging.
    #[display("never")]
    Never,
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
    #[builder(default)]
    pub server_executable: EphemeralExe,
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

/// State for a workflow environment that starts a local dev server.
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
            .exe(options.server_executable.into_core())
            .namespace(options.client_options.namespace.clone())
            .maybe_port(options.port)
            .ui(options.ui)
            .maybe_ui_port(options.ui_port)
            .maybe_db_filename(database_filename)
            .log((
                options.log_format.to_string(),
                options.log_level.to_string(),
            ))
            .extra_args(options.extra_args)
            .build();
        let mut server = server_config
            .start_server()
            .await
            .map_err(EphemeralServerError::from_core)
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
                        shutdown: Box::new(EphemeralServerError::from_core(shutdown)),
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
            .map_err(EphemeralServerError::from_core)
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
    use std::sync::Mutex;
    use temporalio_common::data_converters::{MultiArgs2, MultiArgs3};
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
            self: std::sync::Arc<Self>,
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

    struct StaticActivities;

    #[activities]
    impl StaticActivities {
        #[activity]
        async fn echo(_ctx: ActivityContext, value: String) -> Result<String, ActivityError> {
            Ok(format!("static:{value}"))
        }
    }

    struct ActivityMacroShapes;

    #[activities]
    impl ActivityMacroShapes {
        #[activity]
        fn sync(_ctx: ActivityContext, value: bool) -> Result<String, ActivityError> {
            Ok(value.to_string())
        }

        #[activity]
        async fn no_input(_ctx: ActivityContext) -> Result<String, ActivityError> {
            Ok("no input".to_owned())
        }

        #[activity]
        async fn async_no_return(_ctx: ActivityContext, _value: String) {}

        #[activity]
        fn sync_no_return(_ctx: ActivityContext) {}
    }

    struct MultiArgActivities;

    #[activities]
    impl MultiArgActivities {
        #[activity]
        async fn two_args(
            _ctx: ActivityContext,
            first: String,
            second: i32,
        ) -> Result<String, ActivityError> {
            Ok(format!("{first}:{second}"))
        }

        #[activity]
        async fn three_args(
            _ctx: ActivityContext,
            first: String,
            second: i32,
            third: bool,
        ) -> Result<String, ActivityError> {
            Ok(format!("{first}:{second}:{third}"))
        }

        #[activity]
        async fn instance_two_args(
            self: Arc<Self>,
            _ctx: ActivityContext,
            first: String,
            second: i32,
        ) -> Result<String, ActivityError> {
            let _ = self;
            Ok(format!("{first}:{second}"))
        }

        #[activity]
        fn sync_two_args(
            _ctx: ActivityContext,
            first: String,
            second: i32,
        ) -> Result<String, ActivityError> {
            Ok(format!("{first}:{second}"))
        }
    }

    #[tokio::test]
    async fn runs_static_activities_without_instance() {
        let env = ActivityEnvironment::builder().build();

        assert_eq!(
            env.run(StaticActivities::echo, "value".to_owned())
                .await
                .unwrap(),
            "static:value"
        );
    }

    #[tokio::test]
    async fn runs_activities_with_instance() {
        let env = ActivityEnvironment::builder()
            .register_activities(TestActivities {
                prefix: "pre:".to_owned(),
            })
            .build();

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
            "pre:value"
        );
    }

    #[tokio::test]
    async fn runs_sync_and_unit_output_activities() {
        let env = ActivityEnvironment::builder().build();

        assert_eq!(
            env.run(ActivityMacroShapes::sync, true).await.unwrap(),
            "true"
        );
        assert_eq!(
            env.run(ActivityMacroShapes::no_input, ()).await.unwrap(),
            "no input"
        );
        env.run(ActivityMacroShapes::async_no_return, "value".to_owned())
            .await
            .unwrap();
        env.run(ActivityMacroShapes::sync_no_return, ())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn runs_multi_argument_activities() {
        let env = ActivityEnvironment::builder()
            .register_activities(MultiArgActivities)
            .build();

        assert_eq!(
            env.run(
                MultiArgActivities::two_args,
                MultiArgs2("one".to_owned(), 2),
            )
            .await
            .unwrap(),
            "one:2"
        );
        assert_eq!(
            env.run(
                MultiArgActivities::three_args,
                MultiArgs3("one".to_owned(), 2, true),
            )
            .await
            .unwrap(),
            "one:2:true"
        );
        assert_eq!(
            env.run(
                MultiArgActivities::instance_two_args,
                MultiArgs2("one".to_owned(), 2),
            )
            .await
            .unwrap(),
            "one:2"
        );
        assert_eq!(
            env.run(
                MultiArgActivities::sync_two_args,
                MultiArgs2("one".to_owned(), 2),
            )
            .await
            .unwrap(),
            "one:2"
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
    async fn converts_previous_and_observes_typed_outbound_heartbeat_details() {
        let heartbeats = Arc::new(Mutex::new(Vec::new()));
        let env = ActivityEnvironment::builder_with_default()
            .heartbeat_details(4_u32)
            .unwrap()
            .on_heartbeat({
                let heartbeats = heartbeats.clone();
                move |details| {
                    let details = details
                        .downcast::<u32>()
                        .expect("heartbeat details should retain their concrete type");
                    heartbeats.lock().unwrap().push(*details);
                }
            })
            .build();

        assert_eq!(env.run(TestActivities::heartbeat, 3).await.unwrap(), 4);
        assert_eq!(heartbeats.lock().unwrap().pop(), Some(7));
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
}
