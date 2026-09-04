#![warn(missing_docs)]

//! OpenTelemetry tracing and context propagation for the Temporal Rust SDK.
//!
//! [`crate::opentelemetry::OpenTelemetryPlugin`] installs interceptors for clients, Workflows, and
//! Activities. The application configures and shuts down its OpenTelemetry provider and exporters.
//! The plugin puts trace context in the cross-SDK `_tracer-data` Temporal header. By default, the
//! plugin propagates W3C Trace Context and W3C Baggage.
//!
//! ```no_run
//! use temporalio_client::ClientOptions;
//! use temporalio_sdk::opentelemetry::OpenTelemetryPlugin;
//!
//! let client_options = ClientOptions::new("default")
//!     .plugin(OpenTelemetryPlugin::new())
//!     .build();
//! # let _ = client_options;
//! ```
//!
//! The plugin creates spans for these operations:
//!
//! - It traces client Workflow starts, Signals, Queries, and Updates.
//! - It traces Workflow execution and message handlers.
//! - It traces Activity execution.
//! - It traces Workflow calls to Activities, local Activities, child Workflows, and Signals.
//!
//! The plugin also propagates context through Continue-as-New.
//!
//! During replay, the plugin does not export Workflow spans. Use
//! [`crate::opentelemetry::WorkflowIdGenerator`] in the tracer provider for Workflow spans. If
//! application code creates Workflow spans, also wrap each span processor in
//! [`crate::opentelemetry::WorkflowSpanProcessor`]. These types give the same span IDs during
//! execution and replay. They do not export application spans that finish during replay. The Rust
//! interceptor API does not support inbound Nexus handler interception.

use ::opentelemetry::{
    Context, KeyValue, global,
    propagation::{Extractor, Injector, TextMapCompositePropagator, TextMapPropagator},
    trace::{
        FutureExt as _, SpanBuilder, SpanContext, SpanId, SpanKind, Status, TraceContextExt,
        TraceFlags, TraceId, Tracer,
    },
};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    resource::Resource,
    trace::{IdGenerator, RandomIdGenerator, SpanData, SpanProcessor},
};
use std::{
    cell::RefCell,
    collections::HashMap,
    fmt::Debug,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use temporalio_client::{
    CancelWorkflowInput, ClientInterceptor, ClientOptions, ClientPlugin, DescribeWorkflowInput,
    DescribeWorkflowOutput, ErasedClientPlugin, Next as ClientNext, PluginError,
    QueryWorkflowInput, QueryWorkflowOutput, SignalWithStartWorkflowInput, SignalWorkflowInput,
    StartWorkflowInput, StartWorkflowOutput, StartWorkflowUpdateInput, StartWorkflowUpdateOutput,
    TerminateWorkflowInput, UpdateWithStartWorkflowInput, UpdateWithStartWorkflowOutput,
    errors::{
        WorkflowInteractionError, WorkflowQueryError, WorkflowStartError, WorkflowUpdateError,
        WorkflowUpdateWithStartError,
    },
};
use temporalio_common::{
    data_converters::{
        GenericPayloadConverter, PayloadConverter, SerializationContext, SerializationContextData,
        WorkflowSerializationContext,
    },
    protos::temporal::api::common::v1::{Header, Payload},
};
use temporalio_sdk::{
    ClientAndWorkerPlugin, WorkerOptions, WorkerPlugin, WorkflowContextKey,
    activities::ActivityError,
    interceptors::{
        ActivityInboundInterceptor, ExecuteActivityInput, ExecuteActivityOutput,
        Next as ActivityNext,
    },
    workflow_interceptors::{
        CancelExternalWorkflowInput, CancelExternalWorkflowResult,
        CancellableWorkflowOutboundFuture, ContinueAsNewInput, ContinueAsNewResult,
        ExecuteWorkflowInput, ExecuteWorkflowResult, HandleQueryInput, HandleQueryResult,
        HandleSignalInput, HandleSignalResult, HandleUpdateInput, HandleUpdateResult,
        InitializeWorkflowInput, InitializeWorkflowOutput, ScheduleActivityInput,
        ScheduleActivityResult, ScheduleLocalActivityInput, SignalWorkflowInput as WfSignalInput,
        SignalWorkflowResult, StartChildWorkflowInput, StartChildWorkflowResult,
        SyncWorkflowInterceptorContext, ValidateUpdateInput, ValidateUpdateResult,
        WorkflowInterceptor, WorkflowInterceptorConstructor, WorkflowInterceptorContext,
        WorkflowInterceptorFuture, WorkflowNext, WorkflowOutboundFuture,
    },
    workflow_replayer::WorkflowReplayerOptions,
};
use temporalio_workflow::WorkflowRandomStream;

struct CurrentOpenTelemetryContext;

impl WorkflowContextKey for CurrentOpenTelemetryContext {
    type Value = Context;
}

/// The Temporal header that OpenTelemetry integrations use in other Temporal SDKs.
pub const TRACE_HEADER_KEY: &str = "_tracer-data";
/// The OpenTelemetry instrumentation scope that the default tracer uses.
pub const INSTRUMENTATION_SCOPE: &str = "temporalio-sdk";

const WORKFLOW_ID_ATTRIBUTE: &str = "temporalWorkflowID";
const RUN_ID_ATTRIBUTE: &str = "temporalRunID";
const ACTIVITY_ID_ATTRIBUTE: &str = "temporalActivityID";
const UPDATE_ID_ATTRIBUTE: &str = "temporalUpdateID";
const WORKFLOW_ID_RANDOM_STREAM: &str = "temporalio-sdk/opentelemetry";

#[derive(Clone)]
struct WorkflowTelemetryState {
    random: Option<WorkflowRandomStream>,
    replaying: bool,
}

thread_local! {
    static WORKFLOW_TELEMETRY_STATE: RefCell<Option<WorkflowTelemetryState>> = const { RefCell::new(None) };
}

struct WorkflowTelemetryStateGuard(Option<WorkflowTelemetryState>);

impl WorkflowTelemetryStateGuard {
    fn enter(random: Option<WorkflowRandomStream>, replaying: bool) -> Self {
        Self(
            WORKFLOW_TELEMETRY_STATE
                .with(|state| state.replace(Some(WorkflowTelemetryState { random, replaying }))),
        )
    }
}

impl Drop for WorkflowTelemetryStateGuard {
    fn drop(&mut self) {
        WORKFLOW_TELEMETRY_STATE.with(|state| {
            state.replace(self.0.take());
        });
    }
}

/// An OpenTelemetry ID generator that uses replay-safe Workflow randomness.
///
/// Use this generator in the tracer provider for [`OpenTelemetryPlugin`]. This generator gives
/// the same IDs to spans during execution and replay. Outside a Workflow context that requires
/// replay safety, it uses the standard OpenTelemetry random ID generator.
#[derive(Clone, Debug, Default)]
pub struct WorkflowIdGenerator {
    fallback: RandomIdGenerator,
}

impl IdGenerator for WorkflowIdGenerator {
    fn new_trace_id(&self) -> TraceId {
        WORKFLOW_TELEMETRY_STATE.with(|state| {
            if let Some(random) = state
                .borrow()
                .as_ref()
                .and_then(|state| state.random.clone())
            {
                loop {
                    let id = TraceId::from(random.random::<u128>());
                    if id != TraceId::INVALID {
                        return id;
                    }
                }
            }
            self.fallback.new_trace_id()
        })
    }

    fn new_span_id(&self) -> SpanId {
        WORKFLOW_TELEMETRY_STATE.with(|state| {
            if let Some(random) = state
                .borrow()
                .as_ref()
                .and_then(|state| state.random.clone())
            {
                loop {
                    let id = SpanId::from(random.random::<u64>());
                    if id != SpanId::INVALID {
                        return id;
                    }
                }
            }
            self.fallback.new_span_id()
        })
    }
}

/// A span processor that does not export application Workflow spans that finish during replay.
///
/// Wrap each processor in the tracer provider for [`OpenTelemetryPlugin`] when application
/// Workflow code creates spans. The application flushes and stops the provider and exporters.
#[derive(Debug)]
pub struct WorkflowSpanProcessor<P> {
    inner: P,
}

impl<P> WorkflowSpanProcessor<P> {
    /// Prevents the export of replay spans from a span processor.
    pub fn new(inner: P) -> Self {
        Self { inner }
    }

    /// Returns the wrapped processor.
    pub fn into_inner(self) -> P {
        self.inner
    }
}

impl<P: SpanProcessor> SpanProcessor for WorkflowSpanProcessor<P> {
    fn on_start(&self, span: &mut opentelemetry_sdk::trace::Span, cx: &Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, span: SpanData) {
        let replaying = WORKFLOW_TELEMETRY_STATE
            .with(|state| state.borrow().as_ref().is_some_and(|state| state.replaying));
        if !replaying {
            self.inner.on_end(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

// The Workflow context map stores the OpenTelemetry parent context. OpenTelemetry does not give a
// Workflow context to its ID generator or span processor. This wrapper gives those callbacks the
// replay state and random stream only while it polls the Workflow. It restores the previous state
// after each poll because Workflow Futures can share an executor thread.
struct WorkflowTelemetryFuture<F> {
    ctx: WorkflowInterceptorContext,
    random: WorkflowRandomStream,
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for WorkflowTelemetryFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = WorkflowTelemetryStateGuard::enter(
            Some(this.random.clone()),
            this.ctx.is_replaying_history_events(),
        );
        this.inner.as_mut().poll(cx)
    }
}

#[derive(Clone)]
enum TracerSource {
    Global,
    Configured(SpanStarter),
}

type SpanStarter = Arc<dyn Fn(SpanBuilder, &Context) -> Context + Send + Sync>;

#[derive(Clone)]
enum PropagatorSource {
    Global,
    Configured(Arc<dyn TextMapPropagator + Send + Sync>),
}

#[derive(Clone)]
struct Config {
    tracer: TracerSource,
    propagator: PropagatorSource,
    header_key: Arc<str>,
}

impl Config {
    fn inject(&self, context: &Context, carrier: &mut Carrier) {
        match &self.propagator {
            PropagatorSource::Global => global::get_text_map_propagator(|propagator| {
                propagator.inject_context(context, carrier)
            }),
            PropagatorSource::Configured(propagator) => propagator.inject_context(context, carrier),
        }
    }

    fn extract(&self, carrier: &Carrier) -> Context {
        match &self.propagator {
            PropagatorSource::Global => global::get_text_map_propagator(|propagator| {
                propagator.extract_with_context(&Context::new(), carrier)
            }),
            PropagatorSource::Configured(propagator) => {
                propagator.extract_with_context(&Context::new(), carrier)
            }
        }
    }
}

/// A Temporal client and worker plugin for OpenTelemetry tracing.
///
/// By default, the plugin uses the OpenTelemetry global tracer. It propagates W3C Trace Context and
/// W3C Baggage. The application starts, flushes, and stops the provider and exporters.
#[derive(Clone)]
pub struct OpenTelemetryPlugin {
    config: Arc<Config>,
}

impl OpenTelemetryPlugin {
    /// Creates a plugin that uses the global tracer and W3C propagators.
    pub fn new() -> Self {
        Self {
            config: Arc::new(Config {
                tracer: TracerSource::Global,
                propagator: PropagatorSource::Configured(Arc::new(
                    TextMapCompositePropagator::new(vec![
                        Box::new(opentelemetry_sdk::propagation::TraceContextPropagator::new()),
                        Box::new(opentelemetry_sdk::propagation::BaggagePropagator::new()),
                    ]),
                )),
                header_key: TRACE_HEADER_KEY.into(),
            }),
        }
    }

    /// Sets the tracer that the plugin uses.
    pub fn with_tracer<T>(mut self, tracer: T) -> Self
    where
        T: Tracer + Send + Sync + 'static,
        T::Span: Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.config).tracer =
            TracerSource::Configured(Arc::new(move |builder, parent| {
                parent.with_span(builder.start_with_context(&tracer, parent))
            }));
        self
    }

    /// Sets the text-map propagator that the plugin uses.
    pub fn with_propagator(
        mut self,
        propagator: impl TextMapPropagator + Send + Sync + 'static,
    ) -> Self {
        Arc::make_mut(&mut self.config).propagator =
            PropagatorSource::Configured(Arc::new(propagator));
        self
    }

    /// Uses the global text-map propagator for each operation.
    pub fn with_global_propagator(mut self) -> Self {
        Arc::make_mut(&mut self.config).propagator = PropagatorSource::Global;
        self
    }

    /// Sets the Temporal payload header that contains trace context.
    ///
    /// Keep the default `_tracer-data` value to propagate context to other Temporal SDKs.
    pub fn with_header_key(mut self, header_key: impl Into<Arc<str>>) -> Self {
        Arc::make_mut(&mut self.config).header_key = header_key.into();
        self
    }

    fn workflow_interceptor_constructor(&self) -> WorkflowInterceptorConstructor {
        let config = self.config.clone();
        WorkflowInterceptorConstructor::new(move |_| {
            OpenTelemetryWorkflowInterceptor::new(config.clone())
        })
    }
}

impl Default for OpenTelemetryPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl From<OpenTelemetryPlugin> for ErasedClientPlugin {
    fn from(plugin: OpenTelemetryPlugin) -> Self {
        ClientAndWorkerPlugin::new(plugin).into()
    }
}

impl ClientPlugin for OpenTelemetryPlugin {
    fn name(&self) -> &str {
        "opentelemetry"
    }

    fn configure_client_options(&self, options: &mut ClientOptions) -> Result<(), PluginError> {
        options
            .client_interceptors
            .push(Arc::new(OpenTelemetryClientInterceptor {
                config: self.config.clone(),
            }));
        Ok(())
    }
}

impl WorkerPlugin for OpenTelemetryPlugin {
    fn name(&self) -> &str {
        "opentelemetry"
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        options.activity_inbound_interceptor(OpenTelemetryActivityInboundInterceptor {
            config: self.config.clone(),
        });
        options.workflow_interceptor(self.workflow_interceptor_constructor());
        Ok(())
    }

    fn configure_workflow_replayer_options(
        &self,
        options: &mut WorkflowReplayerOptions,
    ) -> Result<(), PluginError> {
        options.workflow_interceptor(self.workflow_interceptor_constructor());
        Ok(())
    }
}

#[derive(Default)]
struct Carrier(HashMap<String, String>);

impl Injector for Carrier {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

impl Extractor for Carrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

fn carrier_payload(carrier: &Carrier) -> Payload {
    let converter = PayloadConverter::default();
    let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
    converter
        .to_payload(
            &SerializationContext::new(&context_data, &converter),
            &carrier.0,
        )
        .expect("A string map must be JSON serializable.")
}

fn payload_carrier(payload: &Payload) -> Option<Carrier> {
    let converter = PayloadConverter::default();
    let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
    converter
        .from_payload::<HashMap<String, String>>(
            &SerializationContext::new(&context_data, &converter),
            payload.clone(),
        )
        .ok()
        .map(Carrier)
}

fn inject_into_fields(config: &Config, context: &Context, fields: &mut HashMap<String, Payload>) {
    let mut carrier = Carrier::default();
    config.inject(context, &mut carrier);
    if !carrier.0.is_empty() {
        fields.insert(config.header_key.to_string(), carrier_payload(&carrier));
    }
}

fn inject_into_header(config: &Config, context: &Context, header: &mut Option<Header>) {
    inject_into_fields(config, context, &mut header.get_or_insert_default().fields);
}

fn extract_from_fields(config: &Config, fields: &HashMap<String, Payload>) -> Context {
    fields
        .get(config.header_key.as_ref())
        .and_then(payload_carrier)
        .map(|carrier| config.extract(&carrier))
        .unwrap_or_default()
}

fn start_span(
    config: &Config,
    parent: &Context,
    name: String,
    kind: SpanKind,
    attributes: Vec<KeyValue>,
) -> Context {
    let builder = SpanBuilder::from_name(name)
        .with_kind(kind)
        .with_attributes(attributes);
    match &config.tracer {
        TracerSource::Global => {
            let tracer = global::tracer(INSTRUMENTATION_SCOPE);
            parent.with_span(builder.start_with_context(&tracer, parent))
        }
        TracerSource::Configured(start) => start(builder, parent),
    }
}

fn replay_span_context(parent: &Context, random: &WorkflowRandomStream) -> Context {
    let span_id = loop {
        let id = SpanId::from(random.random::<u64>());
        if id != SpanId::INVALID {
            break id;
        }
    };
    let parent_span = parent.span();
    let parent_span_context = parent_span.span_context();
    let trace_id = if parent_span_context.is_valid() {
        parent_span_context.trace_id()
    } else {
        loop {
            let id = TraceId::from(random.random::<u128>());
            if id != TraceId::INVALID {
                break id;
            }
        }
    };
    let trace_flags = if parent_span_context.is_valid() {
        parent_span_context.trace_flags()
    } else {
        TraceFlags::SAMPLED
    };
    let trace_state = parent_span_context.trace_state().clone();
    parent.with_remote_span_context(SpanContext::new(
        trace_id,
        span_id,
        trace_flags,
        false,
        trace_state,
    ))
}

fn start_workflow_span(
    config: &Config,
    ctx: &WorkflowInterceptorContext,
    parent: &Context,
    name: String,
    kind: SpanKind,
    attributes: Vec<KeyValue>,
) -> (Context, bool) {
    let random = ctx.random_stream(WORKFLOW_ID_RANDOM_STREAM);
    let replaying = ctx.is_replaying_history_events();
    let _guard = WorkflowTelemetryStateGuard::enter(Some(random.clone()), replaying);
    if replaying {
        (replay_span_context(parent, &random), false)
    } else {
        (start_span(config, parent, name, kind, attributes), true)
    }
}

fn scoped_workflow_future<F: Future>(
    ctx: WorkflowInterceptorContext,
    context: Context,
    future: F,
) -> impl Future<Output = F::Output> {
    let random = ctx.random_stream(WORKFLOW_ID_RANDOM_STREAM);
    let contextual = ctx.with_context_value::<CurrentOpenTelemetryContext, _>(
        context.clone(),
        future.with_context(context),
    );
    WorkflowTelemetryFuture {
        ctx,
        random,
        inner: Box::pin(contextual),
    }
}

fn finish_span<T, E: Debug>(context: &Context, result: &Result<T, E>) {
    if let Err(error) = result {
        let description = format!("{error:?}");
        context.span().add_event(
            "exception",
            vec![KeyValue::new("exception.message", description.clone())],
        );
        context.span().set_status(Status::error(description));
    }
    context.span().end();
}

fn workflow_attributes(workflow_id: &str, run_id: Option<&str>) -> Vec<KeyValue> {
    let mut attributes = vec![KeyValue::new(WORKFLOW_ID_ATTRIBUTE, workflow_id.to_owned())];
    if let Some(run_id) = run_id.filter(|run_id| !run_id.is_empty()) {
        attributes.push(KeyValue::new(RUN_ID_ATTRIBUTE, run_id.to_owned()));
    }
    attributes
}

struct OpenTelemetryClientInterceptor {
    config: Arc<Config>,
}

macro_rules! client_operation {
    ($self:ident, $input:ident, $next:ident, $name:expr, $attrs:expr, $context:ident, $headers:block) => {{
        let parent = Context::current();
        let $context = start_span(&$self.config, &parent, $name, SpanKind::Client, $attrs);
        $headers
        Box::pin(async move {
            let result = $next.run($input).with_context($context.clone()).await;
            finish_span(&$context, &result);
            result
        })
    }};
}

impl ClientInterceptor for OpenTelemetryClientInterceptor {
    fn start_workflow<'a>(
        &'a self,
        mut input: StartWorkflowInput,
        next: ClientNext<
            'a,
            StartWorkflowInput,
            futures_util::future::BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>,
        >,
    ) -> futures_util::future::BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>> {
        let name = format!("StartWorkflow:{}", input.workflow_type);
        let attrs = workflow_attributes(&input.options.workflow_id, None);
        client_operation!(self, input, next, name, attrs, context, {
            inject_into_header(&self.config, &context, &mut input.options.header);
        })
    }

    fn signal_with_start_workflow<'a>(
        &'a self,
        mut input: SignalWithStartWorkflowInput,
        next: ClientNext<
            'a,
            SignalWithStartWorkflowInput,
            futures_util::future::BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>>,
        >,
    ) -> futures_util::future::BoxFuture<'a, Result<StartWorkflowOutput, WorkflowStartError>> {
        let name = format!("SignalWithStartWorkflow:{}", input.workflow_type);
        let attrs = workflow_attributes(&input.options.workflow_id, None);
        client_operation!(self, input, next, name, attrs, context, {
            inject_into_header(&self.config, &context, &mut input.options.header);
        })
    }

    fn signal_workflow<'a>(
        &'a self,
        mut input: SignalWorkflowInput,
        next: ClientNext<
            'a,
            SignalWorkflowInput,
            futures_util::future::BoxFuture<'a, Result<(), WorkflowInteractionError>>,
        >,
    ) -> futures_util::future::BoxFuture<'a, Result<(), WorkflowInteractionError>> {
        let name = format!("SignalWorkflow:{}", input.signal_name);
        let attrs = workflow_attributes(&input.workflow_id, Some(&input.run_id));
        client_operation!(self, input, next, name, attrs, context, {
            inject_into_header(&self.config, &context, &mut input.options.header);
        })
    }

    fn describe_workflow<'a>(
        &'a self,
        input: DescribeWorkflowInput,
        next: ClientNext<
            'a,
            DescribeWorkflowInput,
            futures_util::future::BoxFuture<
                'a,
                Result<DescribeWorkflowOutput, WorkflowInteractionError>,
            >,
        >,
    ) -> futures_util::future::BoxFuture<'a, Result<DescribeWorkflowOutput, WorkflowInteractionError>>
    {
        let attrs = workflow_attributes(&input.workflow_id, Some(&input.run_id));
        client_operation!(
            self,
            input,
            next,
            "DescribeWorkflow".to_owned(),
            attrs,
            context,
            {}
        )
    }

    fn query_workflow<'a>(
        &'a self,
        mut input: QueryWorkflowInput,
        next: ClientNext<
            'a,
            QueryWorkflowInput,
            futures_util::future::BoxFuture<'a, Result<QueryWorkflowOutput, WorkflowQueryError>>,
        >,
    ) -> futures_util::future::BoxFuture<'a, Result<QueryWorkflowOutput, WorkflowQueryError>> {
        let name = format!("QueryWorkflow:{}", input.query_name);
        let attrs = workflow_attributes(&input.workflow_id, Some(&input.run_id));
        client_operation!(self, input, next, name, attrs, context, {
            inject_into_header(&self.config, &context, &mut input.options.header);
        })
    }

    fn start_workflow_update<'a>(
        &'a self,
        mut input: StartWorkflowUpdateInput,
        next: ClientNext<
            'a,
            StartWorkflowUpdateInput,
            futures_util::future::BoxFuture<
                'a,
                Result<StartWorkflowUpdateOutput, WorkflowUpdateError>,
            >,
        >,
    ) -> futures_util::future::BoxFuture<'a, Result<StartWorkflowUpdateOutput, WorkflowUpdateError>>
    {
        let name = format!("StartWorkflowUpdate:{}", input.update_name);
        let mut attrs = workflow_attributes(&input.workflow_id, Some(&input.run_id));
        if let Some(update_id) = &input.options.update_id {
            attrs.push(KeyValue::new(UPDATE_ID_ATTRIBUTE, update_id.clone()));
        }
        client_operation!(self, input, next, name, attrs, context, {
            inject_into_header(&self.config, &context, &mut input.options.header);
        })
    }

    fn update_with_start_workflow<'a>(
        &'a self,
        mut input: UpdateWithStartWorkflowInput,
        next: ClientNext<
            'a,
            UpdateWithStartWorkflowInput,
            futures_util::future::BoxFuture<
                'a,
                Result<UpdateWithStartWorkflowOutput, WorkflowUpdateWithStartError>,
            >,
        >,
    ) -> futures_util::future::BoxFuture<
        'a,
        Result<UpdateWithStartWorkflowOutput, WorkflowUpdateWithStartError>,
    > {
        let name = format!("UpdateWithStartWorkflow:{}", input.workflow_type);
        let mut attrs = workflow_attributes(&input.options.workflow_id, None);
        if let Some(update_id) = &input.options.update_id {
            attrs.push(KeyValue::new(UPDATE_ID_ATTRIBUTE, update_id.clone()));
        }
        client_operation!(self, input, next, name, attrs, context, {
            inject_into_header(&self.config, &context, &mut input.options.start_header);
            inject_into_header(&self.config, &context, &mut input.options.update_header);
        })
    }

    fn cancel_workflow<'a>(
        &'a self,
        input: CancelWorkflowInput,
        next: ClientNext<
            'a,
            CancelWorkflowInput,
            futures_util::future::BoxFuture<'a, Result<(), WorkflowInteractionError>>,
        >,
    ) -> futures_util::future::BoxFuture<'a, Result<(), WorkflowInteractionError>> {
        let attrs = workflow_attributes(&input.workflow_id, Some(&input.run_id));
        client_operation!(
            self,
            input,
            next,
            "CancelWorkflow".to_owned(),
            attrs,
            context,
            {}
        )
    }

    fn terminate_workflow<'a>(
        &'a self,
        input: TerminateWorkflowInput,
        next: ClientNext<
            'a,
            TerminateWorkflowInput,
            futures_util::future::BoxFuture<'a, Result<(), WorkflowInteractionError>>,
        >,
    ) -> futures_util::future::BoxFuture<'a, Result<(), WorkflowInteractionError>> {
        let attrs = workflow_attributes(&input.workflow_id, Some(&input.run_id));
        client_operation!(
            self,
            input,
            next,
            "TerminateWorkflow".to_owned(),
            attrs,
            context,
            {}
        )
    }
}

struct OpenTelemetryActivityInboundInterceptor {
    config: Arc<Config>,
}

impl ActivityInboundInterceptor for OpenTelemetryActivityInboundInterceptor {
    fn execute_activity<'a>(
        &'a self,
        input: ExecuteActivityInput,
        next: ActivityNext<'a, ExecuteActivityInput, ExecuteActivityOutput<'a>>,
    ) -> ExecuteActivityOutput<'a> {
        let info = input.activity_info();
        let mut attributes = vec![
            KeyValue::new(ACTIVITY_ID_ATTRIBUTE, info.activity_id.clone()),
            KeyValue::new("temporalActivityType", info.activity_type.clone()),
        ];
        if let Some(workflow_id) = &info.workflow_id {
            attributes.extend(workflow_attributes(
                workflow_id,
                info.workflow_run_id.as_deref(),
            ));
        }
        let parent = extract_from_fields(&self.config, input.headers());
        let context = start_span(
            &self.config,
            &parent,
            format!("RunActivity:{}", info.activity_type),
            SpanKind::Server,
            attributes,
        );
        Box::pin(async move {
            let result: Result<_, ActivityError> =
                next.run(input).with_context(context.clone()).await;
            finish_span(&context, &result);
            result
        })
    }
}

struct OpenTelemetryWorkflowInterceptor {
    config: Arc<Config>,
    workflow_context: RefCell<Option<Context>>,
}

impl OpenTelemetryWorkflowInterceptor {
    fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            workflow_context: RefCell::new(None),
        }
    }

    fn parent_for(&self, fields: &HashMap<String, Payload>) -> Context {
        if fields.contains_key(self.config.header_key.as_ref()) {
            extract_from_fields(&self.config, fields)
        } else {
            self.workflow_context.borrow().clone().unwrap_or_default()
        }
    }

    fn outbound_context(&self, workflow_context: Option<Rc<Context>>) -> Context {
        let current = Context::current();
        if current.has_active_span() {
            current
        } else if let Some(context) = workflow_context {
            (*context).clone()
        } else {
            self.workflow_context.borrow().clone().unwrap_or_default()
        }
    }
}

macro_rules! workflow_handler {
    ($self:ident, $ctx:ident, $input:ident, $next:ident, $name:expr, $attrs:expr) => {{
        let parent = $self.parent_for($input.headers());
        let (context, recording) = start_workflow_span(
            &$self.config,
            &$ctx,
            &parent,
            $name,
            SpanKind::Server,
            $attrs,
        );
        let scoped_context = context.clone();
        let future = async move {
            let result = $next.run($input).await;
            if recording {
                finish_span(&context, &result);
            }
            result
        };
        WorkflowInterceptorFuture::new(scoped_workflow_future($ctx, scoped_context, future))
    }};
}

impl WorkflowInterceptor for OpenTelemetryWorkflowInterceptor {
    fn initialize_workflow(
        &self,
        _ctx: temporalio_workflow::WorkflowContextView,
        input: InitializeWorkflowInput,
        next: WorkflowNext<'_, InitializeWorkflowInput, InitializeWorkflowOutput>,
    ) -> InitializeWorkflowOutput {
        *self.workflow_context.borrow_mut() =
            Some(extract_from_fields(&self.config, input.headers()));
        next.run(input)
    }

    fn execute<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        input: ExecuteWorkflowInput,
        next: WorkflowNext<
            'a,
            ExecuteWorkflowInput,
            WorkflowInterceptorFuture<'a, ExecuteWorkflowResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, ExecuteWorkflowResult> {
        let parent = self.parent_for(input.headers());
        let (context, recording) = start_workflow_span(
            &self.config,
            &ctx,
            &parent,
            format!("RunWorkflow:{}", ctx.workflow_type()),
            SpanKind::Server,
            workflow_attributes(ctx.workflow_id(), Some(ctx.run_id())),
        );
        *self.workflow_context.borrow_mut() = Some(context.clone());
        let active_context = context.clone();
        let future = async move {
            let result = next.run(input).await;
            if recording {
                finish_span(&context, &result);
            }
            result
        };
        WorkflowInterceptorFuture::new(scoped_workflow_future(ctx, active_context, future))
    }

    fn handle_signal<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        input: HandleSignalInput,
        next: WorkflowNext<
            'a,
            HandleSignalInput,
            WorkflowInterceptorFuture<'a, HandleSignalResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleSignalResult> {
        let name = format!("HandleSignal:{}", input.name());
        workflow_handler!(self, ctx, input, next, name, Vec::new())
    }

    fn handle_update<'a>(
        &'a self,
        ctx: WorkflowInterceptorContext,
        input: HandleUpdateInput,
        next: WorkflowNext<
            'a,
            HandleUpdateInput,
            WorkflowInterceptorFuture<'a, HandleUpdateResult>,
        >,
    ) -> WorkflowInterceptorFuture<'a, HandleUpdateResult> {
        let name = format!("HandleUpdate:{}", input.name());
        let attrs = vec![KeyValue::new(UPDATE_ID_ATTRIBUTE, input.id().to_owned())];
        workflow_handler!(self, ctx, input, next, name, attrs)
    }

    fn handle_query(
        &self,
        ctx: SyncWorkflowInterceptorContext,
        input: HandleQueryInput,
        next: WorkflowNext<'_, HandleQueryInput, HandleQueryResult>,
    ) -> HandleQueryResult {
        let parent = self.parent_for(input.headers());
        let _workflow_guard =
            WorkflowTelemetryStateGuard::enter(None, ctx.is_replaying_history_events());
        let context = start_span(
            &self.config,
            &parent,
            format!("HandleQuery:{}", input.name()),
            SpanKind::Server,
            Vec::new(),
        );
        let result =
            ctx.with_context_value::<CurrentOpenTelemetryContext, _>(context.clone(), || {
                let _guard = context.clone().attach();
                next.run(input)
            });
        finish_span(&context, &result);
        result
    }

    fn validate_update(
        &self,
        ctx: SyncWorkflowInterceptorContext,
        input: ValidateUpdateInput,
        next: WorkflowNext<'_, ValidateUpdateInput, ValidateUpdateResult>,
    ) -> ValidateUpdateResult {
        let parent = self.parent_for(input.headers());
        let _workflow_guard =
            WorkflowTelemetryStateGuard::enter(None, ctx.is_replaying_history_events());
        if ctx.is_replaying_history_events() {
            return ctx.with_context_value::<CurrentOpenTelemetryContext, _>(
                parent.clone(),
                || {
                    let _guard = parent.attach();
                    next.run(input)
                },
            );
        }
        let context = start_span(
            &self.config,
            &parent,
            format!("ValidateUpdate:{}", input.name()),
            SpanKind::Server,
            vec![KeyValue::new(UPDATE_ID_ATTRIBUTE, input.id().to_owned())],
        );
        let result =
            ctx.with_context_value::<CurrentOpenTelemetryContext, _>(context.clone(), || {
                let _guard = context.clone().attach();
                next.run(input)
            });
        finish_span(&context, &result);
        result
    }

    #[allow(clippy::result_large_err)]
    fn schedule_activity(
        &self,
        ctx: WorkflowInterceptorContext,
        mut input: ScheduleActivityInput,
        next: WorkflowNext<
            'static,
            ScheduleActivityInput,
            CancellableWorkflowOutboundFuture<ScheduleActivityResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<ScheduleActivityResult> {
        let parent = self.outbound_context(ctx.context_value::<CurrentOpenTelemetryContext>());
        let (context, recording) = start_workflow_span(
            &self.config,
            &ctx,
            &parent,
            format!("StartActivity:{}", input.activity_type()),
            SpanKind::Client,
            workflow_attributes(ctx.workflow_id(), Some(ctx.run_id())),
        );
        if recording {
            inject_into_fields(&self.config, &context, input.headers_mut());
        }
        next.run(input).map(move |result| {
            if recording {
                finish_span(&context, &result);
            }
            result
        })
    }

    #[allow(clippy::result_large_err)]
    fn schedule_local_activity(
        &self,
        ctx: WorkflowInterceptorContext,
        mut input: ScheduleLocalActivityInput,
        next: WorkflowNext<
            'static,
            ScheduleLocalActivityInput,
            CancellableWorkflowOutboundFuture<ScheduleActivityResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<ScheduleActivityResult> {
        let parent = self.outbound_context(ctx.context_value::<CurrentOpenTelemetryContext>());
        let (context, recording) = start_workflow_span(
            &self.config,
            &ctx,
            &parent,
            format!("StartActivity:{}", input.activity_type()),
            SpanKind::Client,
            workflow_attributes(ctx.workflow_id(), Some(ctx.run_id())),
        );
        if recording {
            inject_into_fields(&self.config, &context, input.headers_mut());
        }
        next.run(input).map(move |result| {
            if recording {
                finish_span(&context, &result);
            }
            result
        })
    }

    fn start_child_workflow(
        &self,
        ctx: WorkflowInterceptorContext,
        mut input: StartChildWorkflowInput,
        next: WorkflowNext<
            'static,
            StartChildWorkflowInput,
            CancellableWorkflowOutboundFuture<StartChildWorkflowResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<StartChildWorkflowResult> {
        let parent = self.outbound_context(ctx.context_value::<CurrentOpenTelemetryContext>());
        let (context, recording) = start_workflow_span(
            &self.config,
            &ctx,
            &parent,
            format!("StartChildWorkflow:{}", input.workflow_type()),
            SpanKind::Client,
            workflow_attributes(ctx.workflow_id(), Some(ctx.run_id())),
        );
        if recording {
            inject_into_fields(&self.config, &context, input.headers_mut());
        }
        next.run(input).map(move |result| {
            if recording {
                finish_span(&context, &result);
            }
            result
        })
    }

    fn signal_workflow(
        &self,
        ctx: WorkflowInterceptorContext,
        mut input: WfSignalInput,
        next: WorkflowNext<
            'static,
            WfSignalInput,
            CancellableWorkflowOutboundFuture<SignalWorkflowResult>,
        >,
    ) -> CancellableWorkflowOutboundFuture<SignalWorkflowResult> {
        let parent = self.outbound_context(ctx.context_value::<CurrentOpenTelemetryContext>());
        let (context, recording) = start_workflow_span(
            &self.config,
            &ctx,
            &parent,
            format!("SignalWorkflow:{}", input.signal_name()),
            SpanKind::Client,
            workflow_attributes(ctx.workflow_id(), Some(ctx.run_id())),
        );
        if recording {
            inject_into_fields(&self.config, &context, input.headers_mut());
        }
        next.run(input).map(move |result| {
            if recording {
                finish_span(&context, &result);
            }
            result
        })
    }

    fn cancel_external_workflow(
        &self,
        ctx: WorkflowInterceptorContext,
        input: CancelExternalWorkflowInput,
        next: WorkflowNext<
            'static,
            CancelExternalWorkflowInput,
            WorkflowOutboundFuture<CancelExternalWorkflowResult>,
        >,
    ) -> WorkflowOutboundFuture<CancelExternalWorkflowResult> {
        let parent = self.outbound_context(ctx.context_value::<CurrentOpenTelemetryContext>());
        let (context, recording) = start_workflow_span(
            &self.config,
            &ctx,
            &parent,
            "CancelWorkflow".to_owned(),
            SpanKind::Client,
            workflow_attributes(ctx.workflow_id(), Some(ctx.run_id())),
        );
        next.run(input).map(move |result| {
            if recording {
                finish_span(&context, &result);
            }
            result
        })
    }

    fn continue_as_new(
        &self,
        ctx: SyncWorkflowInterceptorContext,
        mut input: ContinueAsNewInput,
        next: WorkflowNext<'static, ContinueAsNewInput, ContinueAsNewResult>,
    ) -> ContinueAsNewResult {
        if !ctx.is_replaying() {
            let context = self.outbound_context(ctx.context_value::<CurrentOpenTelemetryContext>());
            inject_into_fields(&self.config, &context, input.headers_mut());
        }
        next.run(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::opentelemetry::{
        Context,
        baggage::BaggageExt,
        trace::{
            Span as _, SpanContext, SpanId, TraceFlags, TraceId, TraceState, TracerProvider as _,
        },
    };
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SimpleSpanProcessor};

    fn test_context(span_id: u64) -> Context {
        Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from(1),
            SpanId::from(span_id),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        ))
    }

    #[test]
    fn payload_wire_format_round_trips() {
        let carrier = Carrier(HashMap::from([
            (
                "traceparent".to_owned(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
            ),
            ("baggage".to_owned(), "tenant=acme".to_owned()),
        ]));
        let payload = carrier_payload(&carrier);

        assert_eq!(payload.metadata["encoding"], b"json/plain");
        assert_eq!(payload_carrier(&payload).unwrap().0, carrier.0);
    }

    #[test]
    fn default_propagator_uses_cross_sdk_header_and_baggage() {
        let plugin = OpenTelemetryPlugin::new();
        let remote = SpanContext::new(
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let context = Context::new()
            .with_remote_span_context(remote.clone())
            .with_baggage([KeyValue::new("tenant", "acme")]);
        let mut fields = HashMap::new();

        inject_into_fields(&plugin.config, &context, &mut fields);
        let extracted = extract_from_fields(&plugin.config, &fields);

        assert!(fields.contains_key(TRACE_HEADER_KEY));
        assert_eq!(extracted.span().span_context(), &remote);
        assert_eq!(
            extracted.baggage().get("tenant").unwrap().to_string(),
            "acme"
        );
    }

    #[test]
    fn configured_tracer_receives_temporal_span_data() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let plugin = OpenTelemetryPlugin::new().with_tracer(provider.tracer("test"));
        let context = start_span(
            &plugin.config,
            &Context::new(),
            "StartWorkflow:Example".to_owned(),
            SpanKind::Client,
            workflow_attributes("workflow-id", None),
        );

        finish_span::<(), &str>(&context, &Ok(()));

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "StartWorkflow:Example");
        assert_eq!(spans[0].span_kind, SpanKind::Client);
        assert!(
            spans[0]
                .attributes
                .contains(&KeyValue::new(WORKFLOW_ID_ATTRIBUTE, "workflow-id"))
        );
    }

    #[test]
    fn outbound_context_prefers_active_then_workflow_scoped_context() {
        let plugin = OpenTelemetryPlugin::new();
        let interceptor = OpenTelemetryWorkflowInterceptor::new(plugin.config);
        *interceptor.workflow_context.borrow_mut() = Some(test_context(1));
        let scoped = Rc::new(test_context(2));

        assert_eq!(
            interceptor
                .outbound_context(Some(scoped.clone()))
                .span()
                .span_context()
                .span_id(),
            SpanId::from(2)
        );

        let active = test_context(3);
        let _guard = active.attach();
        assert_eq!(
            interceptor
                .outbound_context(Some(scoped))
                .span()
                .span_context()
                .span_id(),
            SpanId::from(3)
        );
    }

    #[test]
    fn workflow_span_processor_suppresses_replay_spans() {
        let exporter = InMemorySpanExporter::default();
        let processor = WorkflowSpanProcessor::new(SimpleSpanProcessor::new(exporter.clone()));
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .build();
        let tracer = provider.tracer("test");

        {
            let _guard = WorkflowTelemetryStateGuard::enter(None, true);
            tracer.start("replayed").end();
        }
        tracer.start("live").end();

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "live");
    }

    #[test]
    fn plugin_appends_each_supported_interceptor() {
        let plugin = OpenTelemetryPlugin::new();
        let mut client_options = ClientOptions::new("namespace").build();
        ClientPlugin::configure_client_options(&plugin, &mut client_options).unwrap();
        assert_eq!(client_options.client_interceptors.len(), 1);
    }
}
