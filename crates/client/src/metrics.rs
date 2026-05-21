use crate::{AttachMetricLabels, CallType, callback_based, dbg_panic};
use bytes::Bytes;
use futures_util::{
    FutureExt, TryFutureExt,
    future::{BoxFuture, Either},
};
use http_body_util::BodyExt;
use std::{
    fmt,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use temporalio_common::telemetry::{
    TaskQueueLabelStrategy,
    metrics::{
        Counter, CounterBase, Histogram, HistogramBase, HistogramDuration, HistogramDurationBase,
        MESSAGE_DIRECTION_REQUEST, MESSAGE_DIRECTION_RESPONSE, MetricAttributable,
        MetricAttributes, MetricKeyValue, MetricParameters, TemporalMeter, message_direction,
    },
};
use tonic::{Code, body::Body, transport::Channel};
use tower::Service;

pub use temporalio_common::telemetry::metrics::RPC_MESSAGE_SIZE_HISTOGRAM_NAME;

/// The string name (which may be prefixed) for this metric
pub static REQUEST_LATENCY_HISTOGRAM_NAME: &str = "request_latency";
/// The string name (which may be prefixed) for this metric
pub static LONG_REQUEST_LATENCY_HISTOGRAM_NAME: &str = "long_request_latency";

/// Used to track context associated with metrics, and record/update them
#[derive(Clone, derive_more::Debug)]
#[debug("MetricsContext {{ poll_is_long: {poll_is_long} }}")]
pub(crate) struct MetricsContext {
    meter: TemporalMeter,
    poll_is_long: bool,
    instruments: Instruments,
}
#[derive(Clone)]
struct Instruments {
    svc_request: Counter,
    svc_request_failed: Counter,
    long_svc_request: Counter,
    long_svc_request_failed: Counter,

    svc_request_latency: HistogramDuration,
    long_svc_request_latency: HistogramDuration,
    rpc_message_size: Histogram,
}

impl MetricsContext {
    pub(crate) fn new(tm: TemporalMeter) -> Self {
        let instruments = Instruments {
            svc_request: tm.counter(MetricParameters {
                name: "request".into(),
                description: "Count of client request successes by rpc name".into(),
                unit: "".into(),
            }),
            svc_request_failed: tm.counter(MetricParameters {
                name: "request_failure".into(),
                description: "Count of client request failures by rpc name".into(),
                unit: "".into(),
            }),
            long_svc_request: tm.counter(MetricParameters {
                name: "long_request".into(),
                description: "Count of long-poll request successes by rpc name".into(),
                unit: "".into(),
            }),
            long_svc_request_failed: tm.counter(MetricParameters {
                name: "long_request_failure".into(),
                description: "Count of long-poll request failures by rpc name".into(),
                unit: "".into(),
            }),
            svc_request_latency: tm.histogram_duration(MetricParameters {
                name: REQUEST_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of client request latencies".into(),
            }),
            long_svc_request_latency: tm.histogram_duration(MetricParameters {
                name: LONG_REQUEST_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of client long-poll request latencies".into(),
            }),
            rpc_message_size: tm.histogram(MetricParameters {
                name: RPC_MESSAGE_SIZE_HISTOGRAM_NAME.into(),
                unit: "By".into(),
                description: "Histogram of client gRPC request and response body sizes".into(),
            }),
        };
        Self {
            poll_is_long: false,
            instruments,
            meter: tm,
        }
    }

    /// Mutate this metrics context with new attributes
    pub(crate) fn with_new_attrs(&mut self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) {
        self.meter.merge_attributes(new_kvs.into());

        let _ = self
            .instruments
            .svc_request
            .with_attributes(self.meter.get_default_attributes())
            .and_then(|v| {
                self.instruments.svc_request = v;
                self.instruments
                    .long_svc_request
                    .with_attributes(self.meter.get_default_attributes())
            })
            .and_then(|v| {
                self.instruments.long_svc_request = v;
                self.instruments
                    .svc_request_latency
                    .with_attributes(self.meter.get_default_attributes())
            })
            .and_then(|v| {
                self.instruments.svc_request_latency = v;
                self.instruments
                    .long_svc_request_latency
                    .with_attributes(self.meter.get_default_attributes())
            })
            .and_then(|v| {
                self.instruments.long_svc_request_latency = v;
                self.instruments
                    .rpc_message_size
                    .with_attributes(self.meter.get_default_attributes())
            })
            .map(|v| {
                self.instruments.rpc_message_size = v;
            })
            .inspect_err(|e| {
                dbg_panic!("Failed to extend client metrics attributes: {:?}", e);
            });
    }

    pub(crate) fn set_is_long_poll(&mut self) {
        self.poll_is_long = true;
    }

    /// A request to the temporal service was made
    pub(crate) fn svc_request(&self) {
        if self.poll_is_long {
            self.instruments.long_svc_request.adds(1);
        } else {
            self.instruments.svc_request.adds(1);
        }
    }

    /// A request to the temporal service failed
    pub(crate) fn svc_request_failed(&self, code: Option<Code>) {
        self.svc_request_failed_with_label(code.map(status_code_kv));
    }

    /// A request to the temporal service failed due to a transport-level error
    /// (no gRPC status received from the server).
    pub(crate) fn svc_request_failed_transport(&self) {
        self.svc_request_failed_with_label(Some(transport_error_kv()));
    }

    fn svc_request_failed_with_label(&self, label: Option<MetricKeyValue>) {
        let refme: MetricAttributes;
        let kvs = if let Some(kv) = label {
            refme = self
                .meter
                .extend_attributes(self.meter.get_default_attributes().clone(), [kv].into());
            &refme
        } else {
            self.meter.get_default_attributes()
        };
        if self.poll_is_long {
            self.instruments.long_svc_request_failed.add(1, kvs);
        } else {
            self.instruments.svc_request_failed.add(1, kvs);
        }
    }

    /// Record service request latency
    pub(crate) fn record_svc_req_latency(&self, dur: Duration) {
        if self.poll_is_long {
            self.instruments.long_svc_request_latency.records(dur);
        } else {
            self.instruments.svc_request_latency.records(dur);
        }
    }

    pub(crate) fn rpc_message_size(&self, size_bytes: u64) {
        self.instruments.rpc_message_size.records(size_bytes);
    }
}

const KEY_NAMESPACE: &str = "namespace";
const KEY_SVC_METHOD: &str = "operation";
const KEY_TASK_QUEUE: &str = "task_queue";
const KEY_STATUS_CODE: &str = "status_code";

pub(crate) fn namespace_kv(ns: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_NAMESPACE, ns)
}

pub(crate) fn task_queue_kv(tq: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_TASK_QUEUE, tq)
}

pub(crate) fn svc_operation(op: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_SVC_METHOD, op)
}

pub(crate) fn status_code_kv(code: Code) -> MetricKeyValue {
    MetricKeyValue::new(KEY_STATUS_CODE, code_as_screaming_snake(&code))
}

fn transport_error_kv() -> MetricKeyValue {
    MetricKeyValue::new(KEY_STATUS_CODE, "TRANSPORT_ERROR")
}

/// This is done to match the way Java sdk labels these codes (and also matches gRPC spec)
fn code_as_screaming_snake(code: &Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

struct BodySizeRecorder {
    size_bytes: u64,
    record: Box<dyn Fn(u64) + Send + Sync>,
}

impl BodySizeRecorder {
    fn new(record: impl Fn(u64) + Send + Sync + 'static) -> Self {
        Self {
            size_bytes: 0,
            record: Box::new(record),
        }
    }

    fn add_frame(&mut self, frame: &Bytes) {
        self.size_bytes += frame.len() as u64;
    }
}

impl Drop for BodySizeRecorder {
    fn drop(&mut self) {
        (self.record)(self.size_bytes);
    }
}

fn body_with_size_recorder(body: Body, recorder: BodySizeRecorder) -> Body {
    let mut recorder = recorder;
    Body::new(body.map_frame(move |frame| {
        if let Some(data) = frame.data_ref() {
            recorder.add_frame(data);
        }
        frame
    }))
}

fn body_size_recorder(metrics: MetricsContext) -> BodySizeRecorder {
    BodySizeRecorder::new(move |size_bytes| metrics.rpc_message_size(size_bytes))
}

/// Implements metrics functionality for gRPC (really, any http) calls
#[derive(Debug, Clone)]
pub(crate) struct GrpcMetricSvc {
    pub(crate) inner: ChannelOrGrpcOverride,
    // If set to none, metrics are a no-op
    pub(crate) metrics: Option<MetricsContext>,
    pub(crate) disable_errcode_label: bool,
}

#[derive(Clone)]
pub(crate) enum ChannelOrGrpcOverride {
    Channel(Channel),
    GrpcOverride(callback_based::CallbackBasedGrpcService),
}

impl fmt::Debug for ChannelOrGrpcOverride {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChannelOrGrpcOverride::Channel(inner) => fmt::Debug::fmt(inner, f),
            ChannelOrGrpcOverride::GrpcOverride(_) => f.write_str("<callback-based-grpc-service>"),
        }
    }
}

// TODO: Rewrite as a RawGrpcCaller implementation
impl Service<http::Request<Body>> for GrpcMetricSvc {
    type Response = http::Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.inner {
            ChannelOrGrpcOverride::Channel(inner) => inner.poll_ready(cx).map_err(Into::into),
            ChannelOrGrpcOverride::GrpcOverride(inner) => inner.poll_ready(cx).map_err(Into::into),
        }
    }

    fn call(&mut self, mut req: http::Request<Body>) -> Self::Future {
        let metrics = self
            .metrics
            .clone()
            .map(|mut m| {
                // Attach labels from client wrapper
                if let Some(other_labels) = req.extensions_mut().remove::<AttachMetricLabels>() {
                    m.with_new_attrs(other_labels.labels);

                    if other_labels.normal_task_queue.is_some()
                        || other_labels.sticky_task_queue.is_some()
                    {
                        let task_queue_name = match m.meter.get_task_queue_label_strategy() {
                            TaskQueueLabelStrategy::UseNormal => other_labels.normal_task_queue,
                            TaskQueueLabelStrategy::UseNormalAndSticky => other_labels
                                .sticky_task_queue
                                .or(other_labels.normal_task_queue),
                            _ => other_labels.normal_task_queue,
                        };

                        if let Some(tq_name) = task_queue_name {
                            m.with_new_attrs([task_queue_kv(tq_name)]);
                        }
                    }
                }
                if let Some(ct) = req.extensions().get::<CallType>()
                    && ct.is_long()
                {
                    m.set_is_long_poll();
                }
                m
            })
            .and_then(|mut metrics| {
                // Attach method name label if possible
                req.uri().to_string().rsplit_once('/').map(|split_tup| {
                    let method_name = split_tup.1;
                    metrics.with_new_attrs([svc_operation(method_name.to_string())]);
                    metrics.svc_request();
                    metrics
                })
            });
        if let Some(metrics) = metrics.as_ref() {
            let mut req_metrics = metrics.clone();
            req_metrics.with_new_attrs([message_direction(MESSAGE_DIRECTION_REQUEST)]);
            req = req.map(|body| body_with_size_recorder(body, body_size_recorder(req_metrics)));
        }
        let callfut = match &mut self.inner {
            ChannelOrGrpcOverride::Channel(inner) => {
                Either::Left(inner.call(req).map_err(Into::into))
            }
            ChannelOrGrpcOverride::GrpcOverride(inner) => {
                Either::Right(inner.call(req).map_err(Into::into))
            }
        };
        let errcode_label_disabled = self.disable_errcode_label;
        async move {
            let started = Instant::now();
            let res = callfut.await;
            if let Some(metrics) = metrics.as_ref() {
                metrics.record_svc_req_latency(started.elapsed());
                match res {
                    Ok(ref ok_res) => {
                        if let Some(number) = ok_res
                            .headers()
                            .get("grpc-status")
                            .and_then(|s| s.to_str().ok())
                            .and_then(|s| s.parse::<i32>().ok())
                        {
                            let code = Code::from(number);
                            if code != Code::Ok {
                                let code = if errcode_label_disabled {
                                    None
                                } else {
                                    Some(code)
                                };
                                metrics.svc_request_failed(code);
                            }
                        }
                    }
                    Err(_) => {
                        // Transport-level errors (connection closed, GOAWAY, etc.) never
                        // produce a grpc-status header. Record them so they are visible
                        // in dashboards rather than silently disappearing.
                        if !errcode_label_disabled {
                            metrics.svc_request_failed_transport();
                        } else {
                            metrics.svc_request_failed(None);
                        }
                    }
                }
            }
            match (res, metrics) {
                (Ok(res), Some(metrics)) => {
                    let mut resp_metrics = metrics;
                    resp_metrics.with_new_attrs([message_direction(MESSAGE_DIRECTION_RESPONSE)]);
                    Ok(res.map(|body| {
                        body_with_size_recorder(body, body_size_recorder(resp_metrics))
                    }))
                }
                (res, _) => res,
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    #[tokio::test]
    async fn body_size_recorder_counts_data_frames() {
        let recorded = Arc::new(AtomicU64::new(0));
        let recorded_clone = recorded.clone();
        let body = Body::new(Full::new(Bytes::from_static(b"hello")));
        let body = body_with_size_recorder(
            body,
            BodySizeRecorder::new(move |size_bytes| {
                recorded_clone.store(size_bytes, Ordering::Relaxed);
            }),
        );

        let _ = body.collect().await.unwrap();

        assert_eq!(recorded.load(Ordering::Relaxed), 5);
    }
}
