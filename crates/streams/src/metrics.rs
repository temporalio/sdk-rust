//! The crate's metrics. They use the meter that lang gives, or a no-op meter.

use temporalio_common::telemetry::metrics::{
    Counter, MetricAttributes, MetricParameters, TemporalMeter, UpDownCounter,
};

#[derive(Clone)]
pub(crate) struct Metrics {
    attributes: MetricAttributes,
    fenced: Counter,
    blocking_reads_waiting: UpDownCounter,
}

fn parameters(
    name: &'static str,
    description: &'static str,
    unit: &'static str,
) -> MetricParameters {
    MetricParameters {
        name: name.into(),
        description: description.into(),
        unit: unit.into(),
    }
}

impl Metrics {
    pub(crate) fn new(meter: &TemporalMeter) -> Self {
        Self {
            attributes: meter.get_default_attributes().clone(),
            fenced: meter.counter(parameters(
                "stream_fenced",
                "Batches that the Store refused for a lower epoch",
                "",
            )),
            blocking_reads_waiting: meter.up_down_counter(parameters(
                "stream_blocking_reads_waiting",
                "Reads that wait on the Store",
                "",
            )),
        }
    }

    pub(crate) fn fenced(&self) {
        self.fenced.add(1, &self.attributes);
    }

    pub(crate) fn read_waits(&self, waiting: bool) {
        self.blocking_reads_waiting
            .add(if waiting { 1 } else { -1 }, &self.attributes);
    }
}
