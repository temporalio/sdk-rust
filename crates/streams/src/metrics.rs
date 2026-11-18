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

    /// Counts a read as waiting until the guard drops. The guard drops with the read's future when
    /// a caller drops it, so the count never stays up.
    pub(crate) fn read_waiting(&self) -> ReadWaiting<'_> {
        self.blocking_reads_waiting.add(1, &self.attributes);
        ReadWaiting(self)
    }
}

pub(crate) struct ReadWaiting<'a>(&'a Metrics);

impl Drop for ReadWaiting<'_> {
    fn drop(&mut self) {
        let metrics = self.0;
        metrics.blocking_reads_waiting.add(-1, &metrics.attributes);
    }
}
