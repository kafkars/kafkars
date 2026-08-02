//! Curated fixed-size projection of staged engine metrics capture.

use crate::driver::owner::observation::DriverMetricsSnapshot;

use super::{
    EngineCallMetrics, EngineFailureMetrics, EngineLatencyMetrics, EngineMailboxMetrics,
    EngineProducerMetrics,
};

/// One bounded operational snapshot with explicit owner capture points.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineMetricsSnapshot {
    inner: DriverMetricsSnapshot,
    producer: EngineProducerMetrics,
}

impl EngineMetricsSnapshot {
    pub(crate) const fn from_parts(
        inner: DriverMetricsSnapshot,
        producer: EngineProducerMetrics,
    ) -> Self {
        Self { inner, producer }
    }

    /// Returns cumulative broker-call lifecycle totals.
    pub const fn calls(&self) -> EngineCallMetrics {
        EngineCallMetrics::from_driver(self.inner.calls())
    }

    /// Returns cumulative classified broker-call failures.
    pub const fn failures(&self) -> EngineFailureMetrics {
        EngineFailureMetrics::from_driver(self.inner.failures())
    }

    /// Returns current bounded driver mailbox pressure and rejection totals.
    pub const fn mailbox(&self) -> EngineMailboxMetrics {
        EngineMailboxMetrics::from_driver(self.inner.mailbox())
    }

    /// Returns cumulative call-stage and end-to-end latency summaries.
    pub const fn latency(&self) -> EngineLatencyMetrics {
        EngineLatencyMetrics::from_driver(self.inner.latency())
    }

    /// Returns producer ownership captured before driver observation admission.
    ///
    /// Driver counters are captured later by the reactor and are not an atomic
    /// cross-owner snapshot with these producer fields.
    pub const fn producer(&self) -> EngineProducerMetrics {
        self.producer
    }
}
