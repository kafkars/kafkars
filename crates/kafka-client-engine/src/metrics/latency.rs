//! Curated duration summaries for each broker-call lifecycle stage.

use std::time::Duration;

use crate::driver::owner::observation::{DriverLatencyMetric, DriverLatencyMetrics};

/// Cumulative monotonic duration summaries for broker-call stages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineLatencyMetrics(DriverLatencyMetrics);

impl EngineLatencyMetrics {
    pub(super) const fn from_driver(inner: DriverLatencyMetrics) -> Self {
        Self(inner)
    }

    /// Returns submission-to-reactor-admission latency.
    pub const fn mailbox(self) -> EngineLatencyMetric {
        EngineLatencyMetric::from_driver(self.0.mailbox())
    }

    /// Returns reactor-admission-to-route latency.
    pub const fn routing(self) -> EngineLatencyMetric {
        EngineLatencyMetric::from_driver(self.0.routing())
    }

    /// Returns route-to-frame-preparation latency.
    pub const fn preparation(self) -> EngineLatencyMetric {
        EngineLatencyMetric::from_driver(self.0.preparation())
    }

    /// Returns preparation-to-writer-admission latency.
    pub const fn writer_admission(self) -> EngineLatencyMetric {
        EngineLatencyMetric::from_driver(self.0.writer_admission())
    }

    /// Returns writer-admission-to-terminal latency.
    pub const fn in_flight(self) -> EngineLatencyMetric {
        EngineLatencyMetric::from_driver(self.0.in_flight())
    }

    /// Returns public submission-to-terminal latency.
    pub const fn end_to_end(self) -> EngineLatencyMetric {
        EngineLatencyMetric::from_driver(self.0.end_to_end())
    }

    /// Returns deadline settlement lateness.
    pub const fn deadline_lateness(self) -> EngineLatencyMetric {
        EngineLatencyMetric::from_driver(self.0.deadline_lateness())
    }
}

/// Count, saturating total, and maximum for one latency stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineLatencyMetric(DriverLatencyMetric);

impl EngineLatencyMetric {
    const fn from_driver(inner: DriverLatencyMetric) -> Self {
        Self(inner)
    }

    /// Returns the number of completed observations.
    pub const fn samples(self) -> u64 {
        self.0.samples()
    }

    /// Returns the saturating sum of observed durations.
    pub const fn total(self) -> Duration {
        self.0.total()
    }

    /// Returns the largest observed duration.
    pub const fn max(self) -> Duration {
        self.0.max()
    }
}
