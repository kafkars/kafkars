//! Curated fixed-size projection of one complete driver metrics snapshot.

use crate::driver::owner::observation::DriverMetricsSnapshot;

use super::{EngineCallMetrics, EngineFailureMetrics, EngineLatencyMetrics, EngineMailboxMetrics};

/// One bounded point-in-time operational snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineMetricsSnapshot {
    inner: DriverMetricsSnapshot,
}

impl EngineMetricsSnapshot {
    pub(crate) const fn from_driver(inner: DriverMetricsSnapshot) -> Self {
        Self { inner }
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
}
