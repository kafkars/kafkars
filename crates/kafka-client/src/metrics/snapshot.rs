//! Top-level facade-owned view over one engine metrics snapshot.

use crate::bridge::client::metrics::ClientMetricsSnapshot;

use super::{CallMetrics, FailureMetrics, LatencyMetrics, MailboxMetrics};

/// One bounded point-in-time client operational snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsSnapshot {
    inner: ClientMetricsSnapshot,
}

impl MetricsSnapshot {
    pub(crate) const fn from_bridge(inner: ClientMetricsSnapshot) -> Self {
        Self { inner }
    }

    /// Returns cumulative broker-call lifecycle totals.
    pub const fn calls(&self) -> CallMetrics {
        CallMetrics::from_bridge(self.inner.calls())
    }

    /// Returns cumulative classified broker-call failures.
    pub const fn failures(&self) -> FailureMetrics {
        FailureMetrics::from_bridge(self.inner.failures())
    }

    /// Returns current bounded driver mailbox pressure and rejection totals.
    pub const fn mailbox(&self) -> MailboxMetrics {
        MailboxMetrics::from_bridge(self.inner.mailbox())
    }

    /// Returns cumulative call-stage and end-to-end latency summaries.
    pub const fn latency(&self) -> LatencyMetrics {
        LatencyMetrics::from_bridge(self.inner.latency())
    }
}
