//! Public cumulative broker-call lifecycle metrics.

use crate::bridge::client::metrics::ClientCallMetrics;

/// Cumulative broker-call admission, completion, and delivery totals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallMetrics(ClientCallMetrics);

impl CallMetrics {
    pub(super) const fn from_bridge(inner: ClientCallMetrics) -> Self {
        Self(inner)
    }

    /// Returns calls accepted for driver interpretation.
    pub const fn admitted(self) -> u64 {
        self.0.admitted()
    }

    /// Returns calls completed with a generated response.
    pub const fn succeeded(self) -> u64 {
        self.0.succeeded()
    }

    /// Returns calls completed with a typed failure.
    pub const fn failed(self) -> u64 {
        self.0.failed()
    }

    /// Returns terminal values discarded after observer abandonment.
    pub const fn observer_abandoned(self) -> u64 {
        self.0.observer_abandoned()
    }

    /// Returns failures known not to have crossed transport ownership.
    pub const fn not_sent(self) -> u64 {
        self.0.not_sent()
    }

    /// Returns failures whose requests may have reached Kafka.
    pub const fn possibly_sent(self) -> u64 {
        self.0.possibly_sent()
    }
}
