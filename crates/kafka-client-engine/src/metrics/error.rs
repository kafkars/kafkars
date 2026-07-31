//! Stable failures for operational-metrics admission and observation.

use core::fmt;

use crate::driver::owner::observation::{
    DriverObservationAdmissionError, DriverObservationAdmissionErrorKind, DriverObservationError,
    DriverObservationErrorKind,
};

/// Stable category for a metrics request that never entered driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineMetricsAdmissionErrorKind {
    /// The bounded driver command lane is full.
    Capacity,
    /// Driver admission has closed permanently.
    Closed,
    /// The poller could not be notified or an impossible request category appeared.
    HostUnavailable,
}

/// Immediate failure to admit one operational-metrics snapshot.
#[derive(Debug)]
pub struct EngineMetricsAdmissionError {
    kind: EngineMetricsAdmissionErrorKind,
    source: DriverObservationAdmissionError,
}

impl EngineMetricsAdmissionError {
    pub(crate) const fn from_driver(source: DriverObservationAdmissionError) -> Self {
        let kind = match source.kind() {
            DriverObservationAdmissionErrorKind::Capacity => {
                EngineMetricsAdmissionErrorKind::Capacity
            }
            DriverObservationAdmissionErrorKind::Closed => EngineMetricsAdmissionErrorKind::Closed,
            DriverObservationAdmissionErrorKind::HostUnavailable => {
                EngineMetricsAdmissionErrorKind::HostUnavailable
            }
        };
        Self { kind, source }
    }

    /// Returns the stable admission-failure category.
    pub const fn kind(&self) -> EngineMetricsAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for EngineMetricsAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "metrics admission failed: {}", self.source)
    }
}

impl std::error::Error for EngineMetricsAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Stable category for failure after metrics observation was admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineMetricsObserverErrorKind {
    /// Graceful shutdown won before the reactor interpreted the snapshot.
    Draining,
    /// The snapshot completion owner disappeared or was observed twice.
    Completion,
}

/// Failure to observe one accepted operational-metrics snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineMetricsObserverError {
    kind: EngineMetricsObserverErrorKind,
}

impl EngineMetricsObserverError {
    pub(crate) const fn from_driver(source: DriverObservationError) -> Self {
        let kind = match source.kind() {
            DriverObservationErrorKind::Draining => EngineMetricsObserverErrorKind::Draining,
            DriverObservationErrorKind::Completion => EngineMetricsObserverErrorKind::Completion,
        };
        Self { kind }
    }

    /// Returns the stable observation-failure category.
    pub const fn kind(self) -> EngineMetricsObserverErrorKind {
        self.kind
    }
}

impl fmt::Display for EngineMetricsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            EngineMetricsObserverErrorKind::Draining => {
                formatter.write_str("the client began draining before metrics were captured")
            }
            EngineMetricsObserverErrorKind::Completion => {
                formatter.write_str("the metrics completion owner became unavailable")
            }
        }
    }
}

impl std::error::Error for EngineMetricsObserverError {}
