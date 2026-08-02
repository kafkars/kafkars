//! Stable failures for operational-metrics admission and observation.

use core::fmt;

use crate::driver::owner::observation::{
    DriverObservationAdmissionError, DriverObservationAdmissionErrorKind, DriverObservationError,
    DriverObservationErrorKind,
};
use crate::producer::ingress::ProducerShardLockError;

/// Stable category for a metrics request that never entered driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineMetricsAdmissionErrorKind {
    /// A bounded metrics admission owner is temporarily full or contended.
    Capacity,
    /// Driver admission has closed permanently.
    Closed,
    /// A metrics owner was unavailable, could not be notified, or rejected an
    /// impossible request category.
    HostUnavailable,
}

/// Immediate failure to admit one operational-metrics snapshot.
#[derive(Debug)]
pub struct EngineMetricsAdmissionError {
    kind: EngineMetricsAdmissionErrorKind,
    source: EngineMetricsAdmissionSource,
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
        Self {
            kind,
            source: EngineMetricsAdmissionSource::Driver(source),
        }
    }

    pub(crate) const fn from_producer(source: ProducerShardLockError) -> Self {
        let (kind, source) = match source {
            ProducerShardLockError::Contended => (
                EngineMetricsAdmissionErrorKind::Capacity,
                EngineMetricsAdmissionSource::ProducerContended,
            ),
            ProducerShardLockError::Poisoned => (
                EngineMetricsAdmissionErrorKind::HostUnavailable,
                EngineMetricsAdmissionSource::ProducerUnavailable,
            ),
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
        match &self.source {
            EngineMetricsAdmissionSource::Driver(source) => Some(source),
            EngineMetricsAdmissionSource::ProducerContended
            | EngineMetricsAdmissionSource::ProducerUnavailable => None,
        }
    }
}

#[derive(Debug)]
enum EngineMetricsAdmissionSource {
    Driver(DriverObservationAdmissionError),
    ProducerContended,
    ProducerUnavailable,
}

impl fmt::Display for EngineMetricsAdmissionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
            Self::ProducerContended => {
                formatter.write_str("producer metrics ownership is temporarily contended")
            }
            Self::ProducerUnavailable => {
                formatter.write_str("producer metrics ownership is unavailable")
            }
        }
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
