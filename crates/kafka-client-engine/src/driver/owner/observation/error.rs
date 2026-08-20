//! Driver-neutral observation failure categories at the engine adapter.

use core::fmt;

use kafka_driver::{CompletionError, SnapshotError, SubmitError};

/// Stable adapter category for a snapshot that never entered driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DriverObservationAdmissionErrorKind {
    Capacity,
    Closed,
    HostUnavailable,
}

/// Immediate adapter failure to admit one driver snapshot.
#[derive(Debug)]
pub(crate) struct DriverObservationAdmissionError {
    kind: DriverObservationAdmissionErrorKind,
    source: SubmitError,
}

impl DriverObservationAdmissionError {
    #[allow(
        clippy::match_same_arms,
        unreachable_patterns,
        reason = "the published driver RC exposes a non-exhaustive admission error while the reviewed path dependency is exhaustive"
    )]
    pub(super) const fn from_driver(source: SubmitError) -> Self {
        let kind = match source {
            SubmitError::Full => DriverObservationAdmissionErrorKind::Capacity,
            SubmitError::Closed => DriverObservationAdmissionErrorKind::Closed,
            SubmitError::Wake(_)
            | SubmitError::IdentityExhausted
            | SubmitError::ForeignDriver
            | SubmitError::VersionBoundsInvalid { .. } => {
                DriverObservationAdmissionErrorKind::HostUnavailable
            }
            _ => DriverObservationAdmissionErrorKind::HostUnavailable,
        };
        Self { kind, source }
    }

    pub(crate) const fn kind(&self) -> DriverObservationAdmissionErrorKind {
        self.kind
    }
}

impl fmt::Display for DriverObservationAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for DriverObservationAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Stable adapter category for failure after snapshot admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DriverObservationErrorKind {
    Draining,
    Completion,
}

/// Adapter failure to observe one admitted driver snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DriverObservationError {
    kind: DriverObservationErrorKind,
}

impl DriverObservationError {
    pub(super) const fn draining(_source: SnapshotError) -> Self {
        Self {
            kind: DriverObservationErrorKind::Draining,
        }
    }

    pub(super) const fn completion(_source: CompletionError) -> Self {
        Self {
            kind: DriverObservationErrorKind::Completion,
        }
    }

    pub(crate) const fn kind(self) -> DriverObservationErrorKind {
        self.kind
    }
}

impl fmt::Display for DriverObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            DriverObservationErrorKind::Draining => {
                formatter.write_str("driver draining before snapshot interpretation")
            }
            DriverObservationErrorKind::Completion => {
                formatter.write_str("driver snapshot completion unavailable")
            }
        }
    }
}

impl std::error::Error for DriverObservationError {}
