//! Driver-adapter facade for bounded operational observation.

mod error;
mod observer;
mod snapshot;

pub(crate) use error::{
    DriverObservationAdmissionError, DriverObservationAdmissionErrorKind, DriverObservationError,
    DriverObservationErrorKind,
};
pub(crate) use observer::{DriverObservation, DriverObservationHandle};
pub(crate) use snapshot::{
    DriverCallMetrics, DriverFailureMetrics, DriverLatencyMetric, DriverLatencyMetrics,
    DriverMailboxMetrics, DriverMetricsSnapshot,
};
