//! Driver-owned snapshot admission and single-consumer completion.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_driver::{Call, Driver, DriverSnapshot, SnapshotError};

use super::{DriverMetricsSnapshot, DriverObservationAdmissionError, DriverObservationError};

pub(crate) struct DriverObservationHandle {
    driver: Driver,
}

pub(crate) struct DriverObservation {
    inner: Call<Result<DriverSnapshot, SnapshotError>>,
}

impl DriverObservationHandle {
    pub(crate) const fn new(driver: Driver) -> Self {
        Self { driver }
    }

    pub(crate) fn observe(&self) -> Result<DriverObservation, DriverObservationAdmissionError> {
        self.driver
            .snapshot()
            .map(DriverObservation::new)
            .map_err(DriverObservationAdmissionError::from_driver)
    }
}

impl DriverObservation {
    const fn new(inner: Call<Result<DriverSnapshot, SnapshotError>>) -> Self {
        Self { inner }
    }

    pub(crate) fn wait(self) -> Result<DriverMetricsSnapshot, DriverObservationError> {
        translate(self.inner.wait())
    }
}

impl Future for DriverObservation {
    type Output = Result<DriverMetricsSnapshot, DriverObservationError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(context)
            .map(translate)
    }
}

fn translate(
    result: Result<Result<DriverSnapshot, SnapshotError>, kafka_driver::CompletionError>,
) -> Result<DriverMetricsSnapshot, DriverObservationError> {
    result
        .map_err(DriverObservationError::completion)?
        .map(DriverMetricsSnapshot::from_driver)
        .map_err(DriverObservationError::draining)
}
