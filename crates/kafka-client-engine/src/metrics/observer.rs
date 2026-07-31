//! Single-consumer observation of one reactor-owned metrics snapshot.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::Engine;
use crate::driver::owner::observation::DriverObservation;

use super::{EngineMetricsAdmissionError, EngineMetricsObserverError, EngineMetricsSnapshot};

/// Sole observer for one accepted point-in-time operational snapshot.
#[must_use = "dropping abandons observation without cancelling accepted metrics work"]
pub struct EngineMetricsObserver {
    inner: DriverObservation,
}

impl Engine {
    /// Requests one bounded point-in-time view of driver operational metrics.
    pub fn metrics(&self) -> Result<EngineMetricsObserver, EngineMetricsAdmissionError> {
        self.inner
            .metrics
            .observe()
            .map(EngineMetricsObserver::new)
            .map_err(EngineMetricsAdmissionError::from_driver)
    }
}

impl EngineMetricsObserver {
    const fn new(inner: DriverObservation) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observation used by [`Future::poll`].
    pub fn wait(self) -> Result<EngineMetricsSnapshot, EngineMetricsObserverError> {
        self.inner
            .wait()
            .map(EngineMetricsSnapshot::from_driver)
            .map_err(EngineMetricsObserverError::from_driver)
    }
}

impl Future for EngineMetricsObserver {
    type Output = Result<EngineMetricsSnapshot, EngineMetricsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(context)
            .map(|result| {
                result
                    .map(EngineMetricsSnapshot::from_driver)
                    .map_err(EngineMetricsObserverError::from_driver)
            })
    }
}

impl fmt::Debug for EngineMetricsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EngineMetricsObserver")
            .finish_non_exhaustive()
    }
}
