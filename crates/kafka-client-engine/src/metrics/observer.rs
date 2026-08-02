//! Single-consumer observation pairing caller- and reactor-owned metrics.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::Engine;
use crate::driver::owner::observation::DriverObservation;

use super::{
    EngineMetricsAdmissionError, EngineMetricsObserverError, EngineMetricsSnapshot,
    EngineProducerMetrics,
};

/// Sole observer for one accepted operational snapshot with staged capture points.
#[must_use = "dropping abandons observation without cancelling accepted metrics work"]
pub struct EngineMetricsObserver {
    inner: DriverObservation,
    producer: EngineProducerMetrics,
}

impl Engine {
    /// Requests one bounded operational view at this public admission boundary.
    pub fn metrics(&self) -> Result<EngineMetricsObserver, EngineMetricsAdmissionError> {
        let producer = self
            .inner
            .admission
            .try_shard_stats()
            .map(EngineProducerMetrics::from_shard)
            .map_err(EngineMetricsAdmissionError::from_producer)?;
        self.inner
            .metrics
            .observe()
            .map(|inner| EngineMetricsObserver::new(inner, producer))
            .map_err(EngineMetricsAdmissionError::from_driver)
    }
}

impl EngineMetricsObserver {
    const fn new(inner: DriverObservation, producer: EngineProducerMetrics) -> Self {
        Self { inner, producer }
    }

    /// Blocks on the same terminal observation used by [`Future::poll`].
    pub fn wait(self) -> Result<EngineMetricsSnapshot, EngineMetricsObserverError> {
        self.inner
            .wait()
            .map(|driver| EngineMetricsSnapshot::from_parts(driver, self.producer))
            .map_err(EngineMetricsObserverError::from_driver)
    }
}

impl Future for EngineMetricsObserver {
    type Output = Result<EngineMetricsSnapshot, EngineMetricsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context).map(|result| {
            result
                .map(|driver| EngineMetricsSnapshot::from_parts(driver, this.producer))
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
