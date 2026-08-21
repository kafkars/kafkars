//! Runtime-neutral observer for one accepted metrics snapshot.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::client::metrics::ClientMetricsObserver};

use super::MetricsSnapshot;

/// Sole observer for one accepted operational snapshot with staged capture points.
#[must_use = "dropping abandons observation without cancelling accepted metrics work"]
pub struct Metrics {
    inner: ClientMetricsObserver,
}

impl Metrics {
    pub(crate) const fn from_bridge(inner: ClientMetricsObserver) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observation used by [`Future::poll`].
    pub fn wait(self) -> Result<MetricsSnapshot, KafkaError> {
        self.inner.wait().map(MetricsSnapshot::from_bridge)
    }
}

impl Future for Metrics {
    type Output = Result<MetricsSnapshot, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(context)
            .map(|result| result.map(MetricsSnapshot::from_bridge))
    }
}

impl fmt::Debug for Metrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Metrics").finish_non_exhaustive()
    }
}
