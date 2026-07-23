//! Named single-observer delivery operation over the private engine bridge.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError, RecordMetadata, bridge::producer_delivery::ProducerDelivery as BridgeDelivery,
};

use super::CancellationOutcome;

/// Sole terminal observer for one record accepted by `Producer::try_send`.
///
/// Dropping a delivery abandons observation only. It does not cancel engine
/// work or imply that Kafka did not receive the record.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted producer work"]
pub struct Delivery {
    inner: BridgeDelivery,
}

impl Delivery {
    pub(crate) const fn from_bridge(inner: BridgeDelivery) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by `Future::poll`.
    pub fn wait(self) -> Result<RecordMetadata, KafkaError> {
        self.inner.wait()
    }

    /// Attempts stage-aware cancellation without consuming terminal observation.
    ///
    /// `CancelledNotSent` means the engine proved transport never owned the
    /// operation. `TooLate` does not imply delivery, only that cancellation can
    /// no longer make a not-sent promise. The attempt never waits for shard
    /// ownership; a contended attempt reports `Backpressure` and may be retried
    /// on this same delivery. Dropping a delivery remains observation
    /// abandonment and never invokes this method.
    pub fn cancel(&mut self) -> Result<CancellationOutcome, KafkaError> {
        self.inner.cancel()
    }
}

impl Future for Delivery {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
