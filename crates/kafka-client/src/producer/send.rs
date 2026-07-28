//! Named ergonomic producer operation spanning local waiting and delivery.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, RecordMetadata, bridge::producer::ProducerSend as BridgeProducerSend};

use super::CancellationOutcome;

/// One producer call that waits within configured local bounds.
///
/// Dropping before active admission cancels with a definitely-not-sent
/// terminal and releases the exact waiting bytes. Dropping after active
/// admission abandons observation without cancelling transport work.
#[derive(Debug)]
#[must_use = "dropping before admission cancels; dropping after admission abandons observation"]
pub struct Send {
    inner: BridgeProducerSend,
}

impl Send {
    pub(crate) const fn from_bridge(inner: BridgeProducerSend) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal path used by `Future::poll`.
    pub fn wait(self) -> Result<RecordMetadata, KafkaError> {
        self.inner.wait()
    }

    /// Attempts cancellation without retry polling.
    pub fn cancel(&mut self) -> Result<CancellationOutcome, KafkaError> {
        self.inner.cancel()
    }
}

impl Future for Send {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
