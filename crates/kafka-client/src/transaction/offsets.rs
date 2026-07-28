//! Named runtime-neutral observation of one accepted transactional offset transfer.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::transaction::TransactionOffsetsEngine};

/// Sole terminal observer for one accepted transactional offset transfer.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling the accepted transfer"]
pub struct SendTransactionOffsets<'send, 'producer> {
    inner: TransactionOffsetsEngine<'send, 'producer>,
}

impl<'send, 'producer> SendTransactionOffsets<'send, 'producer> {
    pub(crate) const fn from_bridge(inner: TransactionOffsetsEngine<'send, 'producer>) -> Self {
        Self { inner }
    }

    /// Reports an advisory reactor-wake failure after accepted admission.
    pub const fn wake_failed(&self) -> bool {
        self.inner.wake_failed()
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for SendTransactionOffsets<'_, '_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
