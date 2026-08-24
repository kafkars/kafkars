//! Named runtime-neutral observation of one accepted homogeneous transactional batch.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::transaction::TransactionBatchSendEngine};

use super::TransactionBatchMetadata;

/// Sole terminal observer for one accepted homogeneous transactional batch.
///
/// `'send` retains the mutable transaction reborrow until observation or drop;
/// `'producer` is the active transaction's producer-owner borrow.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling the accepted batch"]
pub struct SendTransactionBatch<'send, 'producer> {
    inner: TransactionBatchSendEngine<'send, 'producer>,
}

impl<'send, 'producer> SendTransactionBatch<'send, 'producer> {
    pub(crate) const fn from_bridge(inner: TransactionBatchSendEngine<'send, 'producer>) -> Self {
        Self { inner }
    }

    /// Reports an advisory reactor-wake failure after accepted batch admission.
    pub const fn wake_failed(&self) -> bool {
        self.inner.wake_failed()
    }

    /// Blocks on the same single terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<TransactionBatchMetadata, KafkaError> {
        self.inner.wait()
    }
}

impl Future for SendTransactionBatch<'_, '_> {
    type Output = Result<TransactionBatchMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
