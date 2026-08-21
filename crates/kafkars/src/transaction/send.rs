//! Named runtime-neutral observation of one accepted transactional record send.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, RecordMetadata, bridge::transaction::TransactionSendEngine};

/// Sole terminal observer for one accepted transactional record send.
///
/// `'send` retains the mutable transaction reborrow until observation or drop;
/// `'producer` is the active transaction's producer-owner borrow.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling the accepted send"]
pub struct SendTransactionRecord<'send, 'producer> {
    inner: TransactionSendEngine<'send, 'producer>,
}

impl<'send, 'producer> SendTransactionRecord<'send, 'producer> {
    pub(crate) const fn from_bridge(inner: TransactionSendEngine<'send, 'producer>) -> Self {
        Self { inner }
    }

    /// Reports an advisory reactor-wake failure after accepted send admission.
    pub const fn wake_failed(&self) -> bool {
        self.inner.wake_failed()
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<RecordMetadata, KafkaError> {
        self.inner.wait()
    }
}

impl Future for SendTransactionRecord<'_, '_> {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
