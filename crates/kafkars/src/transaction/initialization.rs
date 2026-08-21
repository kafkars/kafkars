//! Named single-observer transactional producer initialization.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::transaction::TransactionInitialization};

use super::TransactionalProducer;

/// Sole terminal observer for one submitted transactional-owner initialization.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted initialization"]
pub struct InitializeTransactionalProducer {
    inner: TransactionInitialization,
}

impl InitializeTransactionalProducer {
    pub(crate) const fn from_bridge(inner: TransactionInitialization) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<TransactionalProducer, KafkaError> {
        self.inner.wait().map(TransactionalProducer::from_bridge)
    }
}

impl Future for InitializeTransactionalProducer {
    type Output = Result<TransactionalProducer, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(TransactionalProducer::from_bridge))
    }
}
