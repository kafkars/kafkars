//! Named runtime-neutral observers for accepted commit and abort operations.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::transaction::TransactionEndEngine};

/// Sole terminal observer for one accepted transaction commit.
///
/// The mutable producer borrow remains held until this observer is consumed or
/// dropped. Dropping abandons observation without cancelling the accepted end.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling the accepted commit"]
pub struct CommitTransaction<'producer> {
    inner: TransactionEndEngine<'producer>,
}

impl<'producer> CommitTransaction<'producer> {
    pub(crate) const fn from_bridge(inner: TransactionEndEngine<'producer>) -> Self {
        Self { inner }
    }

    /// Reports the advisory wake failure retained from accepted begin.
    pub const fn begin_wake_failed(&self) -> bool {
        self.inner.begin_wake_failed()
    }

    /// Reports an advisory reactor-wake failure after accepted commit.
    pub const fn end_wake_failed(&self) -> bool {
        self.inner.end_wake_failed()
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for CommitTransaction<'_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}

/// Sole terminal observer for one accepted transaction abort.
///
/// The mutable producer borrow remains held until this observer is consumed or
/// dropped. Dropping abandons observation without cancelling the accepted end.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling the accepted abort"]
pub struct AbortTransaction<'producer> {
    inner: TransactionEndEngine<'producer>,
}

impl<'producer> AbortTransaction<'producer> {
    pub(crate) const fn from_bridge(inner: TransactionEndEngine<'producer>) -> Self {
        Self { inner }
    }

    /// Reports the advisory wake failure retained from accepted begin.
    pub const fn begin_wake_failed(&self) -> bool {
        self.inner.begin_wake_failed()
    }

    /// Reports an advisory reactor-wake failure after accepted abort.
    pub const fn end_wake_failed(&self) -> bool {
        self.inner.end_wake_failed()
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for AbortTransaction<'_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
