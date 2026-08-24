//! Runtime-neutral observation of one accepted homogeneous transactional batch.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{
    TransactionBatchSendOutcome, TransactionSendObserver, TransactionSendObserverError,
    send_batch_outcome::batch_outcome,
};

/// Sole named observer for one accepted homogeneous transactional batch.
#[must_use = "dropping abandons observation without cancelling the accepted batch"]
pub struct TransactionBatchSendObserver<'send, 'owner> {
    inner: TransactionSendObserver<'send, 'owner>,
    record_count: usize,
}

impl<'send, 'owner> TransactionBatchSendObserver<'send, 'owner> {
    pub(super) const fn new(
        inner: TransactionSendObserver<'send, 'owner>,
        record_count: usize,
    ) -> Self {
        Self {
            inner,
            record_count,
        }
    }

    /// Blocks on the same single bounded terminal cell used by [`Future::poll`].
    pub fn wait(self) -> Result<TransactionBatchSendOutcome, TransactionSendObserverError> {
        self.inner
            .wait()
            .map(|outcome| batch_outcome(outcome, self.record_count))
    }
}

impl Future for TransactionBatchSendObserver<'_, '_> {
    type Output = Result<TransactionBatchSendOutcome, TransactionSendObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(|outcome| batch_outcome(outcome, this.record_count)))
    }
}

impl fmt::Debug for TransactionBatchSendObserver<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionBatchSendObserver")
            .field("record_count", &self.record_count)
            .finish_non_exhaustive()
    }
}
