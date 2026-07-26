//! Runtime-neutral observation of one transaction-initialization terminal.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    RetainedTransactionInitializationOutcome, TransactionInitializationObserverError,
    TransactionInitializationOutcome,
};

/// Single runtime-neutral observer for one accepted initialization.
#[must_use = "dropping abandons observation without cancelling accepted initialization"]
pub struct TransactionInitializationObserver {
    inner: CompletionObserver<RetainedTransactionInitializationOutcome>,
    lifetime: Option<Arc<dyn Send + Sync>>,
}

impl TransactionInitializationObserver {
    pub(super) const fn new(
        inner: CompletionObserver<RetainedTransactionInitializationOutcome>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            inner,
            lifetime: Some(lifetime),
        }
    }

    /// Blocks on the same terminal cell used by [`Future::poll`].
    pub fn wait(
        mut self,
    ) -> Result<TransactionInitializationOutcome, TransactionInitializationObserverError> {
        let terminal = self.inner.wait().map_err(observer_error)?;
        let lifetime = self
            .lifetime
            .take()
            .ok_or(TransactionInitializationObserverError::AlreadyObserved)?;
        Ok(terminal.into_observed(lifetime))
    }
}

impl Future for TransactionInitializationObserver {
    type Output = Result<TransactionInitializationOutcome, TransactionInitializationObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(terminal)) => {
                let Some(lifetime) = this.lifetime.take() else {
                    return Poll::Ready(Err(
                        TransactionInitializationObserverError::AlreadyObserved,
                    ));
                };
                Poll::Ready(Ok(terminal.into_observed(lifetime)))
            }
            Poll::Ready(Err(error)) => {
                drop(this.lifetime.take());
                Poll::Ready(Err(observer_error(error)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl std::fmt::Debug for TransactionInitializationObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionInitializationObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> TransactionInitializationObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            TransactionInitializationObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => TransactionInitializationObserverError::Stale,
    }
}
