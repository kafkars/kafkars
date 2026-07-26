//! Runtime-neutral observation of one transaction-initialization terminal.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{RetainedTransactionInitializationOutcome, TransactionInitializationOutcome};

pub(crate) struct TransactionInitializationObserver {
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

    pub(crate) fn wait(
        mut self,
    ) -> Result<TransactionInitializationOutcome, CompletionObserverError> {
        let terminal = self.inner.wait()?;
        let lifetime = self
            .lifetime
            .take()
            .ok_or(CompletionObserverError::AlreadyObserved)?;
        Ok(terminal.into_observed(lifetime))
    }
}

impl Future for TransactionInitializationObserver {
    type Output = Result<TransactionInitializationOutcome, CompletionObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(terminal)) => {
                let Some(lifetime) = this.lifetime.take() else {
                    return Poll::Ready(Err(CompletionObserverError::AlreadyObserved));
                };
                Poll::Ready(Ok(terminal.into_observed(lifetime)))
            }
            Poll::Ready(Err(error)) => {
                drop(this.lifetime.take());
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
