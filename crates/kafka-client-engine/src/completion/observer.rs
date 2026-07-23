//! Single-owner observation over one shared completion cell.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::{CompletionId, CompletionObserverError, cell::CompletionCell};

/// Non-cloneable observer supporting both `Future` and blocking wait.
pub(crate) struct CompletionObserver<T> {
    id: CompletionId,
    cell: Arc<CompletionCell<T>>,
    observed: bool,
}

impl<T> CompletionObserver<T> {
    pub(super) const fn new(id: CompletionId, cell: Arc<CompletionCell<T>>) -> Self {
        Self {
            id,
            cell,
            observed: false,
        }
    }

    /// Blocks on the same terminal state used by `Future::poll`.
    pub(crate) fn wait(mut self) -> Result<T, CompletionObserverError> {
        if self.observed {
            return Err(CompletionObserverError::AlreadyObserved);
        }
        let result = self.cell.wait(self.id);
        if result.is_ok() {
            self.observed = true;
        }
        result
    }
}

impl<T> Future for CompletionObserver<T> {
    type Output = Result<T, CompletionObserverError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.observed {
            return Poll::Ready(Err(CompletionObserverError::AlreadyObserved));
        }
        match self.cell.poll(self.id, context) {
            Ok(Poll::Ready(value)) => {
                self.observed = true;
                Poll::Ready(Ok(value))
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl<T> Drop for CompletionObserver<T> {
    fn drop(&mut self) {
        if !self.observed {
            self.cell.abandon(self.id);
        }
    }
}

impl<T> fmt::Debug for CompletionObserver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletionObserver")
            .field("id", &self.id)
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}
