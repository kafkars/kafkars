//! Named runtime-neutral observation of one partition transaction abort.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AbortPartitionTransactionTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AbortPartitionTransactionObserverError, AbortPartitionTransactionOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted partition transaction abort.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AbortPartitionTransactionObserver {
    inner: CompletionObserver<AbortPartitionTransactionTerminal>,
}

impl AbortPartitionTransactionObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AbortPartitionTransactionTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<AbortPartitionTransactionOutcome, AbortPartitionTransactionObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AbortPartitionTransactionObserver {
    type Output = Result<AbortPartitionTransactionOutcome, AbortPartitionTransactionObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AbortPartitionTransactionObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AbortPartitionTransactionObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AbortPartitionTransactionObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AbortPartitionTransactionObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AbortPartitionTransactionObserverError::Stale,
    }
}
