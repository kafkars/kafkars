//! Named runtime-neutral observation of one incremental configuration terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::IncrementalAlterConfigsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    IncrementalAlterConfigsObserverError, IncrementalAlterConfigsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted incremental configuration batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct IncrementalAlterConfigsObserver {
    inner: CompletionObserver<IncrementalAlterConfigsTerminal>,
}

impl IncrementalAlterConfigsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<IncrementalAlterConfigsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<IncrementalAlterConfigsOutcome, IncrementalAlterConfigsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for IncrementalAlterConfigsObserver {
    type Output = Result<IncrementalAlterConfigsOutcome, IncrementalAlterConfigsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for IncrementalAlterConfigsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IncrementalAlterConfigsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> IncrementalAlterConfigsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            IncrementalAlterConfigsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => IncrementalAlterConfigsObserverError::Stale,
    }
}
