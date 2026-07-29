//! Named runtime-neutral observation of one share-group offset deletion.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DeleteShareGroupOffsetsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DeleteShareGroupOffsetsObserverError, DeleteShareGroupOffsetsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted share-group offset deletion.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteShareGroupOffsetsObserver {
    inner: CompletionObserver<DeleteShareGroupOffsetsTerminal>,
}

impl DeleteShareGroupOffsetsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DeleteShareGroupOffsetsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<DeleteShareGroupOffsetsOutcome, DeleteShareGroupOffsetsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DeleteShareGroupOffsetsObserver {
    type Output = Result<DeleteShareGroupOffsetsOutcome, DeleteShareGroupOffsetsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DeleteShareGroupOffsetsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteShareGroupOffsetsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DeleteShareGroupOffsetsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DeleteShareGroupOffsetsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DeleteShareGroupOffsetsObserverError::Stale,
    }
}
