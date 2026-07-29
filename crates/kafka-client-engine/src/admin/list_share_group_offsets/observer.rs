//! Named runtime-neutral observation of one share-group offset listing.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::ListShareGroupOffsetsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    ListShareGroupOffsetsObserverError, ListShareGroupOffsetsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted share-group offset listing.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListShareGroupOffsetsObserver {
    inner: CompletionObserver<ListShareGroupOffsetsTerminal>,
}

impl ListShareGroupOffsetsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<ListShareGroupOffsetsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<ListShareGroupOffsetsOutcome, ListShareGroupOffsetsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for ListShareGroupOffsetsObserver {
    type Output = Result<ListShareGroupOffsetsOutcome, ListShareGroupOffsetsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for ListShareGroupOffsetsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListShareGroupOffsetsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> ListShareGroupOffsetsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            ListShareGroupOffsetsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => ListShareGroupOffsetsObserverError::Stale,
    }
}
