//! Named runtime-neutral observation of one share-group offset alteration.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AlterShareGroupOffsetsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AlterShareGroupOffsetsObserverError, AlterShareGroupOffsetsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted share-group offset alteration.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterShareGroupOffsetsObserver {
    inner: CompletionObserver<AlterShareGroupOffsetsTerminal>,
}

impl AlterShareGroupOffsetsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AlterShareGroupOffsetsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<AlterShareGroupOffsetsOutcome, AlterShareGroupOffsetsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AlterShareGroupOffsetsObserver {
    type Output = Result<AlterShareGroupOffsetsOutcome, AlterShareGroupOffsetsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AlterShareGroupOffsetsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterShareGroupOffsetsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AlterShareGroupOffsetsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AlterShareGroupOffsetsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AlterShareGroupOffsetsObserverError::Stale,
    }
}
