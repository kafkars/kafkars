//! Named runtime-neutral observation of one `DeleteTopics` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DeleteTopicsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{DeleteTopicsObserverError, DeleteTopicsOutcome, delete_outcome::translate_terminal};

/// Single observer for one accepted `DeleteTopics` batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteTopicsObserver {
    inner: CompletionObserver<DeleteTopicsTerminal>,
}

impl DeleteTopicsObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<DeleteTopicsTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DeleteTopicsOutcome, DeleteTopicsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DeleteTopicsObserver {
    type Output = Result<DeleteTopicsOutcome, DeleteTopicsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DeleteTopicsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteTopicsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DeleteTopicsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DeleteTopicsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DeleteTopicsObserverError::Stale,
    }
}
