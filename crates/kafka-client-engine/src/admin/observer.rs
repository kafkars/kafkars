//! Named runtime-neutral observation of one `CreateTopics` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::CreateTopicsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{CreateTopicsObserverError, CreateTopicsOutcome, outcome::translate_terminal};

/// Single observer for one accepted `CreateTopics` batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct CreateTopicsObserver {
    inner: CompletionObserver<CreateTopicsTerminal>,
}

impl CreateTopicsObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<CreateTopicsTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<CreateTopicsOutcome, CreateTopicsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for CreateTopicsObserver {
    type Output = Result<CreateTopicsOutcome, CreateTopicsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for CreateTopicsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateTopicsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> CreateTopicsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => CreateTopicsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => CreateTopicsObserverError::Stale,
    }
}
