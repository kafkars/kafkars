//! Named runtime-neutral observation of one metadata-quorum voter removal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::RemoveRaftVoterTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{RemoveRaftVoterObserverError, RemoveRaftVoterOutcome, outcome::translate_terminal};

/// Single observer for one accepted metadata-quorum voter removal.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct RemoveRaftVoterObserver {
    inner: CompletionObserver<RemoveRaftVoterTerminal>,
}

impl RemoveRaftVoterObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<RemoveRaftVoterTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<RemoveRaftVoterOutcome, RemoveRaftVoterObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for RemoveRaftVoterObserver {
    type Output = Result<RemoveRaftVoterOutcome, RemoveRaftVoterObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for RemoveRaftVoterObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoveRaftVoterObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> RemoveRaftVoterObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => RemoveRaftVoterObserverError::AlreadyObserved,
        CompletionObserverError::Stale => RemoveRaftVoterObserverError::Stale,
    }
}
