//! Named runtime-neutral observation of one committed voter addition.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AddRaftVoterTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{AddRaftVoterObserverError, AddRaftVoterOutcome, outcome::translate_terminal};

/// Single observer for one accepted voter addition.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AddRaftVoterObserver {
    inner: CompletionObserver<AddRaftVoterTerminal>,
}

impl AddRaftVoterObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<AddRaftVoterTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<AddRaftVoterOutcome, AddRaftVoterObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AddRaftVoterObserver {
    type Output = Result<AddRaftVoterOutcome, AddRaftVoterObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AddRaftVoterObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AddRaftVoterObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AddRaftVoterObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => AddRaftVoterObserverError::AlreadyObserved,
        CompletionObserverError::Stale => AddRaftVoterObserverError::Stale,
    }
}
