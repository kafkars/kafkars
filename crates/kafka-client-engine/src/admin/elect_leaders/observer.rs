//! Runtime-neutral observation of one election terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::ElectLeadersTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{ElectLeadersObserverError, ElectLeadersOutcome, outcome::translate_terminal};

/// Single observer for one accepted leader election.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ElectLeadersObserver {
    inner: CompletionObserver<ElectLeadersTerminal>,
}

impl ElectLeadersObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<ElectLeadersTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks the calling thread until the retained terminal is available.
    pub fn wait(self) -> Result<ElectLeadersOutcome, ElectLeadersObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for ElectLeadersObserver {
    type Output = Result<ElectLeadersOutcome, ElectLeadersObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for ElectLeadersObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ElectLeadersObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> ElectLeadersObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => ElectLeadersObserverError::AlreadyObserved,
        CompletionObserverError::Stale => ElectLeadersObserverError::Stale,
    }
}
