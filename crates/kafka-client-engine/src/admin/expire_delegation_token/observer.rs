//! Named runtime-neutral observation of one delegation-token expiration.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::ExpireDelegationTokenTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    ExpireDelegationTokenObserverError, ExpireDelegationTokenOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted token expiration.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ExpireDelegationTokenObserver {
    inner: CompletionObserver<ExpireDelegationTokenTerminal>,
}

impl ExpireDelegationTokenObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<ExpireDelegationTokenTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<ExpireDelegationTokenOutcome, ExpireDelegationTokenObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for ExpireDelegationTokenObserver {
    type Output = Result<ExpireDelegationTokenOutcome, ExpireDelegationTokenObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for ExpireDelegationTokenObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExpireDelegationTokenObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> ExpireDelegationTokenObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            ExpireDelegationTokenObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => ExpireDelegationTokenObserverError::Stale,
    }
}
