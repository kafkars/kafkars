//! Named runtime-neutral observation of one broker unregistration.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::UnregisterBrokerTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{UnregisterBrokerObserverError, UnregisterBrokerOutcome, outcome::translate_terminal};

/// Single observer for one accepted broker unregistration.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct UnregisterBrokerObserver {
    inner: CompletionObserver<UnregisterBrokerTerminal>,
}

impl UnregisterBrokerObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<UnregisterBrokerTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<UnregisterBrokerOutcome, UnregisterBrokerObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for UnregisterBrokerObserver {
    type Output = Result<UnregisterBrokerOutcome, UnregisterBrokerObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for UnregisterBrokerObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnregisterBrokerObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> UnregisterBrokerObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => UnregisterBrokerObserverError::AlreadyObserved,
        CompletionObserverError::Stale => UnregisterBrokerObserverError::Stale,
    }
}
