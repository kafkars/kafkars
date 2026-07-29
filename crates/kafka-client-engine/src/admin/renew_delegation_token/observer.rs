//! Named runtime-neutral observation of one delegation-token renewal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::RenewDelegationTokenTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    RenewDelegationTokenObserverError, RenewDelegationTokenOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted token renewal.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct RenewDelegationTokenObserver {
    inner: CompletionObserver<RenewDelegationTokenTerminal>,
}

impl RenewDelegationTokenObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<RenewDelegationTokenTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<RenewDelegationTokenOutcome, RenewDelegationTokenObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for RenewDelegationTokenObserver {
    type Output = Result<RenewDelegationTokenOutcome, RenewDelegationTokenObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for RenewDelegationTokenObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RenewDelegationTokenObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> RenewDelegationTokenObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            RenewDelegationTokenObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => RenewDelegationTokenObserverError::Stale,
    }
}
