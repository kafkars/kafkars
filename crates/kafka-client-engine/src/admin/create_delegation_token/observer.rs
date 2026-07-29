//! Named runtime-neutral observation of one delegation-token creation.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::CreateDelegationTokenTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    CreateDelegationTokenObserverError, CreateDelegationTokenOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted token creation.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct CreateDelegationTokenObserver {
    inner: CompletionObserver<CreateDelegationTokenTerminal>,
}

impl CreateDelegationTokenObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<CreateDelegationTokenTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<CreateDelegationTokenOutcome, CreateDelegationTokenObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for CreateDelegationTokenObserver {
    type Output = Result<CreateDelegationTokenOutcome, CreateDelegationTokenObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for CreateDelegationTokenObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateDelegationTokenObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> CreateDelegationTokenObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            CreateDelegationTokenObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => CreateDelegationTokenObserverError::Stale,
    }
}
