//! Named runtime-neutral observation of one SCRAM credential-alteration terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AlterUserScramCredentialsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AlterUserScramCredentialsObserverError, AlterUserScramCredentialsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted SCRAM credential-alteration batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterUserScramCredentialsObserver {
    inner: CompletionObserver<AlterUserScramCredentialsTerminal>,
}

impl AlterUserScramCredentialsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AlterUserScramCredentialsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<AlterUserScramCredentialsOutcome, AlterUserScramCredentialsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AlterUserScramCredentialsObserver {
    type Output = Result<AlterUserScramCredentialsOutcome, AlterUserScramCredentialsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AlterUserScramCredentialsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterUserScramCredentialsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AlterUserScramCredentialsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AlterUserScramCredentialsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AlterUserScramCredentialsObserverError::Stale,
    }
}
