//! Named runtime-neutral observation of one Admin `AlterClientQuotas` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AlterClientQuotasTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AlterClientQuotasObserverError, AlterClientQuotasOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted client-quota alteration batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterClientQuotasObserver {
    inner: CompletionObserver<AlterClientQuotasTerminal>,
}

impl AlterClientQuotasObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AlterClientQuotasTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<AlterClientQuotasOutcome, AlterClientQuotasObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AlterClientQuotasObserver {
    type Output = Result<AlterClientQuotasOutcome, AlterClientQuotasObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AlterClientQuotasObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterClientQuotasObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AlterClientQuotasObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => AlterClientQuotasObserverError::AlreadyObserved,
        CompletionObserverError::Stale => AlterClientQuotasObserverError::Stale,
    }
}
