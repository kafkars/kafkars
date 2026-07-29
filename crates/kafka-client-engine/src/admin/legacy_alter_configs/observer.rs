//! Named runtime-neutral observation of one legacy full-snapshot topic configuration terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::LegacyAlterConfigsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    LegacyAlterConfigsObserverError, LegacyAlterConfigsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted legacy full-snapshot topic configuration batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct LegacyAlterConfigsObserver {
    inner: CompletionObserver<LegacyAlterConfigsTerminal>,
}

impl LegacyAlterConfigsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<LegacyAlterConfigsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<LegacyAlterConfigsOutcome, LegacyAlterConfigsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for LegacyAlterConfigsObserver {
    type Output = Result<LegacyAlterConfigsOutcome, LegacyAlterConfigsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for LegacyAlterConfigsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyAlterConfigsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> LegacyAlterConfigsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            LegacyAlterConfigsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => LegacyAlterConfigsObserverError::Stale,
    }
}
