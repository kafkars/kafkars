//! Named runtime-neutral observation of one finalized-feature mutation.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::UpdateFeaturesTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{UpdateFeaturesObserverError, UpdateFeaturesOutcome, outcome::translate_terminal};

/// Single observer for one accepted finalized-feature update.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct UpdateFeaturesObserver {
    inner: CompletionObserver<UpdateFeaturesTerminal>,
}

impl UpdateFeaturesObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<UpdateFeaturesTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<UpdateFeaturesOutcome, UpdateFeaturesObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for UpdateFeaturesObserver {
    type Output = Result<UpdateFeaturesOutcome, UpdateFeaturesObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for UpdateFeaturesObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpdateFeaturesObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> UpdateFeaturesObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => UpdateFeaturesObserverError::AlreadyObserved,
        CompletionObserverError::Stale => UpdateFeaturesObserverError::Stale,
    }
}
