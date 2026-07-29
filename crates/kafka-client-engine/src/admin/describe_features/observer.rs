//! Named runtime-neutral observation of one feature description.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeFeaturesTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{DescribeFeaturesObserverError, DescribeFeaturesOutcome, outcome::translate_terminal};

/// Single observer for one accepted feature description query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeFeaturesObserver {
    inner: CompletionObserver<DescribeFeaturesTerminal>,
}

impl DescribeFeaturesObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeFeaturesTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeFeaturesOutcome, DescribeFeaturesObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeFeaturesObserver {
    type Output = Result<DescribeFeaturesOutcome, DescribeFeaturesObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeFeaturesObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeFeaturesObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeFeaturesObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DescribeFeaturesObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DescribeFeaturesObserverError::Stale,
    }
}
