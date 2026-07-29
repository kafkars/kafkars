//! Named runtime-neutral observation of one configuration-resource listing.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::ListConfigResourcesTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    ListConfigResourcesObserverError, ListConfigResourcesOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted configuration-resource query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListConfigResourcesObserver {
    inner: CompletionObserver<ListConfigResourcesTerminal>,
}

impl ListConfigResourcesObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<ListConfigResourcesTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<ListConfigResourcesOutcome, ListConfigResourcesObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for ListConfigResourcesObserver {
    type Output = Result<ListConfigResourcesOutcome, ListConfigResourcesObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for ListConfigResourcesObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListConfigResourcesObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> ListConfigResourcesObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            ListConfigResourcesObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => ListConfigResourcesObserverError::Stale,
    }
}
