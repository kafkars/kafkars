//! Named runtime-neutral observation of one client-metrics resource listing.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::ListClientMetricsResourcesTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    ListClientMetricsResourcesObserverError, ListClientMetricsResourcesOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted client-metrics resource query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListClientMetricsResourcesObserver {
    inner: CompletionObserver<ListClientMetricsResourcesTerminal>,
}

impl ListClientMetricsResourcesObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<ListClientMetricsResourcesTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<ListClientMetricsResourcesOutcome, ListClientMetricsResourcesObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for ListClientMetricsResourcesObserver {
    type Output =
        Result<ListClientMetricsResourcesOutcome, ListClientMetricsResourcesObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for ListClientMetricsResourcesObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListClientMetricsResourcesObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> ListClientMetricsResourcesObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            ListClientMetricsResourcesObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => ListClientMetricsResourcesObserverError::Stale,
    }
}
