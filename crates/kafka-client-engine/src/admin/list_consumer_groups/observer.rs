//! Named runtime-neutral observation of one cluster group-listing terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AdminListConsumerGroupsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    ListConsumerGroupsObserverError, ListConsumerGroupsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted cluster-wide listing.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListConsumerGroupsObserver {
    inner: CompletionObserver<AdminListConsumerGroupsTerminal>,
}

impl ListConsumerGroupsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AdminListConsumerGroupsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<ListConsumerGroupsOutcome, ListConsumerGroupsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for ListConsumerGroupsObserver {
    type Output = Result<ListConsumerGroupsOutcome, ListConsumerGroupsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for ListConsumerGroupsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> ListConsumerGroupsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            ListConsumerGroupsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => ListConsumerGroupsObserverError::Stale,
    }
}
