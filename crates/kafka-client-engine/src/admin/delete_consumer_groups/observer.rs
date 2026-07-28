//! Named runtime-neutral observation of one Admin `DeleteConsumerGroups` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DeleteConsumerGroupsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DeleteConsumerGroupsObserverError, DeleteConsumerGroupsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted Admin `DeleteConsumerGroups` query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteConsumerGroupsObserver {
    inner: CompletionObserver<DeleteConsumerGroupsTerminal>,
}

impl DeleteConsumerGroupsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DeleteConsumerGroupsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DeleteConsumerGroupsOutcome, DeleteConsumerGroupsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DeleteConsumerGroupsObserver {
    type Output = Result<DeleteConsumerGroupsOutcome, DeleteConsumerGroupsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DeleteConsumerGroupsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteConsumerGroupsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DeleteConsumerGroupsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DeleteConsumerGroupsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DeleteConsumerGroupsObserverError::Stale,
    }
}
