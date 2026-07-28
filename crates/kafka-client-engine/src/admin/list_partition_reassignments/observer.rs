//! Named runtime-neutral observation of one reassignment-listing terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::ListPartitionReassignmentsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    ListPartitionReassignmentsObserverError, ListPartitionReassignmentsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted reassignment query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListPartitionReassignmentsObserver {
    inner: CompletionObserver<ListPartitionReassignmentsTerminal>,
}

impl ListPartitionReassignmentsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<ListPartitionReassignmentsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<ListPartitionReassignmentsOutcome, ListPartitionReassignmentsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for ListPartitionReassignmentsObserver {
    type Output =
        Result<ListPartitionReassignmentsOutcome, ListPartitionReassignmentsObserverError>;

    fn poll(self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for ListPartitionReassignmentsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListPartitionReassignmentsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> ListPartitionReassignmentsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            ListPartitionReassignmentsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => ListPartitionReassignmentsObserverError::Stale,
    }
}
