//! Runtime-neutral observation of one reassignment terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AlterPartitionReassignmentsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AlterPartitionReassignmentsObserverError, AlterPartitionReassignmentsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted reassignment alteration.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterPartitionReassignmentsObserver {
    inner: CompletionObserver<AlterPartitionReassignmentsTerminal>,
}

impl AlterPartitionReassignmentsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AlterPartitionReassignmentsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks the calling thread until the retained terminal is available.
    pub fn wait(
        self,
    ) -> Result<AlterPartitionReassignmentsOutcome, AlterPartitionReassignmentsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AlterPartitionReassignmentsObserver {
    type Output =
        Result<AlterPartitionReassignmentsOutcome, AlterPartitionReassignmentsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AlterPartitionReassignmentsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterPartitionReassignmentsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(
    error: CompletionObserverError,
) -> AlterPartitionReassignmentsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AlterPartitionReassignmentsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AlterPartitionReassignmentsObserverError::Stale,
    }
}
