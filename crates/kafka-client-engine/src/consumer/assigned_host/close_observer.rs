//! Named runtime-neutral observation of one assigned-consumer close terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AssignedConsumerCloseId;

use crate::completion::{CompletionObserver, CompletionObserverError};

/// Terminal authority for one accepted assigned-consumer close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerCloseTerminal {
    Closed(AssignedConsumerCloseId),
    ExecutionUnavailable,
}

/// Observation failure without changing close or delivery semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerCloseObserverError {
    AlreadyObserved,
    Stale,
}

/// Single non-clone observer shared by asynchronous and blocking callers.
#[must_use = "dropping abandons close observation without cancelling accepted close work"]
pub(crate) struct AssignedConsumerCloseObserver {
    inner: CompletionObserver<AssignedConsumerCloseTerminal>,
}

impl AssignedConsumerCloseObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AssignedConsumerCloseTerminal>,
    ) -> Self {
        Self { inner }
    }

    pub(crate) fn wait(
        self,
    ) -> Result<AssignedConsumerCloseTerminal, AssignedConsumerCloseObserverError> {
        self.inner.wait().map_err(observer_error)
    }
}

impl Future for AssignedConsumerCloseObserver {
    type Output = Result<AssignedConsumerCloseTerminal, AssignedConsumerCloseObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map_err(observer_error))
    }
}

impl fmt::Debug for AssignedConsumerCloseObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssignedConsumerCloseObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AssignedConsumerCloseObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AssignedConsumerCloseObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AssignedConsumerCloseObserverError::Stale,
    }
}
