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
pub enum AssignedConsumerCloseObserverError {
    /// The engine lost execution ownership after close was accepted.
    ExecutionUnavailable,
    /// This single observer already yielded its terminal result.
    AlreadyObserved,
    /// The completion generation no longer belongs to this observer.
    Stale,
}

/// Single non-clone observer shared by asynchronous and blocking callers.
#[must_use = "dropping abandons close observation without cancelling accepted close work"]
pub struct AssignedConsumerCloseObserver {
    inner: CompletionObserver<AssignedConsumerCloseTerminal>,
}

impl AssignedConsumerCloseObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AssignedConsumerCloseTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<(), AssignedConsumerCloseObserverError> {
        translate_terminal(self.inner.wait().map_err(observer_error)?)
    }
}

impl Future for AssignedConsumerCloseObserver {
    type Output = Result<(), AssignedConsumerCloseObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map_err(observer_error).and_then(translate_terminal))
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

const fn translate_terminal(
    terminal: AssignedConsumerCloseTerminal,
) -> Result<(), AssignedConsumerCloseObserverError> {
    match terminal {
        AssignedConsumerCloseTerminal::Closed(_close_id) => Ok(()),
        AssignedConsumerCloseTerminal::ExecutionUnavailable => {
            Err(AssignedConsumerCloseObserverError::ExecutionUnavailable)
        }
    }
}

impl fmt::Display for AssignedConsumerCloseObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionUnavailable => {
                formatter.write_str("assigned-consumer close execution became unavailable")
            }
            Self::AlreadyObserved => formatter.write_str("close completion was already observed"),
            Self::Stale => formatter.write_str("close observer is stale"),
        }
    }
}

impl std::error::Error for AssignedConsumerCloseObserverError {}
