//! Runtime-neutral observation of one explicit transaction end.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use kafka_client_core::TransactionLifecycleTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

/// Exact terminal consequence of an accepted explicit transaction end.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionEndOutcome {
    /// Kafka committed the transaction.
    Committed,
    /// Kafka aborted the transaction.
    Aborted,
    /// Transaction execution became permanently unusable.
    Fatal,
}

/// Sole named observer for one accepted explicit commit or abort.
#[must_use = "dropping abandons observation without cancelling the accepted transaction end"]
pub struct TransactionEndObserver {
    inner: CompletionObserver<TransactionLifecycleTerminal>,
    _lifetime: Arc<dyn Send + Sync>,
}

impl TransactionEndObserver {
    pub(super) const fn new(
        inner: CompletionObserver<TransactionLifecycleTerminal>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            inner,
            _lifetime: lifetime,
        }
    }

    /// Blocks on the same bounded terminal cell used by [`Future::poll`].
    pub fn wait(self) -> Result<TransactionEndOutcome, TransactionEndObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for TransactionEndObserver {
    type Output = Result<TransactionEndOutcome, TransactionEndObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(terminal)) => Poll::Ready(Ok(translate_terminal(terminal))),
            Poll::Ready(Err(error)) => Poll::Ready(Err(observer_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for TransactionEndObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionEndObserver")
            .finish_non_exhaustive()
    }
}

/// Failure to observe one accepted transaction end terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionEndObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The bounded completion generation is no longer live.
    Stale,
}

impl fmt::Display for TransactionEndObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "transaction end was already observed",
            Self::Stale => "transaction end observer is stale",
        })
    }
}

impl std::error::Error for TransactionEndObserverError {}

const fn translate_terminal(terminal: TransactionLifecycleTerminal) -> TransactionEndOutcome {
    match terminal {
        TransactionLifecycleTerminal::Committed => TransactionEndOutcome::Committed,
        TransactionLifecycleTerminal::Aborted => TransactionEndOutcome::Aborted,
        TransactionLifecycleTerminal::Fatal => TransactionEndOutcome::Fatal,
    }
}

const fn observer_error(error: CompletionObserverError) -> TransactionEndObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => TransactionEndObserverError::AlreadyObserved,
        CompletionObserverError::Stale => TransactionEndObserverError::Stale,
    }
}
