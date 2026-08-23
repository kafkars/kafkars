//! Runtime-neutral single observation of one accepted share acknowledgement.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::ShareAcknowledgeOutcome;

/// Sole named observer for one accepted `ShareAcknowledge` operation.
#[must_use = "dropping observation does not cancel an accepted acknowledgement"]
pub struct ShareAcknowledgementObserver {
    inner: CompletionObserver<ShareAcknowledgeOutcome>,
    _lifetime: Arc<dyn Send + Sync>,
}

impl ShareAcknowledgementObserver {
    pub(in crate::consumer) fn new(
        inner: CompletionObserver<ShareAcknowledgeOutcome>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            inner,
            _lifetime: lifetime,
        }
    }

    /// Blocks on the same bounded terminal cell used by [`Future::poll`].
    pub fn wait(self) -> Result<ShareAcknowledgeOutcome, ShareAcknowledgementObserverError> {
        self.inner.wait().map_err(observer_error)
    }
}

impl Future for ShareAcknowledgementObserver {
    type Output = Result<ShareAcknowledgeOutcome, ShareAcknowledgementObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(outcome)) => Poll::Ready(Ok(outcome)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(observer_error(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for ShareAcknowledgementObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementObserver")
            .finish_non_exhaustive()
    }
}

/// Failure in the single-observer completion lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcknowledgementObserverError {
    /// This observer already consumed its terminal value.
    AlreadyObserved,
    /// The bounded completion generation no longer belongs to this observer.
    Stale,
}

impl fmt::Display for ShareAcknowledgementObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "share acknowledgement was already observed",
            Self::Stale => "share acknowledgement observer is stale",
        })
    }
}

impl std::error::Error for ShareAcknowledgementObserverError {}

const fn observer_error(error: CompletionObserverError) -> ShareAcknowledgementObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            ShareAcknowledgementObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => ShareAcknowledgementObserverError::Stale,
    }
}
