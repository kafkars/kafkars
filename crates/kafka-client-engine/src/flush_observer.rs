//! Named runtime-neutral observation of one accepted producer flush.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    ProducerFlushError, ProducerObserverError, completion::CompletionObserver,
    producer::ProducerTerminal,
};

/// Terminal result returned by asynchronous and blocking flush observation.
pub type ProducerFlushResult = Result<(), ProducerFlushError>;

/// Single non-cloneable observer for one accepted producer flush.
#[must_use = "dropping abandons flush observation without cancelling the flush"]
pub struct ProducerFlushObserver {
    inner: CompletionObserver<ProducerTerminal>,
}

impl ProducerFlushObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<ProducerTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> ProducerFlushResult {
        match self.inner.wait() {
            Ok(terminal) => translate(terminal),
            Err(error) => Err(observer_error(error)),
        }
    }
}

impl Future for ProducerFlushObserver {
    type Output = ProducerFlushResult;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(terminal)) => Poll::Ready(translate(terminal)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(observer_error(error))),
        }
    }
}

impl fmt::Debug for ProducerFlushObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProducerFlushObserver")
            .finish_non_exhaustive()
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "observation consumes the linear terminal envelope"
)]
const fn translate(terminal: ProducerTerminal) -> ProducerFlushResult {
    match terminal {
        ProducerTerminal::FlushCompleted => Ok(()),
        ProducerTerminal::ExecutionUnavailable => Err(ProducerFlushError::ExecutionUnavailable),
        ProducerTerminal::Record(_) => Err(ProducerFlushError::Observer(
            ProducerObserverError::TerminalTypeMismatch,
        )),
    }
}

const fn observer_error(error: crate::completion::CompletionObserverError) -> ProducerFlushError {
    ProducerFlushError::Observer(ProducerObserverError::from_completion(error))
}
