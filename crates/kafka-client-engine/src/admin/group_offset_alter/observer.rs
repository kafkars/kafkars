//! Named runtime-neutral observation of one offset-alteration terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AlterConsumerGroupOffsetsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AlterConsumerGroupOffsetsObserverError, AlterConsumerGroupOffsetsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted consumer-group offset alteration.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterConsumerGroupOffsetsObserver {
    inner: CompletionObserver<AlterConsumerGroupOffsetsTerminal>,
}

impl AlterConsumerGroupOffsetsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AlterConsumerGroupOffsetsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<AlterConsumerGroupOffsetsOutcome, AlterConsumerGroupOffsetsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AlterConsumerGroupOffsetsObserver {
    type Output = Result<AlterConsumerGroupOffsetsOutcome, AlterConsumerGroupOffsetsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AlterConsumerGroupOffsetsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterConsumerGroupOffsetsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AlterConsumerGroupOffsetsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AlterConsumerGroupOffsetsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AlterConsumerGroupOffsetsObserverError::Stale,
    }
}
