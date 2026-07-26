//! Named runtime-neutral observation of one offset-deletion terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DeleteConsumerGroupOffsetsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DeleteConsumerGroupOffsetsObserverError, DeleteConsumerGroupOffsetsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted consumer-group offset deletion.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteConsumerGroupOffsetsObserver {
    inner: CompletionObserver<DeleteConsumerGroupOffsetsTerminal>,
}

impl DeleteConsumerGroupOffsetsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DeleteConsumerGroupOffsetsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<DeleteConsumerGroupOffsetsOutcome, DeleteConsumerGroupOffsetsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DeleteConsumerGroupOffsetsObserver {
    type Output =
        Result<DeleteConsumerGroupOffsetsOutcome, DeleteConsumerGroupOffsetsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DeleteConsumerGroupOffsetsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteConsumerGroupOffsetsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DeleteConsumerGroupOffsetsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DeleteConsumerGroupOffsetsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DeleteConsumerGroupOffsetsObserverError::Stale,
    }
}
