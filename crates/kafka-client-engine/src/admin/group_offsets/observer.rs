//! Named runtime-neutral observation of one group-offset terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::ListConsumerGroupOffsetsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    ListConsumerGroupOffsetsObserverError, ListConsumerGroupOffsetsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted consumer-group offset query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListConsumerGroupOffsetsObserver {
    inner: CompletionObserver<ListConsumerGroupOffsetsTerminal>,
}

impl ListConsumerGroupOffsetsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<ListConsumerGroupOffsetsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<ListConsumerGroupOffsetsOutcome, ListConsumerGroupOffsetsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for ListConsumerGroupOffsetsObserver {
    type Output = Result<ListConsumerGroupOffsetsOutcome, ListConsumerGroupOffsetsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for ListConsumerGroupOffsetsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupOffsetsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> ListConsumerGroupOffsetsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            ListConsumerGroupOffsetsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => ListConsumerGroupOffsetsObserverError::Stale,
    }
}
