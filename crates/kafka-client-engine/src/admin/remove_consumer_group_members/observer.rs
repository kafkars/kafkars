//! Runtime-neutral observation of one static-member removal terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::RemoveConsumerGroupMembersTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    RemoveConsumerGroupMembersObserverError, RemoveConsumerGroupMembersOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted static-member removal.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct RemoveConsumerGroupMembersObserver {
    inner: CompletionObserver<RemoveConsumerGroupMembersTerminal>,
}

impl RemoveConsumerGroupMembersObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<RemoveConsumerGroupMembersTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks the calling thread until the retained terminal is available.
    pub fn wait(
        self,
    ) -> Result<RemoveConsumerGroupMembersOutcome, RemoveConsumerGroupMembersObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for RemoveConsumerGroupMembersObserver {
    type Output =
        Result<RemoveConsumerGroupMembersOutcome, RemoveConsumerGroupMembersObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for RemoveConsumerGroupMembersObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoveConsumerGroupMembersObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> RemoveConsumerGroupMembersObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            RemoveConsumerGroupMembersObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => RemoveConsumerGroupMembersObserverError::Stale,
    }
}
