//! Named runtime-neutral observation of one streams-group description.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeStreamsGroupTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeStreamsGroupObserverError, DescribeStreamsGroupOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted streams-group description.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeStreamsGroupObserver {
    inner: CompletionObserver<DescribeStreamsGroupTerminal>,
}

impl DescribeStreamsGroupObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeStreamsGroupTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeStreamsGroupOutcome, DescribeStreamsGroupObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeStreamsGroupObserver {
    type Output = Result<DescribeStreamsGroupOutcome, DescribeStreamsGroupObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeStreamsGroupObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeStreamsGroupObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeStreamsGroupObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DescribeStreamsGroupObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DescribeStreamsGroupObserverError::Stale,
    }
}
