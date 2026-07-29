//! Named runtime-neutral observation of one share-group description.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeShareGroupTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeShareGroupObserverError, DescribeShareGroupOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted share-group description.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeShareGroupObserver {
    inner: CompletionObserver<DescribeShareGroupTerminal>,
}

impl DescribeShareGroupObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeShareGroupTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeShareGroupOutcome, DescribeShareGroupObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeShareGroupObserver {
    type Output = Result<DescribeShareGroupOutcome, DescribeShareGroupObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeShareGroupObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeShareGroupObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeShareGroupObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DescribeShareGroupObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DescribeShareGroupObserverError::Stale,
    }
}
