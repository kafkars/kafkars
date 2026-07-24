//! Named runtime-neutral observation of one `DescribeTopics` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeTopicsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{DescribeTopicsObserverError, DescribeTopicsOutcome, outcome::translate_terminal};

/// Single observer for one accepted `DescribeTopics` batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeTopicsObserver {
    inner: CompletionObserver<DescribeTopicsTerminal>,
}

impl DescribeTopicsObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<DescribeTopicsTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeTopicsOutcome, DescribeTopicsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeTopicsObserver {
    type Output = Result<DescribeTopicsOutcome, DescribeTopicsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeTopicsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeTopicsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeTopicsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DescribeTopicsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DescribeTopicsObserverError::Stale,
    }
}
