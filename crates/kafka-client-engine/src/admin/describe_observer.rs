//! Named runtime-neutral observation of one `DescribeCluster` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeClusterTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeClusterObserverError, DescribeClusterOutcome, describe_outcome::translate_terminal,
};

/// Single observer for one accepted `DescribeCluster` call.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeClusterObserver {
    inner: CompletionObserver<DescribeClusterTerminal>,
}

impl DescribeClusterObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeClusterTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeClusterOutcome, DescribeClusterObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeClusterObserver {
    type Output = Result<DescribeClusterOutcome, DescribeClusterObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeClusterObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeClusterObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeClusterObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DescribeClusterObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DescribeClusterObserverError::Stale,
    }
}
