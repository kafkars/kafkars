//! Named runtime-neutral observation of one group-description terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AdminDescribeConsumerGroupsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeConsumerGroupsObserverError, DescribeConsumerGroupsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted description operation.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeConsumerGroupsObserver {
    inner: CompletionObserver<AdminDescribeConsumerGroupsTerminal>,
}

impl DescribeConsumerGroupsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AdminDescribeConsumerGroupsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<DescribeConsumerGroupsOutcome, DescribeConsumerGroupsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeConsumerGroupsObserver {
    type Output = Result<DescribeConsumerGroupsOutcome, DescribeConsumerGroupsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeConsumerGroupsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeConsumerGroupsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeConsumerGroupsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DescribeConsumerGroupsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DescribeConsumerGroupsObserverError::Stale,
    }
}
