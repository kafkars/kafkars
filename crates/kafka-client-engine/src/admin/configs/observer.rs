//! Named runtime-neutral observation for one `DescribeConfigs` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeConfigsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{DescribeConfigsObserverError, DescribeConfigsOutcome, translate::translate_terminal};

/// Single observer for one accepted `DescribeConfigs` batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeConfigsObserver {
    inner: CompletionObserver<DescribeConfigsTerminal>,
}

impl DescribeConfigsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeConfigsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeConfigsOutcome, DescribeConfigsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeConfigsObserver {
    type Output = Result<DescribeConfigsOutcome, DescribeConfigsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeConfigsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeConfigsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeConfigsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DescribeConfigsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DescribeConfigsObserverError::Stale,
    }
}
