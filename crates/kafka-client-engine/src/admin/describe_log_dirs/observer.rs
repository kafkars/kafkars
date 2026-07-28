//! Named runtime-neutral observation of one Admin `DescribeLogDirs` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AdminDescribeLogDirsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{DescribeLogDirsObserverError, DescribeLogDirsOutcome, outcome::translate_terminal};

/// Single observer for one accepted Admin `DescribeLogDirs` query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeLogDirsObserver {
    inner: CompletionObserver<AdminDescribeLogDirsTerminal>,
}

impl DescribeLogDirsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AdminDescribeLogDirsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeLogDirsOutcome, DescribeLogDirsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeLogDirsObserver {
    type Output = Result<DescribeLogDirsOutcome, DescribeLogDirsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeLogDirsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeLogDirsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeLogDirsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DescribeLogDirsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DescribeLogDirsObserverError::Stale,
    }
}
