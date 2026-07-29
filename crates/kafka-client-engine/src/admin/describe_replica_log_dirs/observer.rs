//! Named runtime-neutral observation of one Admin `DescribeReplicaLogDirs` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeReplicaLogDirsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeReplicaLogDirsObserverError, DescribeReplicaLogDirsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted Admin `DescribeReplicaLogDirs` query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeReplicaLogDirsObserver {
    inner: CompletionObserver<DescribeReplicaLogDirsTerminal>,
}

impl DescribeReplicaLogDirsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeReplicaLogDirsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<DescribeReplicaLogDirsOutcome, DescribeReplicaLogDirsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeReplicaLogDirsObserver {
    type Output = Result<DescribeReplicaLogDirsOutcome, DescribeReplicaLogDirsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeReplicaLogDirsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeReplicaLogDirsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeReplicaLogDirsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DescribeReplicaLogDirsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DescribeReplicaLogDirsObserverError::Stale,
    }
}
