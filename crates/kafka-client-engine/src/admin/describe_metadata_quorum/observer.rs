//! Named runtime-neutral observation of one Admin `DescribeMetadataQuorum` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeMetadataQuorumTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeMetadataQuorumObserverError, DescribeMetadataQuorumOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted metadata-quorum query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeMetadataQuorumObserver {
    inner: CompletionObserver<DescribeMetadataQuorumTerminal>,
}

impl DescribeMetadataQuorumObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeMetadataQuorumTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<DescribeMetadataQuorumOutcome, DescribeMetadataQuorumObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeMetadataQuorumObserver {
    type Output = Result<DescribeMetadataQuorumOutcome, DescribeMetadataQuorumObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeMetadataQuorumObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeMetadataQuorumObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeMetadataQuorumObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DescribeMetadataQuorumObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DescribeMetadataQuorumObserverError::Stale,
    }
}
