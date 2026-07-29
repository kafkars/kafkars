//! Named runtime-neutral observation of one Admin `DescribeTopicPartitions` page.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeTopicPartitionsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AdminDescribeTopicPartitionsObserverError, AdminDescribeTopicPartitionsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted explicit page request.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AdminDescribeTopicPartitionsObserver {
    inner: CompletionObserver<DescribeTopicPartitionsTerminal>,
}

impl AdminDescribeTopicPartitionsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeTopicPartitionsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<AdminDescribeTopicPartitionsOutcome, AdminDescribeTopicPartitionsObserverError>
    {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AdminDescribeTopicPartitionsObserver {
    type Output =
        Result<AdminDescribeTopicPartitionsOutcome, AdminDescribeTopicPartitionsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AdminDescribeTopicPartitionsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeTopicPartitionsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(
    error: CompletionObserverError,
) -> AdminDescribeTopicPartitionsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AdminDescribeTopicPartitionsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AdminDescribeTopicPartitionsObserverError::Stale,
    }
}
