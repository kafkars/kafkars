//! Named runtime-neutral observation of one Admin `DescribeProducers` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AdminDescribeProducersTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AdminDescribeProducersObserverError, AdminDescribeProducersOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted Admin `DescribeProducers` query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AdminDescribeProducersObserver {
    inner: CompletionObserver<AdminDescribeProducersTerminal>,
}

impl AdminDescribeProducersObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AdminDescribeProducersTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<AdminDescribeProducersOutcome, AdminDescribeProducersObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AdminDescribeProducersObserver {
    type Output = Result<AdminDescribeProducersOutcome, AdminDescribeProducersObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AdminDescribeProducersObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeProducersObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AdminDescribeProducersObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AdminDescribeProducersObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AdminDescribeProducersObserverError::Stale,
    }
}
