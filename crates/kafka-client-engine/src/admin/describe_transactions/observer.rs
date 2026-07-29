//! Named runtime-neutral observation of one Admin `DescribeTransactions` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AdminDescribeTransactionsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AdminDescribeTransactionsObserverError, AdminDescribeTransactionsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted Admin `DescribeTransactions` query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AdminDescribeTransactionsObserver {
    inner: CompletionObserver<AdminDescribeTransactionsTerminal>,
}

impl AdminDescribeTransactionsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AdminDescribeTransactionsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<AdminDescribeTransactionsOutcome, AdminDescribeTransactionsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AdminDescribeTransactionsObserver {
    type Output = Result<AdminDescribeTransactionsOutcome, AdminDescribeTransactionsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AdminDescribeTransactionsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeTransactionsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AdminDescribeTransactionsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AdminDescribeTransactionsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AdminDescribeTransactionsObserverError::Stale,
    }
}
