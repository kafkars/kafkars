//! Named runtime-neutral observation of one Admin `ListTransactions` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AdminListTransactionsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AdminListTransactionsObserverError, AdminListTransactionsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted cluster-wide transaction listing.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AdminListTransactionsObserver {
    inner: CompletionObserver<AdminListTransactionsTerminal>,
}

impl AdminListTransactionsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AdminListTransactionsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<AdminListTransactionsOutcome, AdminListTransactionsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AdminListTransactionsObserver {
    type Output = Result<AdminListTransactionsOutcome, AdminListTransactionsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AdminListTransactionsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListTransactionsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AdminListTransactionsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AdminListTransactionsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AdminListTransactionsObserverError::Stale,
    }
}
