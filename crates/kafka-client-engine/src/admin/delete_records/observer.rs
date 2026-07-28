//! Named runtime-neutral observation of one Admin `DeleteRecords` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DeleteRecordsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{DeleteRecordsObserverError, DeleteRecordsOutcome, outcome::translate_terminal};

/// Single observer for one accepted Admin `DeleteRecords` query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteRecordsObserver {
    inner: CompletionObserver<DeleteRecordsTerminal>,
}

impl DeleteRecordsObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<DeleteRecordsTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DeleteRecordsOutcome, DeleteRecordsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DeleteRecordsObserver {
    type Output = Result<DeleteRecordsOutcome, DeleteRecordsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DeleteRecordsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteRecordsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DeleteRecordsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DeleteRecordsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DeleteRecordsObserverError::Stale,
    }
}
