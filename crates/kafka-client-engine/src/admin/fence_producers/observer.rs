//! Named runtime-neutral observation of one Admin `FenceProducers` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AdminFenceProducersTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AdminFenceProducersObserverError, AdminFenceProducersOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted Admin `FenceProducers` query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AdminFenceProducersObserver {
    inner: CompletionObserver<AdminFenceProducersTerminal>,
}

impl AdminFenceProducersObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AdminFenceProducersTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<AdminFenceProducersOutcome, AdminFenceProducersObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AdminFenceProducersObserver {
    type Output = Result<AdminFenceProducersOutcome, AdminFenceProducersObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AdminFenceProducersObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminFenceProducersObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AdminFenceProducersObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AdminFenceProducersObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AdminFenceProducersObserverError::Stale,
    }
}
