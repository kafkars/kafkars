//! Named runtime-neutral observation of one Admin `ListOffsets` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AdminListOffsetsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{AdminListOffsetsObserverError, AdminListOffsetsOutcome, outcome::translate_terminal};

/// Single observer for one accepted Admin `ListOffsets` query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AdminListOffsetsObserver {
    inner: CompletionObserver<AdminListOffsetsTerminal>,
}

impl AdminListOffsetsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AdminListOffsetsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<AdminListOffsetsOutcome, AdminListOffsetsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AdminListOffsetsObserver {
    type Output = Result<AdminListOffsetsOutcome, AdminListOffsetsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AdminListOffsetsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListOffsetsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AdminListOffsetsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => AdminListOffsetsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => AdminListOffsetsObserverError::Stale,
    }
}
