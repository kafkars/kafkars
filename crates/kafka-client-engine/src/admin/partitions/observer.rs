//! Named runtime-neutral observation of one `CreatePartitions` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::CreatePartitionsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{CreatePartitionsObserverError, CreatePartitionsOutcome, outcome::translate_terminal};

/// Single observer for one accepted `CreatePartitions` batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct CreatePartitionsObserver {
    inner: CompletionObserver<CreatePartitionsTerminal>,
}

impl CreatePartitionsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<CreatePartitionsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<CreatePartitionsOutcome, CreatePartitionsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for CreatePartitionsObserver {
    type Output = Result<CreatePartitionsOutcome, CreatePartitionsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for CreatePartitionsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreatePartitionsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> CreatePartitionsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => CreatePartitionsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => CreatePartitionsObserverError::Stale,
    }
}
