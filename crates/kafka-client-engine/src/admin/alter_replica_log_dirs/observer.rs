//! Named runtime-neutral observation of one Admin `AlterReplicaLogDirs` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::AlterReplicaLogDirsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    AlterReplicaLogDirsObserverError, AlterReplicaLogDirsOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted Admin `AlterReplicaLogDirs` mutation.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterReplicaLogDirsObserver {
    inner: CompletionObserver<AlterReplicaLogDirsTerminal>,
}

impl AlterReplicaLogDirsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<AlterReplicaLogDirsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<AlterReplicaLogDirsOutcome, AlterReplicaLogDirsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for AlterReplicaLogDirsObserver {
    type Output = Result<AlterReplicaLogDirsOutcome, AlterReplicaLogDirsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for AlterReplicaLogDirsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterReplicaLogDirsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> AlterReplicaLogDirsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            AlterReplicaLogDirsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => AlterReplicaLogDirsObserverError::Stale,
    }
}
