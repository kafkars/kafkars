//! Named runtime-neutral observation of one Admin `DeleteAcls` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{DeleteAclsObserverError, DeleteAclsOutcome};

/// Single observer for one accepted ACL deletion batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteAclsObserver {
    inner: CompletionObserver<DeleteAclsOutcome>,
}

impl DeleteAclsObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<DeleteAclsOutcome>) -> Self {
        Self { inner }
    }

    /// Blocks on the same stable terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DeleteAclsOutcome, DeleteAclsObserverError> {
        self.inner.wait().map_err(observer_error)
    }
}

impl Future for DeleteAclsObserver {
    type Output = Result<DeleteAclsOutcome, DeleteAclsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map_err(observer_error))
    }
}

impl fmt::Debug for DeleteAclsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteAclsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DeleteAclsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DeleteAclsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DeleteAclsObserverError::Stale,
    }
}
