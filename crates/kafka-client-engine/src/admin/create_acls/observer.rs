//! Named runtime-neutral observation of one Admin `CreateAcls` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{CreateAclsObserverError, CreateAclsOutcome};

/// Single observer for one accepted ACL creation batch.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct CreateAclsObserver {
    inner: CompletionObserver<CreateAclsOutcome>,
}

impl CreateAclsObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<CreateAclsOutcome>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<CreateAclsOutcome, CreateAclsObserverError> {
        self.inner.wait().map_err(observer_error)
    }
}

impl Future for CreateAclsObserver {
    type Output = Result<CreateAclsOutcome, CreateAclsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map_err(observer_error))
    }
}

impl fmt::Debug for CreateAclsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateAclsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> CreateAclsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => CreateAclsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => CreateAclsObserverError::Stale,
    }
}
