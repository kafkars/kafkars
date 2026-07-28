//! Named runtime-neutral observation of one Admin `DescribeAcls` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeAclsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{DescribeAclsObserverError, DescribeAclsOutcome, outcome::translate_terminal};

/// Single observer for one accepted ACL description query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeAclsObserver {
    inner: CompletionObserver<DescribeAclsTerminal>,
}

impl DescribeAclsObserver {
    pub(crate) const fn from_completion(inner: CompletionObserver<DescribeAclsTerminal>) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeAclsOutcome, DescribeAclsObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeAclsObserver {
    type Output = Result<DescribeAclsOutcome, DescribeAclsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeAclsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeAclsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeAclsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => DescribeAclsObserverError::AlreadyObserved,
        CompletionObserverError::Stale => DescribeAclsObserverError::Stale,
    }
}
