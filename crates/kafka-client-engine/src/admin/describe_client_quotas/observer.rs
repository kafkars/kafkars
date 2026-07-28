//! Named runtime-neutral observation of one Admin `DescribeClientQuotas` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeClientQuotasTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeClientQuotasObserverError, DescribeClientQuotasOutcome, outcome::translate_terminal,
};

/// Single observer for one accepted CLIENT QUOTA description query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeClientQuotasObserver {
    inner: CompletionObserver<DescribeClientQuotasTerminal>,
}

impl DescribeClientQuotasObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeClientQuotasTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(self) -> Result<DescribeClientQuotasOutcome, DescribeClientQuotasObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeClientQuotasObserver {
    type Output = Result<DescribeClientQuotasOutcome, DescribeClientQuotasObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeClientQuotasObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeClientQuotasObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeClientQuotasObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DescribeClientQuotasObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DescribeClientQuotasObserverError::Stale,
    }
}
