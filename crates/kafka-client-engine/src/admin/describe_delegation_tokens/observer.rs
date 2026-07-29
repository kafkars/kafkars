//! Named runtime-neutral observation of one delegation-token description.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeDelegationTokensTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeDelegationTokensObserverError, DescribeDelegationTokensOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted token description.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeDelegationTokensObserver {
    inner: CompletionObserver<DescribeDelegationTokensTerminal>,
}

impl DescribeDelegationTokensObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeDelegationTokensTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<DescribeDelegationTokensOutcome, DescribeDelegationTokensObserverError> {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeDelegationTokensObserver {
    type Output = Result<DescribeDelegationTokensOutcome, DescribeDelegationTokensObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeDelegationTokensObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeDelegationTokensObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(error: CompletionObserverError) -> DescribeDelegationTokensObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DescribeDelegationTokensObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DescribeDelegationTokensObserverError::Stale,
    }
}
