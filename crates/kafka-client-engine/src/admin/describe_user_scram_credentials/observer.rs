//! Named runtime-neutral observation of one Admin `DescribeUserScramCredentials` terminal.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_core::DescribeUserScramCredentialsTerminal;

use crate::completion::{CompletionObserver, CompletionObserverError};

use super::{
    DescribeUserScramCredentialsObserverError, DescribeUserScramCredentialsOutcome,
    outcome::translate_terminal,
};

/// Single observer for one accepted SCRAM credential-description query.
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeUserScramCredentialsObserver {
    inner: CompletionObserver<DescribeUserScramCredentialsTerminal>,
}

impl DescribeUserScramCredentialsObserver {
    pub(crate) const fn from_completion(
        inner: CompletionObserver<DescribeUserScramCredentialsTerminal>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal cell used by `Future::poll`.
    pub fn wait(
        self,
    ) -> Result<DescribeUserScramCredentialsOutcome, DescribeUserScramCredentialsObserverError>
    {
        self.inner
            .wait()
            .map(translate_terminal)
            .map_err(observer_error)
    }
}

impl Future for DescribeUserScramCredentialsObserver {
    type Output =
        Result<DescribeUserScramCredentialsOutcome, DescribeUserScramCredentialsObserverError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(translate_terminal).map_err(observer_error))
    }
}

impl fmt::Debug for DescribeUserScramCredentialsObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeUserScramCredentialsObserver")
            .finish_non_exhaustive()
    }
}

const fn observer_error(
    error: CompletionObserverError,
) -> DescribeUserScramCredentialsObserverError {
    match error {
        CompletionObserverError::AlreadyObserved => {
            DescribeUserScramCredentialsObserverError::AlreadyObserved
        }
        CompletionObserverError::Stale => DescribeUserScramCredentialsObserverError::Stale,
    }
}
