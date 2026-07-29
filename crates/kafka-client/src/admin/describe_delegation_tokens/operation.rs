//! Named single-observer delegation-token description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_delegation_tokens::AdminDescribeDelegationTokens};

use super::DescribeDelegationTokensResult;

/// Sole terminal observer for one submitted delegation-token description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeDelegationTokens {
    inner: AdminDescribeDelegationTokens,
}

impl DescribeDelegationTokens {
    pub(crate) const fn from_bridge(inner: AdminDescribeDelegationTokens) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeDelegationTokensResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeDelegationTokens {
    type Output = Result<DescribeDelegationTokensResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
