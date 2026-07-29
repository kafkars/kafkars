//! Named single-observer delegation-token expiration operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::expire_delegation_token::AdminExpireDelegationToken};

use super::ExpireDelegationTokenResult;

/// Sole terminal observer for one submitted delegation-token expiration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ExpireDelegationToken {
    inner: AdminExpireDelegationToken,
}

impl ExpireDelegationToken {
    pub(crate) const fn from_bridge(inner: AdminExpireDelegationToken) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ExpireDelegationTokenResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ExpireDelegationToken {
    type Output = Result<ExpireDelegationTokenResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
