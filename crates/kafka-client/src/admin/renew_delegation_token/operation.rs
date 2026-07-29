//! Named single-observer delegation-token renewal operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::renew_delegation_token::AdminRenewDelegationToken};

use super::RenewDelegationTokenResult;

/// Sole terminal observer for one submitted delegation-token renewal.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct RenewDelegationToken {
    inner: AdminRenewDelegationToken,
}

impl RenewDelegationToken {
    pub(crate) const fn from_bridge(inner: AdminRenewDelegationToken) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<RenewDelegationTokenResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for RenewDelegationToken {
    type Output = Result<RenewDelegationTokenResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
