//! Named single-observer delegation-token creation operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::create_delegation_token::AdminCreateDelegationToken};

use super::CreateDelegationTokenResult;

/// Sole terminal observer for one submitted delegation-token creation.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct CreateDelegationToken {
    inner: AdminCreateDelegationToken,
}

impl CreateDelegationToken {
    pub(crate) const fn from_bridge(inner: AdminCreateDelegationToken) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<CreateDelegationTokenResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for CreateDelegationToken {
    type Output = Result<CreateDelegationTokenResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
