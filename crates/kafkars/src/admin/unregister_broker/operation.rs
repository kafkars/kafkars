//! Named single-observer Kafka broker-unregistration operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::unregister_broker::AdminUnregisterBroker};

use super::UnregisterBrokerResult;

/// Sole terminal observer for one submitted broker unregistration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct UnregisterBroker {
    inner: AdminUnregisterBroker,
}

impl UnregisterBroker {
    pub(crate) const fn from_bridge(inner: AdminUnregisterBroker) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<UnregisterBrokerResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for UnregisterBroker {
    type Output = Result<UnregisterBrokerResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
