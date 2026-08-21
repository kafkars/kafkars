//! Named public observation of assigned-consumer close settlement.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::consumer::AssignedConsumerClose};

/// Sole terminal observer for one assigned-consumer close.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted consumer close work"]
pub struct CloseAssignedConsumer {
    inner: AssignedConsumerClose,
}

impl CloseAssignedConsumer {
    pub(crate) const fn from_bridge(inner: AssignedConsumerClose) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for CloseAssignedConsumer {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
