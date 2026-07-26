//! Named runtime-neutral observation of one assigned-consumer failure event.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::consumer::AssignedConsumerNextEvent as BridgeNextEvent};

use super::AssignedConsumerEvent;

/// Waits for one already-retained direct-consumer failure event.
#[derive(Debug)]
#[must_use = "dropping next_event abandons only this observation"]
pub struct NextAssignedEvent<'consumer> {
    inner: BridgeNextEvent<'consumer>,
}

impl<'consumer> NextAssignedEvent<'consumer> {
    pub(crate) const fn from_bridge(inner: BridgeNextEvent<'consumer>) -> Self {
        Self { inner }
    }

    /// Blocks on the same generation-fenced signal used by [`Future::poll`].
    pub fn wait(self) -> Result<Option<AssignedConsumerEvent>, KafkaError> {
        self.inner.wait()
    }
}

impl Future for NextAssignedEvent<'_> {
    type Output = Result<Option<AssignedConsumerEvent>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
