//! Named runtime-neutral observation of one assigned-consumer record batch.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::consumer::AssignedConsumerRecv as BridgeRecv};

use super::RecordBatch;

/// Waits for one already-authorized direct-consumer delivery.
#[derive(Debug)]
#[must_use = "dropping recv abandons only observation; background Fetch continues"]
pub struct RecvAssignedBatch<'consumer> {
    inner: BridgeRecv<'consumer>,
}

impl<'consumer> RecvAssignedBatch<'consumer> {
    pub(crate) const fn from_bridge(inner: BridgeRecv<'consumer>) -> Self {
        Self { inner }
    }

    /// Blocks on the same generation-fenced signal used by [`Future::poll`].
    pub fn wait(self) -> Result<Option<RecordBatch>, KafkaError> {
        self.inner
            .wait()
            .map(|batch| batch.map(RecordBatch::from_bridge))
    }
}

impl Future for RecvAssignedBatch<'_> {
    type Output = Result<Option<RecordBatch>, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| result.map(|batch| batch.map(RecordBatch::from_bridge)))
    }
}
