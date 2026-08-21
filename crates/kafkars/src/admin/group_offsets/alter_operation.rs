//! Named single-observer group-offset alteration for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_group_offsets::AdminAlterConsumerGroupOffsets};

use super::AlterConsumerGroupOffsetsResult;

/// Sole terminal observer for one submitted consumer-group offset alteration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterConsumerGroupOffsets {
    inner: AdminAlterConsumerGroupOffsets,
}

impl AlterConsumerGroupOffsets {
    pub(crate) const fn from_bridge(inner: AdminAlterConsumerGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<AlterConsumerGroupOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for AlterConsumerGroupOffsets {
    type Output = Result<AlterConsumerGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
