//! Named single-observer group-offset query for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_group_offsets::AdminListConsumerGroupOffsets};

use super::ListConsumerGroupOffsetsResult;

/// Sole terminal observer for one submitted consumer-group offset query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListConsumerGroupOffsets {
    inner: AdminListConsumerGroupOffsets,
}

impl ListConsumerGroupOffsets {
    pub(crate) const fn from_bridge(inner: AdminListConsumerGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListConsumerGroupOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListConsumerGroupOffsets {
    type Output = Result<ListConsumerGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
