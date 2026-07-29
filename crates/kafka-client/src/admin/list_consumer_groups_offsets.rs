//! Named observer for one multi-consumer-group committed-offset operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_group_offsets::AdminListConsumerGroupsOffsets};

use super::ListConsumerGroupsOffsetsResult;

/// Sole terminal observer for one submitted multi-group offset query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListConsumerGroupsOffsets {
    inner: AdminListConsumerGroupsOffsets,
}

impl ListConsumerGroupsOffsets {
    pub(crate) const fn from_bridge(inner: AdminListConsumerGroupsOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListConsumerGroupsOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListConsumerGroupsOffsets {
    type Output = Result<ListConsumerGroupsOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
