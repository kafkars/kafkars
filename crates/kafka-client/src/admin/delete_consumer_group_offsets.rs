//! Named single-observer group-offset deletion for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError, bridge::admin_group_offset_delete_operation::AdminDeleteConsumerGroupOffsets,
};

use super::DeleteConsumerGroupOffsetsResult;

/// Sole terminal observer for one submitted consumer-group offset deletion.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteConsumerGroupOffsets {
    inner: AdminDeleteConsumerGroupOffsets,
}

impl DeleteConsumerGroupOffsets {
    pub(crate) const fn from_bridge(inner: AdminDeleteConsumerGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DeleteConsumerGroupOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DeleteConsumerGroupOffsets {
    type Output = Result<DeleteConsumerGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
