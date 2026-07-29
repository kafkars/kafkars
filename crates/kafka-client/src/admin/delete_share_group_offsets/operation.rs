//! Named single-observer ShareGroup offset-deletion operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::delete_share_group_offsets::AdminDeleteShareGroupOffsets};

use super::DeleteShareGroupOffsetsResult;

/// Sole terminal observer for one submitted ShareGroup offset deletion.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteShareGroupOffsets {
    inner: AdminDeleteShareGroupOffsets,
}

impl DeleteShareGroupOffsets {
    pub(crate) const fn from_bridge(inner: AdminDeleteShareGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DeleteShareGroupOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DeleteShareGroupOffsets {
    type Output = Result<DeleteShareGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
