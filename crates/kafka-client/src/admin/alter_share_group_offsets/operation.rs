//! Named single-observer ShareGroup offset-alteration operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::alter_share_group_offsets::AdminAlterShareGroupOffsets};

use super::AlterShareGroupOffsetsResult;

/// Sole terminal observer for one submitted ShareGroup offset alteration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterShareGroupOffsets {
    inner: AdminAlterShareGroupOffsets,
}

impl AlterShareGroupOffsets {
    pub(crate) const fn from_bridge(inner: AdminAlterShareGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<AlterShareGroupOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for AlterShareGroupOffsets {
    type Output = Result<AlterShareGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
