//! Named single-observer `ShareGroup` offset-listing operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::list_share_group_offsets::AdminListShareGroupOffsets};

use super::ListShareGroupOffsetsResult;

/// Sole terminal observer for one submitted `ShareGroup` offset listing.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListShareGroupOffsets {
    inner: AdminListShareGroupOffsets,
}

impl ListShareGroupOffsets {
    pub(crate) const fn from_bridge(inner: AdminListShareGroupOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListShareGroupOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListShareGroupOffsets {
    type Output = Result<ListShareGroupOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
