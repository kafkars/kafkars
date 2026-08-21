//! Named observer for one multi-ShareGroup offset operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::list_share_group_offsets::AdminListShareGroupsOffsets};

use super::ListShareGroupsOffsetsResult;

/// Sole terminal observer for one accepted multi-ShareGroup offset query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListShareGroupsOffsets {
    inner: AdminListShareGroupsOffsets,
}

impl ListShareGroupsOffsets {
    pub(crate) const fn from_bridge(inner: AdminListShareGroupsOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListShareGroupsOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListShareGroupsOffsets {
    type Output = Result<ListShareGroupsOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
