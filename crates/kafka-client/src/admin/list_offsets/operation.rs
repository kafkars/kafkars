//! Named single-observer Admin `ListOffsets` operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_list_offsets::AdminListOffsets};

use super::ListOffsetsResult;

/// Sole terminal observer for one submitted Admin `ListOffsets` query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListOffsets {
    inner: AdminListOffsets,
}

impl ListOffsets {
    pub(crate) const fn from_bridge(inner: AdminListOffsets) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListOffsetsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListOffsets {
    type Output = Result<ListOffsetsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
