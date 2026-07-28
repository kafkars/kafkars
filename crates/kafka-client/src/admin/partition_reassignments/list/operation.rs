//! Named single-observer reassignment query for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError, bridge::admin_list_partition_reassignments::AdminListPartitionReassignments,
};

use super::ListPartitionReassignmentsResult;

/// Sole terminal observer for one submitted reassignment query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListPartitionReassignments {
    inner: AdminListPartitionReassignments,
}

impl ListPartitionReassignments {
    pub(crate) const fn from_bridge(inner: AdminListPartitionReassignments) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListPartitionReassignmentsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListPartitionReassignments {
    type Output = Result<ListPartitionReassignmentsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
