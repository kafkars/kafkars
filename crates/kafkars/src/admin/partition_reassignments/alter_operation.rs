//! Named single-observer reassignment alteration for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_partition_reassignments::AdminAlterPartitionReassignments};

use super::AlterPartitionReassignmentsResult;

/// Sole terminal observer for one submitted reassignment alteration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterPartitionReassignments {
    inner: AdminAlterPartitionReassignments,
}

impl AlterPartitionReassignments {
    pub(crate) const fn from_bridge(inner: AdminAlterPartitionReassignments) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<AlterPartitionReassignmentsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for AlterPartitionReassignments {
    type Output = Result<AlterPartitionReassignmentsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
