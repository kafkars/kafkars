//! Named single-observer member removal for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError, bridge::admin_remove_consumer_group_members::AdminRemoveConsumerGroupMembers,
};

use super::RemoveConsumerGroupMembersResult;

/// Sole terminal observer for one submitted static-member removal.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct RemoveConsumerGroupMembers {
    inner: AdminRemoveConsumerGroupMembers,
}

impl RemoveConsumerGroupMembers {
    pub(crate) const fn from_bridge(inner: AdminRemoveConsumerGroupMembers) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<RemoveConsumerGroupMembersResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for RemoveConsumerGroupMembers {
    type Output = Result<RemoveConsumerGroupMembersResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
