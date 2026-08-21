//! Named single-observer metadata-quorum voter-removal operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::remove_raft_voter::AdminRemoveRaftVoter};

use super::RemoveRaftVoterResult;

/// Sole terminal observer for one submitted metadata-quorum voter removal.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct RemoveRaftVoter {
    inner: AdminRemoveRaftVoter,
}

impl RemoveRaftVoter {
    pub(crate) const fn from_bridge(inner: AdminRemoveRaftVoter) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<RemoveRaftVoterResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for RemoveRaftVoter {
    type Output = Result<RemoveRaftVoterResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
