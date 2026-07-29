//! Named single-observer metadata-quorum voter-addition operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::add_raft_voter::AdminAddRaftVoter};

use super::AddRaftVoterResult;

/// Sole terminal observer for one submitted metadata-quorum voter addition.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AddRaftVoter {
    inner: AdminAddRaftVoter,
}

impl AddRaftVoter {
    pub(crate) const fn from_bridge(inner: AdminAddRaftVoter) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<AddRaftVoterResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for AddRaftVoter {
    type Output = Result<AddRaftVoterResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
