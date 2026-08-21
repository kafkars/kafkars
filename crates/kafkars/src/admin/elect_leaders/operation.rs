//! Named single-observer leader election alteration for async and blocking Rust.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_elect_leaders::AdminElectLeaders};

use super::ElectLeadersResult;

/// Sole terminal observer for one submitted leader election.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ElectLeaders {
    inner: AdminElectLeaders,
}

impl ElectLeaders {
    pub(crate) const fn from_bridge(inner: AdminElectLeaders) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ElectLeadersResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ElectLeaders {
    type Output = Result<ElectLeadersResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
