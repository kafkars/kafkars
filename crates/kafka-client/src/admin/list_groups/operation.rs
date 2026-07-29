//! Named single-observer cluster-wide all-group listing operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_list_groups::AdminListGroups};

use super::ListGroupsResult;

/// Sole terminal observer for one submitted all-group listing.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListGroups {
    inner: AdminListGroups,
}

impl ListGroups {
    pub(crate) const fn from_bridge(inner: AdminListGroups) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListGroupsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListGroups {
    type Output = Result<ListGroupsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
