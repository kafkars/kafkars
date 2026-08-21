//! Named single-observer ACL deletion operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_delete_acls::AdminDeleteAcls};

use super::DeleteAclsResult;

/// Sole terminal observer for one submitted ACL deletion batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteAcls {
    inner: AdminDeleteAcls,
}

impl DeleteAcls {
    pub(crate) const fn from_bridge(inner: AdminDeleteAcls) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DeleteAclsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DeleteAcls {
    type Output = Result<DeleteAclsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
