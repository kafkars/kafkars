//! Named single-observer ACL creation operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_create_acls::AdminCreateAcls};

use super::CreateAclsResult;

/// Sole terminal observer for one submitted ACL creation batch.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct CreateAcls {
    inner: AdminCreateAcls,
}

impl CreateAcls {
    pub(crate) const fn from_bridge(inner: AdminCreateAcls) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<CreateAclsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for CreateAcls {
    type Output = Result<CreateAclsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
