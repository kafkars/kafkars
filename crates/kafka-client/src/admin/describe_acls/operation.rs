//! Named single-observer ACL description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_describe_acls::AdminDescribeAcls};

use super::DescribeAclsResult;

/// Sole terminal observer for one submitted ACL description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeAcls {
    inner: AdminDescribeAcls,
}

impl DescribeAcls {
    pub(crate) const fn from_bridge(inner: AdminDescribeAcls) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeAclsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeAcls {
    type Output = Result<DescribeAclsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
