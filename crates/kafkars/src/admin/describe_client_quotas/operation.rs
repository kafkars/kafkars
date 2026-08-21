//! Named single-observer client-quota description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_client_quotas::AdminDescribeClientQuotas};

use super::DescribeClientQuotasResult;

/// Sole terminal observer for one submitted client-quota description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeClientQuotas {
    inner: AdminDescribeClientQuotas,
}

impl DescribeClientQuotas {
    pub(crate) const fn from_bridge(inner: AdminDescribeClientQuotas) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeClientQuotasResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeClientQuotas {
    type Output = Result<DescribeClientQuotasResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
