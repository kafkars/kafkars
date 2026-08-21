//! Named single-observer client-quota alteration operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::alter_client_quotas::AdminAlterClientQuotas};

use super::AlterClientQuotasResult;

/// Sole terminal observer for one submitted client-quota alteration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterClientQuotas {
    inner: AdminAlterClientQuotas,
}

impl AlterClientQuotas {
    pub(crate) const fn from_bridge(inner: AdminAlterClientQuotas) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<AlterClientQuotasResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for AlterClientQuotas {
    type Output = Result<AlterClientQuotasResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
