//! Named single-observer cluster-wide transaction-listing operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_list_transactions::AdminListTransactions};

use super::ListTransactionsResult;

/// Sole terminal observer for one submitted cluster-wide transaction listing.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct ListTransactions {
    inner: AdminListTransactions,
}

impl ListTransactions {
    pub(crate) const fn from_bridge(inner: AdminListTransactions) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<ListTransactionsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for ListTransactions {
    type Output = Result<ListTransactionsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
