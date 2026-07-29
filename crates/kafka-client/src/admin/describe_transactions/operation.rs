//! Named single-observer Admin `DescribeTransactions` operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_transactions::AdminDescribeTransactions};

use super::DescribeTransactionsResult;

/// Sole terminal observer for one submitted Admin `DescribeTransactions` query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeTransactions {
    inner: AdminDescribeTransactions,
}

impl DescribeTransactions {
    pub(crate) const fn from_bridge(inner: AdminDescribeTransactions) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeTransactionsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeTransactions {
    type Output = Result<DescribeTransactionsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
