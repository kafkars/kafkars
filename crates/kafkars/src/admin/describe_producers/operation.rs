//! Named single-observer Admin `DescribeProducers` operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_producers::AdminDescribeProducers};

use super::DescribeProducersResult;

/// Sole terminal observer for one submitted Admin `DescribeProducers` query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeProducers {
    inner: AdminDescribeProducers,
}

impl DescribeProducers {
    pub(crate) const fn from_bridge(inner: AdminDescribeProducers) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeProducersResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeProducers {
    type Output = Result<DescribeProducersResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
