//! Named single-observer Kafka feature discovery operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_features::AdminDescribeFeatures};

use super::DescribeFeaturesResult;

/// Sole terminal observer for one submitted Kafka feature discovery.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeFeatures {
    inner: AdminDescribeFeatures,
}

impl DescribeFeatures {
    pub(crate) const fn from_bridge(inner: AdminDescribeFeatures) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeFeaturesResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeFeatures {
    type Output = Result<DescribeFeaturesResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
