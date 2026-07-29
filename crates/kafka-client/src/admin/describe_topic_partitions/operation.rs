//! Named single-observer operation for one Admin `DescribeTopicPartitions` page.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_topic_partitions::AdminDescribeTopicPartitions};

use super::DescribeTopicPartitionsPage;

/// Sole terminal observer for one submitted topic-partition page.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeTopicPartitions {
    inner: AdminDescribeTopicPartitions,
}

impl DescribeTopicPartitions {
    pub(crate) const fn from_bridge(inner: AdminDescribeTopicPartitions) -> Self {
        Self { inner }
    }

    /// Blocks on the same sole terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeTopicPartitionsPage, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeTopicPartitions {
    type Output = Result<DescribeTopicPartitionsPage, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
