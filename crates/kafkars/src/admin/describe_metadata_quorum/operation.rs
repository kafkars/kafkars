//! Named single-observer metadata-quorum description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::describe_metadata_quorum::AdminDescribeMetadataQuorum};

use super::MetadataQuorumDescription;

/// Sole terminal observer for one submitted metadata-quorum description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeMetadataQuorum {
    inner: AdminDescribeMetadataQuorum,
}

impl DescribeMetadataQuorum {
    pub(crate) const fn from_bridge(inner: AdminDescribeMetadataQuorum) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<MetadataQuorumDescription, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeMetadataQuorum {
    type Output = Result<MetadataQuorumDescription, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
