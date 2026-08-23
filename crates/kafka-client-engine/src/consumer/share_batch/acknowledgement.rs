//! Linear public acknowledgement ownership and lossless normalization rejection.

use std::sync::Arc;

use kafka_client_core::{
    ShareAcknowledgement as CoreShareAcknowledgement, ShareAcknowledgementBuildErrorKind,
    ShareRecordDecision,
};

use crate::consumer::share::{ShareConsumerPort, ShareFetchDelivery, ShareFetchDeliveryPartition};

use super::ShareConsumerBatch;

/// One exact normalized share-delivery acknowledgement not yet admitted to transport.
#[must_use = "dropping an acknowledgement sends nothing and abandons its acquisitions"]
pub struct ShareAcknowledgement {
    pub(super) inner: Option<CoreShareAcknowledgement>,
    partitions: Vec<ShareFetchDeliveryPartition>,
    return_to: ShareConsumerPort,
    _lifetime: Arc<dyn Send + Sync>,
}

impl ShareAcknowledgement {
    pub(super) fn new(
        inner: CoreShareAcknowledgement,
        partitions: Vec<ShareFetchDeliveryPartition>,
        return_to: ShareConsumerPort,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            inner: Some(inner),
            partitions,
            return_to,
            _lifetime: lifetime,
        }
    }

    /// Returns the number of exact acquired ranges consumed by this capability.
    pub fn acquisition_count(&self) -> usize {
        self.inner().acquisitions().len()
    }

    /// Returns the number of normalized topic-partition ranges sent to Kafka.
    pub fn range_count(&self) -> usize {
        self.inner().batches().len()
    }

    fn inner(&self) -> &CoreShareAcknowledgement {
        self.inner
            .as_ref()
            .unwrap_or_else(|| unreachable!("public acknowledgement retains core ownership"))
    }
}

impl Drop for ShareAcknowledgement {
    fn drop(&mut self) {
        let Some(acknowledgement) = self.inner.take() else {
            return;
        };
        let fence = acknowledgement.fence();
        let (acquisitions, batches) = acknowledgement.into_parts();
        drop(batches);
        let delivery =
            ShareFetchDelivery::restore(fence, std::mem::take(&mut self.partitions), acquisitions);
        self.return_to.return_delivery_blocking(delivery);
    }
}

impl std::fmt::Debug for ShareAcknowledgement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgement")
            .field("acquisition_count", &self.acquisition_count())
            .field("range_count", &self.range_count())
            .finish_non_exhaustive()
    }
}

/// Normalization rejection retaining the exact batch and caller decisions.
#[must_use = "a rejected acknowledgement build still owns the exact share batch"]
pub struct ShareAcknowledgementBuildError {
    kind: ShareAcknowledgementBuildErrorKind,
    batch: Box<ShareConsumerBatch>,
    decisions: Vec<ShareRecordDecision>,
}

impl ShareAcknowledgementBuildError {
    pub(super) fn new(
        kind: ShareAcknowledgementBuildErrorKind,
        batch: ShareConsumerBatch,
        decisions: Vec<ShareRecordDecision>,
    ) -> Self {
        Self {
            kind,
            batch: Box::new(batch),
            decisions,
        }
    }

    /// Returns the stable normalization rejection category.
    pub const fn kind(&self) -> ShareAcknowledgementBuildErrorKind {
        self.kind
    }

    /// Recovers the exact batch and caller decisions without reconstruction.
    pub fn into_parts(self) -> (ShareConsumerBatch, Vec<ShareRecordDecision>) {
        (*self.batch, self.decisions)
    }
}

impl std::fmt::Debug for ShareAcknowledgementBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementBuildError")
            .field("kind", &self.kind)
            .field("decision_count", &self.decisions.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ShareAcknowledgementBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "share acknowledgement build failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ShareAcknowledgementBuildError {}
