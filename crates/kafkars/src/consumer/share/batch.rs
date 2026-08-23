//! Linear facade ownership over one exact share-consumer delivery.

use crate::bridge::share_consumer::ShareConsumerBatch as BridgeBatch;

use super::{
    ShareAcknowledgement, ShareAcknowledgementBuildError, ShareConsumerRecord,
    ShareConsumerRecords, ShareRecordDecision,
};

/// One response-wide share delivery and its exact broker acquisitions.
///
/// Dropping the batch synchronously returns its local retained bytes without
/// sending an acknowledgement. Kafka may redeliver records after their broker
/// acquisition locks expire.
#[must_use = "dropping the batch abandons its share acquisitions without acknowledging them"]
pub struct ShareConsumerBatch {
    inner: BridgeBatch,
}

impl ShareConsumerBatch {
    pub(crate) const fn from_bridge(inner: BridgeBatch) -> Self {
        Self { inner }
    }

    /// Returns the number of normalized application records.
    pub fn len(&self) -> usize {
        self.inner.record_count()
    }

    /// Returns whether the batch contains no application records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of topic-partitions represented by this delivery.
    pub fn partition_count(&self) -> usize {
        self.inner.partition_count()
    }

    /// Returns the number of exact broker-acquired offset ranges.
    pub fn acquisition_count(&self) -> usize {
        self.inner.acquisition_count()
    }

    /// Iterates normalized application records without copying their bytes.
    pub fn records(&self) -> ShareConsumerRecords<'_> {
        ShareConsumerRecords::from_bridge(self.inner.records())
    }

    /// Iterates normalized application records without copying their bytes.
    pub fn iter(&self) -> ShareConsumerRecords<'_> {
        self.records()
    }

    /// Consumes this batch into one `Accept` decision for every record.
    pub fn accept_all(self) -> Result<ShareAcknowledgement, ShareAcknowledgementBuildError> {
        self.inner
            .accept_all()
            .map(ShareAcknowledgement::from_bridge)
            .map_err(ShareAcknowledgementBuildError::from_bridge)
    }

    /// Consumes this batch and exact record decisions into one acknowledgement.
    pub fn into_acknowledgement(
        self,
        decisions: Vec<ShareRecordDecision>,
    ) -> Result<ShareAcknowledgement, ShareAcknowledgementBuildError> {
        self.inner
            .into_acknowledgement(
                decisions
                    .into_iter()
                    .map(ShareRecordDecision::into_bridge)
                    .collect(),
            )
            .map(ShareAcknowledgement::from_bridge)
            .map_err(ShareAcknowledgementBuildError::from_bridge)
    }
}

impl<'batch> IntoIterator for &'batch ShareConsumerBatch {
    type Item = ShareConsumerRecord<'batch>;
    type IntoIter = ShareConsumerRecords<'batch>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl std::fmt::Debug for ShareConsumerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumerBatch")
            .field("partition_count", &self.partition_count())
            .field("acquisition_count", &self.acquisition_count())
            .field("len", &self.len())
            .finish()
    }
}
