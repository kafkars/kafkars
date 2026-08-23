//! Linear public ownership of one complete share-consumer delivery.

use std::sync::Arc;

use super::ShareConsumerRecords;
use crate::consumer::share::{ShareConsumerPort, ShareFetchDelivery};

/// One response-wide share delivery and its exact broker-lock capabilities.
///
/// Dropping the batch synchronously returns every acquisition to its broker
/// session without sending an acknowledgement. Kafka may redeliver those
/// records after their broker lock expires.
#[must_use = "dropping the batch abandons its share acquisitions without acknowledging them"]
pub struct ShareConsumerBatch {
    delivery: Option<ShareFetchDelivery>,
    return_to: ShareConsumerPort,
    _lifetime: Arc<dyn Send + Sync>,
}

impl ShareConsumerBatch {
    pub(in crate::consumer) fn new(
        delivery: ShareFetchDelivery,
        return_to: ShareConsumerPort,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            return_to,
            _lifetime: lifetime,
        }
    }

    /// Iterates all normalized application records in response order.
    pub fn records(&self) -> ShareConsumerRecords<'_> {
        ShareConsumerRecords::new(self.delivery())
    }

    /// Counts normalized application records without copying their bytes.
    pub fn record_count(&self) -> usize {
        self.records().count()
    }

    /// Returns the number of topic-partitions represented by this response.
    pub fn partition_count(&self) -> usize {
        self.delivery().partitions().len()
    }

    /// Returns the number of exact broker-acquired offset ranges.
    pub fn acquisition_count(&self) -> usize {
        self.delivery().acquisitions().len()
    }

    pub(super) fn delivery(&self) -> &ShareFetchDelivery {
        self.delivery
            .as_ref()
            .unwrap_or_else(|| unreachable!("public share batch methods cannot run after Drop"))
    }
}

impl Drop for ShareConsumerBatch {
    fn drop(&mut self) {
        if let Some(delivery) = self.delivery.take() {
            self.return_to.return_delivery_blocking(delivery);
        }
    }
}

impl std::fmt::Debug for ShareConsumerBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumerBatch")
            .field("partition_count", &self.partition_count())
            .field("acquisition_count", &self.acquisition_count())
            .field("record_count", &self.record_count())
            .finish()
    }
}
