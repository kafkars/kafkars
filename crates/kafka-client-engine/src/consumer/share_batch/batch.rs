//! Linear public ownership of one complete share-consumer delivery.

use std::sync::Arc;

use kafka_client_core::{
    ShareAcknowledgement as CoreShareAcknowledgement, ShareAcknowledgementBuildErrorKind,
    ShareDisposition, ShareRecordDecision,
};

use super::{ShareAcknowledgement, ShareAcknowledgementBuildError, ShareConsumerRecords};
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
    lifetime: Arc<dyn Send + Sync>,
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
            lifetime,
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

    /// Consumes this batch into one `Accept` decision for every application record.
    pub fn accept_all(self) -> Result<ShareAcknowledgement, ShareAcknowledgementBuildError> {
        let mut decisions = Vec::new();
        if decisions.try_reserve_exact(self.record_count()).is_err() {
            return Err(ShareAcknowledgementBuildError::new(
                ShareAcknowledgementBuildErrorKind::AllocationFailed,
                self,
                decisions,
            ));
        }
        decisions.extend(
            self.records()
                .map(|record| record.decision(ShareDisposition::Accept)),
        );
        self.into_acknowledgement(decisions)
    }

    /// Consumes this batch and exact record decisions into normalized wire ranges.
    pub fn into_acknowledgement(
        mut self,
        decisions: Vec<ShareRecordDecision>,
    ) -> Result<ShareAcknowledgement, ShareAcknowledgementBuildError> {
        let delivery = self
            .delivery
            .take()
            .unwrap_or_else(|| unreachable!("share batch owns one delivery"));
        let (fence, partitions, acquisitions) = delivery.into_parts();
        match CoreShareAcknowledgement::try_new(acquisitions, decisions) {
            Ok(acknowledgement) => Ok(ShareAcknowledgement::new(
                acknowledgement,
                partitions,
                self.return_to.clone(),
                Arc::clone(&self.lifetime),
            )),
            Err(error) => {
                let kind = error.kind();
                let (acquisitions, decisions) = error.into_parts();
                self.delivery = Some(ShareFetchDelivery::restore(fence, partitions, acquisitions));
                Err(ShareAcknowledgementBuildError::new(kind, self, decisions))
            }
        }
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
