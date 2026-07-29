//! Engine-owned scalar request for one partition transaction abort.

use kafka_client_core::{AbortPartitionTransactionPlan, AbortPartitionTransactionPlanError};

/// One inert API27 abort specification validated only at admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortPartitionTransactionRequest {
    topic: String,
    partition: i32,
    producer_id: i64,
    producer_epoch: i16,
    coordinator_epoch: i32,
    transaction_version: i8,
}

impl AbortPartitionTransactionRequest {
    /// Creates one inert partition transaction-abort request.
    pub const fn new(
        topic: String,
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i32,
    ) -> Self {
        Self {
            topic,
            partition,
            producer_id,
            producer_epoch,
            coordinator_epoch,
            transaction_version: 0,
        }
    }

    /// Replaces Kafka's transaction-marker version.
    pub const fn with_transaction_version(mut self, transaction_version: i8) -> Self {
        self.transaction_version = transaction_version;
        self
    }

    /// Consumes the request into stable scalar parts.
    pub fn into_parts(self) -> (String, i32, i64, i16, i32, i8) {
        (
            self.topic,
            self.partition,
            self.producer_id,
            self.producer_epoch,
            self.coordinator_epoch,
            self.transaction_version,
        )
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.topic = self.topic.into_boxed_str().into_string();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AbortPartitionTransactionPlan, AbortPartitionTransactionPlanError> {
        let (topic, partition, producer_id, producer_epoch, coordinator_epoch, transaction_version) =
            self.into_parts();
        AbortPartitionTransactionPlan::new(
            topic,
            partition,
            producer_id,
            producer_epoch,
            coordinator_epoch,
        )
        .and_then(|plan| plan.with_transaction_version(transaction_version))
    }
}
