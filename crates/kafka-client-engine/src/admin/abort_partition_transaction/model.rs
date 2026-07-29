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
        }
    }

    /// Consumes the request into stable scalar parts.
    pub fn into_parts(self) -> (String, i32, i64, i16, i32) {
        (
            self.topic,
            self.partition,
            self.producer_id,
            self.producer_epoch,
            self.coordinator_epoch,
        )
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.topic = self.topic.into_boxed_str().into_string();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AbortPartitionTransactionPlan, AbortPartitionTransactionPlanError> {
        let (topic, partition, producer_id, producer_epoch, coordinator_epoch) = self.into_parts();
        AbortPartitionTransactionPlan::new(
            topic,
            partition,
            producer_id,
            producer_epoch,
            coordinator_epoch,
        )
    }
}
