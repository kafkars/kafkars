//! Inert public partition-transaction abort intent translated at submission.

use kafka_client_engine::AbortPartitionTransactionRequest as EngineRequest;

use crate::admin::AbortTransactionSpec;

// Kafka partitions are nonnegative. Preserving assignment-only misuse as this
// sentinel defers its rejection until after the engine captures the public
// deadline at `submit`.
const INVALID_ASSIGNMENT_POSITION_PARTITION: i32 = i32::MIN;

/// Linear request retained by the public builder before submission.
pub(crate) struct AbortPartitionTransactionAdminRequest {
    spec: AbortTransactionSpec,
}

impl AbortPartitionTransactionAdminRequest {
    pub(crate) const fn new(spec: AbortTransactionSpec) -> Self {
        Self { spec }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        let (topic_partition, producer_id, producer_epoch, coordinator_epoch, transaction_version) =
            self.spec.into_parts();
        let (topic, partition, start) = topic_partition.into_parts();
        let partition = if start.is_some() {
            INVALID_ASSIGNMENT_POSITION_PARTITION
        } else {
            partition
        };
        EngineRequest::new(
            topic,
            partition,
            producer_id,
            producer_epoch,
            coordinator_epoch,
        )
        .with_transaction_version(transaction_version)
    }
}

impl std::fmt::Debug for AbortPartitionTransactionAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AbortPartitionTransactionAdminRequest")
            .field("spec", &self.spec)
            .finish()
    }
}
