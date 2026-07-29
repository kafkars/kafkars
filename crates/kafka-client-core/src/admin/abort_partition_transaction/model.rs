//! Validated identity of one open transaction on one Kafka partition.

use core::fmt;

/// Kafka's maximum retained UTF-8 topic-name bytes.
pub const ABORT_PARTITION_TRANSACTION_MAX_TOPIC_NAME_BYTES: usize = 249;

/// Validated intent for one destructive partition-transaction abort.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortPartitionTransactionPlan {
    topic: String,
    partition: i32,
    producer_id: i64,
    producer_epoch: i16,
    coordinator_epoch: i32,
}

impl AbortPartitionTransactionPlan {
    /// Validates the complete broker-issued transaction identity.
    pub fn new(
        topic: String,
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i32,
    ) -> Result<Self, AbortPartitionTransactionPlanError> {
        if topic.is_empty() {
            return Err(AbortPartitionTransactionPlanError::EmptyTopicName);
        }
        if topic.len() > ABORT_PARTITION_TRANSACTION_MAX_TOPIC_NAME_BYTES {
            return Err(AbortPartitionTransactionPlanError::TopicNameTooLong);
        }
        if partition < 0 {
            return Err(AbortPartitionTransactionPlanError::NegativePartition);
        }
        if producer_id < 0 {
            return Err(AbortPartitionTransactionPlanError::NegativeProducerId);
        }
        if producer_epoch < 0 {
            return Err(AbortPartitionTransactionPlanError::NegativeProducerEpoch);
        }
        if coordinator_epoch < 0 {
            return Err(AbortPartitionTransactionPlanError::NegativeCoordinatorEpoch);
        }
        Ok(Self {
            topic,
            partition,
            producer_id,
            producer_epoch,
            coordinator_epoch,
        })
    }

    /// Returns the exact topic containing the open transaction.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns Kafka's exact nonnegative producer identity.
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's exact nonnegative producer epoch.
    pub const fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns Kafka's exact nonnegative transaction-coordinator epoch.
    pub const fn coordinator_epoch(&self) -> i32 {
        self.coordinator_epoch
    }

    /// Consumes the plan into adapter-owned scalar values.
    pub fn into_parts(self) -> (String, i32, i64, i16, i32) {
        (
            self.topic,
            self.partition,
            self.producer_id,
            self.producer_epoch,
            self.coordinator_epoch,
        )
    }
}

/// Invalid deterministic partition-transaction abort intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionPlanError {
    /// Topic names must contain at least one UTF-8 byte.
    EmptyTopicName,
    /// The topic name exceeds Kafka's bounded string domain.
    TopicNameTooLong,
    /// Kafka partition indices cannot be negative.
    NegativePartition,
    /// Kafka producer identities cannot be negative.
    NegativeProducerId,
    /// Kafka producer epochs cannot be negative.
    NegativeProducerEpoch,
    /// Kafka transaction-coordinator epochs cannot be negative.
    NegativeCoordinatorEpoch,
}

impl fmt::Display for AbortPartitionTransactionPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid partition-transaction abort plan: {self:?}"
        )
    }
}

impl std::error::Error for AbortPartitionTransactionPlanError {}
