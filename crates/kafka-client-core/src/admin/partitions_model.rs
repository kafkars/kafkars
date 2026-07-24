//! Validated automatic-assignment input for one ordered `CreatePartitions` batch.

use core::fmt;
use std::collections::BTreeSet;

/// One topic and its requested new total partition count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsSpecification {
    topic: String,
    total_count: i32,
}

impl CreatePartitionsSpecification {
    /// Creates one automatic-assignment partition increase.
    pub const fn new(topic: String, total_count: i32) -> Self {
        Self { topic, total_count }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the requested new total partition count.
    pub const fn total_count(&self) -> i32 {
        self.total_count
    }
}

/// Ordered validated policy input for one `CreatePartitions` RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsPlan {
    topics: Vec<CreatePartitionsSpecification>,
    validate_only: bool,
}

impl CreatePartitionsPlan {
    /// Validates a nonempty batch with unique names and positive total counts.
    pub fn new(
        topics: Vec<CreatePartitionsSpecification>,
        validate_only: bool,
    ) -> Result<Self, CreatePartitionsPlanError> {
        if topics.is_empty() {
            return Err(CreatePartitionsPlanError::EmptyBatch);
        }
        let mut names = BTreeSet::new();
        for topic in &topics {
            if topic.topic.is_empty() {
                return Err(CreatePartitionsPlanError::EmptyTopicName);
            }
            if topic.total_count <= 0 {
                return Err(CreatePartitionsPlanError::InvalidTotalCount);
            }
            if !names.insert(topic.topic.as_str()) {
                return Err(CreatePartitionsPlanError::DuplicateTopic);
            }
        }
        Ok(Self {
            topics,
            validate_only,
        })
    }

    /// Returns requests in original caller order.
    pub fn topics(&self) -> &[CreatePartitionsSpecification] {
        &self.topics
    }

    /// Returns whether Kafka should validate without mutating.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }
}

/// Invalid deterministic `CreatePartitions` input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatePartitionsPlanError {
    /// Kafka cannot execute an empty batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// New total partition counts must be positive.
    InvalidTotalCount,
    /// Topic names in one batch must be unique.
    DuplicateTopic,
}

impl fmt::Display for CreatePartitionsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "CreatePartitions batch is empty",
            Self::EmptyTopicName => "CreatePartitions topic name is empty",
            Self::InvalidTotalCount => "CreatePartitions total count must be positive",
            Self::DuplicateTopic => "CreatePartitions batch contains a duplicate topic",
        })
    }
}

impl std::error::Error for CreatePartitionsPlanError {}
