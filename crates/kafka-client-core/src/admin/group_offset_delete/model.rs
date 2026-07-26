//! Validated semantic input for one consumer-group offset deletion.

use core::fmt;
use std::collections::BTreeSet;

const MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;
const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

/// One caller-ordered topic-partition whose committed offset must be deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetTarget {
    topic: String,
    partition: i32,
}

impl DeleteConsumerGroupOffsetTarget {
    /// Creates one target for validation by the enclosing request plan.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the exact UTF-8 topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }
}

/// Validated intent for one destructive consumer-group offset request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetsPlan {
    group_id: String,
    targets: Vec<DeleteConsumerGroupOffsetTarget>,
}

impl DeleteConsumerGroupOffsetsPlan {
    /// Validates one group and a nonempty caller-ordered unique target set.
    pub fn new(
        group_id: String,
        targets: Vec<DeleteConsumerGroupOffsetTarget>,
    ) -> Result<Self, DeleteConsumerGroupOffsetsPlanError> {
        validate_group_id(&group_id)?;
        if targets.is_empty() {
            return Err(DeleteConsumerGroupOffsetsPlanError::EmptyTargetBatch);
        }
        let mut identities = BTreeSet::new();
        for target in &targets {
            validate_target(target)?;
            if !identities.insert((target.topic.as_str(), target.partition)) {
                return Err(DeleteConsumerGroupOffsetsPlanError::DuplicateTopicPartition);
            }
        }
        Ok(Self { group_id, targets })
    }

    /// Returns the exact consumer-group coordinator key.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns topic-partitions in original caller order.
    pub fn targets(&self) -> &[DeleteConsumerGroupOffsetTarget] {
        &self.targets
    }
}

fn validate_group_id(group_id: &str) -> Result<(), DeleteConsumerGroupOffsetsPlanError> {
    if group_id.is_empty() {
        return Err(DeleteConsumerGroupOffsetsPlanError::EmptyGroupId);
    }
    if group_id.len() > MAX_GROUP_ID_BYTES {
        return Err(DeleteConsumerGroupOffsetsPlanError::GroupIdTooLong);
    }
    Ok(())
}

fn validate_target(
    target: &DeleteConsumerGroupOffsetTarget,
) -> Result<(), DeleteConsumerGroupOffsetsPlanError> {
    if target.topic.is_empty() {
        return Err(DeleteConsumerGroupOffsetsPlanError::EmptyTopicName);
    }
    if target.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(DeleteConsumerGroupOffsetsPlanError::TopicNameTooLong);
    }
    if target.partition < 0 {
        return Err(DeleteConsumerGroupOffsetsPlanError::NegativePartition);
    }
    Ok(())
}

/// Invalid deterministic consumer-group offset deletion intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteConsumerGroupOffsetsPlanError {
    /// The request must name one explicit consumer group.
    EmptyGroupId,
    /// The group identity cannot fit Kafka's coordinator key domain.
    GroupIdTooLong,
    /// Kafka cannot delete an empty topic-partition batch.
    EmptyTargetBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// A topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// One request cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
}

impl fmt::Display for DeleteConsumerGroupOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyGroupId => "consumer group id is empty",
            Self::GroupIdTooLong => "consumer group id exceeds the coordinator key limit",
            Self::EmptyTargetBatch => "consumer group offset deletion batch is empty",
            Self::EmptyTopicName => "consumer group offset deletion topic is empty",
            Self::TopicNameTooLong => "consumer group offset deletion topic is too long",
            Self::NegativePartition => "consumer group offset deletion partition is negative",
            Self::DuplicateTopicPartition => {
                "consumer group offset deletion contains a duplicate topic-partition"
            }
        })
    }
}

impl std::error::Error for DeleteConsumerGroupOffsetsPlanError {}
