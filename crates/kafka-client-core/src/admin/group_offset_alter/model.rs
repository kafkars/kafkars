//! Validated semantic input for one consumer-group offset alteration.

use core::fmt;
use std::collections::BTreeSet;

const MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;
const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;
const MAX_METADATA_BYTES: usize = i16::MAX as usize;

/// One caller-ordered next offset and its optional Kafka facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetTarget {
    topic: String,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

impl AlterConsumerGroupOffsetTarget {
    /// Creates one target for validation by the enclosing request plan.
    pub const fn new(
        topic: String,
        partition: i32,
        next_offset: i64,
        leader_epoch: Option<i32>,
        metadata: Option<String>,
    ) -> Self {
        Self {
            topic,
            partition,
            next_offset,
            leader_epoch,
            metadata,
        }
    }

    /// Returns the exact UTF-8 topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the nonnegative next offset committed to Kafka.
    pub const fn next_offset(&self) -> i64 {
        self.next_offset
    }

    /// Returns the optional nonnegative leader epoch.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Returns nullable metadata without collapsing present empty text.
    pub fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }
}

/// Validated intent for one destructive consumer-group offset alteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetsPlan {
    group_id: String,
    targets: Vec<AlterConsumerGroupOffsetTarget>,
    retention_time_ms: Option<i64>,
}

impl AlterConsumerGroupOffsetsPlan {
    /// Validates one group and a nonempty caller-ordered unique target set.
    pub fn new(
        group_id: String,
        targets: Vec<AlterConsumerGroupOffsetTarget>,
    ) -> Result<Self, AlterConsumerGroupOffsetsPlanError> {
        validate_group_id(&group_id)?;
        if targets.is_empty() {
            return Err(AlterConsumerGroupOffsetsPlanError::EmptyTargetBatch);
        }
        let mut identities = BTreeSet::new();
        for target in &targets {
            validate_target(target)?;
            if !identities.insert((target.topic.as_str(), target.partition)) {
                return Err(AlterConsumerGroupOffsetsPlanError::DuplicateTopicPartition);
            }
        }
        Ok(Self {
            group_id,
            targets,
            retention_time_ms: None,
        })
    }

    /// Selects an explicit nonnegative Kafka retention duration in milliseconds.
    pub fn with_retention_time_ms(
        mut self,
        retention_time_ms: i64,
    ) -> Result<Self, AlterConsumerGroupOffsetsPlanError> {
        if retention_time_ms < 0 {
            return Err(AlterConsumerGroupOffsetsPlanError::NegativeRetentionTime);
        }
        if self.requires_leader_epoch() {
            return Err(AlterConsumerGroupOffsetsPlanError::RetentionTimeWithLeaderEpoch);
        }
        self.retention_time_ms = Some(retention_time_ms);
        Ok(self)
    }

    /// Returns the exact consumer-group coordinator key.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns alterations in original caller order.
    pub fn targets(&self) -> &[AlterConsumerGroupOffsetTarget] {
        &self.targets
    }

    /// Returns explicit retention milliseconds or omission for Kafka's `-1` sentinel.
    pub const fn retention_time_ms(&self) -> Option<i64> {
        self.retention_time_ms
    }

    /// Reports whether the request requires `OffsetCommit` v6 or newer.
    pub fn requires_leader_epoch(&self) -> bool {
        self.targets
            .iter()
            .any(|target| target.leader_epoch.is_some())
    }
}

fn validate_group_id(group_id: &str) -> Result<(), AlterConsumerGroupOffsetsPlanError> {
    if group_id.is_empty() {
        return Err(AlterConsumerGroupOffsetsPlanError::EmptyGroupId);
    }
    if group_id.len() > MAX_GROUP_ID_BYTES {
        return Err(AlterConsumerGroupOffsetsPlanError::GroupIdTooLong);
    }
    Ok(())
}

fn validate_target(
    target: &AlterConsumerGroupOffsetTarget,
) -> Result<(), AlterConsumerGroupOffsetsPlanError> {
    if target.topic.is_empty() {
        return Err(AlterConsumerGroupOffsetsPlanError::EmptyTopicName);
    }
    if target.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(AlterConsumerGroupOffsetsPlanError::TopicNameTooLong);
    }
    if target.partition < 0 {
        return Err(AlterConsumerGroupOffsetsPlanError::NegativePartition);
    }
    if target.next_offset < 0 {
        return Err(AlterConsumerGroupOffsetsPlanError::NegativeNextOffset);
    }
    if target.leader_epoch.is_some_and(|epoch| epoch < 0) {
        return Err(AlterConsumerGroupOffsetsPlanError::NegativeLeaderEpoch);
    }
    if target
        .metadata
        .as_ref()
        .is_some_and(|metadata| metadata.len() > MAX_METADATA_BYTES)
    {
        return Err(AlterConsumerGroupOffsetsPlanError::MetadataTooLong);
    }
    Ok(())
}

/// Invalid deterministic consumer-group offset alteration intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterConsumerGroupOffsetsPlanError {
    /// The request must name one explicit consumer group.
    EmptyGroupId,
    /// The group identity cannot fit Kafka's coordinator key domain.
    GroupIdTooLong,
    /// Kafka cannot alter an empty topic-partition batch.
    EmptyTargetBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// A topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// A committed next offset must be nonnegative.
    NegativeNextOffset,
    /// A present leader epoch must be nonnegative.
    NegativeLeaderEpoch,
    /// Present metadata cannot fit Kafka's nullable-string domain.
    MetadataTooLong,
    /// One request cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
    /// Kafka retention milliseconds must be nonnegative when explicitly selected.
    NegativeRetentionTime,
    /// Kafka versions carrying retention do not also carry leader epochs.
    RetentionTimeWithLeaderEpoch,
    /// The requested retention duration exceeds Kafka's signed-millisecond domain.
    RetentionTimeTooLarge,
}

impl fmt::Display for AlterConsumerGroupOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyGroupId => "consumer group id is empty",
            Self::GroupIdTooLong => "consumer group id exceeds the coordinator key limit",
            Self::EmptyTargetBatch => "consumer group offset alteration batch is empty",
            Self::EmptyTopicName => "consumer group offset alteration topic is empty",
            Self::TopicNameTooLong => "consumer group offset alteration topic is too long",
            Self::NegativePartition => "consumer group offset alteration partition is negative",
            Self::NegativeNextOffset => "consumer group next offset is negative",
            Self::NegativeLeaderEpoch => "consumer group offset leader epoch is negative",
            Self::MetadataTooLong => "consumer group offset metadata is too long",
            Self::DuplicateTopicPartition => {
                "consumer group offset alteration contains a duplicate topic-partition"
            }
            Self::NegativeRetentionTime => {
                "consumer group offset alteration retention time is negative"
            }
            Self::RetentionTimeWithLeaderEpoch => {
                "consumer group offset alteration retention time cannot be combined with a leader epoch"
            }
            Self::RetentionTimeTooLarge => {
                "consumer group offset alteration retention time exceeds Kafka's millisecond domain"
            }
        })
    }
}

impl std::error::Error for AlterConsumerGroupOffsetsPlanError {}
