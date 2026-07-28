//! Validated caller-ordered intent for one Admin `DeleteRecords` operation.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TOPIC_NAME_BYTES: usize = 249;
const HIGH_WATERMARK_OFFSET: i64 = -1;

/// One topic-partition and the first offset that must remain after deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTarget {
    topic: String,
    partition: i32,
    before_offset: i64,
}

impl DeleteRecordsTarget {
    /// Creates one target for validation by the enclosing request plan.
    pub const fn new(topic: String, partition: i32, before_offset: i64) -> Self {
        Self {
            topic,
            partition,
            before_offset,
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

    /// Returns the requested offset; `-1` means the current high watermark.
    pub const fn before_offset(&self) -> i64 {
        self.before_offset
    }
}

/// Validated intent for one bounded Admin `DeleteRecords` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPlan {
    targets: Vec<DeleteRecordsTarget>,
}

impl DeleteRecordsPlan {
    /// Validates one nonempty caller-ordered set of unique topic-partitions.
    pub fn new(targets: Vec<DeleteRecordsTarget>) -> Result<Self, DeleteRecordsPlanError> {
        if targets.is_empty() {
            return Err(DeleteRecordsPlanError::EmptyTargetBatch);
        }
        let mut identities = BTreeSet::new();
        for target in &targets {
            validate_target(target)?;
            if !identities.insert((target.topic.as_str(), target.partition)) {
                return Err(DeleteRecordsPlanError::DuplicateTopicPartition);
            }
        }
        Ok(Self { targets })
    }

    /// Returns targets in exact caller order.
    pub fn targets(&self) -> &[DeleteRecordsTarget] {
        &self.targets
    }
}

fn validate_target(target: &DeleteRecordsTarget) -> Result<(), DeleteRecordsPlanError> {
    if target.topic.is_empty() {
        return Err(DeleteRecordsPlanError::EmptyTopicName);
    }
    if target.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(DeleteRecordsPlanError::TopicNameTooLong);
    }
    if target.partition < 0 {
        return Err(DeleteRecordsPlanError::NegativePartition);
    }
    if target.before_offset < HIGH_WATERMARK_OFFSET {
        return Err(DeleteRecordsPlanError::InvalidOffset);
    }
    Ok(())
}

/// Invalid deterministic Admin `DeleteRecords` intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteRecordsPlanError {
    /// An operation must contain at least one topic-partition.
    EmptyTargetBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// A topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// Only `-1` or a nonnegative deletion offset is representable.
    InvalidOffset,
    /// One operation cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
}

impl fmt::Display for DeleteRecordsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyTargetBatch => "Admin DeleteRecords target batch is empty",
            Self::EmptyTopicName => "Admin DeleteRecords topic is empty",
            Self::TopicNameTooLong => "Admin DeleteRecords topic is too long",
            Self::NegativePartition => "Admin DeleteRecords partition is negative",
            Self::InvalidOffset => "Admin DeleteRecords offset is below -1",
            Self::DuplicateTopicPartition => {
                "Admin DeleteRecords contains a duplicate topic-partition"
            }
        })
    }
}

impl std::error::Error for DeleteRecordsPlanError {}
