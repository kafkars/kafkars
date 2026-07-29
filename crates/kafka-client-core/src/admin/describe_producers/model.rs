//! Bounded caller-ordered intent for one Admin `DescribeProducers` operation.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TOPIC_NAME_BYTES: usize = 249;

/// Maximum topic-partitions retained by one producer-description operation.
pub const DESCRIBE_PRODUCERS_MAX_TARGETS: usize = 4 * 1024;
/// Maximum aggregate topic-name bytes retained by one request plan.
pub const DESCRIBE_PRODUCERS_MAX_TARGET_TOPIC_BYTES: usize = 256 * 1024;

/// One caller-selected topic-partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducerTarget {
    topic: String,
    partition: i32,
}

impl AdminDescribeProducerTarget {
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

    /// Consumes the target into stable scalar parts.
    pub fn into_parts(self) -> (String, i32) {
        (self.topic, self.partition)
    }
}

/// Validated intent for one bounded active-producer query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducersPlan {
    targets: Vec<AdminDescribeProducerTarget>,
    broker_id: Option<i32>,
}

impl AdminDescribeProducersPlan {
    /// Validates targets and an optional exact nonnegative broker identity.
    pub fn new(
        targets: Vec<AdminDescribeProducerTarget>,
        broker_id: Option<i32>,
    ) -> Result<Self, AdminDescribeProducersPlanError> {
        if broker_id.is_some_and(|broker_id| broker_id < 0) {
            return Err(AdminDescribeProducersPlanError::NegativeBrokerId);
        }
        if targets.is_empty() {
            return Err(AdminDescribeProducersPlanError::EmptyTargetBatch);
        }
        if targets.len() > DESCRIBE_PRODUCERS_MAX_TARGETS {
            return Err(AdminDescribeProducersPlanError::TooManyTargets);
        }
        let mut identities = BTreeSet::new();
        let mut topic_bytes = 0usize;
        for target in &targets {
            validate_target(target)?;
            topic_bytes = topic_bytes
                .checked_add(target.topic.len())
                .ok_or(AdminDescribeProducersPlanError::TargetTopicBytesExceeded)?;
            if topic_bytes > DESCRIBE_PRODUCERS_MAX_TARGET_TOPIC_BYTES {
                return Err(AdminDescribeProducersPlanError::TargetTopicBytesExceeded);
            }
            if !identities.insert((target.topic.as_str(), target.partition)) {
                return Err(AdminDescribeProducersPlanError::DuplicateTopicPartition);
            }
        }
        Ok(Self { targets, broker_id })
    }

    /// Returns targets in exact caller order.
    pub fn targets(&self) -> &[AdminDescribeProducerTarget] {
        &self.targets
    }

    /// Returns the exact caller-selected broker, when present.
    pub const fn broker_id(&self) -> Option<i32> {
        self.broker_id
    }
}

fn validate_target(
    target: &AdminDescribeProducerTarget,
) -> Result<(), AdminDescribeProducersPlanError> {
    if target.topic.is_empty() {
        return Err(AdminDescribeProducersPlanError::EmptyTopicName);
    }
    if target.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(AdminDescribeProducersPlanError::TopicNameTooLong);
    }
    if target.partition < 0 {
        return Err(AdminDescribeProducersPlanError::NegativePartition);
    }
    Ok(())
}

/// Invalid deterministic active-producer query intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersPlanError {
    /// Exact broker identities must be nonnegative.
    NegativeBrokerId,
    /// At least one topic-partition must be requested.
    EmptyTargetBatch,
    /// One operation cannot retain more than 4,096 targets.
    TooManyTargets,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// Topic names must fit Kafka's topic-name domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// One operation cannot repeat a topic-partition.
    DuplicateTopicPartition,
    /// Aggregate target topic bytes exceeded the deterministic request bound.
    TargetTopicBytesExceeded,
}

impl fmt::Display for AdminDescribeProducersPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeProducers plan: {self:?}")
    }
}

impl std::error::Error for AdminDescribeProducersPlanError {}
