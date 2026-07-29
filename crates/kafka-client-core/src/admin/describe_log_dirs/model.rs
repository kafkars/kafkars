//! Validated caller-ordered broker and partition selection for `DescribeLogDirs`.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum UTF-8 bytes accepted for one selected topic name.
pub const ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPIC_BYTES: usize = 249;
/// Maximum distinct selected topics accepted by one operation.
pub const ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPICS: usize = 16 * 1_024;
/// Maximum selected topic-partitions accepted by one operation.
pub const ADMIN_DESCRIBE_LOG_DIRS_MAX_PARTITIONS: usize = 1_024 * 1_024;

/// One caller-ordered topic-partition selected on every queried broker.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AdminDescribeLogDirsPartition {
    topic: String,
    partition: i32,
}

impl AdminDescribeLogDirsPartition {
    /// Creates inert scalar identity validated with its enclosing plan.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the selected topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the selected partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Consumes the identity into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, i32) {
        (self.topic, self.partition)
    }
}

/// Kafka's nullable topic selection without conflating empty with all topics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsSelection {
    /// Query every topic visible on each selected broker.
    AllTopics,
    /// Query one validated nonempty caller-ordered topic-partition batch.
    Selected(Vec<AdminDescribeLogDirsPartition>),
}

impl AdminDescribeLogDirsSelection {
    /// Returns the explicit caller-ordered selection, or `None` for all topics.
    pub fn selected_partitions(&self) -> Option<&[AdminDescribeLogDirsPartition]> {
        match self {
            Self::AllTopics => None,
            Self::Selected(partitions) => Some(partitions),
        }
    }
}

/// Validated intent for one bounded broker log-directory query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeLogDirsPlan {
    broker_ids: Vec<i32>,
    selection: AdminDescribeLogDirsSelection,
}

impl AdminDescribeLogDirsPlan {
    /// Validates unique brokers and selects all topics on every broker.
    pub fn new(broker_ids: Vec<i32>) -> Result<Self, AdminDescribeLogDirsPlanError> {
        validate_brokers(&broker_ids)?;
        Ok(Self {
            broker_ids,
            selection: AdminDescribeLogDirsSelection::AllTopics,
        })
    }

    /// Validates unique brokers and one explicit nonempty partition selection.
    pub fn selected(
        broker_ids: Vec<i32>,
        partitions: Vec<AdminDescribeLogDirsPartition>,
    ) -> Result<Self, AdminDescribeLogDirsPlanError> {
        validate_brokers(&broker_ids)?;
        validate_partitions(&partitions)?;
        Ok(Self {
            broker_ids,
            selection: AdminDescribeLogDirsSelection::Selected(partitions),
        })
    }

    /// Returns broker IDs in exact caller order.
    pub fn broker_ids(&self) -> &[i32] {
        &self.broker_ids
    }

    /// Returns the validated all-topic or explicit partition selection.
    pub const fn selection(&self) -> &AdminDescribeLogDirsSelection {
        &self.selection
    }
}

/// Invalid deterministic `DescribeLogDirs` intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsPlanError {
    /// At least one broker must be requested.
    EmptyBrokerBatch,
    /// Broker IDs must be nonnegative.
    NegativeBrokerId,
    /// One operation cannot repeat a broker ID.
    DuplicateBrokerId,
    /// An explicit selection must contain at least one topic-partition.
    EmptySelection,
    /// An explicit selection exceeded the bounded distinct-topic count.
    TooManyTopics,
    /// An explicit selection exceeded the bounded partition count.
    TooManyPartitions,
    /// Topic names cannot be empty.
    EmptyTopic,
    /// A topic exceeded the name-based API-key 35 request bound.
    TopicTooLong,
    /// Partition indexes must be nonnegative.
    NegativePartition,
    /// One explicit selection cannot repeat a topic-partition identity.
    DuplicatePartition,
}

impl fmt::Display for AdminDescribeLogDirsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBrokerBatch => "DescribeLogDirs broker batch is empty",
            Self::NegativeBrokerId => "DescribeLogDirs broker ID is negative",
            Self::DuplicateBrokerId => "DescribeLogDirs contains a duplicate broker ID",
            Self::EmptySelection => "DescribeLogDirs explicit partition selection is empty",
            Self::TooManyTopics => "DescribeLogDirs selected too many topics",
            Self::TooManyPartitions => "DescribeLogDirs selected too many partitions",
            Self::EmptyTopic => "DescribeLogDirs selected an empty topic name",
            Self::TopicTooLong => "DescribeLogDirs selected an oversized topic name",
            Self::NegativePartition => "DescribeLogDirs selected a negative partition",
            Self::DuplicatePartition => "DescribeLogDirs contains a duplicate topic-partition",
        })
    }
}

impl std::error::Error for AdminDescribeLogDirsPlanError {}

fn validate_brokers(broker_ids: &[i32]) -> Result<(), AdminDescribeLogDirsPlanError> {
    if broker_ids.is_empty() {
        return Err(AdminDescribeLogDirsPlanError::EmptyBrokerBatch);
    }
    let mut identities = BTreeSet::new();
    for broker_id in broker_ids {
        if *broker_id < 0 {
            return Err(AdminDescribeLogDirsPlanError::NegativeBrokerId);
        }
        if !identities.insert(*broker_id) {
            return Err(AdminDescribeLogDirsPlanError::DuplicateBrokerId);
        }
    }
    Ok(())
}

fn validate_partitions(
    partitions: &[AdminDescribeLogDirsPartition],
) -> Result<(), AdminDescribeLogDirsPlanError> {
    if partitions.is_empty() {
        return Err(AdminDescribeLogDirsPlanError::EmptySelection);
    }
    if partitions.len() > ADMIN_DESCRIBE_LOG_DIRS_MAX_PARTITIONS {
        return Err(AdminDescribeLogDirsPlanError::TooManyPartitions);
    }
    let mut topics = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for selected in partitions {
        if selected.topic.is_empty() {
            return Err(AdminDescribeLogDirsPlanError::EmptyTopic);
        }
        if selected.topic.len() > ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPIC_BYTES {
            return Err(AdminDescribeLogDirsPlanError::TopicTooLong);
        }
        if selected.partition < 0 {
            return Err(AdminDescribeLogDirsPlanError::NegativePartition);
        }
        topics.insert(selected.topic.as_str());
        if topics.len() > ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPICS {
            return Err(AdminDescribeLogDirsPlanError::TooManyTopics);
        }
        if !identities.insert((selected.topic.as_str(), selected.partition)) {
            return Err(AdminDescribeLogDirsPlanError::DuplicatePartition);
        }
    }
    Ok(())
}
