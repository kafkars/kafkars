//! Validated semantic input for one batched `CreateTopics` operation.

use core::fmt;
use std::collections::BTreeSet;

/// One nullable topic configuration entry, retained in caller order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicConfig {
    name: String,
    value: Option<String>,
}

impl CreateTopicConfig {
    /// Creates one semantic topic configuration entry.
    pub fn new(name: impl Into<String>, value: Option<String>) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    /// Returns the configuration name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the optional configuration value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }
}

/// One topic specification in a batched `CreateTopics` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicSpecification {
    name: String,
    partitions: i32,
    replication_factor: i16,
    configs: Vec<CreateTopicConfig>,
}

impl CreateTopicSpecification {
    /// Creates one topic specification with automatic replica assignment.
    pub fn new(
        name: impl Into<String>,
        partitions: i32,
        replication_factor: i16,
        configs: Vec<CreateTopicConfig>,
    ) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor,
            configs,
        }
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the requested partition count.
    pub const fn partitions(&self) -> i32 {
        self.partitions
    }

    /// Returns the replication factor, or `-1` for the broker default.
    pub const fn replication_factor(&self) -> i16 {
        self.replication_factor
    }

    /// Returns configuration entries in caller order.
    pub fn configs(&self) -> &[CreateTopicConfig] {
        &self.configs
    }
}

/// Ordered, validated policy input for one `CreateTopics` RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsPlan {
    topics: Vec<CreateTopicSpecification>,
    validate_only: bool,
}

impl CreateTopicsPlan {
    /// Validates a nonempty, uniquely named `CreateTopics` batch.
    pub fn new(
        topics: Vec<CreateTopicSpecification>,
        validate_only: bool,
    ) -> Result<Self, CreateTopicsPlanError> {
        if topics.is_empty() {
            return Err(CreateTopicsPlanError::EmptyBatch);
        }
        let mut names = BTreeSet::new();
        for topic in &topics {
            validate_topic(topic)?;
            if !names.insert(topic.name()) {
                return Err(CreateTopicsPlanError::DuplicateTopic);
            }
        }
        Ok(Self {
            topics,
            validate_only,
        })
    }

    /// Returns topic specifications in caller order.
    pub fn topics(&self) -> &[CreateTopicSpecification] {
        &self.topics
    }

    /// Returns whether Kafka should validate without creating topics.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }
}

fn validate_topic(topic: &CreateTopicSpecification) -> Result<(), CreateTopicsPlanError> {
    if topic.name.is_empty() {
        return Err(CreateTopicsPlanError::EmptyTopicName);
    }
    if topic.partitions <= 0 {
        return Err(CreateTopicsPlanError::InvalidPartitionCount);
    }
    if topic.replication_factor != -1 && topic.replication_factor <= 0 {
        return Err(CreateTopicsPlanError::InvalidReplicationFactor);
    }
    if topic.configs.iter().any(|config| config.name.is_empty()) {
        return Err(CreateTopicsPlanError::EmptyConfigName);
    }
    Ok(())
}

/// Invalid deterministic `CreateTopics` input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTopicsPlanError {
    /// Kafka cannot execute an empty `CreateTopics` batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// Topic names in one batch must be unique.
    DuplicateTopic,
    /// Automatic topic creation requires a positive partition count.
    InvalidPartitionCount,
    /// Replication factor must be positive or the broker-default sentinel.
    InvalidReplicationFactor,
    /// Topic configuration names must not be empty.
    EmptyConfigName,
}

impl fmt::Display for CreateTopicsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "CreateTopics batch is empty",
            Self::EmptyTopicName => "CreateTopics topic name is empty",
            Self::DuplicateTopic => "CreateTopics batch contains a duplicate topic",
            Self::InvalidPartitionCount => "CreateTopics partition count is not positive",
            Self::InvalidReplicationFactor => "CreateTopics replication factor is invalid",
            Self::EmptyConfigName => "CreateTopics configuration name is empty",
        })
    }
}

impl std::error::Error for CreateTopicsPlanError {}
