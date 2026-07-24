//! Validated semantic input for ordered topic configuration alterations.

use core::fmt;
use std::collections::BTreeSet;

/// One configuration change whose value contract is encoded by its variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigAlterationOperation {
    /// Replaces the configuration with the supplied value.
    Set(String),
    /// Removes the explicit configuration value.
    Delete,
    /// Appends the supplied value using Kafka's configuration semantics.
    Append(String),
    /// Subtracts the supplied value using Kafka's configuration semantics.
    Subtract(String),
}

impl ConfigAlterationOperation {
    /// Returns the exact value, with absence reserved exclusively for deletion.
    pub fn value(&self) -> Option<&str> {
        match self {
            Self::Set(value) | Self::Append(value) | Self::Subtract(value) => Some(value),
            Self::Delete => None,
        }
    }
}

/// One named configuration alteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigAlteration {
    key: String,
    operation: ConfigAlterationOperation,
}

impl ConfigAlteration {
    /// Creates one value replacement.
    pub const fn set(key: String, value: String) -> Self {
        Self {
            key,
            operation: ConfigAlterationOperation::Set(value),
        }
    }

    /// Creates one explicit-value deletion.
    pub const fn delete(key: String) -> Self {
        Self {
            key,
            operation: ConfigAlterationOperation::Delete,
        }
    }

    /// Creates one append operation.
    pub const fn append(key: String, value: String) -> Self {
        Self {
            key,
            operation: ConfigAlterationOperation::Append(value),
        }
    }

    /// Creates one subtract operation.
    pub const fn subtract(key: String, value: String) -> Self {
        Self {
            key,
            operation: ConfigAlterationOperation::Subtract(value),
        }
    }

    /// Returns the configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the exact semantic operation.
    pub const fn operation(&self) -> &ConfigAlterationOperation {
        &self.operation
    }

    /// Consumes the alteration into adapter-owned semantic parts.
    pub fn into_parts(self) -> (String, ConfigAlterationOperation) {
        (self.key, self.operation)
    }
}

/// One topic and its caller-ordered configuration alterations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigAlteration {
    topic: String,
    alterations: Vec<ConfigAlteration>,
}

impl TopicConfigAlteration {
    /// Creates one topic alteration for validation by the enclosing plan.
    pub const fn new(topic: String, alterations: Vec<ConfigAlteration>) -> Self {
        Self { topic, alterations }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns alterations in caller order.
    pub fn alterations(&self) -> &[ConfigAlteration] {
        &self.alterations
    }
}

/// Ordered, validated policy input for one topic-only incremental update RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsPlan {
    topics: Vec<TopicConfigAlteration>,
    validate_only: bool,
}

impl IncrementalAlterConfigsPlan {
    /// Validates nonempty topics and unambiguous alteration identities.
    pub fn new(
        topics: Vec<TopicConfigAlteration>,
        validate_only: bool,
    ) -> Result<Self, IncrementalAlterConfigsPlanError> {
        if topics.is_empty() {
            return Err(IncrementalAlterConfigsPlanError::EmptyBatch);
        }
        let mut topic_names = BTreeSet::new();
        for topic in &topics {
            if topic.topic.is_empty() {
                return Err(IncrementalAlterConfigsPlanError::EmptyTopicName);
            }
            if !topic_names.insert(topic.topic.as_str()) {
                return Err(IncrementalAlterConfigsPlanError::DuplicateTopic);
            }
            validate_alterations(&topic.alterations)?;
        }
        Ok(Self {
            topics,
            validate_only,
        })
    }

    /// Returns topics in original caller order.
    pub fn topics(&self) -> &[TopicConfigAlteration] {
        &self.topics
    }

    /// Returns whether Kafka should validate without mutating.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }
}

fn validate_alterations(
    alterations: &[ConfigAlteration],
) -> Result<(), IncrementalAlterConfigsPlanError> {
    if alterations.is_empty() {
        return Err(IncrementalAlterConfigsPlanError::EmptyAlterations);
    }
    let mut keys = BTreeSet::new();
    for alteration in alterations {
        if alteration.key.is_empty() {
            return Err(IncrementalAlterConfigsPlanError::EmptyConfigurationKey);
        }
        if !keys.insert(alteration.key.as_str()) {
            return Err(IncrementalAlterConfigsPlanError::DuplicateConfigurationKey);
        }
    }
    Ok(())
}

/// Invalid deterministic topic configuration input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAlterConfigsPlanError {
    /// Kafka cannot execute an empty resource batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// One topic may occur only once in a batch.
    DuplicateTopic,
    /// Every topic must contain at least one alteration.
    EmptyAlterations,
    /// Configuration keys must not be empty.
    EmptyConfigurationKey,
    /// One key may occur only once for a topic.
    DuplicateConfigurationKey,
}

impl fmt::Display for IncrementalAlterConfigsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid IncrementalAlterConfigs plan: {self:?}")
    }
}

impl std::error::Error for IncrementalAlterConfigsPlanError {}
