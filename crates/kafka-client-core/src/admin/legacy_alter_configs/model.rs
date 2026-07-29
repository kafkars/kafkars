//! Validated semantic input for explicit legacy configuration-resource replacement.

use core::fmt;
use std::collections::BTreeSet;

/// One exact key/value entry in a topic's replacement snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyConfigEntry {
    key: String,
    value: Option<String>,
}

impl LegacyConfigEntry {
    /// Creates one entry, preserving Kafka's nullable value representation.
    pub const fn new(key: String, value: Option<String>) -> Self {
        Self { key, value }
    }

    /// Returns the nonempty configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the exact nullable replacement value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Consumes the entry into protocol-adapter-owned parts.
    pub fn into_parts(self) -> (String, Option<String>) {
        (self.key, self.value)
    }
}

/// One Kafka configuration resource and its complete caller-ordered snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyTopicConfigReplacement {
    resource_type: i8,
    resource_name: String,
    configs: Vec<LegacyConfigEntry>,
}

impl LegacyTopicConfigReplacement {
    /// Creates one topic replacement for validation by the enclosing plan.
    ///
    /// An empty entry list is meaningful: it asks Kafka to reset every dynamic
    /// topic configuration represented by the legacy full-snapshot operation.
    pub const fn new(topic: String, configs: Vec<LegacyConfigEntry>) -> Self {
        Self {
            resource_type: 2,
            resource_name: topic,
            configs,
        }
    }

    /// Creates one exact Kafka resource replacement for plan-time validation.
    ///
    /// Empty entry lists remain meaningful full snapshots for every resource.
    pub const fn resource(
        resource_type: i8,
        resource_name: String,
        configs: Vec<LegacyConfigEntry>,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            configs,
        }
    }

    /// Returns Kafka's exact resource-type code.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns the topic name.
    ///
    /// This compatibility accessor is intended for values made with
    /// [`Self::new`].
    pub fn topic(&self) -> &str {
        &self.resource_name
    }

    /// Returns the complete replacement entries in caller order.
    pub fn configs(&self) -> &[LegacyConfigEntry] {
        &self.configs
    }
}

/// Resource-generic name for one exact type/name full-snapshot replacement.
pub type LegacyConfigResourceReplacement = LegacyTopicConfigReplacement;

/// Ordered input for one explicitly destructive API 33 operation.
///
/// Each resource's list is a complete dynamic-configuration snapshot. Kafka
/// implicitly resets omitted keys; this plan is never an incremental fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyAlterConfigsPlan {
    resources: Vec<LegacyConfigResourceReplacement>,
    validate_only: bool,
}

impl LegacyAlterConfigsPlan {
    /// Validates a nonempty unique-topic batch and unambiguous snapshots.
    pub fn new(
        topics: Vec<LegacyTopicConfigReplacement>,
        validate_only: bool,
    ) -> Result<Self, LegacyAlterConfigsPlanError> {
        validate_resources(&topics, true)?;
        Ok(Self {
            resources: topics,
            validate_only,
        })
    }

    /// Validates exact positive resource identities and unambiguous snapshots.
    pub fn for_resources(
        resources: Vec<LegacyConfigResourceReplacement>,
        validate_only: bool,
    ) -> Result<Self, LegacyAlterConfigsPlanError> {
        validate_resources(&resources, false)?;
        Ok(Self {
            resources,
            validate_only,
        })
    }

    /// Returns resources in original caller order.
    pub fn resources(&self) -> &[LegacyConfigResourceReplacement] {
        &self.resources
    }

    /// Returns topic snapshots in original caller order.
    ///
    /// Existing topic-only callers continue to receive the same values.
    pub fn topics(&self) -> &[LegacyTopicConfigReplacement] {
        &self.resources
    }

    /// Returns whether Kafka should validate without mutating.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }
}

fn validate_resources(
    resources: &[LegacyConfigResourceReplacement],
    topic_compatibility_errors: bool,
) -> Result<(), LegacyAlterConfigsPlanError> {
    if resources.is_empty() {
        return Err(LegacyAlterConfigsPlanError::EmptyBatch);
    }
    let mut identities = BTreeSet::new();
    for resource in resources {
        if resource.resource_type <= 0 {
            return Err(LegacyAlterConfigsPlanError::NonPositiveResourceType);
        }
        if resource.resource_name.is_empty() {
            if topic_compatibility_errors {
                return Err(LegacyAlterConfigsPlanError::EmptyTopicName);
            }
            return Err(LegacyAlterConfigsPlanError::EmptyResourceName);
        }
        if !identities.insert((resource.resource_type, resource.resource_name.as_str())) {
            if topic_compatibility_errors {
                return Err(LegacyAlterConfigsPlanError::DuplicateTopic);
            }
            return Err(LegacyAlterConfigsPlanError::DuplicateResource);
        }
        validate_configs(&resource.configs)?;
    }
    Ok(())
}

fn validate_configs(configs: &[LegacyConfigEntry]) -> Result<(), LegacyAlterConfigsPlanError> {
    let mut keys = BTreeSet::new();
    for config in configs {
        if config.key.is_empty() {
            return Err(LegacyAlterConfigsPlanError::EmptyConfigurationKey);
        }
        if !keys.insert(config.key.as_str()) {
            return Err(LegacyAlterConfigsPlanError::DuplicateConfigurationKey);
        }
    }
    Ok(())
}

/// Invalid deterministic legacy replacement input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyAlterConfigsPlanError {
    /// Kafka cannot execute an empty resource batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// One topic may occur only once in a batch.
    DuplicateTopic,
    /// Kafka configuration-resource type codes must be positive.
    NonPositiveResourceType,
    /// Generic Kafka resource names must not be empty.
    EmptyResourceName,
    /// One exact resource type and name may occur only once in a batch.
    DuplicateResource,
    /// Configuration keys must not be empty.
    EmptyConfigurationKey,
    /// One key may occur only once in a topic snapshot.
    DuplicateConfigurationKey,
}

impl fmt::Display for LegacyAlterConfigsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid LegacyAlterConfigs plan: {self:?}")
    }
}

impl std::error::Error for LegacyAlterConfigsPlanError {}
