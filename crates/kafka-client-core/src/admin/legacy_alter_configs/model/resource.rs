//! Exact legacy configuration resource values and plan-time validation.

use std::collections::BTreeSet;

use super::{LegacyAlterConfigsPlanError, LegacyAlterConfigsRoute};

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

    pub(crate) fn route(&self) -> LegacyAlterConfigsRoute {
        if matches!(self.resource_type, 4 | 8) {
            return LegacyAlterConfigsRoute::ExactBroker(
                canonical_broker_id(&self.resource_name)
                    .expect("validated broker resource name must remain canonical"),
            );
        }
        LegacyAlterConfigsRoute::AnyBroker
    }
}

/// Resource-generic name for one exact type/name full-snapshot replacement.
pub type LegacyConfigResourceReplacement = LegacyTopicConfigReplacement;

pub(super) fn validate_resources(
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
        if matches!(resource.resource_type, 4 | 8)
            && canonical_broker_id(&resource.resource_name).is_none()
        {
            return Err(LegacyAlterConfigsPlanError::InvalidBrokerResourceName);
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

fn canonical_broker_id(resource_name: &str) -> Option<i32> {
    let bytes = resource_name.as_bytes();
    if bytes == b"0" {
        return Some(0);
    }
    if !matches!(bytes.first(), Some(b'1'..=b'9'))
        || bytes.get(1..)?.iter().any(|byte| !byte.is_ascii_digit())
    {
        return None;
    }
    resource_name.parse().ok()
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
