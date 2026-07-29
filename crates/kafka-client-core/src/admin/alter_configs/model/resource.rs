//! Exact configuration-resource identity and caller-ordered alteration values.

use std::collections::BTreeSet;

use super::IncrementalAlterConfigsPlanError;

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

/// One Kafka configuration resource and its caller-ordered alterations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigAlteration {
    resource_type: i8,
    resource_name: String,
    alterations: Vec<ConfigAlteration>,
}

impl TopicConfigAlteration {
    /// Creates one topic alteration for validation by the enclosing plan.
    ///
    /// This compatibility constructor is retained for the original
    /// topic-scoped engine and facade paths.
    pub const fn new(topic: String, alterations: Vec<ConfigAlteration>) -> Self {
        Self {
            resource_type: 2,
            resource_name: topic,
            alterations,
        }
    }

    /// Creates one exact Kafka resource alteration for plan-time validation.
    pub const fn resource(
        resource_type: i8,
        resource_name: String,
        alterations: Vec<ConfigAlteration>,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            alterations,
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

    /// Returns alterations in caller order.
    pub fn alterations(&self) -> &[ConfigAlteration] {
        &self.alterations
    }
}

/// Resource-generic name for the exact type/name alteration value.
pub type IncrementalConfigResourceAlteration = TopicConfigAlteration;

pub(super) fn validate_resources(
    resources: &[IncrementalConfigResourceAlteration],
    topic_compatibility_errors: bool,
) -> Result<(), IncrementalAlterConfigsPlanError> {
    if resources.is_empty() {
        return Err(IncrementalAlterConfigsPlanError::EmptyBatch);
    }
    let mut identities = BTreeSet::new();
    for resource in resources {
        if resource.resource_type <= 0 {
            return Err(IncrementalAlterConfigsPlanError::NonPositiveResourceType);
        }
        if resource.resource_name.is_empty() {
            return Err(if topic_compatibility_errors {
                IncrementalAlterConfigsPlanError::EmptyTopicName
            } else {
                IncrementalAlterConfigsPlanError::EmptyResourceName
            });
        }
        if !identities.insert((resource.resource_type, resource.resource_name.as_str())) {
            return Err(if topic_compatibility_errors {
                IncrementalAlterConfigsPlanError::DuplicateTopic
            } else {
                IncrementalAlterConfigsPlanError::DuplicateResource
            });
        }
        validate_alterations(&resource.alterations)?;
    }
    Ok(())
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
