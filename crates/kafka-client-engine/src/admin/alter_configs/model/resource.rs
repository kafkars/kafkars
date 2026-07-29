//! Canonical engine-owned incremental configuration resource values.

use kafka_client_core::{
    ConfigAlteration as CoreAlteration,
    IncrementalConfigResourceAlteration as CoreResourceAlteration,
};

use super::{canonical_string, canonical_vec};

/// One exact incremental configuration operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalConfigOperation {
    /// Replaces the current value.
    Set(String),
    /// Removes the explicit value.
    Delete,
    /// Appends using Kafka's configuration semantics.
    Append(String),
    /// Subtracts using Kafka's configuration semantics.
    Subtract(String),
}

impl IncrementalConfigOperation {
    fn canonicalize(self) -> Self {
        match self {
            Self::Set(value) => Self::Set(canonical_string(value)),
            Self::Delete => Self::Delete,
            Self::Append(value) => Self::Append(canonical_string(value)),
            Self::Subtract(value) => Self::Subtract(canonical_string(value)),
        }
    }

    fn value_bytes(&self) -> usize {
        match self {
            Self::Set(value) | Self::Append(value) | Self::Subtract(value) => value.len(),
            Self::Delete => 0,
        }
    }

    fn into_core(self, key: String) -> CoreAlteration {
        match self {
            Self::Set(value) => CoreAlteration::set(key, value),
            Self::Delete => CoreAlteration::delete(key),
            Self::Append(value) => CoreAlteration::append(key, value),
            Self::Subtract(value) => CoreAlteration::subtract(key, value),
        }
    }
}

/// One named configuration change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalConfigAlteration {
    key: String,
    operation: IncrementalConfigOperation,
}

impl IncrementalConfigAlteration {
    /// Creates one raw alteration for validation at admission.
    pub const fn new(key: String, operation: IncrementalConfigOperation) -> Self {
        Self { key, operation }
    }

    fn canonicalize(mut self) -> Self {
        self.key = canonical_string(self.key);
        self.operation = self.operation.canonicalize();
        self
    }

    fn text_bytes(&self) -> Option<usize> {
        self.key.len().checked_add(self.operation.value_bytes())
    }

    fn into_core(self) -> CoreAlteration {
        self.operation.into_core(self.key)
    }
}

/// One Kafka configuration resource and its caller-ordered changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicConfigAlterations {
    resource_type: i8,
    resource_name: String,
    alterations: Vec<IncrementalConfigAlteration>,
}

impl TopicConfigAlterations {
    /// Creates one raw topic change set for the compatibility path.
    pub const fn new(topic: String, alterations: Vec<IncrementalConfigAlteration>) -> Self {
        Self {
            resource_type: 2,
            resource_name: topic,
            alterations,
        }
    }

    /// Creates one exact raw resource change set for admission-time validation.
    pub const fn resource(
        resource_type: i8,
        resource_name: String,
        alterations: Vec<IncrementalConfigAlteration>,
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

    pub(super) fn canonicalize(mut self) -> Self {
        self.resource_name = canonical_string(self.resource_name);
        self.alterations = canonical_vec(
            self.alterations
                .into_iter()
                .map(IncrementalConfigAlteration::canonicalize)
                .collect(),
        );
        self
    }

    pub(super) fn alteration_count(&self) -> usize {
        self.alterations.len()
    }

    pub(super) fn text_bytes(&self) -> Option<usize> {
        self.alterations
            .iter()
            .try_fold(self.resource_name.len(), |bytes, alteration| {
                bytes.checked_add(alteration.text_bytes()?)
            })
    }

    pub(super) fn resource_name_bytes(&self) -> usize {
        self.resource_name.len()
    }

    pub(super) fn into_core(self) -> CoreResourceAlteration {
        CoreResourceAlteration::resource(
            self.resource_type,
            self.resource_name,
            self.alterations
                .into_iter()
                .map(IncrementalConfigAlteration::into_core)
                .collect(),
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.resource_name.capacity() == self.resource_name.len()
            && self.alterations.capacity() == self.alterations.len()
            && self
                .alterations
                .iter()
                .all(|alteration| alteration.key.capacity() == alteration.key.len())
    }
}

/// Resource-generic name for the exact type/name change-set value.
pub type IncrementalConfigResourceAlterations = TopicConfigAlterations;
