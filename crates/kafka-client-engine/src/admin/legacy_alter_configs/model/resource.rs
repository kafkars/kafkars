//! Canonical engine-owned legacy configuration resource snapshots.

use kafka_client_core::{
    LegacyConfigEntry as CoreConfigEntry,
    LegacyConfigResourceReplacement as CoreResourceReplacement,
};

use super::{canonical_string, canonical_vec};

/// One exact nullable configuration entry in a legacy replacement snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyConfigEntry {
    key: String,
    value: Option<String>,
}

impl LegacyConfigEntry {
    /// Creates one raw entry for validation at admission.
    pub const fn new(key: String, value: Option<String>) -> Self {
        Self { key, value }
    }

    fn canonicalize(mut self) -> Self {
        self.key = canonical_string(self.key);
        self.value = self.value.map(canonical_string);
        self
    }

    fn text_bytes(&self) -> Option<usize> {
        self.key
            .len()
            .checked_add(self.value.as_ref().map_or(0, String::len))
    }

    fn into_core(self) -> CoreConfigEntry {
        CoreConfigEntry::new(self.key, self.value)
    }
}

/// One Kafka resource and its complete caller-ordered configuration snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyTopicConfigReplacement {
    resource_type: i8,
    resource_name: String,
    configs: Vec<LegacyConfigEntry>,
}

impl LegacyTopicConfigReplacement {
    /// Creates one raw topic snapshot; an empty snapshot clears dynamic values.
    pub const fn new(topic: String, configs: Vec<LegacyConfigEntry>) -> Self {
        Self {
            resource_type: 2,
            resource_name: topic,
            configs,
        }
    }

    /// Creates one exact raw resource snapshot for admission-time validation.
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

    pub(super) fn canonicalize(mut self) -> Self {
        self.resource_name = canonical_string(self.resource_name);
        self.configs = canonical_vec(
            self.configs
                .into_iter()
                .map(LegacyConfigEntry::canonicalize)
                .collect(),
        );
        self
    }

    pub(super) fn config_count(&self) -> usize {
        self.configs.len()
    }

    pub(super) fn text_bytes(&self) -> Option<usize> {
        self.configs
            .iter()
            .try_fold(self.resource_name.len(), |bytes, config| {
                bytes.checked_add(config.text_bytes()?)
            })
    }

    pub(super) fn resource_name_bytes(&self) -> usize {
        self.resource_name.len()
    }

    pub(super) fn into_core(self) -> CoreResourceReplacement {
        CoreResourceReplacement::resource(
            self.resource_type,
            self.resource_name,
            self.configs
                .into_iter()
                .map(LegacyConfigEntry::into_core)
                .collect(),
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.resource_name.capacity() == self.resource_name.len()
            && self.configs.capacity() == self.configs.len()
            && self.configs.iter().all(|config| {
                config.key.capacity() == config.key.len()
                    && config
                        .value
                        .as_ref()
                        .is_none_or(|value| value.capacity() == value.len())
            })
    }
}

/// Resource-generic name for one exact type/name replacement snapshot.
pub type LegacyConfigResourceReplacement = LegacyTopicConfigReplacement;
