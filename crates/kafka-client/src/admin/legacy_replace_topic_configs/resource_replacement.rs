//! Stable destructive full snapshot for one generic configuration resource.

use crate::admin::ConfigResourceType;

use super::LegacyTopicConfigEntry;

/// One resource and its complete caller-ordered legacy configuration snapshot.
///
/// Kafka treats omitted keys as deleted or reset. An empty entry list is
/// deliberately representable and clears the resource's explicit snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyConfigResourceReplacement {
    resource_type: ConfigResourceType,
    resource_name: String,
    entries: Vec<LegacyTopicConfigEntry>,
}

impl LegacyConfigResourceReplacement {
    /// Creates one inert destructive replacement; validation occurs at submit.
    pub fn new<I>(
        resource_type: ConfigResourceType,
        resource_name: impl Into<String>,
        entries: I,
    ) -> Self
    where
        I: IntoIterator<Item = LegacyTopicConfigEntry>,
    {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            entries: entries.into_iter().collect(),
        }
    }

    /// Returns Kafka's exact resource type, including future positive values.
    pub const fn resource_type(&self) -> ConfigResourceType {
        self.resource_type
    }

    /// Returns the exact requested resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns the complete destructive snapshot in caller order.
    pub fn entries(&self) -> &[LegacyTopicConfigEntry] {
        &self.entries
    }

    pub(crate) fn into_parts(self) -> (ConfigResourceType, String, Vec<LegacyTopicConfigEntry>) {
        (self.resource_type, self.resource_name, self.entries)
    }
}
