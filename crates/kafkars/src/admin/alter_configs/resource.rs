//! Stable resource-generic incremental configuration change set.

use crate::admin::ConfigResourceType;

use super::ConfigAlteration;

/// One exact configuration resource and its caller-ordered changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResourceAlterations {
    resource_type: ConfigResourceType,
    resource_name: String,
    alterations: Vec<ConfigAlteration>,
}

impl ConfigResourceAlterations {
    /// Creates one inert resource change set; validation occurs at submission.
    pub fn new<I>(
        resource_type: ConfigResourceType,
        resource_name: impl Into<String>,
        alterations: I,
    ) -> Self
    where
        I: IntoIterator<Item = ConfigAlteration>,
    {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            alterations: alterations.into_iter().collect(),
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

    /// Returns changes in caller order.
    pub fn alterations(&self) -> &[ConfigAlteration] {
        &self.alterations
    }

    pub(crate) fn into_parts(self) -> (ConfigResourceType, String, Vec<ConfigAlteration>) {
        (self.resource_type, self.resource_name, self.alterations)
    }
}
